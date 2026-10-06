import { Injectable, Logger, NotFoundException } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { DataSource, EntityManager, Repository } from 'typeorm';
import { v4 as uuidv4 } from 'uuid';
import {
  ActivationEntity,
  ActivationHistoryItem,
} from '../entities/activation.entity';
import { ActivationGateway } from '../gateways/activation.gateway';
import {
  TOPICS,
  EventEnvelope,
  ActivationRequestedEvent,
  ActivationCompletedEvent,
  ActivationFailedEvent,
  FailureMode,
} from '@activation-poc/contracts';
import { IdempotencyService, OutboxService } from '@activation-poc/kafka-toolkit';

type SagaStep = 'billing' | 'provisioning';

@Injectable()
export class ActivationService {
  private readonly logger = new Logger(ActivationService.name);
  private readonly consumerGroup = process.env.KAFKA_GROUP_ID || 'activation-api-group';

  constructor(
    @InjectRepository(ActivationEntity)
    private readonly repo: Repository<ActivationEntity>,
    private readonly dataSource: DataSource,
    private readonly gateway: ActivationGateway,
    private readonly outboxService: OutboxService,
    private readonly idempotencyService: IdempotencyService,
  ) {}

  async createActivation(data: {
    customerId: string;
    planId: string;
    simulateFailure?: FailureMode;
    channel?: string;
  }) {
    const activationId = `act-${Math.floor(1000 + Math.random() * 9000)}`;
    const eventId = uuidv4();
    const now = new Date().toISOString();

    const historyItem: ActivationHistoryItem = {
      eventType: 'ActivationRequested',
      at: now,
      details: {
        customerId: data.customerId,
        planId: data.planId,
        simulateFailure: data.simulateFailure || 'none',
      },
    };

    const event: ActivationRequestedEvent = {
      eventId,
      eventType: 'ActivationRequested',
      version: 1,
      occurredAt: now,
      correlationId: activationId,
      customerId: data.customerId,
      source: 'activation-api',
      payload: {
        planId: data.planId,
        channel: data.channel || 'web',
        simulateFailure: data.simulateFailure || 'none',
      },
    };

    // Activación y evento en la misma transacción (outbox transaccional): si la API se
    // cae entre medio, no queda una activación sin su ActivationRequested
    const saved = await this.dataSource.transaction(async (manager) => {
      const repo = manager.getRepository(ActivationEntity);
      const activation = await repo.save(
        repo.create({
          id: activationId,
          customerId: data.customerId,
          planId: data.planId,
          status: 'PENDING',
          simulateFailure: data.simulateFailure || 'none',
          steps: {},
          history: [historyItem],
        }),
      );

      await this.outboxService.addEvent(TOPICS.ACTIVATION_REQUESTED, data.customerId, event, manager);
      return activation;
    });

    this.logger.log(`Created activation [${activationId}] for customer [${data.customerId}]`);
    this.gateway.notifyActivationUpdate(saved, 'activation:created');

    return {
      activationId: saved.id,
      status: saved.status,
    };
  }

  async getActivation(id: string): Promise<ActivationEntity> {
    const activation = await this.repo.findOne({ where: { id } });
    if (!activation) {
      throw new NotFoundException(`Activation with id ${id} not found`);
    }
    return activation;
  }

  async listActivations(): Promise<ActivationEntity[]> {
    return await this.repo.find({
      order: { createdAt: 'DESC' },
      take: 50,
    });
  }

  async handleBillingResult(event: EventEnvelope) {
    await this.handleStepResult(event, 'billing', event.eventType === 'BillingAccountCreated');
  }

  async handleProvisioningResult(event: EventEnvelope) {
    await this.handleStepResult(event, 'provisioning', event.eventType === 'ProvisioningCompleted');
  }

  /**
   * Aplica el resultado de un paso de la saga. Idempotencia, lectura con lock, cambio
   * de estado y evento de salida van en una única transacción (RNF-03): un resultado
   * repetido no duplica el historial ni vuelve a publicar ActivationCompleted/Failed.
   */
  private async handleStepResult(event: EventEnvelope, stepName: SagaStep, isSuccess: boolean) {
    const updated = await this.dataSource.transaction(async (manager) => {
      const isNew = await this.idempotencyService.checkAndRecord(
        event.eventId,
        event.eventType,
        this.consumerGroup,
        manager,
      );
      if (!isNew) {
        return null;
      }

      const activation = await manager.getRepository(ActivationEntity).findOne({
        where: { id: event.correlationId },
        lock: { mode: 'pessimistic_write' },
      });
      if (!activation) {
        this.logger.warn(`Received ${stepName} result for unknown activation: ${event.correlationId}`);
        return null;
      }

      const now = new Date().toISOString();
      activation.steps = activation.steps || {};
      activation.steps[stepName] = {
        status: isSuccess ? 'OK' : 'FAILED',
        at: now,
        details: event.payload,
      };

      activation.history = activation.history || [];
      activation.history.push({
        eventType: event.eventType,
        at: now,
        details: event.payload,
      });

      return await this.evaluateSaga(manager, activation, isSuccess, stepName, event.payload?.reason);
    });

    // Se notifica a la UI recién después del commit
    if (updated) {
      this.gateway.notifyActivationUpdate(updated);
    }
  }

  private async evaluateSaga(
    manager: EntityManager,
    activation: ActivationEntity,
    currentStepSuccess: boolean,
    stepName: SagaStep,
    failureReason?: string,
  ): Promise<ActivationEntity> {
    const repo = manager.getRepository(ActivationEntity);
    const now = new Date().toISOString();

    // If activation is already in final state (FAILED), just update history and return
    if (activation.status === 'FAILED') {
      return await repo.save(activation);
    }

    // If current step failed, fail the saga immediately
    if (!currentStepSuccess) {
      activation.status = 'FAILED';
      const failEvent: ActivationFailedEvent = {
        eventId: uuidv4(),
        eventType: 'ActivationFailed',
        version: 1,
        occurredAt: now,
        correlationId: activation.id,
        customerId: activation.customerId,
        source: 'activation-api',
        payload: {
          reason: failureReason || `${stepName} step failed`,
          failedStep: stepName,
        },
      };

      activation.history.push({
        eventType: 'ActivationFailed',
        at: now,
        details: failEvent.payload,
      });

      await this.outboxService.addEvent(TOPICS.ACTIVATION_EVENTS, activation.customerId, failEvent, manager);

      const saved = await repo.save(activation);
      this.logger.error(`Activation [${activation.id}] marked as FAILED due to ${stepName}`);
      return saved;
    }

    // Both steps succeeded
    if (
      activation.steps.billing?.status === 'OK' &&
      activation.steps.provisioning?.status === 'OK'
    ) {
      activation.status = 'ACTIVE';
      const completedEvent: ActivationCompletedEvent = {
        eventId: uuidv4(),
        eventType: 'ActivationCompleted',
        version: 1,
        occurredAt: now,
        correlationId: activation.id,
        customerId: activation.customerId,
        source: 'activation-api',
        payload: {
          planId: activation.planId,
          billingAccountId: activation.steps.billing?.details?.billingAccountId,
          provisioningId: activation.steps.provisioning?.details?.provisioningId,
        },
      };

      activation.history.push({
        eventType: 'ActivationCompleted',
        at: now,
        details: completedEvent.payload,
      });

      await this.outboxService.addEvent(TOPICS.ACTIVATION_EVENTS, activation.customerId, completedEvent, manager);

      const saved = await repo.save(activation);
      this.logger.log(`Activation [${activation.id}] successfully marked as ACTIVE!`);
      return saved;
    }

    // One step completed OK, waiting for the other
    activation.status = 'IN_PROGRESS';
    const saved = await repo.save(activation);
    this.logger.log(`Activation [${activation.id}] is IN_PROGRESS (waiting for complementary step)`);
    return saved;
  }
}
