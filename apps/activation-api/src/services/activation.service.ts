import { Injectable, Logger, NotFoundException } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { v4 as uuidv4 } from 'uuid';
import {
  ActivationEntity,
  ActivationHistoryItem,
} from '../entities/activation.entity';
import { ActivationGateway } from '../gateways/activation.gateway';
import {
  TOPICS,
  ActivationRequestedEvent,
  ActivationCompletedEvent,
  ActivationFailedEvent,
  FailureMode,
} from '@activation-poc/contracts';
import { OutboxService } from '@activation-poc/kafka-toolkit';

@Injectable()
export class ActivationService {
  private readonly logger = new Logger(ActivationService.name);

  constructor(
    @InjectRepository(ActivationEntity)
    private readonly repo: Repository<ActivationEntity>,
    private readonly gateway: ActivationGateway,
    private readonly outboxService: OutboxService,
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

    const activation = this.repo.create({
      id: activationId,
      customerId: data.customerId,
      planId: data.planId,
      status: 'PENDING',
      simulateFailure: data.simulateFailure || 'none',
      steps: {},
      history: [historyItem],
    });

    const saved = await this.repo.save(activation);

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

    // Save event in transactional outbox for reliable delivery
    await this.outboxService.addEvent(
      TOPICS.ACTIVATION_REQUESTED,
      data.customerId,
      event,
    );

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

  async handleBillingResult(event: any) {
    const activationId = event.correlationId;
    const activation = await this.repo.findOne({ where: { id: activationId } });
    if (!activation) {
      this.logger.warn(`Received billing result for unknown activation: ${activationId}`);
      return;
    }

    const isSuccess = event.eventType === 'BillingAccountCreated';
    const now = new Date().toISOString();

    activation.steps = activation.steps || {};
    activation.steps.billing = {
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

    await this.evaluateSaga(activation, isSuccess, 'billing', event.payload?.reason);
  }

  async handleProvisioningResult(event: any) {
    const activationId = event.correlationId;
    const activation = await this.repo.findOne({ where: { id: activationId } });
    if (!activation) {
      this.logger.warn(`Received provisioning result for unknown activation: ${activationId}`);
      return;
    }

    const isSuccess = event.eventType === 'ProvisioningCompleted';
    const now = new Date().toISOString();

    activation.steps = activation.steps || {};
    activation.steps.provisioning = {
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

    await this.evaluateSaga(activation, isSuccess, 'provisioning', event.payload?.reason);
  }

  private async evaluateSaga(
    activation: ActivationEntity,
    currentStepSuccess: boolean,
    stepName: 'billing' | 'provisioning',
    failureReason?: string,
  ) {
    const now = new Date().toISOString();

    // If activation is already in final state (FAILED), just update history and return
    if (activation.status === 'FAILED') {
      await this.repo.save(activation);
      this.gateway.notifyActivationUpdate(activation);
      return;
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

      await this.outboxService.addEvent(
        TOPICS.ACTIVATION_EVENTS,
        activation.customerId,
        failEvent,
      );

      const saved = await this.repo.save(activation);
      this.logger.error(`Activation [${activation.id}] marked as FAILED due to ${stepName}`);
      this.gateway.notifyActivationUpdate(saved);
      return;
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

      await this.outboxService.addEvent(
        TOPICS.ACTIVATION_EVENTS,
        activation.customerId,
        completedEvent,
      );

      const saved = await this.repo.save(activation);
      this.logger.log(`Activation [${activation.id}] successfully marked as ACTIVE!`);
      this.gateway.notifyActivationUpdate(saved);
      return;
    }

    // One step completed OK, waiting for the other
    activation.status = 'IN_PROGRESS';
    const saved = await this.repo.save(activation);
    this.logger.log(`Activation [${activation.id}] is IN_PROGRESS (waiting for complementary step)`);
    this.gateway.notifyActivationUpdate(saved);
  }
}
