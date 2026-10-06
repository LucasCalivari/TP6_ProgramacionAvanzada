import { Injectable, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { v4 as uuidv4 } from 'uuid';
import { ProvisioningOrderEntity } from '../entities/provisioning-order.entity';
import {
  TOPICS,
  ActivationRequestedEvent,
  ProvisioningCompletedEvent,
  ProvisioningFailedEvent,
} from '@activation-poc/contracts';
import { IdempotencyService, OutboxService } from '@activation-poc/kafka-toolkit';

@Injectable()
export class ProvisioningService {
  private readonly logger = new Logger(ProvisioningService.name);

  constructor(
    @InjectRepository(ProvisioningOrderEntity)
    private readonly repo: Repository<ProvisioningOrderEntity>,
    private readonly idempotencyService: IdempotencyService,
    private readonly outboxService: OutboxService,
  ) {}

  async handleActivationRequested(event: ActivationRequestedEvent) {
    const { eventId, eventType, correlationId, customerId, payload } = event;

    // Check idempotency (RNF-03)
    const isNew = await this.idempotencyService.checkAndRecord(
      eventId,
      eventType,
      process.env.KAFKA_GROUP_ID || 'provisioning-svc',
    );
    if (!isNew) {
      this.logger.warn(`Event ${eventId} already processed by provisioning-service, skipping.`);
      return;
    }

    // Simulate provisioning delay of 1 to 3 seconds as required by architecture
    const delay = Math.floor(1000 + Math.random() * 2000);
    this.logger.log(`Simulating network provisioning work (${delay}ms) for activation [${correlationId}]...`);
    await new Promise((resolve) => setTimeout(resolve, delay));

    const now = new Date().toISOString();

    // Check if failure simulation requested for provisioning
    if (payload.simulateFailure === 'provisioning') {
      this.logger.warn(`Simulating provisioning network failure for activation [${correlationId}]`);

      const orderId = `prov-${uuidv4().substring(0, 8)}`;
      const order = this.repo.create({
        id: orderId,
        activationId: correlationId,
        customerId,
        planId: payload.planId,
        status: 'FAILED',
        failureReason: 'Simulated network allocation timeout / HLR failure',
      });
      await this.repo.save(order);

      const failedEvent: ProvisioningFailedEvent = {
        eventId: uuidv4(),
        eventType: 'ProvisioningFailed',
        version: 1,
        occurredAt: now,
        correlationId,
        customerId,
        source: 'provisioning-service',
        payload: {
          reason: 'Network allocation timeout / HLR provisioning error',
          planId: payload.planId,
        },
      };

      await this.outboxService.addEvent(
        TOPICS.PROVISIONING_EVENTS,
        customerId,
        failedEvent,
      );
      return;
    }

    const orderId = `prov-${uuidv4().substring(0, 8)}`;
    const order = this.repo.create({
      id: orderId,
      activationId: correlationId,
      customerId,
      planId: payload.planId,
      status: 'COMPLETED',
    });
    await this.repo.save(order);

    this.logger.log(`Provisioning completed successfully [${orderId}] for activation [${correlationId}]`);

    const completedEvent: ProvisioningCompletedEvent = {
      eventId: uuidv4(),
      eventType: 'ProvisioningCompleted',
      version: 1,
      occurredAt: now,
      correlationId,
      customerId,
      source: 'provisioning-service',
      payload: {
        provisioningId: orderId,
        planId: payload.planId,
      },
    };

    await this.outboxService.addEvent(
      TOPICS.PROVISIONING_EVENTS,
      customerId,
      completedEvent,
    );
  }
}
