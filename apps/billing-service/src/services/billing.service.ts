import { Injectable, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { v4 as uuidv4 } from 'uuid';
import { BillingAccountEntity } from '../entities/billing-account.entity';
import {
  TOPICS,
  ActivationRequestedEvent,
  ActivationFailedEvent,
  BillingAccountCreatedEvent,
  BillingFailedEvent,
  BillingAccountCancelledEvent,
} from '@activation-poc/contracts';
import { IdempotencyService, OutboxService } from '@activation-poc/kafka-toolkit';

@Injectable()
export class BillingService {
  private readonly logger = new Logger(BillingService.name);
  private cancelledActivationIds = new Set<string>();

  constructor(
    @InjectRepository(BillingAccountEntity)
    private readonly repo: Repository<BillingAccountEntity>,
    private readonly idempotencyService: IdempotencyService,
    private readonly outboxService: OutboxService,
  ) {}

  async handleActivationRequested(event: ActivationRequestedEvent) {
    const { eventId, eventType, correlationId, customerId, payload } = event;

    // Check idempotency (RNF-03)
    const isNew = await this.idempotencyService.checkAndRecord(
      eventId,
      eventType,
      process.env.KAFKA_GROUP_ID || 'billing-svc',
    );
    if (!isNew) {
      this.logger.warn(`Event ${eventId} already processed by billing-service, skipping.`);
      return;
    }

    const now = new Date().toISOString();

    // Check if failure is requested for billing
    if (payload.simulateFailure === 'billing') {
      this.logger.warn(`Simulating billing failure for activation [${correlationId}]`);
      const failedEvent: BillingFailedEvent = {
        eventId: uuidv4(),
        eventType: 'BillingFailed',
        version: 1,
        occurredAt: now,
        correlationId,
        customerId,
        source: 'billing-service',
        payload: {
          reason: 'Simulated payment processing / billing rejection',
          planId: payload.planId,
        },
      };

      await this.outboxService.addEvent(
        TOPICS.BILLING_EVENTS,
        customerId,
        failedEvent,
      );
      return;
    }

    // Edge case: check if this activation was cancelled before creation arrived
    const isPreCancelled = this.cancelledActivationIds.has(correlationId);

    const billingAccountId = `bill-${uuidv4().substring(0, 8)}`;
    const account = this.repo.create({
      id: billingAccountId,
      activationId: correlationId,
      customerId,
      planId: payload.planId,
      status: isPreCancelled ? 'CANCELLED' : 'CREATED',
      cancellationReason: isPreCancelled ? 'Pre-emptively cancelled by failed saga' : undefined,
    });

    await this.repo.save(account);

    if (isPreCancelled) {
      this.logger.warn(`Created billing account [${billingAccountId}] as CANCELLED directly due to previous failure.`);
      return;
    }

    this.logger.log(`Billing account created: [${billingAccountId}] for activation: [${correlationId}]`);

    const createdEvent: BillingAccountCreatedEvent = {
      eventId: uuidv4(),
      eventType: 'BillingAccountCreated',
      version: 1,
      occurredAt: now,
      correlationId,
      customerId,
      source: 'billing-service',
      payload: {
        billingAccountId,
        planId: payload.planId,
      },
    };

    await this.outboxService.addEvent(
      TOPICS.BILLING_EVENTS,
      customerId,
      createdEvent,
    );
  }

  async handleActivationFailed(event: ActivationFailedEvent) {
    const { correlationId, customerId, payload } = event;
    this.logger.log(`Compensating billing for failed activation: [${correlationId}]`);

    this.cancelledActivationIds.add(correlationId);

    const account = await this.repo.findOne({ where: { activationId: correlationId } });
    if (!account) {
      this.logger.log(`No billing account found yet for activation [${correlationId}]. Flagged for cancellation.`);
      return;
    }

    if (account.status === 'CREATED') {
      account.status = 'CANCELLED';
      account.cancellationReason = payload.reason;
      await this.repo.save(account);

      this.logger.log(`Billing account [${account.id}] has been CANCELLED (Compensated)`);

      const cancelledEvent: BillingAccountCancelledEvent = {
        eventId: uuidv4(),
        eventType: 'BillingAccountCancelled',
        version: 1,
        occurredAt: new Date().toISOString(),
        correlationId,
        customerId,
        source: 'billing-service',
        payload: {
          billingAccountId: account.id,
          reason: payload.reason,
        },
      };

      await this.outboxService.addEvent(
        TOPICS.BILLING_EVENTS,
        customerId,
        cancelledEvent,
      );
    }
  }
}
