import { Injectable, Logger } from '@nestjs/common';
import { DataSource } from 'typeorm';
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
  private readonly consumerGroup = process.env.KAFKA_GROUP_ID || 'billing-svc';

  constructor(
    private readonly dataSource: DataSource,
    private readonly idempotencyService: IdempotencyService,
    private readonly outboxService: OutboxService,
  ) {}

  async handleActivationRequested(event: ActivationRequestedEvent) {
    const { eventId, eventType, correlationId, customerId, payload } = event;
    const now = new Date().toISOString();

    // Idempotencia, efecto y evento de salida en la misma transacción (RNF-03)
    await this.dataSource.transaction(async (manager) => {
      const isNew = await this.idempotencyService.checkAndRecord(eventId, eventType, this.consumerGroup, manager);
      if (!isNew) {
        return;
      }

      const repo = manager.getRepository(BillingAccountEntity);

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

        await this.outboxService.addEvent(TOPICS.BILLING_EVENTS, customerId, failedEvent, manager);
        return;
      }

      // Caso borde: ActivationFailed llegó antes que ActivationRequested y ya dejó la
      // cuenta registrada como CANCELLED. No se crea ni se factura nada.
      const existing = await repo.findOne({ where: { activationId: correlationId } });
      if (existing) {
        this.logger.warn(
          `Billing account for activation [${correlationId}] already exists with status ${existing.status}. Skipping creation.`,
        );
        return;
      }

      const billingAccountId = `bill-${uuidv4().substring(0, 8)}`;
      await repo.save(
        repo.create({
          id: billingAccountId,
          activationId: correlationId,
          customerId,
          planId: payload.planId,
          status: 'CREATED',
        }),
      );

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

      await this.outboxService.addEvent(TOPICS.BILLING_EVENTS, customerId, createdEvent, manager);
    });
  }

  async handleActivationFailed(event: ActivationFailedEvent) {
    const { eventId, eventType, correlationId, customerId, payload } = event;

    await this.dataSource.transaction(async (manager) => {
      const isNew = await this.idempotencyService.checkAndRecord(eventId, eventType, this.consumerGroup, manager);
      if (!isNew) {
        return;
      }

      // Si falló billing no hay cuenta que compensar
      if (payload.failedStep === 'billing') {
        return;
      }

      this.logger.log(`Compensating billing for failed activation: [${correlationId}]`);
      const repo = manager.getRepository(BillingAccountEntity);
      const account = await repo.findOne({ where: { activationId: correlationId } });

      if (!account) {
        // ActivationRequested todavía no llegó: se deja la cuenta registrada como
        // CANCELLED para que, cuando llegue, no se cree (persistente, sobrevive reinicios
        // y funciona con varias instancias de billing)
        await repo.save(
          repo.create({
            id: `bill-${uuidv4().substring(0, 8)}`,
            activationId: correlationId,
            customerId,
            planId: 'unknown',
            status: 'CANCELLED',
            cancellationReason: `Pre-emptively cancelled: ${payload.reason}`,
          }),
        );
        this.logger.warn(`No billing account yet for activation [${correlationId}]. Registered as pre-cancelled.`);
        return;
      }

      if (account.status !== 'CREATED') {
        return;
      }

      account.status = 'CANCELLED';
      account.cancellationReason = payload.reason;
      await repo.save(account);

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

      await this.outboxService.addEvent(TOPICS.BILLING_EVENTS, customerId, cancelledEvent, manager);
    });
  }
}
