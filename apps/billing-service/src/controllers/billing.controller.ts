import { Controller, Logger } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';
import { BillingService } from '../services/billing.service';
import { TOPICS, ActivationRequestedEvent, ActivationFailedEvent, EventEnvelope } from '@activation-poc/contracts';
import { DlqService } from '@activation-poc/kafka-toolkit';

@Controller()
export class BillingController {
  private readonly logger = new Logger(BillingController.name);

  constructor(
    private readonly billingService: BillingService,
    private readonly dlqService: DlqService,
  ) {}

  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  async handleActivationRequested(@Payload() message: any, @Ctx() context: KafkaContext) {
    await this.dlqService.consume<ActivationRequestedEvent>(context, message, (event) =>
      this.billingService.handleActivationRequested(event),
    );
  }

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  async handleActivationEvents(@Payload() message: any, @Ctx() context: KafkaContext) {
    await this.dlqService.consume<EventEnvelope>(context, message, async (event) => {
      if (event.eventType === 'ActivationFailed') {
        await this.billingService.handleActivationFailed(event as ActivationFailedEvent);
      }
    });
  }
}
