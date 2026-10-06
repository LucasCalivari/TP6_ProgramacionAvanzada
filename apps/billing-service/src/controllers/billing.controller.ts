import { Controller, Logger } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { BillingService } from '../services/billing.service';
import { TOPICS, ActivationRequestedEvent, ActivationFailedEvent } from '@activation-poc/contracts';

@Controller()
export class BillingController {
  private readonly logger = new Logger(BillingController.name);

  constructor(private readonly billingService: BillingService) {}

  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  async handleActivationRequested(@Payload() message: any) {
    const event: ActivationRequestedEvent =
      typeof message === 'string' ? JSON.parse(message) : message;
    await this.billingService.handleActivationRequested(event);
  }

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  async handleActivationEvents(@Payload() message: any) {
    const event = typeof message === 'string' ? JSON.parse(message) : message;
    if (event.eventType === 'ActivationFailed') {
      await this.billingService.handleActivationFailed(event as ActivationFailedEvent);
    }
  }
}
