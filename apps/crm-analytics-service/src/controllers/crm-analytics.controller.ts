import { Controller, Logger } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { CrmAnalyticsService } from '../services/crm-analytics.service';
import { TOPICS, EventEnvelope } from '@activation-poc/contracts';

@Controller()
export class CrmAnalyticsController {
  private readonly logger = new Logger(CrmAnalyticsController.name);

  constructor(private readonly crmService: CrmAnalyticsService) {}

  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  async handleActivationRequested(@Payload() message: any) {
    const event: EventEnvelope = typeof message === 'string' ? JSON.parse(message) : message;
    await this.crmService.logEvent(TOPICS.ACTIVATION_REQUESTED, event);
  }

  @EventPattern(TOPICS.BILLING_EVENTS)
  async handleBillingEvents(@Payload() message: any) {
    const event: EventEnvelope = typeof message === 'string' ? JSON.parse(message) : message;
    await this.crmService.logEvent(TOPICS.BILLING_EVENTS, event);
  }

  @EventPattern(TOPICS.PROVISIONING_EVENTS)
  async handleProvisioningEvents(@Payload() message: any) {
    const event: EventEnvelope = typeof message === 'string' ? JSON.parse(message) : message;
    await this.crmService.logEvent(TOPICS.PROVISIONING_EVENTS, event);
  }

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  async handleActivationEvents(@Payload() message: any) {
    const event: EventEnvelope = typeof message === 'string' ? JSON.parse(message) : message;
    await this.crmService.logEvent(TOPICS.ACTIVATION_EVENTS, event);
  }
}
