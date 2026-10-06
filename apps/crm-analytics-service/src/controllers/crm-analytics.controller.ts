import { Controller, Logger } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';
import { CrmAnalyticsService } from '../services/crm-analytics.service';
import { TOPICS, EventEnvelope } from '@activation-poc/contracts';
import { DlqService } from '@activation-poc/kafka-toolkit';

@Controller()
export class CrmAnalyticsController {
  private readonly logger = new Logger(CrmAnalyticsController.name);

  constructor(
    private readonly crmService: CrmAnalyticsService,
    private readonly dlqService: DlqService,
  ) {}

  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  async handleActivationRequested(@Payload() message: any, @Ctx() context: KafkaContext) {
    await this.log(message, context);
  }

  @EventPattern(TOPICS.BILLING_EVENTS)
  async handleBillingEvents(@Payload() message: any, @Ctx() context: KafkaContext) {
    await this.log(message, context);
  }

  @EventPattern(TOPICS.PROVISIONING_EVENTS)
  async handleProvisioningEvents(@Payload() message: any, @Ctx() context: KafkaContext) {
    await this.log(message, context);
  }

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  async handleActivationEvents(@Payload() message: any, @Ctx() context: KafkaContext) {
    await this.log(message, context);
  }

  private async log(message: any, context: KafkaContext) {
    await this.dlqService.consume<EventEnvelope>(context, message, (event) =>
      this.crmService.logEvent(context.getTopic(), event),
    );
  }
}
