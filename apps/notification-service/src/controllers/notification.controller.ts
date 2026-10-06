import { Controller, Logger } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';
import { NotificationService } from '../services/notification.service';
import { TOPICS, ActivationCompletedEvent, ActivationFailedEvent, EventEnvelope } from '@activation-poc/contracts';
import { DlqService } from '@activation-poc/kafka-toolkit';

@Controller()
export class NotificationController {
  private readonly logger = new Logger(NotificationController.name);

  constructor(
    private readonly notificationService: NotificationService,
    private readonly dlqService: DlqService,
  ) {}

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  async handleActivationEvents(@Payload() message: any, @Ctx() context: KafkaContext) {
    await this.dlqService.consume<EventEnvelope>(context, message, async (event) => {
      if (event.eventType === 'ActivationCompleted') {
        await this.notificationService.handleActivationCompleted(event as ActivationCompletedEvent);
      } else if (event.eventType === 'ActivationFailed') {
        await this.notificationService.handleActivationFailed(event as ActivationFailedEvent);
      }
    });
  }
}
