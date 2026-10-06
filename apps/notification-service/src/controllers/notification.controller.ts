import { Controller, Logger } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { NotificationService } from '../services/notification.service';
import { TOPICS, ActivationCompletedEvent, ActivationFailedEvent } from '@activation-poc/contracts';

@Controller()
export class NotificationController {
  private readonly logger = new Logger(NotificationController.name);

  constructor(private readonly notificationService: NotificationService) {}

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  async handleActivationEvents(@Payload() message: any) {
    const event = typeof message === 'string' ? JSON.parse(message) : message;
    if (event.eventType === 'ActivationCompleted') {
      await this.notificationService.handleActivationCompleted(event as ActivationCompletedEvent);
    } else if (event.eventType === 'ActivationFailed') {
      await this.notificationService.handleActivationFailed(event as ActivationFailedEvent);
    }
  }
}
