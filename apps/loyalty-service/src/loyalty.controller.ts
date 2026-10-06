import { Controller, Logger } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';
import { TOPICS, ActivationCompletedEvent } from '@activation-poc/contracts';
import { LoyaltyService } from './loyalty.service';

@Controller()
export class LoyaltyController {
  private readonly logger = new Logger(LoyaltyController.name);

  constructor(private readonly loyaltyService: LoyaltyService) {}

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  handleActivationEvents(@Payload() message: any, @Ctx() context: KafkaContext) {
    let event: any;
    try {
      event = typeof message === 'string' ? JSON.parse(message) : message;
    } catch {
      this.logger.warn(`Ignoring non-JSON message at offset ${context.getMessage().offset}`);
      return;
    }

    if (event?.eventType === 'ActivationCompleted') {
      this.loyaltyService.awardPoints(
        event as ActivationCompletedEvent,
        context.getPartition(),
        context.getMessage().offset,
      );
    }
  }
}
