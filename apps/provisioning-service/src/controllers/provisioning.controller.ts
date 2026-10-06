import { Controller, Logger } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';
import { ProvisioningService } from '../services/provisioning.service';
import { TOPICS, ActivationRequestedEvent } from '@activation-poc/contracts';
import { DlqService } from '@activation-poc/kafka-toolkit';

@Controller()
export class ProvisioningController {
  private readonly logger = new Logger(ProvisioningController.name);

  constructor(
    private readonly provisioningService: ProvisioningService,
    private readonly dlqService: DlqService,
  ) {}

  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  async handleActivationRequested(@Payload() message: any, @Ctx() context: KafkaContext) {
    await this.dlqService.consume<ActivationRequestedEvent>(context, message, (event) =>
      this.provisioningService.handleActivationRequested(event),
    );
  }
}
