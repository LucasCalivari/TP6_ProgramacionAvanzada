import { Controller, Logger } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { ProvisioningService } from '../services/provisioning.service';
import { TOPICS, ActivationRequestedEvent } from '@activation-poc/contracts';

@Controller()
export class ProvisioningController {
  private readonly logger = new Logger(ProvisioningController.name);

  constructor(private readonly provisioningService: ProvisioningService) {}

  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  async handleActivationRequested(@Payload() message: any) {
    const event: ActivationRequestedEvent =
      typeof message === 'string' ? JSON.parse(message) : message;
    await this.provisioningService.handleActivationRequested(event);
  }
}
