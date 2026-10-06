import {
  Controller,
  Post,
  Get,
  Param,
  Body,
  HttpCode,
  HttpStatus,
  Logger,
} from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { ActivationService } from '../services/activation.service';
import { TOPICS, FailureMode } from '@activation-poc/contracts';

export class CreateActivationDto {
  customerId: string;
  planId: string;
  simulateFailure?: FailureMode;
  channel?: string;
}

@Controller('activations')
export class ActivationController {
  private readonly logger = new Logger(ActivationController.name);

  constructor(private readonly activationService: ActivationService) {}

  @Post()
  @HttpCode(HttpStatus.ACCEPTED) // RF-02: 202 Accepted
  async createActivation(@Body() dto: CreateActivationDto) {
    return await this.activationService.createActivation(dto);
  }

  @Get(':id')
  async getActivation(@Param('id') id: string) {
    return await this.activationService.getActivation(id);
  }

  @Get()
  async listActivations() {
    return await this.activationService.listActivations();
  }

  // Kafka consumers for saga aggregation
  @EventPattern(TOPICS.BILLING_EVENTS)
  async handleBillingEvent(@Payload() message: any) {
    const event = typeof message === 'string' ? JSON.parse(message) : message;
    this.logger.log(`Received billing event: ${event.eventType} for correlationId: ${event.correlationId}`);
    await this.activationService.handleBillingResult(event);
  }

  @EventPattern(TOPICS.PROVISIONING_EVENTS)
  async handleProvisioningEvent(@Payload() message: any) {
    const event = typeof message === 'string' ? JSON.parse(message) : message;
    this.logger.log(`Received provisioning event: ${event.eventType} for correlationId: ${event.correlationId}`);
    await this.activationService.handleProvisioningResult(event);
  }
}
