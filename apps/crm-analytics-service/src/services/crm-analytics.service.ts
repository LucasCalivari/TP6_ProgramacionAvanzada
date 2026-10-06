import { Injectable, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { v4 as uuidv4 } from 'uuid';
import { EventLogEntity } from '../entities/event-log.entity';
import { EventEnvelope } from '@activation-poc/contracts';

@Injectable()
export class CrmAnalyticsService {
  private readonly logger = new Logger(CrmAnalyticsService.name);

  constructor(
    @InjectRepository(EventLogEntity)
    private readonly repo: Repository<EventLogEntity>,
  ) {}

  async logEvent(topic: string, event: EventEnvelope) {
    try {
      // Check if already logged by eventId (idempotent logging)
      const existing = await this.repo.findOne({ where: { eventId: event.eventId } });
      if (existing) {
        this.logger.debug(`Event [${event.eventId}] already in crm-analytics log, skipping.`);
        return;
      }

      const entry = this.repo.create({
        id: `crm-${uuidv4().substring(0, 8)}`,
        eventId: event.eventId,
        eventType: event.eventType,
        correlationId: event.correlationId,
        customerId: event.customerId,
        source: event.source,
        topic,
        payload: event.payload,
      });

      await this.repo.save(entry);
      this.logger.log(`Logged event [${event.eventType}] (ID: ${event.eventId}) from topic [${topic}]`);
    } catch (err: any) {
      this.logger.error(`Failed to log event in CRM Analytics: ${err.message}`);
    }
  }

  async getAllLogs() {
    return await this.repo.find({
      order: { receivedAt: 'DESC' },
      take: 100,
    });
  }
}
