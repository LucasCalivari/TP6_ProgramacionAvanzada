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

  /**
   * Idempotente por eventId (índice único en event_log), así un replay desde el
   * inicio del topic no duplica registros (RF-10, RNF-03). Los errores se propagan
   * para que DlqService reintente y, si persisten, mande el mensaje a la DLQ.
   */
  async logEvent(topic: string, event: EventEnvelope) {
    const result = await this.repo
      .createQueryBuilder()
      .insert()
      .into(EventLogEntity)
      .values({
        id: `crm-${uuidv4().substring(0, 8)}`,
        eventId: event.eventId,
        eventType: event.eventType,
        correlationId: event.correlationId,
        customerId: event.customerId,
        source: event.source,
        topic,
        payload: event.payload ?? {},
      })
      .orIgnore()
      .returning(['eventId'])
      .execute();

    if (result.raw.length === 0) {
      this.logger.debug(`Event [${event.eventId}] already in crm-analytics log, skipping.`);
      return;
    }
    this.logger.log(`Logged event [${event.eventType}] (ID: ${event.eventId}) from topic [${topic}]`);
  }

  async getAllLogs() {
    return await this.repo.find({
      order: { receivedAt: 'DESC' },
      take: 100,
    });
  }
}
