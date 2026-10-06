import { Injectable, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, EntityManager } from 'typeorm';
import { ProcessedEventEntity } from '../entities/processed-event.entity';

@Injectable()
export class IdempotencyService {
  private readonly logger = new Logger(IdempotencyService.name);

  constructor(
    @InjectRepository(ProcessedEventEntity)
    private readonly repo: Repository<ProcessedEventEntity>,
  ) {}

  /**
   * Attempts to record the event execution for a specific consumer group.
   * Returns true if it was recorded successfully (first time processing).
   * Returns false if it was already processed (duplicate detected).
   */
  async checkAndRecord(
    eventId: string,
    eventType: string,
    consumerGroup: string,
    manager?: EntityManager,
  ): Promise<boolean> {
    const repository = manager ? manager.getRepository(ProcessedEventEntity) : this.repo;

    try {
      const record = repository.create({
        eventId,
        eventType,
        consumerGroup,
        processedAt: new Date(),
      });
      await repository.save(record);
      return true;
    } catch (err: any) {
      // Postgres unique constraint violation code is 23505
      if (err.code === '23505' || err.message?.includes('duplicate key') || err.message?.includes('unique constraint')) {
        this.logger.warn(`Duplicate event detected [${eventType} - ID: ${eventId}] for group ${consumerGroup}. Skipping.`);
        return false;
      }
      this.logger.error(`Error saving processed event [${eventId}]: ${err.message}`, err.stack);
      throw err;
    }
  }
}
