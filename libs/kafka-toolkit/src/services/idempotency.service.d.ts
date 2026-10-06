import { Repository, EntityManager } from 'typeorm';
import { ProcessedEventEntity } from '../entities/processed-event.entity';
export declare class IdempotencyService {
    private readonly repo;
    private readonly logger;
    constructor(repo: Repository<ProcessedEventEntity>);
    checkAndRecord(eventId: string, eventType: string, consumerGroup: string, manager?: EntityManager): Promise<boolean>;
}
