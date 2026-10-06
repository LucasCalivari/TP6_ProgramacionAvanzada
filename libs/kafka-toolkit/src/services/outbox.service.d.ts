import { OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { Repository, EntityManager } from 'typeorm';
import { OutboxEntity } from '../entities/outbox.entity';
export declare class OutboxService implements OnModuleInit, OnModuleDestroy {
    private readonly outboxRepo;
    private readonly logger;
    private producer;
    private intervalId;
    private isProcessing;
    constructor(outboxRepo: Repository<OutboxEntity>);
    onModuleInit(): Promise<void>;
    onModuleDestroy(): Promise<void>;
    addEvent(topic: string, key: string, payload: any, manager?: EntityManager): Promise<OutboxEntity>;
    private processOutbox;
}
