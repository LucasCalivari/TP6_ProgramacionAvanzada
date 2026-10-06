import { Injectable, Logger, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, EntityManager } from 'typeorm';
import { OutboxEntity } from '../entities/outbox.entity';
import { Kafka, Producer } from 'kafkajs';

@Injectable()
export class OutboxService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(OutboxService.name);
  private producer: Producer;
  private intervalId: NodeJS.Timeout | null = null;
  private isProcessing = false;

  constructor(
    @InjectRepository(OutboxEntity)
    private readonly outboxRepo: Repository<OutboxEntity>,
  ) {
    const brokers = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
    const kafka = new Kafka({
      clientId: `${process.env.KAFKA_CLIENT_ID || 'service'}-outbox`,
      brokers,
    });
    this.producer = kafka.producer();
  }

  async onModuleInit() {
    try {
      await this.producer.connect();
      // Start polling outbox every 500 ms as specified in architecture document
      this.intervalId = setInterval(() => this.processOutbox(), 500);
    } catch (err: any) {
      this.logger.warn(`Could not start outbox processor immediately: ${err.message}`);
    }
  }

  async onModuleDestroy() {
    if (this.intervalId) {
      clearInterval(this.intervalId);
    }
    await this.producer.disconnect();
  }

  /**
   * Adds an event to the outbox table inside the same transaction
   */
  async addEvent(
    topic: string,
    key: string,
    payload: any,
    manager?: EntityManager,
  ): Promise<OutboxEntity> {
    const repo = manager ? manager.getRepository(OutboxEntity) : this.outboxRepo;
    const entry = repo.create({
      topic,
      key,
      payload,
      status: 'PENDING',
    });
    return await repo.save(entry);
  }

  private async processOutbox() {
    if (this.isProcessing) return;
    this.isProcessing = true;

    try {
      const pendingEvents = await this.outboxRepo.find({
        where: { status: 'PENDING' },
        order: { createdAt: 'ASC' },
        take: 20,
      });

      for (const event of pendingEvents) {
        try {
          await this.producer.send({
            topic: event.topic,
            messages: [
              {
                key: event.key,
                value: JSON.stringify(event.payload),
              },
            ],
          });
          event.status = 'SENT';
          event.sentAt = new Date();
          await this.outboxRepo.save(event);
        } catch (err: any) {
          this.logger.error(`Failed to publish outbox event ${event.id}: ${err.message}`);
          event.status = 'FAILED';
          event.error = err.message;
          await this.outboxRepo.save(event);
        }
      }
    } catch (err: any) {
      this.logger.debug(`Outbox polling error: ${err.message}`);
    } finally {
      this.isProcessing = false;
    }
  }
}
