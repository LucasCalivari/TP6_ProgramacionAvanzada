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
  private isConnected = false;

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
    // Start polling outbox every 500 ms unconditionally
    this.intervalId = setInterval(() => this.processOutbox(), 500);

    // Initial connection attempt with graceful retry
    this.ensureConnected().catch((err) => {
      this.logger.warn(`Initial Kafka connection pending in outbox: ${err.message}`);
    });
  }

  async onModuleDestroy() {
    if (this.intervalId) {
      clearInterval(this.intervalId);
    }
    if (this.isConnected) {
      try {
        await this.producer.disconnect();
      } catch (e) {}
    }
  }

  private async ensureConnected(): Promise<void> {
    if (!this.isConnected) {
      await this.producer.connect();
      this.isConnected = true;
      this.logger.log('Outbox Kafka producer connected successfully');
    }
  }

  /**
   * Adds an event to the outbox table inside the same transaction
   * and immediately triggers the poller so there is no lag.
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
    const saved = await repo.save(entry);

    // Trigger immediate flush asynchronously
    setImmediate(() => this.processOutbox());

    return saved;
  }

  private async processOutbox() {
    if (this.isProcessing) return;
    this.isProcessing = true;

    try {
      await this.ensureConnected();

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
          this.logger.log(`[Outbox] Sent event [${event.topic}] for key: ${event.key}`);
        } catch (err: any) {
          this.logger.error(`[Outbox] Failed to send message for ${event.id}: ${err.message}`);
        }
      }
    } catch (err: any) {
      this.isConnected = false;
      this.logger.debug(`Outbox connection retry needed: ${err.message}`);
    } finally {
      this.isProcessing = false;
    }
  }
}
