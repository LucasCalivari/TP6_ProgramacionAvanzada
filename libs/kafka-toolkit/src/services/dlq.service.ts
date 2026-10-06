import { Injectable, Logger, OnModuleDestroy } from '@nestjs/common';
import { Kafka, Producer } from 'kafkajs';

@Injectable()
export class DlqService implements OnModuleDestroy {
  private readonly logger = new Logger(DlqService.name);
  private producer: Producer;
  private isConnected = false;

  constructor() {
    const brokers = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
    const kafka = new Kafka({
      clientId: `${process.env.KAFKA_CLIENT_ID || 'service'}-dlq`,
      brokers,
    });
    this.producer = kafka.producer();
  }

  async onModuleDestroy() {
    if (this.isConnected) {
      try {
        await this.producer.disconnect();
      } catch (e) {}
    }
  }

  private async ensureConnected() {
    if (!this.isConnected) {
      await this.producer.connect();
      this.isConnected = true;
    }
  }

  /**
   * Publishes failed message to <topic>.dlq
   */
  async sendToDlq(topic: string, key: string, message: any, errorReason: string, headers: Record<string, string> = {}) {
    const dlqTopic = `${topic}.dlq`;
    this.logger.error(`Sending failed event to DLQ topic [${dlqTopic}]: ${errorReason}`);

    try {
      await this.ensureConnected();
      await this.producer.send({
        topic: dlqTopic,
        messages: [
          {
            key,
            value: typeof message === 'string' ? message : JSON.stringify(message),
            headers: {
              ...headers,
              'x-dlq-reason': errorReason,
              'x-dlq-timestamp': new Date().toISOString(),
              'x-original-topic': topic,
            },
          },
        ],
      });
      this.logger.log(`Successfully moved message to ${dlqTopic}`);
    } catch (err: any) {
      this.isConnected = false;
      this.logger.error(`Failed to publish message to DLQ [${dlqTopic}]: ${err.message}`);
    }
  }

  /**
   * Executes a business action with up to 3 retries (1s, 2s, 4s).
   * If it fails after 3 retries, sends to DLQ.
   */
  async executeWithRetry<T>(
    topic: string,
    key: string,
    message: any,
    fn: () => Promise<T>,
  ): Promise<T | null> {
    const delays = [1000, 2000, 4000];
    let lastError: any;

    for (let attempt = 0; attempt <= delays.length; attempt++) {
      try {
        return await fn();
      } catch (err: any) {
        lastError = err;
        this.logger.warn(`Execution attempt ${attempt + 1} failed: ${err.message}`);

        if (attempt < delays.length) {
          const delay = delays[attempt];
          this.logger.log(`Waiting ${delay}ms before retry...`);
          await new Promise((resolve) => setTimeout(resolve, delay));
        }
      }
    }

    // All attempts failed -> Send to DLQ and do not block partition
    await this.sendToDlq(topic, key, message, lastError?.message || 'Unknown error');
    return null;
  }
}
