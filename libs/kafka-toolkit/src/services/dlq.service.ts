import { Injectable, Logger, OnModuleDestroy } from '@nestjs/common';
import { KafkaContext } from '@nestjs/microservices';
import { Kafka, Producer } from 'kafkajs';
import { EventEnvelope } from '@activation-poc/contracts';

const RETRY_DELAYS_MS = [1000, 2000, 4000];

const REQUIRED_ENVELOPE_FIELDS = ['eventId', 'eventType', 'correlationId', 'customerId'] as const;

/**
 * Error de contrato: el mensaje no es un sobre de evento válido.
 */
export class InvalidEventError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'InvalidEventError';
  }
}

/**
 * Convierte el payload recibido de Kafka en un sobre de evento validado.
 * Lanza InvalidEventError si el JSON está roto o faltan campos del sobre.
 */
export function parseEventEnvelope<T = EventEnvelope>(message: unknown): T {
  let event: any = message;
  if (typeof message === 'string') {
    try {
      event = JSON.parse(message);
    } catch {
      throw new InvalidEventError(`Payload is not valid JSON: ${message.substring(0, 100)}`);
    }
  }

  if (!event || typeof event !== 'object') {
    throw new InvalidEventError(`Payload is not an event envelope: ${String(message).substring(0, 100)}`);
  }

  const missing = REQUIRED_ENVELOPE_FIELDS.filter((field) => typeof event[field] !== 'string' || !event[field]);
  if (missing.length > 0) {
    throw new InvalidEventError(`Event envelope is missing fields: ${missing.join(', ')}`);
  }

  return event as T;
}

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
   * Punto de entrada común de todos los consumidores (RNF-04).
   * Valida el sobre, ejecuta el handler con 3 reintentos (1 s, 2 s, 4 s) y, si sigue
   * fallando, publica el mensaje original en <topic>.dlq. Nunca lanza: el offset avanza
   * y el mensaje roto no bloquea la partición.
   */
  async consume<T = EventEnvelope>(
    context: KafkaContext,
    message: unknown,
    handler: (event: T) => Promise<void>,
  ): Promise<void> {
    const topic = context.getTopic();
    const kafkaMessage = context.getMessage();
    const key = kafkaMessage.key == null ? null : String(kafkaMessage.key);

    await this.executeWithRetry(
      topic,
      key,
      message,
      () => handler(parseEventEnvelope<T>(message)),
      {
        'x-original-partition': String(context.getPartition()),
        'x-original-offset': String(kafkaMessage.offset),
        'x-consumer-group': process.env.KAFKA_GROUP_ID || 'unknown',
      },
    );
  }

  /**
   * Publishes failed message to <topic>.dlq
   */
  async sendToDlq(topic: string, key: string | null, message: any, errorReason: string, headers: Record<string, string> = {}) {
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
    key: string | null,
    message: any,
    fn: () => Promise<T>,
    headers: Record<string, string> = {},
  ): Promise<T | null> {
    let lastError: any;

    for (let attempt = 0; attempt <= RETRY_DELAYS_MS.length; attempt++) {
      try {
        return await fn();
      } catch (err: any) {
        lastError = err;
        this.logger.warn(`[${topic}] Execution attempt ${attempt + 1} failed: ${err.message}`);

        if (attempt < RETRY_DELAYS_MS.length) {
          const delay = RETRY_DELAYS_MS[attempt];
          this.logger.log(`Waiting ${delay}ms before retry...`);
          await new Promise((resolve) => setTimeout(resolve, delay));
        }
      }
    }

    // All attempts failed -> Send to DLQ and do not block partition
    await this.sendToDlq(topic, key, message, lastError?.message || 'Unknown error', {
      ...headers,
      'x-dlq-attempts': String(RETRY_DELAYS_MS.length + 1),
    });
    return null;
  }
}
