import { Injectable, Logger, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, EntityManager } from 'typeorm';
import { ProcessedEventEntity } from '../entities/processed-event.entity';

// Igual a la retención de los topics: un evento más viejo ya no puede volver a llegar
const PROCESSED_EVENTS_TTL_DAYS = 7;
const CLEANUP_INTERVAL_MS = 60 * 60 * 1000;

@Injectable()
export class IdempotencyService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(IdempotencyService.name);
  private cleanupIntervalId: NodeJS.Timeout | null = null;

  constructor(
    @InjectRepository(ProcessedEventEntity)
    private readonly repo: Repository<ProcessedEventEntity>,
  ) {}

  onModuleInit() {
    this.cleanupIntervalId = setInterval(() => this.purgeExpired(), CLEANUP_INTERVAL_MS);
  }

  onModuleDestroy() {
    if (this.cleanupIntervalId) {
      clearInterval(this.cleanupIntervalId);
    }
  }

  /**
   * Registra el eventId como procesado. Debe llamarse con el EntityManager de la
   * misma transacción que aplica el efecto del evento (RNF-03): si el efecto falla,
   * el rollback también deshace el registro y el reintento lo vuelve a procesar.
   *
   * Devuelve true si es la primera vez, false si es un duplicado.
   */
  async checkAndRecord(
    eventId: string,
    eventType: string,
    consumerGroup: string,
    manager?: EntityManager,
  ): Promise<boolean> {
    const runner = manager ?? this.repo.manager;

    // ON CONFLICT DO NOTHING no aborta la transacción ante un duplicado (un INSERT que
    // falla por clave única sí lo haría en Postgres)
    const rows = await runner.query(
      `INSERT INTO processed_events (event_id, event_type, consumer_group)
       VALUES ($1, $2, $3)
       ON CONFLICT (event_id) DO NOTHING
       RETURNING event_id`,
      [eventId, eventType, consumerGroup],
    );

    if (rows.length === 0) {
      this.logger.warn(`Duplicate event detected [${eventType} - ID: ${eventId}] for group ${consumerGroup}. Skipping.`);
      return false;
    }
    return true;
  }

  /**
   * Chequeo rápido sin bloqueo, útil para evitar trabajo caro (ej: un retardo simulado)
   * antes de abrir la transacción. No reemplaza a checkAndRecord.
   */
  async isProcessed(eventId: string): Promise<boolean> {
    return (await this.repo.count({ where: { eventId } })) > 0;
  }

  private async purgeExpired() {
    try {
      await this.repo.query(
        `DELETE FROM processed_events WHERE processed_at < now() - make_interval(days => $1)`,
        [PROCESSED_EVENTS_TTL_DAYS],
      );
    } catch (err: any) {
      this.logger.warn(`Could not purge expired processed events: ${err.message}`);
    }
  }
}
