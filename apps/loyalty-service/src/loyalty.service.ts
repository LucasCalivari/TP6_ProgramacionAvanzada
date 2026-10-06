import { Injectable, Logger } from '@nestjs/common';
import { ActivationCompletedEvent } from '@activation-poc/contracts';

const POINTS_PER_PLAN: Record<string, number> = {
  'FLOW-FULL': 300,
  'FIBRA-500M': 200,
  'GIGA-1000': 500,
};
const DEFAULT_POINTS = 100;

/**
 * Servicio de fidelización agregado después de que el sistema ya estaba en marcha.
 * No publica nada ni toca a los demás servicios: solo lee el log de activation.events
 * desde el inicio y acredita puntos por cada activación completada (escenario 5).
 * El estado vive en memoria a propósito: la fuente de verdad es el log de Kafka.
 */
@Injectable()
export class LoyaltyService {
  private readonly logger = new Logger(LoyaltyService.name);
  private readonly processedEventIds = new Set<string>();
  private readonly pointsByCustomer = new Map<string, number>();

  awardPoints(event: ActivationCompletedEvent, partition: number, offset: string) {
    // Idempotencia en memoria: un mismo evento no suma dos veces
    if (this.processedEventIds.has(event.eventId)) {
      return;
    }
    this.processedEventIds.add(event.eventId);

    const points = POINTS_PER_PLAN[event.payload?.planId] ?? DEFAULT_POINTS;
    const total = (this.pointsByCustomer.get(event.customerId) ?? 0) + points;
    this.pointsByCustomer.set(event.customerId, total);

    this.logger.log(
      `[partition ${partition} | offset ${offset}] +${points} pts to ${event.customerId} ` +
        `for ${event.correlationId} (${event.payload?.planId}). Total: ${total} pts`,
    );
  }
}
