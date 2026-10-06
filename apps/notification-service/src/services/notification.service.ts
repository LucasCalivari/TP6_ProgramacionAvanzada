import { Injectable, Logger } from '@nestjs/common';
import { DataSource } from 'typeorm';
import { v4 as uuidv4 } from 'uuid';
import * as nodemailer from 'nodemailer';
import { NotificationEntity } from '../entities/notification.entity';
import {
  EventEnvelope,
  ActivationCompletedEvent,
  ActivationFailedEvent,
} from '@activation-poc/contracts';
import { IdempotencyService } from '@activation-poc/kafka-toolkit';

@Injectable()
export class NotificationService {
  private readonly logger = new Logger(NotificationService.name);
  private readonly consumerGroup = process.env.KAFKA_GROUP_ID || 'notification-svc';
  private transporter: nodemailer.Transporter;

  constructor(
    private readonly dataSource: DataSource,
    private readonly idempotencyService: IdempotencyService,
  ) {
    const smtpHost = process.env.SMTP_HOST || 'localhost';
    const smtpPort = parseInt(process.env.SMTP_PORT || '1025', 10);

    this.transporter = nodemailer.createTransport({
      host: smtpHost,
      port: smtpPort,
      ignoreTLS: true,
    });
  }

  async handleActivationCompleted(event: ActivationCompletedEvent) {
    const { correlationId, customerId, payload } = event;

    const subject = `¡Tu plan ${payload.planId} fue activado con éxito!`;
    const body = `Hola ${customerId},\n\nTu servicio con el plan ${payload.planId} está activo.\n` +
      `ID de Activación: ${correlationId}\n` +
      `ID de Facturación: ${payload.billingAccountId || 'N/A'}\n` +
      `ID de Aprovisionamiento: ${payload.provisioningId || 'N/A'}\n\n` +
      `¡Gracias por confiar en nosotros!`;

    await this.notify(event, subject, body);
  }

  async handleActivationFailed(event: ActivationFailedEvent) {
    const { correlationId, customerId, payload } = event;

    const subject = `Aviso importante: No se pudo activar tu servicio (${correlationId})`;
    const body = `Hola ${customerId},\n\nLamentamos informarte que la activación de tu plan falló.\n` +
      `Motivo: ${payload.reason}\n` +
      `Paso fallido: ${payload.failedStep || 'desconocido'}\n\n` +
      `Cualquier cobro simulado ha sido anulado automáticamente (Compensación de Saga).`;

    await this.notify(event, subject, body);
  }

  /**
   * Registra el evento, envía el email y guarda la notificación en una sola transacción
   * (RNF-03). Si el SMTP falla se lanza el error: la transacción se deshace y el
   * reintento / DLQ de DlqService se encarga del resto.
   */
  private async notify(event: EventEnvelope, subject: string, body: string) {
    const { eventId, eventType, correlationId, customerId } = event;
    const recipient = `${customerId.toLowerCase()}@telecom-demo.com`;

    await this.dataSource.transaction(async (manager) => {
      const isNew = await this.idempotencyService.checkAndRecord(eventId, eventType, this.consumerGroup, manager);
      if (!isNew) {
        return;
      }

      await this.transporter.sendMail({
        from: '"Telecom Activation Demo" <no-reply@telecom-demo.com>',
        to: recipient,
        subject,
        text: body,
      });
      this.logger.log(`Email sent successfully to ${recipient} for activation [${correlationId}]`);

      const repo = manager.getRepository(NotificationEntity);
      await repo.save(
        repo.create({
          id: `notif-${uuidv4().substring(0, 8)}`,
          activationId: correlationId,
          eventType,
          recipient,
          subject,
          body,
          status: 'SENT',
        }),
      );
    });
  }
}
