import { Injectable, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { v4 as uuidv4 } from 'uuid';
import * as nodemailer from 'nodemailer';
import { NotificationEntity } from '../entities/notification.entity';
import {
  ActivationCompletedEvent,
  ActivationFailedEvent,
} from '@activation-poc/contracts';
import { IdempotencyService } from '@activation-poc/kafka-toolkit';

@Injectable()
export class NotificationService {
  private readonly logger = new Logger(NotificationService.name);
  private transporter: nodemailer.Transporter;

  constructor(
    @InjectRepository(NotificationEntity)
    private readonly repo: Repository<NotificationEntity>,
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
    const { eventId, eventType, correlationId, customerId, payload } = event;

    const isNew = await this.idempotencyService.checkAndRecord(
      eventId,
      eventType,
      process.env.KAFKA_GROUP_ID || 'notification-svc',
    );
    if (!isNew) {
      this.logger.warn(`Event ${eventId} already processed by notification-service, skipping.`);
      return;
    }

    const recipient = `${customerId.toLowerCase()}@telecom-demo.com`;
    const subject = `¡Tu plan ${payload.planId} fue activado con éxito!`;
    const body = `Hola ${customerId},\n\nTu servicio con el plan ${payload.planId} está activo.\n` +
      `ID de Activación: ${correlationId}\n` +
      `ID de Facturación: ${payload.billingAccountId || 'N/A'}\n` +
      `ID de Aprovisionamiento: ${payload.provisioningId || 'N/A'}\n\n` +
      `¡Gracias por confiar en nosotros!`;

    await this.sendEmail(correlationId, eventType, recipient, subject, body);
  }

  async handleActivationFailed(event: ActivationFailedEvent) {
    const { eventId, eventType, correlationId, customerId, payload } = event;

    const isNew = await this.idempotencyService.checkAndRecord(
      eventId,
      eventType,
      process.env.KAFKA_GROUP_ID || 'notification-svc',
    );
    if (!isNew) {
      this.logger.warn(`Event ${eventId} already processed by notification-service, skipping.`);
      return;
    }

    const recipient = `${customerId.toLowerCase()}@telecom-demo.com`;
    const subject = `Aviso importante: No se pudo activar tu servicio (${correlationId})`;
    const body = `Hola ${customerId},\n\nLamentamos informarte que la activación de tu plan falló.\n` +
      `Motivo: ${payload.reason}\n` +
      `Paso fallido: ${payload.failedStep || 'desconocido'}\n\n` +
      `Cualquier cobro simulado ha sido anulado automáticamente (Compensación de Saga).`;

    await this.sendEmail(correlationId, eventType, recipient, subject, body);
  }

  private async sendEmail(
    activationId: string,
    eventType: string,
    recipient: string,
    subject: string,
    body: string,
  ) {
    let status: 'SENT' | 'FAILED' = 'SENT';

    try {
      await this.transporter.sendMail({
        from: '"Telecom Activation Demo" <no-reply@telecom-demo.com>',
        to: recipient,
        subject,
        text: body,
      });
      this.logger.log(`Email sent successfully to ${recipient} for activation [${activationId}]`);
    } catch (err: any) {
      this.logger.error(`Error sending email to ${recipient}: ${err.message}`);
      status = 'FAILED';
    }

    const notification = this.repo.create({
      id: `notif-${uuidv4().substring(0, 8)}`,
      activationId,
      eventType,
      recipient,
      subject,
      body,
      status,
    });
    await this.repo.save(notification);
  }
}
