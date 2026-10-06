import { Entity, PrimaryColumn, Column, CreateDateColumn, Index } from 'typeorm';

@Entity('notifications')
@Index(['activationId', 'eventType'], { unique: false })
export class NotificationEntity {
  @PrimaryColumn({ type: 'varchar', length: 64 })
  id: string;

  @Column({ name: 'activation_id', type: 'varchar', length: 64 })
  activationId: string;

  @Column({ name: 'event_type', type: 'varchar', length: 64 })
  eventType: string;

  @Column({ type: 'varchar', length: 128 })
  recipient: string;

  @Column({ type: 'varchar', length: 255 })
  subject: string;

  @Column({ type: 'text' })
  body: string;

  @Column({ type: 'varchar', length: 32, default: 'SENT' })
  status: 'SENT' | 'FAILED';

  @CreateDateColumn({ name: 'sent_at', type: 'timestamp with time zone' })
  sentAt: Date;
}
