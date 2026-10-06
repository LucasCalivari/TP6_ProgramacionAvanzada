import { Entity, PrimaryColumn, Column, CreateDateColumn, Index } from 'typeorm';

@Entity('event_log')
export class EventLogEntity {
  @PrimaryColumn({ type: 'varchar', length: 64 })
  id: string;

  @Index({ unique: true })
  @Column({ name: 'event_id', type: 'varchar', length: 128 })
  eventId: string;

  @Column({ name: 'event_type', type: 'varchar', length: 64 })
  eventType: string;

  @Index()
  @Column({ name: 'correlation_id', type: 'varchar', length: 64 })
  correlationId: string;

  @Column({ name: 'customer_id', type: 'varchar', length: 64 })
  customerId: string;

  @Column({ type: 'varchar', length: 64 })
  source: string;

  @Column({ type: 'varchar', length: 64 })
  topic: string;

  @Column({ type: 'jsonb' })
  payload: any;

  @CreateDateColumn({ name: 'received_at', type: 'timestamp with time zone' })
  receivedAt: Date;
}
