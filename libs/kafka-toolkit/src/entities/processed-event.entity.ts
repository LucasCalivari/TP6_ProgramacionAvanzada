import { Entity, PrimaryColumn, Column, CreateDateColumn } from 'typeorm';

@Entity('processed_events')
export class ProcessedEventEntity {
  @PrimaryColumn({ name: 'event_id', type: 'varchar', length: 128 })
  eventId: string;

  @Column({ name: 'event_type', type: 'varchar', length: 128 })
  eventType: string;

  @Column({ name: 'consumer_group', type: 'varchar', length: 128 })
  consumerGroup: string;

  @CreateDateColumn({ name: 'processed_at', type: 'timestamp with time zone' })
  processedAt: Date;
}
