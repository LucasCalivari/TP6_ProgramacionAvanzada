import { Entity, PrimaryColumn, Column, CreateDateColumn, UpdateDateColumn, Index } from 'typeorm';

@Entity('billing_accounts')
export class BillingAccountEntity {
  @PrimaryColumn({ type: 'varchar', length: 64 })
  id: string;

  @Index({ unique: true })
  @Column({ name: 'activation_id', type: 'varchar', length: 64 })
  activationId: string;

  @Column({ name: 'customer_id', type: 'varchar', length: 64 })
  customerId: string;

  @Column({ name: 'plan_id', type: 'varchar', length: 64 })
  planId: string;

  @Column({ type: 'varchar', length: 32, default: 'CREATED' })
  status: 'CREATED' | 'CANCELLED';

  @Column({ type: 'text', nullable: true })
  cancellationReason?: string;

  @CreateDateColumn({ name: 'created_at', type: 'timestamp with time zone' })
  createdAt: Date;

  @UpdateDateColumn({ name: 'updated_at', type: 'timestamp with time zone' })
  updatedAt: Date;
}
