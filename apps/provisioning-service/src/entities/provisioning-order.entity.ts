import { Entity, PrimaryColumn, Column, CreateDateColumn, Index } from 'typeorm';

@Entity('provisioning_orders')
export class ProvisioningOrderEntity {
  @PrimaryColumn({ type: 'varchar', length: 64 })
  id: string;

  @Index({ unique: true })
  @Column({ name: 'activation_id', type: 'varchar', length: 64 })
  activationId: string;

  @Column({ name: 'customer_id', type: 'varchar', length: 64 })
  customerId: string;

  @Column({ name: 'plan_id', type: 'varchar', length: 64 })
  planId: string;

  @Column({ type: 'varchar', length: 32, default: 'COMPLETED' })
  status: 'COMPLETED' | 'FAILED';

  @Column({ type: 'text', nullable: true })
  failureReason?: string;

  @CreateDateColumn({ name: 'created_at', type: 'timestamp with time zone' })
  createdAt: Date;
}
