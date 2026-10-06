import { Entity, PrimaryColumn, Column, CreateDateColumn, UpdateDateColumn } from 'typeorm';

export interface ActivationStepInfo {
  status: 'OK' | 'FAILED';
  at: string;
  details?: any;
}

export interface ActivationHistoryItem {
  eventType: string;
  at: string;
  details?: any;
}

@Entity('activations')
export class ActivationEntity {
  @PrimaryColumn({ type: 'varchar', length: 64 })
  id: string;

  @Column({ name: 'customer_id', type: 'varchar', length: 64 })
  customerId: string;

  @Column({ name: 'plan_id', type: 'varchar', length: 64 })
  planId: string;

  @Column({ type: 'varchar', length: 32, default: 'PENDING' })
  status: 'PENDING' | 'IN_PROGRESS' | 'ACTIVE' | 'FAILED';

  @Column({ name: 'simulate_failure', type: 'varchar', length: 32, default: 'none' })
  simulateFailure: string;

  @Column({ type: 'jsonb', default: () => "'{}'" })
  steps: {
    billing?: ActivationStepInfo;
    provisioning?: ActivationStepInfo;
  };

  @Column({ type: 'jsonb', default: () => "'[]'" })
  history: ActivationHistoryItem[];

  @CreateDateColumn({ name: 'created_at', type: 'timestamp with time zone' })
  createdAt: Date;

  @UpdateDateColumn({ name: 'updated_at', type: 'timestamp with time zone' })
  updatedAt: Date;
}
