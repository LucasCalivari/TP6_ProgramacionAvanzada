import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { ProcessedEventEntity } from './entities/processed-event.entity';
import { OutboxEntity } from './entities/outbox.entity';
import { IdempotencyService } from './services/idempotency.service';
import { DlqService } from './services/dlq.service';
import { OutboxService } from './services/outbox.service';

@Module({
  imports: [TypeOrmModule.forFeature([ProcessedEventEntity, OutboxEntity])],
  providers: [IdempotencyService, DlqService, OutboxService],
  exports: [IdempotencyService, DlqService, OutboxService, TypeOrmModule],
})
export class KafkaToolkitModule {}
