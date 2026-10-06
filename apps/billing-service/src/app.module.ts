import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { KafkaToolkitModule, ProcessedEventEntity, OutboxEntity } from '@activation-poc/kafka-toolkit';
import { BillingAccountEntity } from './entities/billing-account.entity';
import { BillingService } from './services/billing.service';
import { BillingController } from './controllers/billing.controller';

@Module({
  imports: [
    TypeOrmModule.forRoot({
      type: 'postgres',
      host: process.env.DB_HOST || 'localhost',
      port: parseInt(process.env.DB_PORT || '5432', 10),
      username: process.env.DB_USER || 'postgres',
      password: process.env.DB_PASSWORD || 'postgrespassword',
      database: process.env.DB_NAME || 'billing_db',
      entities: [BillingAccountEntity, ProcessedEventEntity, OutboxEntity],
      synchronize: true,
    }),
    TypeOrmModule.forFeature([BillingAccountEntity]),
    KafkaToolkitModule,
  ],
  controllers: [BillingController],
  providers: [BillingService],
})
export class AppModule {}
