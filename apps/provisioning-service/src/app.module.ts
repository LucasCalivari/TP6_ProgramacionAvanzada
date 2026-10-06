import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { KafkaToolkitModule, ProcessedEventEntity, OutboxEntity } from '@activation-poc/kafka-toolkit';
import { ProvisioningOrderEntity } from './entities/provisioning-order.entity';
import { ProvisioningService } from './services/provisioning.service';
import { ProvisioningController } from './controllers/provisioning.controller';

@Module({
  imports: [
    TypeOrmModule.forRoot({
      type: 'postgres',
      host: process.env.DB_HOST || 'localhost',
      port: parseInt(process.env.DB_PORT || '5432', 10),
      username: process.env.DB_USER || 'postgres',
      password: process.env.DB_PASSWORD || 'postgrespassword',
      database: process.env.DB_NAME || 'provisioning_db',
      entities: [ProvisioningOrderEntity, ProcessedEventEntity, OutboxEntity],
      synchronize: true,
    }),
    TypeOrmModule.forFeature([ProvisioningOrderEntity]),
    KafkaToolkitModule,
  ],
  controllers: [ProvisioningController],
  providers: [ProvisioningService],
})
export class AppModule {}
