import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { KafkaToolkitModule, ProcessedEventEntity, OutboxEntity } from '@activation-poc/kafka-toolkit';
import { ActivationEntity } from './entities/activation.entity';
import { ActivationGateway } from './gateways/activation.gateway';
import { ActivationService } from './services/activation.service';
import { ActivationController } from './controllers/activation.controller';

@Module({
  imports: [
    TypeOrmModule.forRoot({
      type: 'postgres',
      host: process.env.DB_HOST || 'localhost',
      port: parseInt(process.env.DB_PORT || '5432', 10),
      username: process.env.DB_USER || 'postgres',
      password: process.env.DB_PASSWORD || 'postgrespassword',
      database: process.env.DB_NAME || 'activation_db',
      entities: [ActivationEntity, ProcessedEventEntity, OutboxEntity],
      synchronize: true, // auto creates / syncs tables
    }),
    TypeOrmModule.forFeature([ActivationEntity]),
    KafkaToolkitModule,
  ],
  controllers: [ActivationController],
  providers: [ActivationService, ActivationGateway],
})
export class AppModule {}
