import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { KafkaToolkitModule, ProcessedEventEntity, OutboxEntity } from '@activation-poc/kafka-toolkit';
import { NotificationEntity } from './entities/notification.entity';
import { NotificationService } from './services/notification.service';
import { NotificationController } from './controllers/notification.controller';

@Module({
  imports: [
    TypeOrmModule.forRoot({
      type: 'postgres',
      host: process.env.DB_HOST || 'localhost',
      port: parseInt(process.env.DB_PORT || '5432', 10),
      username: process.env.DB_USER || 'postgres',
      password: process.env.DB_PASSWORD || 'postgrespassword',
      database: process.env.DB_NAME || 'notification_db',
      entities: [NotificationEntity, ProcessedEventEntity, OutboxEntity],
      synchronize: true,
    }),
    TypeOrmModule.forFeature([NotificationEntity]),
    KafkaToolkitModule,
  ],
  controllers: [NotificationController],
  providers: [NotificationService],
})
export class AppModule {}
