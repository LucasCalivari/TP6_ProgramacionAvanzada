import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { EventLogEntity } from './entities/event-log.entity';
import { CrmAnalyticsService } from './services/crm-analytics.service';
import { CrmAnalyticsController } from './controllers/crm-analytics.controller';

@Module({
  imports: [
    TypeOrmModule.forRoot({
      type: 'postgres',
      host: process.env.DB_HOST || 'localhost',
      port: parseInt(process.env.DB_PORT || '5432', 10),
      username: process.env.DB_USER || 'postgres',
      password: process.env.DB_PASSWORD || 'postgrespassword',
      database: process.env.DB_NAME || 'crm_analytics_db',
      entities: [EventLogEntity],
      synchronize: true,
    }),
    TypeOrmModule.forFeature([EventLogEntity]),
  ],
  controllers: [CrmAnalyticsController],
  providers: [CrmAnalyticsService],
})
export class AppModule {}
