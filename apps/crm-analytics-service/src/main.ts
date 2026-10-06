import { NestFactory } from '@nestjs/core';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { AppModule } from './app.module';
import { Logger } from '@nestjs/common';

async function bootstrap() {
  const logger = new Logger('CrmAnalyticsServiceMain');
  const kafkaBrokers = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
  const kafkaGroupId = process.env.KAFKA_GROUP_ID || 'crm-analytics';

  const app = await NestFactory.createMicroservice<MicroserviceOptions>(AppModule, {
    transport: Transport.KAFKA,
    options: {
      client: {
        clientId: process.env.KAFKA_CLIENT_ID || 'crm-analytics-service',
        brokers: kafkaBrokers,
      },
      consumer: {
        groupId: kafkaGroupId,
      },
      // Sin offset commiteado, leer desde el inicio para no perder eventos publicados antes de que el consumer se una
      subscribe: {
        fromBeginning: true,
      },
    },
  });

  await app.listen();
  logger.log(`CRM Analytics service is listening on Kafka group [${kafkaGroupId}] across all topics`);
}
bootstrap();
