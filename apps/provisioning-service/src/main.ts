import { NestFactory } from '@nestjs/core';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { AppModule } from './app.module';
import { Logger } from '@nestjs/common';

async function bootstrap() {
  const logger = new Logger('ProvisioningServiceMain');
  const kafkaBrokers = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
  const kafkaGroupId = process.env.KAFKA_GROUP_ID || 'provisioning-svc';

  const app = await NestFactory.createMicroservice<MicroserviceOptions>(AppModule, {
    transport: Transport.KAFKA,
    options: {
      client: {
        clientId: process.env.KAFKA_CLIENT_ID || 'provisioning-service',
        brokers: kafkaBrokers,
      },
      consumer: {
        groupId: kafkaGroupId,
      },
    },
  });

  await app.listen();
  logger.log(`Provisioning service is listening on Kafka group [${kafkaGroupId}]`);
}
bootstrap();
