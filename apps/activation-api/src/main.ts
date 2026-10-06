import { NestFactory } from '@nestjs/core';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { AppModule } from './app.module';
import { Logger } from '@nestjs/common';

async function bootstrap() {
  const logger = new Logger('ActivationApiMain');
  const app = await NestFactory.create(AppModule);

  app.enableCors({ origin: '*' });

  const kafkaBrokers = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
  const kafkaGroupId = process.env.KAFKA_GROUP_ID || 'activation-api-group';

  // Connect Kafka microservice for saga events
  app.connectMicroservice<MicroserviceOptions>({
    transport: Transport.KAFKA,
    options: {
      client: {
        clientId: process.env.KAFKA_CLIENT_ID || 'activation-api',
        brokers: kafkaBrokers,
      },
      consumer: {
        groupId: kafkaGroupId,
      },
    },
  });

  await app.startAllMicroservices();
  const port = process.env.PORT || 3000;
  await app.listen(port);
  logger.log(`Activation API running on http://localhost:${port} and Kafka connected`);
}
bootstrap();
