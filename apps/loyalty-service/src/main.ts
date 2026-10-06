import { NestFactory } from '@nestjs/core';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { AppModule } from './app.module';
import { Logger } from '@nestjs/common';

async function bootstrap() {
  const logger = new Logger('LoyaltyServiceMain');
  const kafkaBrokers = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
  const kafkaGroupId = process.env.KAFKA_GROUP_ID || 'loyalty-svc';

  const app = await NestFactory.createMicroservice<MicroserviceOptions>(AppModule, {
    transport: Transport.KAFKA,
    options: {
      client: {
        clientId: process.env.KAFKA_CLIENT_ID || 'loyalty-service',
        brokers: kafkaBrokers,
      },
      consumer: {
        groupId: kafkaGroupId,
      },
      // Escenario 5: un consumer group nuevo arranca desde el offset más viejo
      // (auto.offset.reset=earliest) y reprocesa todo el historial del topic
      subscribe: {
        fromBeginning: true,
      },
    },
  });

  await app.listen();
  logger.log(`Loyalty service is listening on Kafka group [${kafkaGroupId}] from the beginning of the log`);
}
bootstrap();
