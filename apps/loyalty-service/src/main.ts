import 'reflect-metadata';
import { Controller, Module } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { EventPattern, Payload, Transport } from '@nestjs/microservices';
import { TOPICS } from '@tp6/contracts';

const brokers = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');

@Controller()
class Consumer {
  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  handle(@Payload() data: any) {
    const raw = data?.value ?? data;
    const event = typeof raw === 'string' ? JSON.parse(raw) : Buffer.isBuffer(raw) ? JSON.parse(raw.toString()) : raw;
    console.log(`[loyalty] ${event.eventType} ${event.correlationId}`);
  }
}

@Module({ controllers: [Consumer] })
class AppModule {}

async function bootstrap() {
  const app = await NestFactory.createMicroservice(AppModule, { transport: Transport.KAFKA, options: { client: { clientId: 'loyalty-service', brokers }, consumer: { groupId: 'loyalty-demo' }, subscribe: { fromBeginning: true } } });
  await app.listen();
}
bootstrap();
