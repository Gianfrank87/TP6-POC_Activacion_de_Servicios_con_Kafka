import 'reflect-metadata';
import { Controller, Module } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { EventPattern, Payload, Transport } from '@nestjs/microservices';
import { MongoClient } from 'mongodb';
import { EventEnvelope, TOPICS } from '@tp6/contracts';

const brokers = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
const mongo = new MongoClient(process.env.MONGO_URI || 'mongodb://localhost:27017');
const db = mongo.db('crm_analytics');

@Controller()
class Consumer {
  private async save(data: any) {
    const event = parseEvent(data);
    await db.collection('event_log').updateOne({ eventId: event.eventId }, { $setOnInsert: { ...event, receivedAt: new Date() } }, { upsert: true });
  }

  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  requested(@Payload() data: any) { return this.save(data); }

  @EventPattern(TOPICS.BILLING_EVENTS)
  billing(@Payload() data: any) { return this.save(data); }

  @EventPattern(TOPICS.PROVISIONING_EVENTS)
  provisioning(@Payload() data: any) { return this.save(data); }

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  activation(@Payload() data: any) { return this.save(data); }
}

function parseEvent(data: any): EventEnvelope {
  const raw = data?.value ?? data;
  return typeof raw === 'string' ? JSON.parse(raw) : Buffer.isBuffer(raw) ? JSON.parse(raw.toString()) : raw;
}

@Module({ controllers: [Consumer] })
class AppModule {}

async function bootstrap() {
  await mongo.connect();
  await db.collection('event_log').createIndex({ eventId: 1 }, { unique: true });
  const app = await NestFactory.createMicroservice(AppModule, { transport: Transport.KAFKA, options: { client: { clientId: 'crm-analytics-service', brokers }, consumer: { groupId: 'crm-analytics' } } });
  await app.listen();
}
bootstrap();
