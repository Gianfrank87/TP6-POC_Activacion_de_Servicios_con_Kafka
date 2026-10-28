import 'reflect-metadata';
import { Controller, Inject, Module, OnModuleInit } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { ClientKafka, ClientsModule, EventPattern, Payload, Transport } from '@nestjs/microservices';
import { MongoClient } from 'mongodb';
import { EventEnvelope, makeEvent, TOPICS } from '@tp6/contracts';
import { alreadyProcessed, retryOrDlq } from '@tp6/kafka-toolkit';

const brokers = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
const mongo = new MongoClient(process.env.MONGO_URI || 'mongodb://localhost:27017');
const db = mongo.db('billing');

@Controller()
class BillingConsumer implements OnModuleInit {
  constructor(@Inject('KAFKA') private readonly kafka: ClientKafka) {}
  async onModuleInit() { await this.kafka.connect(); }

  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  async activate(@Payload() data: any) {
    return retryOrDlq(TOPICS.ACTIVATION_REQUESTED, data, async () => {
      const event = parseEvent(data);
      if (await alreadyProcessed(db.collection('processed_events'), event.eventId)) return;
      if (event.payload?.simulateFailure === 'billing') {
        const failed = makeEvent('BillingFailed', event.correlationId, event.customerId, 'billing-service', { reason: 'Fallo simulado en billing' });
        this.kafka.emit(TOPICS.BILLING_EVENTS, { key: event.customerId, value: JSON.stringify(failed) });
        return;
      }
      await db.collection('billing_accounts').updateOne({ activationId: event.correlationId }, { $set: { activationId: event.correlationId, customerId: event.customerId, status: 'CREATED', updatedAt: new Date() } }, { upsert: true });
      const ok = makeEvent('BillingAccountCreated', event.correlationId, event.customerId, 'billing-service', { accountId: `B-${event.correlationId}` });
      this.kafka.emit(TOPICS.BILLING_EVENTS, { key: event.customerId, value: JSON.stringify(ok) });
    });
  }

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  async compensate(@Payload() data: any) {
    return retryOrDlq(TOPICS.ACTIVATION_EVENTS, data, async () => {
      const event = parseEvent(data);
      if (event.eventType !== 'ActivationFailed') return;
      const account = await db.collection('billing_accounts').findOne({ activationId: event.correlationId });
      if (!account || account.status === 'CANCELLED') return;
      await db.collection('billing_accounts').updateOne({ activationId: event.correlationId }, { $set: { status: 'CANCELLED', updatedAt: new Date() } });
      const cancelled = makeEvent('BillingAccountCancelled', event.correlationId, event.customerId, 'billing-service', {});
      this.kafka.emit(TOPICS.BILLING_EVENTS, { key: event.customerId, value: JSON.stringify(cancelled) });
    });
  }
}

function parseEvent(data: any): EventEnvelope {
  const raw = data?.value ?? data;
  return typeof raw === 'string' ? JSON.parse(raw) : Buffer.isBuffer(raw) ? JSON.parse(raw.toString()) : raw;
}

@Module({
  imports: [ClientsModule.register([{ name: 'KAFKA', transport: Transport.KAFKA, options: { client: { clientId: 'billing-producer', brokers }, consumer: { groupId: 'billing-producer' } } }])],
  controllers: [BillingConsumer]
})
class AppModule {}

async function bootstrap() {
  await mongo.connect();
  const app = await NestFactory.createMicroservice(AppModule, { transport: Transport.KAFKA, options: { client: { clientId: 'billing-service', brokers }, consumer: { groupId: 'billing-svc' } } });
  await app.listen();
}
bootstrap();
