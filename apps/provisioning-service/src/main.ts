import 'reflect-metadata';
import { Controller, Inject, Module, OnModuleInit } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { ClientKafka, ClientsModule, EventPattern, Payload, Transport } from '@nestjs/microservices';
import { MongoClient } from 'mongodb';
import { EventEnvelope, makeEvent, TOPICS } from '@tp6/contracts';
import { alreadyProcessed, retryOrDlq } from '@tp6/kafka-toolkit';

const brokers = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
const mongo = new MongoClient(process.env.MONGO_URI || 'mongodb://localhost:27017');
const db = mongo.db('provisioning');

@Controller()
class Consumer implements OnModuleInit {
  constructor(@Inject('KAFKA') private readonly kafka: ClientKafka) {}
  async onModuleInit() { await this.kafka.connect(); }

  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  async activate(@Payload() data: any) {
    return retryOrDlq(TOPICS.ACTIVATION_REQUESTED, data, async () => {
      const event = parseEvent(data);
      if (await alreadyProcessed(db.collection('processed_events'), event.eventId)) return;
      await new Promise(r => setTimeout(r, 1000 + Math.floor(Math.random() * 2000)));
      if (event.payload?.simulateFailure === 'provisioning') {
        const failed = makeEvent('ProvisioningFailed', event.correlationId, event.customerId, 'provisioning-service', { reason: 'Fallo simulado en provisioning' });
        this.kafka.emit(TOPICS.PROVISIONING_EVENTS, { key: event.customerId, value: JSON.stringify(failed) });
        return;
      }
      await db.collection('provisioning_orders').updateOne({ activationId: event.correlationId }, { $set: { activationId: event.correlationId, customerId: event.customerId, status: 'COMPLETED', updatedAt: new Date() } }, { upsert: true });
      const ok = makeEvent('ProvisioningCompleted', event.correlationId, event.customerId, 'provisioning-service', { orderId: `P-${event.correlationId}` });
      this.kafka.emit(TOPICS.PROVISIONING_EVENTS, { key: event.customerId, value: JSON.stringify(ok) });
    });
  }
}

function parseEvent(data: any): EventEnvelope {
  const raw = data?.value ?? data;
  return typeof raw === 'string' ? JSON.parse(raw) : Buffer.isBuffer(raw) ? JSON.parse(raw.toString()) : raw;
}

@Module({
  imports: [ClientsModule.register([{ name: 'KAFKA', transport: Transport.KAFKA, options: { client: { clientId: 'provisioning-producer', brokers }, consumer: { groupId: 'provisioning-producer' } } }])],
  controllers: [Consumer]
})
class AppModule {}

async function bootstrap() {
  await mongo.connect();
  const app = await NestFactory.createMicroservice(AppModule, { transport: Transport.KAFKA, options: { client: { clientId: 'provisioning-service', brokers }, consumer: { groupId: 'provisioning-svc' } } });
  await app.listen();
}
bootstrap();
