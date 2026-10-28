import 'reflect-metadata';
import { Body, Controller, Get, Inject, Module, OnModuleInit, Param, Post } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { ClientKafka, ClientsModule, EventPattern, Payload, Transport } from '@nestjs/microservices';
import { WebSocketGateway, WebSocketServer } from '@nestjs/websockets';
import { Server } from 'socket.io';
import { MongoClient } from 'mongodb';
import { randomUUID } from 'crypto';
import { EventEnvelope, FailureMode, makeEvent, TOPICS } from '@tp6/contracts';
import { alreadyProcessed, retryOrDlq } from '@tp6/kafka-toolkit';

const brokers = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
const mongo = new MongoClient(process.env.MONGO_URI || 'mongodb://localhost:27017');
const db = mongo.db('activation_api');

@WebSocketGateway({ cors: { origin: '*' } })
class EventsGateway {
  @WebSocketServer() server: Server;
  emitEvent(event: EventEnvelope) { this.server?.emit('activation-event', event); }
}

@Controller('activations')
class ActivationController {
  constructor(@Inject('KAFKA') private readonly kafka: ClientKafka, private readonly gateway: EventsGateway) {}

  @Post()
  async create(@Body() body: { customerId: string; planId: string; simulateFailure?: FailureMode }) {
    const activationId = `act-${randomUUID().slice(0, 8)}`;
    const doc = { _id: activationId, customerId: body.customerId, planId: body.planId, simulateFailure: body.simulateFailure || 'none', status: 'PENDING', steps: { billing: { status: 'PENDING' }, provisioning: { status: 'PENDING' } }, history: [], createdAt: new Date(), updatedAt: new Date() };
    await db.collection('activations').insertOne(doc as any);
    const event = makeEvent('ActivationRequested', activationId, body.customerId, 'activation-api', { planId: body.planId, channel: 'web', simulateFailure: body.simulateFailure || 'none' });
    await db.collection('activations').updateOne({ _id: activationId } as any, { $push: { history: { eventType: event.eventType, at: event.occurredAt } } as any });
    this.kafka.emit(TOPICS.ACTIVATION_REQUESTED, { key: body.customerId, value: JSON.stringify(event) });
    this.gateway.emitEvent(event);
    return { activationId, status: 'PENDING' };
  }

  @Get(':id')
  async get(@Param('id') id: string) { return db.collection('activations').findOne({ _id: id } as any); }
}

@Controller()
class ActivationEvents implements OnModuleInit {
  constructor(@Inject('KAFKA') private readonly kafka: ClientKafka, private readonly gateway: EventsGateway) {}
  async onModuleInit() { await this.kafka.connect(); }

  private async applyResult(event: EventEnvelope) {
    const activation = await db.collection('activations').findOne({ _id: event.correlationId } as any);
    if (!activation) return;
    const isBilling = event.eventType === 'BillingAccountCreated' || event.eventType === 'BillingFailed';
    const isProvisioning = event.eventType === 'ProvisioningCompleted' || event.eventType === 'ProvisioningFailed';
    if (!isBilling && !isProvisioning) return;
    const failed = event.eventType.endsWith('Failed');
    const step = isBilling ? 'billing' : 'provisioning';
    const nextStep = failed ? 'FAILED' : 'OK';
    const update: any = { $set: { [`steps.${step}.status`]: nextStep, [`steps.${step}.at`]: event.occurredAt, updatedAt: new Date() }, $push: { history: { eventType: event.eventType, at: event.occurredAt } } };
    if (activation.status === 'PENDING') update.$set.status = 'IN_PROGRESS';
    await db.collection('activations').updateOne({ _id: event.correlationId } as any, update);
    this.gateway.emitEvent(event);
    const current: any = await db.collection('activations').findOne({ _id: event.correlationId } as any);
    if (current.status === 'ACTIVE' || current.status === 'FAILED') return;
    if (failed) {
      await db.collection('activations').updateOne({ _id: event.correlationId } as any, { $set: { status: 'FAILED', updatedAt: new Date() } });
      const finalEvent = makeEvent('ActivationFailed', event.correlationId, event.customerId, 'activation-api', { reason: event.payload?.reason || event.eventType });
      await db.collection('activations').updateOne({ _id: event.correlationId } as any, { $push: { history: { eventType: finalEvent.eventType, at: finalEvent.occurredAt } } as any });
      this.kafka.emit(TOPICS.ACTIVATION_EVENTS, { key: event.customerId, value: JSON.stringify(finalEvent) });
      this.gateway.emitEvent(finalEvent);
      return;
    }
    if (current.steps?.billing?.status === 'OK' && current.steps?.provisioning?.status === 'OK') {
      await db.collection('activations').updateOne({ _id: event.correlationId } as any, { $set: { status: 'ACTIVE', updatedAt: new Date() } });
      const finalEvent = makeEvent('ActivationCompleted', event.correlationId, event.customerId, 'activation-api', { planId: current.planId });
      await db.collection('activations').updateOne({ _id: event.correlationId } as any, { $push: { history: { eventType: finalEvent.eventType, at: finalEvent.occurredAt } } as any });
      this.kafka.emit(TOPICS.ACTIVATION_EVENTS, { key: event.customerId, value: JSON.stringify(finalEvent) });
      this.gateway.emitEvent(finalEvent);
    }
  }

  @EventPattern(TOPICS.BILLING_EVENTS)
  async billing(@Payload() data: any) {
    return retryOrDlq(TOPICS.BILLING_EVENTS, data, async () => {
      const event = parseEvent(data);
      if (await alreadyProcessed(db.collection('processed_events'), event.eventId)) return;
      await this.applyResult(event);
    });
  }

  @EventPattern(TOPICS.PROVISIONING_EVENTS)
  async provisioning(@Payload() data: any) {
    return retryOrDlq(TOPICS.PROVISIONING_EVENTS, data, async () => {
      const event = parseEvent(data);
      if (await alreadyProcessed(db.collection('processed_events'), event.eventId)) return;
      await this.applyResult(event);
    });
  }
}

function parseEvent(data: any): EventEnvelope {
  const raw = data?.value ?? data;
  return typeof raw === 'string' ? JSON.parse(raw) : Buffer.isBuffer(raw) ? JSON.parse(raw.toString()) : raw;
}

@Module({
  imports: [ClientsModule.register([{ name: 'KAFKA', transport: Transport.KAFKA, options: { client: { clientId: 'activation-api-producer', brokers }, consumer: { groupId: 'activation-api-producer' } } }])],
  controllers: [ActivationController, ActivationEvents],
  providers: [EventsGateway]
})
class AppModule {}

async function bootstrap() {
  await mongo.connect();
  const app = await NestFactory.create(AppModule);
  app.enableCors();
  app.connectMicroservice({ transport: Transport.KAFKA, options: { client: { clientId: 'activation-api-consumer', brokers }, consumer: { groupId: 'activation-api' } } });
  await app.startAllMicroservices();
  await app.listen(3000);
}
bootstrap();
