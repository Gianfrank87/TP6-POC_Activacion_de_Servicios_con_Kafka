import 'reflect-metadata';
import { Controller, Module } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { EventPattern, Payload, Transport } from '@nestjs/microservices';
import { MongoClient } from 'mongodb';
import nodemailer from 'nodemailer';
import { EventEnvelope, TOPICS } from '@tp6/contracts';
import { alreadyProcessed, retryOrDlq } from '@tp6/kafka-toolkit';

const brokers = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
const mongo = new MongoClient(process.env.MONGO_URI || 'mongodb://localhost:27017');
const db = mongo.db('notification');
const mailer = nodemailer.createTransport({ host: process.env.SMTP_HOST || 'localhost', port: Number(process.env.SMTP_PORT || 1025), secure: false });

@Controller()
class Consumer {
  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  async notify(@Payload() data: any) {
    return retryOrDlq(TOPICS.ACTIVATION_EVENTS, data, async () => {
      const event = parseEvent(data);
      if (!['ActivationCompleted', 'ActivationFailed'].includes(event.eventType)) return;
      if (await alreadyProcessed(db.collection('processed_events'), event.eventId)) return;
      const ok = event.eventType === 'ActivationCompleted';
      const subject = ok ? 'Activación completada' : 'No se pudo completar la activación';
      await mailer.sendMail({ from: 'demo@tp6.local', to: `${event.customerId}@example.com`, subject, text: `${subject}. Activación: ${event.correlationId}` });
      await db.collection('notifications').insertOne({ activationId: event.correlationId, eventType: event.eventType, customerId: event.customerId, sentAt: new Date() });
    });
  }
}

function parseEvent(data: any): EventEnvelope {
  const raw = data?.value ?? data;
  return typeof raw === 'string' ? JSON.parse(raw) : Buffer.isBuffer(raw) ? JSON.parse(raw.toString()) : raw;
}

@Module({ controllers: [Consumer] })
class AppModule {}

async function bootstrap() {
  await mongo.connect();
  const app = await NestFactory.createMicroservice(AppModule, { transport: Transport.KAFKA, options: { client: { clientId: 'notification-service', brokers }, consumer: { groupId: 'notification-svc' } } });
  await app.listen();
}
bootstrap();
