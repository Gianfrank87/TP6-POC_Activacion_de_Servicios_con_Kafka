import { Collection } from 'mongodb';
import { Kafka } from 'kafkajs';

export async function alreadyProcessed(collection: Collection, eventId: string) {
  try {
    await collection.createIndex({ eventId: 1 }, { unique: true });
    await collection.createIndex({ expiresAt: 1 }, { expireAfterSeconds: 0 });
    await collection.insertOne({ eventId, createdAt: new Date(), expiresAt: new Date(Date.now() + 7 * 24 * 60 * 60 * 1000) });
    return false;
  } catch (e: any) {
    if (e?.code === 11000) return true;
    throw e;
  }
}

export async function withRetry<T>(fn: () => Promise<T>, attempts = 3) {
  let error: any;
  for (let i = 0; i < attempts; i++) {
    try { return await fn(); } catch (e) {
      error = e;
      if (i < attempts - 1) await new Promise(r => setTimeout(r, 1000 * 2 ** i));
    }
  }
  throw error;
}

export async function retryOrDlq(sourceTopic: string, raw: any, fn: () => Promise<any>) {
  try {
    return await withRetry(fn, 3);
  } catch (error: any) {
    const kafka = new Kafka({ clientId: 'dlq-publisher', brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(',') });
    const producer = kafka.producer();
    await producer.connect();
    let original: any = raw;
    if (Buffer.isBuffer(raw)) original = raw.toString();
    await producer.send({ topic: `${sourceTopic}.dlq`, messages: [{ value: JSON.stringify({ original, error: error?.message || String(error), failedAt: new Date().toISOString() }) }] });
    await producer.disconnect();
  }
}
