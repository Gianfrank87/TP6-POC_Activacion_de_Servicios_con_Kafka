import { randomUUID } from 'crypto';

export const TOPICS = {
  ACTIVATION_REQUESTED: 'activation.requested',
  BILLING_EVENTS: 'billing.events',
  PROVISIONING_EVENTS: 'provisioning.events',
  ACTIVATION_EVENTS: 'activation.events',
} as const;

export type FailureMode = 'none' | 'billing' | 'provisioning';
export type EventType =
  | 'ActivationRequested'
  | 'BillingAccountCreated'
  | 'BillingFailed'
  | 'BillingAccountCancelled'
  | 'ProvisioningCompleted'
  | 'ProvisioningFailed'
  | 'ActivationCompleted'
  | 'ActivationFailed';

export interface EventEnvelope<T = any> {
  eventId: string;
  eventType: EventType;
  version: 1;
  occurredAt: string;
  correlationId: string;
  customerId: string;
  source: string;
  payload: T;
}

export function makeEvent<T>(eventType: EventType, correlationId: string, customerId: string, source: string, payload: T): EventEnvelope<T> {
  return {
    eventId: randomUUID(), eventType, version: 1, occurredAt: new Date().toISOString(),
    correlationId, customerId, source, payload,
  };
}
