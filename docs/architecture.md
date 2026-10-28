# Arquitectura

La POC usa Kafka como canal de integración entre servicios. `activation-api` recibe la contratación por REST, guarda la activación y publica `ActivationRequested`. Billing y provisioning consumen ese evento en paralelo y publican sus resultados. `activation-api` agrega esos resultados y publica `ActivationCompleted` o `ActivationFailed`.

Topics principales:

- `activation.requested`
- `billing.events`
- `provisioning.events`
- `activation.events`

Cada evento lleva `eventId`, `eventType`, `version`, `occurredAt`, `correlationId`, `customerId`, `source` y `payload`. La clave de Kafka es `customerId` para conservar el orden de los eventos del mismo cliente.

MongoDB usa una base distinta por servicio. Kafka UI permite inspeccionar topics, particiones y consumer groups. Mailhog muestra los emails enviados por notification-service.

Los consumidores usan `eventId` para idempotencia. Los errores técnicos se reintentan tres veces con espera creciente y, si continúan, el mensaje se envía al topic `.dlq` correspondiente. Los fallos simulados de negocio se publican como eventos de fallo y no van a DLQ.
