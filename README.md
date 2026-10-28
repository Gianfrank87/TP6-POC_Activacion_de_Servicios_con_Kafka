# TP6 - POC Activación de Servicios con Kafka

POC de activación de planes basada en eventos con Kafka, NestJS, React y MongoDB.

## Componentes

- Demo UI en React + Vite
- activation-api
- billing-service
- provisioning-service
- notification-service
- crm-analytics-service
- Kafka en modo KRaft
- Kafka UI
- MongoDB
- Mailhog

## Ejecutar

Requiere Docker y Docker Compose.

```bash
docker compose up --build
```

Una vez iniciado:

- Demo UI: http://localhost:5173
- API: http://localhost:3000
- Kafka UI: http://localhost:8080
- Mailhog: http://localhost:8025

## Probar la activación

Desde la UI se puede indicar `customerId`, `planId` y un fallo opcional en billing o provisioning. La línea de tiempo se actualiza por WebSocket.

También se puede crear una activación por REST:

```bash
curl -X POST http://localhost:3000/activations \
  -H "Content-Type: application/json" \
  -d '{"customerId":"C-1234","planId":"Plan Digital","simulateFailure":"none"}'
```

Consultar estado:

```bash
curl http://localhost:3000/activations/ACTIVATION_ID
```

## Escenarios de demo

Los comandos están en `docs/demo-script.md`.
