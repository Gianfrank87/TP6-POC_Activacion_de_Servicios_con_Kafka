# Guión de demo

## 1. Camino feliz
Crear una activación sin fallo. La UI debe terminar en `ActivationCompleted` y Mailhog debe mostrar el correo.

## 2. Fallo y compensación
Crear una activación con fallo en provisioning. Deben aparecer `ProvisioningFailed`, `ActivationFailed` y, si billing ya creó la cuenta, `BillingAccountCancelled`.

## 3. Servicio caído
```bash
docker compose stop notification-service
# crear activaciones
docker compose start notification-service
```
Al volver el servicio, consume los mensajes pendientes.

## 4. Escalar billing
```bash
docker compose up -d --scale billing-service=2
```
Kafka UI permite ver cómo se reparten las particiones dentro del consumer group.

## 5. Nuevo consumidor
```bash
docker compose --profile loyalty up -d loyalty-service
```
El servicio loyalty lee el historial desde el inicio con un consumer group nuevo.
