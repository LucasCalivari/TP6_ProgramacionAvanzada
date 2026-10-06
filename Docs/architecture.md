# Arquitectura del Sistema: POC Activación de Servicios con Kafka y PostgreSQL

## 1. Resumen
Esta POC demuestra una arquitectura orientada a eventos (EDA) usando Apache Kafka para la coreografía de un proceso de activación de servicios de telecomunicaciones/conectividad, sustituyendo MongoDB por **PostgreSQL** como motor de persistencia relacional con soporte transaccional ACID nativo y desacoplamiento estricto por servicio (Database-per-service).

## 2. Componentes

- **activation-api**: API REST (`POST /activations`, `GET /activations/:id`), WebSocket Gateway (Socket.io) para updates en vivo a la UI y agregador de saga. Base de datos: `activation_db`.
- **billing-service**: Consumidor de `activation.requested`, genera facturación o simula fallos. Consume `activation.events` para compensación (`BillingAccountCancelled`). Base de datos: `billing_db`.
- **provisioning-service**: Consumidor de `activation.requested`, simula aprovisionamiento de red (delay de 1-3 segundos o fallo a pedido). Base de datos: `provisioning_db`.
- **notification-service**: Consumidor de `activation.events`, envía correos simulados a Mailhog. Base de datos: `notification_db`.
- **crm-analytics-service**: Consumidor pasivo de todos los topics para auditoría y log replay. Base de datos: `crm_analytics_db`.
- **demo-ui**: Interfaz gráfica en React + Vite + Tailwind con formulario de contratación, selector de fallos simulados y visualización de la línea de tiempo en tiempo real vía WebSockets.

## 3. Sustitución de MongoDB por PostgreSQL
- Cada microservicio posee su propia base de datos aislada en la instancia de PostgreSQL (`activation_db`, `billing_db`, `provisioning_db`, `notification_db`, `crm_analytics_db`).
- Tablas comunes de infraestructura por servicio:
  - `processed_events`: Garantiza idempotencia (`event_id` único).
  - `outbox`: Permite Transactional Outbox pattern garantizando consistencia eventual atómica con la entidad de negocio.

## 4. Topics de Kafka (3 particiones, retención 7 días, clave customerId)
- `activation.requested` y DLQ: `activation.requested.dlq`
- `billing.events` y DLQ: `billing.events.dlq`
- `provisioning.events` y DLQ: `provisioning.events.dlq`
- `activation.events` y DLQ: `activation.events.dlq`
