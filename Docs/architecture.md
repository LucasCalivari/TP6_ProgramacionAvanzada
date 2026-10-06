# Arquitectura del Sistema: POC Activación de Servicios con Kafka y PostgreSQL

## 1. Resumen
Esta POC demuestra una arquitectura orientada a eventos (EDA) usando Apache Kafka para la coreografía de un proceso de activación de servicios de telecomunicaciones/conectividad, sustituyendo MongoDB por **PostgreSQL** como motor de persistencia relacional con soporte transaccional ACID nativo y desacoplamiento estricto por servicio (Database-per-service).

## 2. Componentes

- **activation-api**: API REST (`POST /activations`, `GET /activations/:id`), WebSocket Gateway (Socket.io) para updates en vivo a la UI y agregador de saga. Base de datos: `activation_db`.
- **billing-service**: Consumidor de `activation.requested`, genera facturación o simula fallos. Consume `activation.events` para compensación (`BillingAccountCancelled`). Base de datos: `billing_db`.
- **provisioning-service**: Consumidor de `activation.requested`, simula aprovisionamiento de red (delay de 1-3 segundos o fallo a pedido). Base de datos: `provisioning_db`.
- **notification-service**: Consumidor de `activation.events`, envía correos simulados a Mailhog. Base de datos: `notification_db`.
- **crm-analytics-service**: Consumidor pasivo de todos los topics para auditoría y log replay. Base de datos: `crm_analytics_db`.
- **loyalty-service**: Consumidor agregado a posteriori (profile `loyalty`) que lee `activation.events` desde el inicio y acredita puntos por activación completada. Demuestra el replay del log (escenario 5). Sin base de datos.
- **demo-ui**: Interfaz gráfica en React + Vite + Tailwind con formulario de contratación, selector de fallos simulados y visualización de la línea de tiempo en tiempo real vía WebSockets.

## 3. Sustitución de MongoDB por PostgreSQL
- Cada microservicio posee su propia base de datos aislada en la instancia de PostgreSQL (`activation_db`, `billing_db`, `provisioning_db`, `notification_db`, `crm_analytics_db`).
- Tablas comunes de infraestructura por servicio:
  - `processed_events`: Garantiza idempotencia (`event_id` único, purga a los 7 días).
  - `outbox`: Permite Transactional Outbox pattern garantizando consistencia eventual atómica con la entidad de negocio.

## 4. Topics de Kafka (3 particiones, retención 7 días, clave customerId)
- `activation.requested` y DLQ: `activation.requested.dlq`
- `billing.events` y DLQ: `billing.events.dlq`
- `provisioning.events` y DLQ: `provisioning.events.dlq`
- `activation.events` y DLQ: `activation.events.dlq`

## 5. Confiabilidad (libs/kafka-toolkit)
- **Idempotencia (RNF-03)**: cada consumidor registra el `eventId` en `processed_events` (`INSERT ... ON CONFLICT DO NOTHING`) en la misma transacción que aplica el efecto y encola los eventos de salida. Un duplicado se descarta sin efectos.
- **Reintentos y DLQ (RNF-04)**: todos los handlers pasan por `DlqService.consume()`: valida el sobre, reintenta 3 veces (1 s, 2 s, 4 s) y si sigue fallando publica el mensaje original en `<topic>.dlq` con el motivo en los headers, sin bloquear la partición.
- **Outbox**: los eventos se guardan en `outbox` dentro de la transacción de negocio y un poller los publica cada 500 ms.
- **Sin pérdida (RNF-05)**: offsets commiteados después de procesar; los consumer groups nuevos leen desde el inicio (`fromBeginning`).
