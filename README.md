# TP6 · POC Activación de Servicios con Kafka

Licenciatura en Sistemas de Información · Programación Avanzada 2026 · FCyT UADER

Prueba de concepto de una arquitectura orientada a eventos: un cliente contrata un plan desde una UI en React y **un único evento en Kafka** dispara la facturación, el aprovisionamiento, la notificación por email y el registro en CRM, sin que ningún servicio llame directamente a otro. Si un paso falla, una saga coreografiada compensa lo hecho (anula la cuenta de facturación).

Alcance implementado: **fases 1, 2 y 3** del documento de arquitectura (`Docs/TP6 POC_Activacion_de_Servicios_con_Kafka.pdf`).

## Stack

| Componente | Tecnología |
|---|---|
| Mensajería | Apache Kafka 3.7 en modo KRaft, 1 broker |
| Servicios | NestJS 10 + `@nestjs/microservices` (transporte Kafka, kafkajs) |
| Frontend | React + Vite + Tailwind + socket.io-client |
| Persistencia | PostgreSQL 16, una base por servicio (ver nota abajo) |
| Observabilidad | Kafka UI (kafbat) |
| Email simulado | Mailhog |
| Entorno | Docker Compose, monorepo con npm workspaces |

> **Nota sobre la base de datos:** el documento propone MongoDB; se usó PostgreSQL con acuerdo de la cátedra. Las colecciones del documento se mapean a tablas (`activations`, `billing_accounts`, `provisioning_orders`, `notifications`, `event_log`, `outbox`, `processed_events`) y las transacciones ACID de Postgres cubren lo que en Mongo requería un replica set.

## Cómo levantarlo

Requisito: Docker Desktop.

```bash
docker compose up -d --build
```

Un solo comando levanta todo (RNF-01). `kafka-init` crea los topics y los servicios esperan a que termine antes de arrancar.

| URL | Qué es |
|---|---|
| http://localhost:5173 | Demo UI (contratar plan + timeline en vivo) |
| http://localhost:3000/activations | API REST de activaciones |
| http://localhost:8080 | Kafka UI: topics, mensajes, particiones, consumer groups y lag |
| http://localhost:8025 | Mailhog: bandeja de entrada de los emails |

Para bajar todo: `docker compose down` (agregar `-v` para borrar también los datos de Postgres).

## Arquitectura

```
Demo UI ──POST /activations──▶ activation-api ──▶ activation.requested
   ▲                              │  ▲                 │
   └──────── WebSocket ───────────┘  │        ┌────────┴────────┐
                                     │        ▼                 ▼
                                     │     billing        provisioning
                                     │        │                 │
                                     │  billing.events   provisioning.events
                                     └────────┴────────┬────────┘
                                                       ▼
                        activation-api (agregador de la saga)
                                                       │
                                               activation.events
                                  ┌────────────────┬───┴────────────┬──────────────┐
                                  ▼                ▼                ▼              ▼
                            notification     billing (compensa)  crm-analytics   loyalty
                                                                 (todos los      (escenario 5)
                                                                  topics)
```

| Servicio | Consume | Publica | Base |
|---|---|---|---|
| `activation-api` | `billing.events`, `provisioning.events` | `activation.requested`, `activation.events` | `activation_db` |
| `billing-service` | `activation.requested`, `activation.events` | `billing.events` | `billing_db` |
| `provisioning-service` | `activation.requested` | `provisioning.events` | `provisioning_db` |
| `notification-service` | `activation.events` | — (envía email a Mailhog) | `notification_db` |
| `crm-analytics-service` | los 4 topics | — | `crm_analytics_db` |
| `loyalty-service` | `activation.events` (desde el inicio) | — | — (estado en memoria) |

Topics: `activation.requested`, `billing.events`, `provisioning.events`, `activation.events` y sus `<topic>.dlq`. Todos con 3 particiones, retención de 7 días y **`customerId` como clave**, así los eventos de un mismo cliente se procesan en orden (RNF-02).

Estados de una activación: `PENDING` → `IN_PROGRESS` (llegó el primer resultado) → `ACTIVE` (billing y provisioning OK) o `FAILED` (el primer fallo; los resultados posteriores solo se registran en el historial).

## Estructura del repo

```
apps/
  activation-api/          REST + WebSocket + agregador de la saga
  billing-service/
  provisioning-service/
  notification-service/
  crm-analytics-service/
  loyalty-service/         consumidor de replay (profile "loyalty")
  demo-ui/                 React + Vite + socket.io-client
libs/
  contracts/               sobre común de eventos, tipos versionados, nombres de topics
  kafka-toolkit/           idempotencia, reintentos + DLQ, outbox
docker/
  create-topics.sh, init-db.sql
Docs/
  architecture.md, demo-script.md, enunciado (PDF)
docker-compose.yml
```

## Decisiones técnicas de confiabilidad

### Idempotencia (RNF-03)

Kafka entrega "al menos una vez", así que todo consumidor tolera duplicados:

- Cada servicio registra el `eventId` en `processed_events` con `INSERT ... ON CONFLICT DO NOTHING`, **dentro de la misma transacción** que aplica el efecto (crear la cuenta, guardar la orden, actualizar la saga, enviar el email) y que encola el evento de salida en el outbox. Si el efecto falla, el rollback deshace también el registro y el reintento lo vuelve a procesar. Si el `eventId` ya estaba, el evento se descarta.
- `crm-analytics` usa el índice único de `event_log.event_id`; `loyalty` deduplica en memoria.
- Un proceso purga cada hora los `processed_events` de más de 7 días (igual a la retención de los topics).

Implementación: [`libs/kafka-toolkit/src/services/idempotency.service.ts`](libs/kafka-toolkit/src/services/idempotency.service.ts).

### Reintentos y DLQ (RNF-04)

Todos los handlers de Kafka pasan por `DlqService.consume()`, que:

1. Valida el sobre del evento (JSON válido y campos `eventId`, `eventType`, `correlationId`, `customerId`).
2. Ejecuta el handler; si falla, reintenta 3 veces con espera creciente (1 s, 2 s, 4 s).
3. Si sigue fallando, publica el mensaje original en `<topic>.dlq` con headers `x-dlq-reason`, `x-original-topic`, `x-original-partition`, `x-original-offset`, `x-consumer-group` y `x-dlq-attempts`, y **sigue con el siguiente mensaje**: la partición no se bloquea.

Los fallos de negocio simulados (`simulateFailure`) no son errores técnicos: se publican como `BillingFailed` / `ProvisioningFailed` y no van a la DLQ.

Implementación: [`libs/kafka-toolkit/src/services/dlq.service.ts`](libs/kafka-toolkit/src/services/dlq.service.ts).

### Outbox transaccional

Los eventos no se publican directo: se guardan en la tabla `outbox` en la misma transacción que el cambio de estado, y un poller los publica cada 500 ms. Si un servicio se cae entre guardar y publicar, el evento no se pierde.

### Sin pérdida de eventos (RNF-05)

- Los consumer groups commitean el offset recién después de que el handler termina (kafkajs con autocommit sobre `eachMessage`). Un servicio que se cae retoma desde el último offset confirmado.
- `subscribe.fromBeginning: true`: un consumer group sin offset previo lee desde el inicio en vez de saltearse lo publicado antes de unirse.
- Compose espera a que `kafka-init` cree los topics y reinicia los servicios ante una falla (`restart: on-failure`).

### Caso borde de la compensación

Si `ActivationFailed` (por fallo de provisioning) llega a billing **antes** que `ActivationRequested`, billing registra la cuenta como `CANCELLED`; cuando llega el pedido, ve la cuenta ya anulada y no crea nada. El registro se guarda en la base, así que sobrevive reinicios y funciona con varias instancias de billing.

## Guion de la demo (5 escenarios)

Pantalla dividida: Demo UI (http://localhost:5173) a la izquierda y Kafka UI (http://localhost:8080) a la derecha.

| # | Escenario | Qué se hace | Qué se ve | Concepto |
|---|---|---|---|---|
| 1 | Camino feliz | Contratar un plan con "Ninguno (OK)" | Timeline con 4 eventos y estado ACTIVE; email de bienvenida en Mailhog | Fan-out |
| 2 | Fallo y compensación | Contratar con "Fallo Provisioning" | `ProvisioningFailed` → `ActivationFailed` → `BillingAccountCancelled`; email de error | Saga sin llamadas directas |
| 3 | Servicio caído | Detener notification, crear 3 activaciones, volver a levantarlo | Kafka UI muestra lag de 3 en `notification-svc-server`; al volver llegan los 3 emails | Offsets: nadie pierde mensajes |
| 4 | Escalar | Levantar una segunda instancia de billing | Kafka UI reparte las 3 particiones entre 2 consumidores | Consumer groups y particiones |
| 5 | Nuevo consumidor | Levantar `loyalty-service` | Sus logs acreditan puntos por todas las activaciones históricas | Replay del log |

Comandos:

```bash
# Escenario 3
docker compose stop notification-service
#   ...crear 3 activaciones desde la UI...
docker compose start notification-service

# Escenario 4
docker compose up -d --scale billing-service=2 billing-service
docker compose up -d --scale billing-service=1 billing-service   # volver a 1

# Escenario 5 (consumer group nuevo con auto.offset.reset=earliest)
docker compose --profile loyalty up -d loyalty-service
docker compose logs -f loyalty-service
```

> Nest agrega el sufijo `-server` al `groupId`: en Kafka UI los grupos se ven como `billing-svc-server`, `notification-svc-server`, `loyalty-svc-server`, etc.

## Cómo verificar los criterios de aceptación

### Un mensaje inválido termina en la DLQ y el flujo sigue

```bash
echo 'C-1234:esto-no-es-json' | docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic activation.requested --property parse.key=true --property key.separator=:
```

En los logs de billing/provisioning/crm-analytics se ven los 4 intentos (`Execution attempt 1..4 failed`) y luego `Sending failed event to DLQ`. El mensaje aparece en `activation.requested.dlq` (Kafka UI → Topics) con el motivo en los headers. Una activación creada inmediatamente después para el mismo cliente (misma partición) termina normalmente.

### Reenviar un evento ya procesado no duplica cuentas ni emails

Copiar desde Kafka UI un mensaje ya consumido (por ejemplo un `ActivationRequested` o un `ActivationCompleted`) y volver a producirlo en el mismo topic con la misma clave. La cantidad de filas en `billing_accounts`, `provisioning_orders`, `notifications` y los emails de Mailhog no cambia; en los logs aparece `Duplicate event detected ... Skipping`.

```bash
docker exec postgres psql -U postgres -d billing_db -c "select count(*) from billing_accounts"
```

### Rastreo por correlationId

Todos los eventos llevan `correlationId` = id de la activación (RNF-06):

```bash
curl http://localhost:3000/activations/act-XXXX
docker exec postgres psql -U postgres -d crm_analytics_db -c "select topic, event_type, received_at from event_log where correlation_id = 'act-XXXX' order by received_at"
```

### Reprocesar el historial en crm-analytics (RF-10)

```bash
docker compose stop crm-analytics-service
# esperar ~30 s a que el grupo quede vacío (estado "Empty")
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --group crm-analytics-server --describe --state
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --group crm-analytics-server --reset-offsets --to-earliest --all-topics --execute
docker compose start crm-analytics-service
```

crm-analytics vuelve a leer todos los topics desde el offset 0; como es idempotente por `eventId`, `event_log` no se duplica.

## API

| Método | Ruta | Descripción |
|---|---|---|
| `POST` | `/activations` | Body `{ customerId, planId, simulateFailure?: "none" \| "billing" \| "provisioning" }`. Responde **202** con `{ activationId, status: "PENDING" }` sin esperar a los demás servicios. |
| `GET` | `/activations/:id` | Estado actual, resultado de cada paso e historial de eventos. |
| `GET` | `/activations` | Últimas 50 activaciones. |

WebSocket (socket.io en el puerto 3000): eventos `activation:created` y `activation:update` con la activación completa.

## Contrato de eventos

Todos los eventos comparten el mismo sobre, definido en [`libs/contracts`](libs/contracts/src/index.ts) (RNF-07):

```json
{
  "eventId": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
  "eventType": "ActivationRequested",
  "version": 1,
  "occurredAt": "2026-09-27T16:40:00Z",
  "correlationId": "act-0001",
  "customerId": "C-1234",
  "source": "activation-api",
  "payload": { "planId": "FLOW-FULL", "channel": "web", "simulateFailure": "none" }
}
```

Eventos: `ActivationRequested`, `BillingAccountCreated`, `BillingFailed`, `BillingAccountCancelled`, `ProvisioningCompleted`, `ProvisioningFailed`, `ActivationCompleted`, `ActivationFailed`. Los nombres van en pasado; los cambios compatibles suman campos opcionales y los incompatibles suben `version`.

## Fuera de alcance

Fase 4 opcional: Schema Registry y trazas distribuidas (OpenTelemetry). Tampoco hay autenticación, alta disponibilidad ni pruebas de carga.
