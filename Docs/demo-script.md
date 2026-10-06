# Guión de la Demo (5 Escenarios)

Pantalla dividida: Demo UI (`http://localhost:5173`) a la izquierda, Kafka UI (`http://localhost:8080`) a la derecha. Duración estimada: 15 minutos.

| # | Escenario | Qué se hace | Qué se ve | Concepto Demostrado |
|---|---|---|---|---|
| 1 | **Camino feliz** | Contratar un plan sin fallo | Timeline con 4 eventos en verde; email de bienvenida en Mailhog (`http://localhost:8025`). | Fan-out: un evento de negocio produce varias reacciones independientes en paralelo. |
| 2 | **Fallo y compensación** | Contratar con fallo forzado en `provisioning` | `ProvisioningFailed` -> `ActivationFailed` -> `BillingAccountCancelled`; email de error en Mailhog. | Coreografía de Saga con compensación sin llamadas directas HTTP. |
| 3 | **Servicio caído** | Detener `notification-service`, crear 3 activaciones, volver a levantarlo | Kafka UI muestra lag de 3 en el grupo `notification-svc-server`; al reanudar, llegan los 3 correos pendientes. | Offsets y durabilidad del log: ningún mensaje se pierde. |
| 4 | **Escalar consumidor** | Levantar una segunda instancia de `billing-service` | En Kafka UI las 3 particiones de cada topic se reparten entre las 2 réplicas del grupo `billing-svc-server`. | Consumer Groups, particionamiento y rebalanceo. |
| 5 | **Nuevo consumidor / Replay** | Levantar `loyalty-service`, un consumer group nuevo que lee desde el inicio | Sus logs acreditan puntos por todas las activaciones históricas, sin tocar a los demás servicios. | Inmutabilidad del log de Kafka y Replay. |

## Comandos de apoyo

```bash
# Escenario 3
docker compose stop notification-service
docker compose start notification-service

# Escenario 4
docker compose up -d --scale billing-service=2 billing-service
docker compose up -d --scale billing-service=1 billing-service

# Escenario 5 (loyalty con auto.offset.reset=earliest)
docker compose --profile loyalty up -d loyalty-service
docker compose logs -f loyalty-service
```

## Extra: DLQ

```bash
echo 'C-1234:esto-no-es-json' | docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic activation.requested --property parse.key=true --property key.separator=:
```

Los consumidores reintentan 3 veces (1 s, 2 s, 4 s) y publican el mensaje en `activation.requested.dlq` con el motivo en los headers; el resto del flujo sigue normalmente.
