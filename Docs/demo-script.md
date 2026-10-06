# Guión de la Demo (5 Escenarios)

| # | Escenario | Qué se hace | Qué se ve | Concepto Demostrado |
|---|---|---|---|---|
| 1 | **Camino feliz** | Contratar un plan sin fallo | Timeline con 4 eventos en verde; email de bienvenida en Mailhog (`http://localhost:8025`). | Fan-out: un evento de negocio produce varias reacciones independientes en paralelo. |
| 2 | **Fallo y compensación** | Contratar con fallo forzado en `provisioning` | `ProvisioningFailed` -> `ActivationFailed` -> `BillingAccountCancelled`; email de error en Mailhog. | Coreografía de Saga con compensación sin llamadas directas HTTP. |
| 3 | **Servicio caído** | `docker compose stop notification-service`, crear 3 activaciones, volver a levantarlo (`docker compose start notification-service`) | Kafka UI (`http://localhost:8080`) muestra lag de 3 mensajes; al reanudar, llegan los 3 correos pendientes. | Offsets y durabilidad del log: ningún mensaje se pierde. |
| 4 | **Escalar consumidor** | `docker compose up -d --scale billing-service=2` | En Kafka UI se observa cómo las 3 particiones del topic se reparten dinámicamente entre las 2 réplicas. | Consumer Groups, particionamiento y rebalanceo. |
| 5 | **Nuevo consumidor / Replay** | Levantar nuevo consumidor leyendo con `auto.offset.reset=earliest` | Procesa todo el historial histórico desde el inicio sin alterar los demás servicios. | Inmutabilidad del log de Kafka y Replay. |
