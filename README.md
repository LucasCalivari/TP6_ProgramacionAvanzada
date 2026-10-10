# TP6 · POC Activación de Servicios con Kafka

Licenciatura en Sistemas de Información · Programación Avanzada 2026 · FCyT UADER

## De qué se trata

Esta prueba de concepto muestra cómo resolver la activación de un plan con una arquitectura orientada a eventos. Un cliente contrata un plan desde una interfaz web y eso publica un único evento en Kafka. A partir de ese evento, la facturación, el aprovisionamiento, el envío de emails y el registro en el CRM reaccionan cada uno por su cuenta, sin que ningún servicio llame directamente a otro.

Si alguno de los pasos falla, el sistema lo resuelve con una saga coreografiada: la activación se marca como fallida y el servicio de facturación anula la cuenta que había creado, todo mediante eventos.

El proyecto implementa las fases 1, 2 y 3 del documento de arquitectura que está en la carpeta Docs.

## Tecnologías

La mensajería usa Apache Kafka 3.7 en modo KRaft con un solo broker. Los servicios están hechos en NestJS 10 con el transporte de Kafka de `@nestjs/microservices`, que por debajo usa kafkajs. La interfaz es una aplicación React con Vite y Tailwind, que recibe las actualizaciones en vivo por WebSocket con socket.io. Para observar lo que pasa dentro de Kafka se incluye Kafka UI, y los emails se simulan con Mailhog. Todo corre con Docker Compose y el código está organizado como monorepo con npm workspaces.

Como base de datos se usa PostgreSQL 16, con una base separada para cada servicio. El documento original proponía MongoDB, pero se usó PostgreSQL con el acuerdo de la cátedra. Las colecciones del documento pasaron a ser tablas, y las transacciones de Postgres cubren lo que en Mongo hubiera requerido configurar un replica set.

## Cómo levantarlo

Solo hace falta tener Docker Desktop instalado. Desde la carpeta del proyecto se ejecuta:

```bash
docker compose up -d --build
```

Ese único comando levanta todo. Primero arrancan Kafka y PostgreSQL, después un contenedor auxiliar llamado kafka-init crea los topics, y recién cuando termina arrancan los servicios. Conviene esperar alrededor de un minuto antes de usarlo.

La interfaz de la demo queda en http://localhost:5173, la API REST en http://localhost:3000/activations, Kafka UI en http://localhost:8080 y la bandeja de Mailhog en http://localhost:8025.

Para apagar todo se usa `docker compose down`. Si además se quiere borrar la base de datos y empezar de cero, se agrega la opción `-v`.

## Cómo funciona

El punto de entrada es activation-api. Cuando la interfaz pide una activación, este servicio la guarda con estado PENDING, responde enseguida con código 202 sin esperar a nadie, y publica el evento ActivationRequested en el topic activation.requested.

Ese evento lo reciben en paralelo billing-service y provisioning-service. Billing crea una cuenta de facturación simulada y publica BillingAccountCreated, o BillingFailed si se pidió simular un fallo. Provisioning simula el aprovisionamiento de la línea con una demora de uno a tres segundos y publica ProvisioningCompleted o ProvisioningFailed. Cada uno publica en su propio topic, billing.events y provisioning.events.

Activation-api escucha esos dos topics y actúa como agregador de la saga. Cuando llega el primer resultado, la activación pasa a IN_PROGRESS. Si los dos pasos salieron bien, pasa a ACTIVE y publica ActivationCompleted. Ante el primer fallo pasa a FAILED y publica ActivationFailed; los resultados que lleguen después solo se agregan al historial. Cada cambio se envía a la interfaz por WebSocket, y así se arma la línea de tiempo en vivo.

Los eventos finales van al topic activation.events. Ahí los escucha notification-service, que manda el email de bienvenida o de error a Mailhog, y también billing-service, que ante un ActivationFailed anula la cuenta y publica BillingAccountCancelled. Esa es la compensación de la saga.

Hay dos servicios más que solo escuchan. Crm-analytics-service lee todos los topics y guarda una copia de cada evento, lo que demuestra que se puede sumar un consumidor sin tocar a los demás. Loyalty-service acredita puntos por cada activación completada; no arranca con el resto porque está pensado para levantarse más tarde y mostrar cómo un consumidor nuevo puede reprocesar todo el historial.

Todos los topics tienen tres particiones, retención de siete días y usan el customerId como clave. Gracias a eso, los eventos de un mismo cliente siempre caen en la misma partición y se procesan en orden. Cada topic tiene además su cola de mensajes muertos, con el mismo nombre terminado en .dlq.

Todos los eventos comparten el mismo formato, definido en la librería libs/contracts. Cada uno lleva un eventId único, el tipo de evento con su número de versión, la fecha, el correlationId (que es el id de la activación y permite seguirla de punta a punta), el customerId, el servicio que lo publicó y los datos propios del evento. Los nombres de los eventos están en pasado porque describen hechos que ya ocurrieron.

## Organización del código

En la carpeta apps están los seis servicios NestJS y la interfaz React. En libs hay dos librerías compartidas: contracts, que define los eventos y los nombres de los topics, y kafka-toolkit, que concentra la lógica de idempotencia, reintentos, cola de mensajes muertos y outbox para no repetirla en cada servicio. La carpeta docker tiene el script que crea los topics y el que crea las bases de datos, y Docs contiene el enunciado, la descripción de la arquitectura y el guion de la demo.

## Decisiones para que el flujo sea confiable

Kafka garantiza que cada mensaje se entregue al menos una vez, lo que significa que un mismo evento puede llegar repetido. Por eso cada consumidor es idempotente. Al procesar un evento, el servicio registra su eventId en la tabla processed_events dentro de la misma transacción en la que aplica el efecto, ya sea crear la cuenta, guardar la orden, actualizar la saga o enviar el email. Si el eventId ya estaba registrado, el evento se descarta sin hacer nada. Y si el efecto falla, la transacción se deshace entera, así que el reintento lo procesa como si fuera la primera vez. Los registros de más de siete días se borran solos, porque pasado ese tiempo el evento ya no puede volver a llegar.

Cuando el procesamiento de un mensaje falla por un error técnico, el consumidor lo reintenta tres veces, esperando uno, dos y cuatro segundos entre intento e intento. Si después de eso sigue fallando, publica el mensaje original en la cola de mensajes muertos de ese topic, con el motivo del error en los encabezados, y sigue con el próximo mensaje. De esta forma un mensaje roto nunca bloquea la partición. Los fallos simulados desde la interfaz no cuentan como errores técnicos: son resultados de negocio y se publican como BillingFailed o ProvisioningFailed.

Los servicios tampoco publican los eventos directamente. Los guardan en una tabla outbox dentro de la misma transacción que el cambio de estado, y un proceso aparte revisa esa tabla cada medio segundo y publica lo pendiente. Así, si un servicio se cae justo después de guardar, el evento no se pierde.

Para que ningún servicio pierda mensajes si se cae, el offset se confirma recién después de procesar cada mensaje, y al volver el servicio retoma desde ahí. Además, un consumidor que se une por primera vez empieza a leer desde el principio del topic en lugar de saltearse lo que ya estaba publicado.

Hay un caso borde en la compensación. Si provisioning falla muy rápido, el ActivationFailed puede llegarle a billing antes que el pedido original. En ese caso billing deja registrada la cuenta como cancelada, y cuando llega el pedido ve que ya está anulada y no crea nada. Este registro queda en la base de datos, así que funciona aunque el servicio se reinicie o haya varias instancias de billing.

## Guion de la demo

La demo se hace con la pantalla dividida: la interfaz a la izquierda y Kafka UI a la derecha. Son cinco escenarios y en total llevan menos de quince minutos.

En el primer escenario, el camino feliz, se contrata un plan sin fallo. La línea de tiempo muestra los cuatro eventos, la activación termina ACTIVE y llega el email de bienvenida a Mailhog. Esto muestra el fan-out: un solo evento produce varias reacciones independientes.

En el segundo escenario se contrata un plan con fallo en provisioning. La línea de tiempo muestra ProvisioningFailed, ActivationFailed y BillingAccountCancelled, y llega el email de error. Es la saga funcionando sin ninguna llamada directa entre servicios.

En el tercer escenario se detiene el servicio de notificaciones, se crean tres activaciones y se lo vuelve a levantar. Mientras está caído, Kafka UI muestra que el grupo notification-svc-server tiene tres mensajes pendientes; al volver, llegan los tres emails. Esto demuestra que con los offsets nadie pierde mensajes. Los comandos son:

```bash
docker compose stop notification-service
```

```bash
docker compose start notification-service
```

En el cuarto escenario se levanta una segunda instancia de billing. Al cabo de unos treinta segundos, Kafka UI muestra que el grupo billing-svc-server tiene dos miembros y que las tres particiones se repartieron entre ellos. Se levanta con el primer comando y se vuelve a una sola instancia con el segundo:

```bash
docker compose up -d --scale billing-service=2 billing-service
```

```bash
docker compose up -d --scale billing-service=1 billing-service
```

En el quinto escenario se levanta loyalty-service, un consumidor nuevo que lee desde el inicio del topic. En sus logs se ve cómo acredita puntos por todas las activaciones que se completaron antes de que existiera, sin afectar a ningún otro servicio. Es algo que con llamadas directas entre APIs no se podría hacer. Se levanta y se miran sus logs así:

```bash
docker compose --profile loyalty up -d loyalty-service
```

```bash
docker compose logs -f loyalty-service
```

Un detalle a tener en cuenta: Nest le agrega el sufijo -server al nombre de cada consumer group, por eso en Kafka UI aparecen como billing-svc-server, notification-svc-server y así con los demás.

## Cómo verificar los criterios de aceptación

Para comprobar que un mensaje inválido termina en la cola de mensajes muertos, se puede publicar un texto que no sea JSON en el topic activation.requested con este comando:

```bash
echo 'C-1234:esto-no-es-json' | docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic activation.requested --property parse.key=true --property key.separator=:
```

En los logs de billing se ven los cuatro intentos fallidos y el envío a la cola. En Kafka UI, dentro del topic activation.requested.dlq, aparece el mensaje con el motivo del error en los encabezados. Si inmediatamente después se crea una activación para el cliente C-1234, que cae en la misma partición, termina normalmente: el mensaje roto no frenó a nadie.

Para comprobar que reenviar un evento no genera duplicados, se puede copiar desde Kafka UI un mensaje que ya fue procesado, por ejemplo un ActivationCompleted, y volver a publicarlo en el mismo topic con la misma clave usando la opción Produce Message. No llega un email nuevo, no se crea otra cuenta de facturación y el historial de la activación no cambia. En los logs del servicio aparece el aviso de que el evento estaba duplicado y se descartó.

Para seguir una activación de punta a punta alcanza con su id. La API lo devuelve con su historial completo en http://localhost:3000/activations/ seguido del id, y en la base de crm-analytics se pueden ver todos sus eventos de todos los topics con esta consulta, reemplazando act-XXXX por el id real:

```bash
docker exec postgres psql -U postgres -d crm_analytics_db -c "select topic, event_type, received_at from event_log where correlation_id = 'act-XXXX' order by received_at"
```

Crm-analytics también puede reprocesar todo el historial desde el principio. Para eso se detiene el servicio, se espera alrededor de treinta segundos a que Kafka lo dé por desconectado, se mueve el offset de su grupo al inicio y se lo vuelve a levantar. Como guarda los eventos por su eventId, el reproceso no genera registros duplicados.

```bash
docker compose stop crm-analytics-service
```

```bash
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --group crm-analytics-server --reset-offsets --to-earliest --all-topics --execute
```

```bash
docker compose start crm-analytics-service
```

## La API

La API tiene tres rutas. Con un POST a /activations se crea una activación enviando el customerId, el planId y opcionalmente el modo de fallo a simular, que puede ser none, billing o provisioning; responde con código 202, el id de la activación y el estado PENDING. Con un GET a /activations seguido de un id se obtiene el estado actual, el resultado de cada paso y el historial de eventos. Y con un GET a /activations se listan las últimas cincuenta activaciones. Además, por WebSocket en el mismo puerto se emiten los eventos activation:created y activation:update cada vez que una activación cambia.

## Qué quedó fuera

No se implementó la fase 4, que el documento marca como opcional y que incluye Schema Registry y trazas distribuidas con OpenTelemetry. Tampoco forman parte de la prueba la autenticación de usuarios, la alta disponibilidad con varios brokers ni las pruebas de carga.
