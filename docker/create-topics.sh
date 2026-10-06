#!/bin/bash
echo "Waiting for Kafka broker to be ready..."
cub kafka-ready -b kafka:9092 1 30 2>/dev/null || sleep 10

echo "Creating Kafka topics with 3 partitions and 7 days retention..."

TOPICS=(
  "activation.requested"
  "billing.events"
  "provisioning.events"
  "activation.events"
  "activation.requested.dlq"
  "billing.events.dlq"
  "provisioning.events.dlq"
  "activation.events.dlq"
)

for topic in "${TOPICS[@]}"; do
  /opt/kafka/bin/kafka-topics.sh --create --if-not-exists \
    --bootstrap-server kafka:9092 \
    --partitions 3 \
    --replication-factor 1 \
    --config retention.ms=604800000 \
    --topic "$topic"
done

echo "Kafka topics created successfully!"
/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
