#!/bin/bash
# Wait for Kafka broker to be ready
echo "Waiting for Kafka broker to be ready..."
until /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092 > /dev/null 2>&1; do
  sleep 2
done
echo "Kafka is ready. Creating topics..."

TOPICS="raw_data processed_data anomalies"

for TOPIC in $TOPICS; do
  EXISTS=$(/opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092 | grep "^${TOPIC}$")
  if [ -z "$EXISTS" ]; then
    /opt/kafka/bin/kafka-topics.sh --create \
      --topic $TOPIC \
      --bootstrap-server localhost:9092 \
      --partitions 3 \
      --replication-factor 1
    echo "Created topic: $TOPIC"
  else
    echo "Topic already exists, skipping: $TOPIC"
  fi
done

echo "Done."