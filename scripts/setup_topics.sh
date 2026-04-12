#!/bin/bash

echo "Creating Kafka topics..."

# Use the correct container name
docker exec fraud-detection-system-kafka-1 /opt/kafka/bin/kafka-topics.sh \
    --create \
    --topic transactions \
    --bootstrap-server localhost:9092 \
    --partitions 5 \
    --replication-factor 1 \
    --config retention.ms=604800000 \
    --config compression.type=snappy || true

docker exec fraud-detection-system-kafka-1 /opt/kafka/bin/kafka-topics.sh \
    --create \
    --topic user-profiles \
    --bootstrap-server localhost:9092 \
    --partitions 3 \
    --replication-factor 1 \
    --config cleanup.policy=compact \
    --config delete.retention.ms=86400000 || true

docker exec fraud-detection-system-kafka-1 /opt/kafka/bin/kafka-topics.sh \
    --create \
    --topic enriched-transactions \
    --bootstrap-server localhost:9092 \
    --partitions 5 \
    --replication-factor 1 || true

docker exec fraud-detection-system-kafka-1 /opt/kafka/bin/kafka-topics.sh \
    --create \
    --topic fraud-alerts \
    --bootstrap-server localhost:9092 \
    --partitions 3 \
    --replication-factor 1 || true

docker exec fraud-detection-system-kafka-1 /opt/kafka/bin/kafka-topics.sh \
    --create \
    --topic audit-log \
    --bootstrap-server localhost:9092 \
    --partitions 3 \
    --replication-factor 1 \
    --config retention.ms=2592000000 || true

echo ""
echo "Listing all topics:"
docker exec fraud-detection-system-kafka-1 /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092