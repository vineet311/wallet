#!/bin/bash
# Creates every Kafka topic the system uses. Safe to run again (--if-not-exists).
# Usage: BOOTSTRAP=kafka:9092 ./create-topics.sh
set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-localhost:9092}"
KAFKA_TOPICS="${KAFKA_TOPICS:-/opt/kafka/bin/kafka-topics.sh}"
REPLICATION="${REPLICATION:-1}"   # use 3 on a real multi-broker cluster

# name:partitions
TOPICS=(
  "transfers.completed:16"
  "wallet.deposited:6"
  "fraud.alerts:3"
  "transfers.completed.dlq:1"
  "wallet.deposited.dlq:1"
  "fraud.alerts.dlq:1"
)

for entry in "${TOPICS[@]}"; do
  name="${entry%%:*}"
  partitions="${entry##*:}"
  "$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$name" --partitions "$partitions" --replication-factor "$REPLICATION"
  echo "topic ready: $name ($partitions partitions)"
done