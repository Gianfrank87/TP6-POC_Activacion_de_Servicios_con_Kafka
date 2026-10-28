#!/bin/bash
set -e
BROKER=kafka:29092
for topic in activation.requested billing.events provisioning.events activation.events; do
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BROKER" --create --if-not-exists --topic "$topic" --partitions 3 --replication-factor 1 --config retention.ms=604800000
done
for topic in activation.requested billing.events provisioning.events activation.events; do
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BROKER" --create --if-not-exists --topic "$topic.dlq" --partitions 3 --replication-factor 1 --config retention.ms=604800000
done
