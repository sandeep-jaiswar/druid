#!/bin/bash
set -e

# Set defaults
KAFKA_CONFIG=/opt/kafka/config/server.properties
CLUSTER_ID=${KAFKA_KRAFT_CLUSTER_ID:-cluster-1}
DATA_DIR=${KAFKA_CFG_LOG_DIRS:-/var/lib/kafka/data}

echo "==== Checking if Kafka data dir exists: $DATA_DIR ===="
if [ ! -d "$DATA_DIR" ] || [ -z "$(ls -A "$DATA_DIR")" ]; then
    echo "Kafka data directory empty. Formatting KRaft storage..."
    /opt/kafka/bin/kafka-storage.sh format \
        --cluster-id "$CLUSTER_ID" \
        --ignore-formatted \
        --config "$KAFKA_CONFIG"
else
    echo "Kafka data directory already exists. Skipping format."
fi

echo "==== Starting Kafka broker ===="
exec /opt/kafka/bin/kafka-server-start.sh "$KAFKA_CONFIG"
