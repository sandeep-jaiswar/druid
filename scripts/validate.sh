#!/bin/bash
# Validation script for the data ingestion pipeline

set -e

echo "=== Docker Compose Configuration Validation ==="
cd "$(dirname "$0")/.."

# Validate docker-compose.yml syntax
echo "Checking docker-compose.yml syntax..."
docker compose config > /dev/null 2>&1
if [ $? -eq 0 ]; then
    echo "✓ docker-compose.yml syntax is valid"
else
    echo "✗ docker-compose.yml has syntax errors"
    exit 1
fi

# Check for required services
echo ""
echo "=== Checking Required Services ==="
required_services=("kafka" "zookeeper" "postgres" "druid-coordinator" "druid-broker" "druid-historical" "druid-middlemanager" "druid-router" "flask")

for service in "${required_services[@]}"; do
    if docker compose config --services | grep -q "^${service}$"; then
        echo "✓ Service '${service}' is defined"
    else
        echo "✗ Service '${service}' is missing"
        exit 1
    fi
done

# Check for required volumes
echo ""
echo "=== Checking Required Volumes ==="
required_volumes=("pgdata" "kafka_data" "druid_data" "zk_data" "zk_datalog")

for volume in "${required_volumes[@]}"; do
    if docker compose config --volumes | grep -q "^${volume}$"; then
        echo "✓ Volume '${volume}' is defined"
    else
        echo "✗ Volume '${volume}' is missing"
        exit 1
    fi
done

# Check for Druid Kafka extension in all Druid services
echo ""
echo "=== Checking Druid Kafka Extension ==="
for service in druid-coordinator druid-broker druid-historical druid-middlemanager druid-router; do
    if docker compose config | grep -A 30 "${service}:" | grep -q "druid-kafka-indexing-service"; then
        echo "✓ Service '${service}' has kafka-indexing-service extension"
    else
        echo "✗ Service '${service}' is missing kafka-indexing-service extension"
        exit 1
    fi
done

# Check for required ports
echo ""
echo "=== Checking Port Mappings ==="
required_ports=(
    "5000:flask"
    "9092:kafka"
    "2181:zookeeper"
    "5432:postgres"
    "8081:druid-coordinator"
    "8082:druid-broker"
    "8083:druid-historical"
    "8091:druid-middlemanager"
    "8888:druid-router"
)

config_output=$(docker compose config)
for port_mapping in "${required_ports[@]}"; do
    port="${port_mapping%%:*}"
    service="${port_mapping##*:}"
    # Check if the port appears in the config anywhere near the service
    if echo "$config_output" | grep -q "published:.*\"${port}\""; then
        echo "✓ Port ${port} is mapped for ${service}"
    else
        echo "✗ Port ${port} is not mapped for ${service}"
        exit 1
    fi
done

echo ""
echo "=== All Validations Passed! ==="
echo ""
echo "To start the services, run:"
echo "  docker compose up -d"
echo ""
echo "To check service health:"
echo "  docker compose ps"
echo ""
echo "To access Druid Console:"
echo "  http://localhost:8888"
