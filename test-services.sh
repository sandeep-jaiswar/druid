#!/bin/bash

echo "=== Testing Docker Services Step by Step ==="

# Function to wait for service health
wait_for_service() {
    local service_name=$1
    local max_attempts=30
    local attempt=1
    
    echo "Waiting for $service_name to be healthy..."
    while [ $attempt -le $max_attempts ]; do
        if docker-compose ps $service_name | grep -q "healthy\|Up"; then
            echo "$service_name is ready!"
            return 0
        fi
        echo "Attempt $attempt/$max_attempts - $service_name not ready yet..."
        sleep 10
        attempt=$((attempt + 1))
    done
    echo "ERROR: $service_name failed to start within expected time"
    return 1
}

# Step 1: Start PostgreSQL
echo "Step 1: Starting PostgreSQL..."
docker-compose up -d postgres
wait_for_service postgres

# Step 2: Start Kafka
echo "Step 2: Starting Kafka..."
docker-compose up -d kafka
sleep 20  # Kafka needs time to initialize
wait_for_service kafka

# Step 3: Test Kafka
echo "Step 3: Testing Kafka..."
docker-compose exec kafka kafka-topics.sh --create --topic stock-data --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker-compose exec kafka kafka-topics.sh --list --bootstrap-server localhost:9092

# Step 4: Start Druid Coordinator
echo "Step 4: Starting Druid Coordinator..."
docker-compose up -d coordinator
wait_for_service coordinator

# Step 5: Start Druid services one by one
echo "Step 5: Starting Druid Overlord..."
docker-compose up -d overlord
wait_for_service overlord

echo "Starting Druid Broker..."
docker-compose up -d broker
wait_for_service broker

echo "Starting Druid Router..."
docker-compose up -d router
wait_for_service router

echo "Starting Druid Historical..."
docker-compose up -d historical
wait_for_service historical

echo "Starting Druid MiddleManager..."
docker-compose up -d middlemanager
wait_for_service middlemanager

# Step 6: Start Stock API
echo "Step 6: Starting Stock API..."
docker-compose up -d stock-api
wait_for_service stock-api

echo "=== All services started successfully! ==="
echo "Services available at:"
echo "- Stock API: http://localhost:5000"
echo "- Druid Console: http://localhost:8888"
echo "- Druid Coordinator: http://localhost:8081"
echo "- Druid Broker: http://localhost:8082"
