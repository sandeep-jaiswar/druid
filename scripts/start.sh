#!/bin/bash
# Quick start script for the data ingestion pipeline

set -e

echo "=== Starting Data Ingestion Pipeline ==="
echo ""

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "Error: Docker is not running. Please start Docker and try again."
    exit 1
fi

# Navigate to project directory
cd "$(dirname "$0")/.."

echo "Step 1: Starting all services..."
docker compose up -d

echo ""
echo "Step 2: Waiting for services to become healthy..."
echo "This may take 1-2 minutes..."
echo ""

# Wait for services to be healthy
max_wait=120
elapsed=0
while [ $elapsed -lt $max_wait ]; do
    sleep 5
    elapsed=$((elapsed + 5))
    
    # Check if all services are running
    running_services=$(docker compose ps --services --filter "status=running" | wc -l)
    total_services=$(docker compose ps --services | wc -l)
    
    echo "[$elapsed s] $running_services/$total_services services running..."
    
    if [ $running_services -eq $total_services ]; then
        echo ""
        echo "✓ All services are running!"
        break
    fi
done

echo ""
echo "=== Service Status ==="
docker compose ps

echo ""
echo "=== Quick Access URLs ==="
echo "• Druid Console:   http://localhost:8888"
echo "• Flask API:       http://localhost:5000/health"
echo ""
echo "=== Testing the Pipeline ==="
echo ""
echo "1. Check Flask health:"
echo "   curl http://localhost:5000/health"
echo ""
echo "2. Fetch stock data:"
echo "   curl -X POST 'http://localhost:5000/fetch?ticker=AAPL&period=1d&interval=1h'"
echo ""
echo "3. Open Druid Console (http://localhost:8888) and configure Kafka ingestion:"
echo "   - Go to 'Load data' → 'Streaming' → 'Apache Kafka'"
echo "   - Use the spec in scripts/kafka-ingestion-spec.json as a template"
echo ""
echo "4. To stop all services:"
echo "   docker compose down"
echo ""
echo "=== Pipeline Started Successfully! ==="
