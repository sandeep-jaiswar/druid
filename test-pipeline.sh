#!/bin/bash

echo "=== Testing Complete Data Pipeline ==="

# Function to make HTTP request and check response
test_api() {
    local url=$1
    local expected_status=${2:-200}
    
    echo "Testing: $url"
    response=$(curl -s -w "\n%{http_code}" "$url")
    status_code=$(echo "$response" | tail -n1)
    body=$(echo "$response" | head -n -1)
    
    if [ "$status_code" -eq "$expected_status" ]; then
        echo "✅ SUCCESS: $url returned $status_code"
        echo "Response: $body" | jq '.' 2>/dev/null || echo "Response: $body"
    else
        echo "❌ FAILED: $url returned $status_code (expected $expected_status)"
        echo "Response: $body"
        return 1
    fi
    echo ""
}

# Step 1: Test individual services
echo "Step 1: Testing individual services..."

test_api "http://localhost:5000/health"
test_api "http://localhost:8081/status/health"
test_api "http://localhost:8082/status/health"
test_api "http://localhost:8888/status/health"

# Step 2: Test stock data fetching
echo "Step 2: Testing stock data fetching..."
test_api "http://localhost:5000/stock/AAPL"

# Step 3: Verify Kafka has received data
echo "Step 3: Checking Kafka for stock data..."
docker-compose exec kafka kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic stock-data \
    --from-beginning \
    --max-messages 5 \
    --timeout-ms 10000

# Step 4: Create Druid ingestion spec
echo "Step 4: Creating Druid ingestion spec..."
cat > /tmp/stock-ingestion-spec.json << 'EOF'
{
  "type": "kafka",
  "spec": {
    "ioConfig": {
      "type": "kafka",
      "consumerProperties": {
        "bootstrap.servers": "kafka:9092"
      },
      "topic": "stock-data",
      "inputFormat": {
        "type": "json"
      },
      "useEarliestOffset": true
    },
    "tuningConfig": {
      "type": "kafka",
      "maxRowsPerSegment": 5000000
    },
    "dataSchema": {
      "dataSource": "stock_data",
      "timestampSpec": {
        "column": "timestamp",
        "format": "iso"
      },
      "dimensionsSpec": {
        "dimensions": [
          "symbol",
          "event_time"
        ]
      },
      "metricsSpec": [
        {
          "type": "doubleSum",
          "name": "open",
          "fieldName": "open"
        },
        {
          "type": "doubleSum",
          "name": "high",
          "fieldName": "high"
        },
        {
          "type": "doubleSum",
          "name": "low",
          "fieldName": "low"
        },
        {
          "type": "doubleSum",
          "name": "close",
          "fieldName": "close"
        },
        {
          "type": "longSum",
          "name": "volume",
          "fieldName": "volume"
        }
      ],
      "granularitySpec": {
        "type": "uniform",
        "segmentGranularity": "HOUR",
        "queryGranularity": "MINUTE",
        "rollup": false
      }
    }
  }
}
EOF

# Submit ingestion spec to Druid
echo "Submitting ingestion spec to Druid..."
curl -X POST \
  -H 'Content-Type: application/json' \
  -d @/tmp/stock-ingestion-spec.json \
  http://localhost:8081/druid/indexer/v1/supervisor

echo ""

# Step 5: Wait and test Druid query
echo "Step 5: Waiting for data ingestion (30 seconds)..."
sleep 30

echo "Testing Druid query..."
curl -X POST \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "SELECT symbol, COUNT(*) as record_count, AVG(close) as avg_close FROM stock_data WHERE __time >= CURRENT_TIMESTAMP - INTERVAL '\''1'\'' HOUR GROUP BY symbol",
    "context": {
      "queryId": "test-query-001"
    }
  }' \
  http://localhost:8082/druid/v2/sql | jq '.'

echo ""
echo "=== Pipeline Test Complete ==="
echo "Check Druid console at: http://localhost:8888"
