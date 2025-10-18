# Druid Pipeline Usage Guide

## Overview
This guide shows how to use the SOLID-architected Druid Pipeline Application to ingest stock data from Kafka into Apache Druid.

## Prerequisites
1. Docker and Docker Compose installed
2. Kafka and Druid services running (from existing docker-compose.yml)
3. Flask app sending stock data to Kafka topic 'stock-data'

## Quick Start

### 1. Start Infrastructure
```bash
# Start Kafka, Druid, and Flask services
docker-compose up -d

# Wait for services to be ready (about 2-3 minutes)
docker-compose logs -f coordinator overlord
```

### 2. Build and Run Pipeline
```bash
# Build the pipeline application
docker-compose -f docker-compose.pipeline.yml build

# Run the full pipeline
docker-compose -f docker-compose.pipeline.yml up druid-pipeline
```

### 3. Submit Ingestion Task Only
```bash
# Just submit the task without running consumer
docker run --rm --network druid_default \
  -e DRUID_OVERLORD_URL=http://overlord:8090 \
  druid-pipeline python main.py submit
```

### 4. Monitor Tasks
```bash
# List active tasks
docker run --rm --network druid_default \
  -e DRUID_OVERLORD_URL=http://overlord:8090 \
  druid-pipeline python main.py list-tasks

# Check specific task status
docker run --rm --network druid_default \
  -e DRUID_OVERLORD_URL=http://overlord:8090 \
  druid-pipeline python main.py status
```

## Advanced Usage

### Custom Configuration
Create a custom environment file:
```bash
# Create custom config
cp .env.example .env.production
# Edit .env.production with your settings

# Run with custom config
docker run --rm --network druid_default \
  --env-file .env.production \
  druid-pipeline python main.py run
```

### Extending the Application

#### Add a New Data Source
```python
# 1. Implement IDataConsumer interface
class RedisConsumerService(IDataConsumer):
    def connect(self) -> bool:
        # Connect to Redis
        pass
    
    def consume_messages(self) -> Iterator[StockRecord]:
        # Consume from Redis streams
        pass

# 2. Use with dependency injection
config = EnvironmentConfigProvider()
redis_consumer = RedisConsumerService(config)
app = DruidPipelineApplication(consumer=redis_consumer)
```

#### Add Custom Configuration Source
```python
# 1. Implement IConfigProvider interface
class DatabaseConfigProvider(IConfigProvider):
    def get_kafka_config(self) -> Dict[str, Any]:
        # Load from database
        pass
    
    def get_druid_config(self) -> Dict[str, Any]:
        # Load from database
        pass

# 2. Inject into services
db_config = DatabaseConfigProvider()
app = DruidPipelineApplication(config_provider=db_config)
```

## Testing the Pipeline

### 1. Test Individual Components
```bash
python test_pipeline.py
```

### 2. Send Test Data
```bash
# Use Flask app to send stock data
curl -X POST "http://localhost:5000/fetch?ticker=AAPL&period=1d&interval=15m"
```

### 3. Verify Ingestion
```bash
# Check Druid segments
curl -X GET "http://localhost:8888/druid/v2/datasources/stock_data/segments"

# Query ingested data
curl -X POST "http://localhost:8888/druid/v2/sql" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "SELECT * FROM stock_data ORDER BY __time DESC LIMIT 10"
  }'
```

## Architecture Benefits

### SOLID Principles in Action

1. **Single Responsibility Principle (SRP)**
   - `KafkaConsumerService`: Only handles Kafka consumption
   - `DruidIngestionService`: Only manages Druid tasks
   - `StockRecord`: Only represents stock data

2. **Open/Closed Principle (OCP)**
   - Add new consumer types without modifying existing code
   - Extend with new ingestion services easily

3. **Liskov Substitution Principle (LSP)**
   - Any `IDataConsumer` implementation is interchangeable
   - Any `IConfigProvider` implementation works seamlessly

4. **Interface Segregation Principle (ISP)**
   - Small, focused interfaces with only needed methods
   - No client depends on methods it doesn't use

5. **Dependency Inversion Principle (DIP)**
   - High-level modules depend on abstractions
   - Easy to mock and test components

### Maintainability Features
- **Separation of Concerns**: Each component has a clear purpose
- **Dependency Injection**: Easy to test and extend
- **Configuration Management**: Centralized and flexible
- **Error Handling**: Comprehensive logging and recovery
- **CLI Interface**: User-friendly command-line operations

## Troubleshooting

### Common Issues

1. **Connection Errors**
   ```bash
   # Check network connectivity
   docker network ls
   docker network inspect druid_default
   ```

2. **Task Submission Failures**
   ```bash
   # Check Druid Overlord health
   curl http://localhost:8090/status/health
   
   # Check Overlord logs
   docker-compose logs overlord
   ```

3. **Kafka Consumption Issues**
   ```bash
   # Verify Kafka topic exists
   docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list
   
   # Check messages in topic
   docker exec kafka kafka-console-consumer.sh \
     --bootstrap-server localhost:9092 \
     --topic stock-data --from-beginning --max-messages 5
   ```

4. **Data Not Appearing in Druid**
   ```bash
   # Check task status
   curl http://localhost:8090/druid/indexer/v1/tasks
   
   # Verify data format matches schema
   python demo_ingestion_spec.py
   ```

## Performance Tuning

### Memory Settings
```yaml
# In docker-compose.pipeline.yml
environment:
  - INGESTION_MAX_ROWS_MEMORY=200000  # Increase for more memory
  - INGESTION_TASK_COUNT=2           # Multiple tasks for parallel processing
```

### Segment Configuration
```yaml
environment:
  - INGESTION_SEGMENT_GRANULARITY=HOUR  # Smaller segments for faster queries
  - INGESTION_QUERY_GRANULARITY=SECOND  # Higher resolution
```

## Monitoring and Alerting

### Health Checks
```bash
# Application health
python main.py status

# Druid health
curl http://localhost:8081/status/health  # Coordinator
curl http://localhost:8090/status/health  # Overlord
```

### Metrics Collection
The application logs structured information that can be collected by log aggregation systems for monitoring and alerting.

## Production Deployment

### Recommended Settings
```yaml
# Production docker-compose override
services:
  druid-pipeline:
    deploy:
      replicas: 2
      resources:
        limits:
          memory: 1G
          cpus: '0.5'
    environment:
      - LOG_LEVEL=WARNING
      - INGESTION_TASK_COUNT=2
      - INGESTION_REPLICAS=2
    restart: always
```

This architecture provides a solid foundation for building scalable, maintainable data ingestion pipelines that can grow with your needs.