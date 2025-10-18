# Stock Data Pipeline Application

A well-architected, enterprise-grade data pipeline system that follows SOLID principles and clean architecture to provide stock market data ingestion from external APIs through Kafka into Apache Druid for real-time analytics.

## Architecture Overview

This architecture provides a solid foundation for building scalable, maintainable data ingestion pipelines that can grow with your needs.

## 🧹 Clean Architecture

The codebase has been thoroughly cleaned and refactored to follow best practices:

- **Framework-agnostic design**: Core business logic is independent of Flask
- **Zero duplication**: Single source of truth for all components  
- **SOLID principles**: Comprehensive implementation throughout
- **Clean separation**: API layer completely separated from pipeline logic
- **Production-ready**: Proper error handling, logging, and health checks

See [CLEANUP.md](CLEANUP.md) for detailed information about the refactoring process and benefits.

### SOLID Principles Applied

- **Single Responsibility Principle (SRP)**: Each class has one reason to change
  - `StockRecord`: Only manages stock data representation
  - `KafkaConsumerService`: Only handles Kafka consumption
  - `DruidIngestionService`: Only manages Druid ingestion tasks

- **Open/Closed Principle (OCP)**: Open for extension, closed for modification
  - New consumer types can be added without changing existing code
  - New ingestion services can be implemented using the same interfaces

- **Liskov Substitution Principle (LSP)**: Derived classes are substitutable
  - Any `IDataConsumer` implementation can replace `KafkaConsumerService`
  - Any `IIngestionService` implementation can replace `DruidIngestionService`

- **Interface Segregation Principle (ISP)**: No client depends on unused methods
  - `IDataConsumer`: Only methods needed for data consumption
  - `IIngestionService`: Only methods needed for ingestion management
  - `IConfigProvider`: Only methods needed for configuration

- **Dependency Inversion Principle (DIP)**: Depend on abstractions, not concretions
  - Services depend on interfaces, not concrete implementations
  - Configuration is injected through `IConfigProvider` interface

## Folder Structure

```
src/
├── api/                    # API layer (SRP - HTTP endpoints)
│   ├── stock_api_service.py       # Stock data API endpoints
│   ├── message_producer_service.py # Message broker abstraction
│   └── flask_app_factory.py       # Flask application factory
├── models/                 # Data models and domain entities (SRP)
│   ├── stock_data.py      # Stock market data representations
│   └── ingestion_spec.py  # Druid ingestion specifications
├── interfaces/            # Abstract interfaces (ISP, DIP)
│   ├── data_consumer.py       # Data consumption contract
│   ├── ingestion_service.py   # Ingestion management contract
│   └── config_provider.py    # Configuration contract
├── services/              # Business logic implementations (SRP)
│   ├── kafka_consumer_service.py   # Kafka message consumption
│   └── druid_ingestion_service.py  # Druid task management
├── config/                # Configuration providers (DIP)
│   └── environment_config.py # Environment-based configuration
├── utils/                 # Utilities and CLI (SRP)
│   └── cli.py            # Command line interface
└── druid_pipeline_app.py  # Main pipeline orchestrator (OCP, DIP)

# Application entry points
├── api_main.py           # API server entry point
└── main.py              # Pipeline CLI entry point
```

## Usage

### API Server
```bash
# Start the stock data API server
python api_main.py

# Or using Docker
docker-compose up stock-api

# Test the API
curl -X POST "http://localhost:5000/fetch?ticker=AAPL&period=1d&interval=15m"
curl http://localhost:5000/health
```

### Pipeline CLI
```bash
# Run the full pipeline
python main.py run

# Monitor existing tasks only  
python main.py run --monitor-only

# Submit ingestion task only
python main.py submit

# Check application status
python main.py status

# List active Druid tasks
python main.py list-tasks

# Cancel a specific task
python main.py cancel --task-id TASK_ID
```

### Environment Configuration

Copy `.env.example` to `.env` and adjust settings:

```bash
cp .env.example .env
```

### Docker Integration

The application is designed to run in Docker containers alongside Kafka and Druid:

```bash
# Build and run with existing docker-compose
docker-compose up -d

# Add druid-pipeline service to docker-compose.yml
```

## Key Components

### Data Models
- **StockRecord**: Represents individual stock market data points
- **DruidIngestionSpec**: Complete Druid ingestion configuration
- **KafkaIOConfig**: Kafka-specific ingestion settings

### Services  
- **KafkaConsumerService**: Consumes stock data from Kafka topics
- **DruidIngestionService**: Manages Druid ingestion tasks via API
- **EnvironmentConfigProvider**: Provides configuration from environment variables

### Main Application
- **DruidPipelineApplication**: Orchestrates the entire pipeline with proper error handling and monitoring

## Extension Examples

### Adding a New Data Source

1. Implement `IDataConsumer` interface:
```python
class RedisConsumerService(IDataConsumer):
    def connect(self) -> bool: ...
    def consume_messages(self) -> Iterator[StockRecord]: ...
```

2. Inject into application:
```python
redis_consumer = RedisConsumerService(config_provider)
app = DruidPipelineApplication(consumer=redis_consumer)
```

### Adding a New Configuration Source

1. Implement `IConfigProvider` interface:
```python
class DatabaseConfigProvider(IConfigProvider):
    def get_kafka_config(self) -> Dict: ...
    def get_druid_config(self) -> Dict: ...
```

2. Use in services:
```python
db_config = DatabaseConfigProvider()
consumer = KafkaConsumerService(db_config)
```

## Testing

The architecture enables easy unit testing through dependency injection:

```python
# Mock dependencies for testing
mock_config = Mock(spec=IConfigProvider)
mock_consumer = Mock(spec=IDataConsumer)
app = DruidPipelineApplication(
    config_provider=mock_config,
    consumer=mock_consumer
)
```

## Benefits of This Architecture

1. **Maintainability**: Clear separation of concerns makes code easy to understand and modify
2. **Testability**: Dependency injection enables comprehensive unit testing
3. **Extensibility**: New features can be added without modifying existing code
4. **Flexibility**: Different implementations can be swapped easily
5. **Reliability**: Proper error handling and monitoring throughout the pipeline