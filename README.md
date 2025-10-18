# End-to-End Data Ingestion Pipeline

This project implements a complete data ingestion pipeline using Flask, Kafka, and Apache Druid.

## Architecture

```
Flask App → Kafka (KRaft) → Apache Druid
                ↓              ↓
              Zookeeper    PostgreSQL
```

## Components

- **Flask App**: Fetches stock data from Yahoo Finance and publishes to Kafka
- **Kafka (KRaft mode)**: Streaming data buffer
- **Apache Druid**: Real-time analytics database
  - Coordinator: Manages segment distribution
  - Broker: Handles queries
  - Historical: Serves historical segments
  - MiddleManager: Handles ingestion tasks
  - Router: Provides unified UI (port 8888)
- **Zookeeper**: Coordination service for Druid
- **PostgreSQL**: Metadata storage for Druid

## Prerequisites

- Docker and Docker Compose
- At least 4GB RAM available for Docker

## Quick Start

1. Start all services:
   ```bash
   docker compose up -d
   ```

2. Wait for all services to be healthy (this may take 1-2 minutes):
   ```bash
   docker compose ps
   ```

3. Access the Druid Console:
   ```
   http://localhost:8888
   ```

4. Fetch stock data via Flask:
   ```bash
   curl -X POST "http://localhost:5000/fetch?ticker=AAPL&period=1d&interval=1h"
   ```

5. Check Flask health:
   ```bash
   curl http://localhost:5000/health
   ```

## Service Ports

- Flask App: `5000`
- Kafka: `9092`
- Zookeeper: `2181`
- PostgreSQL: `5432`
- Druid Coordinator: `8081`
- Druid Broker: `8082`
- Druid Historical: `8083`
- Druid Router (Console): `8888`
- Druid MiddleManager: `8091`

## Setting up Kafka Ingestion in Druid

1. Access the Druid Console at http://localhost:8888
2. Go to "Load data" → "Streaming" → "Apache Kafka"
3. Configure the supervisor spec:
   - Bootstrap servers: `kafka:9092`
   - Topic: `stocks`
   - Input format: JSON

## Stopping the Services

```bash
docker compose down
```

To remove all data volumes:
```bash
docker compose down -v
```

## Troubleshooting

- If services fail to start, check logs: `docker compose logs <service-name>`
- Ensure sufficient memory is allocated to Docker
- Wait for healthchecks to pass before accessing services
