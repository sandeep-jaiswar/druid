from flask import Flask, request, jsonify
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import yfinance as yf
import pandas as pd
import json
import os
import time
import logging

app = Flask(__name__)

# Kafka broker (from env or default)
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
TOPIC_NAME = os.environ.get("KAFKA_TOPIC", "stocks")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create topic if it doesn't exist
def ensure_topic_exists():
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BROKER,
            request_timeout_ms=10000,
            api_version_auto_timeout_ms=10000
        )
        
        # Check if topic exists first
        existing_topics = admin_client.list_topics(timeout_ms=10000)
        if TOPIC_NAME in existing_topics:
            logger.info(f"Topic {TOPIC_NAME} already exists")
            admin_client.close()
            return
            
        topic_list = [NewTopic(name=TOPIC_NAME, num_partitions=1, replication_factor=1)]
        admin_client.create_topics(new_topics=topic_list, validate_only=False, timeout_ms=10000)
        logger.info(f"Topic {TOPIC_NAME} created successfully")
        admin_client.close()
    except Exception as e:
        logger.info(f"Topic {TOPIC_NAME} might already exist or creation failed: {e}")

# Create Kafka producer with optimized settings for containers
def create_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            # Connection settings
            api_version_auto_timeout_ms=10000,
            connections_max_idle_ms=540000,
            # Metadata settings  
            metadata_max_age_ms=30000,
            # Request settings
            request_timeout_ms=10000,
            retry_backoff_ms=100,
            retries=3,
            # Block settings
            max_block_ms=10000,
            # Buffer settings
            buffer_memory=33554432,
            batch_size=16384,
            linger_ms=100,
            # Security
            security_protocol='PLAINTEXT'
        )
        logger.info("Kafka producer created successfully")
        return producer
    except Exception as e:
        logger.error(f"Failed to create Kafka producer: {e}")
        return None

# Global producer variable
producer = None

# Initialize function
def initialize_kafka():
    global producer
    logger.info("Initializing Kafka connections...")
    ensure_topic_exists()
    producer = create_producer()
    if producer:
        logger.info("Kafka initialization successful")
        return True
    else:
        logger.error("Kafka initialization failed")
        return False

# Try to initialize Kafka on startup
try:
    initialize_kafka()
except Exception as e:
    logger.error(f"Failed to initialize Kafka on startup: {e}")
    producer = None

@app.route("/fetch", methods=["POST"])
def fetch_stock():
    """
    Example request: POST /fetch?ticker=AAPL&period=1d&interval=1m
    """
    global producer
    ticker = request.args.get("ticker")
    period = request.args.get("period", "1d")
    interval = request.args.get("interval", "1m")

    if not ticker:
        return jsonify({"error": "ticker is required"}), 400

    # Ensure producer is available, retry if needed
    if not producer:
        logger.info("Producer not available, attempting to initialize...")
        if not initialize_kafka():
            return jsonify({"error": "Kafka producer not available"}), 500

    try:
            
        logger.info(f"Fetching data for {ticker}")
        data = yf.download(tickers=ticker, period=period, interval=interval)
        if data.empty:
            return jsonify({"error": "no data returned"}), 404

        # Debug: print the column structure
        logger.info(f"Data columns: {data.columns}")
        logger.info(f"Data columns type: {type(data.columns)}")
        logger.info(f"Data shape: {data.shape}")

        # Flatten multi-level column names if they exist
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = ['_'.join(col).strip() for col in data.columns]
            logger.info(f"Flattened columns: {data.columns}")
        else:
            # Ensure all column names are strings
            data.columns = [str(col) for col in data.columns]
            
        # Convert to JSON string directly - this handles MultiIndex columns properly
        try:
            logger.info("About to reset index and convert to JSON...")
            json_str = data.reset_index().to_json(orient="records", date_format="iso")
            logger.info("JSON conversion successful")
        except Exception as e:
            logger.error(f"Error in to_json conversion: {e}")
            raise
            
        try:
            records = json.loads(json_str)
            logger.info(f"Retrieved {len(records)} records for {ticker}")
        except Exception as e:
            logger.error(f"Error parsing JSON: {e}")
            raise

        # Produce each record to Kafka using string serialization to avoid object issues
        for i, record in enumerate(records):
            try:
                # Add metadata
                record['ticker'] = ticker
                record['fetch_timestamp'] = pd.Timestamp.now().isoformat()
                
                # Convert the record to JSON string manually to avoid serializer issues
                logger.info(f"About to serialize record {i}...")
                record_json = json.dumps(record, default=str)
                logger.info(f"Record {i} serialized successfully")
                producer.send(TOPIC_NAME, value=record_json.encode('utf-8'))
            except Exception as e:
                logger.error(f"Error processing record {i}: {e}")
                logger.error(f"Problematic record keys: {list(record.keys())}")
                logger.error(f"Problematic record key types: {[type(k) for k in record.keys()]}")
                raise

        producer.flush()
        logger.info(f"Successfully sent {len(records)} records to Kafka topic {TOPIC_NAME}")
        return jsonify({
            "ticker": ticker,
            "rows_sent": len(records),
            "status": "success"
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/health", methods=["GET"])
def health():
    kafka_status = "connected" if producer else "disconnected"
    return jsonify({
        "status": "ok",
        "kafka": kafka_status,
        "kafka_broker": KAFKA_BROKER,
        "topic": TOPIC_NAME
    })

@app.route("/kafka/reinit", methods=["POST"])
def reinit_kafka():
    """Manually reinitialize Kafka connection"""
    try:
        success = initialize_kafka()
        if success:
            return jsonify({"status": "Kafka reinitialized successfully"})
        else:
            return jsonify({"error": "Failed to reinitialize Kafka"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/test", methods=["GET"])
def test_data():
    """Test endpoint to debug yfinance data structure"""
    try:
        import yfinance as yf
        data = yf.download(tickers="AAPL", period="1d", interval="1h")
        
        result = {
            "shape": str(data.shape),
            "columns": str(data.columns),
            "columns_type": str(type(data.columns)),
            "index_type": str(type(data.index)),
            "first_row_keys": str(list(data.reset_index().iloc[0].keys())),
            "first_row_sample": str(dict(list(data.reset_index().iloc[0].items())[:3]))
        }
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
