import os
import json
import logging
import yfinance as yf
from datetime import datetime, timedelta
from flask import Flask, request, jsonify
from kafka import KafkaProducer
import pandas as pd
import time

# Configure logging
logging.basicConfig(level=os.getenv('LOG_LEVEL', 'INFO'))
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'stock-data')
FLASK_HOST = os.getenv('FLASK_HOST', '0.0.0.0')
FLASK_PORT = int(os.getenv('FLASK_PORT', 5000))

# Initialize Kafka producer
producer = None

def get_kafka_producer():
    global producer
    if producer is None:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(','),
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                retries=3,
                retry_backoff_ms=1000
            )
            logger.info(f"Kafka producer initialized for servers: {KAFKA_BOOTSTRAP_SERVERS}")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")
            raise
    return producer

@app.route('/health')
def health():
    try:
        # Test Kafka connection
        get_kafka_producer()
        return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()})
    except Exception as e:
        return jsonify({"status": "unhealthy", "error": str(e)}), 500

@app.route('/stock/<symbol>')
def get_stock_data(symbol):
    try:
        # Fetch stock data from Yahoo Finance
        stock = yf.Ticker(symbol.upper())
        
        # Get recent data (last 5 days)
        hist = stock.hist(period="5d", interval="1h")
        
        if hist.empty:
            return jsonify({"error": f"No data found for symbol {symbol}"}), 404
        
        # Convert to records
        records = []
        for timestamp, row in hist.iterrows():
            record = {
                "symbol": symbol.upper(),
                "timestamp": timestamp.isoformat(),
                "open": float(row['Open']),
                "high": float(row['High']),
                "low": float(row['Low']),
                "close": float(row['Close']),
                "volume": int(row['Volume']),
                "event_time": datetime.now().isoformat()
            }
            records.append(record)
        
        # Send to Kafka
        kafka_producer = get_kafka_producer()
        sent_count = 0
        
        for record in records:
            try:
                future = kafka_producer.send(
                    KAFKA_TOPIC,
                    key=f"{record['symbol']}_{record['timestamp']}",
                    value=record
                )
                future.get(timeout=10)  # Wait for send confirmation
                sent_count += 1
            except Exception as e:
                logger.error(f"Failed to send record to Kafka: {e}")
        
        kafka_producer.flush()
        
        return jsonify({
            "symbol": symbol.upper(),
            "records_fetched": len(records),
            "records_sent_to_kafka": sent_count,
            "topic": KAFKA_TOPIC,
            "sample_record": records[0] if records else None
        })
        
    except Exception as e:
        logger.error(f"Error fetching stock data for {symbol}: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/bulk_stock')
def get_bulk_stock_data():
    symbols = request.args.get('symbols', 'AAPL,GOOGL,MSFT,TSLA').split(',')
    results = {}
    
    for symbol in symbols:
        try:
            # Use the existing endpoint logic
            response = get_stock_data(symbol.strip())
            if hasattr(response, 'get_json'):
                results[symbol] = response.get_json()
            else:
                results[symbol] = response[0].get_json()
        except Exception as e:
            results[symbol] = {"error": str(e)}
    
    return jsonify(results)

if __name__ == '__main__':
    logger.info(f"Starting Stock API on {FLASK_HOST}:{FLASK_PORT}")
    logger.info(f"Kafka servers: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Kafka topic: {KAFKA_TOPIC}")
    
    app.run(host=FLASK_HOST, port=FLASK_PORT, debug=False)
