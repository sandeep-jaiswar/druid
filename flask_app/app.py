from flask import Flask, request, jsonify
from kafka import KafkaProducer
import yfinance as yf
import pandas as pd
import json
import os
import time

app = Flask(__name__)

# Kafka broker (from env or default)
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
TOPIC_NAME = os.environ.get("KAFKA_TOPIC", "stocks")

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

@app.route("/fetch", methods=["POST"])
def fetch_stock():
    """
    Example request: POST /fetch?ticker=AAPL&period=1d&interval=1m
    """
    ticker = request.args.get("ticker")
    period = request.args.get("period", "1d")
    interval = request.args.get("interval", "1m")

    if not ticker:
        return jsonify({"error": "ticker is required"}), 400

    try:
        data = yf.download(tickers=ticker, period=period, interval=interval)
        if data.empty:
            return jsonify({"error": "no data returned"}), 404

        # Convert dataframe to JSON dict
        records = data.reset_index().to_dict(orient="records")

        # Produce each record to Kafka
        for record in records:
            producer.send(TOPIC_NAME, record)

        producer.flush()
        return jsonify({
            "ticker": ticker,
            "rows_sent": len(records),
            "status": "success"
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
