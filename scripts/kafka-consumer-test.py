#!/usr/bin/env python3
"""
Simple Kafka consumer to inspect message structure
"""
import json
from kafka import KafkaConsumer

def main():
    try:
        # Create consumer
        consumer = KafkaConsumer(
            'stock-data',
            bootstrap_servers=['localhost:9092'],
            consumer_timeout_ms=10000,  # Timeout after 10 seconds
            value_deserializer=lambda x: x.decode('utf-8'),
            auto_offset_reset='earliest'  # Read from beginning
        )
        
        print("Connected to Kafka. Consuming messages...")
        message_count = 0
        
        for message in consumer:
            message_count += 1
            print(f"\n--- Message {message_count} ---")
            print(f"Topic: {message.topic}")
            print(f"Partition: {message.partition}")
            print(f"Offset: {message.offset}")
            print(f"Timestamp: {message.timestamp}")
            print("Value:")
            
            try:
                # Try to parse as JSON for pretty printing
                data = json.loads(message.value)
                print(json.dumps(data, indent=2))
            except json.JSONDecodeError:
                print(message.value)
            
            # Only show first few messages
            if message_count >= 3:
                break
                
        consumer.close()
        print(f"\nConsumed {message_count} messages")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()