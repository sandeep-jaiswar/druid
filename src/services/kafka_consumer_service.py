"""
Kafka consumer service implementation.
SRP: Only responsible for consuming messages from Kafka.
DIP: Depends on IConfigProvider abstraction, not concrete config.
"""

import json
import logging
from typing import Iterator, Optional, Dict, Any
from kafka import KafkaConsumer
from kafka.errors import KafkaError

from ..interfaces.data_consumer import IDataConsumer
from ..interfaces.config_provider import IConfigProvider
from ..models.stock_data import StockRecord


logger = logging.getLogger(__name__)


class KafkaConsumerService(IDataConsumer):
    """
    Kafka consumer implementation following SOLID principles.
    SRP: Only handles Kafka message consumption.
    DIP: Depends on IConfigProvider interface.
    """
    
    def __init__(self, config_provider: IConfigProvider):
        """
        Initialize Kafka consumer with dependency injection.
        
        Args:
            config_provider: Configuration provider implementation
        """
        self._config_provider = config_provider
        self._consumer: Optional[KafkaConsumer] = None
        self._is_connected = False
        
    def connect(self) -> bool:
        """
        Establish connection to Kafka broker.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            kafka_config = self._config_provider.get_kafka_config()
            
            self._consumer = KafkaConsumer(
                kafka_config['topic'],
                bootstrap_servers=kafka_config['bootstrap_servers'],
                group_id=kafka_config['group_id'],
                auto_offset_reset=kafka_config['auto_offset_reset'],
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
                consumer_timeout_ms=10000,  # 10 second timeout
                api_version_auto_timeout_ms=10000,
                metadata_max_age_ms=30000
            )
            
            # Test connection by getting metadata
            self._consumer.topics()
            self._is_connected = True
            logger.info(f"Connected to Kafka topic: {kafka_config['topic']}")
            return True
            
        except KafkaError as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            self._is_connected = False
            return False
        except Exception as e:
            logger.error(f"Unexpected error connecting to Kafka: {e}")
            self._is_connected = False
            return False
    
    def consume_messages(self, timeout_ms: Optional[int] = None) -> Iterator[StockRecord]:
        """
        Consume messages from Kafka and yield StockRecord objects.
        
        Args:
            timeout_ms: Timeout for message consumption
            
        Yields:
            StockRecord: Parsed stock data records
        """
        if not self._is_connected or not self._consumer:
            logger.warning("Consumer not connected. Attempting to reconnect...")
            if not self.connect():
                logger.error("Failed to reconnect to Kafka")
                return
        
        try:
            # Set timeout if provided
            if timeout_ms:
                self._consumer._consumer_timeout = timeout_ms
            
            for message in self._consumer:
                try:
                    if message.value is None:
                        logger.warning("Received empty message, skipping...")
                        continue
                    
                    # Parse message data into StockRecord
                    stock_record = StockRecord.from_kafka_message(message.value)
                    logger.debug(f"Processed message for ticker: {stock_record.ticker}")
                    yield stock_record
                    
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    logger.error(f"Message content: {message.value}")
                    # Continue processing other messages
                    continue
                    
        except KafkaError as e:
            logger.error(f"Kafka consumption error: {e}")
        except Exception as e:
            logger.error(f"Unexpected error during consumption: {e}")
    
    def close(self) -> None:
        """Close Kafka consumer connection and cleanup resources."""
        if self._consumer:
            try:
                self._consumer.close()
                logger.info("Kafka consumer closed successfully")
            except Exception as e:
                logger.error(f"Error closing Kafka consumer: {e}")
            finally:
                self._consumer = None
                self._is_connected = False
    
    def get_consumer_info(self) -> Dict[str, Any]:
        """
        Get consumer metadata and status information.
        
        Returns:
            Dict containing consumer status and configuration
        """
        kafka_config = self._config_provider.get_kafka_config()
        
        info = {
            'connected': self._is_connected,
            'bootstrap_servers': kafka_config['bootstrap_servers'],
            'topic': kafka_config['topic'],
            'group_id': kafka_config['group_id'],
            'auto_offset_reset': kafka_config['auto_offset_reset']
        }
        
        if self._consumer and self._is_connected:
            try:
                # Add partition assignment info
                assignment = self._consumer.assignment()
                info['assigned_partitions'] = [
                    {'topic': tp.topic, 'partition': tp.partition} 
                    for tp in assignment
                ]
            except Exception as e:
                logger.warning(f"Could not get partition assignment: {e}")
                info['assigned_partitions'] = []
        
        return info