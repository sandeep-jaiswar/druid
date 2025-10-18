"""
Message Producer Service - Framework agnostic message publishing.
SRP: Only responsible for producing messages to message brokers.
DIP: Depends on IConfigProvider abstraction.
OCP: Open for extension to different message brokers.
"""

import json
import logging
from typing import Dict, Any, Optional, Protocol
from abc import ABC, abstractmethod

from ..interfaces.config_provider import IConfigProvider


logger = logging.getLogger(__name__)


class IMessageBroker(Protocol):
    """Interface for message broker implementations (ISP)."""
    
    def send_message(self, topic: str, message: str) -> bool:
        """Send a message to the specified topic."""
        ...
    
    def send_bulk_messages(self, topic: str, messages: list) -> Dict[str, Any]:
        """Send multiple messages to the specified topic."""
        ...
    
    def get_broker_info(self) -> Dict[str, Any]:
        """Get broker connection information."""
        ...
    
    def close(self) -> None:
        """Close broker connection."""
        ...


class KafkaMessageBroker:
    """
    Kafka implementation of message broker.
    SRP: Only handles Kafka-specific operations.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize Kafka broker with configuration."""
        from kafka import KafkaProducer
        from kafka.admin import KafkaAdminClient, NewTopic
        from kafka.errors import KafkaError
        
        self._config = config
        self._producer: Optional[KafkaProducer] = None
        self._KafkaError = KafkaError
        self._initialize()
    
    def _initialize(self) -> None:
        """Initialize Kafka producer and create topic if needed."""
        try:
            self._ensure_topic_exists()
            self._create_producer()
        except Exception as e:
            logger.error(f"Failed to initialize Kafka broker: {e}")
    
    def _ensure_topic_exists(self) -> None:
        """Create Kafka topic if it doesn't exist."""
        try:
            from kafka.admin import KafkaAdminClient, NewTopic
            
            admin_client = KafkaAdminClient(
                bootstrap_servers=self._config['bootstrap_servers'],
                request_timeout_ms=10000,
                api_version_auto_timeout_ms=10000
            )
            
            topic = self._config['topic']
            existing_topics = admin_client.list_topics(timeout_ms=10000)
            
            if topic in existing_topics:
                logger.info(f"Topic {topic} already exists")
                admin_client.close()
                return
            
            topic_list = [NewTopic(name=topic, num_partitions=1, replication_factor=1)]
            admin_client.create_topics(new_topics=topic_list, validate_only=False, timeout_ms=10000)
            logger.info(f"Topic {topic} created successfully")
            admin_client.close()
            
        except Exception as e:
            logger.warning(f"Topic creation might have failed: {e}")
    
    def _create_producer(self) -> None:
        """Create Kafka producer with optimized settings."""
        try:
            from kafka import KafkaProducer
            
            self._producer = KafkaProducer(
                bootstrap_servers=self._config['bootstrap_servers'],
                api_version_auto_timeout_ms=10000,
                connections_max_idle_ms=540000,
                metadata_max_age_ms=30000,
                request_timeout_ms=10000,
                retry_backoff_ms=100,
                retries=3,
                max_block_ms=10000,
                buffer_memory=33554432,
                batch_size=16384,
                linger_ms=100,
                security_protocol='PLAINTEXT'
            )
            logger.info("Kafka producer created successfully")
            
        except Exception as e:
            logger.error(f"Failed to create Kafka producer: {e}")
            self._producer = None
    
    def send_message(self, topic: str, message: str) -> bool:
        """Send a single message to Kafka topic."""
        if not self._producer:
            logger.error("Kafka producer not available")
            return False
        
        try:
            self._producer.send(topic, value=message.encode('utf-8'))
            self._producer.flush()
            return True
        except self._KafkaError as e:
            logger.error(f"Kafka error sending message: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error sending message: {e}")
            return False
    
    def send_bulk_messages(self, topic: str, messages: list) -> Dict[str, Any]:
        """Send multiple messages to Kafka topic."""
        if not self._producer:
            return {'success': False, 'error': 'Producer not available', 'sent': 0}
        
        sent_count = 0
        errors = []
        
        try:
            for i, message in enumerate(messages):
                try:
                    self._producer.send(topic, value=message.encode('utf-8'))
                    sent_count += 1
                except Exception as e:
                    errors.append(f"Message {i}: {str(e)}")
            
            self._producer.flush()
            logger.info(f"Successfully sent {sent_count}/{len(messages)} messages")
            
            return {
                'success': True,
                'sent': sent_count,
                'total': len(messages),
                'errors': errors if errors else None
            }
            
        except Exception as e:
            logger.error(f"Bulk send failed: {e}")
            return {'success': False, 'error': str(e), 'sent': sent_count}
    
    def get_broker_info(self) -> Dict[str, Any]:
        """Get Kafka broker information."""
        return {
            'type': 'kafka',
            'connected': self._producer is not None,
            'bootstrap_servers': self._config['bootstrap_servers'],
            'topic': self._config['topic']
        }
    
    def close(self) -> None:
        """Close Kafka producer."""
        if self._producer:
            try:
                self._producer.close()
                logger.info("Kafka producer closed successfully")
            except Exception as e:
                logger.error(f"Error closing Kafka producer: {e}")
            finally:
                self._producer = None


class MessageProducerService:
    """
    Generic message producer service following SOLID principles.
    SRP: Only responsible for message production orchestration.
    DIP: Depends on IConfigProvider and IMessageBroker abstractions.
    OCP: Open for extension with new message brokers.
    """
    
    def __init__(self, config_provider: IConfigProvider, broker: Optional[IMessageBroker] = None):
        """
        Initialize message producer with dependency injection.
        
        Args:
            config_provider: Configuration provider implementation
            broker: Message broker implementation (defaults to Kafka)
        """
        self._config_provider = config_provider
        self._broker_config = config_provider.get_kafka_config()  # TODO: Make this generic
        
        # Use dependency injection for broker (DIP)
        if broker:
            self._broker = broker
        else:
            # Default to Kafka broker
            self._broker = KafkaMessageBroker(self._broker_config)
    
    def send_data(self, data: Dict[str, Any]) -> bool:
        """
        Send structured data as JSON message.
        
        Args:
            data: Dictionary containing data to send
            
        Returns:
            True if successful, False otherwise
        """
        try:
            json_message = json.dumps(data, default=str)
            topic = self._broker_config['topic']
            return self._broker.send_message(topic, json_message)
        except Exception as e:
            logger.error(f"Error sending data: {e}")
            return False
    
    def send_bulk_data(self, data_list: list) -> Dict[str, Any]:
        """
        Send multiple data records as JSON messages.
        
        Args:
            data_list: List of dictionaries to send
            
        Returns:
            Dict with success information
        """
        try:
            json_messages = [json.dumps(data, default=str) for data in data_list]
            topic = self._broker_config['topic']
            return self._broker.send_bulk_messages(topic, json_messages)
        except Exception as e:
            logger.error(f"Error sending bulk data: {e}")
            return {'success': False, 'error': str(e), 'sent': 0}
    
    def get_service_info(self) -> Dict[str, Any]:
        """Get service status and configuration information."""
        broker_info = self._broker.get_broker_info()
        return {
            'service': 'message_producer',
            'broker': broker_info,
            'config': {
                'topic': self._broker_config['topic'],
                'bootstrap_servers': self._broker_config.get('bootstrap_servers', 'N/A')
            }
        }
    
    def close(self) -> None:
        """Close message producer and cleanup resources."""
        if self._broker:
            self._broker.close()