"""
Environment-based configuration provider.
DIP: Concrete implementation of IConfigProvider interface.
SRP: Only responsible for environment-based configuration.
"""

import os
from typing import Dict, Any, Optional
from ..interfaces.config_provider import IConfigProvider


class EnvironmentConfigProvider(IConfigProvider):
    """
    Configuration provider that reads from environment variables.
    DIP: Implements IConfigProvider interface for dependency injection.
    """
    
    def __init__(self):
        """Initialize with environment variable defaults."""
        self._kafka_config = {
            'bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'),
            'topic': os.getenv('KAFKA_TOPIC', 'stock-data'),
            'group_id': os.getenv('KAFKA_GROUP_ID', 'druid-kafka-indexing-service'),
            'auto_offset_reset': os.getenv('KAFKA_AUTO_OFFSET_RESET', 'latest')
        }
        
        self._druid_config = {
            'coordinator_url': os.getenv('DRUID_COORDINATOR_URL', 'http://coordinator:8081'),
            'overlord_url': os.getenv('DRUID_OVERLORD_URL', 'http://overlord:8090'),
            'router_url': os.getenv('DRUID_ROUTER_URL', 'http://router:8888'),
            'datasource': os.getenv('DRUID_DATASOURCE', 'stock_data')
        }
        
        self._ingestion_config = {
            'task_count': int(os.getenv('INGESTION_TASK_COUNT', '1')),
            'replicas': int(os.getenv('INGESTION_REPLICAS', '1')), 
            'task_duration': os.getenv('INGESTION_TASK_DURATION', 'PT3600S'),
            'max_rows_in_memory': int(os.getenv('INGESTION_MAX_ROWS_MEMORY', '100000')),
            'segment_granularity': os.getenv('INGESTION_SEGMENT_GRANULARITY', 'DAY'),
            'query_granularity': os.getenv('INGESTION_QUERY_GRANULARITY', 'MINUTE')
        }
        
        self._flask_config = {
            'host': os.getenv('FLASK_HOST', '0.0.0.0'),
            'port': int(os.getenv('FLASK_PORT', '5000')),
            'debug': os.getenv('FLASK_DEBUG', 'False').lower() == 'true'
        }
    
    def get_kafka_config(self) -> Dict[str, Any]:
        """Get Kafka connection configuration from environment."""
        return self._kafka_config.copy()
    
    def get_druid_config(self) -> Dict[str, Any]:
        """Get Druid connection configuration from environment."""
        return self._druid_config.copy()
    
    def get_ingestion_config(self) -> Dict[str, Any]:
        """Get ingestion-specific configuration from environment."""
        return self._ingestion_config.copy()
    
    def get_flask_config(self) -> Dict[str, Any]:
        """Get Flask application configuration from environment."""
        return self._flask_config.copy()
    
    def get_config_value(self, key: str, default: Optional[Any] = None) -> Any:
        """
        Get specific configuration value from environment.
        
        Args:
            key: Environment variable name
            default: Default value if not found
            
        Returns:
            Configuration value from environment or default
        """
        return os.getenv(key, default)