"""
Configuration provider interface following Dependency Inversion Principle.
DIP: High-level modules depend on this abstraction, not concrete implementations.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional


class IConfigProvider(ABC):
    """
    Interface for configuration management.
    DIP: Allows dependency injection of different config sources.
    """
    
    @abstractmethod
    def get_kafka_config(self) -> Dict[str, Any]:
        """Get Kafka connection configuration."""
        pass
    
    @abstractmethod
    def get_druid_config(self) -> Dict[str, Any]:
        """Get Druid connection configuration."""
        pass
    
    @abstractmethod
    def get_ingestion_config(self) -> Dict[str, Any]:
        """Get ingestion-specific configuration."""
        pass
    
    @abstractmethod
    def get_flask_config(self) -> Dict[str, Any]:
        """Get Flask application configuration."""
        pass
    
    @abstractmethod
    def get_config_value(self, key: str, default: Optional[Any] = None) -> Any:
        """
        Get specific configuration value.
        
        Args:
            key: Configuration key
            default: Default value if key not found
            
        Returns:
            Configuration value
        """
        pass