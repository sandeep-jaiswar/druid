"""
Data consumer interface following Interface Segregation Principle.
ISP: Interface only contains methods related to data consumption.
"""

from abc import ABC, abstractmethod
from typing import Iterator, Optional, Any, Dict
from ..models.stock_data import StockRecord


class IDataConsumer(ABC):
    """
    Interface for consuming data from external sources.
    ISP: Only methods needed for data consumption.
    """
    
    @abstractmethod
    def connect(self) -> bool:
        """
        Establish connection to data source.
        Returns True if successful, False otherwise.
        """
        pass
    
    @abstractmethod
    def consume_messages(self, timeout_ms: Optional[int] = None) -> Iterator[StockRecord]:
        """
        Consume messages and yield StockRecord objects.
        
        Args:
            timeout_ms: Timeout for consumption in milliseconds
            
        Yields:
            StockRecord: Parsed stock data records
        """
        pass
    
    @abstractmethod
    def close(self) -> None:
        """Close connection and cleanup resources."""
        pass
    
    @abstractmethod
    def get_consumer_info(self) -> Dict[str, Any]:
        """Get consumer metadata and status information."""
        pass