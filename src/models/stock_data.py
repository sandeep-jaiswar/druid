"""
Stock data models following Single Responsibility Principle.
Each class has one reason to change - representing stock market data.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, Any
import json


@dataclass
class StockRecord:
    """
    Represents a single stock data record.
    SRP: Only responsible for stock record data structure.
    """
    ticker: str
    timestamp: datetime
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    volume: Optional[int] = None
    fetch_timestamp: Optional[datetime] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            'ticker': self.ticker,
            'timestamp': self.timestamp.isoformat() if self.timestamp else None,
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'close': self.close,
            'volume': self.volume,
            'fetch_timestamp': self.fetch_timestamp.isoformat() if self.fetch_timestamp else None
        }
    
    @classmethod
    def from_kafka_message(cls, message_data: Dict[str, Any]) -> 'StockRecord':
        """
        Create StockRecord from Kafka message data.
        Handles various timestamp formats and missing fields.
        """
        # Parse timestamps
        timestamp = None
        if 'Datetime' in message_data:
            timestamp = datetime.fromisoformat(message_data['Datetime'].replace('Z', '+00:00'))
        
        fetch_timestamp = None
        if 'fetch_timestamp' in message_data:
            fetch_timestamp = datetime.fromisoformat(message_data['fetch_timestamp'].replace('Z', '+00:00'))
        
        return cls(
            ticker=message_data.get('ticker', ''),
            timestamp=timestamp,
            open=message_data.get('Open_AAPL') or message_data.get('Open'),
            high=message_data.get('High_AAPL') or message_data.get('High'),
            low=message_data.get('Low_AAPL') or message_data.get('Low'),
            close=message_data.get('Close_AAPL') or message_data.get('Close'),
            volume=message_data.get('Volume_AAPL') or message_data.get('Volume'),
            fetch_timestamp=fetch_timestamp
        )


@dataclass
class StockData:
    """
    Container for multiple stock records.
    SRP: Only responsible for managing collections of stock records.
    """
    records: list[StockRecord]
    ticker: str
    
    def __len__(self) -> int:
        return len(self.records)
    
    def to_json_lines(self) -> str:
        """Convert to JSON Lines format for Druid ingestion."""
        lines = []
        for record in self.records:
            lines.append(json.dumps(record.to_dict()))
        return '\n'.join(lines)
    
    def get_latest_record(self) -> Optional[StockRecord]:
        """Get the most recent record by timestamp."""
        if not self.records:
            return None
        return max(self.records, key=lambda r: r.timestamp or datetime.min)