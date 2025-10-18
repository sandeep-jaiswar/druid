"""
Interfaces package for abstraction contracts.
Follows Interface Segregation Principle - small, focused interfaces.
"""

from .data_consumer import IDataConsumer
from .ingestion_service import IIngestionService
from .config_provider import IConfigProvider

__all__ = [
    'IDataConsumer',
    'IIngestionService',
    'IConfigProvider'
]