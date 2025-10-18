"""
API package for data ingestion endpoints.
Follows SOLID principles with framework-agnostic design.
"""

from .stock_api_service import StockAPIService
from .message_producer_service import MessageProducerService
from .flask_app_factory import create_flask_app

__all__ = [
    'StockAPIService',
    'MessageProducerService', 
    'create_flask_app'
]