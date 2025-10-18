"""
Stock API Service - Handles stock data fetching and API endpoints.
SRP: Only responsible for stock data operations and HTTP endpoints.
DIP: Depends on MessageProducerService and IConfigProvider abstractions.
OCP: Open for extension with new data sources and endpoints.
"""

import logging
import pandas as pd
import yfinance as yf
from typing import Dict, Any, Optional
from datetime import datetime

from .message_producer_service import MessageProducerService
from ..interfaces.config_provider import IConfigProvider
from ..models.stock_data import StockRecord


logger = logging.getLogger(__name__)


class StockDataProvider:
    """
    Stock data provider using yfinance.
    SRP: Only responsible for fetching stock data.
    OCP: Can be extended with other data providers.
    """
    
    @staticmethod
    def fetch_stock_data(ticker: str, period: str = "1d", interval: str = "1m") -> Optional[pd.DataFrame]:
        """
        Fetch stock data from yfinance.
        
        Args:
            ticker: Stock symbol
            period: Time period (1d, 5d, 1mo, etc.)
            interval: Data interval (1m, 5m, 15m, 1h, etc.)
            
        Returns:
            DataFrame with stock data or None if failed
        """
        try:
            logger.info(f"Fetching stock data: {ticker} (period={period}, interval={interval})")
            
            # Download data from yfinance
            data = yf.download(tickers=ticker, period=period, interval=interval)
            
            if data.empty:
                logger.warning(f"No data returned for {ticker}")
                return None
            
            logger.info(f"Successfully fetched data: {data.shape}")
            return data
            
        except Exception as e:
            logger.error(f"Error fetching stock data for {ticker}: {e}")
            return None


class StockDataProcessor:
    """
    Stock data processor for formatting and transformation.
    SRP: Only responsible for data processing and formatting.
    """
    
    @staticmethod
    def process_dataframe(data: pd.DataFrame, ticker: str) -> list:
        """
        Process pandas DataFrame into list of stock records.
        
        Args:
            data: Raw stock data DataFrame
            ticker: Stock symbol
            
        Returns:
            List of processed stock records
        """
        try:
            # Handle MultiIndex columns (flatten them)
            if isinstance(data.columns, pd.MultiIndex):
                data.columns = ['_'.join(col).strip() for col in data.columns]
                logger.info(f"Flattened MultiIndex columns: {data.columns}")
            else:
                # Ensure all column names are strings
                data.columns = [str(col) for col in data.columns]
            
            # Reset index to make datetime a column
            data_with_time = data.reset_index()
            
            # Convert to records
            records = StockDataProcessor._convert_to_records(data_with_time, ticker)
            logger.info(f"Processed {len(records)} records for {ticker}")
            
            return records
            
        except Exception as e:
            logger.error(f"Error processing DataFrame: {e}")
            return []
    
    @staticmethod
    def _convert_to_records(data: pd.DataFrame, ticker: str) -> list:
        """Convert DataFrame to list of dictionaries."""
        try:
            # Convert to JSON-serializable format
            json_str = data.to_json(orient="records", date_format="iso")
            records = pd.read_json(json_str).to_dict('records')
            
            # Add metadata and ensure JSON serializable
            processed_records = []
            fetch_timestamp = datetime.now().isoformat()
            
            for record in records:
                processed_record = StockDataProcessor._clean_record(record)
                processed_record['ticker'] = ticker
                processed_record['fetch_timestamp'] = fetch_timestamp
                processed_records.append(processed_record)
            
            return processed_records
            
        except Exception as e:
            logger.error(f"Error converting to records: {e}")
            return []
    
    @staticmethod
    def _clean_record(record: Dict[str, Any]) -> Dict[str, Any]:
        """Clean record to ensure JSON serialization."""
        cleaned = {}
        
        for key, value in record.items():
            if pd.isna(value):
                cleaned[key] = None
            elif isinstance(value, (pd.Timestamp, pd.Timedelta)):
                cleaned[key] = str(value)
            else:
                cleaned[key] = value
        
        return cleaned


class StockAPIService:
    """
    Stock API service following SOLID principles.
    SRP: Only handles stock API operations and endpoint routing.
    DIP: Depends on MessageProducerService and IConfigProvider abstractions.
    OCP: Open for extension with new endpoints and data sources.
    """
    
    def __init__(self, message_producer: MessageProducerService, config_provider: IConfigProvider):
        """
        Initialize stock API service with dependency injection.
        
        Args:
            message_producer: Message producer service implementation
            config_provider: Configuration provider implementation
        """
        self._message_producer = message_producer
        self._config_provider = config_provider
        self._data_provider = StockDataProvider()
        self._data_processor = StockDataProcessor()
        
        # Configure logging
        logging.basicConfig(level=logging.INFO)
    
    def register_flask_routes(self, app) -> None:
        """
        Register Flask routes with the application.
        
        Args:
            app: Flask application instance
        """
        from flask import request, jsonify
        
        @app.route('/fetch', methods=['POST'])
        def fetch_stock():
            return self._handle_fetch_stock_request(request)
        
        @app.route('/health', methods=['GET'])
        def health():
            return self._handle_health_request()
        
        @app.route('/status', methods=['GET'])
        def status():
            return self._handle_status_request()
        
        @app.route('/reinit', methods=['POST'])
        def reinit():
            return self._handle_reinit_request()
    
    def _handle_fetch_stock_request(self, request):
        """Handle stock data fetch request."""
        from flask import jsonify
        
        try:
            # Get parameters
            ticker = request.args.get("ticker")
            period = request.args.get("period", "1d")
            interval = request.args.get("interval", "1m")
            
            if not ticker:
                return jsonify({"error": "ticker parameter is required"}), 400
            
            # Fetch and process data
            raw_data = self._data_provider.fetch_stock_data(ticker, period, interval)
            if raw_data is None:
                return jsonify({"error": "Failed to fetch stock data"}), 404
            
            # Process data
            processed_data = self._data_processor.process_dataframe(raw_data, ticker)
            if not processed_data:
                return jsonify({"error": "Failed to process stock data"}), 500
            
            # Send to message broker
            result = self._message_producer.send_bulk_data(processed_data)
            
            if result['success']:
                return jsonify({
                    "ticker": ticker,
                    "rows_sent": result['sent'],
                    "total_rows": result['total'],
                    "status": "success",
                    "period": period,
                    "interval": interval,
                    "errors": result.get('errors')
                })
            else:
                return jsonify({
                    "error": result['error'],
                    "rows_sent": result.get('sent', 0)
                }), 500
                
        except Exception as e:
            logger.error(f"Fetch request error: {e}")
            return jsonify({"error": str(e)}), 500
    
    def _handle_health_request(self):
        """Handle health check request."""
        from flask import jsonify
        
        try:
            service_info = self._message_producer.get_service_info()
            
            return jsonify({
                "status": "healthy",
                "service": "stock-api",
                "version": "2.0.0",
                "message_producer": {
                    "connected": service_info['broker']['connected'],
                    "type": service_info['broker']['type'],
                    "topic": service_info['config']['topic']
                }
            })
            
        except Exception as e:
            return jsonify({
                "status": "unhealthy", 
                "error": str(e)
            }), 500
    
    def _handle_status_request(self):
        """Handle detailed status request."""
        from flask import jsonify
        
        try:
            service_info = self._message_producer.get_service_info()
            return jsonify(service_info)
            
        except Exception as e:
            return jsonify({"error": str(e)}), 500
    
    def _handle_reinit_request(self):
        """Handle service reinitialization request."""
        from flask import jsonify
        
        try:
            # Close and reinitialize message producer
            self._message_producer.close()
            
            # Create new message producer
            self._message_producer = MessageProducerService(self._config_provider)
            
            service_info = self._message_producer.get_service_info()
            
            if service_info['broker']['connected']:
                return jsonify({
                    "status": "success",
                    "message": "Service reinitialized successfully"
                })
            else:
                return jsonify({
                    "status": "error", 
                    "message": "Failed to reinitialize service"
                }), 500
                
        except Exception as e:
            return jsonify({
                "status": "error",
                "error": str(e)
            }), 500