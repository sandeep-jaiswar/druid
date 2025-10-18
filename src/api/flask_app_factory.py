"""
Flask Application Factory following SOLID principles.
SRP: Only responsible for Flask app creation and configuration.
DIP: Depends on abstractions through dependency injection.
"""

from flask import Flask
import logging

from .stock_api_service import StockAPIService
from .message_producer_service import MessageProducerService
from ..config.environment_config import EnvironmentConfigProvider
from ..interfaces.config_provider import IConfigProvider


def create_flask_app(config_provider: IConfigProvider = None) -> Flask:
    """
    Flask application factory with dependency injection.
    
    Args:
        config_provider: Configuration provider implementation
        
    Returns:
        Configured Flask application instance
    """
    # Use default config provider if none provided (DIP)
    if config_provider is None:
        config_provider = EnvironmentConfigProvider()
    
    # Create Flask app
    app = Flask(__name__)
    
    # Configure logging
    log_level = config_provider.get_config_value('LOG_LEVEL', 'INFO')
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Initialize services with dependency injection (DIP)
    message_producer = MessageProducerService(config_provider)
    stock_api_service = StockAPIService(message_producer, config_provider)
    
    # Register routes (SRP - service handles its own routing)
    stock_api_service.register_flask_routes(app)
    
    # Add app-level error handlers
    _register_error_handlers(app)
    
    return app


def _register_error_handlers(app: Flask) -> None:
    """Register global error handlers for Flask app."""
    
    @app.errorhandler(404)
    def not_found(error):
        from flask import jsonify
        return jsonify({
            "error": "Endpoint not found",
            "message": "The requested endpoint does not exist"
        }), 404
    
    @app.errorhandler(405)
    def method_not_allowed(error):
        from flask import jsonify
        return jsonify({
            "error": "Method not allowed",
            "message": "The method is not allowed for the requested endpoint"
        }), 405
    
    @app.errorhandler(500)
    def internal_error(error):
        from flask import jsonify
        return jsonify({
            "error": "Internal server error",
            "message": "An unexpected error occurred"
        }), 500