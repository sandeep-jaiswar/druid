#!/usr/bin/env python3
"""
API Application Entry Point.
Follows SOLID principles with clean architecture and dependency injection.
"""

import os
import logging
from src.api.flask_app_factory import create_flask_app
from src.config.environment_config import EnvironmentConfigProvider


def main():
    """Main API application entry point."""
    # Create configuration provider (DIP)
    config_provider = EnvironmentConfigProvider()
    
    # Get Flask configuration
    flask_config = config_provider.get_flask_config()
    
    # Create Flask application with dependency injection (DIP)
    app = create_flask_app(config_provider)
    
    # Configure Flask app
    app.config.update({
        'JSON_SORT_KEYS': False,
        'JSONIFY_PRETTYPRINT_REGULAR': True
    })
    
    # Log startup information
    logger = logging.getLogger(__name__)
    logger.info("Starting Stock Data API Server")
    logger.info(f"Host: {flask_config['host']}")
    logger.info(f"Port: {flask_config['port']}")
    logger.info(f"Debug: {flask_config['debug']}")
    
    # Run application
    app.run(
        host=flask_config['host'],
        port=flask_config['port'],
        debug=flask_config['debug']
    )


if __name__ == '__main__':
    main()