"""
Main Druid pipeline application.
OCP: Open for extension (new services), closed for modification.
DIP: Depends on abstractions (interfaces) not concrete implementations.
"""

import logging
import time
import signal
import sys
from typing import Optional, Dict, Any
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError

from .interfaces.data_consumer import IDataConsumer
from .interfaces.ingestion_service import IIngestionService  
from .interfaces.config_provider import IConfigProvider
from .services.kafka_consumer_service import KafkaConsumerService
from .services.druid_ingestion_service import DruidIngestionService
from .config.environment_config import EnvironmentConfigProvider


logger = logging.getLogger(__name__)


class DruidPipelineApplication:
    """
    Main application orchestrating the Kafka → Druid data pipeline.
    
    SOLID Principles Applied:
    - SRP: Responsible only for application orchestration
    - OCP: Open for extension (new consumers/services), closed for modification  
    - LSP: Uses interfaces, any implementation can be substituted
    - ISP: Depends only on needed interface methods
    - DIP: Depends on abstractions (interfaces) not concrete classes
    """
    
    def __init__(self, 
                 config_provider: Optional[IConfigProvider] = None,
                 consumer: Optional[IDataConsumer] = None,
                 ingestion_service: Optional[IIngestionService] = None):
        """
        Initialize application with dependency injection.
        DIP: Dependencies injected as interfaces, not concrete classes.
        
        Args:
            config_provider: Configuration provider implementation
            consumer: Data consumer implementation  
            ingestion_service: Ingestion service implementation
        """
        # Use default implementations if not provided (still following DIP)
        self._config_provider = config_provider or EnvironmentConfigProvider()
        self._consumer = consumer or KafkaConsumerService(self._config_provider)
        self._ingestion_service = ingestion_service or DruidIngestionService(self._config_provider)
        
        # Application state
        self._running = False
        self._task_id: Optional[str] = None
        self._executor: Optional[ThreadPoolExecutor] = None
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        # Configure logging
        self._setup_logging()
    
    def _setup_logging(self) -> None:
        """Configure application logging."""
        log_level = self._config_provider.get_config_value('LOG_LEVEL', 'INFO')
        logging.basicConfig(
            level=getattr(logging, log_level.upper()),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
    
    def _signal_handler(self, signum: int, frame) -> None:
        """Handle shutdown signals gracefully."""
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.stop()
    
    def initialize(self) -> bool:
        """
        Initialize the application components.
        
        Returns:
            True if initialization successful, False otherwise
        """
        logger.info("Initializing Druid Pipeline Application...")
        
        # Check Druid health
        if not self._ingestion_service.health_check():
            logger.error("Druid health check failed")
            return False
        
        # Connect to Kafka
        if not self._consumer.connect():
            logger.error("Failed to connect to Kafka")
            return False
        
        logger.info("Application initialized successfully")
        return True
    
    def submit_ingestion_task(self) -> bool:
        """
        Submit Kafka ingestion task to Druid.
        
        Returns:
            True if task submission successful, False otherwise
        """
        try:
            logger.info("Creating and submitting Druid ingestion task...")
            
            # Create ingestion specification
            spec = self._ingestion_service.create_stock_ingestion_spec()
            
            # Submit task
            result = self._ingestion_service.submit_ingestion_task(spec)
            
            if result.get('success', True) and 'task' in result:
                self._task_id = result['task']
                logger.info(f"Ingestion task submitted successfully: {self._task_id}")
                return True
            else:
                logger.error(f"Task submission failed: {result}")
                return False
                
        except Exception as e:
            logger.error(f"Error submitting ingestion task: {e}")
            return False
    
    def monitor_ingestion(self, check_interval: int = 30) -> None:
        """
        Monitor the ingestion task status.
        
        Args:
            check_interval: Seconds between status checks
        """
        if not self._task_id:
            logger.warning("No task ID available for monitoring")
            return
        
        logger.info(f"Monitoring ingestion task: {self._task_id}")
        
        while self._running:
            try:
                status = self._ingestion_service.get_task_status(self._task_id)
                
                if status.get('success', True):
                    task_status = status.get('status', {})
                    state = task_status.get('status', 'UNKNOWN')
                    
                    logger.info(f"Task {self._task_id} status: {state}")
                    
                    if state in ['SUCCESS', 'FAILED']:
                        logger.info(f"Task completed with status: {state}")
                        if state == 'FAILED':
                            error_msg = task_status.get('errorMsg', 'Unknown error')
                            logger.error(f"Task failed: {error_msg}")
                        break
                else:
                    logger.error(f"Failed to get task status: {status}")
                
                time.sleep(check_interval)
                
            except Exception as e:
                logger.error(f"Error monitoring task: {e}")
                time.sleep(check_interval)
    
    def run(self, monitor_only: bool = False) -> None:
        """
        Run the application.
        
        Args:
            monitor_only: If True, only monitor existing tasks without consuming
        """
        self._running = True
        
        try:
            if not self.initialize():
                logger.error("Application initialization failed")
                return
            
            if not monitor_only:
                # Submit ingestion task
                if not self.submit_ingestion_task():
                    logger.error("Failed to submit ingestion task")
                    return
            
            # Start monitoring in a separate thread
            self._executor = ThreadPoolExecutor(max_workers=2)
            monitor_future = self._executor.submit(self.monitor_ingestion)
            
            if not monitor_only:
                # Start consuming messages (this will run until stopped)
                self._consume_messages()
            
            # Wait for monitoring to complete
            try:
                monitor_future.result(timeout=5)
            except FutureTimeoutError:
                logger.info("Monitor still running...")
            
        except KeyboardInterrupt:
            logger.info("Application interrupted by user")
        except Exception as e:
            logger.error(f"Application error: {e}")
        finally:
            self.stop()
    
    def _consume_messages(self) -> None:
        """
        Consume messages from Kafka.
        This method can be extended for additional processing.
        """
        logger.info("Starting message consumption...")
        
        try:
            message_count = 0
            for stock_record in self._consumer.consume_messages():
                if not self._running:
                    break
                
                message_count += 1
                logger.debug(f"Consumed message #{message_count}: {stock_record.ticker}")
                
                # Log periodic stats
                if message_count % 100 == 0:
                    logger.info(f"Processed {message_count} messages")
                    
        except Exception as e:
            logger.error(f"Error during message consumption: {e}")
    
    def stop(self) -> None:
        """Stop the application gracefully."""
        logger.info("Stopping Druid Pipeline Application...")
        self._running = False
        
        # Close consumer
        if self._consumer:
            self._consumer.close()
        
        # Shutdown executor
        if self._executor:
            self._executor.shutdown(wait=True, timeout=10)
        
        logger.info("Application stopped")
    
    def get_status(self) -> Dict[str, Any]:
        """
        Get application status information.
        
        Returns:
            Dict containing application status
        """
        status = {
            'running': self._running,
            'task_id': self._task_id,
            'consumer_info': self._consumer.get_consumer_info(),
            'druid_healthy': self._ingestion_service.health_check()
        }
        
        if self._task_id:
            task_status = self._ingestion_service.get_task_status(self._task_id)
            status['task_status'] = task_status
        
        return status


def main():
    """Main entry point for the application."""
    app = DruidPipelineApplication()
    
    # Check command line arguments
    import sys
    monitor_only = '--monitor-only' in sys.argv
    
    try:
        app.run(monitor_only=monitor_only)
    except Exception as e:
        logger.error(f"Application failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()