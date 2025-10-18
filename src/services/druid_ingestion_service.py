"""
Druid ingestion service implementation.
SRP: Only responsible for managing Druid ingestion tasks.
DIP: Depends on IConfigProvider abstraction.
"""

import json
import logging
import requests
from typing import Dict, Any
from requests.exceptions import RequestException, ConnectionError, Timeout

from ..interfaces.ingestion_service import IIngestionService
from ..interfaces.config_provider import IConfigProvider
from ..models.ingestion_spec import DruidIngestionSpec, DataSchema, KafkaIOConfig, TuningConfig


logger = logging.getLogger(__name__)


class DruidIngestionService(IIngestionService):
    """
    Druid ingestion service implementation following SOLID principles.
    SRP: Only handles Druid ingestion task management.
    DIP: Depends on IConfigProvider interface.
    """
    
    def __init__(self, config_provider: IConfigProvider):
        """
        Initialize Druid ingestion service with dependency injection.
        
        Args:
            config_provider: Configuration provider implementation
        """
        self._config_provider = config_provider
        self._druid_config = config_provider.get_druid_config()
        self._session = requests.Session()
        self._session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })
    
    def submit_ingestion_task(self, spec: DruidIngestionSpec) -> Dict[str, Any]:
        """
        Submit an ingestion task to Druid Overlord.
        
        Args:
            spec: Druid ingestion specification
            
        Returns:
            Dict containing task submission response
        """
        try:
            overlord_url = self._druid_config['overlord_url']
            submit_url = f"{overlord_url}/druid/indexer/v1/task"
            
            payload = spec.to_dict()
            logger.info(f"Submitting ingestion task to {submit_url}")
            logger.debug(f"Task payload: {json.dumps(payload, indent=2)}")
            
            response = self._session.post(
                submit_url,
                json=payload,
                timeout=30
            )
            
            response.raise_for_status()
            result = response.json()
            
            logger.info(f"Task submitted successfully. Task ID: {result.get('task')}")
            return result
            
        except ConnectionError as e:
            logger.error(f"Failed to connect to Druid Overlord: {e}")
            return {'error': f'Connection failed: {e}', 'success': False}
        except Timeout as e:
            logger.error(f"Timeout submitting task to Druid: {e}")
            return {'error': f'Request timeout: {e}', 'success': False}
        except RequestException as e:
            logger.error(f"HTTP error submitting task: {e}")
            if hasattr(e, 'response') and e.response is not None:
                try:
                    error_detail = e.response.json()
                    return {'error': error_detail, 'success': False}
                except json.JSONDecodeError:
                    return {'error': e.response.text, 'success': False}
            return {'error': str(e), 'success': False}
        except Exception as e:
            logger.error(f"Unexpected error submitting task: {e}")
            return {'error': str(e), 'success': False}
    
    def get_task_status(self, task_id: str) -> Dict[str, Any]:
        """
        Get status of a Druid ingestion task.
        
        Args:
            task_id: Unique task identifier
            
        Returns:
            Dict containing task status information
        """
        try:
            overlord_url = self._druid_config['overlord_url']
            status_url = f"{overlord_url}/druid/indexer/v1/task/{task_id}/status"
            
            response = self._session.get(status_url, timeout=10)
            response.raise_for_status()
            
            return response.json()
            
        except Exception as e:
            logger.error(f"Error getting task status for {task_id}: {e}")
            return {'error': str(e), 'success': False}
    
    def cancel_task(self, task_id: str) -> bool:
        """
        Cancel a running Druid ingestion task.
        
        Args:
            task_id: Unique task identifier
            
        Returns:
            True if cancellation successful, False otherwise
        """
        try:
            overlord_url = self._druid_config['overlord_url']
            cancel_url = f"{overlord_url}/druid/indexer/v1/task/{task_id}/shutdown"
            
            response = self._session.post(cancel_url, timeout=10)
            response.raise_for_status()
            
            result = response.json()
            success = result.get('task') == task_id
            
            if success:
                logger.info(f"Task {task_id} cancelled successfully")
            else:
                logger.warning(f"Task {task_id} cancellation may have failed: {result}")
            
            return success
            
        except Exception as e:
            logger.error(f"Error cancelling task {task_id}: {e}")
            return False
    
    def list_active_tasks(self) -> Dict[str, Any]:
        """
        List all active Druid ingestion tasks.
        
        Returns:
            Dict containing active tasks information
        """
        try:
            overlord_url = self._druid_config['overlord_url']
            tasks_url = f"{overlord_url}/druid/indexer/v1/tasks"
            
            response = self._session.get(tasks_url, timeout=10)
            response.raise_for_status()
            
            return response.json()
            
        except Exception as e:
            logger.error(f"Error listing active tasks: {e}")
            return {'error': str(e), 'success': False}
    
    def health_check(self) -> bool:
        """
        Check if Druid services are healthy and accessible.
        
        Returns:
            True if services are healthy, False otherwise
        """
        try:
            # Check Overlord health
            overlord_url = self._druid_config['overlord_url']
            health_url = f"{overlord_url}/status/health"
            
            response = self._session.get(health_url, timeout=5)
            overlord_healthy = response.status_code == 200
            
            if overlord_healthy:
                logger.info("Druid Overlord is healthy")
            else:
                logger.warning(f"Druid Overlord health check failed: {response.status_code}")
            
            return overlord_healthy
            
        except Exception as e:
            logger.error(f"Druid health check failed: {e}")
            return False
    
    def create_stock_ingestion_spec(self) -> DruidIngestionSpec:
        """
        Create a Druid ingestion specification for stock data.
        
        Returns:
            DruidIngestionSpec configured for stock data ingestion
        """
        druid_config = self._druid_config
        kafka_config = self._config_provider.get_kafka_config()
        ingestion_config = self._config_provider.get_ingestion_config()
        
        # Create data schema
        data_schema = DataSchema(
            dataSource=druid_config['datasource']
        )
        
        # Create Kafka IO config  
        kafka_io_config = KafkaIOConfig(
            topic=kafka_config['topic'],
            taskCount=ingestion_config['task_count'],
            replicas=ingestion_config['replicas'],
            taskDuration=ingestion_config['task_duration']
        )
        
        # Create tuning config
        tuning_config = TuningConfig(
            maxRowsInMemory=ingestion_config['max_rows_in_memory']
        )
        
        return DruidIngestionSpec(
            dataSchema=data_schema,
            ioConfig=kafka_io_config,
            tuningConfig=tuning_config
        )