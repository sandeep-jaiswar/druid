"""
Ingestion service interface following Interface Segregation Principle.
ISP: Only methods related to data ingestion management.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from ..models.ingestion_spec import DruidIngestionSpec


class IIngestionService(ABC):
    """
    Interface for managing data ingestion tasks.
    ISP: Focused only on ingestion operations.
    """
    
    @abstractmethod
    def submit_ingestion_task(self, spec: DruidIngestionSpec) -> Dict[str, Any]:
        """
        Submit an ingestion task to the system.
        
        Args:
            spec: Ingestion specification
            
        Returns:
            Dict containing task submission response
        """
        pass
    
    @abstractmethod
    def get_task_status(self, task_id: str) -> Dict[str, Any]:
        """
        Get status of an ingestion task.
        
        Args:
            task_id: Unique task identifier
            
        Returns:
            Dict containing task status information
        """
        pass
    
    @abstractmethod
    def cancel_task(self, task_id: str) -> bool:
        """
        Cancel a running ingestion task.
        
        Args:
            task_id: Unique task identifier
            
        Returns:
            True if cancellation successful, False otherwise
        """
        pass
    
    @abstractmethod
    def list_active_tasks(self) -> Dict[str, Any]:
        """
        List all active ingestion tasks.
        
        Returns:
            Dict containing active tasks information
        """
        pass
    
    @abstractmethod
    def health_check(self) -> bool:
        """
        Check if ingestion service is healthy and accessible.
        
        Returns:
            True if service is healthy, False otherwise
        """
        pass