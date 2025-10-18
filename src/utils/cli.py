"""
Command Line Interface for Druid Pipeline Application.
SRP: Only responsible for CLI interactions and command parsing.
"""

import argparse
import json
import sys
import logging
from typing import Optional

from ..druid_pipeline_app import DruidPipelineApplication
from ..config.environment_config import EnvironmentConfigProvider


logger = logging.getLogger(__name__)


class DruidPipelineCLI:
    """
    Command Line Interface for the Druid Pipeline Application.
    SRP: Handles only CLI-related functionality.
    """
    
    def __init__(self):
        """Initialize CLI with argument parser."""
        self.app: Optional[DruidPipelineApplication] = None
        self.parser = self._create_parser()
    
    def _create_parser(self) -> argparse.ArgumentParser:
        """Create and configure argument parser."""
        parser = argparse.ArgumentParser(
            description='Druid Pipeline Application - Kafka to Druid Data Ingestion',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="""
Examples:
  %(prog)s run                     # Start full pipeline
  %(prog)s run --monitor-only      # Monitor existing tasks only
  %(prog)s submit                  # Submit ingestion task only
  %(prog)s status                  # Show application status
  %(prog)s list-tasks             # List active Druid tasks
  %(prog)s cancel --task-id TASK   # Cancel specific task
            """
        )
        
        # Global options
        parser.add_argument(
            '--log-level',
            choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
            default='INFO',
            help='Set logging level (default: INFO)'
        )
        
        parser.add_argument(
            '--config',
            help='Configuration file path (not implemented yet)'
        )
        
        # Subcommands
        subparsers = parser.add_subparsers(dest='command', help='Available commands')
        
        # Run command
        run_parser = subparsers.add_parser('run', help='Run the pipeline application')
        run_parser.add_argument(
            '--monitor-only',
            action='store_true',
            help='Only monitor existing tasks, do not start new ingestion'
        )
        
        # Submit command
        submit_parser = subparsers.add_parser('submit', help='Submit ingestion task to Druid')
        
        # Status command
        status_parser = subparsers.add_parser('status', help='Show application and task status')
        
        # List tasks command
        list_parser = subparsers.add_parser('list-tasks', help='List active Druid ingestion tasks')
        
        # Cancel command
        cancel_parser = subparsers.add_parser('cancel', help='Cancel ingestion task')
        cancel_parser.add_argument(
            '--task-id',
            required=True,
            help='Task ID to cancel'
        )
        
        return parser
    
    def run(self, args: Optional[list] = None) -> int:
        """
        Run CLI with provided arguments.
        
        Args:
            args: Command line arguments (defaults to sys.argv[1:])
            
        Returns:
            Exit code (0 for success, non-zero for error)
        """
        try:
            parsed_args = self.parser.parse_args(args)
            
            # Configure logging
            logging.basicConfig(
                level=getattr(logging, parsed_args.log_level),
                format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            
            # Initialize application
            self.app = DruidPipelineApplication()
            
            # Execute command
            if parsed_args.command == 'run':
                return self._run_pipeline(parsed_args.monitor_only)
            elif parsed_args.command == 'submit':
                return self._submit_task()
            elif parsed_args.command == 'status':
                return self._show_status()
            elif parsed_args.command == 'list-tasks':
                return self._list_tasks()
            elif parsed_args.command == 'cancel':
                return self._cancel_task(parsed_args.task_id)
            else:
                self.parser.print_help()
                return 1
                
        except KeyboardInterrupt:
            logger.info("Operation cancelled by user")
            return 130
        except Exception as e:
            logger.error(f"CLI error: {e}")
            return 1
    
    def _run_pipeline(self, monitor_only: bool = False) -> int:
        """Run the full pipeline application."""
        try:
            logger.info("Starting Druid Pipeline Application...")
            self.app.run(monitor_only=monitor_only)
            return 0
        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}")
            return 1
    
    def _submit_task(self) -> int:
        """Submit ingestion task to Druid."""
        try:
            if not self.app.initialize():
                logger.error("Application initialization failed")
                return 1
            
            if self.app.submit_ingestion_task():
                logger.info("Ingestion task submitted successfully")
                return 0
            else:
                logger.error("Failed to submit ingestion task")
                return 1
                
        except Exception as e:
            logger.error(f"Task submission failed: {e}")
            return 1
    
    def _show_status(self) -> int:
        """Show application and task status."""
        try:
            if not self.app.initialize():
                logger.error("Application initialization failed")
                return 1
            
            status = self.app.get_status()
            print(json.dumps(status, indent=2))
            return 0
            
        except Exception as e:
            logger.error(f"Status check failed: {e}")
            return 1
    
    def _list_tasks(self) -> int:
        """List active Druid ingestion tasks."""
        try:
            config_provider = EnvironmentConfigProvider()
            from ..services.druid_ingestion_service import DruidIngestionService
            ingestion_service = DruidIngestionService(config_provider)
            
            if not ingestion_service.health_check():
                logger.error("Druid service is not healthy")
                return 1
            
            tasks = ingestion_service.list_active_tasks()
            print(json.dumps(tasks, indent=2))
            return 0
            
        except Exception as e:
            logger.error(f"Failed to list tasks: {e}")
            return 1
    
    def _cancel_task(self, task_id: str) -> int:
        """Cancel a specific ingestion task."""
        try:
            config_provider = EnvironmentConfigProvider()
            from ..services.druid_ingestion_service import DruidIngestionService
            ingestion_service = DruidIngestionService(config_provider)
            
            if ingestion_service.cancel_task(task_id):
                logger.info(f"Task {task_id} cancelled successfully")
                return 0
            else:
                logger.error(f"Failed to cancel task {task_id}")
                return 1
                
        except Exception as e:
            logger.error(f"Task cancellation failed: {e}")
            return 1


def main():
    """Main CLI entry point."""
    cli = DruidPipelineCLI()
    exit_code = cli.run()
    sys.exit(exit_code)


if __name__ == '__main__':
    main()