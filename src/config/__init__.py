"""
Configuration package for centralized settings management.
Follows Dependency Inversion Principle - provides abstractions for configuration.
"""

from .environment_config import EnvironmentConfigProvider

__all__ = [
    'EnvironmentConfigProvider'
]