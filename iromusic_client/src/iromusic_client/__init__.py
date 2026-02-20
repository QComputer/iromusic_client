"""
iromusic_client - Python client for iromusicapp.ir API.

A comprehensive Python library for fetching and processing data from the
iromusicapp.ir API endpoints. Supports movie, series, and music content
with pagination, caching, and graceful error handling.

Main Components:
    - API Client: HTTP communication with retry logic
    - Data Processor: JSON validation and normalization
    - File Handler: Organized output with timestamps
    - Orchestrator: Coordinated workflow management
    - CLI: Command-line interface

Example:
    >>> from iromusic_client import Orchestrator
    >>> orchestrator = Orchestrator()
    >>> results = orchestrator.run_endpoint('movie', 'movies')
"""

__version__ = '1.0.0'
__author__ = 'iromusic_client'
__license__ = 'MIT'

# Import main components for easy access
from .config import Config, get_config
from .api_client import APIClient, create_client, APIError, RateLimitError
from .data_processor import DataProcessor, ValidationError
from .file_handler import FileHandler, FileWriteError
from .orchestrator import Orchestrator, run_orchestrator

# Default configuration
__all__ = [
    # Config
    'Config',
    'get_config',
    
    # API Client
    'APIClient',
    'create_client',
    'APIError',
    'RateLimitError',
    
    # Data Processor
    'DataProcessor',
    'ValidationError',
    
    # File Handler
    'FileHandler',
    'FileWriteError',
    
    # Orchestrator
    'Orchestrator',
    'run_orchestrator',
]
