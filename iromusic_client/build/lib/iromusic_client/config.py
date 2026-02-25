"""
Configuration module for iromusic_client.

This module provides centralized configuration management through environment
variables and defaults for the API client, file handling, logging, and retry logic.

Configuration can be set via environment variables:
    IROMUSIC_API_BASE_URL: Base URL for the API (default: https://iromusicapp.ir)
    IROMUSIC_TIMEOUT: Request timeout in seconds (default: 30)
    IROMUSIC_MAX_RETRIES: Maximum retry attempts (default: 5)
    IROMUSIC_BACKOFF_FACTOR: Exponential backoff multiplier (default: 2)
    IROMUSIC_OUTPUT_DIR: Output directory for downloaded data (default: ./output)
    IROMUSIC_LOG_LEVEL: Logging level (default: INFO)
    IROMUSIC_CACHE_DIR: Directory for caching responses (default: ./cache)
    IROMUSIC_ENABLE_CACHE: Enable response caching (default: true)
    IROMUSIC_USER_AGENT: Custom User-Agent string
    IROMUSIC_PAGE_SIZE: Number of items per page (default: 20)

Classes:
    Config: Singleton configuration class with environment variable support.

Example:
    >>> from iromusic_client.config import Config
    >>> config = Config()
    >>> print(config.api_base_url)
    https://iromusicapp.ir
    >>> print(config.timeout)
    30
"""

import os
import logging
from typing import Optional
from pathlib import Path


class Config:
    """
    Singleton configuration class for iromusic_client.
    
    This class provides centralized access to all configuration settings,
    loading values from environment variables with sensible defaults.
    
    Attributes:
        api_base_url (str): Base URL for the iromusicapp.ir API.
        timeout (int): Request timeout in seconds.
        max_retries (int): Maximum number of retry attempts.
        backoff_factor (float): Exponential backoff multiplier.
        output_dir (Path): Directory for output files.
        log_level (str): Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
        cache_dir (Path): Directory for caching responses.
        enable_cache (bool): Whether to enable response caching.
        user_agent (str): Custom User-Agent header.
        page_size (int): Number of items per page for pagination.
    
    Example:
        >>> config = Config()
        >>> config.timeout = 60  # Modify timeout
        >>> print(f"API URL: {config.api_base_url}")
    """
    
    _instance: Optional['Config'] = None
    _initialized: bool = False
    
    def __new__(cls) -> 'Config':
        """
        Create or return the singleton Config instance.
        
        Returns:
            Config: The singleton configuration instance.
        """
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self) -> None:
        """
        Initialize configuration from environment variables and defaults.
        
        This method loads configuration values from environment variables
        with fallback to sensible default values.
        """
        if self._initialized:
            return
            
        # API Configuration
        self._api_base_url: str = os.environ.get(
            'IROMUSIC_API_BASE_URL', 
            'https://iromusicapp.ir'
        )
        
        # Request Configuration
        self._timeout: int = int(os.environ.get('IROMUSIC_TIMEOUT', '30'))
        self._max_retries: int = int(os.environ.get('IROMUSIC_MAX_RETRIES', '5'))
        self._backoff_factor: float = float(os.environ.get('IROMUSIC_BACKOFF_FACTOR', '2'))
        
        # Output Configuration
        self._output_dir: Path = Path(os.environ.get('IROMUSIC_OUTPUT_DIR', './output'))
        
        # Logging Configuration
        self._log_level: str = os.environ.get('IROMUSIC_LOG_LEVEL', 'INFO')
        
        # Cache Configuration
        self._cache_dir: Path = Path(os.environ.get('IROMUSIC_CACHE_DIR', './cache'))
        self._enable_cache: bool = os.environ.get('IROMUSIC_ENABLE_CACHE', 'true').lower() == 'true'
        
        # User Agent
        self._user_agent: str = os.environ.get(
            'IROMUSIC_USER_AGENT',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        
        # Pagination
        self._page_size: int = int(os.environ.get('IROMUSIC_PAGE_SIZE', '20'))
        
        self._initialized = True
    
    @property
    def api_base_url(self) -> str:
        """
        Get the base URL for the API.
        
        Returns:
            str: The API base URL.
        """
        return self._api_base_url
    
    @api_base_url.setter
    def api_base_url(self, value: str) -> None:
        """
        Set the base URL for the API.
        
        Args:
            value: The new API base URL.
        """
        self._api_base_url = value
    
    @property
    def timeout(self) -> int:
        """
        Get the request timeout in seconds.
        
        Returns:
            int: Timeout in seconds.
        """
        return self._timeout
    
    @timeout.setter
    def timeout(self, value: int) -> None:
        """
        Set the request timeout.
        
        Args:
            value: Timeout value in seconds (must be positive).
        """
        if value <= 0:
            raise ValueError("Timeout must be a positive integer")
        self._timeout = value
    
    @property
    def max_retries(self) -> int:
        """
        Get the maximum number of retry attempts.
        
        Returns:
            int: Maximum retry count.
        """
        return self._max_retries
    
    @max_retries.setter
    def max_retries(self, value: int) -> None:
        """
        Set the maximum number of retry attempts.
        
        Args:
            value: Maximum retry count (must be non-negative).
        """
        if value < 0:
            raise ValueError("Max retries must be non-negative")
        self._max_retries = value
    
    @property
    def backoff_factor(self) -> float:
        """
        Get the exponential backoff multiplier.
        
        Returns:
            float: Backoff factor for exponential retry delay.
        """
        return self._backoff_factor
    
    @backoff_factor.setter
    def backoff_factor(self, value: float) -> None:
        """
        Set the exponential backoff multiplier.
        
        Args:
            value: Backoff factor (must be positive).
        """
        if value <= 0:
            raise ValueError("Backoff factor must be positive")
        self._backoff_factor = value
    
    @property
    def output_dir(self) -> Path:
        """
        Get the output directory path.
        
        Returns:
            Path: Output directory path.
        """
        return self._output_dir
    
    @output_dir.setter
    def output_dir(self, value: str) -> None:
        """
        Set the output directory.
        
        Args:
            value: Path string for the output directory.
        """
        self._output_dir = Path(value)
    
    @property
    def log_level(self) -> str:
        """
        Get the logging level.
        
        Returns:
            str: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
        """
        return self._log_level
    
    @log_level.setter
    def log_level(self, value: str) -> None:
        """
        Set the logging level.
        
        Args:
            value: Logging level string.
        """
        valid_levels = {'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'}
        if value.upper() not in valid_levels:
            raise ValueError(f"Invalid log level. Must be one of: {valid_levels}")
        self._log_level = value.upper()
    
    @property
    def cache_dir(self) -> Path:
        """
        Get the cache directory path.
        
        Returns:
            Path: Cache directory path.
        """
        return self._cache_dir
    
    @cache_dir.setter
    def cache_dir(self, value: str) -> None:
        """
        Set the cache directory.
        
        Args:
            value: Path string for the cache directory.
        """
        self._cache_dir = Path(value)
    
    @property
    def enable_cache(self) -> bool:
        """
        Check if caching is enabled.
        
        Returns:
            bool: True if caching is enabled, False otherwise.
        """
        return self._enable_cache
    
    @enable_cache.setter
    def enable_cache(self, value: bool) -> None:
        """
        Enable or disable caching.
        
        Args:
            value: Boolean to enable/disable caching.
        """
        self._enable_cache = value
    
    @property
    def user_agent(self) -> str:
        """
        Get the User-Agent header string.
        
        Returns:
            str: User-Agent string.
        """
        return self._user_agent
    
    @user_agent.setter
    def user_agent(self, value: str) -> None:
        """
        Set the User-Agent header string.
        
        Args:
            value: Custom User-Agent string.
        """
        self._user_agent = value
    
    @property
    def page_size(self) -> int:
        """
        Get the number of items per page.
        
        Returns:
            int: Page size for pagination.
        """
        return self._page_size
    
    @page_size.setter
    def page_size(self, value: int) -> None:
        """
        Set the number of items per page.
        
        Args:
            value: Page size (must be positive).
        """
        if value <= 0:
            raise ValueError("Page size must be positive")
        self._page_size = value
    
    def get_movie_posts_url(self, content_type: Optional[str] = None) -> str:
        """
        Get the full URL for movie/series posts endpoint.
        
        Args:
            content_type: Optional content type ('movies' or 'series').
        
        Returns:
            str: Complete URL for the endpoint.
        
        Example:
            >>> config = Config()
            >>> config.get_movie_posts_url()
            'https://iromusicapp.ir/iroapi/movie/posts'
            >>> config.get_movie_posts_url('movies')
            'https://iromusicapp.ir/iroapi/movie/posts?type=movies'
        """
        url = f"{self._api_base_url}/iroapi/movie/posts"
        if content_type:
            url += f"?type={content_type}"
        return url
    
    def get_music_posts_url(self, content_type: str = 'albums') -> str:
        """
        Get the full URL for music posts endpoint.
        
        Args:
            content_type: Content type ('albums' or 'singles').
        
        Returns:
            str: Complete URL for the endpoint.
        
        Example:
            >>> config = Config()
            >>> config.get_music_posts_url('albums')
            'https://iromusicapp.ir/iroapi/music/posts?type=albums'
        """
        return f"{self._api_base_url}/iroapi/music/posts?type={content_type}"
    
    def get_log_level_int(self) -> int:
        """
        Get the logging level as an integer.
        
        Returns:
            int: Logging level (e.g., logging.DEBUG, logging.INFO).
        """
        level_map = {
            'DEBUG': logging.DEBUG,
            'INFO': logging.INFO,
            'WARNING': logging.WARNING,
            'ERROR': logging.ERROR,
            'CRITICAL': logging.CRITICAL
        }
        return level_map.get(self._log_level, logging.INFO)
    
    def reset(self) -> None:
        """
        Reset the singleton instance.
        
        This method clears the singleton instance, allowing reinitialization
        with new environment variables. Primarily useful for testing.
        """
        Config._instance = None
        Config._initialized = False


def get_config() -> Config:
    """
    Get the global configuration instance.
    
    This is a convenience function that returns the singleton Config instance.
    
    Returns:
        Config: The global configuration instance.
    
    Example:
        >>> config = get_config()
        >>> print(config.api_base_url)
    """
    return Config()
