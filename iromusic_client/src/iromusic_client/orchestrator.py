"""
Orchestration module for iromusic_client.

This module provides the main orchestration layer that coordinates API calls,
data processing, and file output. It manages pagination, tracks statistics,
and provides colored console output for monitoring progress.

Classes:
    Orchestrator: Main orchestration class for coordinating all operations.
    ConsoleColors: ANSI color codes for colored output.
    RunStats: Statistics for a complete run.

Functions:
    run_orchestrator: Main entry point for running the orchestration.
    print_colored: Print colored text to console.

Example:
    >>> from iromusic_client.orchestrator import Orchestrator
    >>> orchestrator = Orchestrator()
    >>> results = orchestrator.run_all_endpoints()
"""

import sys
import time
import logging
from typing import Optional, Dict, Any, List, Set
from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum

from .config import Config, get_config
from .api_client import APIClient, create_client, RateLimitError, APIError, RequestStats
from .data_processor import DataProcessor, ValidationError
from .file_handler import FileHandler, create_file_handler


# Module logger
logger = logging.getLogger(__name__)


class ConsoleColors:
    """
    ANSI escape codes for colored console output.
    
    This class provides static attributes for ANSI color codes
    that work across different terminals.
    
    Example:
        >>> print(f"{ConsoleColors.GREEN}Success!{ConsoleColors.RESET}")
    """
    
    # Reset
    RESET = '\033[0m'
    
    # Regular colors
    BLACK = '\033[30m'
    RED = '\033[31m'
    GREEN = '\033[32m'
    YELLOW = '\033[33m'
    BLUE = '\033[34m'
    MAGENTA = '\033[35m'
    CYAN = '\033[36m'
    WHITE = '\033[37m'
    
    # Bold colors
    BOLD_RED = '\033[1;31m'
    BOLD_GREEN = '\033[1;32m'
    BOLD_YELLOW = '\033[1;33m'
    BOLD_BLUE = '\033[1;34m'
    
    # Background colors
    BG_RED = '\033[41m'
    BG_GREEN = '\033[42m'
    BG_YELLOW = '\033[43m'
    
    # Styles
    DIM = '\033[2m'
    ITALIC = '\033[3m'
    UNDERLINE = '\033[4m'
    
    # Status indicators
    SUCCESS = GREEN
    ERROR = RED
    WARNING = YELLOW
    INFO = CYAN
    PROGRESS = MAGENTA


class OutputLevel(Enum):
    """Output verbosity levels."""
    QUIET = 0
    NORMAL = 1
    VERBOSE = 2
    DEBUG = 3


@dataclass
class RunStats:
    """
    Statistics for a complete orchestration run.
    
    Tracks overall metrics including endpoints processed,
    total items fetched, timing information, and success rates.
    
    Attributes:
        start_time (datetime): When the run started.
        end_time (Optional[datetime]): When the run ended.
        endpoints_processed (int): Number of endpoints processed.
        total_items (int): Total items fetched.
        successful_endpoints (int): Endpoints that completed successfully.
        failed_endpoints (int): Endpoints that failed.
        total_duration (float): Total duration in seconds.
        api_stats (RequestStats): Aggregated API statistics.
    
    Example:
        >>> stats = RunStats()
        >>> stats.successful_endpoints += 1
    """
    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None
    endpoints_processed: int = 0
    total_items: int = 0
    successful_endpoints: int = 0
    failed_endpoints: int = 0
    total_duration: float = 0.0
    api_stats: RequestStats = field(default_factory=RequestStats)
    
    def finalize(self) -> None:
        """Mark the run as complete."""
        self.end_time = datetime.now()
        self.total_duration = (self.end_time - self.start_time).total_seconds()
    
    def get_summary(self) -> Dict[str, Any]:
        """
        Get a summary dictionary of the run statistics.
        
        Returns:
            Dictionary containing run summary.
        """
        return {
            'start_time': self.start_time.isoformat(),
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'duration_seconds': self.total_duration,
            'endpoints_processed': self.endpoints_processed,
            'successful_endpoints': self.successful_endpoints,
            'failed_endpoints': self.failed_endpoints,
            'total_items': self.total_items,
            'total_api_requests': self.api_stats.total_requests,
            'successful_requests': self.api_stats.successful_requests,
            'failed_requests': self.api_stats.failed_requests,
            'total_retries': self.api_stats.total_retries,
            'success_rate': self.api_stats.get_success_rate()
        }


class Orchestrator:
    """
    Main orchestration class for coordinating API operations.
    
    This class manages the complete workflow: making API requests,
    processing responses, validating data, and saving to files.
    It handles pagination, tracks statistics, and provides
    colored console output for monitoring.
    
    Attributes:
        config (Config): Configuration settings.
        client (APIClient): API client for HTTP requests.
        processor (DataProcessor): Data processor for validation/transformation.
        file_handler (FileHandler): Handler for file operations.
        stats (RunStats): Statistics for this run.
        output_level (OutputLevel): Verbosity level for console output.
    
    Example:
        >>> orchestrator = Orchestrator()
        >>> results = orchestrator.run_endpoint('movie', 'movies')
    """
    
    # Define available endpoints
    ENDPOINTS = {
        'movie': {
            'base_url': '/iroapi/movie/posts',
            'types': [None, 'movies', 'series'],
            'name': 'Movie/Series'
        },
        'music': {
            'base_url': '/iroapi/music/posts',
            'types': ['albums', 'singles'],
            'name': 'Music'
        }
    }
    
    def __init__(
        self,
        config: Optional[Config] = None,
        output_level: OutputLevel = OutputLevel.NORMAL
    ) -> None:
        """
        Initialize the Orchestrator.
        
        Args:
            config: Optional configuration object.
            output_level: Verbosity level for console output.
        """
        self.config = config or get_config()
        self.client = create_client(self.config)
        self.processor = DataProcessor()
        self.file_handler = create_file_handler(config=self.config)
        self.stats = RunStats()
        self.output_level = output_level
    
    def print_status(
        self,
        message: str,
        level: str = 'info',
        bold: bool = False
    ) -> None:
        """
        Print a colored status message to the console.
        
        Args:
            message: Message to print.
            level: Status level ('success', 'error', 'warning', 'info', 'progress').
            bold: Whether to use bold formatting.
        """
        if self.output_level == OutputLevel.QUIET:
            return
        
        colors = {
            'success': ConsoleColors.SUCCESS,
            'error': ConsoleColors.ERROR,
            'warning': ConsoleColors.WARNING,
            'info': ConsoleColors.INFO,
            'progress': ConsoleColors.PROGRESS
        }
        
        prefix = {
            'success': '✓',
            'error': '✗',
            'warning': '⚠',
            'info': 'ℹ',
            'progress': '→'
        }
        
        color = colors.get(level, ConsoleColors.WHITE)
        symbol = prefix.get(level, '•')
        
        if bold:
            color = ConsoleColors.BOLD_RED if level == 'error' else color.replace('\033', '\033[1;')
        
        # Check if terminal supports colors
        if sys.stdout.isatty():
            print(f"{color}{symbol} {message}{ConsoleColors.RESET}")
        else:
            print(f"{symbol} {message}")
    
    def print_progress(
        self,
        current: int,
        total: int,
        prefix: str = ''
    ) -> None:
        """
        Print a progress indicator.
        
        Args:
            current: Current item number.
            total: Total items.
            prefix: Prefix text for the progress line.
        """
        if self.output_level < OutputLevel.VERBOSE:
            return
        
        percentage = (current / total * 100) if total > 0 else 0
        bar_length = 30
        filled = int(bar_length * current / total) if total > 0 else 0
        bar = '█' * filled + '░' * (bar_length - filled)
        
        print(f"\r{prefix} |{bar}| {current}/{total} ({percentage:.1f}%)", end='')
        
        if current >= total:
            print()
    
    def run_endpoint(
        self,
        endpoint: str,
        content_type: Optional[str] = None,
        max_pages: Optional[int] = None,
        save: bool = True
    ) -> Dict[str, Any]:
        """
        Run API requests for a specific endpoint and content type.
        
        Args:
            endpoint: Endpoint category ('movie' or 'music').
            content_type: Content type (e.g., 'movies', 'series', 'albums', 'singles').
            max_pages: Maximum number of pages to fetch (None for unlimited).
            save: Whether to save results to file.
        
        Returns:
            Dictionary containing results and statistics.
        
        Raises:
            ValueError: If endpoint is invalid.
        
        Example:
            >>> orchestrator = Orchestrator()
            >>> results = orchestrator.run_endpoint('movie', 'movies', max_pages=5)
        """
        self.stats.endpoints_processed += 1
        
        # Get endpoint config
        if endpoint not in self.ENDPOINTS:
            raise ValueError(f"Invalid endpoint: {endpoint}")
        
        endpoint_config = self.ENDPOINTS[endpoint]
        endpoint_name = f"{endpoint_config['name']} ({content_type or 'all'})"
        
        self.print_status(f"Fetching {endpoint_name}...", 'progress')
        
        result = {
            'endpoint': endpoint,
            'content_type': content_type,
            'items': [],
            'success': False,
            'error': None,
            'pages_fetched': 0,
            'items_count': 0,
            'duration': 0.0
        }
        
        start_time = time.time()
        
        try:
            # Build URL
            if endpoint == 'movie':
                url = self.config.get_movie_posts_url(content_type)
            else:
                url = self.config.get_music_posts_url(content_type or 'albums')
            
            # Fetch data with pagination
            items = self.client.get_with_pagination(
                url=url,
                page_param='page',
                max_pages=max_pages
            )
            
            result['items'] = items
            result['pages_fetched'] = len(items) // self.config.page_size + 1 if items else 0
            result['items_count'] = len(items)
            result['success'] = True
            
            self.print_status(
                f"Fetched {len(items)} {endpoint_name} items",
                'success'
            )
            
            # Process data
            if items:
                processed = self.processor.process_response(items)
                result['processed_data'] = processed.get('data', items)
            
            # Save to file
            if save and items:
                filename = f"{endpoint}_{content_type or 'all'}.json"
                subdir = f"{endpoint}"
                
                filepath = self.file_handler.save_json(
                    data=result.get('processed_data', items),
                    filename=filename,
                    subdirectory=subdir
                )
                
                result['filepath'] = str(filepath)
                self.print_status(
                    f"Saved to: {filepath}",
                    'info'
                )
            
            self.stats.successful_endpoints += 1
            self.stats.total_items += len(items)
            
        except (APIError, RateLimitError) as e:
            result['error'] = str(e)
            result['success'] = False
            
            self.print_status(
                f"API Error for {endpoint_name}: {e}",
                'error'
            )
            
            # Check for cached data
            if self.config.enable_cache and self.client.cache.get_cached_urls():
                self.print_status(
                    "Using cached data as fallback",
                    'warning'
                )
                result['used_cache'] = True
            
            self.stats.failed_endpoints += 1
            
        except ValidationError as e:
            result['error'] = str(e)
            result['success'] = False
            
            self.print_status(
                f"Validation Error for {endpoint_name}: {e}",
                'error'
            )
            
            self.stats.failed_endpoints += 1
            
        except Exception as e:
            result['error'] = str(e)
            result['success'] = False
            
            self.print_status(
                f"Unexpected Error for {endpoint_name}: {e}",
                'error'
            )
            
            self.stats.failed_endpoints += 1
        
        finally:
            result['duration'] = time.time() - start_time
        
        return result
    
    def run_all_endpoints(
        self,
        endpoints: Optional[List[str]] = None,
        content_types: Optional[Dict[str, List[str]]] = None,
        max_pages: Optional[int] = None,
        save: bool = True
    ) -> Dict[str, Any]:
        """
        Run API requests for all configured endpoints.
        
        This is the main entry point for fetching data from all endpoints.
        It coordinates requests across multiple endpoints and content types.
        
        Args:
            endpoints: List of endpoints to process (default: all).
            content_types: Dict mapping endpoints to content types to fetch.
            max_pages: Maximum pages per endpoint.
            save: Whether to save results to files.
        
        Returns:
            Dictionary containing results for all endpoints.
        
        Example:
            >>> orchestrator = Orchestrator()
            >>> results = orchestrator.run_all_endpoints(
            ...     max_pages=10,
            ...     save=True
            ... )
        """
        # Default content types for each endpoint
        default_content_types = {
            'movie': [None, 'movies', 'series'],
            'music': ['albums', 'singles']
        }
        
        content_types = content_types or default_content_types
        endpoints = endpoints or list(self.ENDPOINTS.keys())
        
        self.print_status("=" * 50, 'info')
        self.print_status("Starting iromusic data fetch", 'info', bold=True)
        self.print_status("=" * 50, 'info')
        
        results: Dict[str, Any] = {
            'endpoints': {},
            'overall_stats': {}
        }
        
        start_time = time.time()
        
        try:
            for endpoint in endpoints:
                if endpoint not in self.ENDPOINTS:
                    self.print_status(
                        f"Skipping unknown endpoint: {endpoint}",
                        'warning'
                    )
                    continue
                
                types_to_fetch = content_types.get(endpoint, [None])
                
                for content_type in types_to_fetch:
                    type_name = content_type or 'all'
                    
                    self.print_status(
                        f"\nProcessing {endpoint}/{type_name}...",
                        'progress',
                        bold=True
                    )
                    
                    result = self.run_endpoint(
                        endpoint=endpoint,
                        content_type=content_type,
                        max_pages=max_pages,
                        save=save
                    )
                    
                    results['endpoints'][f"{endpoint}_{type_name}"] = result
                    
                    # Brief pause between requests
                    if content_types and len(types_to_fetch) > 1:
                        time.sleep(0.5)
            
        finally:
            # Finalize and collect stats
            self.stats.finalize()
            self.stats.api_stats = self.client.stats
            
            # Get file handler stats
            file_stats = self.file_handler.get_stats()
            
            results['overall_stats'] = {
                'run': self.stats.get_summary(),
                'file': file_stats,
                'duration': time.time() - start_time
            }
        
        # Print summary
        self._print_summary(results)
        
        return results
    
    def _print_summary(self, results: Dict[str, Any]) -> None:
        """
        Print a summary of the run results.
        
        Args:
            results: Results dictionary from run_all_endpoints.
        """
        self.print_status("\n" + "=" * 50, 'info')
        self.print_status("Run Complete", 'info', bold=True)
        self.print_status("=" * 50, 'info')
        
        stats = results.get('overall_stats', {})
        run_stats = stats.get('run', {})
        
        # Endpoint results
        endpoints = results.get('endpoints', {})
        successful = sum(1 for r in endpoints.values() if r.get('success'))
        total = len(endpoints)
        
        self.print_status(
            f"Endpoints: {successful}/{total} successful",
            'success' if successful == total else 'warning'
        )
        
        # Items fetched
        total_items = sum(r.get('items_count', 0) for r in endpoints.values())
        self.print_status(f"Total items: {total_items}", 'info')
        
        # API stats
        api_stats = run_stats.get('api_stats', {})
        self.print_status(
            f"API requests: {api_stats.get('successful_requests', 0)}/"
            f"{api_stats.get('total_requests', 0)} successful",
            'info'
        )
        
        if api_stats.get('total_retries', 0) > 0:
            self.print_status(
                f"Total retries: {api_stats.get('total_retries', 0)}",
                'warning'
            )
        
        # File stats
        file_stats = stats.get('file', {})
        files_written = file_stats.get('files_written', 0)
        
        if files_written > 0:
            self.print_status(
                f"Files written: {files_written}",
                'success'
            )
        
        # Duration
        duration = stats.get('duration', 0)
        self.print_status(
            f"Total duration: {duration:.2f}s",
            'info'
        )
    
    def close(self) -> None:
        """Clean up resources."""
        self.client.close()
        logger.info("Orchestrator closed")


def run_orchestrator(
    endpoints: Optional[List[str]] = None,
    content_types: Optional[Dict[str, List[str]]] = None,
    max_pages: Optional[int] = None,
    output_dir: Optional[str] = None,
    log_level: str = 'INFO',
    output_level: str = 'NORMAL'
) -> Dict[str, Any]:
    """
    Main entry point for running the orchestrator.
    
    This function creates and runs the orchestrator with the given parameters.
    
    Args:
        endpoints: List of endpoints to process.
        content_types: Dict of content types per endpoint.
        max_pages: Maximum pages per endpoint.
        output_dir: Directory for output files.
        log_level: Logging level.
        output_level: Console output level ('quiet', 'normal', 'verbose', 'debug').
    
    Returns:
        Results dictionary from the orchestrator run.
    
    Example:
        >>> results = run_orchestrator(
        ...     endpoints=['movie'],
        ...     max_pages=5
        ... )
    """
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Get output level
    level_map = {
        'quiet': OutputLevel.QUIET,
        'normal': OutputLevel.NORMAL,
        'verbose': OutputLevel.VERBOSE,
        'debug': OutputLevel.DEBUG
    }
    output_level_enum = level_map.get(output_level.lower(), OutputLevel.NORMAL)
    
    # Create config with overrides
    config = get_config()
    if output_dir:
        config.output_dir = output_dir
    
    # Create and run orchestrator
    orchestrator = Orchestrator(
        config=config,
        output_level=output_level_enum
    )
    
    try:
        results = orchestrator.run_all_endpoints(
            endpoints=endpoints,
            content_types=content_types,
            max_pages=max_pages
        )
        return results
    finally:
        orchestrator.close()


def print_colored(
    message: str,
    color: str = 'white',
    bold: bool = False,
    file = None
) -> None:
    """
    Print colored text to console.
    
    Args:
        message: Message to print.
        color: Color name ('red', 'green', 'yellow', 'blue', 'cyan', 'magenta').
        bold: Whether to use bold formatting.
        file: Output file (default: sys.stdout).
    """
    colors_map = {
        'black': ConsoleColors.BLACK,
        'red': ConsoleColors.RED,
        'green': ConsoleColors.GREEN,
        'yellow': ConsoleColors.YELLOW,
        'blue': ConsoleColors.BLUE,
        'magenta': ConsoleColors.MAGENTA,
        'cyan': ConsoleColors.CYAN,
        'white': ConsoleColors.WHITE
    }
    
    color_code = colors_map.get(color.lower(), ConsoleColors.WHITE)
    
    if bold:
        color_code = color_code.replace('\033[', '\033[1;')
    
    if file is None:
        file = sys.stdout
    
    print(f"{color_code}{message}{ConsoleColors.RESET}", file=file)
