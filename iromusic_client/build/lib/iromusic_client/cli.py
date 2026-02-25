"""
CLI Entry Point module for iromusic_client.

This module provides a command-line interface for the iromusic data fetcher.
It uses Click for CLI argument parsing and provides commands for fetching
data from various endpoints.

Commands:
    fetch: Fetch data from API endpoints.
    config: Show or modify configuration.
    status: Show status and statistics.

Example:
    >>> from iromusic_client.cli import cli
    >>> cli()
"""

import sys
import logging
from typing import Optional, List, Dict

import click

from .config import Config, get_config
from .orchestrator import Orchestrator, run_orchestrator, OutputLevel, print_colored
from .api_client import create_client
from .file_handler import create_file_handler


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@click.group()
@click.version_option(version='1.0.0')
def cli() -> None:
    """
    iromusic_client - Fetch data from iromusicapp.ir API.
    
    This tool provides a command-line interface for fetching movie, series,
    and music data from the iromusicapp.ir API with support for pagination,
    data validation, and organized file output.
    
    For more information, visit: https://iromusicapp.ir
    """
    pass


@cli.command()
@click.option(
    '--endpoint', '-e',
    type=click.Choice(['movie', 'music', 'all'], case_sensitive=False),
    default='all',
    help='API endpoint to fetch from'
)
@click.option(
    '--type', '-t',
    'content_type',
    type=click.Choice(['movies', 'series', 'albums', 'singles', 'all'], case_sensitive=False),
    default='all',
    help='Content type to fetch'
)
@click.option(
    '--max-pages', '-m',
    type=int,
    default=None,
    help='Maximum number of pages to fetch per endpoint'
)
@click.option(
    '--output-dir', '-o',
    type=click.Path(),
    default=None,
    help='Output directory for downloaded data'
)
@click.option(
    '--no-save',
    is_flag=True,
    default=False,
    help='Do not save results to files'
)
@click.option(
    '--verbose', '-v',
    is_flag=True,
    default=False,
    help='Enable verbose output'
)
@click.option(
    '--debug',
    is_flag=True,
    default=False,
    help='Enable debug output'
)
@click.option(
    '--quiet', '-q',
    is_flag=True,
    default=False,
    help='Suppress all output except errors'
)
def fetch(
    endpoint: str,
    content_type: str,
    max_pages: Optional[int],
    output_dir: Optional[str],
    no_save: bool,
    verbose: bool,
    debug: bool,
    quiet: bool
) -> None:
    """
    Fetch data from iromusicapp.ir API endpoints.
    
    This command fetches data from the specified endpoint(s) and saves
    the results to JSON files in the output directory.
    
    Examples:
        \b
        # Fetch all data
        $ iromusic fetch
        
        \b
        # Fetch only movies
        $ iromusic fetch --endpoint movie --type movies
        
        \b
        # Fetch with limited pages
        $ iromusic fetch --max-pages 5
        
        \b
        # Save to custom directory
        $ iromusic fetch --output-dir ./data
    """
    # Determine output level
    if quiet:
        output_level = 'quiet'
    elif debug:
        output_level = 'debug'
    elif verbose:
        output_level = 'verbose'
    else:
        output_level = 'normal'
    
    # Determine log level
    if debug:
        log_level = 'DEBUG'
    elif verbose:
        log_level = 'INFO'
    else:
        log_level = 'WARNING'
    
    # Build endpoints list
    if endpoint == 'all':
        endpoints = ['movie', 'music']
    else:
        endpoints = [endpoint]
    
    # Build content types dict
    if content_type == 'all':
        content_types = {
            'movie': [None, 'movies', 'series'],
            'music': ['albums', 'singles']
        }
    else:
        if endpoint == 'movie':
            content_types = {'movie': [content_type if content_type in ['movies', 'series'] else None]}
        elif endpoint == 'music':
            content_types = {'music': [content_type if content_type in ['albums', 'singles'] else 'albums']}
        else:
            content_types = {'movie': [None], 'music': ['albums']}
    
    # Show configuration
    if not quiet:
        print_colored("iromusic_client", 'cyan', bold=True)
        print(f"Endpoints: {', '.join(endpoints)}")
        print(f"Content types: {content_types}")
        if max_pages:
            print(f"Max pages: {max_pages}")
        print(f"Output level: {output_level}")
        print()
    
    try:
        # Run orchestrator
        results = run_orchestrator(
            endpoints=endpoints,
            content_types=content_types,
            max_pages=max_pages,
            output_dir=output_dir,
            log_level=log_level,
            output_level=output_level
        )
        
        # Exit with appropriate code
        stats = results.get('overall_stats', {}).get('run', {})
        failed = stats.get('failed_endpoints', 0)
        
        sys.exit(0 if failed == 0 else 1)
        
    except KeyboardInterrupt:
        print_colored("\nOperation cancelled by user", 'yellow')
        sys.exit(130)
    except Exception as e:
        print_colored(f"Error: {e}", 'red', bold=True)
        if debug:
            import traceback
            traceback.print_exc()
        sys.exit(1)


@cli.command()
@click.option(
    '--show', '-s',
    is_flag=True,
    default=False,
    help='Show current configuration'
)
@click.option(
    '--set-timeout',
    type=int,
    default=None,
    help='Set request timeout in seconds'
)
@click.option(
    '--set-retries',
    type=int,
    default=None,
    help='Set maximum retry attempts'
)
@click.option(
    '--set-output-dir',
    type=click.Path(),
    default=None,
    help='Set output directory'
)
def config(
    show: bool,
    set_timeout: Optional[int],
    set_retries: Optional[int],
    set_output_dir: Optional[str]
) -> None:
    """
    Manage iromusic_client configuration.
    
    This command allows viewing and modifying configuration settings.
    Changes are temporary and apply to the current session only.
    
    Examples:
        \b
        # Show current configuration
        $ iromusic config --show
        
        \b
        # Set timeout to 60 seconds
        $ iromusic config --set-timeout 60
    """
    config_obj = get_config()
    
    if show:
        print_colored("Current Configuration", 'cyan', bold=True)
        print(f"API Base URL: {config_obj.api_base_url}")
        print(f"Timeout: {config_obj.timeout}s")
        print(f"Max Retries: {config_obj.max_retries}")
        print(f"Backoff Factor: {config_obj.backoff_factor}")
        print(f"Output Directory: {config_obj.output_dir}")
        print(f"Cache Directory: {config_obj.cache_dir}")
        print(f"Cache Enabled: {config_obj.enable_cache}")
        print(f"Page Size: {config_obj.page_size}")
        print(f"Log Level: {config_obj.log_level}")
        
        # Show environment variables
        print_colored("\nEnvironment Variables", 'cyan', bold=True)
        print("IROMUSIC_API_BASE_URL")
        print("IROMUSIC_TIMEOUT")
        print("IROMUSIC_MAX_RETRIES")
        print("IROMUSIC_BACKOFF_FACTOR")
        print("IROMUSIC_OUTPUT_DIR")
        print("IROMUSIC_CACHE_DIR")
        print("IROMUSIC_ENABLE_CACHE")
        print("IROMUSIC_PAGE_SIZE")
        print("IROMUSIC_LOG_LEVEL")
        return
    
    # Apply changes
    if set_timeout is not None:
        config_obj.timeout = set_timeout
        print_colored(f"Timeout set to {set_timeout}s", 'green')
    
    if set_retries is not None:
        config_obj.max_retries = set_retries
        print_colored(f"Max retries set to {set_retries}", 'green')
    
    if set_output_dir is not None:
        config_obj.output_dir = set_output_dir
        print_colored(f"Output directory set to {set_output_dir}", 'green')


@cli.command()
@click.option(
    '--show-cache',
    is_flag=True,
    default=False,
    help='Show cached URLs'
)
@click.option(
    '--clear-cache',
    is_flag=True,
    default=False,
    help='Clear the response cache'
)
def status(
    show_cache: bool,
    clear_cache: bool
) -> None:
    """
    Show status and statistics.
    
    This command displays information about the client, including
    cached responses and request statistics.
    
    Examples:
        \b
        # Show cached URLs
        $ iromusic status --show-cache
        
        \b
        # Clear cache
        $ iromusic status --clear-cache
    """
    config_obj = get_config()
    
    # Show client info
    print_colored("iromusic_client Status", 'cyan', bold=True)
    print(f"API: {config_obj.api_base_url}")
    print(f"Timeout: {config_obj.timeout}s")
    print(f"Max Retries: {config_obj.max_retries}")
    
    # Test API connectivity
    print_colored("\nAPI Connectivity", 'cyan', bold=True)
    
    try:
        client = create_client(config_obj)
        
        # Make a simple request
        url = config_obj.get_movie_posts_url()
        response = client.get(url, use_cache=False)
        
        print_colored("API connection: OK", 'green')
        print(f"Response status: {response.status_code}")
        
        client.close()
        
    except Exception as e:
        print_colored(f"API connection: FAILED", 'red')
        print(f"Error: {e}")
    
    # Show cache info
    if show_cache or clear_cache:
        print_colored("\nCache", 'cyan', bold=True)
        
        if clear_cache:
            client = create_client(config_obj)
            client.cache.clear()
            print_colored("Cache cleared", 'green')
        
        if show_cache:
            client = create_client(config_obj)
            cached = client.cache.get_cached_urls()
            if cached:
                print(f"Cached URLs ({len(cached)}):")
                for url in cached:
                    print(f"  - {url}")
            else:
                print("No cached URLs")
            client.close()


@cli.command()
@click.argument('url')
@click.option(
    '--method', '-m',
    type=click.Choice(['GET', 'POST'], case_sensitive=False),
    default='GET',
    help='HTTP method to use'
)
@click.option(
    '--params', '-p',
    multiple=True,
    help='Query parameters (key=value format)'
)
def request(url: str, method: str, params: tuple) -> None:
    """
    Make a raw API request.
    
    This command allows making direct API requests for testing
    and debugging purposes.
    
    Example:
        \b
        $ iromusic request "https://iromusicapp.ir/iroapi/movie/posts?type=movies"
    """
    config_obj = get_config()
    client = create_client(config_obj)
    
    # Parse params
    params_dict = {}
    for param in params:
        if '=' in param:
            key, value = param.split('=', 1)
            params_dict[key] = value
    
    try:
        print_colored(f"Making {method} request to: {url}", 'cyan')
        if params_dict:
            print(f"Parameters: {params_dict}")
        
        response = client.get(url, params=params_dict if params_dict else None)
        
        print_colored(f"Status: {response.status_code} {response.reason}", 'green')
        print(f"Headers: {dict(response.headers)}")
        
        # Try to show JSON response
        try:
            data = response.json()
            import json
            print("\nResponse Body:")
            print(json.dumps(data, indent=2))
        except Exception:
            print("\nResponse Body:")
            print(response.text[:500])
        
    except Exception as e:
        print_colored(f"Request failed: {e}", 'red', bold=True)
        sys.exit(1)
    finally:
        client.close()


def main() -> None:
    """Main entry point for the CLI."""
    cli()


if __name__ == '__main__':
    main()
