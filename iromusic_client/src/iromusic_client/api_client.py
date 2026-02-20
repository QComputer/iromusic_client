"""
API Client module for iromusic_client.

This module provides the HTTP client for communicating with the iromusicapp.ir
API endpoints. It handles all network communications with configurable timeout
settings, custom headers, exponential backoff retry logic, and rate limit handling.

Classes:
    APIError: Custom exception for API-related errors.
    RateLimitError: Custom exception for rate limit (429) responses.
    APIClient: Main client for making HTTP requests to the API.
    ResponseCache: In-memory cache for API responses.

Functions:
    create_client: Factory function to create an API client instance.

Example:
    >>> from iromusic_client.api_client import create_client
    >>> client = create_client()
    >>> response = client.get('https://iromusicapp.ir/iroapi/movie/posts')
    >>> print(response.status_code)
"""

import json
import time
import hashlib
import logging
from typing import Optional, Dict, Any, List, Union
from datetime import datetime, timedelta
from dataclasses import dataclass, field

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import Config


# Module logger
logger = logging.getLogger(__name__)


class APIError(Exception):
    """
    Base exception class for API-related errors.
    
    This exception is raised when an API request fails due to client or
    server errors that are not specifically rate limits or timeouts.
    
    Attributes:
        message (str): Human-readable error message.
        status_code (int): HTTP status code if available.
        response (Optional[requests.Response]: The response object if available.
    
    Example:
        >>> try:
        ...     client.get(url)
        ... except APIError as e:
        ...     print(f"API Error: {e.message}, Status: {e.status_code}")
    """
    
    def __init__(
        self, 
        message: str, 
        status_code: Optional[int] = None,
        response: Optional[requests.Response] = None
    ) -> None:
        """
        Initialize the APIError.
        
        Args:
            message: Error message describing the issue.
            status_code: HTTP status code if available.
            response: The requests Response object if available.
        """
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.response = response


class RateLimitError(APIError):
    """
    Exception raised when the API returns a 429 (Too Many Requests) status.
    
    This exception indicates that the client has exceeded the API's rate limit
    and should wait before making additional requests.
    
    Attributes:
        retry_after (Optional[int]): Seconds to wait before retrying.
        message (str): Human-readable error message.
    
    Example:
        >>> try:
        ...     client.get(url)
        ... except RateLimitError as e:
        ...     print(f"Rate limited. Retry after {e.retry_after} seconds")
    """
    
    def __init__(
        self, 
        message: str, 
        retry_after: Optional[int] = None,
        response: Optional[requests.Response] = None
    ) -> None:
        """
        Initialize the RateLimitError.
        
        Args:
            message: Error message describing the rate limit.
            retry_after: Seconds to wait before retrying (from Retry-After header).
            response: The requests Response object.
        """
        super().__init__(message, status_code=429, response=response)
        self.retry_after = retry_after


@dataclass
class RequestStats:
    """
    Statistics for API requests.
    
    This dataclass tracks various metrics about API requests including
    success/failure counts, timing information, and retry attempts.
    
    Attributes:
        total_requests (int): Total number of requests made.
        successful_requests (int): Number of successful requests.
        failed_requests (int): Number of failed requests.
        total_retries (int): Total number of retry attempts.
        total_time (float): Total time spent on all requests in seconds.
        last_request_time (Optional[float]): Time of the last request in seconds.
    
    Example:
        >>> stats = RequestStats()
        >>> stats.successful_requests += 1
        >>> print(f"Success rate: {stats.successful_requests / stats.total_requests}")
    """
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    total_retries: int = 0
    total_time: float = 0.0
    last_request_time: Optional[float] = None
    
    def record_success(self, duration: float, retries: int = 0) -> None:
        """
        Record a successful request.
        
        Args:
            duration: Time taken for the request in seconds.
            retries: Number of retries attempted.
        """
        self.total_requests += 1
        self.successful_requests += 1
        self.total_retries += retries
        self.total_time += duration
        self.last_request_time = duration
    
    def record_failure(self, duration: float, retries: int = 0) -> None:
        """
        Record a failed request.
        
        Args:
            duration: Time taken for the request in seconds.
            retries: Number of retries attempted.
        """
        self.total_requests += 1
        self.failed_requests += 1
        self.total_retries += retries
        self.total_time += duration
        self.last_request_time = duration
    
    def get_success_rate(self) -> float:
        """
        Calculate the success rate of requests.
        
        Returns:
            float: Success rate as a percentage (0-100).
        """
        if self.total_requests == 0:
            return 0.0
        return (self.successful_requests / self.total_requests) * 100
    
    def get_average_time(self) -> float:
        """
        Calculate the average request time.
        
        Returns:
            float: Average time per request in seconds.
        """
        if self.total_requests == 0:
            return 0.0
        return self.total_time / self.total_requests


class ResponseCache:
    """
    In-memory cache for API responses.
    
    This class provides caching functionality to store successful API responses
    for graceful degradation when APIs become unavailable.
    
    Attributes:
        cache (Dict[str, Dict[str, Any]]): Internal cache storage.
        ttl (timedelta): Time-to-live for cached responses.
    
    Example:
        >>> cache = ResponseCache(ttl=timedelta(hours=1))
        >>> cache.set('url_key', {'data': 'value'})
        >>> cached = cache.get('url_key')
    """
    
    def __init__(self, ttl: timedelta = timedelta(hours=1)) -> None:
        """
        Initialize the ResponseCache.
        
        Args:
            ttl: Time-to-live for cached responses.
        """
        self.cache: Dict[str, Dict[str, Any]] = {}
        self.ttl = ttl
    
    def _generate_key(self, url: str, params: Optional[Dict[str, Any]] = None) -> str:
        """
        Generate a cache key from URL and parameters.
        
        Args:
            url: The URL being requested.
            params: Optional query parameters.
        
        Returns:
            str: A hash-based cache key.
        """
        key_data = url
        if params:
            key_data += json.dumps(params, sort_keys=True)
        return hashlib.md5(key_data.encode()).hexdigest()
    
    def set(
        self, 
        url: str, 
        data: Any, 
        params: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Store data in the cache.
        
        Args:
            url: The URL that was requested.
            data: The response data to cache.
            params: Optional query parameters used.
        """
        key = self._generate_key(url, params)
        self.cache[key] = {
            'data': data,
            'timestamp': datetime.now(),
            'url': url,
            'params': params
        }
        logger.debug(f"Cached response for URL: {url}")
    
    def get(
        self, 
        url: str, 
        params: Optional[Dict[str, Any]] = None
    ) -> Optional[Any]:
        """
        Retrieve data from the cache.
        
        Args:
            url: The URL that was requested.
            params: Optional query parameters used.
        
        Returns:
            Cached data if available and not expired, None otherwise.
        """
        key = self._generate_key(url, params)
        cached = self.cache.get(key)
        
        if cached is None:
            return None
        
        # Check if expired
        if datetime.now() - cached['timestamp'] > self.ttl:
            del self.cache[key]
            logger.debug(f"Cache expired for URL: {url}")
            return None
        
        logger.debug(f"Cache hit for URL: {url}")
        return cached['data']
    
    def clear(self) -> None:
        """Clear all cached data."""
        self.cache.clear()
        logger.debug("Cache cleared")
    
    def get_cached_urls(self) -> List[str]:
        """
        Get list of cached URLs.
        
        Returns:
            List of URLs currently in the cache.
        """
        return [item['url'] for item in self.cache.values()]


class APIClient:
    """
    HTTP client for iromusicapp.ir API.
    
    This class handles all HTTP communications with the API, including
    automatic retry with exponential backoff, rate limit handling, and
    response caching for graceful degradation.
    
    Attributes:
        config (Config): Configuration object.
        session (requests.Session): Requests session for connection pooling.
        stats (RequestStats): Request statistics tracker.
        cache (ResponseCache): Response cache for fallback.
    
    Example:
        >>> client = APIClient()
        >>> response = client.get('https://iromusicapp.ir/iroapi/movie/posts')
        >>> data = response.json()
    """
    
    DEFAULT_HEADERS: Dict[str, str] = {
        'Accept': 'application/json',
        'Accept-Language': 'en-US,en;q=0.9,fa;q=0.8'
    }
    
    def __init__(self, config: Optional[Config] = None) -> None:
        """
        Initialize the APIClient.
        
        Args:
            config: Optional configuration object. If not provided, uses default Config.
        
        Raises:
            ValueError: If configuration values are invalid.
        """
        self.config = config or Config()
        
        # Initialize session with retry adapter
        self.session = requests.Session()
        self._setup_session()
        
        # Initialize stats and cache
        self.stats = RequestStats()
        self.cache = ResponseCache()
        
        # Build headers
        self.headers = self.DEFAULT_HEADERS.copy()
        self.headers['User-Agent'] = self.config.user_agent
        
        logger.info("APIClient initialized")
    
    def _setup_session(self) -> None:
        """
        Set up the requests session with retry strategy.
        
        Configures the session with an HTTPAdapter that implements
        exponential backoff retry logic for transient failures.
        """
        retry_strategy = Retry(
            total=self.config.max_retries,
            backoff_factor=self.config.backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
            raise_on_status=False
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
    
    def _calculate_backoff(self, attempt: int, retry_after: Optional[int] = None) -> float:
        """
        Calculate the backoff time for a retry attempt.
        
        Args:
            attempt: The current retry attempt number (0-indexed).
            retry_after: Optional Retry-After header value in seconds.
        
        Returns:
            float: Backoff time in seconds.
        """
        if retry_after:
            return float(retry_after)
        
        # Exponential backoff with jitter
        base_delay = self.config.backoff_factor ** attempt
        import random
        jitter = random.uniform(0, 0.5)
        return base_delay + jitter
    
    def _make_request(
        self,
        method: str,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        allow_redirects: bool = True,
        timeout: Optional[int] = None,
        use_cache: bool = True
    ) -> requests.Response:
        """
        Make an HTTP request with retry logic.
        
        This method implements the core request logic with exponential
        backoff for handling transient failures and rate limits.
        
        Args:
            method: HTTP method (GET, POST, etc.).
            url: URL to request.
            params: Optional query parameters.
            data: Optional request body data.
            headers: Optional additional headers.
            allow_redirects: Whether to follow redirects.
            timeout: Optional timeout override.
            use_cache: Whether to check/use cache for this request.
        
        Returns:
            requests.Response: The HTTP response object.
        
        Raises:
            requests.exceptions.ConnectionError: On network connectivity issues.
            requests.exceptions.Timeout: On request timeout.
            RateLimitError: On 429 status code.
            APIError: On other HTTP errors.
        """
        request_headers = self.headers.copy()
        if headers:
            request_headers.update(headers)
        
        timeout_value = timeout or self.config.timeout
        
        # Check cache for GET requests
        if method.upper() == 'GET' and use_cache and self.config.enable_cache:
            cached_data = self.cache.get(url, params)
            if cached_data is not None:
                logger.info(f"Using cached response for: {url}")
                # Return a mock response-like object with cached data
                return self._create_mock_response(cached_data)
        
        attempt = 0
        last_exception: Optional[Exception] = None
        
        while attempt <= self.config.max_retries:
            try:
                start_time = time.time()
                
                logger.debug(
                    f"Request attempt {attempt + 1}/{self.config.max_retries + 1}: "
                    f"{method} {url} with params={params}"
                )
                
                response = self.session.request(
                    method=method,
                    url=url,
                    params=params,
                    data=data,
                    headers=request_headers,
                    allow_redirects=allow_redirects,
                    timeout=timeout_value
                )
                
                duration = time.time() - start_time
                
                # Handle rate limiting
                if response.status_code == 429:
                    retry_after = response.headers.get('Retry-After')
                    retry_after_val = int(retry_after) if retry_after and retry_after.isdigit() else None
                    
                    if attempt < self.config.max_retries:
                        backoff_time = self._calculate_backoff(attempt, retry_after_val)
                        logger.warning(
                            f"Rate limited. Attempt {attempt + 1}. "
                            f"Waiting {backoff_time:.2f}s before retry."
                        )
                        time.sleep(backoff_time)
                        attempt += 1
                        continue
                    else:
                        raise RateLimitError(
                            "Rate limit exceeded after max retries",
                            retry_after=retry_after_val,
                            response=response
                        )
                
                # Handle successful responses
                if response.ok:
                    self.stats.record_success(duration, attempt)
                    
                    # Cache successful GET responses
                    if method.upper() == 'GET' and self.config.enable_cache:
                        try:
                            self.cache.set(url, response.json(), params)
                        except json.JSONDecodeError:
                            logger.warning("Failed to cache non-JSON response")
                    
                    return response
                
                # Handle other HTTP errors
                if response.status_code >= 500:
                    # Server error - retry
                    if attempt < self.config.max_retries:
                        backoff_time = self._calculate_backoff(attempt)
                        logger.warning(
                            f"Server error {response.status_code}. "
                            f"Attempt {attempt + 1}. Waiting {backoff_time:.2f}s"
                        )
                        time.sleep(backoff_time)
                        attempt += 1
                        continue
                    else:
                        raise APIError(
                            f"Server error after {attempt + 1} attempts",
                            status_code=response.status_code,
                            response=response
                        )
                else:
                    # Client error - don't retry
                    raise APIError(
                        f"HTTP {response.status_code}: {response.reason}",
                        status_code=response.status_code,
                        response=response
                    )
                    
            except requests.exceptions.ConnectionError as e:
                last_exception = e
                duration = time.time() - start_time if 'start_time' in locals() else 0
                
                if attempt < self.config.max_retries:
                    backoff_time = self._calculate_backoff(attempt)
                    logger.warning(
                        f"Connection error: {str(e)}. "
                        f"Attempt {attempt + 1}. Waiting {backoff_time:.2f}s"
                    )
                    time.sleep(backoff_time)
                    attempt += 1
                else:
                    self.stats.record_failure(duration, attempt)
                    logger.error(f"Connection failed after {attempt + 1} attempts")
                    raise
                    
            except requests.exceptions.Timeout as e:
                last_exception = e
                duration = time.time() - start_time if 'start_time' in locals() else 0
                
                if attempt < self.config.max_retries:
                    backoff_time = self._calculate_backoff(attempt)
                    logger.warning(
                        f"Timeout: {str(e)}. "
                        f"Attempt {attempt + 1}. Waiting {backoff_time:.2f}s"
                    )
                    time.sleep(backoff_time)
                    attempt += 1
                else:
                    self.stats.record_failure(duration, attempt)
                    logger.error(f"Timeout after {attempt + 1} attempts")
                    raise
        
        # If we get here, all retries failed
        raise APIError(f"Request failed after {self.config.max_retries + 1} attempts")
    
    def _create_mock_response(self, data: Any) -> 'requests.Response':
        """
        Create a mock response object from cached data.
        
        This is used to return cached data in the same format as
        a real HTTP response.
        
        Args:
            data: The cached data to wrap.
        
        Returns:
            A mock response object with json() method.
        """
        class MockResponse:
            def __init__(self, data: Any):
                self._data = data
                self.ok = True
                self.status_code = 200
                self.reason = "Cached"
                self.headers = {}
            
            def json(self) -> Any:
                return self._data
            
            @property
            def text(self) -> str:
                return json.dumps(self._data)
        
        return MockResponse(data)
    
    def get(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
        use_cache: bool = True
    ) -> requests.Response:
        """
        Perform a GET request.
        
        Args:
            url: URL to request.
            params: Optional query parameters.
            headers: Optional additional headers.
            timeout: Optional timeout override.
            use_cache: Whether to use cached responses.
        
        Returns:
            requests.Response: The HTTP response.
        
        Raises:
            requests.exceptions.ConnectionError: On network issues.
            requests.exceptions.Timeout: On timeout.
            RateLimitError: On rate limit.
            APIError: On other errors.
        
        Example:
            >>> client = APIClient()
            >>> response = client.get(
            ...     'https://iromusicapp.ir/iroapi/movie/posts',
            ...     params={'type': 'movies', 'page': 1}
            ... )
        """
        return self._make_request(
            method='GET',
            url=url,
            params=params,
            headers=headers,
            timeout=timeout,
            use_cache=use_cache
        )
    
    def get_with_pagination(
        self,
        url: str,
        page_param: str = 'page',
        max_pages: Optional[int] = None,
        stop_condition: Optional[callable] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch all pages of data from a paginated endpoint.
        
        This method automatically handles pagination by making sequential
        requests until either max_pages is reached, the stop_condition
        returns True, or no more data is available.
        
        Args:
            url: Base URL for the API endpoint.
            page_param: Name of the page query parameter.
            max_pages: Maximum number of pages to fetch (None for unlimited).
            stop_condition: Optional function that takes response data and 
                          returns True to stop fetching.
        
        Returns:
            List of all items fetched across all pages.
        
        Raises:
            APIError: On API errors.
        
        Example:
            >>> client = APIClient()
            >>> data = client.get_with_pagination(
            ...     'https://iromusicapp.ir/iroapi/movie/posts',
            ...     max_pages=10
            ... )
        """
        all_items: List[Dict[str, Any]] = []
        current_page = 1
        
        while True:
            # Check max pages
            if max_pages and current_page > max_pages:
                logger.info(f"Reached max pages limit: {max_pages}")
                break
            
            # Make request
            params = {page_param: current_page}
            logger.info(f"Fetching page {current_page}: {url}")
            
            try:
                response = self.get(url, params=params)
                data = response.json()
                
                # Extract items - adapt based on API response structure
                items = self._extract_items(data)
                
                if not items:
                    logger.info(f"No more items on page {current_page}")
                    break
                
                all_items.extend(items)
                logger.info(f"Fetched {len(items)} items from page {current_page}")
                
                # Check stop condition
                if stop_condition and stop_condition(data):
                    logger.info("Stop condition met")
                    break
                
                current_page += 1
                
            except (APIError, RateLimitError) as e:
                logger.error(f"Failed to fetch page {current_page}: {e}")
                break
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON on page {current_page}: {e}")
                break
        
        logger.info(f"Pagination complete. Total items: {len(all_items)}")
        return all_items
    
    def _extract_items(self, data: Any) -> List[Dict[str, Any]]:
        """
        Extract items from API response data.
        
        This method attempts to handle different response structures
        commonly returned by APIs.
        
        Args:
            data: The parsed JSON response data.
        
        Returns:
            List of items found in the response.
        """
        if isinstance(data, list):
            return data
        
        if isinstance(data, dict):
            # Common response structures
            for key in ['data', 'items', 'results', 'posts', 'content']:
                if key in data:
                    value = data[key]
                    if isinstance(value, list):
                        return value
                    elif isinstance(value, dict) and 'data' in value:
                        return value['data']
        
        return []
    
    def close(self) -> None:
        """Close the HTTP session and release resources."""
        self.session.close()
        logger.info("APIClient session closed")
    
    def __enter__(self) -> 'APIClient':
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()


def create_client(config: Optional[Config] = None) -> APIClient:
    """
    Factory function to create an API client instance.
    
    This is the recommended way to create an APIClient instance.
    
    Args:
        config: Optional configuration object.
    
    Returns:
        APIClient: Configured API client instance.
    
    Example:
        >>> from iromusic_client.api_client import create_client
        >>> client = create_client()
        >>> with client as c:
        ...     response = c.get('https://iromusicapp.ir/iroapi/movie/posts')
    """
    return APIClient(config)
