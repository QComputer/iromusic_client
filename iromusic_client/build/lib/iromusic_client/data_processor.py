"""
Data Processing module for iromusic_client.

This module provides functionality for validating, parsing, and transforming
API responses into normalized Python data structures suitable for serialization.

Classes:
    ValidationError: Exception raised when data validation fails.
    DataProcessor: Main class for processing API response data.
    DataNormalizer: Class for normalizing data structures.

Functions:
    parse_json: Safely parse JSON data with error handling.
    validate_response: Validate API response against expected schema.

Example:
    >>> from iromusic_client.data_processor import DataProcessor
    >>> processor = DataProcessor()
    >>> data = processor.process_response(raw_response)
"""

import json
import logging
from typing import (
    Optional, Dict, Any, List, Union, Callable, 
    TypeVar, Generic, Set
)
from dataclasses import dataclass, field
from datetime import datetime
from copy import deepcopy


# Module logger
logger = logging.getLogger(__name__)

# Type variable for generic processing
T = TypeVar('T')
TDict = Dict[str, Any]


class ValidationError(Exception):
    """
    Exception raised when data validation fails.
    
    This exception is raised when incoming data does not meet the
    required schema or validation criteria.
    
    Attributes:
        message (str): Human-readable error message.
        errors (List[str]): List of specific validation errors.
        data (Any): The data that failed validation.
    
    Example:
        >>> try:
        ...     validate_response(data, required_fields)
        ... except ValidationError as e:
        ...     print(f"Validation failed: {e.errors}")
    """
    
    def __init__(
        self, 
        message: str, 
        errors: Optional[List[str]] = None,
        data: Any = None
    ) -> None:
        """
        Initialize the ValidationError.
        
        Args:
            message: Error message describing the validation failure.
            errors: List of specific validation error messages.
            data: The data that failed validation.
        """
        super().__init__(message)
        self.message = message
        self.errors = errors or []
        self.data = data
    
    def __str__(self) -> str:
        """Return string representation of the error."""
        if self.errors:
            return f"{self.message}: {'; '.join(self.errors)}"
        return self.message


@dataclass
class ProcessingStats:
    """
    Statistics for data processing operations.
    
    Tracks metrics about processed data including validation
    success/failure counts and transformation statistics.
    
    Attributes:
        total_processed (int): Total items processed.
        successful_validations (int): Items that passed validation.
        failed_validations (int): Items that failed validation.
        transformed_items (int): Items that were transformed.
        null_items (int): Items that were null or empty.
        start_time (datetime): When processing started.
        end_time (Optional[datetime]): When processing ended.
    
    Example:
        >>> stats = ProcessingStats()
        >>> stats.total_processed += 1
        >>> stats.successful_validations += 1
    """
    total_processed: int = 0
    successful_validations: int = 0
    failed_validations: int = 0
    transformed_items: int = 0
    null_items: int = 0
    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None
    
    def finalize(self) -> None:
        """Mark the processing as complete by setting end_time."""
        self.end_time = datetime.now()
    
    def get_duration(self) -> float:
        """
        Get the duration of processing in seconds.
        
        Returns:
            float: Duration in seconds, or 0 if not finalized.
        """
        if self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0
    
    def get_success_rate(self) -> float:
        """
        Calculate the validation success rate.
        
        Returns:
            float: Success rate as a percentage (0-100).
        """
        if self.total_processed == 0:
            return 0.0
        return (self.successful_validations / self.total_processed) * 100


class DataNormalizer:
    """
    Normalizer for transforming API response data.
    
    This class provides methods to transform raw API responses into
    consistent, normalized Python dictionaries suitable for
    serialization and storage.
    
    Attributes:
        field_mappings (Dict[str, str]): Mapping of source to target field names.
        default_values (Dict[str, Any]): Default values for missing fields.
    
    Example:
        >>> normalizer = DataNormalizer()
        >>> normalized = normalizer.normalize(raw_data)
    """
    
    # Common field name variations
    COMMON_FIELD_MAPPINGS: Dict[str, List[str]] = {
        'id': ['id', 'ID', '_id', 'post_id', 'content_id'],
        'title': ['title', 'Title', 'name', 'Name', 'post_title'],
        'description': ['description', 'Description', 'desc', 'summary', 'body'],
        'image': ['image', 'Image', 'thumbnail', 'Thumbnail', 'cover', 'poster'],
        'url': ['url', 'URL', 'link', 'Link', 'source_url'],
        'date': ['date', 'Date', 'created_at', 'published_at', 'timestamp'],
        'type': ['type', 'Type', 'content_type', 'category'],
        'author': ['author', 'Author', 'creator', 'user'],
    }
    
    def __init__(
        self,
        field_mappings: Optional[Dict[str, str]] = None,
        default_values: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Initialize the DataNormalizer.
        
        Args:
            field_mappings: Custom field name mappings.
            default_values: Default values for missing fields.
        """
        self.field_mappings = field_mappings or {}
        self.default_values = default_values or {
            'created_at': lambda: datetime.now().isoformat(),
            'updated_at': lambda: datetime.now().isoformat(),
            'processed_at': lambda: datetime.now().isoformat(),
        }
    
    def _find_field(self, data: Dict[str, Any], possible_names: List[str]) -> Optional[Any]:
        """
        Find a field value using possible field names.
        
        Args:
            data: The data dictionary to search.
            possible_names: List of possible field names.
        
        Returns:
            The field value if found, None otherwise.
        """
        for name in possible_names:
            if name in data:
                return data[name]
        return None
    
    def normalize(self, data: Union[Dict[str, Any], List[Dict[str, Any]]]) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """
        Normalize a single record or list of records.
        
        Args:
            data: Raw data to normalize (dict or list of dicts).
        
        Returns:
            Normalized data in the same structure as input.
        
        Example:
            >>> normalizer = DataNormalizer()
            >>> normalized = normalizer.normalize({'ID': 123, 'Title': 'Test'})
            >>> print(normalized)
            {'id': 123, 'title': 'Test', 'created_at': '...'}
        """
        if isinstance(data, list):
            return [self.normalize_item(item) for item in data]
        elif isinstance(data, dict):
            return self.normalize_item(data)
        else:
            logger.warning(f"Unexpected data type: {type(data)}")
            return data
    
    def normalize_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize a single data item.
        
        Args:
            item: Raw data item to normalize.
        
        Returns:
            Normalized dictionary.
        """
        if not item:
            return {}
        
        normalized: Dict[str, Any] = {}
        
        # Apply field mappings
        for target_name, source_names in self.COMMON_FIELD_MAPPINGS.items():
            # Check custom mappings first
            if target_name in self.field_mappings:
                source_name = self.field_mappings[target_name]
                if source_name in item:
                    normalized[target_name] = item[source_name]
            else:
                # Use common field name variations
                value = self._find_field(item, source_names)
                if value is not None:
                    normalized[target_name] = value
        
        # Copy any unmapped fields that might be useful
        for key, value in item.items():
            if key not in normalized:
                normalized[key] = value
        
        # Apply default values
        for field_name, default_func in self.default_values.items():
            if field_name not in normalized:
                normalized[field_name] = default_func() if callable(default_func) else default_func
        
        return normalized
    
    def add_field(
        self, 
        data: Dict[str, Any], 
        field_name: str, 
        value: Any,
        overwrite: bool = False
    ) -> Dict[str, Any]:
        """
        Add a new field to the data.
        
        Args:
            data: The data dictionary to modify.
            field_name: Name of the field to add.
            value: Value to set for the field.
            overwrite: Whether to overwrite existing fields.
        
        Returns:
            Modified data dictionary.
        """
        if field_name not in data or overwrite:
            data[field_name] = value
        return data
    
    def remove_fields(
        self, 
        data: Dict[str, Any], 
        field_names: List[str]
    ) -> Dict[str, Any]:
        """
        Remove specified fields from the data.
        
        Args:
            data: The data dictionary to modify.
            field_names: List of field names to remove.
        
        Returns:
            Modified data dictionary.
        """
        for field_name in field_names:
            data.pop(field_name, None)
        return data


class DataProcessor:
    """
    Main processor for API response data.
    
    This class handles validation, parsing, and transformation of
    API responses, ensuring data quality and consistency.
    
    Attributes:
        required_fields (Set[str]): Fields required for validation.
        normalizer (DataNormalizer): Normalizer for data transformation.
        stats (ProcessingStats): Processing statistics.
    
    Example:
        >>> processor = DataProcessor(required_fields={'id', 'title'})
        >>> result = processor.process_response(api_response)
    """
    
    def __init__(
        self,
        required_fields: Optional[Set[str]] = None,
        optional_fields: Optional[Set[str]] = None,
        strict_mode: bool = False
    ) -> None:
        """
        Initialize the DataProcessor.
        
        Args:
            required_fields: Set of required field names for validation.
            optional_fields: Set of optional field names.
            strict_mode: If True, fail on any validation error.
        
        Example:
            >>> processor = DataProcessor(
            ...     required_fields={'id', 'title', 'type'},
            ...     strict_mode=False
            ... )
        """
        self.required_fields = required_fields or {'id', 'type'}
        self.optional_fields = optional_fields or set()
        self.strict_mode = strict_mode
        self.normalizer = DataNormalizer()
        self.stats = ProcessingStats()
    
    def parse_json(
        self, 
        response_text: str, 
        allow_empty: bool = False
    ) -> Optional[Dict[str, Any]]:
        """
        Parse JSON string with error handling.
        
        Args:
            response_text: The JSON string to parse.
            allow_empty: Whether to allow empty/null responses.
        
        Returns:
            Parsed JSON data, or None if parsing fails and allow_empty is True.
        
        Raises:
            json.JSONDecodeError: If JSON is invalid and allow_empty is False.
            ValueError: If response is empty and allow_empty is False.
        
        Example:
            >>> processor = DataProcessor()
            >>> data = processor.parse_json('{"key": "value"}')
        """
        # Handle empty or null input
        if not response_text or not response_text.strip():
            if allow_empty:
                logger.debug("Empty response received, allowing")
                return None
            raise ValueError("Empty or null response received")
        
        try:
            data = json.loads(response_text)
            
            if data is None:
                if allow_empty:
                    return None
                raise ValueError("JSON parsed to null")
            
            return data
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON parse error: {e}")
            if not allow_empty:
                raise
    
    def validate_data(
        self, 
        data: Dict[str, Any], 
        required_fields: Optional[Set[str]] = None,
        allow_partial: bool = True
    ) -> List[str]:
        """
        Validate data against required fields.
        
        Args:
            data: The data dictionary to validate.
            required_fields: Override for required fields.
            allow_partial: If True, validate at least some fields exist.
        
        Returns:
            List of validation error messages (empty if valid).
        
        Example:
            >>> errors = processor.validate_data(data)
            >>> if errors:
            ...     print(f"Validation errors: {errors}")
        """
        errors: List[str] = []
        fields_to_check = required_fields or self.required_fields
        
        # Check if data is a dictionary
        if not isinstance(data, dict):
            errors.append(f"Expected dict, got {type(data).__name__}")
            return errors
        
        # Check for missing required fields
        missing_fields = []
        for field in fields_to_check:
            if field not in data or data[field] is None:
                missing_fields.append(field)
        
        if missing_fields:
            if self.strict_mode:
                errors.append(f"Missing required fields: {missing_fields}")
            elif not allow_partial:
                errors.append(f"Missing fields: {missing_fields}")
        
        return errors
    
    def process_response(
        self, 
        response: Any,
        required_fields: Optional[Set[str]] = None,
        normalize: bool = True,
        allow_empty: bool = True
    ) -> Dict[str, Any]:
        """
        Process an API response into normalized data.
        
        This is the main entry point for processing API responses. It handles
        JSON parsing, validation, and normalization.
        
        Args:
            response: The API response (can be Response object, text, or dict).
            required_fields: Optional set of required fields to validate.
            normalize: Whether to normalize the data.
            allow_empty: Whether to allow empty responses.
        
        Returns:
            Processed data dictionary containing:
                - 'data': The processed data
                - 'valid': Boolean indicating if data is valid
                - 'errors': List of validation errors
                - 'stats': Processing statistics
        
        Raises:
            ValidationError: If validation fails in strict mode.
            json.JSONDecodeError: If JSON parsing fails.
        
        Example:
            >>> processor = DataProcessor()
            >>> result = processor.process_response(api_response)
            >>> if result['valid']:
            ...     print(f"Processed {len(result['data'])} items")
        """
        self.stats = ProcessingStats()  # Reset stats
        result: Dict[str, Any] = {
            'data': None,
            'valid': False,
            'errors': [],
            'stats': {}
        }
        
        try:
            # Extract JSON data from response
            if hasattr(response, 'json'):
                # It's a requests Response object
                data = response.json()
            elif isinstance(response, str):
                # It's a text response
                data = self.parse_json(response, allow_empty=allow_empty)
            elif isinstance(response, list):
                # It's already a list
                data = response
            elif isinstance(response, dict):
                # It's already a dict
                data = response
            else:
                raise ValueError(f"Unexpected response type: {type(response)}")
            
            # Handle empty data
            if data is None:
                self.stats.null_items += 1
                if allow_empty:
                    result['data'] = None
                    result['valid'] = True
                    return result
                else:
                    result['errors'].append("Empty response not allowed")
                    return result
            
            # Handle list responses
            processed_items: List[Dict[str, Any]] = []
            
            if isinstance(data, list):
                for item in data:
                    self.stats.total_processed += 1
                    
                    if not item:  # Handle null/empty items
                        self.stats.null_items += 1
                        continue
                    
                    # Validate item
                    errors = self.validate_data(item, required_fields)
                    
                    if errors:
                        self.stats.failed_validations += 1
                        result['errors'].extend(errors)
                        if self.strict_mode:
                            raise ValidationError(
                                "Validation failed",
                                errors=errors,
                                data=item
                            )
                    else:
                        self.stats.successful_validations += 1
                    
                    # Normalize if requested
                    if normalize:
                        processed_item = self.normalizer.normalize_item(item)
                        self.stats.transformed_items += 1
                    else:
                        processed_item = item
                    
                    processed_items.append(processed_item)
                
                result['data'] = processed_items
            
            elif isinstance(data, dict):
                self.stats.total_processed += 1
                
                # Validate
                errors = self.validate_data(data, required_fields)
                
                if errors:
                    self.stats.failed_validations += 1
                    result['errors'].extend(errors)
                    if self.strict_mode:
                        raise ValidationError(
                            "Validation failed",
                            errors=errors,
                            data=data
                        )
                else:
                    self.stats.successful_validations += 1
                
                # Normalize if requested
                if normalize:
                    result['data'] = self.normalizer.normalize_item(data)
                    self.stats.transformed_items += 1
                else:
                    result['data'] = data
            
            else:
                result['data'] = data
            
            result['valid'] = len(result['errors']) == 0 or self.stats.successful_validations > 0
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {e}")
            result['errors'].append(f"JSON decode error: {str(e)}")
        except ValidationError as e:
            logger.error(f"Validation error: {e}")
            result['errors'].append(str(e))
        except Exception as e:
            logger.error(f"Processing error: {e}")
            result['errors'].append(f"Processing error: {str(e)}")
        
        # Add stats to result
        self.stats.finalize()
        result['stats'] = {
            'total_processed': self.stats.total_processed,
            'successful': self.stats.successful_validations,
            'failed': self.stats.failed_validations,
            'transformed': self.stats.transformed_items,
            'null_items': self.stats.null_items,
            'duration_seconds': self.stats.get_duration()
        }
        
        return result
    
    def process_batch(
        self,
        responses: List[Any],
        required_fields: Optional[Set[str]] = None,
        normalize: bool = True
    ) -> Dict[str, Any]:
        """
        Process multiple API responses as a batch.
        
        Args:
            responses: List of API responses.
            required_fields: Optional required fields for validation.
            normalize: Whether to normalize the data.
        
        Returns:
            Dictionary containing combined results and statistics.
        
        Example:
            >>> results = processor.process_batch(responses)
            >>> print(f"Total items: {results['total_items']}")
        """
        all_items: List[Dict[str, Any]] = []
        all_errors: List[str] = []
        total_processed = 0
        total_valid = 0
        
        for response in responses:
            result = self.process_response(
                response, 
                required_fields=required_fields,
                normalize=normalize
            )
            
            if result['data']:
                if isinstance(result['data'], list):
                    all_items.extend(result['data'])
                else:
                    all_items.append(result['data'])
            
            all_errors.extend(result['errors'])
            total_processed += result['stats'].get('total_processed', 0)
            if result['valid']:
                total_valid += 1
        
        return {
            'data': all_items,
            'total_items': len(all_items),
            'total_responses': len(responses),
            'valid_responses': total_valid,
            'errors': all_errors,
            'success_rate': (total_valid / len(responses) * 100) if responses else 0
        }
    
    def filter_items(
        self,
        items: List[Dict[str, Any]],
        filter_func: Callable[[Dict[str, Any]], bool]
    ) -> List[Dict[str, Any]]:
        """
        Filter items using a custom filter function.
        
        Args:
            items: List of items to filter.
            filter_func: Function that returns True for items to keep.
        
        Returns:
            Filtered list of items.
        
        Example:
            >>> filtered = processor.filter_items(
            ...     items,
            ...     lambda x: x.get('type') == 'movie'
            ... )
        """
        return [item for item in items if filter_func(item)]
    
    def sort_items(
        self,
        items: List[Dict[str, Any]],
        sort_key: str = 'id',
        reverse: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Sort items by a specific key.
        
        Args:
            items: List of items to sort.
            sort_key: Key to sort by.
            reverse: Whether to reverse the sort order.
        
        Returns:
            Sorted list of items.
        
        Example:
            >>> sorted_items = processor.sort_items(items, 'date', reverse=True)
        """
        try:
            return sorted(
                items, 
                key=lambda x: x.get(sort_key, ''), 
                reverse=reverse
            )
        except Exception as e:
            logger.error(f"Sort error: {e}")
            return items


def parse_json(
    text: str, 
    default: Any = None,
    log_errors: bool = True
) -> Any:
    """
    Safely parse JSON text with error handling.
    
    This is a convenience function for parsing JSON with minimal
    error handling.
    
    Args:
        text: JSON string to parse.
        default: Default value to return on error.
        log_errors: Whether to log parsing errors.
    
    Returns:
        Parsed JSON data, or default value on error.
    
    Example:
        >>> data = parse_json('{"key": "value"}')
        >>> data = parse_json('invalid', default={})
    """
    try:
        return json.loads(text)
    except (json.JSONDecodeError, ValueError) as e:
        if log_errors:
            logger.error(f"JSON parse error: {e}")
        return default


def validate_response(
    data: Any,
    required_fields: Set[str],
    allow_empty: bool = True
) -> bool:
    """
    Validate API response against required fields.
    
    This is a convenience function for quick validation.
    
    Args:
        data: Data to validate.
        required_fields: Set of required field names.
        allow_empty: Whether to allow empty/null data.
    
    Returns:
        True if valid, False otherwise.
    
    Example:
        >>> is_valid = validate_response(data, {'id', 'title'})
    """
    if data is None:
        return allow_empty
    
    if isinstance(data, list):
        if not data:
            return allow_empty
        # Check first item
        if isinstance(data[0], dict):
            return all(field in data[0] for field in required_fields)
        return False
    
    if isinstance(data, dict):
        return all(field in data and data[field] is not None for field in required_fields)
    
    return False
