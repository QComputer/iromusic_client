"""
File Handling module for iromusic_client.

This module provides functionality for managing output files including
timestamped filenames, dated directory structures, atomic writes,
and file permission validation.

Classes:
    FileWriteError: Exception raised when file operations fail.
    FileHandler: Main class for file operations.
    DirectoryManager: Manager for dated directory structures.

Functions:
    generate_timestamp_filename: Generate filename with ISO 8601 timestamp.
    ensure_directory_exists: Ensure a directory exists, creating if needed.

Example:
    >>> from iromusic_client.file_handler import FileHandler
    >>> handler = FileHandler(output_dir='./output')
    >>> filepath = handler.save_json(data, 'movies')
"""

import os
import json
import tempfile
import shutil
import logging
from typing import Optional, Dict, Any, List, Union
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass


# Module logger
logger = logging.getLogger(__name__)


class FileWriteError(Exception):
    """
    Exception raised when file write operations fail.
    
    This exception is raised when there are issues writing files,
    including permission errors, disk space issues, or invalid paths.
    
    Attributes:
        message (str): Human-readable error message.
        filepath (Optional[Path]): The file path that caused the error.
        original_error (Optional[Exception]): The original exception.
    
    Example:
        >>> try:
        ...     handler.save_json(data, 'output')
        ... except FileWriteError as e:
        ...     print(f"Failed to write: {e.filepath}")
    """
    
    def __init__(
        self, 
        message: str, 
        filepath: Optional[Path] = None,
        original_error: Optional[Exception] = None
    ) -> None:
        """
        Initialize the FileWriteError.
        
        Args:
            message: Error message describing the failure.
            filepath: The file path that caused the error.
            original_error: The original exception if any.
        """
        super().__init__(message)
        self.message = message
        self.filepath = filepath
        self.original_error = original_error


@dataclass
class FileStats:
    """
    Statistics for file operations.
    
    Tracks metrics about file operations including bytes written,
    files created, and operation times.
    
    Attributes:
        files_written (int): Number of files written.
        bytes_written (int): Total bytes written.
        files_read (int): Number of files read.
        bytes_read (int): Total bytes read.
        errors (int): Number of file operation errors.
        start_time (datetime): When operations started.
    
    Example:
        >>> stats = FileStats()
        >>> stats.files_written += 1
    """
    files_written: int = 0
    bytes_written: int = 0
    files_read: int = 0
    bytes_read: int = 0
    errors: int = 0
    start_time: datetime = None
    
    def __post_init__(self):
        if self.start_time is None:
            self.start_time = datetime.now()


def generate_timestamp_filename(
    base_name: str,
    extension: str,
    timestamp: Optional[datetime] = None,
    include_time: bool = True
) -> str:
    """
    Generate a filename with ISO 8601 timestamp.
    
    Args:
        base_name: Base name for the file (without extension).
        extension: File extension (with or without leading dot).
        timestamp: Optional timestamp to use (defaults to now).
        include_time: Whether to include time in the timestamp.
    
    Returns:
        Filename with timestamp in format: base_name_YYYY-MM-DD_HH-MM-SS.ext
    
    Example:
        >>> generate_timestamp_filename('movies', 'json')
        'movies_2024-01-15_14-30-45.json'
        >>> generate_timestamp_filename('data', '.csv')
        'data_2024-01-15.csv'
    """
    if timestamp is None:
        timestamp = datetime.now()
    
    # Normalize extension
    if extension and not extension.startswith('.'):
        extension = '.' + extension
    
    # Format timestamp
    if include_time:
        ts_str = timestamp.strftime('%Y-%m-%d_%H-%M-%S')
    else:
        ts_str = timestamp.strftime('%Y-%m-%d')
    
    return f"{base_name}_{ts_str}{extension}"


def ensure_directory_exists(directory: Union[str, Path]) -> Path:
    """
    Ensure a directory exists, creating it if necessary.
    
    Args:
        directory: Path to the directory.
    
    Returns:
        Path: The directory path.
    
    Raises:
        FileWriteError: If directory cannot be created.
    
    Example:
        >>> path = ensure_directory_exists('./output/data')
        >>> print(path)
        PosixPath('./output/data')
    """
    dir_path = Path(directory)
    
    try:
        dir_path.mkdir(parents=True, exist_ok=True)
        logger.debug(f"Directory ensured: {dir_path}")
        return dir_path
    except OSError as e:
        raise FileWriteError(
            f"Failed to create directory: {directory}",
            filepath=dir_path,
            original_error=e
        )


def check_write_permission(directory: Union[str, Path]) -> bool:
    """
    Check if we have write permission for a directory.
    
    Args:
        directory: Path to check.
    
    Returns:
        bool: True if we can write to the directory.
    
    Example:
        >>> if check_write_permission('./output'):
        ...     print("Can write!")
    """
    dir_path = Path(directory)
    
    # Check if directory exists
    if dir_path.exists():
        return os.access(dir_path, os.W_OK)
    
    # Check parent directory
    parent = dir_path.parent
    if parent.exists():
        return os.access(parent, os.W_OK)
    
    # Check current directory
    return os.access('.', os.W_OK)


class DirectoryManager:
    """
    Manager for dated directory structures.
    
    This class organizes output into dated directory structures,
    creating directories like ./output/2024/01/15/ for easy organization.
    
    Attributes:
        base_dir (Path): Base output directory.
        date_format (str): Date format for subdirectories.
    
    Example:
        >>> manager = DirectoryManager('./output')
        >>> dir_path = manager.get_dated_dir()
        >>> print(dir_path)
        PosixPath('./output/2024/01/15')
    """
    
    def __init__(
        self, 
        base_dir: Union[str, Path],
        date_format: str = '%Y/%m/%d',
        create_dirs: bool = True
    ) -> None:
        """
        Initialize the DirectoryManager.
        
        Args:
            base_dir: Base directory for output.
            date_format: strftime format for date subdirectories.
            create_dirs: Whether to create directories on initialization.
        """
        self.base_dir = Path(base_dir)
        self.date_format = date_format
        
        if create_dirs:
            ensure_directory_exists(self.base_dir)
    
    def get_dated_dir(
        self, 
        date: Optional[datetime] = None,
        relative: bool = False
    ) -> Path:
        """
        Get or create a dated directory path.
        
        Args:
            date: Optional date (defaults to now).
            relative: If True, return relative path from base_dir.
        
        Returns:
            Path: The dated directory path.
        
        Example:
            >>> manager = DirectoryManager('./output')
            >>> manager.get_dated_dir()
            PosixPath('./output/2024/01/15')
        """
        if date is None:
            date = datetime.now()
        
        date_path = date.strftime(self.date_format)
        full_path = self.base_dir / date_path
        
        ensure_directory_exists(full_path)
        
        if relative:
            return Path(date_path)
        
        return full_path
    
    def list_dated_dirs(
        self, 
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> List[Path]:
        """
        List dated directories within a date range.
        
        Args:
            start_date: Start of date range (inclusive).
            end_date: End of date range (inclusive).
        
        Returns:
            List of directory paths.
        
        Example:
            >>> dirs = manager.list_dated_dirs(
            ...     start_date=datetime(2024, 1, 1),
            ...     end_date=datetime(2024, 1, 31)
            ... )
        """
        dirs: List[Path] = []
        
        if not self.base_dir.exists():
            return dirs
        
        for item in self.base_dir.rglob('*'):
            if item.is_dir():
                # Check date range
                if start_date or end_date:
                    try:
                        # Try to parse directory name as date
                        dir_date = datetime.strptime(item.name, '%Y-%m-%d')
                        
                        if start_date and dir_date < start_date:
                            continue
                        if end_date and dir_date > end_date:
                            continue
                    except ValueError:
                        continue
                
                dirs.append(item)
        
        return sorted(dirs)


class FileHandler:
    """
    Main handler for file operations.
    
    This class provides methods for saving and loading data with
    support for atomic writes, timestamps, and organized directory structures.
    
    Attributes:
        output_dir (Path): Base output directory.
        directory_manager (DirectoryManager): Manager for dated directories.
        stats (FileStats): Statistics for file operations.
    
    Example:
        >>> handler = FileHandler('./output')
        >>> filepath = handler.save_json(data, 'movies')
    """
    
    def __init__(
        self, 
        output_dir: Union[str, Path] = './output',
        use_dated_dirs: bool = True,
        validate_permissions: bool = True
    ) -> None:
        """
        Initialize the FileHandler.
        
        Args:
            output_dir: Base output directory.
            use_dated_dirs: Whether to organize by dated subdirectories.
            validate_permissions: Whether to check write permissions.
        
        Raises:
            FileWriteError: If output directory is not writable.
        """
        self.output_dir = Path(output_dir)
        self.use_dated_dirs = use_dated_dirs
        self.directory_manager = DirectoryManager(self.output_dir)
        self.stats = FileStats()
        
        # Validate permissions
        if validate_permissions:
            if not ensure_directory_exists(self.output_dir):
                raise FileWriteError(
                    f"Cannot write to output directory: {output_dir}"
                )
            
            if not check_write_permission(self.output_dir):
                raise FileWriteError(
                    f"No write permission for: {output_dir}"
                )
        
        logger.info(f"FileHandler initialized with output_dir: {self.output_dir}")
    
    def _get_output_path(
        self,
        filename: str,
        subdirectory: Optional[str] = None,
        use_timestamp: bool = True
    ) -> Path:
        """
        Get the full output path for a file.
        
        Args:
            filename: Name of the file.
            subdirectory: Optional subdirectory within output.
            use_timestamp: Whether to add timestamp to filename.
        
        Returns:
            Full path for the file.
        """
        # Get directory
        if self.use_dated_dirs:
            directory = self.directory_manager.get_dated_dir()
        else:
            directory = self.output_dir
        
        if subdirectory:
            directory = directory / subdirectory
            ensure_directory_exists(directory)
        
        # Handle filename
        if use_timestamp:
            # Parse base name and extension
            if '.' in filename:
                parts = filename.rsplit('.', 1)
                base_name = parts[0]
                extension = parts[1]
            else:
                base_name = filename
                extension = ''
            
            filename = generate_timestamp_filename(base_name, extension)
        
        return directory / filename
    
    def save_json(
        self,
        data: Any,
        filename: str,
        subdirectory: Optional[str] = None,
        indent: int = 2,
        use_timestamp: bool = True,
        atomic: bool = True
    ) -> Path:
        """
        Save data as JSON file.
        
        Args:
            data: Data to save (must be JSON serializable).
            filename: Name of the output file.
            subdirectory: Optional subdirectory within output.
            indent: JSON indentation level.
            use_timestamp: Whether to add timestamp to filename.
            atomic: Whether to use atomic write (temp file + rename).
        
        Returns:
            Path: Path to the saved file.
        
        Raises:
            FileWriteError: If write operation fails.
            TypeError: If data is not JSON serializable.
        
        Example:
            >>> handler = FileHandler('./output')
            >>> filepath = handler.save_json(
            ...     {'items': data},
            ...     'movies.json',
            ...     subdirectory='api_data'
            ... )
        """
        filepath = self._get_output_path(filename, subdirectory, use_timestamp)
        
        # Serialize data
        try:
            json_str = json.dumps(data, indent=indent, ensure_ascii=False)
        except TypeError as e:
            raise FileWriteError(
                f"Data is not JSON serializable: {e}",
                filepath=filepath
            )
        
        # Write file
        if atomic:
            filepath = self._atomic_write(filepath, json_str)
        else:
            try:
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(json_str)
            except OSError as e:
                raise FileWriteError(
                    f"Failed to write file: {e}",
                    filepath=filepath,
                    original_error=e
                )
        
        self.stats.files_written += 1
        self.stats.bytes_written += filepath.stat().st_size
        
        logger.info(f"Saved JSON to: {filepath}")
        return filepath
    
    def save_lines(
        self,
        lines: List[str],
        filename: str,
        subdirectory: Optional[str] = None,
        use_timestamp: bool = True,
        atomic: bool = True
    ) -> Path:
        """
        Save data as line-separated text file.
        
        Args:
            lines: List of lines to write.
            filename: Name of the output file.
            subdirectory: Optional subdirectory within output.
            use_timestamp: Whether to add timestamp to filename.
            atomic: Whether to use atomic write.
        
        Returns:
            Path: Path to the saved file.
        
        Raises:
            FileWriteError: If write operation fails.
        """
        filepath = self._get_output_path(filename, subdirectory, use_timestamp)
        
        content = '\n'.join(lines)
        
        if atomic:
            filepath = self._atomic_write(filepath, content)
        else:
            try:
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(content)
            except OSError as e:
                raise FileWriteError(
                    f"Failed to write file: {e}",
                    filepath=filepath,
                    original_error=e
                )
        
        self.stats.files_written += 1
        self.stats.bytes_written += filepath.stat().st_size
        
        logger.info(f"Saved lines to: {filepath}")
        return filepath
    
    def _atomic_write(
        self, 
        filepath: Path, 
        content: str
    ) -> Path:
        """
        Perform atomic write using temporary file.
        
        Writes to a temporary file first, then renames to the target.
        This ensures the target file is never in a partially written state.
        
        Args:
            filepath: Target file path.
            content: Content to write.
        
        Returns:
            Path: Path to the written file.
        
        Raises:
            FileWriteError: If atomic write fails.
        """
        # Get directory
        directory = filepath.parent
        
        try:
            # Create temp file in same directory (for atomic rename)
            fd, temp_path = tempfile.mkstemp(
                dir=directory,
                prefix='.tmp_',
                suffix=filepath.suffix
            )
            
            try:
                # Write to temp file
                with os.fdopen(fd, 'w', encoding='utf-8') as f:
                    f.write(content)
                
                # Atomic rename
                shutil.move(temp_path, filepath)
                
            except Exception:
                # Clean up temp file on error
                try:
                    os.unlink(temp_path)
                except OSError:
                    pass
                raise
                
        except OSError as e:
            raise FileWriteError(
                f"Failed to perform atomic write: {e}",
                filepath=filepath,
                original_error=e
            )
        
        return filepath
    
    def load_json(
        self,
        filepath: Union[str, Path]
    ) -> Any:
        """
        Load data from a JSON file.
        
        Args:
            filepath: Path to the JSON file.
        
        Returns:
            Parsed JSON data.
        
        Raises:
            FileWriteError: If file cannot be read.
            json.JSONDecodeError: If JSON is invalid.
        
        Example:
            >>> data = handler.load_json('./output/data.json')
        """
        path = Path(filepath)
        
        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            self.stats.files_read += 1
            self.stats.bytes_read += path.stat().st_size
            
            return data
            
        except OSError as e:
            raise FileWriteError(
                f"Failed to read file: {e}",
                filepath=path,
                original_error=e
            )
    
    def list_files(
        self,
        pattern: str = '*',
        subdirectory: Optional[str] = None,
        recursive: bool = False
    ) -> List[Path]:
        """
        List files matching a pattern.
        
        Args:
            pattern: Glob pattern to match.
            subdirectory: Optional subdirectory to search.
            recursive: Whether to search recursively.
        
        Returns:
            List of matching file paths.
        
        Example:
            >>> files = handler.list_files('*.json', recursive=True)
        """
        search_dir = self.output_dir
        
        if subdirectory:
            search_dir = search_dir / subdirectory
        
        if not search_dir.exists():
            return []
        
        if recursive:
            return sorted(search_dir.rglob(pattern))
        else:
            return sorted(search_dir.glob(pattern))
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get file operation statistics.
        
        Returns:
            Dictionary containing statistics.
        """
        return {
            'files_written': self.stats.files_written,
            'bytes_written': self.stats.bytes_written,
            'files_read': self.stats.files_read,
            'bytes_read': self.stats.bytes_read,
            'errors': self.stats.errors,
            'start_time': self.stats.start_time.isoformat() if self.stats.start_time else None
        }
    
    def cleanup(
        self, 
        older_than_days: Optional[int] = None,
        pattern: Optional[str] = None
    ) -> int:
        """
        Clean up old files.
        
        Args:
            older_than_days: Remove files older than this many days.
            pattern: Optional glob pattern to filter files.
        
        Returns:
            Number of files removed.
        
        Example:
            >>> removed = handler.cleanup(older_than_days=30, pattern='*.json')
        """
        if not older_than_days:
            return 0
        
        cutoff = datetime.now().timestamp() - (older_than_days * 86400)
        removed = 0
        
        files = self.list_files(pattern=pattern or '*', recursive=True)
        
        for filepath in files:
            try:
                if filepath.stat().st_mtime < cutoff:
                    filepath.unlink()
                    removed += 1
            except OSError as e:
                logger.warning(f"Failed to remove {filepath}: {e}")
        
        logger.info(f"Cleaned up {removed} files")
        return removed


def create_file_handler(
    output_dir: Optional[str] = None,
    config: Optional[Any] = None
) -> FileHandler:
    """
    Factory function to create a FileHandler instance.
    
    Args:
        output_dir: Optional output directory path.
        config: Optional configuration object.
    
    Returns:
        Configured FileHandler instance.
    
    Example:
        >>> handler = create_file_handler('./output')
    """
    if output_dir is None and config:
        output_dir = str(config.output_dir)
    
    return FileHandler(output_dir=output_dir or './output')
