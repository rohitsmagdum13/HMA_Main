"""Custom exceptions for HMA ingestion system."""
from typing import Optional, Any


class HMAIngestionError(Exception):
    """Base exception for all HMA ingestion errors."""
    
    def __init__(self, message: str, details: Optional[dict[str, Any]] = None):
        """Initialize exception with message and optional details."""
        super().__init__(message)
        self.message = message
        self.details = details or {}


class ConfigError(HMAIngestionError):
    """Raised when configuration is invalid or missing."""
    pass


class UploadError(HMAIngestionError):
    """Raised when S3 upload fails."""
    pass


class FileDiscoveryError(HMAIngestionError):
    """Raised when file discovery or processing fails."""
    pass


class QueueError(HMAIngestionError):
    """Raised when queue operations fail."""
    pass