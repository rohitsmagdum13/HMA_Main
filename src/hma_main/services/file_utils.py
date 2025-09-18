"""
File discovery and processing utilities.

This module provides helper functions to:
- Recursively discover files in a directory.
- Detect file type based on extension.
- Detect scope (mba/policy) from path.
- Build structured S3 object keys.
- Parse file extension filters.

Logging is used for detailed insights, and exceptions are raised
with context when discovery fails.
"""

from pathlib import Path
from typing import List, Set, Optional

from ..core.logging_config import get_logger
from ..core.exceptions import FileDiscoveryError

# Initialize logger for this module
logger = get_logger(__name__)

# File type mapping: extension -> category
FILE_TYPE_MAPPING = {
    ".pdf": "pdf",
    ".png": "image",
    ".jpg": "image",
    ".jpeg": "image",
    ".gif": "image",
    ".bmp": "image",
    ".tiff": "image",
    ".csv": "csv",
    ".json": "json",
    ".txt": "text",
    ".log": "text",
    ".md": "text",
    ".docx": "docx",
    ".doc": "docx",
    ".xlsx": "excel",
    ".xls": "excel",
    ".pptx": "powerpoint",
    ".ppt": "powerpoint",
    ".xml": "xml",
    ".yaml": "yaml",
    ".yml": "yaml",
}


def discover_files(
    input_dir: Path,
    include_extensions: Optional[Set[str]] = None,
    exclude_extensions: Optional[Set[str]] = None,
    scope: Optional[str] = None
) -> List[Path]:
    """
    Recursively discover files in a directory with optional filtering.

    Args:
        input_dir: Directory to scan.
        include_extensions: If provided, only include these extensions.
        exclude_extensions: If provided, exclude these extensions.
        scope: Optional scope subdirectory ("mba" or "policy").

    Returns:
        List of discovered file paths.

    Raises:
        FileDiscoveryError: If directory doesn't exist or is invalid.
    """
    # Validate that the path exists and is a directory
    if not input_dir.exists():
        raise FileDiscoveryError(f"Input directory does not exist: {input_dir}")
    if not input_dir.is_dir():
        raise FileDiscoveryError(f"Input path is not a directory: {input_dir}")

    # If scope is provided, adjust scanning directory
    if scope:
        scope_dir = input_dir / scope
        if scope_dir.exists() and scope_dir.is_dir():
            logger.info(f"Scanning scope-specific directory: {scope_dir}")
            scan_dir = scope_dir
        else:
            logger.warning(f"Scope directory {scope_dir} not found, scanning entire {input_dir}")
            scan_dir = input_dir
    else:
        scan_dir = input_dir

    discovered_files: List[Path] = []

    try:
        # Recursively walk the directory
        for file_path in scan_dir.rglob("*"):
            if not file_path.is_file():
                continue

            extension = file_path.suffix.lower()

            # Skip files with no extension
            if not extension:
                logger.debug(f"Skipping {file_path.name} - no extension")
                continue

            # Apply include filter if present
            if include_extensions:
                normalized_includes = {f".{ext.lstrip('.')}" for ext in include_extensions}
                if extension not in normalized_includes:
                    logger.debug(f"Skipping {file_path.name} - not in include list")
                    continue

            # Apply exclude filter if present
            if exclude_extensions:
                normalized_excludes = {f".{ext.lstrip('.')}" for ext in exclude_extensions}
                if extension in normalized_excludes:
                    logger.debug(f"Skipping {file_path.name} - in exclude list")
                    continue

            discovered_files.append(file_path)
            logger.debug(f"Discovered: {file_path.relative_to(input_dir)}")

    except Exception as exc:
        # Wrap errors in FileDiscoveryError with directory context
        raise FileDiscoveryError(f"Error scanning directory: {exc}", {"directory": str(scan_dir)})

    logger.info(f"Discovered {len(discovered_files)} files in {scan_dir}")
    return discovered_files


def detect_file_type(file_path: Path) -> str:
    """
    Detect file type category based on extension.

    Args:
        file_path: Path to file.

    Returns:
        File type category (e.g., "pdf", "image", "other").
    """
    extension = file_path.suffix.lower()
    file_type = FILE_TYPE_MAPPING.get(extension, "other")
    logger.debug(f"File {file_path.name} detected as type: {file_type}")
    return file_type


def detect_scope_from_path(file_path: Path, input_dir: Path) -> Optional[str]:
    """
    Detect scope ("mba" or "policy") from file path relative to input_dir.

    Args:
        file_path: Path to file.
        input_dir: Base input directory.

    Returns:
        Scope if detected, None otherwise.
    """
    try:
        relative_path = file_path.relative_to(input_dir)
        parts = relative_path.parts
        if parts and parts[0].lower() in ("mba", "policy"):
            detected_scope = parts[0].lower()
            logger.debug(f"Detected scope '{detected_scope}' from path: {relative_path}")
            return detected_scope
    except ValueError:
        logger.debug(f"File {file_path} not relative to {input_dir}")

    # Try parent directories
    for parent in file_path.parents:
        parent_name = parent.name.lower()
        if parent_name in ("mba", "policy"):
            logger.debug(f"Detected scope '{parent_name}' from parent directory")
            return parent_name

    logger.debug(f"Could not detect scope for file: {file_path}")
    return None


def build_s3_key(scope: str, file_path: Path, prefix: str = "", auto_detect_type: bool = True) -> str:
    """
    Build structured S3 object key.

    Args:
        scope: "mba" or "policy".
        file_path: Local file path.
        prefix: Optional prefix (defaults to scope).
        auto_detect_type: If True, include file type category in key.

    Returns:
        S3 object key string.
    """
    base_prefix = prefix or f"{scope}/"
    if not base_prefix.endswith("/"):
        base_prefix += "/"

    if auto_detect_type:
        file_type = detect_file_type(file_path)
        s3_key = f"{base_prefix}{file_type}/{file_path.name}"
    else:
        s3_key = f"{base_prefix}{file_path.name}"

    logger.debug(f"Built S3 key: {s3_key}")
    return s3_key


def parse_extensions(extensions_str: str) -> Set[str]:
    """
    Parse comma-separated extensions into a normalized set.

    Args:
        extensions_str: e.g. "pdf,csv,docx".

    Returns:
        Set of normalized extensions with leading dot.
    """
    if not extensions_str:
        return set()

    extensions: Set[str] = set()
    for ext in extensions_str.split(","):
        ext = ext.strip().lower()
        if ext:
            if not ext.startswith("."):
                ext = f".{ext}"
            extensions.add(ext)

    return extensions
