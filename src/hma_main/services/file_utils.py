"""File discovery and processing utilities."""
from pathlib import Path
from typing import List, Set, Optional, Tuple

from ..core.logging_config import get_logger
from ..core.exceptions import FileDiscoveryError

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
    scope: Optional[str] = None  # Added scope parameter
) -> List[Path]:
    """
    Recursively discover files in directory with optional filtering.
    
    Args:
        input_dir: Directory to scan
        include_extensions: If provided, only include these extensions
        exclude_extensions: If provided, exclude these extensions
        scope: If provided, only scan within the scope subdirectory (mba or policy)
        
    Returns:
        List of discovered file paths
        
    Raises:
        FileDiscoveryError: If directory doesn't exist or can't be read
    """
    # Validate input directory
    if not input_dir.exists():
        raise FileDiscoveryError(f"Input directory does not exist: {input_dir}")
    
    if not input_dir.is_dir():
        raise FileDiscoveryError(f"Input path is not a directory: {input_dir}")
    
    # If scope is provided, look for files in the scope subdirectory
    if scope:
        scope_dir = input_dir / scope
        if scope_dir.exists() and scope_dir.is_dir():
            logger.info(f"Scanning scope-specific directory: {scope_dir}")
            scan_dir = scope_dir
        else:
            # If scope directory doesn't exist, scan entire input_dir
            logger.warning(f"Scope directory {scope_dir} not found, scanning entire {input_dir}")
            scan_dir = input_dir
    else:
        scan_dir = input_dir
    
    discovered_files = []
    
    try:
        # Recursively find all files
        for file_path in scan_dir.rglob("*"):
            # Skip directories
            if not file_path.is_file():
                continue
            
            # Get file extension (lowercase, with dot)
            extension = file_path.suffix.lower()
            
            # Skip files without extensions
            if not extension:
                logger.debug(f"Skipping {file_path.name} - no extension")
                continue
            
            # Apply include filter if specified
            if include_extensions:
                # Normalize extensions (ensure they have dots)
                normalized_includes = {f".{ext.lstrip('.')}" for ext in include_extensions}
                if extension not in normalized_includes:
                    logger.debug(f"Skipping {file_path.name} - not in include list")
                    continue
            
            # Apply exclude filter if specified
            if exclude_extensions:
                # Normalize extensions (ensure they have dots)
                normalized_excludes = {f".{ext.lstrip('.')}" for ext in exclude_extensions}
                if extension in normalized_excludes:
                    logger.debug(f"Skipping {file_path.name} - in exclude list")
                    continue
            
            discovered_files.append(file_path)
            logger.debug(f"Discovered: {file_path.relative_to(input_dir)}")
            
    except Exception as e:
        raise FileDiscoveryError(f"Error scanning directory: {e}", {"directory": str(scan_dir)})
    
    logger.info(f"Discovered {len(discovered_files)} files in {scan_dir}")
    return discovered_files


def detect_file_type(file_path: Path) -> str:
    """
    Detect file type category based on extension.
    
    Args:
        file_path: Path to file
        
    Returns:
        File type category (e.g., "pdf", "image", "other")
    """
    # Get extension in lowercase with dot
    extension = file_path.suffix.lower()
    
    # Look up in mapping, default to "other"
    file_type = FILE_TYPE_MAPPING.get(extension, "other")
    
    logger.debug(f"File {file_path.name} detected as type: {file_type}")
    return file_type


def detect_scope_from_path(file_path: Path, input_dir: Path) -> Optional[str]:
    """
    Detect scope (mba/policy) from file path if it's in a scope directory.
    
    Args:
        file_path: Full path to file
        input_dir: Base input directory
        
    Returns:
        Scope if detected ("mba" or "policy"), None otherwise
    """
    try:
        # Get relative path from input directory
        relative_path = file_path.relative_to(input_dir)
        
        # Check if first part of path is a scope
        parts = relative_path.parts
        if parts and parts[0].lower() in ("mba", "policy"):
            detected_scope = parts[0].lower()
            logger.debug(f"Detected scope '{detected_scope}' from path: {relative_path}")
            return detected_scope
            
    except ValueError:
        # file_path is not relative to input_dir
        logger.debug(f"Could not determine relative path for {file_path}")
        pass
    
    # Try to detect from parent directories
    try:
        parents = file_path.parents
        for parent in parents:
            parent_name = parent.name.lower()
            if parent_name in ("mba", "policy"):
                logger.debug(f"Detected scope '{parent_name}' from parent directory")
                return parent_name
    except Exception as e:
        logger.debug(f"Error detecting scope from parents: {e}")
    
    logger.debug(f"Could not detect scope for file: {file_path}")
    return None


def build_s3_key(scope: str, file_path: Path, prefix: str = "", auto_detect_type: bool = True) -> str:
    """
    Build S3 object key from scope, file type, and filename.
    
    Args:
        scope: Either "mba" or "policy"
        file_path: Path to local file
        prefix: Optional prefix (defaults to scope)
        auto_detect_type: If True, detect file type and add to path
        
    Returns:
        S3 object key (e.g., "mba/pdf/report.pdf")
    """
    # Use provided prefix or default to scope
    base_prefix = prefix if prefix else f"{scope}/"
    
    # Ensure prefix ends with slash
    if not base_prefix.endswith("/"):
        base_prefix += "/"
    
    if auto_detect_type:
        # Detect file type
        file_type = detect_file_type(file_path)
        
        # Build key: prefix + type + filename
        s3_key = f"{base_prefix}{file_type}/{file_path.name}"
    else:
        # Build key without type directory: prefix + filename
        s3_key = f"{base_prefix}{file_path.name}"
    
    logger.debug(f"Built S3 key: {s3_key}")
    return s3_key


def parse_extensions(extensions_str: str) -> Set[str]:
    """
    Parse comma-separated extension string into set.
    
    Args:
        extensions_str: Comma-separated extensions (e.g., "pdf,png,jpg")
        
    Returns:
        Set of normalized extensions with dots
    """
    if not extensions_str:
        return set()
    
    # Split by comma, strip whitespace, ensure dot prefix
    extensions = set()
    for ext in extensions_str.split(","):
        ext = ext.strip().lower()
        if ext:
            # Ensure extension starts with dot
            if not ext.startswith("."):
                ext = f".{ext}"
            extensions.add(ext)
    
    return extensions