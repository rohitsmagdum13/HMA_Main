"""Duplicate file detection for local and S3 storage."""
import hashlib
from pathlib import Path
from typing import Dict, Set, List, Tuple, Optional
from datetime import datetime
import json
import boto3

from ..core.logging_config import get_logger
from .s3_client import check_s3_file_exists, list_s3_files, calculate_file_hash

logger = get_logger(__name__)


class DuplicateDetector:
    """Handles duplicate detection for files in local and S3 storage."""
    
    def __init__(self, cache_file: Optional[Path] = None):
        """
        Initialize duplicate detector.
        
        Args:
            cache_file: Optional path to cache file for storing file hashes
        """
        self.cache_file = cache_file or Path("logs/file_cache.json")
        self.local_cache: Dict[str, Dict] = {}
        self.s3_cache: Dict[str, Dict] = {}
        self._load_cache()
    
    def _load_cache(self):
        """Load cache from file if it exists."""
        if self.cache_file.exists():
            try:
                with open(self.cache_file, 'r') as f:
                    data = json.load(f)
                    self.local_cache = data.get('local', {})
                    self.s3_cache = data.get('s3', {})
                logger.debug(f"Loaded cache with {len(self.local_cache)} local and {len(self.s3_cache)} S3 entries")
            except Exception as e:
                logger.warning(f"Could not load cache file: {e}")
    
    def _save_cache(self):
        """Save cache to file."""
        try:
            self.cache_file.parent.mkdir(exist_ok=True)
            with open(self.cache_file, 'w') as f:
                json.dump({
                    'local': self.local_cache,
                    's3': self.s3_cache,
                    'updated': datetime.now().isoformat()
                }, f, indent=2, default=str)
            logger.debug("Cache saved successfully")
        except Exception as e:
            logger.warning(f"Could not save cache file: {e}")
    
    def scan_local_directory(self, directory: Path, recursive: bool = True) -> Dict[str, List[Path]]:
        """
        Scan local directory for files and group duplicates by hash.
        
        Args:
            directory: Directory to scan
            recursive: If True, scan recursively
            
        Returns:
            Dictionary mapping file hash to list of file paths
        """
        hash_to_files: Dict[str, List[Path]] = {}
        
        # Ensure directory is a Path object and resolve it
        directory = Path(directory).resolve()
        
        # Get all files
        if recursive:
            files = [f for f in directory.rglob("*") if f.is_file()]
        else:
            files = [f for f in directory.glob("*") if f.is_file()]
        
        logger.info(f"Scanning {len(files)} files in {directory}")
        
        for file_path in files:
            try:
                # Ensure file_path is absolute
                file_path = file_path.resolve()
                
                # Get file info
                stat = file_path.stat()
                file_key = str(file_path.absolute())
                
                # Check if we have cached hash for this file
                cached = self.local_cache.get(file_key, {})
                if (cached.get('size') == stat.st_size and 
                    cached.get('mtime') == stat.st_mtime):
                    # Use cached hash
                    file_hash = cached.get('hash', '')
                    logger.debug(f"Using cached hash for {file_path.name}")
                else:
                    # Calculate new hash
                    file_hash = calculate_file_hash(file_path)
                    
                    # Update cache
                    self.local_cache[file_key] = {
                        'hash': file_hash,
                        'size': stat.st_size,
                        'mtime': stat.st_mtime,
                        'path': str(file_path)
                    }
                
                # Group by hash
                if file_hash:
                    if file_hash not in hash_to_files:
                        hash_to_files[file_hash] = []
                    hash_to_files[file_hash].append(file_path)
                    
            except Exception as e:
                logger.error(f"Error processing {file_path}: {e}")
        
        # Save updated cache
        self._save_cache()
        
        # Find duplicates
        duplicates = {
            hash_val: paths 
            for hash_val, paths in hash_to_files.items() 
            if len(paths) > 1
        }
        
        if duplicates:
            logger.warning(f"Found {len(duplicates)} sets of duplicate files")
            for hash_val, paths in duplicates.items():
                logger.warning(f"  Duplicate set ({len(paths)} files): {[p.name for p in paths]}")
        
        return hash_to_files
    
    def check_local_duplicate(self, file_path: Path, search_dirs: List[Path]) -> List[Path]:
        """
        Check if a file has duplicates in specified directories.
        
        Args:
            file_path: File to check
            search_dirs: Directories to search for duplicates
            
        Returns:
            List of duplicate file paths
        """
        # Ensure file_path is absolute
        file_path = file_path.resolve()
        
        # Calculate hash of the target file
        target_hash = calculate_file_hash(file_path)
        if not target_hash:
            return []
        
        duplicates = []
        
        for search_dir in search_dirs:
            search_dir = Path(search_dir).resolve()
            if not search_dir.exists():
                continue
            
            # Scan directory
            hash_to_files = self.scan_local_directory(search_dir)
            
            # Check for matching hash
            if target_hash in hash_to_files:
                for dup_path in hash_to_files[target_hash]:
                    if dup_path.absolute() != file_path.absolute():
                        duplicates.append(dup_path)
        
        if duplicates:
            logger.warning(f"File {file_path.name} has {len(duplicates)} local duplicates")
        
        return duplicates
    
    def check_s3_duplicate(
        self,
        session: 'boto3.Session',
        local_path: Path,
        bucket: str,
        s3_key: str
    ) -> Tuple[bool, Optional[Dict]]:
        """
        Check if a local file already exists in S3.
        
        Args:
            session: Boto3 session
            local_path: Local file path
            bucket: S3 bucket
            s3_key: S3 key to check
            
        Returns:
            Tuple of (is_duplicate, s3_metadata)
        """
        # First check if exact key exists
        exists, metadata = check_s3_file_exists(session, bucket, s3_key)
        
        if exists:
            # Compare sizes
            local_size = local_path.stat().st_size
            s3_size = metadata.get('size', 0)
            
            if local_size == s3_size:
                logger.info(f"File {local_path.name} matches S3 object (same size)")
                return True, metadata
            else:
                logger.info(f"File {local_path.name} exists in S3 with different size")
                return False, metadata
        
        return False, None
    
    def find_similar_s3_files(
        self,
        session: 'boto3.Session',
        local_path: Path,
        bucket: str,
        prefix: str = ""
    ) -> List[Dict]:
        """
        Find similar files in S3 based on name and size.
        
        Args:
            session: Boto3 session
            local_path: Local file to compare
            bucket: S3 bucket
            prefix: S3 prefix to search
            
        Returns:
            List of similar S3 objects
        """
        local_size = local_path.stat().st_size
        local_name = local_path.name.lower()
        
        # List S3 files
        s3_files = list_s3_files(session, bucket, prefix)
        
        similar = []
        for s3_file in s3_files:
            s3_name = Path(s3_file['key']).name.lower()
            s3_size = s3_file['size']
            
            # Check for same name or same size
            if s3_name == local_name:
                s3_file['similarity'] = 'same_name'
                similar.append(s3_file)
            elif s3_size == local_size:
                s3_file['similarity'] = 'same_size'
                similar.append(s3_file)
        
        if similar:
            logger.info(f"Found {len(similar)} similar files in S3 for {local_path.name}")
        
        return similar
    
    def generate_report(self, duplicates: Dict[str, List[Path]], base_dir: Optional[Path] = None) -> str:
        """
        Generate a duplicate detection report.
        
        Args:
            duplicates: Dictionary of hash to duplicate files
            base_dir: Optional base directory for relative paths
            
        Returns:
            Formatted report string
        """
        lines = ["=" * 50, "Duplicate Detection Report", "=" * 50]
        
        if not duplicates:
            lines.append("No duplicates found")
        else:
            total_duplicates = sum(len(paths) - 1 for paths in duplicates.values())
            lines.append(f"Found {total_duplicates} duplicate files in {len(duplicates)} groups")
            lines.append("")
            
            for idx, (hash_val, paths) in enumerate(duplicates.items(), 1):
                lines.append(f"Group {idx} ({len(paths)} files):")
                
                # Sort by modification time to identify original
                sorted_paths = sorted(paths, key=lambda p: p.stat().st_mtime)
                
                for i, path in enumerate(sorted_paths):
                    # Ensure path is absolute
                    path = path.resolve()
                    stat = path.stat()
                    marker = " (oldest)" if i == 0 else " (duplicate)"
                    
                    # Try to get relative path for display
                    display_path = str(path)
                    try:
                        if base_dir:
                            # Use provided base directory
                            display_path = str(path.relative_to(base_dir.resolve()))
                        else:
                            # Try to use current directory
                            cwd = Path.cwd()
                            if path.is_relative_to(cwd):
                                display_path = str(path.relative_to(cwd))
                            else:
                                # Just use the name if can't get relative path
                                display_path = path.name
                    except (ValueError, AttributeError):
                        # If relative_to fails or is_relative_to doesn't exist (Python < 3.9)
                        # Just use the file name
                        display_path = path.name
                    
                    lines.append(f"  - {display_path}{marker}")
                    lines.append(f"    Size: {stat.st_size:,} bytes")
                    lines.append(f"    Modified: {datetime.fromtimestamp(stat.st_mtime).strftime('%Y-%m-%d %H:%M:%S')}")
                lines.append("")
        
        lines.append("=" * 50)
        return "\n".join(lines)