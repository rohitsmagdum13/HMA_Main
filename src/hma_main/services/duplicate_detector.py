"""
Duplicate file detection for local and S3 storage.

This module provides functionality to:
- Detect duplicate files in local directories by computing file hashes.
- Maintain a cache of hashes to avoid recomputation.
- Check if a file already exists in S3 (duplicate check).
- Find similar S3 files by name or size.
- Generate human-readable reports of duplicate groups.

It supports both local filesystem and AWS S3 for duplication detection.
Detailed logging and exception handling are included for reliability.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import boto3

from ..core.logging_config import get_logger
from .s3_client import check_s3_file_exists, list_s3_files, calculate_file_hash

# Initialize logger for this module
logger = get_logger(__name__)


class DuplicateDetector:
    """
    Handles duplicate detection for files in both local storage and S3.
    
    Provides methods to scan directories, compare hashes, check S3 existence,
    and produce reports.
    """

    def __init__(self, cache_file: Optional[Path] = None) -> None:
        """
        Initialize duplicate detector.

        Args:
            cache_file: Optional path to JSON file for storing hash cache.
                        Defaults to "logs/file_cache.json".
        """
        # Path to the cache file
        self.cache_file = cache_file or Path("logs/file_cache.json")

        # Local and S3 hash caches (loaded from file if available)
        self.local_cache: Dict[str, Dict] = {}
        self.s3_cache: Dict[str, Dict] = {}

        # Load existing cache from file if present
        self._load_cache()

    def _load_cache(self) -> None:
        """Load hash cache from file if it exists."""
        if self.cache_file.exists():
            try:
                with open(self.cache_file, "r", encoding="utf-8") as file:
                    data = json.load(file)
                    self.local_cache = data.get("local", {})
                    self.s3_cache = data.get("s3", {})
                logger.debug(
                    "Loaded cache with %d local and %d S3 entries",
                    len(self.local_cache),
                    len(self.s3_cache),
                )
            except Exception as exc:
                logger.warning("Could not load cache file %s: %s", self.cache_file, exc)

    def _save_cache(self) -> None:
        """Save hash cache to file."""
        try:
            # Ensure parent directory exists
            self.cache_file.parent.mkdir(exist_ok=True)

            # Write JSON with updated cache
            with open(self.cache_file, "w", encoding="utf-8") as file:
                json.dump(
                    {
                        "local": self.local_cache,
                        "s3": self.s3_cache,
                        "updated": datetime.now().isoformat(),
                    },
                    file,
                    indent=2,
                    default=str,
                )
            logger.debug("Cache saved successfully to %s", self.cache_file)
        except Exception as exc:
            logger.warning("Could not save cache file %s: %s", self.cache_file, exc)

    def scan_local_directory(self, directory: Path, recursive: bool = True) -> Dict[str, List[Path]]:
        """
        Scan a local directory and group files by hash.

        Args:
            directory: Directory to scan.
            recursive: If True, search subdirectories recursively.

        Returns:
            Dictionary mapping file hash to list of file paths.
        """
        hash_to_files: Dict[str, List[Path]] = {}

        # Resolve absolute directory path
        directory = Path(directory).resolve()

        # Collect files
        files = (
            [f for f in directory.rglob("*") if f.is_file()]
            if recursive
            else [f for f in directory.glob("*") if f.is_file()]
        )

        logger.info("Scanning %d files in directory: %s", len(files), directory)

        for file_path in files:
            try:
                file_path = file_path.resolve()
                stat = file_path.stat()
                file_key = str(file_path)

                # Check cache for existing hash
                cached = self.local_cache.get(file_key, {})
                if cached.get("size") == stat.st_size and cached.get("mtime") == stat.st_mtime:
                    file_hash = cached.get("hash", "")
                    logger.debug("Using cached hash for %s", file_path.name)
                else:
                    # Compute new hash
                    file_hash = calculate_file_hash(file_path)

                    # Update cache entry
                    self.local_cache[file_key] = {
                        "hash": file_hash,
                        "size": stat.st_size,
                        "mtime": stat.st_mtime,
                        "path": str(file_path),
                    }

                # Group files by hash
                if file_hash:
                    hash_to_files.setdefault(file_hash, []).append(file_path)

            except Exception as exc:
                logger.error("Error processing file %s: %s", file_path, exc)

        # Save updated cache
        self._save_cache()

        # Log duplicate sets if found
        duplicates = {h: paths for h, paths in hash_to_files.items() if len(paths) > 1}
        if duplicates:
            logger.warning("Found %d sets of duplicate files", len(duplicates))
            for h, paths in duplicates.items():
                logger.warning("Duplicate group (%d files): %s", len(paths), [p.name for p in paths])

        return hash_to_files

    def check_local_duplicate(self, file_path: Path, search_dirs: List[Path]) -> List[Path]:
        """
        Check if a file has duplicates in given directories.

        Args:
            file_path: File to check.
            search_dirs: Directories to search for duplicates.

        Returns:
            List of duplicate file paths.
        """
        file_path = file_path.resolve()
        target_hash = calculate_file_hash(file_path)
        if not target_hash:
            return []

        duplicates: List[Path] = []

        for search_dir in search_dirs:
            search_dir = Path(search_dir).resolve()
            if not search_dir.exists():
                continue

            hash_to_files = self.scan_local_directory(search_dir)
            if target_hash in hash_to_files:
                for dup_path in hash_to_files[target_hash]:
                    if dup_path.resolve() != file_path:
                        duplicates.append(dup_path)

        if duplicates:
            logger.warning("File %s has %d local duplicates", file_path.name, len(duplicates))

        return duplicates

    def check_s3_duplicate(
        self,
        session: "boto3.Session",
        local_path: Path,
        bucket: str,
        s3_key: str,
    ) -> Tuple[bool, Optional[Dict]]:
        """
        Check if a local file already exists in S3.

        Args:
            session: Boto3 session.
            local_path: Local file path.
            bucket: Target S3 bucket.
            s3_key: S3 key to check.

        Returns:
            Tuple (is_duplicate, metadata) where:
              - is_duplicate: True if duplicate found.
              - metadata: S3 metadata if found, else None.
        """
        exists, metadata = check_s3_file_exists(session, bucket, s3_key)

        if exists:
            local_size = local_path.stat().st_size
            s3_size = metadata.get("size", 0)

            if local_size == s3_size:
                logger.info("File %s matches S3 object by size", local_path.name)
                return True, metadata
            logger.info("File %s exists in S3 with different size", local_path.name)
            return False, metadata

        return False, None

    def find_similar_s3_files(
        self,
        session: "boto3.Session",
        local_path: Path,
        bucket: str,
        prefix: str = "",
    ) -> List[Dict]:
        """
        Find similar files in S3 by name or size.

        Args:
            session: Boto3 session.
            local_path: Local file to compare.
            bucket: Target S3 bucket.
            prefix: Optional S3 prefix filter.

        Returns:
            List of similar S3 objects with similarity type.
        """
        local_size = local_path.stat().st_size
        local_name = local_path.name.lower()

        s3_files = list_s3_files(session, bucket, prefix)
        similar: List[Dict] = []

        for s3_file in s3_files:
            s3_name = Path(s3_file["key"]).name.lower()
            s3_size = s3_file["size"]

            if s3_name == local_name:
                s3_file["similarity"] = "same_name"
                similar.append(s3_file)
            elif s3_size == local_size:
                s3_file["similarity"] = "same_size"
                similar.append(s3_file)

        if similar:
            logger.info("Found %d similar files in S3 for %s", len(similar), local_path.name)

        return similar

    def generate_report(self, duplicates: Dict[str, List[Path]], base_dir: Optional[Path] = None) -> str:
        """
        Generate a formatted duplicate detection report.

        Args:
            duplicates: Dictionary mapping hash -> duplicate file list.
            base_dir: Optional base directory for relative paths.

        Returns:
            Formatted string report.
        """
        lines: List[str] = ["=" * 50, "Duplicate Detection Report", "=" * 50]

        if not duplicates:
            lines.append("No duplicates found")
        else:
            total_duplicates = sum(len(paths) - 1 for paths in duplicates.values())
            lines.append(
                f"Found {total_duplicates} duplicate files in {len(duplicates)} groups"
            )
            lines.append("")

            for idx, (_, paths) in enumerate(duplicates.items(), 1):
                lines.append(f"Group {idx} ({len(paths)} files):")
                sorted_paths = sorted(paths, key=lambda p: p.stat().st_mtime)

                for i, path in enumerate(sorted_paths):
                    path = path.resolve()
                    stat = path.stat()
                    marker = " (oldest)" if i == 0 else " (duplicate)"

                    try:
                        if base_dir:
                            display_path = str(path.relative_to(base_dir.resolve()))
                        elif path.is_relative_to(Path.cwd()):
                            display_path = str(path.relative_to(Path.cwd()))
                        else:
                            display_path = path.name
                    except Exception:
                        display_path = path.name

                    lines.append(f"  - {display_path}{marker}")
                    lines.append(f"    Size: {stat.st_size:,} bytes")
                    lines.append(
                        f"    Modified: {datetime.fromtimestamp(stat.st_mtime).strftime('%Y-%m-%d %H:%M:%S')}"
                    )
                lines.append("")

        lines.append("=" * 50)
        return "\n".join(lines)
