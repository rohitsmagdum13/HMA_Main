"""Producer service that discovers files and enqueues jobs."""
import argparse
import sys
from pathlib import Path
from typing import Set

from ..core.settings import settings
from ..core.logging_config import get_logger, setup_root_logger
from ..core.exceptions import FileDiscoveryError, ConfigError
from ..services.file_utils import discover_files, parse_extensions, build_s3_key
from .queue import Job, job_queue

logger = get_logger(__name__)


def enqueue_files(
    input_dir: Path,
    scope: str,
    include_extensions: Set[str],
    exclude_extensions: Set[str]
) -> int:
    """
    Discover files and enqueue upload jobs.
    
    Args:
        input_dir: Directory to scan
        scope: "mba" or "policy"
        include_extensions: Extensions to include
        exclude_extensions: Extensions to exclude
        
    Returns:
        Number of jobs enqueued
    """
    # Get bucket and prefix for scope
    try:
        bucket = settings.get_bucket(scope)
        prefix = settings.get_prefix(scope)
    except ValueError as e:
        raise ConfigError(f"Invalid scope: {scope}")
    
    # Discover files
    files = discover_files(input_dir, include_extensions, exclude_extensions)
    logger.info(f"Found {len(files)} files to process")
    
    # Enqueue jobs
    job_count = 0
    for file_path in files:
        # Build S3 key
        s3_key = build_s3_key(scope, file_path, prefix)
        
        # Create and enqueue job
        job = Job(
            path=file_path,
            scope=scope,
            s3_key=s3_key,
            bucket=bucket
        )
        
        job_queue.put(job)
        job_count += 1
        logger.debug(f"Enqueued: {file_path.name} -> s3://{bucket}/{s3_key}")
    
    logger.info(f"Enqueued {job_count} jobs for processing")
    return job_count


def main():
    """Main entry point for producer service."""
    setup_root_logger()
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="HMA Producer - Discover files and enqueue upload jobs"
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("./data"),
        help="Input directory to scan (default: ./data)"
    )
    parser.add_argument(
        "--scope",
        choices=["mba", "policy"],
        required=True,
        help="Upload scope (mba or policy)"
    )
    parser.add_argument(
        "--include",
        help="Comma-separated list of extensions to include"
    )
    parser.add_argument(
        "--exclude",
        help="Comma-separated list of extensions to exclude"
    )
    parser.add_argument(
        "--enqueue-only",
        action="store_true",
        help="Only enqueue jobs without processing"
    )
    
    args = parser.parse_args()
    
    try:
        # Parse extension filters
        include_exts = parse_extensions(args.include) if args.include else None
        exclude_exts = parse_extensions(args.exclude) if args.exclude else None
        
        # Enqueue files
        job_count = enqueue_files(
            input_dir=args.input,
            scope=args.scope,
            include_extensions=include_exts,
            exclude_extensions=exclude_exts
        )
        
        # Print summary
        print(f"\n{'='*50}")
        print(f"Producer Summary:")
        print(f"  Scope: {args.scope}")
        print(f"  Input: {args.input}")
        print(f"  Jobs enqueued: {job_count}")
        print(f"{'='*50}\n")
        
        # Show queue stats
        stats = job_queue.stats()
        logger.info(f"Queue stats: {stats}")
        
        if not args.enqueue_only:
            logger.info("Jobs enqueued and ready for workers")
        
    except (FileDiscoveryError, ConfigError) as e:
        logger.error(f"Producer error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()