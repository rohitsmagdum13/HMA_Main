"""Worker service that processes upload jobs from queue."""
import argparse
import sys
import time
import threading
import boto3
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from ..core.settings import settings
from ..core.logging_config import get_logger, setup_root_logger
from ..core.exceptions import UploadError
from ..services.s3_client import build_session, upload_file
from .queue import Job, job_queue

logger = get_logger(__name__)


class Worker:
    """Upload worker that processes jobs from queue."""
    
    def __init__(
        self,
        session: 'boto3.Session',
        worker_id: int
    ):
        """
        Initialize worker.
        
        Args:
            session: AWS session to use
            worker_id: Worker identifier
        """
        self.session = session
        self.worker_id = worker_id
        self.processed = 0
        self.failed = 0
        
    def process_job(self, job: Job) -> bool:
        """
        Process a single upload job.
        
        Args:
            job: Job to process
            
        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Worker {self.worker_id}: Processing {job}")
        
        try:
            # Upload file
            success = upload_file(
                session=self.session,
                bucket=job.bucket,
                local_path=job.path,
                s3_key=job.s3_key
            )
            
            if success:
                self.processed += 1
                logger.info(f"Worker {self.worker_id}: Successfully uploaded {job.path.name}")
            else:
                self.failed += 1
                logger.error(f"Worker {self.worker_id}: Failed to upload {job.path.name}")
                
            return success
            
        except UploadError as e:
            self.failed += 1
            logger.error(f"Worker {self.worker_id}: Upload error for {job.path.name}: {e}")
            return False
        except Exception as e:
            self.failed += 1
            logger.error(f"Worker {self.worker_id}: Unexpected error: {e}", exc_info=True)
            return False
            
    def run(self, drain_once: bool = False) -> None:
        """
        Run worker loop.
        
        Args:
            drain_once: If True, exit when queue is empty
        """
        logger.info(f"Worker {self.worker_id} started")
        
        while True:
            # Get job from queue (wait up to 1 second)
            job = job_queue.get(timeout=1.0)
            
            if job is None:
                # No job available
                if drain_once and job_queue.is_empty():
                    logger.info(f"Worker {self.worker_id}: Queue empty, exiting")
                    break
                continue
                
            # Process job
            success = self.process_job(job)
            
            # Mark task as done
            job_queue.task_done()
            
            # Update queue stats
            if not success:
                job_queue.mark_failed()
                
        logger.info(f"Worker {self.worker_id} finished: processed={self.processed}, failed={self.failed}")


def run_workers(
    concurrency: int,
    drain_once: bool,
    aws_profile: Optional[str],
    region: Optional[str]
) -> dict:
    """
    Run multiple workers in parallel.
    
    Args:
        concurrency: Number of workers
        drain_once: Exit when queue is empty
        aws_profile: AWS profile to use
        region: AWS region
        
    Returns:
        Summary statistics
    """
    # Build AWS session
    session = build_session(
        profile=aws_profile or settings.aws_profile,
        access_key=settings.aws_access_key_id,
        secret_key=settings.aws_secret_access_key,
        region=region or settings.aws_default_region
    )
    
    # Create and start workers
    workers = []
    threads = []
    
    for i in range(concurrency):
        worker = Worker(session=session, worker_id=i+1)
        workers.append(worker)
        
        thread = threading.Thread(
            target=worker.run,
            args=(drain_once,),
            daemon=True
        )
        thread.start()
        threads.append(thread)
    
    logger.info(f"Started {concurrency} workers")
    
    # Wait for all workers to finish
    for thread in threads:
        thread.join()
    
    # Collect statistics
    total_processed = sum(w.processed for w in workers)
    total_failed = sum(w.failed for w in workers)
    
    return {
        "workers": concurrency,
        "processed": total_processed,
        "failed": total_failed
    }


def main():
    """Main entry point for worker service."""
    setup_root_logger()
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="HMA Worker - Process upload jobs from queue"
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=4,
        help="Number of concurrent workers (default: 4)"
    )
    parser.add_argument(
        "--drain-once",
        action="store_true",
        help="Exit when queue is empty"
    )
    parser.add_argument(
        "--aws-profile",
        help="AWS profile to use"
    )
    parser.add_argument(
        "--region",
        help="AWS region"
    )
    
    args = parser.parse_args()
    
    try:
        # Check initial queue state
        initial_stats = job_queue.stats()
        logger.info(f"Initial queue state: {initial_stats}")
        
        if initial_stats["queued"] == 0:
            logger.warning("Queue is empty, no jobs to process")
            if args.drain_once:
                sys.exit(0)
        
        # Run workers
        logger.info(f"Starting {args.concurrency} workers...")
        stats = run_workers(
            concurrency=args.concurrency,
            drain_once=args.drain_once,
            aws_profile=args.aws_profile,
            region=args.region
        )
        
        # Print summary
        print(f"\n{'='*50}")
        print(f"Worker Summary:")
        print(f"  Workers: {stats['workers']}")
        print(f"  Processed: {stats['processed']}")
        print(f"  Failed: {stats['failed']}")
        print(f"  Success Rate: {stats['processed']/(stats['processed']+stats['failed'])*100:.1f}%")
        print(f"{'='*50}\n")
        
    except Exception as e:
        logger.error(f"Worker error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()