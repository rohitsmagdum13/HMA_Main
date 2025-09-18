"""FastAPI service for receiving upload jobs via HTTP."""
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from ..core.settings import settings
from ..core.logging_config import get_logger, setup_root_logger
from ..core.exceptions import ConfigError
from ..services.file_utils import build_s3_key
from .queue import Job, job_queue

logger = get_logger(__name__)


class JobRequest(BaseModel):
    """Request model for creating a job."""
    path: str = Field(..., description="File path (absolute or relative)")
    scope: str = Field(..., pattern="^(mba|policy)$", description="Upload scope")


class JobResponse(BaseModel):
    """Response model for job creation."""
    status: str
    message: str
    job: Optional[dict] = None


class HealthResponse(BaseModel):
    """Health check response."""
    status: str
    queue_stats: dict


def create_app() -> FastAPI:
    """
    Create and configure FastAPI application.
    
    Returns:
        Configured FastAPI app
    """
    # Initialize logging
    setup_root_logger()
    
    # Create FastAPI app
    app = FastAPI(
        title="HMA Ingestion API",
        description="API for submitting file upload jobs",
        version="0.1.0"
    )
    
    @app.get("/health", response_model=HealthResponse)
    async def health_check():
        """Check service health and queue status."""
        stats = job_queue.stats()
        return HealthResponse(
            status="healthy",
            queue_stats=stats
        )
    
    @app.post("/jobs", response_model=JobResponse)
    async def create_job(request: JobRequest):
        """
        Create a new upload job.
        
        Args:
            request: Job creation request
            
        Returns:
            Job creation response
        """
        try:
            # Resolve file path
            file_path = Path(request.path).resolve()
            
            # Validate file exists
            if not file_path.exists():
                raise HTTPException(status_code=404, detail=f"File not found: {request.path}")
            
            if not file_path.is_file():
                raise HTTPException(status_code=400, detail=f"Not a file: {request.path}")
            
            # Get bucket and prefix for scope
            try:
                bucket = settings.get_bucket(request.scope)
                prefix = settings.get_prefix(request.scope)
            except ValueError as e:
                raise HTTPException(status_code=400, detail=str(e))
            
            # Build S3 key
            s3_key = build_s3_key(request.scope, file_path, prefix)
            
            # Create job
            job = Job(
                path=file_path,
                scope=request.scope,
                s3_key=s3_key,
                bucket=bucket
            )
            
            # Enqueue job
            job_queue.put(job)
            logger.info(f"API: Enqueued job for {file_path.name}")
            
            return JobResponse(
                status="success",
                message=f"Job enqueued for {file_path.name}",
                job={
                    "file": str(file_path),
                    "scope": request.scope,
                    "bucket": bucket,
                    "key": s3_key
                }
            )
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"API error: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.get("/stats")
    async def get_stats():
        """Get queue statistics."""
        return job_queue.stats()
    
    return app


def run_server():
    """Run the API server using uvicorn."""
    import uvicorn
    
    setup_root_logger()
    logger.info("Starting HMA Ingestion API server...")
    
    # Create app
    app = create_app()
    
    # Run server
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level=settings.log_level.lower()
    )


if __name__ == "__main__":
    run_server()