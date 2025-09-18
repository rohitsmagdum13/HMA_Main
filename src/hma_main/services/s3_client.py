# s3_client.py
"""
S3 client wrapper for uploads, listing, and duplicate detection.

This module provides utilities for:
- Creating a configured boto3 Session from multiple credential sources.
- Checking whether an S3 object exists and retrieving its metadata.
- Listing S3 objects under a prefix using a paginator for scalability.
- Computing a stable file hash (MD5 or SHA-256) for duplicate detection.
- Uploading a file to S3 with retry logic, exponential backoff,
  optional duplicate detection, overwrite controls, and rich logging.

Design principles:
- Fail fast on credential errors; retry network/retriable errors.
- Keep public functions pure where possible and side-effect aware where not.
- Provide consistent, structured logs to aid observability.
- Maintain Pylint/Black-friendly style and comprehensive docstrings.

Raises:
    UploadError: For non-recoverable upload failures or after all retries.

Notes:
    - `UploadError` is expected to be defined in the project's core exceptions.
    - Logging follows the project's `get_logger(__name__)` pattern.

"""

from __future__ import annotations  # Enable forward references in type hints

# Standard library imports
import hashlib  # Used to compute checksums for duplicate detection
import time  # Used for retry backoff and upload timestamp metadata
from pathlib import Path  # Path-safe file handling
from typing import Dict, List, Optional, Tuple  # Static typing support

# Third-party imports
import boto3  # AWS SDK for Python
from botocore.exceptions import ClientError, NoCredentialsError  # AWS error types

# Project imports
from ..core.exceptions import UploadError  # Custom domain exception
from ..core.logging_config import get_logger  # Project-wide logging factory

# Initialize module-level logger once (cheap, thread-safe in practice)
logger = get_logger(__name__)


def build_session(
    profile: Optional[str] = None,
    access_key: Optional[str] = None,
    secret_key: Optional[str] = None,
    region: str = "ap-south-1",
) -> boto3.Session:
    """
    Create and return a boto3 Session using one of three strategies, in order:
    1) Named profile; 2) Explicit access keys; 3) Default credential chain.

    Args:
        profile: Optional AWS named profile; if provided, it takes precedence.
        access_key: Optional AWS access key ID.
        secret_key: Optional AWS secret access key.
        region: AWS region to target (default: "ap-south-1").

    Returns:
        A configured `boto3.Session` instance.

    Logging:
        - DEBUG: Which credential strategy is used.

    """
    # If a profile is specified, prefer it over any explicit keys.
    if profile:
        # Log chosen strategy for traceability without leaking secrets.
        logger.debug("Creating AWS session using profile: %s", profile)
        # Return a session bound to the desired region and profile.
        return boto3.Session(profile_name=profile, region_name=region)

    # If explicit keys are provided (both required), use them.
    if access_key and secret_key:
        # Avoid logging the key material; only log the strategy.
        logger.debug("Creating AWS session using explicit access keys")
        # Return a session initialized with explicit credentials.
        return boto3.Session(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
        )

    # Fallback to the default credential chain (env vars, instance profile, etc.).
    logger.debug("Creating AWS session using default credentials")
    return boto3.Session(region_name=region)


def check_s3_file_exists(
    session: boto3.Session,
    bucket: str,
    s3_key: str,
) -> Tuple[bool, Optional[Dict]]:
    """
    Determine if an object exists in S3 and return a small metadata snapshot.

    Args:
        session: A pre-configured boto3 session.
        bucket: Target S3 bucket name.
        s3_key: Full object key (path) inside the bucket.

    Returns:
        Tuple of:
            - bool: True if the object exists (HTTP 200 from HeadObject).
            - Optional[Dict]: Metadata dict if present; otherwise None.
              Keys: 'size', 'last_modified', 'etag', 'content_type'.

    Logging:
        - DEBUG: Existence check success and basic size metadata.
        - WARNING: Non-404 ClientError (e.g., AccessDenied).
        - ERROR: Unexpected errors.

    Notes:
        - 404 is treated as "does not exist" and not an error.
        - ETag is stripped of surrounding quotes for convenience.

    """
    # Build a low-level S3 client bound to the given session.
    s3_client = session.client("s3")

    try:
        # Issue a HEAD request to avoid downloading the object body.
        response = s3_client.head_object(Bucket=bucket, Key=s3_key)

        # Extract a small, consistent metadata surface.
        metadata = {
            "size": response.get("ContentLength", 0),
            "last_modified": response.get("LastModified"),
            "etag": response.get("ETag", "").strip('"'),  # Normalize quotes
            "content_type": response.get("ContentType", ""),
        }

        # Log at debug level to keep info logs quieter for large scans.
        logger.debug(
            "S3 object exists: s3://%s/%s (size=%s, etag=%s)",
            bucket,
            s3_key,
            metadata["size"],
            metadata["etag"],
        )
        # Indicate existence and return the captured metadata.
        return True, metadata

    except ClientError as exc:
        # Extract the AWS error code safely; default to empty string if missing.
        error_code = exc.response.get("Error", {}).get("Code", "")
        # If the error indicates "Not Found", it is a normal negative result.
        if error_code == "404":
            logger.debug("S3 object not found: s3://%s/%s", bucket, s3_key)
            return False, None

        # For other client errors (e.g., AccessDenied), warn and return no data.
        logger.warning("Error checking S3 file existence: %s", exc)
        return False, None

    except Exception as exc:  # noqa: BLE001 - broad for network/SDK edge cases
        # Catch-all: log and surface a non-fatal negative result.
        logger.error("Unexpected error in head_object for s3://%s/%s: %s", bucket, s3_key, exc)
        return False, None


def list_s3_files(
    session: boto3.Session,
    bucket: str,
    prefix: str = "",
    max_files: int = 10_000,
) -> List[Dict]:
    """
    List objects in an S3 bucket under a given prefix using the V2 paginator.

    Args:
        session: A pre-configured boto3 session.
        bucket: Target S3 bucket name.
        prefix: Key prefix filter (e.g., "mba/csv/"); empty for whole bucket.
        max_files: Upper bound of items across all pages (default 10k).

    Returns:
        A list of dictionaries, each containing:
            - 'key': Object key (str)
            - 'size': Object size (int)
            - 'last_modified': Last modified datetime
            - 'etag': ETag without surrounding quotes (str)

    Logging:
        - INFO: Final count of items discovered.
        - ERROR: Client/Unexpected errors with context.

    """
    # Create the low-level S3 client per session best practice.
    s3_client = session.client("s3")
    # Prepare an output list to collect results across pages.
    files: List[Dict] = []

    try:
        # Initialize paginator to efficiently traverse large listings.
        paginator = s3_client.get_paginator("list_objects_v2")

        # Paginate with a MaxItems cap to avoid unbounded scans.
        page_iterator = paginator.paginate(
            Bucket=bucket,
            Prefix=prefix,
            PaginationConfig={"MaxItems": max_files},
        )

        # Iterate through pages and accumulate object summaries.
        for page in page_iterator:
            # 'Contents' is present only when there are matching objects.
            if "Contents" in page:
                for obj in page["Contents"]:
                    files.append(
                        {
                            "key": obj["Key"],
                            "size": obj["Size"],
                            "last_modified": obj["LastModified"],
                            "etag": obj.get("ETag", "").strip('"'),
                        }
                    )

        # Summarize the discovery in logs for quick visibility.
        logger.info("Listed %d files under s3://%s/%s", len(files), bucket, prefix)
        # Return the collected summaries.
        return files

    except ClientError as exc:
        # Log AWS client-side errors (permissions, request validation, etc.).
        logger.error("Error listing S3 files at s3://%s/%s: %s", bucket, prefix, exc)
        return []

    except Exception as exc:  # noqa: BLE001
        # Log unexpected errors (network, serialization, SDK edge cases).
        logger.error("Unexpected error listing s3://%s/%s: %s", bucket, prefix, exc)
        return []


def calculate_file_hash(file_path: Path, algorithm: str = "md5") -> str:
    """
    Compute a checksum for the given local file to support duplicate detection.

    Args:
        file_path: Path to a local file on disk.
        algorithm: Either 'md5' (default) or 'sha256'.

    Returns:
        A hex digest string of the chosen hash algorithm; empty string on error.

    Logging:
        - ERROR: On read failures; includes the file path.

    Notes:
        - Uses chunked reads to support large files without high memory use.

    """
    # Choose the hash function based on the requested algorithm.
    hash_func = hashlib.md5() if algorithm == "md5" else hashlib.sha256()

    try:
        # Open in binary mode to avoid newline transformations.
        with file_path.open("rb") as handle:
            # Read in fixed-size chunks until EOF to minimize memory footprint.
            for chunk in iter(lambda: handle.read(8192), b""):
                # Update the hash state with the next chunk of bytes.
                hash_func.update(chunk)

        # Return the final hex representation.
        return hash_func.hexdigest()

    except Exception as exc:  # noqa: BLE001
        # Log the failure and return an empty string to signal error to the caller.
        logger.error("Error calculating hash for %s: %s", file_path, exc)
        return ""


def upload_file(
    session: boto3.Session,
    bucket: str,
    local_path: Path,
    s3_key: str,
    max_retries: int = 3,
    check_duplicate: bool = True,
    overwrite: bool = False,
) -> Tuple[bool, str]:
    """
    Upload a local file to S3 with retries, duplicate detection, and metadata.

    Args:
        session: A pre-configured boto3 session.
        bucket: Target S3 bucket name.
        local_path: Path to the local file to upload.
        s3_key: Destination key in S3 (e.g., "mba/csv/MemberData.csv").
        max_retries: Number of upload attempts (default: 3).
        check_duplicate: If True, skip upload when an identical file exists.
        overwrite: If True, overwrite a different-size object at the same key.

    Returns:
        Tuple:
            - bool: True on success (or skipped duplicate), False otherwise.
            - str: Human-readable status message.

    Raises:
        UploadError: For non-retriable failures or after all retries.

    Logging:
        - DEBUG: Each attempt with context.
        - INFO: Successful uploads and duplicate skips.
        - WARNING: Retry announcements with exponential backoff timing.
        - ERROR: Final failure reasons, credential errors, or unexpected issues.

    Behavior:
        - If `check_duplicate` and not `overwrite`: HEAD the object and compare
          sizes; if equal, skip as duplicate. If sizes differ and `overwrite`
          is False, return a failure status instructing to enable overwrite.
        - On transient errors, retry with exponential backoff (2^attempt).

    """
    # Build a low-level S3 client for the upload operation.
    s3_client = session.client("s3")

    # Optional duplicate check (cheap HEAD) to avoid redundant uploads.
    if check_duplicate and not overwrite:
        exists, s3_metadata = check_s3_file_exists(session, bucket, s3_key)

        if exists:
            # Collect local file size for a quick equality comparison.
            local_size = local_path.stat().st_size
            s3_size = (s3_metadata or {}).get("size", 0)

            # If sizes match, treat as a duplicate and skip upload.
            if local_size == s3_size:
                logger.info(
                    "Skipping duplicate upload: %s (s3://%s/%s has same size)",
                    local_path.name,
                    bucket,
                    s3_key,
                )
                return True, "Skipped (duplicate)"

            # Otherwise, alert the caller; overwriting requires explicit consent.
            logger.warning(
                "Object exists with different size (local=%s, s3=%s) at s3://%s/%s",
                local_size,
                s3_size,
                bucket,
                s3_key,
            )
            if not overwrite:
                return False, "Exists with different size (use overwrite to replace)"

    # Calculate a local checksum to embed in metadata for traceability.
    local_hash = calculate_file_hash(local_path)

    # Attempt the upload up to `max_retries` times with exponential backoff.
    for attempt in range(1, max_retries + 1):
        try:
            # Log at debug level to avoid flooding production INFO logs.
            logger.debug(
                "Upload attempt %d/%d: %s -> s3://%s/%s",
                attempt,
                max_retries,
                local_path,
                bucket,
                s3_key,
            )

            # Perform the actual upload with server-side encryption and metadata.
            s3_client.upload_file(
                Filename=str(local_path),  # Ensure str for boto3
                Bucket=bucket,
                Key=s3_key,
                ExtraArgs={
                    "ServerSideEncryption": "AES256",
                    "Metadata": {
                        "original-filename": local_path.name,
                        "local-hash": local_hash,
                        "upload-timestamp": str(int(time.time())),
                    },
                },
            )

            # If no exception is raised, the upload has succeeded.
            logger.info(
                "Uploaded successfully: %s -> s3://%s/%s", local_path.name, bucket, s3_key
            )
            return True, "Uploaded successfully"

        except NoCredentialsError as exc:
            # Credentials problems are not recoverable by retrying.
            error_msg = "AWS credentials not found"
            logger.error("%s while uploading %s: %s", error_msg, local_path, exc)
            raise UploadError(
                error_msg, {"file": str(local_path), "bucket": bucket}
            ) from exc

        except ClientError as exc:
            # Extract structured error details when present.
            error_code = exc.response.get("Error", {}).get("Code", "Unknown")
            error_message = exc.response.get("Error", {}).get("Message", str(exc))

            # Certain errors should not be retried (permissions, wrong bucket).
            if error_code in ("AccessDenied", "NoSuchBucket"):
                logger.error(
                    "Upload failed without retry (%s): %s (s3://%s/%s)",
                    error_code,
                    error_message,
                    bucket,
                    s3_key,
                )
                raise UploadError(
                    f"{error_code}: {error_message}",
                    {"file": str(local_path), "bucket": bucket},
                ) from exc

            # For other client errors, use exponential backoff and retry budget.
            if attempt < max_retries:
                wait_seconds = 2**attempt  # 2, 4, 8, ...
                logger.warning(
                    "Upload attempt %d/%d failed, retrying in %ds: %s",
                    attempt,
                    max_retries,
                    wait_seconds,
                    error_message,
                )
                time.sleep(wait_seconds)
                continue

            # Retries exhausted; escalate as UploadError.
            logger.error(
                "Upload failed after %d attempts: %s", max_retries, error_message
            )
            raise UploadError(
                f"Upload failed: {error_message}",
                {"file": str(local_path), "bucket": bucket},
            ) from exc

        except Exception as exc:  # noqa: BLE001
            # Catch-all for non-botocore exceptions (I/O, OS errors, etc.).
            logger.error("Unexpected upload error for %s: %s", local_path, exc)
            raise UploadError(
                f"Unexpected error: {exc}", {"file": str(local_path), "bucket": bucket}
            ) from exc

    # The loop either returns on success or raises; this is a safety net.
    return False, "Upload failed after all retries"
