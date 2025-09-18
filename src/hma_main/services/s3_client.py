"""S3 client wrapper for file uploads with duplicate detection."""
import time
import hashlib
from pathlib import Path
from typing import Optional, Dict, List, Tuple
import boto3
from botocore.exceptions import ClientError, NoCredentialsError

from ..core.logging_config import get_logger
from ..core.exceptions import UploadError

logger = get_logger(__name__)


def build_session(
    profile: Optional[str] = None,
    access_key: Optional[str] = None,
    secret_key: Optional[str] = None,
    region: str = "ap-south-1"
) -> boto3.Session:
    """
    Build AWS session with provided credentials.
    
    Args:
        profile: AWS profile name (overrides access keys)
        access_key: AWS access key ID
        secret_key: AWS secret access key
        region: AWS region
        
    Returns:
        Configured boto3 Session
    """
    # Profile takes precedence over explicit keys
    if profile:
        logger.debug(f"Creating session with profile: {profile}")
        return boto3.Session(profile_name=profile, region_name=region)
    
    # Use explicit keys if provided
    if access_key and secret_key:
        logger.debug("Creating session with access keys")
        return boto3.Session(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region
        )
    
    # Fall back to default credentials
    logger.debug("Creating session with default credentials")
    return boto3.Session(region_name=region)


def check_s3_file_exists(
    session: boto3.Session,
    bucket: str,
    s3_key: str
) -> Tuple[bool, Optional[Dict]]:
    """
    Check if a file exists in S3 and get its metadata.
    
    Args:
        session: Boto3 session
        bucket: S3 bucket name
        s3_key: S3 object key
        
    Returns:
        Tuple of (exists, metadata_dict)
    """
    s3_client = session.client('s3')
    
    try:
        # Try to get object metadata
        response = s3_client.head_object(Bucket=bucket, Key=s3_key)
        
        metadata = {
            'size': response.get('ContentLength', 0),
            'last_modified': response.get('LastModified'),
            'etag': response.get('ETag', '').strip('"'),  # Remove quotes from ETag
            'content_type': response.get('ContentType', ''),
        }
        
        logger.debug(f"File exists in S3: {s3_key} (size: {metadata['size']} bytes)")
        return True, metadata
        
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', '')
        
        if error_code == '404':
            # File doesn't exist
            logger.debug(f"File not found in S3: {s3_key}")
            return False, None
        else:
            # Other error (permission issues, etc.)
            logger.warning(f"Error checking S3 file existence: {e}")
            return False, None
    except Exception as e:
        logger.error(f"Unexpected error checking S3: {e}")
        return False, None


def list_s3_files(
    session: boto3.Session,
    bucket: str,
    prefix: str = "",
    max_files: int = 10000
) -> List[Dict]:
    """
    List files in S3 bucket with given prefix.
    
    Args:
        session: Boto3 session
        bucket: S3 bucket name
        prefix: S3 prefix to filter
        max_files: Maximum number of files to return
        
    Returns:
        List of file metadata dictionaries
    """
    s3_client = session.client('s3')
    files = []
    
    try:
        # Use paginator for large buckets
        paginator = s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(
            Bucket=bucket,
            Prefix=prefix,
            PaginationConfig={'MaxItems': max_files}
        )
        
        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    files.append({
                        'key': obj['Key'],
                        'size': obj['Size'],
                        'last_modified': obj['LastModified'],
                        'etag': obj.get('ETag', '').strip('"')
                    })
        
        logger.info(f"Found {len(files)} files in s3://{bucket}/{prefix}")
        return files
        
    except ClientError as e:
        logger.error(f"Error listing S3 files: {e}")
        return []
    except Exception as e:
        logger.error(f"Unexpected error listing S3: {e}")
        return []


def calculate_file_hash(file_path: Path, algorithm: str = 'md5') -> str:
    """
    Calculate hash of a file for comparison.
    
    Args:
        file_path: Path to file
        algorithm: Hash algorithm ('md5' or 'sha256')
        
    Returns:
        Hex digest of file hash
    """
    hash_func = hashlib.md5() if algorithm == 'md5' else hashlib.sha256()
    
    try:
        with open(file_path, 'rb') as f:
            # Read file in chunks for large files
            for chunk in iter(lambda: f.read(8192), b''):
                hash_func.update(chunk)
        
        return hash_func.hexdigest()
    except Exception as e:
        logger.error(f"Error calculating hash for {file_path}: {e}")
        return ""


def upload_file(
    session: boto3.Session,
    bucket: str,
    local_path: Path,
    s3_key: str,
    max_retries: int = 3,
    check_duplicate: bool = True,
    overwrite: bool = False
) -> Tuple[bool, str]:
    """
    Upload a file to S3 with retry logic and duplicate detection.
    
    Args:
        session: Boto3 session to use
        bucket: S3 bucket name
        local_path: Local file path
        s3_key: S3 object key
        max_retries: Maximum upload attempts
        check_duplicate: Check if file already exists in S3
        overwrite: If True, overwrite existing files
        
    Returns:
        Tuple of (success, message)
        
    Raises:
        UploadError: If upload fails after all retries
    """
    # Create S3 client from session
    s3_client = session.client('s3')
    
    # Check for duplicate if requested
    if check_duplicate and not overwrite:
        exists, s3_metadata = check_s3_file_exists(session, bucket, s3_key)
        
        if exists:
            # Compare file sizes
            local_size = local_path.stat().st_size
            s3_size = s3_metadata.get('size', 0)
            
            if local_size == s3_size:
                # Same size - likely duplicate
                logger.info(f"Skipping duplicate: {local_path.name} already exists in S3 with same size")
                return True, "Skipped (duplicate)"
            else:
                # Different size - might be updated file
                logger.warning(f"File exists with different size: local={local_size}, s3={s3_size}")
                if not overwrite:
                    return False, f"File exists with different size (use --overwrite to replace)"
    
    # Calculate local file hash for verification
    local_hash = calculate_file_hash(local_path)
    
    # Attempt upload with retries
    for attempt in range(1, max_retries + 1):
        try:
            logger.debug(f"Upload attempt {attempt}/{max_retries}: {local_path} -> s3://{bucket}/{s3_key}")
            
            # Perform upload with metadata
            s3_client.upload_file(
                str(local_path),
                bucket,
                s3_key,
                ExtraArgs={
                    'ServerSideEncryption': 'AES256',
                    'Metadata': {
                        'original-filename': local_path.name,
                        'local-hash': local_hash,
                        'upload-timestamp': str(int(time.time()))
                    }
                }
            )
            
            logger.info(f"Successfully uploaded: {local_path.name} -> s3://{bucket}/{s3_key}")
            return True, "Uploaded successfully"
            
        except NoCredentialsError:
            # Credentials issue - don't retry
            error_msg = "AWS credentials not found"
            logger.error(error_msg)
            raise UploadError(error_msg, {"file": str(local_path), "bucket": bucket})
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            error_msg = e.response.get('Error', {}).get('Message', str(e))
            
            # Don't retry on permission errors
            if error_code in ('AccessDenied', 'NoSuchBucket'):
                logger.error(f"Upload failed (no retry): {error_code} - {error_msg}")
                raise UploadError(f"{error_code}: {error_msg}", {"file": str(local_path), "bucket": bucket})
            
            # Retry on other errors
            if attempt < max_retries:
                wait_time = 2 ** attempt  # Exponential backoff
                logger.warning(f"Upload failed (attempt {attempt}), retrying in {wait_time}s: {error_msg}")
                time.sleep(wait_time)
            else:
                logger.error(f"Upload failed after {max_retries} attempts: {error_msg}")
                raise UploadError(f"Upload failed: {error_msg}", {"file": str(local_path), "bucket": bucket})
                
        except Exception as e:
            # Unexpected error
            logger.error(f"Unexpected upload error: {e}")
            raise UploadError(f"Unexpected error: {e}", {"file": str(local_path), "bucket": bucket})
    
    return False, "Upload failed after all retries"