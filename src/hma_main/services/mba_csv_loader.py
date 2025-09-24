# src/hma_main/services/mba_csv_loader.py
"""
Orchestrates loading of all MBA CSVs (from local or S3) into RDS via ETL classes.
Designed to be callable from Lambda or local batch scripts.
"""

from __future__ import annotations
from pathlib import Path
from typing import Tuple
import tempfile
import shutil

import boto3
from botocore.exceptions import ClientError

from ..core.logging_config import get_logger
from ..core.exceptions import HMAIngestionError
from ..core.settings import settings
from .file_utils import detect_file_type
from ..database.etl_pipeline import (
    CsvContext,
    MemberDataETL, PlanDetailsETL, DeductiblesOOPETL, BenefitAccumulatorETL,
)

logger = get_logger(__name__)

def _route_etl(key: str):
    """Return the ETL class based on filename heuristics."""
    fname = Path(key).name.lower()
    if "memberdata" in fname:
        return MemberDataETL
    if "plan_details" in fname or "plandetails" in fname:
        return PlanDetailsETL
    if "deductibles_oop" in fname or "deductibles" in fname:
        return DeductiblesOOPETL
    if "benefit_accumulator" in fname or "accumulator" in fname:
        return BenefitAccumulatorETL
    raise HMAIngestionError(f"Cannot route ETL for key: {key}")

def load_s3_csv_to_rds(bucket: str, key: str) -> Tuple[str, int]:
    """
    Download a CSV from S3 to /tmp, run ETL, and load into RDS.

    Returns:
        (table_name, rows_loaded)
    """
    if not key.lower().endswith(".csv"):
        raise HMAIngestionError(f"Not a CSV key: s3://{bucket}/{key}")

    # Prepare local temp file
    tmp_dir = Path(tempfile.mkdtemp())
    local_path = tmp_dir / Path(key).name

    logger.info("Downloading s3://%s/%s to %s", bucket, key, local_path)
    session = boto3.Session(
        profile_name=settings.aws_profile,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
        region_name=settings.aws_default_region,
    )
    s3 = session.client("s3")
    try:
        s3.download_file(bucket, key, str(local_path))
    except ClientError as exc:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise HMAIngestionError(f"Download failed for s3://{bucket}/{key}", {"error": str(exc)})

    try:
        # Route to the correct ETL class
        etl_cls = _route_etl(key)
        ctx = CsvContext(bucket=bucket, key=key, local_path=local_path)
        etl = etl_cls(ctx)

        # Run ETL (streaming)
        extracted = etl.extract()
        validated = etl.validate(extracted)
        transformed = etl.transform(validated)
        count = etl.load(transformed)

        logger.info("ETL complete: table=%s rows=%d", etl.table_name, count)
        return etl.table_name, count

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)
