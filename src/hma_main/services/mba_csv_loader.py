"""
Loads MBA CSVs from S3 into MySQL staging tables.
- Reads bucket/prefix/region from settings (.env via settings).
- Skips files already logged in import_log (idempotent).
- Maps filename -> staging table using TABLE_MAP.
"""

from __future__ import annotations
import io
import pandas as pd
from pathlib import Path
from sqlalchemy import text

from hma_main.core.settings import settings
from hma_main.db.engine import get_engine
from hma_main.services.s3_client import build_session  # reuse your session helper


# Map normalized filenames to staging tables (adjust to your exact S3 object names)
TABLE_MAP = {
    "memberdata.csv":          "stg_member_data",
    "plan_details.csv":        "stg_plan_details",
    "deductibles_oop.csv":     "stg_deductibles_oop",
    "benefit_accumulator.csv": "stg_benefit_accumulator",
}

def _normalize_filename(key: str) -> str:
    """Return just the basename in lowercase (e.g., 'plan_details.csv')."""
    return Path(key).name.lower()

def _s3(session):
    return session.client("s3", region_name=settings.aws_default_region)

def list_mba_csv_keys():
    """
    Lists CSV keys under the MBA prefix.
    Uses settings.get_bucket('mba') and settings.get_prefix('mba')/csv/.
    """
    bucket = settings.get_bucket("mba")
    prefix = f"{settings.get_prefix('mba').rstrip('/')}/csv/"
    s3 = _s3(build_session(profile=settings.aws_profile,
                           access_key=settings.aws_access_key_id,
                           secret_key=settings.aws_secret_access_key,
                           region=settings.aws_default_region))
    keys = []
    token = None
    while True:
        kw = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        for obj in resp.get("Contents", []):
            if obj["Key"].lower().endswith(".csv"):
                keys.append((obj["Key"], obj.get("ETag"), obj.get("Size")))
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    return bucket, keys

def _read_csv_from_s3(bucket: str, key: str) -> pd.DataFrame:
    """
    Streams the S3 object directly into pandas.
    """
    session = build_session(profile=settings.aws_profile,
                            access_key=settings.aws_access_key_id,
                            secret_key=settings.aws_secret_access_key,
                            region=settings.aws_default_region)
    s3 = _s3(session)
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read()
    # delegate delimiter/encoding inference to pandas; specify dtype if needed
    return pd.read_csv(io.BytesIO(body))

def _already_loaded(conn, bucket: str, key: str) -> bool:
    res = conn.execute(
        text("SELECT 1 FROM import_log WHERE s3_bucket=:b AND s3_key=:k"),
        {"b": bucket, "k": key}
    ).first()
    return res is not None

def _insert_log(conn, *, bucket, key, etag, file_bytes, rows, status, msg):
    conn.execute(
        text("""INSERT INTO import_log
                (s3_bucket, s3_key, etag, file_bytes, loaded_rows, status, message)
                VALUES (:b,:k,:e,:fb,:r,:s,:m)"""),
        {
            "b": bucket,
            "k": key,
            "e": (etag or "").strip('"'),
            "fb": file_bytes or 0,
            "r": int(rows or 0),
            "s": status,
            "m": (msg or "")[:995],
        }
    )

def load_all_mba_csvs() -> dict:
    """
    Loads every CSV under mba/csv/ exactly once (idempotent via import_log).
    Returns stats dict: {"loaded": X, "skipped": Y, "total": Z}
    """
    bucket, items = list_mba_csv_keys()
    eng = get_engine()
    loaded = skipped = 0
    with eng.begin() as conn:
        for key, etag, size in items:
            table = TABLE_MAP.get(_normalize_filename(key))
            if not table:
                _insert_log(conn, bucket=bucket, key=key, etag=etag, file_bytes=size,
                            rows=0, status="SKIPPED", msg="No table mapping")
                skipped += 1
                continue

            if _already_loaded(conn, bucket, key):
                _insert_log(conn, bucket=bucket, key=key, etag=etag, file_bytes=size,
                            rows=0, status="SKIPPED", msg="Already imported")
                skipped += 1
                continue

            try:
                df = _read_csv_from_s3(bucket, key)
                df.to_sql(table, con=conn, if_exists="append", index=False)
                _insert_log(conn, bucket=bucket, key=key, etag=etag, file_bytes=size,
                            rows=len(df), status="LOADED", msg="OK")
                loaded += 1
            except Exception as e:
                _insert_log(conn, bucket=bucket, key=key, etag=etag, file_bytes=size,
                            rows=0, status="ERROR", msg=str(e))
                # continue with next file
    return {"loaded": loaded, "skipped": skipped, "total": len(items)}
