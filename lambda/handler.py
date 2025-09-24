# lambda/handler.py
"""
Lambda entrypoint: triggered by S3 PutObject on hma-mba-bucket/mba/csv/*

Reads env (mirrors your .env), downloads CSV, and loads into RDS via ETL classes.
No Secrets Manager — env variables only.
"""

from __future__ import annotations
import json
from typing import Any, Dict

from hma_main.core.logging_config import get_logger, setup_root_logger
from hma_main.core.exceptions import HMAIngestionError
from hma_main.services.mba_csv_loader import load_s3_csv_to_rds

logger = get_logger(__name__)
setup_root_logger()  # format logs nicely in Lambda

def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    S3 event handler. Expects Records with bucket name and object key.
    """
    logger.info("Received event: %s", json.dumps(event))
    results = []

    try:
        for record in event.get("Records", []):
            bucket = record["s3"]["bucket"]["name"]
            key = record["s3"]["object"]["key"]
            table, rows = load_s3_csv_to_rds(bucket, key)
            results.append({"bucket": bucket, "key": key, "table": table, "rows": rows})

        return {"status": "ok", "results": results}

    except HMAIngestionError as e:
        logger.error("Ingestion error: %s | details=%s", e.message, getattr(e, "details", {}))
        return {"status": "error", "error": e.message, "details": getattr(e, "details", {})}

    except Exception as e:
        logger.error("Unexpected error: %s", e, exc_info=True)
        return {"status": "error", "error": str(e)}
