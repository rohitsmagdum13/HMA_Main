# src/hma_main/database/etl_pipeline.py
"""
OOP ETL classes to load MBA CSVs into RDS (MySQL).

- BaseCsvETL: common extract/validate/transform/load steps
- Concrete ETLs: MemberDataETL, PlanDetailsETL, DeductiblesOOPETL, BenefitAccumulatorETL
"""

from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import List, Dict, Any, Iterable
import io
import csv
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.engine import Connection
from ..core.logging_config import get_logger
from ..core.exceptions import HMAIngestionError
from .connection import session_scope

logger = get_logger(__name__)

@dataclass
class CsvContext:
    """Runtime context describing the file being processed."""
    bucket: str
    key: str
    local_path: Path

class BaseCsvETL:
    """
    Base class encapsulating ETL steps:
      - extract()   : read CSV rows
      - validate()  : column checks and type coercion
      - transform() : normalize/derive fields
      - load()      : upsert/insert into RDS
    Subclasses must implement table_name and column mapping.
    """

    table_name: str = ""
    expected_columns: List[str] = []

    def __init__(self, ctx: CsvContext):
        self.ctx = ctx

    # ---------- EXTRACT ----------
    def extract(self) -> Iterable[Dict[str, Any]]:
        """
        Stream CSV rows from local_path with UTF-8 decoding.
        Avoids large memory spikes by iterating rows.
        """
        logger.info("Extracting CSV: s3://%s/%s", self.ctx.bucket, self.ctx.key)
        with self.ctx.local_path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                yield {k.strip(): (v.strip() if isinstance(v, str) else v) for k, v in row.items()}

    # ---------- VALIDATE ----------
    def validate(self, rows: Iterable[Dict[str, Any]]) -> Iterable[Dict[str, Any]]:
        """
        Ensure expected columns exist; basic type normalization.
        Subclasses can override for domain-specific rules.
        """
        for idx, row in enumerate(rows, start=1):
            missing = [c for c in self.expected_columns if c not in row]
            if missing:
                raise HMAIngestionError(
                    f"Missing columns {missing} in {self.table_name}",
                    {"row_index": idx, "key": self.ctx.key},
                )
            yield self._coerce_types(row)

    def _coerce_types(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Default passthrough—override as needed."""
        return row

    # ---------- TRANSFORM ----------
    def transform(self, rows: Iterable[Dict[str, Any]]) -> Iterable[Dict[str, Any]]:
        """Hook for per-row enrichment/cleanup; default passthrough."""
        for row in rows:
            yield row

    # ---------- LOAD ----------
    def load(self, rows: Iterable[Dict[str, Any]]) -> int:
        """
        Insert/Upsert rows into MySQL using a single transaction.
        Subclasses implement _upsert_sql() and _row_params().
        """
        inserted = 0
        sql = self._upsert_sql()
        with session_scope() as session:
            conn: Connection = session.connection()
            for row in rows:
                params = self._row_params(row)
                conn.execute(text(sql), params)
                inserted += 1
        logger.info("Loaded %d rows into %s", inserted, self.table_name)
        return inserted

    # ---------- CONTRACT ----------
    def _upsert_sql(self) -> str:
        """Return INSERT ... ON DUPLICATE KEY UPDATE SQL."""
        raise NotImplementedError

    def _row_params(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Map CSV row to SQL parameters."""
        raise NotImplementedError


# -------- Concrete ETLs --------

class MemberDataETL(BaseCsvETL):
    table_name = "member_data"
    expected_columns = ["member_id", "first_name", "last_name", "gender", "dob", "plan_id"]

    def _coerce_types(self, row: Dict[str, Any]) -> Dict[str, Any]:
        # Convert dob -> DATE if present
        if row.get("dob"):
            try:
                row["dob"] = datetime.strptime(row["dob"], "%Y-%m-%d").date()
            except Exception:
                row["dob"] = None
        return row

    def _upsert_sql(self) -> str:
        return """
        INSERT INTO member_data (member_id, first_name, last_name, gender, dob, plan_id)
        VALUES (:member_id, :first_name, :last_name, :gender, :dob, :plan_id)
        ON DUPLICATE KEY UPDATE
            first_name=VALUES(first_name),
            last_name=VALUES(last_name),
            gender=VALUES(gender),
            dob=VALUES(dob),
            plan_id=VALUES(plan_id)
        """

    def _row_params(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return row


class PlanDetailsETL(BaseCsvETL):
    table_name = "plan_details"
    expected_columns = ["plan_id", "plan_name", "coverage_start", "coverage_end", "network"]

    def _coerce_types(self, row: Dict[str, Any]) -> Dict[str, Any]:
        for field in ("coverage_start", "coverage_end"):
            if row.get(field):
                try:
                    row[field] = datetime.strptime(row[field], "%Y-%m-%d").date()
                except Exception:
                    row[field] = None
        return row

    def _upsert_sql(self) -> str:
        return """
        INSERT INTO plan_details (plan_id, plan_name, coverage_start, coverage_end, network)
        VALUES (:plan_id, :plan_name, :coverage_start, :coverage_end, :network)
        ON DUPLICATE KEY UPDATE
            plan_name=VALUES(plan_name),
            coverage_start=VALUES(coverage_start),
            coverage_end=VALUES(coverage_end),
            network=VALUES(network)
        """

    def _row_params(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return row


class DeductiblesOOPETL(BaseCsvETL):
    table_name = "deductibles_oop"
    expected_columns = [
        "member_id", "plan_id", "calendar_year",
        "deductible_total", "deductible_used", "oop_max_total", "oop_max_used"
    ]

    def _coerce_types(self, row: Dict[str, Any]) -> Dict[str, Any]:
        for f in ("calendar_year",):
            if row.get(f):
                try: row[f] = int(row[f])
                except Exception: row[f] = None
        for f in ("deductible_total", "deductible_used", "oop_max_total", "oop_max_used"):
            if row.get(f):
                try: row[f] = float(row[f])
                except Exception: row[f] = None
        return row

    def _upsert_sql(self) -> str:
        # No stable business key; leave as append or dedupe via (member_id, plan_id, calendar_year) if needed
        return """
        INSERT INTO deductibles_oop
          (member_id, plan_id, calendar_year, deductible_total, deductible_used, oop_max_total, oop_max_used)
        VALUES
          (:member_id, :plan_id, :calendar_year, :deductible_total, :deductible_used, :oop_max_total, :oop_max_used)
        """

    def _row_params(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return row


class BenefitAccumulatorETL(BaseCsvETL):
    table_name = "benefit_accumulator"
    expected_columns = ["member_id", "plan_id", "service_category", "allowed_amount", "utilized_amount", "last_updated"]

    def _coerce_types(self, row: Dict[str, Any]) -> Dict[str, Any]:
        for f in ("allowed_amount", "utilized_amount"):
            if row.get(f):
                try: row[f] = float(row[f])
                except Exception: row[f] = None
        if row.get("last_updated"):
            try:
                row["last_updated"] = datetime.strptime(row["last_updated"], "%Y-%m-%d").date()
            except Exception:
                row["last_updated"] = None
        return row

    def _upsert_sql(self) -> str:
        return """
        INSERT INTO benefit_accumulator
          (member_id, plan_id, service_category, allowed_amount, utilized_amount, last_updated)
        VALUES
          (:member_id, :plan_id, :service_category, :allowed_amount, :utilized_amount, :last_updated)
        """

    def _row_params(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return row
