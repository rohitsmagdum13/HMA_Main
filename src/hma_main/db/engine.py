"""
Builds a SQLAlchemy engine from .env via core.settings.
Usage:
  from hma_main.db.engine import get_engine, run_schema_sql
"""

from __future__ import annotations
from pathlib import Path
from sqlalchemy import create_engine, text
from hma_main.core.settings import settings


def get_engine(echo: bool = False):
    """
    Returns a SQLAlchemy engine using the URL from settings.db_url().
    Args:
      echo: set True to see SQL (debug).
    """
    return create_engine(settings.db_url(), echo=echo, pool_pre_ping=True)


def run_schema_sql(schema_path: str | Path) -> None:
    """
    Executes the SQL statements in schema.sql to create tables.
    Args:
      schema_path: path to a .sql file with DDL; multiple statements supported.
    """
    path = Path(schema_path)
    sql = path.read_text(encoding="utf-8")
    stmts = [s.strip() for s in sql.split(";") if s.strip()]
    eng = get_engine()
    with eng.begin() as conn:
        for s in stmts:
            conn.execute(text(s))
