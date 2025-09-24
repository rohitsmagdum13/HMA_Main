# src/hma_main/database/connection.py
"""
SQLAlchemy engine/session factory for RDS (MySQL).

Uses .env-backed settings (no Secrets Manager).
"""

from __future__ import annotations
from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from ..core.settings import settings
from ..core.logging_config import get_logger

logger = get_logger(__name__)

# Lazily create Engine; pool_pre_ping helps long-lived idle connections.
_engine = create_engine(
    settings.db_url(),
    pool_pre_ping=True,
    pool_recycle=300,
    pool_size=5,
    max_overflow=10,
    echo=False,
)

_SessionLocal = sessionmaker(bind=_engine, autoflush=False, autocommit=False)

def get_engine():
    """Return the singleton SQLAlchemy Engine."""
    return _engine

def get_session_factory():
    """Return the sessionmaker factory."""
    return _SessionLocal

@contextmanager
def session_scope() -> Session:
    """
    Provide a transactional scope around a series of operations.
    Commits on success; rolls back on exception; always closes.
    """
    session = _SessionLocal()
    try:
        yield session
        session.commit()
    except Exception as exc:
        logger.error("DB transaction error: %s", exc, exc_info=True)
        session.rollback()
        raise
    finally:
        session.close()
