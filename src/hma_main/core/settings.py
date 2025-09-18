# src/hma_main/core/settings.py
from __future__ import annotations

from pathlib import Path
from typing import Optional
from urllib.parse import quote_plus

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # ---------------- AWS Configuration ----------------
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_default_region: str = "ap-south-1"
    aws_profile: Optional[str] = None  # leave empty to use env/role

    # ---------------- S3 Buckets ----------------
    s3_bucket_mba: str = "hma-mba-bucket"
    s3_bucket_policy: str = "hma-policy-bucket"

    # ---------------- S3 Prefixes ----------------
    # keep trailing slash to match your existing paths (e.g., "mba/csv/…")
    s3_prefix_mba: str = "mba/"
    s3_prefix_policy: str = "policy/"

    # Optional: server-side encryption for uploads (AES256 or aws:kms)
    s3_sse: str = "AES256"

    # ---------------- Database (MySQL/RDS) ----------------
    db_host: str = "localhost"
    db_port: int = 3306
    db_name: str = "hma"
    db_user: str = "root"
    db_password: str = ""
    # e.g., "charset=utf8mb4" or "charset=utf8mb4&ssl_disabled=false"
    db_params: str = "charset=utf8mb4"

    # ---------------- Logging ----------------
    log_level: str = "INFO"
    log_dir: Path = Path("logs")
    log_file: str = "app.log"

    # ---------------- Pydantic Settings ----------------
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ---------------- Helpers ----------------
    def get_bucket(self, scope: str) -> str:
        """Return bucket for 'mba' or 'policy' (case-insensitive)."""
        s = scope.strip().lower()
        if s == "mba":
            return self.s3_bucket_mba
        if s == "policy":
            return self.s3_bucket_policy
        raise ValueError(f"Invalid scope: {scope}")

    def get_prefix(self, scope: str) -> str:
        """Return S3 prefix (with trailing '/'): 'mba/' or 'policy/'."""
        s = scope.strip().lower()
        if s == "mba":
            return self.s3_prefix_mba
        if s == "policy":
            return self.s3_prefix_policy
        raise ValueError(f"Invalid scope: {scope}")

    def db_url(self) -> str:
        """SQLAlchemy URL: mysql+pymysql://USER:PWD@HOST:PORT/NAME?PARAMS"""
        pwd = quote_plus(self.db_password)
        return (
            f"mysql+pymysql://{self.db_user}:{pwd}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
            f"?{self.db_params}"
        )


# Singleton instance used across the app
settings = Settings()
