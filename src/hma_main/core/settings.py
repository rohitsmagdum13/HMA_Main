"""
Centralized configuration management using Pydantic.

This module defines the `Settings` class, which loads application
configuration from environment variables or a `.env` file. It supports
AWS, S3, database, and logging configuration.

Helper methods provide validated access to S3 bucket/prefix and
generate SQLAlchemy database URLs.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional
from urllib.parse import quote_plus

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables and `.env` file."""

    # ---------------- AWS Configuration ----------------
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_default_region: str = "ap-south-1"
    aws_profile: Optional[str] = None  # Profile overrides env/role

    # ---------------- S3 Buckets ----------------
    s3_bucket_mba: str = "hma-mba-bucket"
    s3_bucket_policy: str = "hma-policy-bucket"

    # ---------------- S3 Prefixes ----------------
    s3_prefix_mba: str = "mba/"      # Trailing slash required
    s3_prefix_policy: str = "policy/"

    # Optional: server-side encryption for uploads (AES256 or aws:kms)
    s3_sse: str = "AES256"

    # ---------------- Database (MySQL/RDS) ----------------
    RDS_HOST: str = "mysql-hma.cobyueoimrmh.us-east-1.rds.amazonaws.com"
    RDS_PORT: int = 3306
    RDS_DATABASE: str = "hma_Mysql"
    RDS_USERNAME: str = "admin"
    RDS_PASSWORD: str = "Admin12345"
    RDS_params: str = "charset=utf8mb4"  # Extra params for SQLAlchemy URL

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

    # ---------------- Helper Methods ----------------
    def get_bucket(self, scope: str) -> str:
        """
        Get bucket name for given scope.

        Args:
            scope: "mba" or "policy".

        Returns:
            Corresponding bucket name.

        Raises:
            ValueError: If scope is invalid.
        """
        s = scope.strip().lower()
        if s == "mba":
            return self.s3_bucket_mba
        if s == "policy":
            return self.s3_bucket_policy
        raise ValueError(f"Invalid scope: {scope}")

    def get_prefix(self, scope: str) -> str:
        """
        Get S3 prefix for given scope.

        Args:
            scope: "mba" or "policy".

        Returns:
            Corresponding S3 prefix (always ends with '/').
        """
        s = scope.strip().lower()
        if s == "mba":
            return self.s3_prefix_mba
        if s == "policy":
            return self.s3_prefix_policy
        raise ValueError(f"Invalid scope: {scope}")

    def db_url(self) -> str:
        """
        Build SQLAlchemy connection URL for MySQL.

        Returns:
            URL string: mysql+pymysql://USER:PWD@HOST:PORT/NAME?PARAMS
        """
        pwd = quote_plus(self.RDS_PASSWORD)
        return (
            f"mysql+pymysql://{self.RDS_USERNAME}:{pwd}"
            f"@{self.RDS_HOST}:{self.RDS_PORT}/{self.RDS_DATABASE}"
            f"?{self.RDS_params}"
        )


# Singleton instance shared across the app
settings = Settings()
