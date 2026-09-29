from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")


def _env(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    return value.strip()


@dataclass(frozen=True)
class DatabaseConfig:
    host: str = field(default_factory=lambda: _env("DB_HOST", "localhost"))
    port: int = field(default_factory=lambda: int(_env("DB_PORT", "5432")))
    name: str = field(default_factory=lambda: _env("DB_NAME", "sales_dw"))
    user: str = field(default_factory=lambda: _env("DB_USER", "etl_user"))
    password: str | None = field(default_factory=lambda: _env("DB_PASSWORD"), repr=False)

    def connect_kwargs(self) -> dict:
        return {"host": self.host, "port": self.port, "dbname": self.name,
                "user": self.user, "password": self.password}


@dataclass(frozen=True)
class S3Config:
    bucket: str | None = field(default_factory=lambda: _env("S3_BUCKET"))
    prefix: str = field(default_factory=lambda: _env("S3_PREFIX", "sales-etl").strip("/"))
    region: str = field(default_factory=lambda: _env("AWS_REGION", "us-east-1"))

    @property
    def enabled(self) -> bool:
        return bool(self.bucket)


@dataclass(frozen=True)
class PipelineConfig:
    raw_file: Path = field(
        default_factory=lambda: PROJECT_ROOT / _env("RAW_FILE", "data/raw/sales_raw.csv"))
    processed_dir: Path = PROJECT_ROOT / "data" / "processed"
    rejected_dir: Path = PROJECT_ROOT / "data" / "rejected"
    log_dir: Path = PROJECT_ROOT / "logs"
    sql_dir: Path = PROJECT_ROOT / "sql"
    max_reject_ratio: float = field(
        default_factory=lambda: float(_env("MAX_REJECT_RATIO", "0.20")))
    min_order_date: str = field(default_factory=lambda: _env("MIN_ORDER_DATE", "2015-01-01"))
    log_level: str = field(default_factory=lambda: _env("LOG_LEVEL", "INFO"))
    db: DatabaseConfig = field(default_factory=DatabaseConfig)
    s3: S3Config = field(default_factory=S3Config)
