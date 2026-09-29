from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from etl.config import S3Config

log = logging.getLogger("s3")


class S3Error(RuntimeError):
    pass


class S3Storage:
    def __init__(self, cfg: S3Config):
        if not cfg.enabled:
            raise S3Error("S3_BUCKET is not set")
        self.cfg = cfg
        self.client = boto3.client(
            "s3", region_name=cfg.region,
            config=Config(retries={"max_attempts": 5, "mode": "standard"}))

    def key_for(self, zone: str, run_id: str, filename: str, day: date | None = None) -> str:
        day = day or date.today()
        return f"{self.cfg.prefix}/{zone}/ingest_date={day.isoformat()}/run_id={run_id}/{filename}"

    def uri(self, key: str) -> str:
        return f"s3://{self.cfg.bucket}/{key}"

    def upload(self, path: Path, key: str, run_id: str) -> str:
        try:
            self.client.upload_file(
                str(path), self.cfg.bucket, key,
                ExtraArgs={"ServerSideEncryption": "AES256",
                           "Metadata": {"run-id": run_id, "source-file": path.name}})
        except (BotoCoreError, ClientError) as exc:
            raise S3Error(f"Upload of {path.name} to {self.uri(key)} failed: {exc}") from exc
        log.info("Uploaded %s (%.1f KB) -> %s", path.name, path.stat().st_size / 1024, self.uri(key))
        return self.uri(key)

    def download(self, key: str, dest: Path) -> Path:
        dest.parent.mkdir(parents=True, exist_ok=True)
        try:
            self.client.download_file(self.cfg.bucket, key, str(dest))
        except (BotoCoreError, ClientError) as exc:
            raise S3Error(f"Download of {self.uri(key)} failed: {exc}") from exc
        log.info("Downloaded %s -> %s", self.uri(key), dest)
        return dest
