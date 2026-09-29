from __future__ import annotations

import argparse
import logging
import sys
import time
import uuid
from contextlib import contextmanager
from pathlib import Path

from etl.config import PipelineConfig
from etl.extract import extract_csv
from etl.logger import setup_logging
from etl.transform import build_dimensions, transform

log = logging.getLogger("pipeline")


class DataQualityError(RuntimeError):
    pass


@contextmanager
def step(name: str):
    log.info(">> %s", name)
    started = time.perf_counter()
    yield
    log.info("OK %s (%.2fs)", name, time.perf_counter() - started)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Sales ETL pipeline")
    p.add_argument("--input", type=Path, help="raw CSV path (default: RAW_FILE from .env)")
    p.add_argument("--generate", action="store_true", help="generate a fresh raw dataset first")
    p.add_argument("--rows", type=int, default=12000, help="rows to generate with --generate")
    p.add_argument("--source", choices=["local", "s3"], default="local",
                   help="read the raw file from local disk (default) or from S3")
    p.add_argument("--s3-key", help="S3 object key of the raw file when --source s3")
    p.add_argument("--skip-s3", action="store_true", help="do not use S3 at all")
    p.add_argument("--skip-db", action="store_true", help="do not load into PostgreSQL")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    cfg = PipelineConfig()
    run_id = str(uuid.uuid4())
    log_file = setup_logging(cfg.log_dir, run_id, cfg.log_level)
    raw_path: Path = (args.input or cfg.raw_file).resolve()

    use_s3 = cfg.s3.enabled and not args.skip_s3
    if not cfg.s3.enabled and not args.skip_s3:
        log.warning("S3_BUCKET is not set -> S3 steps are skipped")

    log.info("=" * 70)
    log.info("Sales ETL run %s", run_id)
    log.info("=" * 70)

    s3 = loader = None
    stats = None
    s3_raw_uri = s3_processed_uri = None
    started = time.perf_counter()

    try:
        if use_s3:
            from etl.s3_storage import S3Storage
            s3 = S3Storage(cfg.s3)

        if args.source == "s3":
            if not (s3 and args.s3_key):
                raise ValueError("--source s3 needs S3 enabled and --s3-key")
            with step("Download raw file from S3"):
                raw_path = s3.download(args.s3_key, cfg.processed_dir.parent / "raw" / Path(args.s3_key).name)
                s3_raw_uri = s3.uri(args.s3_key)
        elif args.generate or not raw_path.exists():
            from scripts.generate_dataset import generate
            with step(f"Generate raw dataset ({args.rows:,} rows)"):
                generate(args.rows, seed=42, output=raw_path)

        if s3 and args.source == "local":
            with step("Upload raw file to S3 (landing zone)"):
                s3_raw_uri = s3.upload(raw_path, s3.key_for("raw", run_id, raw_path.name), run_id)

        if not args.skip_db:
            from etl.load import PostgresLoader
            loader = PostgresLoader(cfg.db, cfg.sql_dir).__enter__()
            with step("Apply schema & indexes"):
                loader.apply_schema()
            loader.start_run(run_id, str(raw_path.name), s3_raw_uri)

        with step("Extract"):
            raw = extract_csv(raw_path)
        with step("Transform & validate"):
            result = transform(raw, min_order_date=cfg.min_order_date)
            stats = result.stats
            customers, products = build_dimensions(result.clean)

        with step("Write processed & rejected files"):
            cfg.processed_dir.mkdir(parents=True, exist_ok=True)
            cfg.rejected_dir.mkdir(parents=True, exist_ok=True)
            clean_path = cfg.processed_dir / "sales_clean.csv"
            rejected_path = cfg.rejected_dir / "sales_rejected.csv"
            result.clean.to_csv(clean_path, index=False)
            result.rejected.to_csv(rejected_path, index=False)
            log.info("Clean -> %s | Rejected -> %s", clean_path.name, rejected_path.name)

        if stats.reject_ratio > cfg.max_reject_ratio:
            raise DataQualityError(
                f"Reject ratio {stats.reject_ratio:.1%} exceeds MAX_REJECT_RATIO "
                f"{cfg.max_reject_ratio:.0%}; aborting before load")

        rows_loaded = None
        if loader:
            with step("Load into PostgreSQL"):
                load_result = loader.load(run_id, result.clean, customers, products, result.rejected)
                rows_loaded = load_result.sales_loaded

        if s3:
            with step("Back up processed & rejected files to S3"):
                s3_processed_uri = s3.upload(clean_path, s3.key_for("processed", run_id, clean_path.name), run_id)
                s3.upload(rejected_path, s3.key_for("rejected", run_id, rejected_path.name), run_id)

        if loader:
            loader.finish_run(run_id, "success", stats, rows_loaded, s3_processed_uri)

    except Exception as exc:
        log.exception("Pipeline FAILED: %s", exc)
        if loader:
            try:
                loader.conn.rollback()
                loader.finish_run(run_id, "failed", stats, None, s3_processed_uri, str(exc)[:2000])
            except Exception:
                log.error("Could not record the failure in etl_runs")
        return 1
    finally:
        if loader:
            loader.__exit__(None, None, None)

    log.info("-" * 70)
    log.info("Run %s SUCCEEDED in %.2fs", run_id, time.perf_counter() - started)
    log.info("  extracted : %s", f"{stats.rows_extracted:,}")
    log.info("  clean     : %s", f"{stats.rows_clean:,}")
    log.info("  rejected  : %s (%.1f%%)", f"{stats.rows_rejected:,}", stats.reject_ratio * 100)
    if s3_raw_uri:
        log.info("  S3 raw    : %s", s3_raw_uri)
    if s3_processed_uri:
        log.info("  S3 clean  : %s", s3_processed_uri)
    log.info("  log file  : %s", log_file)
    return 0


if __name__ == "__main__":
    sys.exit(main())
