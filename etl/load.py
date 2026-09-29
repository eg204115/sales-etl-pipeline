from __future__ import annotations

import io
import logging
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import Json, execute_values

from etl.config import DatabaseConfig

log = logging.getLogger("load")

SCHEMA_FILES = ("01_schema.sql", "02_indexes.sql")


@dataclass
class LoadResult:
    customers_upserted: int = 0
    products_upserted: int = 0
    sales_inserted: int = 0
    sales_updated: int = 0
    sales_unchanged: int = 0
    rejected_logged: int = 0

    @property
    def sales_loaded(self) -> int:
        return self.sales_inserted + self.sales_updated + self.sales_unchanged


class PostgresLoader:
    def __init__(self, cfg: DatabaseConfig, sql_dir: Path):
        self.cfg = cfg
        self.sql_dir = sql_dir
        self.conn = None

    def __enter__(self) -> "PostgresLoader":
        log.info("Connecting to PostgreSQL %s@%s:%s/%s",
                 self.cfg.user, self.cfg.host, self.cfg.port, self.cfg.name)
        self.conn = psycopg2.connect(**self.cfg.connect_kwargs(), connect_timeout=10,
                                     application_name="sales_etl_pipeline")
        return self

    def __exit__(self, *exc) -> None:
        if self.conn is not None:
            self.conn.close()

    def apply_schema(self) -> None:
        with self.conn, self.conn.cursor() as cur:
            for name in SCHEMA_FILES:
                cur.execute((self.sql_dir / name).read_text(encoding="utf-8"))
                log.info("Applied %s", name)

    def start_run(self, run_id: str, source_file: str, s3_raw_uri: str | None) -> None:
        with self.conn, self.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO etl_runs (run_id, source_file, s3_raw_uri) VALUES (%s, %s, %s)",
                (run_id, source_file, s3_raw_uri))

    def finish_run(self, run_id: str, status: str, stats=None, rows_loaded: int | None = None,
                   s3_processed_uri: str | None = None, error: str | None = None) -> None:
        with self.conn, self.conn.cursor() as cur:
            cur.execute(
                """UPDATE etl_runs
                      SET finished_at = now(), status = %s,
                          rows_extracted = %s, rows_loaded = %s, rows_rejected = %s,
                          exact_duplicates = %s, s3_processed_uri = %s, error_message = %s
                    WHERE run_id = %s""",
                (status,
                 stats.rows_extracted if stats else None,
                 rows_loaded,
                 stats.rows_rejected if stats else None,
                 stats.exact_duplicates if stats else None,
                 s3_processed_uri, error, run_id))

    def load(self, run_id: str, clean: pd.DataFrame, customers: pd.DataFrame,
             products: pd.DataFrame, rejected: pd.DataFrame) -> LoadResult:
        result = LoadResult()
        with self.conn, self.conn.cursor() as cur:
            cur.execute("""
                CREATE TEMP TABLE stg_customers (
                    customer_id VARCHAR(20), customer_name VARCHAR(150),
                    customer_email VARCHAR(255), country VARCHAR(100)) ON COMMIT DROP;
                CREATE TEMP TABLE stg_products (
                    product_id VARCHAR(20), product_name VARCHAR(200),
                    category VARCHAR(100)) ON COMMIT DROP;
                CREATE TEMP TABLE stg_sales (
                    transaction_id VARCHAR(20), order_date DATE, customer_id VARCHAR(20),
                    product_id VARCHAR(20), quantity INTEGER, unit_price NUMERIC(10,2),
                    discount NUMERIC(4,3), payment_method VARCHAR(30), rating SMALLINT
                ) ON COMMIT DROP;
            """)
            _copy(cur, customers, "stg_customers")
            _copy(cur, products, "stg_products")
            _copy(cur, clean, "stg_sales")

            cur.execute("""
                INSERT INTO customers AS t (customer_id, customer_name, customer_email, country)
                SELECT customer_id, customer_name, customer_email, country FROM stg_customers
                ON CONFLICT (customer_id) DO UPDATE
                   SET customer_name = EXCLUDED.customer_name,
                       customer_email = EXCLUDED.customer_email,
                       country = EXCLUDED.country,
                       updated_at = now()
                 WHERE (t.customer_name, t.customer_email, t.country)
                       IS DISTINCT FROM
                       (EXCLUDED.customer_name, EXCLUDED.customer_email, EXCLUDED.country)
            """)
            result.customers_upserted = cur.rowcount

            cur.execute("""
                INSERT INTO products AS t (product_id, product_name, category)
                SELECT product_id, product_name, category FROM stg_products
                ON CONFLICT (product_id) DO UPDATE
                   SET product_name = EXCLUDED.product_name,
                       category = EXCLUDED.category,
                       updated_at = now()
                 WHERE (t.product_name, t.category)
                       IS DISTINCT FROM (EXCLUDED.product_name, EXCLUDED.category)
            """)
            result.products_upserted = cur.rowcount

            cur.execute("""
                INSERT INTO sales AS t (transaction_id, order_date, customer_id, product_id,
                                        quantity, unit_price, discount, payment_method,
                                        rating, etl_run_id)
                SELECT transaction_id, order_date, customer_id, product_id, quantity,
                       unit_price, discount, payment_method, rating, %s
                FROM   stg_sales
                ON CONFLICT (transaction_id) DO UPDATE
                   SET order_date = EXCLUDED.order_date,
                       customer_id = EXCLUDED.customer_id,
                       product_id = EXCLUDED.product_id,
                       quantity = EXCLUDED.quantity,
                       unit_price = EXCLUDED.unit_price,
                       discount = EXCLUDED.discount,
                       payment_method = EXCLUDED.payment_method,
                       rating = EXCLUDED.rating,
                       etl_run_id = EXCLUDED.etl_run_id,
                       loaded_at = now()
                 WHERE (t.order_date, t.customer_id, t.product_id, t.quantity, t.unit_price,
                        t.discount, t.payment_method, t.rating)
                       IS DISTINCT FROM
                       (EXCLUDED.order_date, EXCLUDED.customer_id, EXCLUDED.product_id,
                        EXCLUDED.quantity, EXCLUDED.unit_price, EXCLUDED.discount,
                        EXCLUDED.payment_method, EXCLUDED.rating)
                RETURNING (xmax = 0) AS inserted
            """, (run_id,))
            flags = [row[0] for row in cur.fetchall()]
            result.sales_inserted = sum(flags)
            result.sales_updated = len(flags) - result.sales_inserted
            result.sales_unchanged = len(clean) - len(flags)

            result.rejected_logged = self._log_rejected(cur, run_id, rejected)

        log.info("Loaded: sales %s new / %s updated / %s unchanged; "
                 "customers upserted %s; products upserted %s; rejected logged %s",
                 f"{result.sales_inserted:,}", f"{result.sales_updated:,}",
                 f"{result.sales_unchanged:,}", result.customers_upserted,
                 result.products_upserted, f"{result.rejected_logged:,}")
        return result

    @staticmethod
    def _log_rejected(cur, run_id: str, rejected: pd.DataFrame) -> int:
        if rejected.empty:
            return 0
        raw_cols = [c for c in rejected.columns if c not in ("source_row_number", "reject_reasons")]
        rows = [
            (run_id, int(r["source_row_number"]), r["transaction_id"].strip()[:50] or None,
             r["reject_reasons"], Json({c: r[c] for c in raw_cols}))
            for r in rejected.to_dict("records")
        ]
        execute_values(cur, """
            INSERT INTO etl_rejected_records
                   (run_id, source_row_number, transaction_id, reject_reasons, raw_record)
            VALUES %s""", rows, page_size=1000)
        return len(rows)


def _copy(cur, df: pd.DataFrame, table: str) -> None:
    cur.execute(f"SELECT * FROM {table} LIMIT 0")
    columns = [d[0] for d in cur.description]
    buf = io.StringIO()
    df[columns].to_csv(buf, index=False, header=False, na_rep="")
    buf.seek(0)
    cur.copy_expert(
        f"COPY {table} ({', '.join(columns)}) FROM STDIN WITH (FORMAT csv, NULL '')", buf)


__all__ = ["PostgresLoader", "LoadResult"]
