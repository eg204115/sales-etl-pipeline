from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

log = logging.getLogger("extract")

EXPECTED_COLUMNS = [
    "transaction_id", "order_date", "customer_id", "customer_name", "customer_email",
    "country", "product_id", "product_name", "category", "quantity", "unit_price",
    "discount", "payment_method", "rating",
]


class SchemaError(ValueError):
    pass


def extract_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Raw file not found: {path}")

    df = pd.read_csv(path, dtype=str, keep_default_na=False, encoding="utf-8")
    df.columns = [c.strip().lower() for c in df.columns]

    missing = [c for c in EXPECTED_COLUMNS if c not in df.columns]
    if missing:
        raise SchemaError(f"Raw file is missing columns: {missing}")

    unexpected = [c for c in df.columns if c not in EXPECTED_COLUMNS]
    if unexpected:
        log.warning("Ignoring unexpected columns: %s", unexpected)

    df = df[EXPECTED_COLUMNS].copy()
    df.insert(0, "source_row_number", range(2, len(df) + 2))

    log.info("Extracted %s rows x %s columns from %s", f"{len(df):,}", len(EXPECTED_COLUMNS), path.name)
    return df
