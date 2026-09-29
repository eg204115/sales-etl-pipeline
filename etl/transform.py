from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date

import numpy as np
import pandas as pd

log = logging.getLogger("transform")

NULL_TOKENS = {"", "na", "n/a", "null", "none", "nan", "-", "?", "unknown"}

DATE_FORMATS = [
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%d/%m/%Y",
    "%b %d, %Y",
    "%d-%b-%Y",
    "%Y-%m-%dT%H:%M:%S",
]

COUNTRY_ALIASES = {
    "united states": "United States", "usa": "United States", "us": "United States",
    "united states of america": "United States",
    "united kingdom": "United Kingdom", "uk": "United Kingdom",
    "great britain": "United Kingdom", "england": "United Kingdom",
    "sri lanka": "Sri Lanka", "srilanka": "Sri Lanka", "lk": "Sri Lanka",
    "india": "India", "in": "India",
    "germany": "Germany", "deutschland": "Germany", "de": "Germany",
    "australia": "Australia", "aus": "Australia", "au": "Australia",
    "canada": "Canada", "ca": "Canada",
    "japan": "Japan", "jp": "Japan",
    "france": "France", "fr": "France",
    "singapore": "Singapore", "sg": "Singapore",
}

CATEGORIES = [
    "Electronics", "Home & Kitchen", "Clothing", "Books",
    "Sports & Outdoors", "Beauty", "Toys", "Groceries",
]
CATEGORY_ALIASES = {c.lower(): c for c in CATEGORIES}

PAYMENT_ALIASES = {
    "credit card": "Credit Card", "cc": "Credit Card",
    "debit card": "Debit Card",
    "paypal": "PayPal", "pay pal": "PayPal",
    "cash on delivery": "Cash on Delivery", "cod": "Cash on Delivery",
    "bank transfer": "Bank Transfer", "wire": "Bank Transfer",
}

EMAIL_RE = r"^[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}$"

BUSINESS_COLUMNS = [
    "transaction_id", "order_date", "customer_id", "product_id",
    "quantity", "unit_price", "discount", "payment_method", "rating",
]

OUTPUT_COLUMNS = [
    "transaction_id", "order_date", "customer_id", "customer_name", "customer_email",
    "country", "product_id", "product_name", "category", "quantity", "unit_price",
    "discount", "total_amount", "payment_method", "rating", "source_row_number",
]


@dataclass
class TransformStats:
    rows_extracted: int = 0
    rows_clean: int = 0
    rows_rejected: int = 0
    exact_duplicates: int = 0
    conflicting_duplicates: int = 0
    reject_reasons: dict[str, int] = field(default_factory=dict)
    fixes: dict[str, int] = field(default_factory=dict)

    @property
    def reject_ratio(self) -> float:
        return self.rows_rejected / self.rows_extracted if self.rows_extracted else 0.0


@dataclass
class TransformResult:
    clean: pd.DataFrame
    rejected: pd.DataFrame
    stats: TransformStats


def clean_text(s: pd.Series) -> pd.Series:
    s = s.astype("string").str.strip().str.replace(r"\s+", " ", regex=True)
    return s.mask(s.str.lower().isin(NULL_TOKENS))


def parse_dates(s: pd.Series) -> pd.Series:
    s = clean_text(s)
    result = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")
    for fmt in DATE_FORMATS:
        todo = result.isna() & s.notna()
        if not todo.any():
            break
        result.loc[todo] = pd.to_datetime(s[todo], format=fmt, errors="coerce")
    return result.dt.normalize()


def parse_decimal(s: pd.Series) -> pd.Series:
    s = clean_text(s).str.replace(r"(?i)usd|\$|,|\s", "", regex=True)
    return pd.to_numeric(s, errors="coerce")


def parse_discount(s: pd.Series) -> pd.Series:
    s = clean_text(s)
    is_pct = s.str.endswith("%").fillna(False).astype(bool)
    value = pd.to_numeric(s.str.rstrip("%"), errors="coerce")
    return value.where(~is_pct, value / 100)


def normalize_key(s: pd.Series) -> pd.Series:
    return (clean_text(s).str.lower()
            .str.replace(".", "", regex=False)
            .str.replace(r"[_\-]+", " ", regex=True)
            .str.replace(r"\s+", " ", regex=True).str.strip())


def standardize_country(s: pd.Series) -> pd.Series:
    key = normalize_key(s)
    mapped = key.map(COUNTRY_ALIASES)
    unmapped = mapped.isna() & key.notna()
    if unmapped.any():
        log.warning("Unmapped country values kept as title case: %s",
                    sorted(key[unmapped].unique().tolist())[:10])
    return mapped.fillna(key.str.title()).astype("string")


def standardize_category(s: pd.Series) -> pd.Series:
    key = clean_text(s).str.lower()
    return key.map(CATEGORY_ALIASES).fillna(key.str.title()).astype("string")


def standardize_payment(s: pd.Series) -> pd.Series:
    key = normalize_key(s)
    mapped = key.map(PAYMENT_ALIASES)
    return mapped.mask(mapped.isna() & key.notna(), "Other").astype("string")


def is_whole_number(s: pd.Series) -> pd.Series:
    return s.notna() & np.isclose(s.fillna(0) % 1, 0)


def conform_by_key(df: pd.DataFrame, key: str, col: str) -> pd.Series:
    known = df[[key, col]].dropna()
    if known.empty:
        return df[col]
    mode = known.groupby(key)[col].agg(lambda x: x.value_counts().index[0])
    return df[key].map(mode).astype("string").fillna(df[col])


def transform(raw: pd.DataFrame, min_order_date: str = "2015-01-01",
              today: date | None = None) -> TransformResult:
    today = today or date.today()
    stats = TransformStats(rows_extracted=len(raw))
    df = pd.DataFrame(index=raw.index)
    df["source_row_number"] = raw["source_row_number"]

    for col in ("transaction_id", "customer_id", "product_id"):
        df[col] = clean_text(raw[col]).str.upper()

    df["order_date"] = parse_dates(raw["order_date"])
    df["customer_name"] = clean_text(raw["customer_name"]).str.title()
    df["customer_email"] = clean_text(raw["customer_email"]).str.lower()
    df["country"] = standardize_country(raw["country"])
    df["product_name"] = clean_text(raw["product_name"])
    df["category"] = standardize_category(raw["category"])
    df["payment_method"] = standardize_payment(raw["payment_method"])

    df["quantity"] = parse_decimal(raw["quantity"])
    df["unit_price"] = parse_decimal(raw["unit_price"]).round(2)
    df["discount"] = parse_discount(raw["discount"])
    df["rating"] = parse_decimal(raw["rating"])

    fixes = stats.fixes

    bad_email = df["customer_email"].notna() & ~df["customer_email"].str.match(EMAIL_RE).fillna(False)
    fixes["invalid_emails_nulled"] = int(bad_email.sum())
    df.loc[bad_email, "customer_email"] = pd.NA

    discount_missing = clean_text(raw["discount"]).isna()
    fixes["missing_discount_set_to_0"] = int(discount_missing.sum())
    df.loc[discount_missing, "discount"] = 0.0
    df["discount"] = df["discount"].round(3)

    for col, key in (("customer_name", "customer_id"), ("customer_email", "customer_id"),
                     ("country", "customer_id"), ("product_name", "product_id"),
                     ("category", "product_id")):
        before = df[col].isna().sum()
        df[col] = conform_by_key(df, key, col)
        fixes[f"{col}_filled_from_same_{key.split('_')[0]}"] = int(before - df[col].isna().sum())

    defaults = {"customer_name": "Unknown Customer", "country": "Unknown",
                "product_name": "Unknown Product", "category": "Uncategorized",
                "payment_method": "Unknown"}
    for col, default in defaults.items():
        n = int(df[col].isna().sum())
        if n:
            fixes[f"{col}_defaulted"] = n
            df[col] = df[col].fillna(default)

    raw_rating_present = clean_text(raw["rating"]).notna()
    raw_discount_present = ~discount_missing
    order_date = df["order_date"]

    rules: dict[str, pd.Series] = {
        "missing_transaction_id": df["transaction_id"].isna(),
        "invalid_transaction_id": df["transaction_id"].notna()
        & ~df["transaction_id"].str.match(r"^TXN-\d{7}$").fillna(False),
        "missing_customer_id": df["customer_id"].isna(),
        "missing_product_id": df["product_id"].isna(),
        "invalid_order_date": order_date.isna(),
        "future_order_date": order_date > pd.Timestamp(today),
        "order_date_before_min": order_date < pd.Timestamp(min_order_date),
        "invalid_quantity": ~is_whole_number(df["quantity"]) | (df["quantity"] <= 0),
        "invalid_unit_price": df["unit_price"].isna() | (df["unit_price"] <= 0),
        "invalid_discount": raw_discount_present
        & (df["discount"].isna() | (df["discount"] < 0) | (df["discount"] >= 1)),
        "invalid_rating": raw_rating_present
        & (~is_whole_number(df["rating"]) | ~df["rating"].between(1, 5)),
    }
    reasons = pd.Series("", index=df.index, dtype="object")
    for name, mask in rules.items():
        mask = mask.fillna(False).astype(bool)
        reasons = reasons.where(~mask, reasons + name + ";")
    reasons = reasons.str.rstrip(";")
    invalid = reasons != ""

    valid = df[~invalid]
    exact_dup = valid.duplicated(subset=BUSINESS_COLUMNS, keep="first")
    key_dup = valid[~exact_dup].duplicated(subset=["transaction_id"], keep="first")
    reasons.loc[exact_dup[exact_dup].index] = "exact_duplicate"
    reasons.loc[key_dup[key_dup].index] = "duplicate_transaction_id_conflict"
    stats.exact_duplicates = int(exact_dup.sum())
    stats.conflicting_duplicates = int(key_dup.sum())

    rejected_mask = reasons != ""

    clean = df[~rejected_mask].copy()
    clean["quantity"] = clean["quantity"].astype(int)
    clean["rating"] = clean["rating"].round().astype("Int64")
    clean["order_date"] = clean["order_date"].dt.date
    clean["total_amount"] = (clean["quantity"] * clean["unit_price"] * (1 - clean["discount"])).round(2)
    clean = clean[OUTPUT_COLUMNS].sort_values("transaction_id").reset_index(drop=True)

    rejected = raw[rejected_mask].copy()
    rejected.insert(1, "reject_reasons", reasons[rejected_mask])
    rejected = rejected.reset_index(drop=True)

    stats.rows_clean = len(clean)
    stats.rows_rejected = len(rejected)
    stats.reject_reasons = (reasons[rejected_mask].str.split(";").explode()
                            .value_counts().to_dict())
    assert stats.rows_clean + stats.rows_rejected == stats.rows_extracted, "row reconciliation failed"

    log.info("Transform done: %s clean, %s rejected (%.1f%%), %s exact duplicates, "
             "%s conflicting duplicates", f"{stats.rows_clean:,}", f"{stats.rows_rejected:,}",
             stats.reject_ratio * 100, stats.exact_duplicates, stats.conflicting_duplicates)
    for reason, count in stats.reject_reasons.items():
        log.info("  rejected  %-34s %6s", reason, f"{count:,}")
    for fix, count in fixes.items():
        if count:
            log.info("  fixed     %-34s %6s", fix, f"{count:,}")
    return TransformResult(clean=clean, rejected=rejected, stats=stats)


def build_dimensions(clean: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    customers = (clean[["customer_id", "customer_name", "customer_email", "country"]]
                 .drop_duplicates("customer_id").sort_values("customer_id").reset_index(drop=True))
    products = (clean[["product_id", "product_name", "category"]]
                .drop_duplicates("product_id").sort_values("product_id").reset_index(drop=True))
    return customers, products
