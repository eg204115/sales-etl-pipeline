from datetime import date

import pandas as pd
import pytest

from etl.extract import EXPECTED_COLUMNS
from etl.transform import (
    clean_text, parse_dates, parse_decimal, parse_discount,
    standardize_category, standardize_country, standardize_payment, transform,
)

TODAY = date(2026, 1, 1)


def make_raw(rows: list[dict]) -> pd.DataFrame:
    base = {
        "transaction_id": "TXN-0000001", "order_date": "2025-03-10", "customer_id": "CUST-00001",
        "customer_name": "Jane Doe", "customer_email": "jane.doe@mail.com", "country": "USA",
        "product_id": "PRD-0001", "product_name": "Wireless Earbuds", "category": "Electronics",
        "quantity": "2", "unit_price": "10.00", "discount": "0.1",
        "payment_method": "Credit Card", "rating": "5",
    }
    df = pd.DataFrame([{**base, **r} for r in rows])[EXPECTED_COLUMNS]
    df.insert(0, "source_row_number", range(2, len(df) + 2))
    return df


def test_clean_text_trims_and_nulls_tokens():
    out = clean_text(pd.Series(["  a   b ", "N/A", "", "null", "ok"]))
    assert out.tolist()[0] == "a b"
    assert out.isna().tolist() == [False, True, True, True, False]


def test_parse_dates_handles_mixed_formats():
    s = pd.Series(["2024-01-05", "2024/01/05", "05/01/2024", "Jan 05, 2024",
                   "05-Jan-2024", "2024-01-05T10:30:00", "not a date", "2024-13-45", ""])
    out = parse_dates(s)
    assert (out[:6] == pd.Timestamp("2024-01-05")).all()
    assert out[6:].isna().all()


@pytest.mark.parametrize("raw, expected", [
    ("$1,299.00", 1299.0), ("1,299.00 USD", 1299.0), (" 12.5 ", 12.5), ("3.0", 3.0),
])
def test_parse_decimal(raw, expected):
    assert parse_decimal(pd.Series([raw]))[0] == expected


def test_parse_decimal_garbage_is_nan():
    assert parse_decimal(pd.Series(["free", "two", ""])).isna().all()


def test_parse_discount_percent_and_fraction():
    out = parse_discount(pd.Series(["15%", "0.15", "", "abc"]))
    assert out[0] == pytest.approx(0.15) and out[1] == pytest.approx(0.15)
    assert pd.isna(out[2]) and pd.isna(out[3])


def test_standardize_country_aliases():
    s = pd.Series(["USA", "U.S.A.", " united states ", "UK", "Srilanka", "Deutschland", "", "n/a"])
    out = standardize_country(s)
    assert out[:6].tolist() == ["United States"] * 3 + ["United Kingdom", "Sri Lanka", "Germany"]
    assert out[6:].isna().all()


def test_standardize_category_and_payment():
    assert standardize_category(pd.Series(["  ELECTRONICS ", "home & kitchen"])).tolist() == \
        ["Electronics", "Home & Kitchen"]
    assert standardize_payment(pd.Series(["CREDIT_CARD", "cc", "Pay Pal", "COD", "bitcoin"])).tolist() == \
        ["Credit Card", "Credit Card", "PayPal", "Cash on Delivery", "Other"]


def test_valid_row_is_cleaned():
    res = transform(make_raw([{"customer_name": "  JANE   DOE ", "unit_price": "$10.00",
                               "category": "electronics", "discount": ""}]), today=TODAY)
    row = res.clean.iloc[0]
    assert res.stats.rows_rejected == 0
    assert row["customer_name"] == "Jane Doe"
    assert row["category"] == "Electronics"
    assert row["country"] == "United States"
    assert row["discount"] == 0.0
    assert row["total_amount"] == 20.0


@pytest.mark.parametrize("override, reason", [
    ({"transaction_id": ""}, "missing_transaction_id"),
    ({"order_date": "not a date"}, "invalid_order_date"),
    ({"order_date": "2027-05-01"}, "future_order_date"),
    ({"quantity": "-1"}, "invalid_quantity"),
    ({"quantity": "two"}, "invalid_quantity"),
    ({"unit_price": "free"}, "invalid_unit_price"),
    ({"discount": "1.5"}, "invalid_discount"),
    ({"rating": "6"}, "invalid_rating"),
    ({"customer_id": ""}, "missing_customer_id"),
])
def test_invalid_rows_are_rejected_with_reason(override, reason):
    res = transform(make_raw([override]), today=TODAY)
    assert res.stats.rows_clean == 0
    assert reason in res.rejected.loc[0, "reject_reasons"]


def test_missing_rating_is_allowed():
    res = transform(make_raw([{"rating": ""}]), today=TODAY)
    assert res.stats.rows_clean == 1 and pd.isna(res.clean.loc[0, "rating"])


def test_multiple_reasons_are_all_reported():
    res = transform(make_raw([{"quantity": "0", "unit_price": "N/A"}]), today=TODAY)
    reasons = res.rejected.loc[0, "reject_reasons"].split(";")
    assert {"invalid_quantity", "invalid_unit_price"} <= set(reasons)


def test_duplicates_exact_and_reformatted():
    raw = make_raw([
        {},
        {},
        {"order_date": "10/03/2025", "category": "ELECTRONICS"},
        {"quantity": "5"},
        {"transaction_id": "TXN-0000002"},
    ])
    res = transform(raw, today=TODAY)
    assert res.stats.rows_clean == 2
    assert res.stats.exact_duplicates == 2
    assert res.stats.conflicting_duplicates == 1
    assert res.clean["transaction_id"].is_unique


def test_missing_attributes_filled_from_same_customer():
    raw = make_raw([
        {"transaction_id": "TXN-0000001", "country": "Sri Lanka"},
        {"transaction_id": "TXN-0000002", "country": "", "customer_name": "NULL"},
    ])
    res = transform(raw, today=TODAY)
    assert res.clean["country"].tolist() == ["Sri Lanka", "Sri Lanka"]
    assert res.clean["customer_name"].tolist() == ["Jane Doe", "Jane Doe"]


def test_invalid_email_is_nulled_not_rejected():
    res = transform(make_raw([{"customer_email": "jane at mail.com"}]), today=TODAY)
    assert res.stats.rows_clean == 1 and pd.isna(res.clean.loc[0, "customer_email"])


def test_reconciliation_invariant():
    raw = make_raw([{}, {}, {"quantity": "-1", "transaction_id": "TXN-0000009"}])
    s = transform(raw, today=TODAY).stats
    assert s.rows_clean + s.rows_rejected == s.rows_extracted == 3
