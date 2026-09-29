from __future__ import annotations

import argparse
import csv
import random
from datetime import date, timedelta
from pathlib import Path

FIRST_NAMES = [
    "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
    "William", "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
    "Thomas", "Sarah", "Charles", "Karen", "Nethmi", "Kasun", "Ayesha", "Ravi", "Priya",
    "Chen", "Mei", "Hiroshi", "Yuki", "Lukas", "Emma", "Mateo", "Sofia", "Olivia", "Liam",
    "Noah", "Amara", "Kofi", "Fatima", "Omar",
]
LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Perera", "Fernando", "Silva", "Jayasinghe", "Wang", "Li",
    "Tanaka", "Sato", "Muller", "Schmidt", "Rossi", "Dubois", "Kumar", "Patel", "Singh",
    "Okafor", "Mensah", "Khan", "Nguyen", "Anderson",
]

COUNTRIES = {
    "United States": ["United States", "USA", "U.S.A.", "united states", "US", "usa "],
    "United Kingdom": ["United Kingdom", "UK", "U.K.", "united kingdom", "Great Britain"],
    "Sri Lanka": ["Sri Lanka", "sri lanka", "SRI LANKA", "Srilanka", " Sri Lanka"],
    "India": ["India", "india", "INDIA", "IN"],
    "Germany": ["Germany", "germany", "Deutschland", "DE"],
    "Australia": ["Australia", "australia", "AUS", "AU"],
    "Canada": ["Canada", "canada", "CANADA", "CA"],
    "Japan": ["Japan", "japan", "JP"],
    "France": ["France", "france", "FR"],
    "Singapore": ["Singapore", "singapore", "SG"],
}
COUNTRY_WEIGHTS = [30, 12, 10, 12, 8, 7, 7, 5, 5, 4]

CATALOG = {
    "Electronics": [
        ("Wireless Earbuds", 79.99), ("Bluetooth Speaker", 49.99), ("4K Monitor", 329.00),
        ("Mechanical Keyboard", 119.50), ("Gaming Mouse", 59.90), ("Smartphone X12", 899.00),
        ("Laptop Pro 14", 1499.00), ("USB-C Hub", 34.99), ("Smartwatch S3", 249.00),
    ],
    "Home & Kitchen": [
        ("Air Fryer", 129.99), ("Coffee Maker", 89.00), ("Chef Knife Set", 74.50),
        ("Blender Max", 99.99), ("Non-stick Pan", 39.90), ("Robot Vacuum", 349.00),
        ("Electric Kettle", 29.99),
    ],
    "Clothing": [
        ("Denim Jacket", 69.00), ("Running Shoes", 110.00), ("Cotton T-Shirt", 15.99),
        ("Wool Sweater", 55.00), ("Rain Coat", 85.00), ("Yoga Pants", 42.00),
    ],
    "Books": [
        ("Data Engineering Handbook", 45.00), ("SQL Performance Explained", 39.00),
        ("Python Cookbook", 49.99), ("Mystery Novel", 12.99), ("Cooking Basics", 24.50),
        ("Kids Picture Book", 9.99),
    ],
    "Sports & Outdoors": [
        ("Camping Tent", 199.00), ("Yoga Mat", 25.00), ("Mountain Bike", 649.00),
        ("Dumbbell Set", 89.99), ("Hiking Backpack", 79.00), ("Tennis Racket", 119.00),
    ],
    "Beauty": [
        ("Face Serum", 32.00), ("Hair Dryer", 59.99), ("Perfume Classic", 95.00),
        ("Sunscreen SPF50", 18.50), ("Makeup Kit", 64.00),
    ],
    "Toys": [
        ("Building Blocks", 39.99), ("Remote Control Car", 54.99), ("Board Game Night", 29.99),
        ("Puzzle 1000pc", 19.99), ("Plush Bear", 14.99),
    ],
    "Groceries": [
        ("Organic Coffee Beans", 16.99), ("Ceylon Tea Box", 8.99), ("Olive Oil 1L", 13.49),
        ("Dark Chocolate", 4.99), ("Almond Butter", 9.49),
    ],
}

PAYMENT_METHODS = {
    "Credit Card": ["Credit Card", "credit card", "CREDIT_CARD", "credit-card", "CC"],
    "Debit Card": ["Debit Card", "debit card", "DEBIT_CARD"],
    "PayPal": ["PayPal", "paypal", "PAYPAL", "Pay Pal"],
    "Cash on Delivery": ["Cash on Delivery", "COD", "cash on delivery"],
    "Bank Transfer": ["Bank Transfer", "bank transfer", "BANK_TRANSFER", "wire"],
}
PAYMENT_WEIGHTS = [45, 20, 18, 10, 7]

DATE_FORMATS_DIRTY = ["%Y/%m/%d", "%d/%m/%Y", "%b %d, %Y", "%d-%b-%Y", "%Y-%m-%dT%H:%M:%S"]

COLUMNS = [
    "transaction_id", "order_date", "customer_id", "customer_name", "customer_email",
    "country", "product_id", "product_name", "category", "quantity", "unit_price",
    "discount", "payment_method", "rating",
]


def build_products(rng: random.Random) -> list[dict]:
    products, n = [], 1
    for category, items in CATALOG.items():
        for name, price in items:
            products.append(
                {"product_id": f"PRD-{n:04d}", "product_name": name,
                 "category": category, "base_price": price}
            )
            n += 1
    return products


def build_customers(rng: random.Random, count: int) -> list[dict]:
    customers = []
    countries = list(COUNTRIES)
    for i in range(1, count + 1):
        first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
        domain = rng.choice(["gmail.com", "yahoo.com", "outlook.com", "mail.com", "example.org"])
        customers.append({
            "customer_id": f"CUST-{i:05d}",
            "customer_name": f"{first} {last}",
            "customer_email": f"{first}.{last}{rng.randint(1, 999)}@{domain}".lower(),
            "country": rng.choices(countries, weights=COUNTRY_WEIGHTS)[0],
        })
    return customers


def random_order_date(rng: random.Random, start: date, end: date) -> date:
    span = (end - start).days
    while True:
        d = start + timedelta(days=rng.randint(0, span))
        weight = 1.0 + (0.6 if d.month in (11, 12) else 0.0) + 0.25 * (d.year - start.year)
        if rng.random() < weight / 2.2:
            return d


def dirty_case(rng: random.Random, value: str) -> str:
    roll = rng.random()
    if roll < 0.08:
        return value.upper()
    if roll < 0.16:
        return value.lower()
    if roll < 0.22:
        return f"  {value} "
    return value


def make_row(rng: random.Random, txn_no: int, customers, products, start, end) -> dict:
    cust = rng.choice(customers)
    prod = rng.choice(products)
    order_date = random_order_date(rng, start, end)
    unit_price = round(prod["base_price"] * rng.uniform(0.92, 1.08), 2)
    rating = rng.choices([1, 2, 3, 4, 5], weights=[4, 6, 15, 35, 40])[0]

    row = {
        "transaction_id": f"TXN-{txn_no:07d}",
        "order_date": order_date.isoformat(),
        "customer_id": cust["customer_id"],
        "customer_name": dirty_case(rng, cust["customer_name"]),
        "customer_email": cust["customer_email"],
        "country": rng.choice(COUNTRIES[cust["country"]]),
        "product_id": prod["product_id"],
        "product_name": prod["product_name"],
        "category": dirty_case(rng, prod["category"]),
        "quantity": str(rng.choices([1, 2, 3, 4, 5], weights=[55, 25, 10, 6, 4])[0]),
        "unit_price": f"{unit_price:.2f}",
        "discount": rng.choices(["0", "0.05", "0.1", "0.15", "0.2"], weights=[60, 15, 12, 8, 5])[0],
        "payment_method": rng.choices(
            [rng.choice(v) for v in PAYMENT_METHODS.values()], weights=PAYMENT_WEIGHTS)[0],
        "rating": str(rating) if rng.random() > 0.30 else "",
    }
    inject_defects(rng, row, order_date)
    return row


def inject_defects(rng: random.Random, row: dict, order_date: date) -> None:
    r = rng.random

    if r() < 0.30:
        row["order_date"] = order_date.strftime(rng.choice(DATE_FORMATS_DIRTY))
    if r() < 0.004:
        row["order_date"] = rng.choice(["", "N/A", "not a date", "2024-13-45", "31/02/2024"])
    elif r() < 0.004:
        row["order_date"] = (date.today() + timedelta(days=rng.randint(30, 400))).isoformat()

    price = float(row["unit_price"])
    if r() < 0.10:
        row["unit_price"] = f"${price:,.2f}"
    elif r() < 0.05:
        row["unit_price"] = f"{price:,.2f} USD"
    if r() < 0.006:
        row["unit_price"] = rng.choice(["", "N/A", "free", f"-{price:.2f}", "0"])
    if r() < 0.05:
        row["quantity"] = f"{row['quantity']}.0"
    if r() < 0.008:
        row["quantity"] = rng.choice(["", "0", "-1", "-3", "two"])
    if r() < 0.03:
        row["discount"] = ""
    elif r() < 0.03:
        row["discount"] = f"{int(float(row['discount']) * 100)}%"
    elif r() < 0.003:
        row["discount"] = rng.choice(["1.5", "-0.1"])
    if row["rating"] and r() < 0.05:
        row["rating"] = f"{row['rating']}.0"
    if r() < 0.005:
        row["rating"] = rng.choice(["0", "6", "10", "-1", "five"])

    if r() < 0.025:
        row["customer_name"] = rng.choice(["", "NULL", "  "])
    if r() < 0.02:
        row["customer_email"] = ""
    elif r() < 0.012:
        row["customer_email"] = rng.choice([
            row["customer_email"].replace("@", " at "),
            row["customer_email"].split("@")[0] + "@",
            row["customer_email"].upper(),
        ])
    if r() < 0.02:
        row["country"] = rng.choice(["", "n/a", "Unknown"])
    if r() < 0.015:
        row["category"] = ""
    if r() < 0.01:
        row["product_name"] = ""
    if r() < 0.01:
        row["payment_method"] = ""

    if r() < 0.002:
        row["customer_id"] = ""
    if r() < 0.002:
        row["product_id"] = ""


def reformat_duplicate(rng: random.Random, row: dict) -> dict:
    dup = dict(row)
    dup["customer_name"] = dirty_case(rng, dup["customer_name"].strip()) if dup["customer_name"].strip() else dup["customer_name"]
    dup["category"] = dup["category"].strip().upper() if dup["category"] else dup["category"]
    dup["country"] = f" {dup['country']} " if dup["country"] else dup["country"]
    return dup


def generate(rows: int, seed: int, output: Path, customers_count: int = 1500) -> Path:
    rng = random.Random(seed)
    products = build_products(rng)
    customers = build_customers(rng, customers_count)
    start, end = date(2023, 1, 1), date(2025, 12, 31)

    base_count = int(rows / 1.055)
    data = [make_row(rng, i, customers, products, start, end) for i in range(1, base_count + 1)]

    extra = rows - base_count
    exact = int(extra * 0.73)
    data += [dict(rng.choice(data)) for _ in range(exact)]
    data += [reformat_duplicate(rng, rng.choice(data)) for _ in range(extra - exact)]

    for row in rng.sample(data, 8):
        row["transaction_id"] = ""

    rng.shuffle(data)

    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(data)
    return output


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate a dirty sales dataset")
    parser.add_argument("--rows", type=int, default=12000, help="total rows to generate (default 12000)")
    parser.add_argument("--seed", type=int, default=42, help="random seed for reproducibility")
    parser.add_argument("--output", type=Path, default=Path("data/raw/sales_raw.csv"))
    args = parser.parse_args()

    path = generate(args.rows, args.seed, args.output)
    print(f"Generated {args.rows:,} rows -> {path}")


if __name__ == "__main__":
    main()
