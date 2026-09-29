from __future__ import annotations

import argparse
import statistics
import sys
from pathlib import Path

import psycopg2

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from etl.config import PipelineConfig  # noqa: E402

SCALED_INDEXES = [
    "CREATE INDEX sales_scaled_order_date_idx ON sales_scaled (order_date) INCLUDE (total_amount, product_id)",
    "CREATE INDEX sales_scaled_customer_date_idx ON sales_scaled (customer_id, order_date DESC)",
    "CREATE INDEX sales_scaled_product_date_idx ON sales_scaled (product_id, order_date) INCLUDE (total_amount)",
]

QUERIES = {
    "Customer order history (1 customer)": """
        SELECT transaction_id, order_date, total_amount
        FROM {t} WHERE customer_id = 'CUST-00042' ORDER BY order_date DESC""",
    "Revenue for one week": """
        SELECT SUM(total_amount) FROM {t}
        WHERE order_date BETWEEN DATE '2025-06-01' AND DATE '2025-06-07'""",
    "Daily revenue, last 30 days": """
        SELECT order_date, SUM(total_amount) FROM {t}
        WHERE order_date > DATE '2025-12-01' GROUP BY order_date ORDER BY order_date""",
    "Sales of one product in 2025": """
        SELECT COUNT(*), SUM(total_amount) FROM {t}
        WHERE product_id = 'PRD-0007' AND order_date >= DATE '2025-01-01'""",
    "Top categories by revenue (full scan)": """
        SELECT p.category, SUM(s.total_amount) AS revenue
        FROM {t} s JOIN products p ON p.product_id = s.product_id
        GROUP BY p.category ORDER BY revenue DESC LIMIT 10""",
}


def build_scaled_table(conn, copies: int) -> None:
    print(f"Building sales_scaled with {copies} copies of `sales` ...", flush=True)
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS sales_scaled")
        cur.execute("""
            CREATE TABLE sales_scaled AS
            SELECT s.transaction_id || '-' || k AS transaction_id,
                   s.order_date, s.customer_id, s.product_id, s.quantity,
                   s.unit_price, s.discount, s.total_amount, s.payment_method, s.rating
            FROM   sales s CROSS JOIN generate_series(1, %s) AS k""", (copies,))
        cur.execute("ALTER TABLE sales_scaled ADD PRIMARY KEY (transaction_id)")
        for ddl in SCALED_INDEXES:
            cur.execute(ddl)
        cur.execute("SELECT COUNT(*) FROM sales_scaled")
        print(f"sales_scaled rows: {cur.fetchone()[0]:,}")
    conn.commit()


def secondary_indexes(cur, table: str) -> list[str]:
    cur.execute("""
        SELECT i.relname FROM pg_index x
        JOIN pg_class i ON i.oid = x.indexrelid
        JOIN pg_class t ON t.oid = x.indrelid
        WHERE t.relname = %s AND NOT x.indisprimary""", (table,))
    return [r[0] for r in cur.fetchall()]


def scan_nodes(plan: dict) -> list[str]:
    nodes = []
    if "Scan" in plan["Node Type"]:
        nodes.append(plan["Node Type"])
    for child in plan.get("Plans", []):
        nodes.extend(scan_nodes(child))
    return nodes


def measure(cur, sql: str, runs: int) -> tuple[float, str]:
    times, nodes = [], []
    for _ in range(runs):
        cur.execute(f"EXPLAIN (ANALYZE, FORMAT JSON) {sql}")
        plan = cur.fetchone()[0][0]
        times.append(plan["Execution Time"])
        nodes = scan_nodes(plan["Plan"])
    return statistics.median(times), ", ".join(dict.fromkeys(nodes))


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare query performance with and without indexes")
    parser.add_argument("--scale", type=int, default=0,
                        help="benchmark a copy of sales with N copies of every row")
    parser.add_argument("--runs", type=int, default=5, help="repetitions per query (median is reported)")
    args = parser.parse_args()

    cfg = PipelineConfig()
    conn = psycopg2.connect(**cfg.db.connect_kwargs())
    table = "sales"
    if args.scale:
        build_scaled_table(conn, args.scale)
        table = "sales_scaled"

    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f"VACUUM ANALYZE {table}")
    conn.autocommit = False

    results = []
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        print(f"\nBenchmarking on `{table}` ({cur.fetchone()[0]:,} rows), median of {args.runs} runs\n")
        indexes = secondary_indexes(cur, table)

        for name, template in QUERIES.items():
            sql = template.format(t=table)
            with_ms, with_plan = measure(cur, sql, args.runs)

            for idx in indexes:
                cur.execute(f'DROP INDEX "{idx}"')
            without_ms, without_plan = measure(cur, sql, args.runs)
            conn.rollback()

            results.append((name, with_ms, with_plan, without_ms, without_plan))

    conn.close()

    header = f"{'Query':<40} {'with idx':>10} {'without':>10} {'speed-up':>9}   plan with index  ->  plan without"
    print(header)
    print("-" * len(header) + "-" * 20)
    for name, w, wp, wo, wop in results:
        print(f"{name:<40} {w:>8.2f}ms {wo:>8.2f}ms {wo / w:>8.1f}x   {wp}  ->  {wop}")
    print("\nNote: a full-table aggregation gains little from indexes - the planner correctly "
          "prefers a sequential scan when it has to read (almost) every row anyway.")


if __name__ == "__main__":
    main()
