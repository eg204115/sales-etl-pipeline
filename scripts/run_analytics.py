from __future__ import annotations

import argparse
import re
import sys
import time
from pathlib import Path

import pandas as pd
import psycopg2

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from etl.config import PipelineConfig  # noqa: E402


def load_queries(path: Path) -> dict[str, str]:
    parts = re.split(r"^-- name:\s*(\w+)\s*$", path.read_text(encoding="utf-8"), flags=re.M)
    return {name: sql.strip().rstrip(";") for name, sql in zip(parts[1::2], parts[2::2])}


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the analytical queries")
    parser.add_argument("--query", help="run only this query name")
    parser.add_argument("--explain", action="store_true", help="print EXPLAIN ANALYZE for each query")
    args = parser.parse_args()

    cfg = PipelineConfig()
    queries = load_queries(cfg.sql_dir / "03_analytical_queries.sql")
    if args.query:
        queries = {args.query: queries[args.query]}

    pd.set_option("display.width", 200)
    pd.set_option("display.max_columns", 20)
    pd.set_option("display.max_rows", 60)

    with psycopg2.connect(**cfg.db.connect_kwargs()) as conn, conn.cursor() as cur:
        for name, sql in queries.items():
            started = time.perf_counter()
            cur.execute(sql)
            rows = cur.fetchall()
            elapsed = (time.perf_counter() - started) * 1000
            df = pd.DataFrame(rows, columns=[d[0] for d in cur.description])

            print(f"\n{'=' * 90}\n{name}  ({len(df)} rows, {elapsed:.1f} ms)\n{'=' * 90}")
            print(df.to_string(index=False) if not df.empty else "(no rows)")

            if args.explain:
                cur.execute(f"EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) {sql}")
                print("\n-- plan --")
                print("\n".join(r[0] for r in cur.fetchall()))


if __name__ == "__main__":
    main()
