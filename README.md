# Sales ETL Pipeline

A Python ETL pipeline that takes a messy sales-transactions export and turns it into
a clean, validated, indexed PostgreSQL star schema. The raw data and outputs are
backed up to AWS S3.

```
             ┌──────────────┐   upload raw (landing zone)   ┌──────────────────────┐
 raw CSV ───►│   Extract    │──────────────────────────────►│  S3  raw/            │
 (12k rows,  └──────┬───────┘                               │      processed/      │
  dirty)            ▼                                       │      rejected/       │
             ┌──────────────┐  clean ▸ standardise ▸ impute └──────────▲───────────┘
             │  Transform   │  ▸ validate ▸ de-duplicate                │ backup outputs
             └──────┬───────┘                                           │
          clean rows│         rejected rows + reasons ──────────────────┤
                    ▼                                                   │
             ┌──────────────┐  COPY → staging → UPSERT (1 transaction)  │
             │     Load     │──────────────────────────────►  PostgreSQL│
             └──────────────┘   customers · products · sales            │
                                etl_runs · etl_rejected_records ────────┘
```

**Domain:** e-commerce sales transactions: 12,000 raw rows, 14 columns, covering 2023–2025.

---

## Table of contents
1. [Architecture](#architecture)
2. [Project structure](#project-structure)
3. [Quick start](#quick-start)
4. [The dataset](#the-dataset)
5. [ETL pipeline](#etl-pipeline)
6. [Database design](#database-design)
7. [Analytical queries](#analytical-queries)
8. [Indexing & performance](#indexing--performance)
9. [AWS S3 integration](#aws-s3-integration)
10. [Scalability & architecture](#scalability--architecture)
11. [Testing](#testing)

---

## Architecture

This is a **batch ETL pipeline organised in medallion-style layers**:

| Layer | Contents | Location |
|---|---|---|
| **Bronze** (raw) | The source CSV exactly as received, stored before any processing | `s3://…/raw/ingest_date=…/run_id=…/` |
| **Silver** (cleaned) | Standardised, validated, de-duplicated rows, plus the rejected rows with their reasons | `s3://…/processed/`, `s3://…/rejected/` |
| **Gold** (business-ready) | Star schema with constraints and indexes, used by the analytical queries | PostgreSQL `sales_dw` |

**Why this design**
- **The source is a batch file export**, so a scheduled batch run (cron or Airflow) matches how the data arrives.
- **Any run can be replayed.** The untouched raw file of every run is kept in S3. If a cleaning rule changes,
  that run can be reprocessed from Bronze (`--source s3 --s3-key …`) without asking the source system to resend anything.
- **Each layer has one job:** Bronze keeps the data exactly as received, Silver makes it correct, and Gold makes it fast to query.
  That's why cleaning happens in Python while constraints, indexes and the star schema live in PostgreSQL.
- **Every row is accounted for.** The rejected rows are stored next to Silver, and every run is recorded in `etl_runs`.

**Why not Lambda or Kappa?** Both are designed for streaming data. Lambda runs a real-time layer next to the batch
layer; Kappa treats all data as an event stream (Kafka and a stream processor). The source here is a daily file,
and nobody needs second-level freshness, so a streaming layer would add infrastructure and a second code path
without any benefit. If real-time dashboards were needed later, a stream (Kinesis or Kafka) feeding the Bronze
layer would be the next step.

---

## Project structure

```
sales-etl-pipeline/
├── run_pipeline.py              # entry point: python run_pipeline.py
├── etl/
│   ├── config.py                # all settings from environment variables / .env
│   ├── logger.py                # console + per-run log file
│   ├── extract.py               # read raw CSV as text, schema check, row numbers
│   ├── transform.py             # cleaning, standardisation, validation, de-duplication
│   ├── load.py                  # COPY + UPSERT into PostgreSQL, run audit
│   └── s3_storage.py            # S3 upload/download (boto3)
├── sql/
│   ├── 00_create_database.sql   # one-time DB + least-privilege role setup
│   ├── 01_schema.sql            # tables, PKs, FKs, CHECK constraints
│   ├── 02_indexes.sql           # indexes (each one justified)
│   ├── 03_analytical_queries.sql# business queries
│   └── 04_index_performance_demo.sql  # EXPLAIN ANALYZE with / without indexes
├── scripts/
│   ├── generate_dataset.py      # builds the dirty raw dataset
│   ├── run_analytics.py         # runs & prints the analytical queries
│   └── benchmark_indexes.py     # measures index impact (also at 1M+ rows)
├── aws/iam_policy.json          # least-privilege IAM policy for the pipeline user
├── dags/sales_etl_dag.py        # Airflow DAG (scheduling concept)
├── tests/test_transform.py      # unit tests for the transform logic
├── data/raw/sales_raw.csv       # the raw dataset (generated, committed for reviewers)
├── data/processed/  data/rejected/  logs/   # pipeline outputs (git-ignored)
├── docker-compose.yml           # optional local PostgreSQL
├── requirements.txt
└── .env.example
```

---

## Quick start

**Prerequisites:** Python 3.10+ and PostgreSQL 13+ (local install or `docker compose`).
An AWS account is optional; the pipeline runs without S3 when `S3_BUCKET` is empty.

```bash
# 1. Python environment
python -m venv .venv
.venv\Scripts\activate            # Windows   (Linux/macOS: source .venv/bin/activate)
pip install -r requirements.txt

# 2. Configuration
cp .env.example .env              # Windows PowerShell: Copy-Item .env.example .env
                                  # then fill in DB_PASSWORD (+ S3/AWS values if used)

# 3. Database (pick one)
psql -U postgres -v etl_password=<same as DB_PASSWORD> -f sql/00_create_database.sql  # a) local install
docker compose up -d                                                                  # b) or Docker

# 4. Run the pipeline
python run_pipeline.py            # generates data/raw/sales_raw.csv if it is missing

# 5. Look at the results
python scripts/run_analytics.py
python scripts/benchmark_indexes.py --scale 100
```

### Command-line options

| Command | What it does |
|---|---|
| `python run_pipeline.py` | Full run: raw → S3, clean, load to PostgreSQL, outputs → S3 |
| `python run_pipeline.py --skip-s3` | Same but without AWS |
| `python run_pipeline.py --skip-db` | Transform only, write the CSV outputs |
| `python run_pipeline.py --generate --rows 50000` | Regenerate the raw dataset first |
| `python run_pipeline.py --source s3 --s3-key <key>` | Read the raw file from S3 instead of disk |
| `python scripts/generate_dataset.py --rows 12000 --seed 42` | Only build the dataset |

The process exits with code `0` on success and `1` on failure, so cron and Airflow can detect failed runs.

---

## The dataset

`scripts/generate_dataset.py` creates a reproducible (seeded) export of **12,000 rows** of
sales transactions with 14 columns and mixed types:

| Column | Type | Example |
|---|---|---|
| transaction_id | string | `TXN-0003937` |
| order_date | date (as text) | `2024-11-29`, `29/11/2024`, `Nov 29, 2024` |
| customer_id, customer_name, customer_email, country | string | `CUST-00931`, `Richard Schmidt`, … |
| product_id, product_name, category | string | `PRD-0018`, `Running Shoes`, `Clothing` |
| quantity | integer (as text) | `2`, `2.0` |
| unit_price | decimal (as text) | `114.59`, `$1,299.00`, `1,299.00 USD` |
| discount | decimal (as text) | `0.15`, `15%` |
| payment_method | string | `Credit Card`, `CREDIT_CARD`, `cc` |
| rating | integer 1–5, optional | `5`, `4.0`, empty |

**Defects that were injected on purpose:**

- **Mixed date formats** (6 formats), plus unparseable dates (`2024-13-45`, `31/02/2024`) and future dates
- **Inconsistent casing and whitespace** (`ELECTRONICS`, `  Sports & Outdoors `), and
  **country aliases** (`USA`, `U.S.A.`, `us`, `Srilanka`, `Deutschland`)
- **Numbers stored as text** with currency symbols, thousands separators, `%` signs and `.0` suffixes
- **Missing values** in both optional fields (rating, discount, email) and mandatory ones (IDs, price, quantity)
- **Out-of-range values**: quantity ≤ 0, rating 0/6/10, discount 1.5 or -0.1, prices like `free`
- **Malformed emails** (`name at mail.com`, `name@`)
- **Duplicates**: exact copies, and "re-exported" copies whose formatting differs but which
  describe the same business record

---

## ETL pipeline

### Extract (`etl/extract.py`)
- Reads every column **as text**, with pandas' automatic NA detection turned off. The
  transform step, not the CSV parser, decides what "missing" means.
- Checks that the expected columns are present and fails fast if not.
- Records each row's original line number (`source_row_number`) so any rejected row can be traced back to the file.

### Transform (`etl/transform.py`)
All steps are vectorised pandas operations, with no row-by-row Python loops.

| Stage | What happens |
|---|---|
| **Clean** | Trim and collapse whitespace; turn `""`, `N/A`, `null`, `none`, `-`, `unknown`, … into NULL |
| **Standardise** | Dates: try 6 known formats, convert to `DATE`. Money: strip `$`, `,`, `USD`. Discount: `15%` → `0.15`. Names: Title Case. Emails: lower case. Countries, categories and payment methods: map to a controlled list of values |
| **Impute** | Missing discount → `0` (business rule: no discount recorded means no discount). Missing country, name or email is filled from **the same customer's other orders**, and missing category or product name from the same product's other rows (master-data consolidation). Anything still missing gets an explicit `Unknown` value. A missing rating stays NULL because the customer didn't rate |
| **Validate** | 11 rules, including mandatory IDs, ID format, parseable date, date not in the future and not before 2015, quantity a positive whole number, price > 0, discount in [0, 1), rating a whole number from 1 to 5. **A row collects every reason it fails**, not just the first |
| **De-duplicate** | Runs only on *valid* rows, so an invalid copy can never win over a valid one. (1) Exact duplicates on the business columns, which also catches re-formatted copies because it runs *after* standardisation. (2) A remaining repeated `transaction_id` with different content is rejected as `duplicate_transaction_id_conflict` |

A bad email is set to NULL rather than rejecting the whole sale, because the transaction itself is still valid.

**Reconciliation:** `rows_extracted == rows_clean + rows_rejected` is asserted on every run,
so no row disappears without a trace. Even duplicates are logged as rejected rows.

### Rejected-record logging
Rejected rows are written in two places:
- `data/rejected/sales_rejected.csv`: the original raw values plus a `reject_reasons` column
- the `etl_rejected_records` table: the raw row as `JSONB`, the reasons, and the run ID

Result on the sample dataset:

```
Extracted 12,000 → 10,957 clean, 1,043 rejected (8.7%)
  exact_duplicate          596     invalid_order_date      48
  invalid_quantity         101     invalid_discount        37
  invalid_unit_price        77     missing_customer_id     28
  invalid_rating            72     missing_product_id      22
  future_order_date         58     missing_transaction_id   8
Fixed: 380 missing discounts → 0, 84 invalid emails nulled,
       243 countries / 326 names / 166 categories filled from the same customer or product
```

### Load (`etl/load.py`)
1. Applies `01_schema.sql` and `02_indexes.sql`. Both are idempotent (`IF NOT EXISTS`).
2. Inserts a row into `etl_runs` with status `running`.
3. In **one transaction**:
   - `COPY` streams the customers, products and sales into `TEMP` staging tables. This is much faster than row-by-row inserts.
   - `INSERT … ON CONFLICT DO UPDATE … WHERE … IS DISTINCT FROM` merges them into the real
     tables. Only rows whose values actually changed are rewritten.
   - The rejected rows go into `etl_rejected_records`.
4. Updates `etl_runs` to `success` or `failed`, with row counts, S3 URIs and the error message.

**Idempotent:** running the pipeline twice on the same file gives
`0 new / 0 updated / 10,957 unchanged` with no duplicates and no needless rewrites.

**Data-quality circuit breaker:** if more than `MAX_REJECT_RATIO` (default 20%) of the rows are rejected, the
run stops **before loading** and is marked `failed`. A broken source export
should never overwrite good data.

---

## Database design

A small **star schema**: one fact table and two dimensions. It also has two audit tables for the ETL itself.

```
 customers                      sales (fact)                       products
 ───────────                    ─────────────────────              ───────────
 customer_id  PK  ◄──────────── customer_id   FK                ┌► product_id  PK
 customer_name                  product_id    FK ───────────────┘  product_name
 customer_email (CHECK regex)   transaction_id PK                  category
 country                        order_date, quantity, unit_price
                                discount, payment_method, rating
                                total_amount  (GENERATED, STORED)
                                etl_run_id    FK ──► etl_runs ◄── etl_rejected_records
```

| Decision | Why |
|---|---|
| Star schema (customers / products / sales) | Customer and product attributes are stored once, not repeated on every sale. Analytical queries stay simple joins |
| Natural business keys as PKs (`TXN-…`, `CUST-…`) | They are stable and unique in the source, which makes UPSERTs and re-runs idempotent without a lookup |
| `CHECK` constraints mirror the Python validation rules | Defence in depth: even a bug in the ETL or a manual insert cannot store invalid data |
| `total_amount` is a **generated stored column** | Revenue is computed once at write time, always consistent, and can be put in an index's `INCLUDE` list |
| `NUMERIC` for money, not `FLOAT` | Exact decimal arithmetic, so no rounding errors on sums |
| `rating` nullable | "Not rated" is a real business state and is different from 0 |
| `etl_runs` + `etl_rejected_records` | Auditability: which run loaded a row, what was rejected and why, and how long each run took |
| Least-privilege DB role (`00_create_database.sql`) | The pipeline user is not a superuser. There is an optional read-only `analyst_ro` role for BI |

---

## Analytical queries

`sql/03_analytical_queries.sql` (run with `python scripts/run_analytics.py`, add `--explain` for plans):

| Query | Technique |
|---|---|
| **Top 10 categories by revenue** with share of total | `JOIN`, `GROUP BY`, window `SUM() OVER ()` |
| **Monthly revenue growth**, month-over-month and year-over-year | CTE, `date_trunc`, `LAG()` window function |
| **Average rating by country**, with % positive ratings | partial-index-friendly `WHERE rating IS NOT NULL`, `FILTER`, `HAVING` minimum sample size |
| **Top customers by lifetime value** (RFM-style) | aggregation over the customer index |
| **Payment mix in the last quarter** | selective date range served by an index |
| **Data-quality summary** of the latest run | `unnest(string_to_array())` over the rejection reasons |

Sample output:

```
top_10_categories_by_revenue
         category  orders  units_sold    revenue  avg_order_value  revenue_share_pct
      Electronics    1969        3498 1264033.95           641.97              52.01
Sports & Outdoors    1367        2463  466777.65           341.46              19.21
   Home & Kitchen    1540        2698  293065.53           190.30              12.06
...
```

---

## Indexing & performance

### Indexes and the reasoning behind them (`sql/02_indexes.sql`)

| Index | Serves | Reasoning |
|---|---|---|
| `sales (order_date) INCLUDE (total_amount, product_id)` | date ranges, monthly revenue | **Covering index**, so revenue per period can be answered with an *Index Only Scan* that never reads the table |
| `sales (customer_id, order_date DESC)` | customer order history, joins | Composite: equality column first, then the sort column, so `ORDER BY` needs no sort step |
| `sales (product_id, order_date) INCLUDE (total_amount)` | product sales in a period, joins | Composite and covering (see the lesson learned below) |
| `sales (customer_id) INCLUDE (rating) WHERE rating IS NOT NULL` | rating analytics | **Partial index**: ~30% of rows have no rating, so the index is smaller and cheaper to maintain |
| `products (category)`, `customers (country)` | dimension filters | Typical slice-and-dice predicates |
| `etl_rejected_records (run_id)` | "what was rejected in run X" | Operational lookup |

PostgreSQL does **not** create indexes on foreign-key columns automatically, which is why the join columns on `sales` are indexed explicitly.

### Measured impact

`scripts/benchmark_indexes.py` runs every query with `EXPLAIN ANALYZE`, first with the indexes
and then after dropping all secondary indexes **inside a transaction that is rolled back**
(PostgreSQL DDL is transactional, so nothing changes permanently). `--scale 100` builds a
**1.1 million-row** copy of the table to show behaviour at production volume.

Measured locally on PostgreSQL 18 with 1,095,700 rows (median of 5 runs):

| Query | With indexes | Without | Speed-up | Plan with index |
|---|---|---|---|---|
| Customer order history (1 customer) | 0.24 ms | 80.7 ms | **329×** | Bitmap Index Scan |
| Revenue for one week | 2.85 ms | 79.0 ms | **28×** | Index Only Scan |
| Sales of one product in 2025 | 3.24 ms | 74.3 ms | **23×** | Index Only Scan |
| Daily revenue, last 30 days | 19.6 ms | 78.9 ms | **4×** | Index Only Scan |
| Top categories by revenue (full scan) | 784 ms | 232 ms | **0.3× (slower)** | Index Scan + Index Only Scan |

Exact timings vary from run to run on a laptop (caching, background load), but the pattern is always the same.

**What this shows**
- Indexes help **selective** queries, which read a small fraction of the rows. Finding one customer's
  orders is the most selective query, so it gains the most (329×). The 30-day query reads more rows, so it gains less (4×).
- A **full-table aggregation** gains nothing, because every row must be read anyway. In this run the
  planner even picked an index-based plan that was **slower** than a plain sequential scan. The planner
  works from cost *estimates*, and they aren't always right. Speeding up this kind of query needs
  pre-aggregation (a materialized view or summary table, see below), not more indexes.
- **Composite index lesson:** a plain index on `product_id` alone took **34 ms** for the "one product in 2025"
  query, because the date filter and `SUM` still had to visit the table. The composite covering index
  `(product_id, order_date) INCLUDE (total_amount)` takes **7 ms**, about **5× faster** again (no index: 242 ms).
  Put the equality column first, then the range column, and include the measured columns.
- Every index slows down writes and uses disk space, so each index here maps to a real query pattern.

For a manual walkthrough in psql: `\i sql/04_index_performance_demo.sql`.

---

## AWS S3 integration

The pipeline uses S3 in all three ways the brief lists:

| When | What | S3 key |
|---|---|---|
| **Before processing** | untouched raw file (landing zone) | `sales-etl/raw/ingest_date=YYYY-MM-DD/run_id=<uuid>/sales_raw.csv` |
| After load | cleaned dataset | `sales-etl/processed/ingest_date=…/run_id=…/sales_clean.csv` |
| After load | rejected records (backup / audit) | `sales-etl/rejected/ingest_date=…/run_id=…/sales_rejected.csv` |

- **Hive-style partitions** (`ingest_date=`) mean Athena, Glue or Spark can query the bucket later without moving any files.
  Each run writes to its own `run_id=` folder, so a run never overwrites another.
- The pipeline can also **read its input from S3**: `--source s3 --s3-key …`
- Uploads use server-side encryption (`AES256`) and carry `run-id` metadata.
- boto3 retries in standard mode (5 attempts, exponential backoff) for throttling and 5xx errors.

### Security: least privilege, no hardcoded secrets
1. Create a bucket (Free Tier), keep **Block Public Access** on.
2. Create an IAM user `sales-etl-pipeline` with **only** the policy in
   [`aws/iam_policy.json`](aws/iam_policy.json) (replace `YOUR-BUCKET-NAME`):
   - `s3:ListBucket` **only for the `sales-etl/` prefix**
   - `s3:GetObject` and `s3:PutObject` **only on `sales-etl/*`**
   - **no** `DeleteObject`, so the pipeline cannot destroy backups
   - an explicit **Deny on unencrypted uploads**
3. Create an access key for that user and put it in `.env` (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`).
   `.env` is git-ignored, and boto3 reads the keys from the environment. The code never
   handles credentials. On EC2, ECS or Airflow you would attach an **IAM role** instead and use no keys at all.

---

## Scalability & architecture

### Scaling to 1 million+ records
The current version handles about 11k rows in about 3 seconds. The indexes were already tested at 1.1M rows. For larger inputs:

| Concern | Now | At 1M – 100M rows |
|---|---|---|
| Memory | whole file in pandas | read in **chunks** (`pd.read_csv(chunksize=100_000)`) and COPY each chunk. The transform is already vectorised, so it works per chunk unchanged. Cross-chunk de-duplication moves into the database (the staging table plus `ON CONFLICT`) |
| File format | CSV | **Parquet** (columnar, compressed, typed): 5–10× smaller and faster to read |
| Loading | COPY → staging → UPSERT | Same pattern, which is already the fastest bulk path in PostgreSQL. For the first full load, drop the indexes, COPY, then recreate them |
| Incremental | full file each run | Load only new data (per `ingest_date` partition or a watermark on `order_date`). The UPSERT already makes late or corrected records safe |
| Compute | single Python process | Beyond ~50M rows, or for many sources: **Spark / AWS Glue** reading the partitioned S3 data |

### Partitioning & indexing evolution
- **Range-partition `sales` by `order_date`** (monthly) once it reaches tens of millions of rows. Queries
  on recent months only touch those partitions (*partition pruning*). Indexes stay small per partition,
  and old data can be detached or archived instantly instead of running a slow `DELETE`.
- **BRIN index on `order_date`**: sales arrive roughly in date order, so a BRIN index is a few KB
  instead of hundreds of MB for a B-tree, at similar speed for range scans.
- **Materialized views** or summary tables for dashboards (monthly revenue by category), refreshed
  after each load. That is the fix for the full-scan queries that indexes can't help.
- Watch `pg_stat_user_indexes` and drop indexes that are never used, because each one costs write speed.
- For truly analytical workloads, move to a columnar warehouse such as **Redshift, Snowflake or BigQuery**, or Athena over the S3 Parquet data.

### Scheduling
- **Simple:** cron, since the exit code tells cron whether the run succeeded:
  ```
  0 2 * * *  cd /opt/sales-etl-pipeline && .venv/bin/python run_pipeline.py >> logs/cron.log 2>&1
  ```
- **Production:** Airflow. See [`dags/sales_etl_dag.py`](dags/sales_etl_dag.py): an
  `S3KeySensor` waits for the day's export, then the ETL runs, then `ANALYZE`. It has retries with
  exponential backoff, failure e-mails, `max_active_runs=1` so two loads never run on the same tables at once,
  and a timeout.

### Failure handling

| Failure | How it is handled |
|---|---|
| Bad rows | Rejected with reasons, logged to CSV and DB, and the run continues |
| Badly broken source file | Circuit breaker (`MAX_REJECT_RATIO`) stops the run before loading |
| Missing or renamed columns | Schema check in extract fails fast with a clear error |
| Crash during load | A single DB transaction rolls back, so the warehouse is never half-loaded |
| Re-running after a failure | Idempotent UPSERT, so it is safe to simply re-run |
| S3 throttling or network errors | boto3 standard retries with exponential backoff |
| Any failure | Logged with a traceback to `logs/pipeline_<run_id>.log`, the run is marked `failed` with the error in `etl_runs`, and the process exits with code 1 so cron or Airflow can alert and retry |
| Something is wrong in the data later | The raw file is kept in S3 per run, so any run can be replayed (`--source s3 --s3-key …`) |

---

## Testing

```bash
pytest -q
```

There are 26 unit tests for the transform logic: every date format, currency parsing, alias mapping,
each rejection rule, multiple reasons per row, both kinds of duplicates, imputation from the same
customer, and the reconciliation invariant. They need no database and no AWS.

### Useful queries after a run
```sql
SELECT * FROM etl_runs ORDER BY started_at DESC LIMIT 5;
SELECT reject_reasons, raw_record FROM etl_rejected_records LIMIT 10;
SELECT COUNT(*) FROM sales;
```
