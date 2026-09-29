CREATE TABLE IF NOT EXISTS customers (
    customer_id     VARCHAR(20)  PRIMARY KEY,
    customer_name   VARCHAR(150) NOT NULL,
    customer_email  VARCHAR(255),
    country         VARCHAR(100) NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT chk_customer_id_format CHECK (customer_id ~ '^CUST-[0-9]{5}$'),
    CONSTRAINT chk_customer_email     CHECK (customer_email IS NULL
                                             OR customer_email ~* '^[^@\s]+@[^@\s]+\.[^@\s]+$')
);

CREATE TABLE IF NOT EXISTS products (
    product_id      VARCHAR(20)  PRIMARY KEY,
    product_name    VARCHAR(200) NOT NULL,
    category        VARCHAR(100) NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT chk_product_id_format CHECK (product_id ~ '^PRD-[0-9]{4}$')
);

CREATE TABLE IF NOT EXISTS etl_runs (
    run_id              UUID         PRIMARY KEY,
    started_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),
    finished_at         TIMESTAMPTZ,
    status              VARCHAR(10)  NOT NULL DEFAULT 'running',
    source_file         TEXT         NOT NULL,
    rows_extracted      INTEGER,
    rows_loaded         INTEGER,
    rows_rejected       INTEGER,
    exact_duplicates    INTEGER,
    s3_raw_uri          TEXT,
    s3_processed_uri    TEXT,
    error_message       TEXT,
    CONSTRAINT chk_etl_status CHECK (status IN ('running', 'success', 'failed'))
);

CREATE TABLE IF NOT EXISTS etl_rejected_records (
    rejected_id         BIGSERIAL    PRIMARY KEY,
    run_id              UUID         NOT NULL REFERENCES etl_runs (run_id) ON DELETE CASCADE,
    source_row_number   INTEGER      NOT NULL,
    transaction_id      VARCHAR(50),
    reject_reasons      TEXT         NOT NULL,
    raw_record          JSONB        NOT NULL,
    rejected_at         TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sales (
    transaction_id  VARCHAR(20)   PRIMARY KEY,
    order_date      DATE          NOT NULL,
    customer_id     VARCHAR(20)   NOT NULL REFERENCES customers (customer_id),
    product_id      VARCHAR(20)   NOT NULL REFERENCES products (product_id),
    quantity        INTEGER       NOT NULL,
    unit_price      NUMERIC(10,2) NOT NULL,
    discount        NUMERIC(4,3)  NOT NULL DEFAULT 0,
    total_amount    NUMERIC(12,2) GENERATED ALWAYS AS
                        (ROUND(quantity * unit_price * (1 - discount), 2)) STORED,
    payment_method  VARCHAR(30)   NOT NULL,
    rating          SMALLINT,
    etl_run_id      UUID          NOT NULL REFERENCES etl_runs (run_id),
    loaded_at       TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CONSTRAINT chk_txn_id_format   CHECK (transaction_id ~ '^TXN-[0-9]{7}$'),
    CONSTRAINT chk_quantity        CHECK (quantity > 0),
    CONSTRAINT chk_unit_price      CHECK (unit_price > 0),
    CONSTRAINT chk_discount        CHECK (discount >= 0 AND discount < 1),
    CONSTRAINT chk_rating          CHECK (rating IS NULL OR rating BETWEEN 1 AND 5),
    CONSTRAINT chk_order_date      CHECK (order_date >= DATE '2015-01-01'),
    CONSTRAINT chk_payment_method  CHECK (payment_method IN
        ('Credit Card', 'Debit Card', 'PayPal', 'Cash on Delivery',
         'Bank Transfer', 'Other', 'Unknown'))
);

COMMENT ON TABLE  sales              IS 'One row per sales transaction (grain: transaction_id).';
COMMENT ON COLUMN sales.total_amount IS 'quantity * unit_price * (1 - discount), rounded to cents.';
COMMENT ON COLUMN sales.rating       IS 'Customer rating 1-5; NULL when the customer did not rate.';
COMMENT ON TABLE  etl_rejected_records IS 'Rows that failed validation, stored as the original raw JSON.';
