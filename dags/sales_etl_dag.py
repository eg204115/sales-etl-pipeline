from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "email_on_failure": True,
    "email": ["data-alerts@example.com"],
    "execution_timeout": timedelta(hours=1),
}

with DAG(
    dag_id="sales_etl_daily",
    description="Raw sales export -> S3 -> clean/validate -> PostgreSQL",
    schedule="0 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["sales", "etl"],
) as dag:

    wait_for_export = S3KeySensor(
        task_id="wait_for_raw_export",
        bucket_name="{{ var.value.sales_bucket }}",
        bucket_key="sales-etl/incoming/{{ ds }}/sales_raw.csv",
        poke_interval=300,
        timeout=60 * 60 * 3,
        mode="reschedule",
    )

    run_etl = BashOperator(
        task_id="run_etl",
        bash_command=(
            "cd /opt/sales-etl-pipeline && "
            "python run_pipeline.py --source s3 --s3-key sales-etl/incoming/{{ ds }}/sales_raw.csv"
        ),
    )

    refresh_stats = BashOperator(
        task_id="analyze_tables",
        bash_command='psql "$SALES_DW_URL" -c "ANALYZE sales; ANALYZE customers; ANALYZE products;"',
    )

    wait_for_export >> run_etl >> refresh_stats
