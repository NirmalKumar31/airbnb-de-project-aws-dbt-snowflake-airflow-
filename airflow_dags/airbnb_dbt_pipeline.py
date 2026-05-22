from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# ── Paths ─────────────────────────────────────────────────────────────────────
DBT_PROJECT_DIR = "/Users/nirmalkumar/Downloads/airbnb-de-project/airbnb_de_project_dbt"
DBT_EXECUTABLE  = "/Users/nirmalkumar/Downloads/airbnb-de-project/.venv/bin/dbt"
DBT_PROFILES    = "/Users/nirmalkumar/.dbt"

DBT_CMD = f"{DBT_EXECUTABLE} --no-use-colors"

# ── Default args ──────────────────────────────────────────────────────────────
default_args = {
    "owner": "nirmalkumar",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

# ── DAG ───────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="airbnb_dbt_pipeline",
    description="Bronze → Silver → Gold with test gates",
    schedule="@hourly",
    start_date=datetime(2026, 5, 21),
    catchup=False,
    default_args=default_args,
    tags=["airbnb", "dbt"],
) as dag:

    # ── Bronze ────────────────────────────────────────────────────────────────
    bronze_run = BashOperator(
        task_id="bronze_run",
        bash_command=f'{DBT_CMD} run --profiles-dir {DBT_PROFILES} --project-dir {DBT_PROJECT_DIR} --select "bronze.*"',
    )

    bronze_test = BashOperator(
        task_id="bronze_test",
        bash_command=f'{DBT_CMD} test --profiles-dir {DBT_PROFILES} --project-dir {DBT_PROJECT_DIR} --select "bronze.*"',
    )

    # ── Silver ────────────────────────────────────────────────────────────────
    silver_run = BashOperator(
        task_id="silver_run",
        bash_command=f'{DBT_CMD} run --profiles-dir {DBT_PROFILES} --project-dir {DBT_PROJECT_DIR} --select "silver.*"',
    )

    silver_test = BashOperator(
        task_id="silver_test",
        # silver.* runs all schema.yml tests on Silver models
        # assert_silver_hosts_no_duplicates and assert_silver_listings_no_duplicates
        # are singular tests that gate the snapshot — if duplicates exist on host_id
        # or listing_id, this task fails and snapshot never runs
        bash_command=f'{DBT_CMD} test --profiles-dir {DBT_PROFILES} --project-dir {DBT_PROJECT_DIR} --select "silver.* assert_silver_hosts_no_duplicates assert_silver_listings_no_duplicates"',
    )

    # ── Snapshot ──────────────────────────────────────────────────────────────
    # Only runs if silver_test passes — guaranteed no duplicates on host_id or listing_id
    snapshot = BashOperator(
        task_id="snapshot",
        bash_command=f"{DBT_CMD} snapshot --profiles-dir {DBT_PROFILES} --project-dir {DBT_PROJECT_DIR}",
    )

    # ── Gold ──────────────────────────────────────────────────────────────────
    gold_run = BashOperator(
        task_id="gold_run",
        bash_command=f'{DBT_CMD} run --profiles-dir {DBT_PROFILES} --project-dir {DBT_PROJECT_DIR} --select "gold.*"',
    )

    gold_test = BashOperator(
        task_id="gold_test",
        # gold.* runs all schema.yml tests on Gold models
        # also picks up assert_revenue_positive and assert_booking_dates_valid
        # assert_ratings_in_range references silver_reviews so also runs here
        bash_command=f'{DBT_CMD} test --profiles-dir {DBT_PROFILES} --project-dir {DBT_PROJECT_DIR} --select "gold.* assert_revenue_positive assert_booking_dates_valid assert_ratings_in_range"',
    )

    # ── Dependency chain ──────────────────────────────────────────────────────
    bronze_run >> bronze_test >> silver_run >> silver_test >> snapshot >> gold_run >> gold_test