import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# ── Paths ─────────────────────────────────────────────────────────────────────
DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/airbnb_de_project_dbt")
DBT_EXECUTABLE  = os.environ.get("DBT_EXECUTABLE", "dbt")
DBT_PROFILES    = os.environ.get("DBT_PROFILES_DIR", "/opt/airflow/dbt")
DBT_TARGET      = os.environ.get("DBT_TARGET", "prod")

DBT_CMD = f"{DBT_EXECUTABLE} --no-use-colors --target {DBT_TARGET}"

# ── Default args ──────────────────────────────────────────────────────────────
default_args = {
    "owner": "nirmalkumar",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=45),
    "email_on_failure": False,
}

# ── DAG ───────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="airbnb_dbt_pipeline",
    description="Bronze → Silver → Gold with test gates",
    schedule="0 */6 * * *",
    start_date=datetime(2026, 5, 21),
    catchup=False,
    max_active_runs=1,
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
        bash_command=f'{DBT_CMD} test --profiles-dir {DBT_PROFILES} --project-dir {DBT_PROJECT_DIR} --select "silver.* assert_silver_hosts_no_duplicates assert_silver_listings_no_duplicates assert_booking_host_matches_listing assert_reviews_reference_bookings assert_review_roles_valid assert_guest_phone_e164"',
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
        bash_command=f'{DBT_CMD} test --profiles-dir {DBT_PROFILES} --project-dir {DBT_PROJECT_DIR} --select "gold.* assert_revenue_positive assert_booking_dates_valid assert_booking_disposition_reconciles assert_ratings_in_range assert_scd_validity_windows_do_not_overlap assert_session_funnel_consistent"',
    )

    # ── Dependency chain ──────────────────────────────────────────────────────
    bronze_run >> bronze_test >> silver_run >> silver_test >> snapshot >> gold_run >> gold_test
