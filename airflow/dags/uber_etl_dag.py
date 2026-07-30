import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

PROJECT_ROOT = "/home/leakono/Engineer/uber-trip-analytics-platform"
DBT_PROJECT_DIR = os.path.join(PROJECT_ROOT, "uber_dbt")
VENV_PYTHON = os.path.join(PROJECT_ROOT, "venv/bin/python3")

default_args = {
    "owner": "emmanuel",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="uber_trip_analytics",
    description="Uber Trip Analytics — CSV ingestion, dbt transformation, data quality tests",
    schedule_interval="@daily",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    default_args=default_args,
    tags=["uber", "production"],
) as dag:

    # Task 1 — Run Python ingestion pipeline
    extract_and_load = BashOperator(
        task_id="extract_and_load",
        bash_command=(
            f"source {PROJECT_ROOT}/venv/bin/activate && "
            f"cd {PROJECT_ROOT} && "
            f"python3 -m ingestion.run_pipeline"
        ),
        env={
            "SNOWFLAKE_ACCOUNT": "CPEISHV-YW02437",
            "SNOWFLAKE_USER": "{{ var.value.uber_snowflake_user }}",
            "SNOWFLAKE_PASSWORD": "{{ var.value.uber_snowflake_password }}",
            "SNOWFLAKE_WAREHOUSE": "COMPUTE_WH",
            "SNOWFLAKE_DATABASE": "UBER_ANALYTICS",
            "SOURCE_FILE_PATH": f"{PROJECT_ROOT}/data/raw/uber_data.csv",
            "HOME": os.environ.get("HOME", "/home/leakono"),
            "PATH": os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin"),
        },
    )

    # Task 2 — dbt staging
    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=(
            f"source {PROJECT_ROOT}/venv/bin/activate && "
            f"cd {PROJECT_ROOT} && "
            f"dbt run --select staging --project-dir {DBT_PROJECT_DIR}"
        ),
        env={
            "HOME": os.environ.get("HOME", "/home/leakono"),
            "PATH": os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin"),
        },
    )

    # Task 3 — dbt marts
    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=(
            f"source {PROJECT_ROOT}/venv/bin/activate && "
            f"cd {PROJECT_ROOT} && "
            f"dbt run --select marts --project-dir {DBT_PROJECT_DIR}"
        ),
        env={
            "HOME": os.environ.get("HOME", "/home/leakono"),
            "PATH": os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin"),
        },
    )

    # Task 4 — dbt tests
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"source {PROJECT_ROOT}/venv/bin/activate && "
            f"cd {PROJECT_ROOT} && "
            f"dbt test --project-dir {DBT_PROJECT_DIR}"
        ),
        env={
            "HOME": os.environ.get("HOME", "/home/leakono"),
            "PATH": os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin"),
        },
    )

    # Task 5 — notify
    def notify_success():
        print("=" * 60)
        print("Uber Trip Analytics Pipeline completed successfully.")
        print(f"Timestamp: {datetime.utcnow().isoformat()}")
        print("RAW → STAGING → MARTS → TESTS: all passed.")
        print("=" * 60)

    notify = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
    )

    # Dependency chain
    extract_and_load >> dbt_run_staging >> dbt_run_marts >> dbt_test >> notify