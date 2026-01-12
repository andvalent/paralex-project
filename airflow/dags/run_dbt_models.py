import os
from datetime import datetime
from airflow import DAG
# Updated import to avoid the deprecation warning
from airflow.providers.standard.operators.bash import BashOperator

# ===========================================
#               CONFIGURATION
# ===========================================
# Based on your last log, the project is exactly here:
DBT_PROJECT_DIR = "/opt/airflow/paralex_project"

with DAG(
    dag_id="run_dbt_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,         
    catchup=False,
    tags=["dbt", "transformation"]
) as dag:

    # Task 1: Actually test the connection
    # This checks if profiles.yml and dbt_project.yml are correct
    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt debug --profiles-dir ."
    )

    # Task 2: Run all models (Cleaning -> Subset Views)
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir ."
    )

    # Set dependency
    dbt_debug >> dbt_run

