from airflow import DAG
from cosmos import DbtTaskGroup
from cosmos.config import ProjectConfig, ProfileConfig, ExecutionConfig
from datetime import datetime

with DAG(
    dag_id="hh_silver_dbt",
    start_date=datetime(2025, 12, 1),
    schedule="0 4 * * *",
    catchup=False,
    max_active_runs=1,
) as dag:

    silver_models = DbtTaskGroup(
        group_id="silver_models",

        project_config=ProjectConfig(
            dbt_project_path="/opt/airflow/dbt/hh_compliance_dbt"
        ),

        profile_config=ProfileConfig(
            profile_name="hh_compliance_dbt",
            target_name="prod",
            profiles_yml_filepath="/opt/airflow/dbt/profiles.yml",
        ),

        execution_config=ExecutionConfig(
            dbt_executable_path="/usr/local/bin/dbt"
        ),

        select=["path:models/silver"]
    )