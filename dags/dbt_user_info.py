from datetime import datetime
from pathlib import Path

from airflow.sdk import DAG
from cosmos import (
    DbtTaskGroup,
    ProjectConfig,
    ProfileConfig,
    ExecutionConfig,
    RenderConfig,
)
from cosmos.constants import InvocationMode
from cosmos.profiles import PostgresUserPasswordProfileMapping

DBT_PROJECT_PATH = Path("/opt/airflow/dbt/data_pipeline_platform")
DBT_EXECUTABLE = "/home/airflow/dbt_venv/bin/dbt"

profile_config = ProfileConfig(
    profile_name="data_pipeline_platform",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="app_postgres",
        profile_args={"schema": "public"},
    ),
)

project_config = ProjectConfig(DBT_PROJECT_PATH)

execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE,
    invocation_mode=InvocationMode.SUBPROCESS,
)


def dbt_layer(group_id: str, tag: str) -> DbtTaskGroup:
    return DbtTaskGroup(
        group_id=group_id,
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(select=[f"tag:{tag}"]),
    )


with DAG(
    dag_id="dbt_users_info",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    raw          = dbt_layer("public_to_raw", "raw")
    staging      = dbt_layer("raw_to_staging", "staging")
    intermediate = dbt_layer("staging_to_intermediate", "intermediate")
    marts        = dbt_layer("intermediate_to_marts", "marts")

    raw >> staging >> intermediate >> marts