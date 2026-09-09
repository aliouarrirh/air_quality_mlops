"""
Dagster pipeline — Delhi Air Quality
Orchestration : dlt → dbt → ML → MLflow + monitoring
"""
import subprocess
import sys
from dagster import (
    asset, AssetExecutionContext,
    define_asset_job, ScheduleDefinition, Definitions,
    AssetSelection,
)


@asset(group_name="ingestion", description="Ingestion OpenAQ → DuckDB via dlt")
def ingest_delhi_data(context: AssetExecutionContext):
    result = subprocess.run(
        [sys.executable, "pipeline/ingestion.py"],
        capture_output=True, text=True,
    )
    context.log.info(result.stdout)
    if result.returncode != 0:
        raise Exception(result.stderr)
    return {"status": "ok"}


@asset(group_name="transform", deps=["ingest_delhi_data"],
       description="Transformations dbt (run + test)")
def run_dbt(context: AssetExecutionContext):
    for cmd in [
        ["dbt", "run",  "--project-dir", "dbt_project", "--profiles-dir", "dbt_project"],
        ["dbt", "test", "--project-dir", "dbt_project", "--profiles-dir", "dbt_project"],
    ]:
        result = subprocess.run(cmd, capture_output=True, text=True)
        context.log.info(result.stdout)
        if result.returncode != 0:
            raise Exception(result.stderr)
    return {"status": "ok"}


@asset(group_name="ml", deps=["run_dbt"],
       description="Entraînement modèle ML + tracking MLflow")
def train_model(context: AssetExecutionContext):
    result = subprocess.run(
        [sys.executable, "ml/train.py"],
        capture_output=True, text=True,
    )
    context.log.info(result.stdout)
    if result.returncode != 0:
        raise Exception(result.stderr)
    return {"status": "ok"}


@asset(group_name="monitoring", description="Collecte disponibilité, latence, métriques ML, dérive → PostgreSQL")
def collect_monitoring(context: AssetExecutionContext):
    result = subprocess.run(
        [sys.executable, "pipeline/monitoring.py"],
        capture_output=True, text=True,
    )
    context.log.info(result.stdout)
    if result.returncode != 0:
        context.log.warning(f"Monitoring warning: {result.stderr}")
    return {"status": "ok"}


daily_job = define_asset_job(
    name="delhi_daily_pipeline",
    selection=AssetSelection.groups("ingestion", "transform", "ml"),
)

monitoring_job = define_asset_job(
    name="monitoring_job",
    selection=AssetSelection.groups("monitoring"),
)

daily_schedule = ScheduleDefinition(
    job=daily_job,
    cron_schedule="0 1 * * *",
)

monitoring_schedule = ScheduleDefinition(
    job=monitoring_job,
    cron_schedule="0 * * * *",  # toutes les heures
)

defs = Definitions(
    assets=[ingest_delhi_data, run_dbt, train_model, collect_monitoring],
    jobs=[daily_job, monitoring_job],
    schedules=[daily_schedule, monitoring_schedule],
)
