"""
dag_etl_hourly.py
-----------------
DAG Airflow — Pipeline ETL temps réel toutes les heures.
Compatible avec pipeline_realtime.py (run_realtime_pipeline sans paramètres).
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner":             "casablanca_aq",
    "retries":           2,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=10),
}


def run_etl(**context):
    """
    Appelle run_realtime_pipeline() depuis pipeline_realtime.py.
    Adapté au pipeline existant : pas de valeur de retour, 
    les erreurs sont catchées et re-levées pour Airflow.
    """
    import sys
    import os

    # ── Chemin vers etl/ dans le container Docker
    # docker-compose monte le projet dans /opt/airflow/project
    etl_path = "/opt/airflow/project/etl"
    if etl_path not in sys.path:
        sys.path.insert(0, etl_path)

    # Import du pipeline
    from pipeline_realtime import run_realtime_pipeline

    # Appel sans paramètres — exactement comme tu le fais en manuel
    try:
        run_realtime_pipeline()
        print("✅ Pipeline ETL exécuté avec succès.")
    except Exception as e:
        # On re-lève l'exception pour qu'Airflow marque le run comme FAILED
        # et déclenche les retries automatiques
        raise RuntimeError(f"Pipeline ETL échoué : {str(e)}") from e


def check_oms_alert(**context):
    """
    Vérifie la dernière prédiction dans PostgreSQL 
    et logue une alerte si PM2.5 > 15 µg/m³.
    """
    import os
    import sys
    from sqlalchemy import create_engine, text
    from dotenv import load_dotenv

    load_dotenv("/opt/airflow/project/.env")

    IN_DOCKER = os.path.exists("/opt/airflow/project")
    db_host = "postgres" if IN_DOCKER else os.getenv('DB_HOST', 'localhost')
    db_port = "5432"     if IN_DOCKER else os.getenv('DB_PORT', '5433')

    engine = create_engine(
        f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}"
        f"@{db_host}:{db_port}/{os.getenv('DB_NAME')}"
    )

    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT datetime, pm25_xgboost, pm25_cams, depasse_oms "
            "FROM predictions ORDER BY datetime DESC LIMIT 1"
        ))
        row = result.fetchone()

    if row:
        dt, xgb, cams, oms = row
        print(f"Dernière prédiction : {dt}")
        print(f"  XGBoost : {xgb:.2f} µg/m³")
        print(f"  CAMS    : {cams:.2f} µg/m³")
        if oms:
            print(f"  ⚠️  ALERTE OMS : PM2.5 = {xgb:.2f} µg/m³ > 15 µg/m³ !")
        else:
            print(f"  ✅ Sous seuil OMS (15 µg/m³)")
    else:
        print("Aucune prédiction en base.")


# ── Définition du DAG ─────────────────────────────────────────
with DAG(
    dag_id="dag_etl_hourly",
    description="Pipeline ETL temps réel Casablanca AQ — toutes les heures",
    default_args=default_args,
    schedule_interval="0 * * * *",  # toutes les heures pile
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["casablanca", "etl", "realtime"],
) as dag:

    etl_task = PythonOperator(
        task_id="etl_realtime",
        python_callable=run_etl,
    )

    alert_task = PythonOperator(
        task_id="check_oms_alert",
        python_callable=check_oms_alert,
    )

    etl_task >> alert_task
