"""
dag_retrain.py
--------------
DAG Airflow — Re-entraînement conditionnel XGBoost.
Se déclenche uniquement si Evidently détecte un data drift.

Flow :
  check_drift → [drift détecté] → retrain_xgboost → register_model → notify
              → [pas de drift]  → skip (rien à faire)
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator

# ── Paramètres du DAG ─────────────────────────────────────────
default_args = {
    "owner":            "casablanca_aq",
    "retries":          1,
    "retry_delay":      timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=30),
}

# ── Fonctions ─────────────────────────────────────────────────

def check_drift(**context):
    """
    Utilise Evidently pour comparer :
    - données de référence (casablanca_master.parquet — entraînement)
    - données récentes (30 derniers jours dans PostgreSQL)

    Retourne l'ID de la tâche suivante selon le résultat.
    """
    import sys
    import pandas as pd
    from sqlalchemy import create_engine
    from evidently.report import Report
    from evidently.metric_preset import DataDriftPreset
    import os
    from dotenv import load_dotenv

    load_dotenv("/opt/airflow/project/.env")

    # ── Charger les données de référence (entraînement)
    ref_path = "/opt/airflow/project/data/processed/casablanca_master.parquet"
    df_reference = pd.read_parquet(ref_path)

    # ── Charger les données récentes depuis PostgreSQL
    engine = create_engine(
        f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )
    df_current = pd.read_sql(
        "SELECT * FROM observations_reelles ORDER BY datetime DESC LIMIT 720",  # 30 jours × 24h
        engine
    )

    if len(df_current) < 48:
        print("⚠️  Pas assez de données récentes pour analyser le drift. Skip.")
        return "no_drift"

    # ── Colonnes communes pour la comparaison
    feature_cols = ["pm2p5", "pm10", "no2", "temp_c", "vent_kmh", "blh_m"]
    df_ref_sample = df_reference[feature_cols].tail(720)
    df_cur_sample = df_current[feature_cols]

    # ── Rapport Evidently
    report = Report(metrics=[DataDriftPreset()])
    report.run(reference_data=df_ref_sample, current_data=df_cur_sample)

    # Sauvegarder le rapport HTML pour consultation
    report_path = "/opt/airflow/project/reports/drift_report.html"
    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    report.save_html(report_path)

    # ── Décision : drift détecté ?
    result      = report.as_dict()
    drift_share = result["metrics"][0]["result"]["share_of_drifted_columns"]
    drift_detected = drift_share > 0.5   # > 50% des colonnes ont drifté

    print(f"📊 Drift share : {drift_share:.0%} | Décision : {'DRIFT ⚠️' if drift_detected else 'OK ✅'}")

    # Pousser le résultat pour la tâche notify
    context["ti"].xcom_push(key="drift_share", value=drift_share)

    return "retrain_xgboost" if drift_detected else "no_drift"


def retrain_xgboost(**context):
    """
    Re-lance l'entraînement XGBoost sur les données fraîches.
    Appelle directement le script d'entraînement existant.
    """
    import sys
    import subprocess

    print("🔄 Drift détecté — Lancement du re-entraînement XGBoost...")

    result = subprocess.run(
        ["python", "/opt/airflow/project/ml/train_xgboost.py"],
        capture_output=True, text=True
    )

    if result.returncode != 0:
        raise RuntimeError(f"Re-entraînement échoué :\n{result.stderr}")

    print(result.stdout)
    print("✅ Re-entraînement terminé.")


def register_new_model(**context):
    """
    Enregistre le nouveau modèle dans MLflow Model Registry
    et lui donne l'alias 'Production' s'il est meilleur.
    """
    import mlflow
    from mlflow.tracking import MlflowClient

    mlflow.set_tracking_uri("sqlite:////opt/airflow/project/mlruns.db")
    client = MlflowClient()

    # Récupérer le dernier run XGBoost
    experiment = client.get_experiment_by_name("Casablanca_AirQuality")
    runs = client.search_runs(
        experiment_ids=[experiment.experiment_id],
        order_by=["metrics.rmse ASC"],
        max_results=1
    )

    if not runs:
        raise ValueError("Aucun run trouvé dans MLflow.")

    best_run = runs[0]
    best_rmse = best_run.data.metrics.get("rmse", 999)

    print(f"🏆 Meilleur run : {best_run.info.run_id} | RMSE = {best_rmse:.4f}")

    # Enregistrer dans le Model Registry
    model_uri = f"runs:/{best_run.info.run_id}/model"
    mv = mlflow.register_model(model_uri, "CasablancaAirQuality")

    # Promouvoir en Production
    client.set_registered_model_alias("CasablancaAirQuality", "Production", mv.version)
    print(f"✅ Modèle v{mv.version} promu en Production (RMSE={best_rmse:.4f})")

    context["ti"].xcom_push(key="new_model_version", value=mv.version)
    context["ti"].xcom_push(key="new_rmse",          value=best_rmse)


def notify_result(**context):
    """Log le résultat du re-entraînement."""
    drift_share = context["ti"].xcom_pull(key="drift_share",       task_ids="check_drift")
    new_version = context["ti"].xcom_pull(key="new_model_version", task_ids="register_model")
    new_rmse    = context["ti"].xcom_pull(key="new_rmse",          task_ids="register_model")

    print("=" * 50)
    print("RAPPORT RE-ENTRAÎNEMENT")
    print(f"  Drift détecté    : {drift_share:.0%} des colonnes")
    print(f"  Nouveau modèle   : v{new_version}")
    print(f"  RMSE             : {new_rmse:.4f}")
    print(f"  Rapport drift    : /reports/drift_report.html")
    print("=" * 50)


# ── Définition du DAG ─────────────────────────────────────────
with DAG(
    dag_id="dag_retrain",
    description="Re-entraînement conditionnel XGBoost si drift Evidently détecté",
    default_args=default_args,
    schedule_interval="0 2 * * 0",   # chaque dimanche à 2h du matin (vérification hebdo)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["casablanca", "mlops", "retrain", "drift"],
) as dag:

    # Tâche 1 — Analyse drift Evidently (branchement)
    drift_check = BranchPythonOperator(
        task_id="check_drift",
        python_callable=check_drift,
    )

    # Tâche 2a — Drift détecté → re-entraîner
    retrain = PythonOperator(
        task_id="retrain_xgboost",
        python_callable=retrain_xgboost,
    )

    # Tâche 2b — Pas de drift → skip
    no_drift = EmptyOperator(
        task_id="no_drift",
    )

    # Tâche 3 — Enregistrer nouveau modèle MLflow
    register = PythonOperator(
        task_id="register_model",
        python_callable=register_new_model,
    )

    # Tâche 4 — Notification résultat
    notify = PythonOperator(
        task_id="notify_result",
        python_callable=notify_result,
    )

    # ── Ordre d'exécution ─────────────────────────────────────
    #
    #  check_drift ──→ retrain_xgboost → register_model → notify_result
    #              └→ no_drift
    #
    drift_check >> [retrain, no_drift]
    retrain     >> register >> notify
