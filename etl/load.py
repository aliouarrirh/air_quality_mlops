import os
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
load_dotenv(dotenv_path=env_path)

logger = logging.getLogger("ETL_LOAD")

DB_USER   = os.getenv("DB_USER")
DB_PASS   = os.getenv("DB_PASS")
DB_NAME   = os.getenv("DB_NAME", "casablanca_aq")
IN_DOCKER = os.path.exists("/opt/airflow/project")
DB_HOST   = "postgres" if IN_DOCKER else os.getenv("DB_HOST", "localhost")
DB_PORT   = "5432"     if IN_DOCKER else os.getenv("DB_PORT", "5433")


def load_predictions_to_postgres(timestamp, obs_pm25, pred_xgboost, pred_cams):
    logger.info("Initialisation de la transaction PostgreSQL.")

    if not DB_PASS:
        logger.error("DB_PASS introuvable. Vérifiez le fichier .env.")
        raise ValueError("Database credentials missing")

    try:
        engine = create_engine(
            f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        )

        alerte_oms = bool(pred_xgboost > 15.0)

        # UPSERT — pas de crash si datetime existe déjà
        upsert_sql = text("""
            INSERT INTO predictions
                (datetime, pm25_observe, pm25_xgboost, pm25_cams, depasse_oms)
            VALUES
                (:datetime, :pm25_observe, :pm25_xgboost, :pm25_cams, :depasse_oms)
            ON CONFLICT (datetime)
            DO UPDATE SET
                pm25_observe  = EXCLUDED.pm25_observe,
                pm25_xgboost  = EXCLUDED.pm25_xgboost,
                pm25_cams     = EXCLUDED.pm25_cams,
                depasse_oms   = EXCLUDED.depasse_oms
        """)

        with engine.begin() as conn:
            conn.execute(upsert_sql, {
                "datetime":     timestamp,
                "pm25_observe": obs_pm25,
                "pm25_xgboost": pred_xgboost,
                "pm25_cams":    pred_cams,
                "depasse_oms":  alerte_oms,
            })

        logger.info(f"Transaction validée (UPSERT). Index temporel : {timestamp}")

    except SQLAlchemyError as db_err:
        logger.error(f"Échec de la transaction base de données : {str(db_err)}")
        raise
    except Exception as e:
        logger.error(f"Erreur d'IO lors du chargement : {str(e)}")
        raise
