import os
import logging
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv



env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
load_dotenv(dotenv_path=env_path)

logger = logging.getLogger("ETL_LOAD")

DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "casablanca_aq")

def load_predictions_to_postgres(timestamp, obs_pm25, pred_xgboost, pred_cams):
    logger.info("Initialisation de la transaction PostgreSQL.")
    
    if not DB_PASS:
        logger.error("Les variables d'environnement (DB_PASS) sont introuvables. Vérifiez le fichier .env.")
        raise ValueError("Database credentials missing")

    try:
        engine_url = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(engine_url)
        
        alerte_oms = bool(pred_xgboost > 15.0)
        
        df_insert = pd.DataFrame({
            "datetime": [timestamp],
            "pm25_observe": [obs_pm25],
            "pm25_xgboost": [pred_xgboost],
            "pm25_cams": [pred_cams],
            "depasse_oms": [alerte_oms]
        })
        
        df_insert.to_sql("predictions", engine, if_exists="append", index=False)
        
        logger.info(f"Transaction validée. Prédictions commitées pour l'index temporel : {timestamp}")
        
    except SQLAlchemyError as db_err:
        logger.error(f"Échec de la transaction base de données : {str(db_err)}")
        raise
    except Exception as e:
        logger.error(f"Erreur d'IO lors du chargement : {str(e)}")
        raise