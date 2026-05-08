import os
import logging
import warnings
import mlflow
import pandas as pd

warnings.filterwarnings("ignore")


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = os.path.join(BASE_DIR, "logs")

os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "real_time_pipeline.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(name)-15s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8'), 
        logging.StreamHandler()                                    
    ]
)
logger = logging.getLogger("ETL_ORCHESTRATOR")


import platform

from extract import extract_latest_window, extract_cams_forecast
from transform import transform_features
from load import load_predictions_to_postgres

MLFLOW_DB_PATH = os.path.join(BASE_DIR, "mlruns.db")
if platform.system() == "Windows":
    MLFLOW_DB_PATH = MLFLOW_DB_PATH.replace("\\", "/")

MLFLOW_TRACKING_URI = f"sqlite:///{MLFLOW_DB_PATH}"
MODEL_NAME = "Casablanca_PM25_Predictor"
MODEL_VERSION = "1"

def load_production_model():
    logger.info(f"Tentative de résolution de l'URI MLflow : {MLFLOW_TRACKING_URI}")
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    try:
        
        model_uri = f"models:/{MODEL_NAME}/{MODEL_VERSION}"
        
        model = mlflow.pyfunc.load_model(model_uri)
        logger.info("Binaire XGBoost chargé en mémoire vive.")
        return model
    except Exception as e:
        logger.error(f"MLflow Model Registry unreachable : {str(e)}")
        raise

def run_realtime_pipeline():
    logger.info("="*60)
    logger.info("DÉMARRAGE DU CYCLE D'INFÉRENCE TEMPS RÉEL (NOWCASTING)")
    logger.info("="*60)
    
    try:
        # 1. EXTRACT
        logger.info("Extract............")
        df_aq, df_meteo = extract_latest_window(window_hours=48)
        df_cams = extract_cams_forecast(forecast_horizon=24)
        
        # 2. TRANSFORM
        logger.info("Transform............")

        df_features = transform_features(df_aq, df_meteo)
        
        # Isolation du vecteur courant (Instant T)
        inference_vector = df_features.iloc[[-1]].copy()
        current_timestamp = inference_vector.index[0]
        
        # 3. VERIFICATION D'INTÉGRITÉ
        features_in = ['pm10', 'no2', 'temp_c', 'vent_kmh', 'blh_m', 'heure', 
                       'jour_semaine', 'mois', 'est_weekend', 
                       'pm2p5_lag_1h', 'pm2p5_lag_2h', 'pm2p5_lag_24h']
        
        inference_vector_clean = inference_vector[features_in]
        
        if inference_vector_clean.isnull().values.any():
            logger.warning(f"Vecteur d'inférence corrompu (NaN détectés) à l'index {current_timestamp}. Interruption du pipeline.")
            return
            
        # 4. PREDICT
        model = load_production_model()
        prediction = model.predict(inference_vector_clean)
        
        # Récupération des scalaires
        pred_xgboost = round(float(prediction[0]), 2)
        pred_cams = round(float(df_cams.iloc[0]['pm2_5_cams']), 2)
        obs_pm25 = round(float(inference_vector['pm2_5'].iloc[0]), 2) # Valeur réelle pour erreur absolu
        
        logger.info(f"Métriques générées -> XGBoost: {pred_xgboost} | CAMS: {pred_cams} | Observé: {obs_pm25}")
        
        # 5. LOAD
        logger.info("Load............")

        load_predictions_to_postgres(current_timestamp, obs_pm25, pred_xgboost, pred_cams)
        
        logger.info("Cycle d'inférence exécuté et commité avec succès.")
        logger.info("="*60)
        
    except Exception as e:
        logger.critical(f"Rupture critique dans l'orchestrateur ETL : {str(e)}")

if __name__ == "__main__":
    run_realtime_pipeline()