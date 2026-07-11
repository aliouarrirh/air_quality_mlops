import logging
from datetime import datetime

from extract import extract_data
from transform import clean_and_merge
from feature_engineering import create_features
from load import save_to_processed


log_filename = f"../../logs/batch_pipeline_{datetime.now().strftime('%Y%m%d')}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(module)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename), 
        logging.StreamHandler()            
    ]
)

def run_batch_pipeline():
    logging.info("DÉMARRAGE DU PIPELINE BATCH ETL...")
    
    try:
        logging.info("--- ÉTAPE 1 : EXTRACTION ---")
        df_aq_raw, df_meteo_raw = extract_data(start_date="2021-01-01", end_date="2026-05-01")
        
        logging.info("--- ÉTAPE 2 : TRANSFORMATION ---")
        df_clean = clean_and_merge(df_aq_raw, df_meteo_raw)
        
        logging.info("--- ÉTAPE 3 : FEATURE ENGINEERING ---")
        df_features = create_features(df_clean)
        
        logging.info("--- ÉTAPE 4 : CHARGEMENT ---")
        save_to_processed(df_features, "../../data/processed/delhi_master.parquet")
        
        logging.info("PIPELINE BATCH TERMINÉ AVEC SUCCÈS !")

    except Exception as e:
        logging.critical(f"ERREUR FATALE DANS LE PIPELINE : {str(e)}", exc_info=True)

if __name__ == "__main__":
    run_batch_pipeline()