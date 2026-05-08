import logging
import pandas as pd

logger = logging.getLogger("ETL_TRANSFORM")

def transform_features(df_aq: pd.DataFrame, df_meteo: pd.DataFrame) -> pd.DataFrame:

    logger.info("Début du pipeline de transformation (Jointure et dérivation de features).")
    
    try:
        df = pd.concat([df_aq, df_meteo], axis=1)
        
        rename_map = {
            "nitrogen_dioxide": "no2",
            "temperature_2m": "temp_c",
            "wind_speed_10m": "vent_kmh",
            "boundary_layer_height": "blh_m"
        }
        df = df.rename(columns=rename_map)
        
        df['heure'] = df.index.hour
        df['jour_semaine'] = df.index.dayofweek
        df['mois'] = df.index.month
        df['est_weekend'] = (df.index.dayofweek >= 5).astype(int)
        
        df['pm2p5_lag_1h'] = df['pm2_5'].shift(1)
        df['pm2p5_lag_2h'] = df['pm2_5'].shift(2)
        df['pm2p5_lag_24h'] = df['pm2_5'].shift(24)
        
        logger.info(f"Transformation terminée. Dimensions de la matrice de sortie : {df.shape}")
        return df
        
    except Exception as e:
        logger.error(f"Erreur fatale lors du Feature Engineering : {str(e)}")
        raise