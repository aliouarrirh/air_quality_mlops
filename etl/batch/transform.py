import pandas as pd
import logging

import pandas as pd
import logging

def clean_and_merge(df_aq_raw, df_meteo_raw):
    """
    Nettoie, renomme et fusionne les données brutes de qualité de l'air et de météo.
    """
    logging.info("Début de l'étape TRANSFORM...")
    
    try:
        df_aq = df_aq_raw.copy()
        df_meteo = df_meteo_raw.copy()

        logging.info("Standardisation des noms de colonnes...")
        df_aq = df_aq.rename(columns={
            "pm2_5": "pm2p5",
            "pm10": "pm10",
            "nitrogen_dioxide": "no2"
        })
        
        df_meteo = df_meteo.rename(columns={
            "temperature_2m": "temp_c",
            "wind_speed_10m": "vent_kmh",
            "boundary_layer_height": "blh_m"
        })

        logging.info("Fusion (Inner Join) de la qualité de l'air et de la météo...")
        df_merged = df_aq.join(df_meteo, how="inner")
        
        taille_avant = len(df_merged)
        
        df_merged = df_merged.dropna(subset=["pm2p5"])
        df_merged = df_merged.interpolate(method="linear", limit=3)
        df_merged = df_merged.dropna()
        
        lignes_supprimees = taille_avant - len(df_merged)
        logging.info(f"Transformation réussie ! {lignes_supprimees} lignes inutilisables ont été purgées.")
        logging.info(f"Taille du dataset fusionné et propre : {len(df_merged)} lignes.")
        
        return df_merged

    except Exception as e:
        logging.error(f"Erreur fatale lors de la transformation : {e}")
        raise