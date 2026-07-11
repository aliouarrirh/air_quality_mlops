import requests
import pandas as pd
import logging

LAT, LON = 28.63576, 77.22445

def call_open_meteo(url, params):
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        df = pd.DataFrame(data["hourly"])
        df["datetime"] = pd.to_datetime(df["time"])
        df = df.drop(columns=["time"]).set_index("datetime")
        return df
    except Exception as e:
        logging.error(f"Échec de l'appel API : {url}. Erreur: {e}")
        raise
    
def extract_data(start_date, end_date):
    logging.info(f"Extraction des données de {start_date} à {end_date}...")
    
    params_aq = {
        "latitude": LAT, "longitude": LON, "start_date": start_date, "end_date": end_date,
        "hourly": ["pm2_5", "pm10", "nitrogen_dioxide"], "timezone": "Asia/Kolkata"
    }
    params_weather = {
        "latitude": LAT, "longitude": LON, "start_date": start_date, "end_date": end_date,
        "hourly": ["temperature_2m", "wind_speed_10m", "boundary_layer_height"], "timezone": "Asia/Kolkata"
    }

    try:
        logging.info("Téléchargement Air Quality...")
        df_aq = call_open_meteo("https://air-quality-api.open-meteo.com/v1/air-quality", params_aq)
        
        logging.info("Téléchargement Météo...")
        df_meteo = call_open_meteo("https://archive-api.open-meteo.com/v1/archive", params_weather)
        
        df_aq.to_parquet("../../data/raw/air_quality_raw.parquet")
        df_meteo.to_parquet("../../data/raw/weather_raw.parquet")
        logging.info("Fichiers RAW sauvegardés avec succès dans data/raw/")
        
        return df_aq, df_meteo
        
    except Exception as e:
        logging.error("Erreur critique lors de l'extraction. Arrêt.")
        raise