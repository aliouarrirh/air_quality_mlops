import logging
from typing import Tuple
import requests
import pandas as pd

# Configuration du logging (Format standard de production)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(name)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("ETL_EXTRACT")

LATITUDE = 33.57
LONGITUDE = -7.59
TIMEZONE = "Africa/Casablanca"

API_URL_AIR_QUALITY = "https://air-quality-api.open-meteo.com/v1/air-quality"
API_URL_WEATHER = "https://api.open-meteo.com/v1/forecast"


def fetch_open_meteo_payload(url: str, params: dict) -> pd.DataFrame:
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        payload = response.json()
        
        df = pd.DataFrame(payload.get("hourly", {}))
        
        if df.empty:
            logger.warning(f"Payload vide retournée par {url}.")
            return df

        df["datetime"] = pd.to_datetime(df["time"])
        df = df.drop(columns=["time"]).set_index("datetime")
        
        if df.index.tz is None:
            df.index = df.index.tz_localize(TIMEZONE, ambiguous="NaT", nonexistent="NaT")
        else:
            df.index = df.index.tz_convert(TIMEZONE)
            
        return df
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Echec de résolution réseau pour l'endpoint {url}. Exception: {str(e)}")
        raise


def extract_latest_window(window_hours: int = 48) -> Tuple[pd.DataFrame, pd.DataFrame]:
    logger.info(f"Initialisation de l'extraction de la fenêtre temporelle: T-{window_hours}h.")
    past_days = max(1, window_hours // 24 + 1)

    params_aq = {
        "latitude": LATITUDE, "longitude": LONGITUDE,
        "hourly": ["pm2_5", "pm10", "nitrogen_dioxide"],
        "timezone": TIMEZONE,
        "past_days": past_days, "forecast_days": 0
    }
    
    params_weather = {
        "latitude": LATITUDE, "longitude": LONGITUDE,
        "hourly": ["temperature_2m", "wind_speed_10m", "boundary_layer_height"],
        "timezone": TIMEZONE,
        "past_days": past_days, "forecast_days": 0
    }

    df_aq = fetch_open_meteo_payload(API_URL_AIR_QUALITY, params_aq)
    df_meteo = fetch_open_meteo_payload(API_URL_WEATHER, params_weather)

    now = pd.Timestamp.now(tz=TIMEZONE).floor("h")
    df_aq_bounded = df_aq[df_aq.index <= now].tail(window_hours)
    df_meteo_bounded = df_meteo[df_meteo.index <= now].tail(window_hours)

    logger.info(f"Extraction terminée. Shape AQ: {df_aq_bounded.shape}, Shape Météo: {df_meteo_bounded.shape}")
    return df_aq_bounded, df_meteo_bounded


def extract_cams_forecast(forecast_horizon: int = 24) -> pd.DataFrame:

    logger.info(f"Récupération de la baseline CAMS. Horizon: T+{forecast_horizon}h.")
    forecast_days = max(1, forecast_horizon // 24 + 1)

    params_cams = {
        "latitude": LATITUDE, "longitude": LONGITUDE,
        "hourly": ["pm2_5"], 
        "timezone": TIMEZONE,
        "past_days": 0, "forecast_days": forecast_days
    }

    df_cams = fetch_open_meteo_payload(API_URL_AIR_QUALITY, params_cams)

    now = pd.Timestamp.now(tz=TIMEZONE).floor("h")
    df_cams_future = df_cams[df_cams.index > now].head(forecast_horizon)
    df_cams_future = df_cams_future.rename(columns={"pm2_5": "pm2_5_cams"})

    logger.info(f"Baseline CAMS acquise. Shape: {df_cams_future.shape}")
    return df_cams_future


if __name__ == "__main__":
    logger.info("Démarrage du test d'intégration local du module d'extraction.")
    try:
        df_aq_hist, df_meteo_hist = extract_latest_window(window_hours=48)
        df_cams_base = extract_cams_forecast(forecast_horizon=24)
        logger.info("Test d'intégration validé. Sortie standard (0).")
    except Exception as exc:
        logger.critical(f"Erreur fatale lors du test d'intégration: {str(exc)}")
        exit(1)