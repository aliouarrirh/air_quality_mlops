import os
import sys
import logging
import numpy as np
import pandas as pd
from prophet import Prophet
from sklearn.metrics import mean_squared_error, r2_score
import mlflow
import mlflow.prophet

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)

log_dir = os.path.join(project_root, "logs")
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "prophet.log")

sys.stdout.reconfigure(encoding='utf-8')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8', mode='w'),
        logging.StreamHandler(sys.stdout)
    ]
)

from mlops.mlflow_config import setup_mlflow

def train_model():
    logging.info("Démarrage de l'entraînement Prophet (Statistiques Temporelles)...")

    setup_mlflow("PM25_Delhi")

    data_path = os.path.join(project_root, "data", "processed", "delhi_master.parquet")
    df = pd.read_parquet(data_path)

    colonnes_a_enlever = [
        'pm2p5_rolling_3h', 'pm2p5_rolling_24h', 
        'pm2p5_lag_1h', 'pm2p5_lag_2h', 'pm2p5_lag_24h', 'index'
    ]
    df_clean = df.drop(columns=colonnes_a_enlever, errors='ignore')
    df_prophet = df_clean.reset_index()
    df_prophet = df_prophet.rename(columns={
            'datetime': 'ds', 
            'index': 'ds', 
            'pm2p5': 'y'
        })
    df_prophet['ds'] = pd.to_datetime(df_prophet['ds']).dt.tz_localize(None)

    split_index = int(len(df_prophet) * 0.8)
    train_df = df_prophet.iloc[:split_index]
    test_df = df_prophet.iloc[split_index:]

    weather_cols = [col for col in df_prophet.columns if col not in ['ds', 'y']]

    with mlflow.start_run(run_name="Prophet_Baseline"):
        
        logging.info("Création du modèle Prophet avec jours fériés de l'Inde")

        model = Prophet(yearly_seasonality=True, weekly_seasonality=True, daily_seasonality=True)

        model.add_country_holidays(country_name='IN')
        
        for col in weather_cols:
            model.add_regressor(col)

        logging.info("Entraînement en cours...")
        model.fit(train_df)

        logging.info("Prédiction sur le futur...")
        future_df = test_df[['ds'] + weather_cols]
        forecast = model.predict(future_df)
        
        y_pred = forecast['yhat'].values
        y_true = test_df['y'].values

        rmse = np.sqrt(mean_squared_error(y_true, y_pred))
        r2 = r2_score(y_true, y_pred)

        logging.info(f"Score RMSE final (Prophet) : {rmse:.2f} µg/m³")
        logging.info(f"Score R2 final   (Prophet) : {r2:.2f}")
        mlflow.log_metric("rmse_real", rmse)
        mlflow.log_metric("r2_real", r2)
        
        mlflow.prophet.log_model(model, artifact_path="prophet-model")

        logging.info("Modèle Prophet enregistré dans MLflow !")

if __name__ == "__main__":
    train_model()