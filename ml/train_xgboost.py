import os
import sys
import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import mlflow
import mlflow.xgboost
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)

from mlops.mlflow_config import setup_mlflow

def train_model():
    logging.info("🚀 Démarrage de l'entraînement XGBoost...")

    setup_mlflow("PM25_Casablanca")

    data_path = os.path.join(project_root, "data", "processed", "casablanca_master.parquet")
    logging.info(f"Chargement des données depuis : {data_path}")
    df = pd.read_parquet(data_path)

    y = df['pm2p5']
    colonnes_a_enlever = ['pm2p5']
    if 'datetime' in df.columns:
        colonnes_a_enlever.append('datetime')
    X = df.drop(columns=colonnes_a_enlever)

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)
    logging.info(f"Taille de l'entraînement : {len(X_train)} lignes. Taille du test : {len(X_test)} lignes.")

    with mlflow.start_run(run_name="XGBoost_Baseline"):
        
        params = {
            "n_estimators": 100,      
            "max_depth": 6,          
            "learning_rate": 0.1,     
            "random_state": 42
        }
        
        mlflow.log_params(params)

        logging.info("Entraînement en cours... (ça va aller très vite !)")
        model = xgb.XGBRegressor(**params)
        model.fit(X_train, y_train)

        logging.info("Prédiction sur les données de test (le futur)...")
        y_pred = model.predict(X_test)

        rmse = np.sqrt(mean_squared_error(y_test, y_pred))
        mae = mean_absolute_error(y_test, y_pred)         
        r2 = r2_score(y_test, y_pred)                     

        logging.info(f"Score RMSE : {rmse:.2f} µg/m³")
        logging.info(f"Score R2   : {r2:.2f}")

        mlflow.log_metric("rmse", rmse)
        mlflow.log_metric("mae", mae)
        mlflow.log_metric("r2", r2)

        mlflow.xgboost.log_model(model, "xgboost-model")
        logging.info("✅ Modèle XGBoost enregistré dans MLflow avec succès !")

if __name__ == "__main__":
    train_model()