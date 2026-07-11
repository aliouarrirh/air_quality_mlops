import os
import sys
import logging
import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout
import mlflow
import mlflow.tensorflow

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)

log_dir = os.path.join(project_root, "logs")
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "lstm.log")

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

def create_sequences(data, target_col_idx, window_size):
    X, y = [], []
    for i in range(len(data) - window_size):
        
        X.append(data[i : i + window_size, :])
        
        y.append(data[i + window_size, target_col_idx])
    return np.array(X), np.array(y)

def train_model():
    logging.info("🧠 Démarrage de l'entraînement Deep Learning (LSTM)...")

    setup_mlflow("PM25_Delhi")

    data_path = os.path.join(project_root, "data", "processed", "delhi_master.parquet")
    df = pd.read_parquet(data_path)

    colonnes_a_enlever = [
        'pm2p5_rolling_3h', 'pm2p5_rolling_24h', 
        'pm2p5_lag_1h', 'pm2p5_lag_2h', 'pm2p5_lag_24h'
    ]
    if 'datetime' in df.columns: colonnes_a_enlever.append('datetime')
    if 'index' in df.columns: colonnes_a_enlever.append('index')
    
    df_clean = df.drop(columns=colonnes_a_enlever, errors='ignore')
    
    target_idx = df_clean.columns.get_loc('pm2p5')

    scaler = MinMaxScaler()
    scaled_data = scaler.fit_transform(df_clean)

    WINDOW_SIZE = 24
    logging.info(f"Découpage en films 3D de {WINDOW_SIZE} heures...")
    X, y = create_sequences(scaled_data, target_idx, WINDOW_SIZE)

    split_index = int(len(X) * 0.8)
    X_train, X_test = X[:split_index], X[split_index:]
    y_train, y_test = y[:split_index], y[split_index:]

    with mlflow.start_run(run_name="LSTM_Baseline"):
        
        mlflow.tensorflow.autolog()

        model = Sequential([
            LSTM(50, activation='relu', input_shape=(WINDOW_SIZE, X_train.shape[2])),
            Dropout(0.2), 
            Dense(1)      
        ])
        
        model.compile(optimizer='adam', loss='mse')

        logging.info("Début de l'apprentissage (ça peut prendre 1 à 2 minutes)... ⏳")
        model.fit(X_train, y_train, epochs=10, batch_size=32, validation_split=0.1, verbose=1)

        logging.info("Prédiction sur le test...")
        y_pred_scaled = model.predict(X_test)

        dummy_array = np.zeros((len(y_pred_scaled), df_clean.shape[1]))
        dummy_array[:, target_idx] = y_pred_scaled.flatten()
        y_pred_real = scaler.inverse_transform(dummy_array)[:, target_idx]

        dummy_array_test = np.zeros((len(y_test), df_clean.shape[1]))
        dummy_array_test[:, target_idx] = y_test
        y_test_real = scaler.inverse_transform(dummy_array_test)[:, target_idx]

        rmse = np.sqrt(mean_squared_error(y_test_real, y_pred_real))
        r2 = r2_score(y_test_real, y_pred_real)

        logging.info(f"Score RMSE final : {rmse:.2f} µg/m³")
        logging.info(f"Score R2 final   : {r2:.2f}")

        mlflow.log_metric("rmse_real", rmse)
        mlflow.log_metric("r2_real", r2)

        logging.info("✅ Modèle LSTM enregistré dans MLflow !")

if __name__ == "__main__":
    train_model()