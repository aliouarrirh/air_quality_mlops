"""
Entraînement XGBoost — Delhi Air Quality
Lit mart_delhi_hourly depuis DuckDB, enregistre dans MLflow.
"""
import os
import duckdb
import pandas as pd
import numpy as np
import xgboost as xgb
import mlflow
import mlflow.xgboost
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

DUCKDB_PATH   = os.getenv("DUCKDB_PATH", "data/delhi_air_quality.duckdb")
MLFLOW_URI    = os.getenv("MLFLOW_TRACKING_URI", "ml/mlruns")
MODEL_NAME    = "DelhiAirQualityModel"

mlflow.set_tracking_uri(MLFLOW_URI)
mlflow.set_experiment("delhi_air_quality")

def load_data() -> pd.DataFrame:
    conn = duckdb.connect(DUCKDB_PATH, read_only=True)
    df = conn.execute("""
        SELECT
            hour_utc, pm25, pm10, no2, so2, co, o3,
            EXTRACT(hour  FROM hour_utc)::INT AS hour_local,
            EXTRACT(dow   FROM hour_utc)::INT AS day_of_week,
            EXTRACT(month FROM hour_utc)::INT AS month,
            season
        FROM main.mart_delhi_hourly
        WHERE pm25 IS NOT NULL
    """).df()
    conn.close()
    return df

def build_features(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    df = df.copy()
    df["season_winter"]  = (df["season"] == "winter").astype(int)
    df["season_summer"]  = (df["season"] == "summer").astype(int)
    df["season_monsoon"] = (df["season"] == "monsoon").astype(int)

    feature_cols = [
        "pm10", "no2", "so2", "co", "o3",
        "hour_local", "day_of_week", "month",
        "season_winter", "season_summer", "season_monsoon",
    ]
    X = df[feature_cols].fillna(0)
    y = df["pm25"]
    return X, y

def train():
    print("Chargement des données depuis mart_delhi_hourly...")
    df = load_data()
    print(f"  {len(df)} lignes chargées")

    X, y = build_features(df)
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, shuffle=False
    )

    params = {
        "n_estimators":  200,
        "max_depth":     6,
        "learning_rate": 0.1,
        "subsample":     0.8,
        "random_state":  42,
    }

    with mlflow.start_run(run_name="XGBoost_Delhi_v1"):
        mlflow.log_params(params)

        model = xgb.XGBRegressor(**params)
        model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)

        y_pred = model.predict(X_test)
        rmse = float(np.sqrt(mean_squared_error(y_test, y_pred)))
        mae  = float(mean_absolute_error(y_test, y_pred))
        r2   = float(r2_score(y_test, y_pred))

        mlflow.log_metric("rmse", rmse)
        mlflow.log_metric("mae",  mae)
        mlflow.log_metric("r2",   r2)
        print(f"  RMSE={rmse:.2f}  MAE={mae:.2f}  R²={r2:.3f}")

        model_info = mlflow.xgboost.log_model(
            model,
            artifact_path="model",
            registered_model_name=MODEL_NAME,
        )
        run_id = mlflow.active_run().info.run_id
        model_uri = f"runs:/{run_id}/model"
        print(f"  Modele enregistre : {model_uri}")

    print(f"\nDone. Modele '{MODEL_NAME}' disponible dans MLflow.")
    print(f"MLFLOW_MODEL_URI={model_uri}")
    return model_uri

if __name__ == "__main__":
    uri = train()
    # Ecrire l'URI dans .env pour que l'API le trouve
    env_path = ".env"
    lines = open(env_path).readlines() if os.path.exists(env_path) else []
    lines = [l for l in lines if not l.startswith("MLFLOW_MODEL_URI=")]
    lines.append(f"MLFLOW_MODEL_URI={uri}\n")
    with open(env_path, "w") as f:
        f.writelines(lines)
    print(f"MLFLOW_MODEL_URI mis a jour dans .env : {uri}")
