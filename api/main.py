from fastapi import FastAPI, HTTPException
import pandas as pd
import mlflow.pyfunc
import os
import platform
from .schemas import AirQualityInput, AirQualityResponse

app = FastAPI(
    title="Casablanca Air Quality API",
    description="API temps réel pour prédire la pollution PM2.5 avec le modèle XGBoost",
    version="1.0.0"
)


current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
db_path = os.path.join(project_root, 'mlruns.db')

if platform.system() == "Windows":
    db_path = db_path.replace("\\", "/")

mlflow.set_tracking_uri(f"sqlite:///{db_path}")

MODEL_NAME = "Casablanca_PM25_Predictor"
MODEL_VERSION = "1"

try:
    print(f"Chargement du modèle {MODEL_NAME} (Version {MODEL_VERSION})...")
    model_uri = f"models:/{MODEL_NAME}/{MODEL_VERSION}"
    model = mlflow.pyfunc.load_model(model_uri=model_uri)
    print("Modèle chargé avec succès dans la RAM !")
except Exception as e:
    print(f"Erreur critique lors du chargement : {e}")
    model = None


@app.get("/")
def home():
    return {"message": "Bienvenue sur l'API MLOps de Qualité de l'Air de Casablanca ! 🇲🇦"}

@app.post("/predict", response_model=AirQualityResponse)
def predict_pollution(data: AirQualityInput):
    if model is None:
        raise HTTPException(status_code=500, detail="Le modèle n'est pas disponible en production.")

    try:
        input_df = pd.DataFrame([data.model_dump()])
        prediction = model.predict(input_df)

        return AirQualityResponse(
            prediction_pm25=round(float(prediction[0]), 2),
            status="success"
        )

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erreur lors de la prédiction : {str(e)}")