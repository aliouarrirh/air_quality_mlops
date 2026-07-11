from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
import mlflow.pyfunc
import pandas as pd
import os

app = FastAPI(
    title="Delhi Air Quality ML API",
    description="Prédiction AQI Delhi — OpenAQ v3 + NAQI",
    version="2.0.0",
)


class PredictRequest(BaseModel):
    pm25: float       = Field(..., ge=0, le=999,  description="PM2.5 µg/m³")
    pm10: float       = Field(None, ge=0, le=999, description="PM10 µg/m³")
    no2: float        = Field(None, ge=0,          description="NO2 µg/m³")
    so2: float        = Field(None, ge=0,          description="SO2 µg/m³")
    co: float         = Field(None, ge=0,          description="CO mg/m³")
    o3: float         = Field(None, ge=0,          description="O3 µg/m³")
    hour_local: int   = Field(..., ge=0, le=23)
    day_of_week: int  = Field(..., ge=0, le=6)
    month: int        = Field(..., ge=1, le=12)
    season: str       = Field(..., pattern="^(winter|summer|monsoon|post_monsoon)$")
    location_id: int  = Field(None, description="ID station OpenAQ (optionnel)")


class PredictResponse(BaseModel):
    aqi_predicted: float
    aqi_category: str
    confidence: str
    model_version: str


def get_aqi_category(aqi: float) -> str:
    """Catégories NAQI — standard indien."""
    if aqi <= 50:  return "Good"
    if aqi <= 100: return "Satisfactory"
    if aqi <= 200: return "Moderate"
    if aqi <= 300: return "Poor"
    if aqi <= 400: return "Very Poor"
    return "Severe"


@app.get("/health")
def health():
    """Endpoint santé — obligatoire (règles §7)."""
    return {
        "status": "ok",
        "service": "delhi-air-quality-api",
        "city": "Delhi, India",
        "source": "OpenAQ v3",
    }


@app.post("/predict", response_model=PredictResponse)
def predict(request: PredictRequest):
    """Endpoint prédiction AQI — obligatoire (règles §7)."""
    model_uri = os.getenv("MLFLOW_MODEL_URI", "models:/DelhiAirQualityModel/Production")
    try:
        model = mlflow.pyfunc.load_model(model_uri)
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Modèle non disponible : {e}")

    features = pd.DataFrame([{
        "pm25":           request.pm25,
        "pm10":           request.pm10 or 0.0,
        "no2":            request.no2  or 0.0,
        "so2":            request.so2  or 0.0,
        "co":             request.co   or 0.0,
        "o3":             request.o3   or 0.0,
        "hour_local":     request.hour_local,
        "day_of_week":    request.day_of_week,
        "month":          request.month,
        "season_winter":  int(request.season == "winter"),
        "season_monsoon": int(request.season == "monsoon"),
        "season_summer":  int(request.season == "summer"),
    }])

    aqi = float(model.predict(features)[0])
    return PredictResponse(
        aqi_predicted=round(aqi, 1),
        aqi_category=get_aqi_category(aqi),
        confidence="high" if request.pm10 and request.no2 else "medium",
        model_version=os.getenv("MODEL_VERSION", "2.0.0"),
    )
