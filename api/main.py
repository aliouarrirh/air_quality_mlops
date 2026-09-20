import os
from contextlib import asynccontextmanager

import mlflow
import mlflow.pyfunc
import pandas as pd
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

load_dotenv()
mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "ml/mlruns"))

MODEL_NAME = "DelhiAirQualityModel"
CHAMPION_ALIAS = "champion"

# Echappatoire : si MLFLOW_MODEL_URI est defini, il prime sur la resolution
# par alias. Sinon l'API suit l'alias @champion, ce qui rend la promotion
# depuis l'interface MLflow effective sans toucher a la configuration.
MODEL_URI_OVERRIDE = os.getenv("MLFLOW_MODEL_URI") or None

_model = None
_model_version = None
_model_source = None


def resolve_model_uri():
    """Determine quelle version servir.

    Priorite : override explicite > alias @champion > version la plus recente.
    Le repli sur la version la plus recente evite une API hors service tant
    qu'aucun champion n'a encore ete promu.
    """
    if MODEL_URI_OVERRIDE:
        return MODEL_URI_OVERRIDE, None, "override MLFLOW_MODEL_URI"

    client = mlflow.MlflowClient()
    try:
        mv = client.get_model_version_by_alias(MODEL_NAME, CHAMPION_ALIAS)
        return f"models:/{MODEL_NAME}@{CHAMPION_ALIAS}", mv.version, f"alias @{CHAMPION_ALIAS}"
    except Exception:
        versions = client.search_model_versions(f"name='{MODEL_NAME}'")
        if not versions:
            raise RuntimeError(f"aucune version enregistree pour {MODEL_NAME}")
        latest = max(versions, key=lambda v: int(v.version))
        return f"models:/{MODEL_NAME}/{latest.version}", latest.version, "derniere version (aucun champion promu)"


def get_model():
    """Charge le modele depuis le registre MLflow, une seule fois par processus."""
    global _model, _model_version, _model_source
    if _model is None:
        uri, version, source = resolve_model_uri()
        _model = mlflow.pyfunc.load_model(uri)
        _model_version, _model_source = version, source
        print(f"[api] modele charge : {uri} (version {version}, {source})")
    return _model


def reload_model():
    """Vide le cache et recharge, pour prendre en compte une promotion."""
    global _model
    _model = None
    get_model()


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        # get_model() journalise lui-meme l'URI, la version et le mode de resolution
        get_model()
    except Exception as e:
        # Demarrage non bloquant : MLflow peut ne pas encore repondre.
        # get_model() retentera au premier appel de /predict.
        print(f"[api] modele indisponible au demarrage ({e}) — nouvelle tentative a la 1ere requete")
    yield


app = FastAPI(
    title="Delhi Air Quality ML API",
    description="Prédiction AQI Delhi — OpenAQ v3 + NAQI",
    version="2.0.0",
    lifespan=lifespan,
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
    if aqi <= 50:
        return "Good"
    if aqi <= 100:
        return "Satisfactory"
    if aqi <= 200:
        return "Moderate"
    if aqi <= 300:
        return "Poor"
    if aqi <= 400:
        return "Very Poor"
    return "Severe"


@app.get("/health")
def health():
    """Endpoint santé — obligatoire (règles §7)."""
    # Renvoie toujours 200 : le monitoring mesure la disponibilite du service,
    # pas celle du modele, qui est rapportee separement via model_loaded.
    return {
        "status": "ok",
        "service": "delhi-air-quality-api",
        "city": "Delhi, India",
        "source": "OpenAQ v3",
        "model_loaded": _model is not None,
        "model_name": MODEL_NAME,
        "model_version": _model_version,
        "model_resolution": _model_source,
    }


@app.post("/reload")
def reload():
    """Recharge le modele depuis le registre, sans redemarrer le conteneur.

    A appeler apres avoir promu une nouvelle version en @champion.
    """
    try:
        reload_model()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Rechargement impossible : {e}")
    return {
        "status": "reloaded",
        "model_name": MODEL_NAME,
        "model_version": _model_version,
        "model_resolution": _model_source,
    }


@app.post("/predict", response_model=PredictResponse)
def predict(request: PredictRequest):
    """Endpoint prédiction AQI — obligatoire (règles §7)."""
    try:
        model = get_model()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Modèle non disponible : {e}")

    # Ordre des colonnes = ordre d'entraînement (pm25 est la cible, pas une feature)
    features = pd.DataFrame([{
        "pm10":           request.pm10 or 0.0,
        "no2":            request.no2  or 0.0,
        "so2":            request.so2  or 0.0,
        "co":             request.co   or 0.0,
        "o3":             request.o3   or 0.0,
        "hour_local":     request.hour_local,
        "day_of_week":    request.day_of_week,
        "month":          request.month,
        "season_winter":  int(request.season == "winter"),
        "season_summer":  int(request.season == "summer"),
        "season_monsoon": int(request.season == "monsoon"),
    }])

    aqi = float(model.predict(features)[0])
    return PredictResponse(
        aqi_predicted=round(aqi, 1),
        aqi_category=get_aqi_category(aqi),
        confidence="high" if request.pm10 and request.no2 else "medium",
        # Version reelle issue du registre MLflow, et non une variable statique
        model_version=str(_model_version) if _model_version else "inconnue",
    )
