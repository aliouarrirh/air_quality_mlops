from pydantic import BaseModel, Field

class AirQualityInput(BaseModel):
    pm10: float = Field(..., description="Particules PM10 en µg/m³", example=45.2)
    no2: float = Field(..., description="Dioxyde d'azote en µg/m³", example=20.5)
    
    temp_c: float = Field(..., description="Température en °C", example=22.5)
    vent_kmh: float = Field(..., description="Vitesse du vent en km/h", example=15.2)
    blh_m: float = Field(..., description="Hauteur de la couche limite (Boundary Layer) en mètres", example=1012.5)
    
    heure: int = Field(..., description="Heure de la journée (0-23)", example=14)
    jour_semaine: int = Field(..., description="Jour de la semaine (0=Lundi, 6=Dimanche)", example=2)
    mois: int = Field(..., description="Mois de l'année (1-12)", example=5)
    est_weekend: int = Field(..., description="Est-ce le week-end ? (0=Non, 1=Oui)", example=0)
    
    pm2p5_lag_1h: float = Field(..., description="Pollution PM2.5 il y a 1h", example=12.4)
    pm2p5_lag_2h: float = Field(..., description="Pollution PM2.5 il y a 2h", example=11.8)
    pm2p5_lag_24h: float = Field(..., description="Pollution PM2.5 il y a exactement 24h", example=14.0)

class AirQualityResponse(BaseModel):
    prediction_pm25: float = Field(..., description="La prédiction de PM2.5 en µg/m³")
    status: str = Field(..., description="Statut de la requête ('success' ou 'error')")