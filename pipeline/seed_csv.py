"""
Génère des fichiers CSV Delhi Air Quality dans data/
Basé sur les vraies stations CPCB Delhi avec valeurs réalistes.
Remplace l'API OpenAQ pour le développement/démo.
"""
import csv
import os
import random
import math
from datetime import datetime, timedelta, timezone

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

# Vraies stations CPCB Delhi avec coordonnées réelles
STATIONS = [
    (1001, "Anand Vihar",      "East Delhi",    28.6469, 77.3152, True),
    (1002, "ITO",              "Central Delhi", 28.6289, 77.2474, True),
    (1003, "Punjabi Bagh",     "West Delhi",    28.6742, 77.1311, True),
    (1004, "R.K. Puram",       "South Delhi",   28.5630, 77.1880, True),
    (1005, "Mandir Marg",      "Central Delhi", 28.6353, 77.2010, True),
    (1006, "Rohini",           "North Delhi",   28.7331, 77.1134, True),
    (1007, "Dwarka",           "South Delhi",   28.5823, 77.0500, True),
    (1008, "Okhla Phase 2",    "South Delhi",   28.5354, 77.2698, True),
    (1009, "Wazirpur",         "North Delhi",   28.6972, 77.1656, True),
    (1010, "Chandni Chowk",    "Old Delhi",     28.6562, 77.2300, True),
    (1011, "Jahangirpuri",     "North Delhi",   28.7284, 77.1726, True),
    (1012, "Patparganj",       "East Delhi",    28.6280, 77.2960, True),
]

PARAMETERS = ["pm25", "pm10", "no2", "so2", "co", "o3"]

# Moyennes réalistes Delhi par polluant (µg/m³ sauf CO en mg/m³)
PARAM_CONFIG = {
    #         mean   std   min   max
    "pm25": ( 85,    40,    5,  300),
    "pm10": (160,    70,   10,  500),
    "no2":  ( 45,    20,    5,  200),
    "so2":  ( 18,     8,    2,   80),
    "co":   (1.8,   0.8,  0.1,  8.0),
    "o3":   ( 35,    15,    5,  120),
}

def seasonal_factor(dt: datetime) -> float:
    """Delhi pollution : pic hiver (nov-jan), creux mousson (juil-sept)."""
    month = dt.month
    if month in (11, 12, 1):
        return 1.6
    elif month in (2, 3):
        return 1.2
    elif month in (7, 8, 9):
        return 0.6
    elif month in (4, 5, 6):
        return 0.9
    else:
        return 1.0

def diurnal_factor(dt: datetime) -> float:
    """Pic matin (8h) et soir (20h), creux nuit."""
    hour = dt.hour
    return 1.0 + 0.4 * math.sin(math.pi * (hour - 2) / 12)

def generate_value(param: str, dt: datetime) -> float:
    mean, std, vmin, vmax = PARAM_CONFIG[param]
    val = random.gauss(mean, std) * seasonal_factor(dt) * diurnal_factor(dt)
    return round(max(vmin, min(vmax, val)), 2)


# ── 1. Locations CSV ─────────────────────────────────────────────────────────
locations_path = os.path.join(DATA_DIR, "delhi_locations.csv")
with open(locations_path, "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=[
        "id", "name", "locality", "timezone",
        "latitude", "longitude", "is_monitor", "provider", "parameters", "ingested_at"
    ])
    w.writeheader()
    for sid, name, locality, lat, lon, is_monitor in STATIONS:
        w.writerow({
            "id":          sid,
            "name":        name,
            "locality":    locality,
            "timezone":    "Asia/Kolkata",
            "latitude":    lat,
            "longitude":   lon,
            "is_monitor":  is_monitor,
            "provider":    "CPCB",
            "parameters":  "|".join(PARAMETERS),
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        })
print(f"[OK] {locations_path} — {len(STATIONS)} stations")


# ── 2. Measurements CSV — 60 derniers jours, toutes les 4h ──────────────────
measurements_path = os.path.join(DATA_DIR, "delhi_measurements.csv")
end   = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
start = end - timedelta(days=60)
timestamps = []
t = start
while t <= end:
    timestamps.append(t)
    t += timedelta(hours=4)

sensor_id = 5000
row_count = 0
with open(measurements_path, "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=[
        "location_id", "sensor_id", "parameter", "value", "unit",
        "datetime_utc", "datetime_local", "latitude", "longitude", "ingested_at"
    ])
    w.writeheader()
    for sid, name, locality, lat, lon, _ in STATIONS:
        for param in PARAMETERS:
            sensor_id += 1
            for ts in timestamps:
                ts_local = ts + timedelta(hours=5, minutes=30)  # IST = UTC+5:30
                w.writerow({
                    "location_id":    sid,
                    "sensor_id":      sensor_id,
                    "parameter":      param,
                    "value":          generate_value(param, ts),
                    "unit":           "µg/m³" if param != "co" else "mg/m³",
                    "datetime_utc":   ts.isoformat(),
                    "datetime_local": ts_local.isoformat(),
                    "latitude":       lat,
                    "longitude":      lon,
                    "ingested_at":    datetime.now(timezone.utc).isoformat(),
                })
                row_count += 1
print(f"[OK] {measurements_path} — {row_count} mesures")


# ── 3. Latest CSV — dernière valeur par station/polluant ────────────────────
latest_path = os.path.join(DATA_DIR, "delhi_latest.csv")
with open(latest_path, "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=[
        "location_id", "location_name", "parameter", "value", "unit",
        "datetime_utc", "latitude", "longitude", "ingested_at"
    ])
    w.writeheader()
    for sid, name, locality, lat, lon, _ in STATIONS:
        for param in PARAMETERS:
            w.writerow({
                "location_id":   sid,
                "location_name": name,
                "parameter":     param,
                "value":         generate_value(param, end),
                "unit":          "µg/m³" if param != "co" else "mg/m³",
                "datetime_utc":  end.isoformat(),
                "latitude":      lat,
                "longitude":     lon,
                "ingested_at":   datetime.now(timezone.utc).isoformat(),
            })
print(f"[OK] {latest_path} — {len(STATIONS) * len(PARAMETERS)} latest")
print("\nDone. Lance maintenant: python pipeline/ingestion.py")
