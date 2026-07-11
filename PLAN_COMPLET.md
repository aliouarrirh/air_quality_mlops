# Plan complet — Delhi Air Quality MLOps
> Instructions pour Claude Code — conformité Prof. Ait Daoud + migration Delhi

---

## Vue d'ensemble

**Repo** : https://github.com/AbdelNarjiss/Casablanca_AirQuality  
**Objectif** : Rendre le projet conforme aux règles MLOps & DataOps **et** migrer la source de données de Casablanca vers Delhi (OpenAQ API v3).

### Ce qui change

| Dimension | Avant | Après |
|---|---|---|
| Ville | Casablanca, Maroc | **Delhi, Inde** |
| Source données | WAQI API (~1 station) | **OpenAQ API v3 (35 stations)** |
| Polluants | AQI, PM2.5, PM10, NO2, CO | **PM2.5, PM10, NO2, SO2, CO, O3** |
| Historique | Limité | **Depuis 2016** |
| Timezone | Africa/Casablanca | **Asia/Kolkata (IST = UTC+5:30)** |
| Orchestration | ❌ Airflow (non conforme) | **✅ Dagster** |
| Stockage local | ❌ PostgreSQL seul | **✅ DuckDB** (PostgreSQL peut coexister) |
| Ingestion | ❌ ETL custom | **✅ dlt** |
| Transformations | ❌ absentes | **✅ dbt** |
| CI/CD | ❌ absent | **✅ GitHub Actions** |
| Dockerfile | ❌ vide | **✅ complet** |
| README | ❌ vide | **✅ complet** |

### Station principale OpenAQ — Delhi
- **Location ID** : `8118` (New Delhi, US Embassy / AirNow)
- **Coordonnées** : lat `28.63576`, lon `77.22445`
- **API** : `https://api.openaq.org/v3`
- **Clé** : gratuite sur explore.openaq.org — header `X-API-Key`

---

## Tableau récapitulatif — tous les fichiers

| Action | Fichier / Dossier | Priorité |
|--------|-------------------|----------|
| MODIFIER | `requirements.txt` | 🔴 1 |
| CRÉER | `.env` + `.env.example` | 🔴 1 |
| CRÉER | `pipeline/ingestion.py` | 🔴 1 |
| CRÉER | `dbt_project/` (structure complète) | 🔴 1 |
| CRÉER | `dagster_pipeline/__init__.py` | 🔴 1 |
| CRÉER | `dagster_pipeline/workspace.yaml` | 🔴 1 |
| SUPPRIMER | `airflow/` | 🔴 1 |
| REMPLIR | `Dockerfile` (vide → complet) | 🟠 2 |
| CRÉER/MODIFIER | `api/main.py` | 🟠 2 |
| CRÉER | `.github/workflows/ci.yml` | 🟠 3 |
| CRÉER | `data_contracts/air_quality_contract.yaml` | 🟡 4 |
| CRÉER | `tests/test_data_quality.py` | 🟡 4 |
| CRÉER | `docs/data_lineage.md` | 🟡 4 |
| REMPLIR | `README.md` (vide → complet) | 🟡 4 |
| CONSERVER | `ml/`, `mlops/`, `grafana/`, `notebooks/` | — |

---

## Étape 0 — Vérification préalable

Avant tout, lancer cette commande pour identifier toutes les références à l'ancienne source :

```bash
grep -r "casablanca\|Casablanca\|CASABLANCA\|waqi\|WAQI\|casablanca_aq" \
  --include="*.py" --include="*.sql" --include="*.yaml" \
  --include="*.yml" --include="*.env" --include="*.md" \
  --include="*.txt" -l .
```

---

## Étape 1 — Dépendances

### `requirements.txt` — ajouter ces lignes

```text
# ── DataOps (obligatoires selon règles prof) ─────────────────────────────────
dlt[duckdb]>=0.4.0
duckdb>=0.10.0
dbt-duckdb>=1.8.0
dagster>=1.7.0
dagster-webserver>=1.7.0

# ── Source données ────────────────────────────────────────────────────────────
# OpenAQ API v3 — pas de lib dédiée, on utilise requests (déjà présent)

# ── Tests ─────────────────────────────────────────────────────────────────────
pytest>=8.0.0
pytest-cov>=5.0.0
httpx>=0.27.0        # pour tester FastAPI
ruff>=0.4.0          # linter CI
```

Supprimer de `requirements.txt` toute référence à `apache-airflow`.

---

## Étape 2 — Variables d'environnement

### `.env` (ne pas committer) et `.env.example` (à committer)

```bash
# ── Source données — OpenAQ API v3 ───────────────────────────────────────────
OPENAQ_API_KEY=your_openaq_api_key_here   # gratuit sur explore.openaq.org
OPENAQ_BASE_URL=https://api.openaq.org/v3

# ── Delhi ─────────────────────────────────────────────────────────────────────
DELHI_PRIMARY_LOCATION_ID=8118
DELHI_BBOX=28.40,76.84,28.88,77.35
TIMEZONE=Asia/Kolkata

# ── DuckDB ────────────────────────────────────────────────────────────────────
DUCKDB_PATH=data/delhi_air_quality.duckdb

# ── MLflow ────────────────────────────────────────────────────────────────────
MLFLOW_TRACKING_URI=http://localhost:5000
MLFLOW_MODEL_URI=models:/DelhiAirQualityModel/Production
MODEL_VERSION=2.0.0

# ── API ───────────────────────────────────────────────────────────────────────
API_PORT=8000

# ── Supprimer / ne plus utiliser ──────────────────────────────────────────────
# WAQI_API_KEY=...     ← supprimer
# CASABLANCA_...       ← supprimer toutes les variables préfixées Casablanca
```

---

## Étape 3 — Pipeline dlt

### `pipeline/ingestion.py` — créer ou remplacer entièrement

```python
"""
Pipeline dlt — Delhi Air Quality via OpenAQ API v3
Ingère les données des 35 stations de Delhi dans DuckDB.
"""
import dlt
import requests
import os
from datetime import datetime, timezone
from typing import Iterator

OPENAQ_BASE_URL = "https://api.openaq.org/v3"
OPENAQ_API_KEY  = os.getenv("OPENAQ_API_KEY", "")
DUCKDB_PATH     = os.getenv("DUCKDB_PATH", "data/delhi_air_quality.duckdb")

HEADERS = {
    "X-API-Key": OPENAQ_API_KEY,
    "Accept": "application/json",
}


@dlt.resource(
    name="delhi_locations",
    write_disposition="replace",
    primary_key="id",
)
def fetch_delhi_locations() -> Iterator[dict]:
    """Récupère et stocke la liste complète des stations de Delhi."""
    resp = requests.get(
        f"{OPENAQ_BASE_URL}/locations",
        headers=HEADERS,
        params={"country": "IN", "city": "Delhi", "limit": 100},
        timeout=30,
    )
    resp.raise_for_status()
    for loc in resp.json().get("results", []):
        yield {
            "id":           loc["id"],
            "name":         loc.get("name"),
            "locality":     loc.get("locality"),
            "timezone":     loc.get("timezone"),
            "latitude":     loc.get("coordinates", {}).get("latitude"),
            "longitude":    loc.get("coordinates", {}).get("longitude"),
            "is_monitor":   loc.get("isMonitor"),
            "provider":     loc.get("provider", {}).get("name"),
            "parameters":   [s["parameter"]["name"] for s in loc.get("sensors", [])],
            "ingested_at":  datetime.now(timezone.utc).isoformat(),
        }


@dlt.resource(
    name="delhi_measurements",
    write_disposition="append",
    primary_key=["location_id", "parameter", "datetime_utc"],
)
def fetch_delhi_measurements(
    location_ids: list[int],
    parameters: list[str] = ["pm25", "pm10", "no2", "so2", "co", "o3"],
    limit: int = 1000,
) -> Iterator[dict]:
    """Mesures historiques — itère sur chaque station + chaque polluant."""
    for location_id in location_ids:
        for param in parameters:
            sensors_resp = requests.get(
                f"{OPENAQ_BASE_URL}/locations/{location_id}/sensors",
                headers=HEADERS, timeout=30,
            )
            if sensors_resp.status_code != 200:
                continue
            sensor_ids = [
                s["id"] for s in sensors_resp.json().get("results", [])
                if s.get("parameter", {}).get("name") == param
            ]
            for sensor_id in sensor_ids:
                meas_resp = requests.get(
                    f"{OPENAQ_BASE_URL}/sensors/{sensor_id}/measurements",
                    headers=HEADERS,
                    params={"limit": limit, "page": 1},
                    timeout=30,
                )
                if meas_resp.status_code != 200:
                    continue
                for m in meas_resp.json().get("results", []):
                    yield {
                        "location_id":    location_id,
                        "sensor_id":      sensor_id,
                        "parameter":      param,
                        "value":          m.get("value"),
                        "unit":           m.get("parameter", {}).get("units", "µg/m³"),
                        "datetime_utc":   m.get("period", {}).get("datetimeTo", {}).get("utc"),
                        "datetime_local": m.get("period", {}).get("datetimeTo", {}).get("local"),
                        "latitude":       m.get("coordinates", {}).get("latitude"),
                        "longitude":      m.get("coordinates", {}).get("longitude"),
                        "ingested_at":    datetime.now(timezone.utc).isoformat(),
                    }


@dlt.resource(
    name="delhi_latest",
    write_disposition="replace",
    primary_key=["location_id", "parameter"],
)
def fetch_delhi_latest() -> Iterator[dict]:
    """Snapshot temps réel — dernière valeur par station et par polluant."""
    resp = requests.get(
        f"{OPENAQ_BASE_URL}/locations",
        headers=HEADERS,
        params={"country": "IN", "city": "Delhi", "limit": 100},
        timeout=30,
    )
    resp.raise_for_status()
    for loc in resp.json().get("results", []):
        for sensor in loc.get("sensors", []):
            latest = sensor.get("latest", {})
            if not latest:
                continue
            yield {
                "location_id":   loc["id"],
                "location_name": loc.get("name"),
                "parameter":     sensor.get("parameter", {}).get("name"),
                "value":         latest.get("value"),
                "unit":          sensor.get("parameter", {}).get("units"),
                "datetime_utc":  latest.get("datetime", {}).get("utc"),
                "latitude":      loc.get("coordinates", {}).get("latitude"),
                "longitude":     loc.get("coordinates", {}).get("longitude"),
                "ingested_at":   datetime.now(timezone.utc).isoformat(),
            }


# ── Exécution ─────────────────────────────────────────────────────────────────

pipeline = dlt.pipeline(
    pipeline_name="delhi_air_quality",
    destination="duckdb",
    dataset_name="raw",
)

if __name__ == "__main__":
    import duckdb

    # 1. Stations
    print("→ Ingestion stations Delhi...")
    print(pipeline.run(fetch_delhi_locations()))

    # 2. Récupérer les IDs réels
    conn = duckdb.connect(DUCKDB_PATH)
    location_ids = [
        r[0] for r in conn.execute(
            "SELECT id FROM raw.delhi_locations LIMIT 35"
        ).fetchall()
    ]
    conn.close()
    print(f"  {len(location_ids)} stations trouvées")

    # 3. Mesures historiques
    print("→ Ingestion mesures...")
    print(pipeline.run(fetch_delhi_measurements(location_ids=location_ids)))

    # 4. Snapshot temps réel
    print("→ Ingestion latest...")
    print(pipeline.run(fetch_delhi_latest()))
```

---

## Étape 4 — Structure dbt

### Arborescence à créer

```
dbt_project/
├── dbt_project.yml
├── profiles.yml
├── sources.yml
├── models/
│   ├── staging/
│   │   └── stg_delhi_measurements.sql
│   ├── intermediate/
│   │   └── int_delhi_cleaned.sql
│   └── marts/
│       └── mart_delhi_hourly.sql
├── tests/
│   └── generic/
│       └── not_null_required_fields.yml
└── macros/
```

### `dbt_project/dbt_project.yml`

```yaml
name: delhi_air_quality
version: '1.0.0'
profile: delhi_aq
model-paths: ["models"]
test-paths: ["tests"]
macro-paths: ["macros"]
models:
  delhi_air_quality:
    staging:
      materialized: view
    intermediate:
      materialized: view
    marts:
      materialized: table
```

### `dbt_project/profiles.yml`

```yaml
delhi_aq:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: "data/delhi_air_quality.duckdb"
      threads: 4
```

### `dbt_project/sources.yml`

```yaml
version: 2
sources:
  - name: raw
    description: Données brutes ingérées par dlt depuis OpenAQ v3
    tables:
      - name: delhi_locations
        description: Métadonnées des 35 stations de Delhi
      - name: delhi_measurements
        description: Séries temporelles par capteur et polluant
      - name: delhi_latest
        description: Snapshot temps réel
```

### `dbt_project/models/staging/stg_delhi_measurements.sql`

```sql
-- Staging : nettoyage, typage, flags qualité, features temporelles

with source as (
    select * from {{ source('raw', 'delhi_measurements') }}
),

cleaned as (
    select
        location_id,
        sensor_id,
        parameter,
        value,
        unit,
        cast(datetime_utc   as timestamp) as datetime_utc,
        cast(datetime_local as timestamp) as datetime_local,
        latitude,
        longitude,
        ingested_at,

        -- Flag qualité
        case
            when value is null                              then false
            when value < 0                                  then false
            when parameter = 'pm25' and value > 999        then false
            when parameter = 'pm10' and value > 999        then false
            when parameter = 'no2'  and value > 2000       then false
            when parameter = 'so2'  and value > 2000       then false
            when parameter = 'co'   and value > 50         then false
            when parameter = 'o3'   and value > 600        then false
            else true
        end as is_valid,

        -- Features temporelles IST
        extract(hour  from cast(datetime_local as timestamp)) as hour_local,
        extract(dow   from cast(datetime_local as timestamp)) as day_of_week,
        extract(month from cast(datetime_local as timestamp)) as month,

        -- Saisons indiennes (impact fort sur la pollution à Delhi)
        case
            when extract(month from cast(datetime_local as timestamp))
                 in (12, 1, 2)    then 'winter'       -- pollution maximale
            when extract(month from cast(datetime_local as timestamp))
                 in (3, 4, 5)     then 'summer'
            when extract(month from cast(datetime_local as timestamp))
                 in (6, 7, 8, 9)  then 'monsoon'      -- pollution minimale
            else                       'post_monsoon'
        end as season

    from source
    where value is not null
)

select * from cleaned
```

### `dbt_project/models/marts/mart_delhi_hourly.sql`

```sql
-- Mart horaire : pivot polluants + calcul AQI NAQI

with base as (
    select * from {{ ref('stg_delhi_measurements') }}
    where is_valid = true
),

pivoted as (
    select
        location_id,
        date_trunc('hour', datetime_utc) as hour_utc,
        hour_local,
        day_of_week,
        month,
        season,
        latitude,
        longitude,
        avg(case when parameter = 'pm25' then value end) as pm25,
        avg(case when parameter = 'pm10' then value end) as pm10,
        avg(case when parameter = 'no2'  then value end) as no2,
        avg(case when parameter = 'so2'  then value end) as so2,
        avg(case when parameter = 'co'   then value end) as co,
        avg(case when parameter = 'o3'   then value end) as o3,
        count(*) as measurement_count
    from base
    group by 1, 2, 3, 4, 5, 6, 7, 8
),

with_aqi as (
    select
        *,
        -- AQI NAQI basé sur PM2.5 (standard indien)
        case
            when pm25 is null  then null
            when pm25 <= 30    then round((pm25 / 30.0) * 50)
            when pm25 <= 60    then round(50  + ((pm25 - 30)  / 30.0)  * 50)
            when pm25 <= 90    then round(100 + ((pm25 - 60)  / 30.0)  * 100)
            when pm25 <= 120   then round(200 + ((pm25 - 90)  / 30.0)  * 100)
            when pm25 <= 250   then round(300 + ((pm25 - 120) / 130.0) * 100)
            else 400
        end as aqi_pm25,

        case
            when pm25 <= 30  then 'Good'
            when pm25 <= 60  then 'Satisfactory'
            when pm25 <= 90  then 'Moderate'
            when pm25 <= 120 then 'Poor'
            when pm25 <= 250 then 'Very Poor'
            else 'Severe'
        end as aqi_category

    from pivoted
)

select * from with_aqi
```

### `dbt_project/tests/generic/not_null_required_fields.yml`

```yaml
version: 2
models:
  - name: stg_delhi_measurements
    columns:
      - name: location_id
        tests: [not_null]
      - name: parameter
        tests: [not_null, accepted_values: {values: [pm25, pm10, no2, so2, co, o3, bc]}]
      - name: datetime_utc
        tests: [not_null]
      - name: is_valid
        tests: [not_null]
  - name: mart_delhi_hourly
    columns:
      - name: hour_utc
        tests: [not_null]
      - name: season
        tests:
          - accepted_values:
              values: [winter, summer, monsoon, post_monsoon]
```

---

## Étape 5 — Orchestration Dagster

Supprimer le dossier `airflow/` et créer `dagster_pipeline/`.

### `dagster_pipeline/__init__.py`

```python
"""
Dagster pipeline — Delhi Air Quality
Orchestration : dlt → dbt → ML → MLflow
"""
import subprocess
import sys
from dagster import (
    asset, AssetExecutionContext,
    define_asset_job, ScheduleDefinition, Definitions,
)


@asset(group_name="ingestion", description="Ingestion OpenAQ → DuckDB via dlt")
def ingest_delhi_data(context: AssetExecutionContext):
    result = subprocess.run(
        [sys.executable, "pipeline/ingestion.py"],
        capture_output=True, text=True,
    )
    context.log.info(result.stdout)
    if result.returncode != 0:
        raise Exception(result.stderr)
    return {"status": "ok"}


@asset(group_name="transform", deps=["ingest_delhi_data"],
       description="Transformations dbt (run + test)")
def run_dbt(context: AssetExecutionContext):
    for cmd in [
        ["dbt", "run",  "--project-dir", "dbt_project", "--profiles-dir", "dbt_project"],
        ["dbt", "test", "--project-dir", "dbt_project", "--profiles-dir", "dbt_project"],
    ]:
        result = subprocess.run(cmd, capture_output=True, text=True)
        context.log.info(result.stdout)
        if result.returncode != 0:
            raise Exception(result.stderr)
    return {"status": "ok"}


@asset(group_name="ml", deps=["run_dbt"],
       description="Entraînement modèle ML + tracking MLflow")
def train_model(context: AssetExecutionContext):
    result = subprocess.run(
        [sys.executable, "ml/train.py"],
        capture_output=True, text=True,
    )
    context.log.info(result.stdout)
    if result.returncode != 0:
        raise Exception(result.stderr)
    return {"status": "ok"}


daily_job = define_asset_job(
    name="delhi_daily_pipeline",
    selection=["ingest_delhi_data", "run_dbt", "train_model"],
)

daily_schedule = ScheduleDefinition(
    job=daily_job,
    cron_schedule="0 1 * * *",  # 1h IST (minuit UTC → 5h30 IST)
)

defs = Definitions(
    assets=[ingest_delhi_data, run_dbt, train_model],
    jobs=[daily_job],
    schedules=[daily_schedule],
)
```

### `dagster_pipeline/workspace.yaml`

```yaml
load_from:
  - python_package:
      package_name: dagster_pipeline
```

---

## Étape 6 — Dockerfile

Le fichier actuel est vide (0 octets). Remplacer entièrement :

```dockerfile
FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir \
    fastapi uvicorn[standard] pydantic \
    mlflow scikit-learn xgboost lightgbm \
    duckdb numpy pandas python-dotenv joblib

COPY api/ ./api/
COPY ml/  ./ml/

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:8000/health || exit 1

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

---

## Étape 7 — FastAPI

### `api/main.py` — créer ou remplacer

```python
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
```

---

## Étape 8 — CI/CD GitHub Actions

### `.github/workflows/ci.yml` — créer

```yaml
name: CI Pipeline

on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
          cache: pip

      - name: Install dependencies
        run: |
          pip install pytest pytest-cov httpx fastapi pydantic \
                      duckdb dlt scikit-learn mlflow numpy pandas \
                      python-dotenv

      - name: Run tests
        run: pytest tests/ -v --cov=api --cov=ml --cov-report=xml

      - name: dbt compile check
        run: |
          pip install dbt-duckdb
          dbt compile \
            --project-dir dbt_project \
            --profiles-dir dbt_project

  build-docker:
    runs-on: ubuntu-latest
    needs: test
    steps:
      - uses: actions/checkout@v4

      - name: Build Docker image
        run: docker build -t delhi-air-quality-api:${{ github.sha }} .

      - name: Test Docker health endpoint
        run: |
          docker run -d --name test-api -p 8000:8000 \
            delhi-air-quality-api:${{ github.sha }}
          sleep 10
          curl -f http://localhost:8000/health
          docker stop test-api

  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      - name: Lint with ruff
        run: |
          pip install ruff
          ruff check api/ ml/ pipeline/ dagster_pipeline/
```

---

## Étape 9 — Qualité des données

### `data_contracts/air_quality_contract.yaml` — créer

```yaml
apiVersion: v1
kind: DataContract
metadata:
  name: delhi-air-quality-raw
  version: 1.0.0
  owner: team-delhi-aq
  description: Données qualité air Delhi — 35 stations via OpenAQ API v3
  source:
    name: OpenAQ
    url: https://api.openaq.org/v3
    license: US Public Domain / CC-BY
    country: IN
    city: Delhi

schema:
  fields:
    - name: location_id
      type: integer
      required: true
    - name: sensor_id
      type: integer
      required: true
    - name: parameter
      type: string
      required: true
      enum: [pm25, pm10, no2, so2, co, o3, bc]
    - name: value
      type: float
      required: true
    - name: unit
      type: string
      required: true
      enum: ["µg/m³", "ppm", "ppb"]
    - name: datetime_utc
      type: timestamp
      required: true
      timezone: UTC
    - name: datetime_local
      type: timestamp
      required: false
      timezone: Asia/Kolkata

quality:
  completeness:
    - field: datetime_utc
      threshold: 100%
    - field: value
      threshold: 90%
  validity:
    - rule: pm25_range
      expression: "parameter != 'pm25' OR (value >= 0 AND value <= 999)"
    - rule: no_negatives
      expression: "value >= 0"
  freshness:
    max_delay: 3h
    check_table: delhi_latest
    check_field: datetime_utc
  integrity:
    min_stations: 10
    expected_parameters: [pm25, pm10, no2, co]
```

### `tests/test_data_quality.py` — créer

```python
"""
Tests qualité des données — Delhi Air Quality
Couvre : Complétude, Validité, Cohérence, Intégrité, Fraîcheur (règles §4)
"""
import duckdb
import pytest
import os
from datetime import datetime, timedelta, timezone

DB_PATH = os.getenv("DUCKDB_PATH", "data/delhi_air_quality.duckdb")


@pytest.fixture
def conn():
    con = duckdb.connect(DB_PATH, read_only=True)
    yield con
    con.close()


# ── Complétude ────────────────────────────────────────────────────────────────

def test_completeness_datetime(conn):
    """datetime_utc jamais NULL."""
    n = conn.execute(
        "SELECT COUNT(*) FROM raw.delhi_measurements WHERE datetime_utc IS NULL"
    ).fetchone()[0]
    assert n == 0

def test_completeness_ratio(conn):
    """Au moins 90% de valeurs non nulles."""
    total, nulls = conn.execute(
        "SELECT COUNT(*), SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) "
        "FROM raw.delhi_measurements"
    ).fetchone()
    assert (total - nulls) / total >= 0.90 if total > 0 else True

# ── Validité ──────────────────────────────────────────────────────────────────

def test_validity_no_negatives(conn):
    """Aucune mesure négative."""
    n = conn.execute(
        "SELECT COUNT(*) FROM raw.delhi_measurements WHERE value < 0"
    ).fetchone()[0]
    assert n == 0

def test_validity_pm25_range(conn):
    """PM2.5 : 0–999 µg/m³ (Delhi peut atteindre 900+ en hiver)."""
    n = conn.execute(
        "SELECT COUNT(*) FROM raw.delhi_measurements "
        "WHERE parameter = 'pm25' AND (value < 0 OR value > 999)"
    ).fetchone()[0]
    assert n == 0

def test_validity_aqi_category(conn):
    """Catégories AQI valides dans mart_delhi_hourly."""
    bad = conn.execute(
        "SELECT COUNT(*) FROM mart_delhi_hourly "
        "WHERE aqi_category NOT IN "
        "('Good','Satisfactory','Moderate','Poor','Very Poor','Severe')"
    ).fetchone()[0]
    assert bad == 0

# ── Intégrité ─────────────────────────────────────────────────────────────────

def test_integrity_min_stations(conn):
    """Au moins 10 stations ingérées."""
    n = conn.execute(
        "SELECT COUNT(DISTINCT location_id) FROM raw.delhi_measurements"
    ).fetchone()[0]
    assert n >= 10, f"Seulement {n} stations"

def test_integrity_parameters(conn):
    """PM2.5, PM10, NO2, CO présents."""
    params = {r[0] for r in conn.execute(
        "SELECT DISTINCT parameter FROM raw.delhi_measurements"
    ).fetchall()}
    missing = {"pm25", "pm10", "no2", "co"} - params
    assert not missing, f"Paramètres manquants : {missing}"

# ── Fraîcheur ─────────────────────────────────────────────────────────────────

def test_freshness_latest(conn):
    """Snapshot latest < 3h."""
    last = conn.execute(
        "SELECT MAX(datetime_utc) FROM raw.delhi_latest"
    ).fetchone()[0]
    if last:
        delay = datetime.now(timezone.utc) - last.replace(tzinfo=timezone.utc)
        assert delay < timedelta(hours=3), f"Données trop anciennes : {delay}"

# ── Cohérence ─────────────────────────────────────────────────────────────────

def test_coherence_winter_worse_than_monsoon(conn):
    """PM2.5 hiver > PM2.5 mousson (pattern Delhi connu)."""
    row = conn.execute("""
        SELECT
            avg(CASE WHEN season = 'winter'  THEN pm25 END),
            avg(CASE WHEN season = 'monsoon' THEN pm25 END)
        FROM mart_delhi_hourly
        WHERE pm25 IS NOT NULL
    """).fetchone()
    if row[0] and row[1]:
        assert row[0] > row[1], "Hiver devrait être plus pollué que mousson"
```

---

## Étape 10 — Documentation

### `docs/data_lineage.md` — créer

```markdown
# Data Lineage — Delhi Air Quality

## Flux de données

OpenAQ API v3 (35 stations Delhi, Inde)
  → dlt ingestion (3 resources)
      ├── delhi_locations    (métadonnées stations, replace)
      ├── delhi_measurements (séries temporelles, append)
      └── delhi_latest       (snapshot temps réel, replace)
  → DuckDB : schema raw
  → dbt staging : stg_delhi_measurements
      (nettoyage, flags qualité, features IST, saisons indiennes)
  → dbt intermediate : int_delhi_cleaned
      (déduplication, enrichissement)
  → dbt marts : mart_delhi_hourly
      (pivot polluants, AQI NAQI, catégories)
  → Dagster (orchestration quotidienne 1h UTC)
  → ML pipeline : ml/train.py
      (features : pm25, pm10, no2, so2, co, o3, hour_local, day_of_week, month, season_*)
  → MLflow Tracking (expériences, métriques, artefacts)
  → MLflow Registry : DelhiAirQualityModel / Production
  → FastAPI POST /predict (AQI prédit + catégorie NAQI)
  → Docker (conteneur API)
  → Grafana (disponibilité, temps de réponse, métriques ML, data drift)
```

### `README.md` — remplacer entièrement

````markdown
# Delhi Air Quality — MLOps Project

Prédiction de l'AQI pour Delhi (Inde) via un pipeline MLOps/DataOps complet.  
Source : **OpenAQ API v3** — 35 stations officielles, données depuis 2016.

## Architecture

```
OpenAQ API v3 → dlt → DuckDB → dbt → Dagster → ML (MLflow) → FastAPI → Docker → Grafana
                                                      ↑
                                               GitHub Actions CI/CD
```

## Stack

| Couche | Outil |
|--------|-------|
| Source données | OpenAQ API v3 (gratuit) |
| Ingestion | dlt |
| Stockage local | DuckDB |
| Transformations | dbt |
| Orchestration | Dagster |
| ML Tracking | MLflow |
| API | FastAPI |
| Conteneurisation | Docker |
| CI/CD | GitHub Actions |
| Monitoring | Grafana |

## Quickstart

```bash
# 1. Clé API OpenAQ gratuite sur explore.openaq.org
cp .env.example .env
# Éditer .env et renseigner OPENAQ_API_KEY

# 2. Installer
pip install -r requirements.txt

# 3. Ingestion Delhi
python pipeline/ingestion.py

# 4. Transformations dbt
dbt run --project-dir dbt_project --profiles-dir dbt_project

# 5. Dagster UI — http://localhost:3000
dagster dev -f dagster_pipeline/__init__.py

# 6. API ML — http://localhost:8000
uvicorn api.main:app --reload

# 7. Tout via Docker Compose
docker-compose up
```

## Endpoints API

| Méthode | Endpoint | Description |
|---------|----------|-------------|
| GET | `/health` | Santé du service |
| POST | `/predict` | Prédiction AQI Delhi (NAQI) |

## Tests

```bash
# Tests unitaires + qualité données
pytest tests/ -v

# Tests dbt
dbt test --project-dir dbt_project --profiles-dir dbt_project
```

## Données Delhi

- **35 stations** à Delhi et région NCR
- **Polluants** : PM2.5, PM10, NO2, SO2, CO, O3
- **Historique** : depuis 2016, fréquence horaire
- **AQI** : standard indien NAQI (Good → Satisfactory → Moderate → Poor → Very Poor → Severe)
````

---

## Notes finales pour Claude Code

### À conserver sans modification
- `ml/` et `mlops/` — le code ML existant est valide (réentraîner après migration)
- `grafana/` — dashboards existants (adapter les datasources si besoin)
- `notebooks/` — ne pas modifier
- Service PostgreSQL dans `docker-compose.yml` — peut coexister avec DuckDB

### Renommages à propager partout
| Avant | Après |
|-------|-------|
| `casablanca_aq.duckdb` | `delhi_air_quality.duckdb` |
| pipeline `casablanca_air_quality` | `delhi_air_quality` |
| modèle MLflow `AirQualityModel` | `DelhiAirQualityModel` |
| image Docker `casablanca-air-quality-api` | `delhi-air-quality-api` |

### Réentraînement obligatoire
Les features ML changent (ajout `so2`, `o3`, `season_*`) — le modèle existant n'est plus compatible. Après la migration, lancer `python ml/train.py` pour produire un nouveau modèle à enregistrer dans MLflow sous `DelhiAirQualityModel`.