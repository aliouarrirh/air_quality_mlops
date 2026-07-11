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
