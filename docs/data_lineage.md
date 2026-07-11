# Data Lineage — Delhi Air Quality

## Flux de données

```
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
