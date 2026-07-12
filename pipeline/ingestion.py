"""
Pipeline dlt — Delhi Air Quality via fichiers CSV locaux
Charge les données des stations Delhi depuis data/ dans DuckDB.
"""
import csv
import dlt
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

DATA_DIR    = Path(os.getenv("DATA_DIR", "data"))
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "data/delhi_air_quality.duckdb")

LOCATIONS_CSV    = DATA_DIR / "delhi_locations.csv"
MEASUREMENTS_CSV = DATA_DIR / "delhi_measurements.csv"
LATEST_CSV       = DATA_DIR / "delhi_latest.csv"


def _read_csv(path: Path) -> list[dict]:
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


@dlt.resource(name="delhi_locations", write_disposition="replace", primary_key="id")
def fetch_delhi_locations() -> Iterator[dict]:
    for row in _read_csv(LOCATIONS_CSV):
        yield {
            "id":          int(row["id"]),
            "name":        row["name"],
            "locality":    row["locality"],
            "timezone":    row["timezone"],
            "latitude":    float(row["latitude"]),
            "longitude":   float(row["longitude"]),
            "is_monitor":  row["is_monitor"] == "True",
            "provider":    row["provider"],
            "parameters":  row["parameters"].split("|"),
            "ingested_at": row["ingested_at"],
        }


@dlt.resource(
    name="delhi_measurements",
    write_disposition="replace",
    primary_key=["location_id", "parameter", "datetime_utc"],
)
def fetch_delhi_measurements() -> Iterator[dict]:
    for row in _read_csv(MEASUREMENTS_CSV):
        yield {
            "location_id":    int(row["location_id"]),
            "sensor_id":      int(row["sensor_id"]),
            "parameter":      row["parameter"],
            "value":          float(row["value"]),
            "unit":           row["unit"],
            "datetime_utc":   row["datetime_utc"],
            "datetime_local": row["datetime_local"],
            "latitude":       float(row["latitude"]) if row["latitude"] else None,
            "longitude":      float(row["longitude"]) if row["longitude"] else None,
            "ingested_at":    row["ingested_at"],
        }


@dlt.resource(
    name="delhi_latest",
    write_disposition="replace",
    primary_key=["location_id", "parameter"],
)
def fetch_delhi_latest() -> Iterator[dict]:
    for row in _read_csv(LATEST_CSV):
        yield {
            "location_id":   int(row["location_id"]),
            "location_name": row["location_name"],
            "parameter":     row["parameter"],
            "value":         float(row["value"]),
            "unit":          row["unit"],
            "datetime_utc":  row["datetime_utc"],
            "latitude":      float(row["latitude"]),
            "longitude":     float(row["longitude"]),
            "ingested_at":   row["ingested_at"],
        }


# ── Pipeline ──────────────────────────────────────────────────────────────────

os.makedirs(os.path.dirname(DUCKDB_PATH) or ".", exist_ok=True)

pipeline = dlt.pipeline(
    pipeline_name="delhi_air_quality",
    destination=dlt.destinations.duckdb(credentials=DUCKDB_PATH),
    dataset_name="raw",
)

if __name__ == "__main__":
    for csv_file in [LOCATIONS_CSV, MEASUREMENTS_CSV, LATEST_CSV]:
        if not csv_file.exists():
            print(f"[ERREUR] {csv_file} introuvable. Lance d'abord: python pipeline/seed_csv.py")
            raise SystemExit(1)

    print("-> Ingestion stations Delhi...")
    print(pipeline.run(fetch_delhi_locations()))

    print("-> Ingestion mesures...")
    print(pipeline.run(fetch_delhi_measurements()))

    print("-> Ingestion latest...")
    print(pipeline.run(fetch_delhi_latest()))

    print("\nDone. DuckDB pret dans:", DUCKDB_PATH)
