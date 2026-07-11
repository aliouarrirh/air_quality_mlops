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
