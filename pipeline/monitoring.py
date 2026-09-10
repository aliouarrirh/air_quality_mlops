"""
Collecte des métriques de monitoring → PostgreSQL.
Appelé par Dagster toutes les heures.
"""
import os, time, statistics
import requests
import psycopg2
from dotenv import load_dotenv

load_dotenv()

API_URL   = os.getenv("API_URL", "http://api:8000")
PG_DSN    = "host=postgres port=5432 dbname=delhi_aq user=postgres password=root"

BASELINE = {
    "pm25": {"mean": 150.0, "std": 45.0},
    "pm10": {"mean": 210.0, "std": 60.0},
    "no2":  {"mean":  45.0, "std": 15.0},
}

PREDICT_PAYLOAD = {
    "pm25": 145.0, "pm10": 205.0, "no2": 42.0,
    "so2": 18.0, "co": 1.2, "o3": 28.0,
    "hour_local": 10, "day_of_week": 2,
    "month": 1, "season": "winter",
}


def _check_endpoint(endpoint: str, method: str = "GET", json=None):
    url = f"{API_URL}{endpoint}"
    t0 = time.time()
    try:
        r = requests.request(method, url, json=json, timeout=5)
        ms = round((time.time() - t0) * 1000, 1)
        return r.status_code, ms, r.status_code < 400
    except Exception:
        ms = round((time.time() - t0) * 1000, 1)
        return 0, ms, False


def collect_service_health(cur):
    for endpoint, method, payload in [
        ("/health", "GET", None),
        ("/predict", "POST", PREDICT_PAYLOAD),
    ]:
        code, ms, ok = _check_endpoint(endpoint, method, payload)
        cur.execute(
            "INSERT INTO service_health (endpoint, is_available, response_ms, status_code) VALUES (%s,%s,%s,%s)",
            (endpoint, ok, ms, code),
        )
        print(f"[monitoring] health {endpoint}: status={code} {ms}ms ok={ok}")


def collect_ml_metrics(cur):
    import mlflow
    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
    try:
        client = mlflow.MlflowClient()
        experiments = client.search_experiments()
        if not experiments:
            return
        exp_ids = [e.experiment_id for e in experiments]
        runs = client.search_runs(
            experiment_ids=exp_ids, order_by=["start_time DESC"], max_results=1
        )
        if not runs:
            print("[monitoring] ml_metrics: aucun run MLflow trouve")
            return
        for k, v in runs[0].data.metrics.items():
            cur.execute(
                "INSERT INTO ml_metrics (model_name, metric_name, metric_value) VALUES (%s,%s,%s)",
                ("DelhiAirQualityModel", k, v),
            )
            print(f"[monitoring] ml_metric {k}={v}")
    except Exception as e:
        print(f"[monitoring] ml_metrics error: {e}")


def collect_drift(cur):
    DUCKDB_PATH = os.getenv("DUCKDB_PATH", "data/delhi_air_quality.duckdb")
    try:
        import duckdb
        con = duckdb.connect(DUCKDB_PATH, read_only=True)
        for feature, bl in BASELINE.items():
            row = con.execute(
                f"SELECT AVG(value) FROM raw.delhi_measurements "
                f"WHERE parameter=? AND datetime_utc >= NOW() - INTERVAL '24 hours'",
                [feature]
            ).fetchone()
            if not row or row[0] is None:
                print(f"[monitoring] drift {feature}: aucune donnee sur 24h")
                continue
            current_mean = row[0]
            score = abs(current_mean - bl["mean"]) / bl["std"]
            cur.execute(
                "INSERT INTO drift_metrics (feature, current_mean, baseline_mean, drift_score, is_drift) VALUES (%s,%s,%s,%s,%s)",
                (feature, round(current_mean, 2), bl["mean"], round(score, 3), score > 2),
            )
            print(f"[monitoring] drift {feature}: mean={current_mean:.2f} score={score:.3f}")
        con.close()
    except Exception as e:
        print(f"[monitoring] drift error: {e}")


def run():
    with psycopg2.connect(PG_DSN) as conn:
        with conn.cursor() as cur:
            collect_service_health(cur)
            collect_ml_metrics(cur)
            collect_drift(cur)
        conn.commit()
    print("[monitoring] collecte OK")


if __name__ == "__main__":
    run()
