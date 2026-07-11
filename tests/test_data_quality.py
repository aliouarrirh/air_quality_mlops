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
