-- =============================================================
-- init.sql — Delhi Air Quality — schéma monitoring complet
-- =============================================================

-- ── 1. Disponibilité + temps de réponse ───────────────────────────────────────
CREATE TABLE IF NOT EXISTS service_health (
    id              SERIAL PRIMARY KEY,
    datetime        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    endpoint        VARCHAR(20) NOT NULL,   -- /health | /predict
    is_available    BOOLEAN NOT NULL,
    response_ms     FLOAT,
    status_code     INT
);
CREATE INDEX IF NOT EXISTS idx_sh_datetime ON service_health(datetime DESC);

-- ── 2. Métriques ML (snapshot périodique depuis MLflow) ───────────────────────
CREATE TABLE IF NOT EXISTS ml_metrics (
    id           SERIAL PRIMARY KEY,
    datetime     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    model_name   VARCHAR(50) NOT NULL,
    metric_name  VARCHAR(20) NOT NULL,
    metric_value FLOAT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ml_datetime ON ml_metrics(datetime DESC);

-- ── 3. Dérive simple (distribution courante vs baseline) ──────────────────────
CREATE TABLE IF NOT EXISTS drift_metrics (
    id             SERIAL PRIMARY KEY,
    datetime       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    feature        VARCHAR(20) NOT NULL,
    current_mean   FLOAT,
    baseline_mean  FLOAT,
    drift_score    FLOAT,   -- |current - baseline| / baseline_std
    is_drift       BOOLEAN  -- vrai si drift_score > 2
);
CREATE INDEX IF NOT EXISTS idx_drift_datetime ON drift_metrics(datetime DESC);

-- ── 4. Prédictions (résultats du pipeline) ────────────────────────────────────
CREATE TABLE IF NOT EXISTS predictions (
    id           SERIAL PRIMARY KEY,
    datetime     TIMESTAMPTZ NOT NULL UNIQUE,
    pm25_observe FLOAT,
    pm25_predit  FLOAT,
    aqi_predit   FLOAT,
    aqi_category VARCHAR(20),
    depasse_naqi BOOLEAN DEFAULT FALSE
);
CREATE INDEX IF NOT EXISTS idx_pred_datetime ON predictions(datetime DESC);

-- ── Journal pipeline ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id               SERIAL PRIMARY KEY,
    run_at           TIMESTAMPTZ DEFAULT NOW(),
    statut           VARCHAR(10),
    nouvelles_lignes INT,
    pm25_actuel      FLOAT,
    message          TEXT
);

-- =============================================================
-- Données de démo
-- =============================================================

-- Service health : 72h de checks toutes les 15 min (~288 points)
INSERT INTO service_health (datetime, endpoint, is_available, response_ms, status_code)
SELECT
    NOW() - (n * 15 || ' minutes')::interval,
    CASE WHEN n % 2 = 0 THEN '/health' ELSE '/predict' END,
    CASE WHEN RANDOM() > 0.04 THEN true ELSE false END,   -- 96% uptime
    ROUND((80 + 120 * RANDOM() + CASE WHEN n % 47 = 0 THEN 800 ELSE 0 END)::numeric, 1),
    CASE WHEN RANDOM() > 0.04 THEN 200 ELSE 503 END
FROM generate_series(0, 287) AS n
ON CONFLICT DO NOTHING;

-- ML metrics : snapshot toutes les 6h sur 72h
INSERT INTO ml_metrics (datetime, model_name, metric_name, metric_value)
SELECT
    NOW() - (n * 6 || ' hours')::interval,
    'DelhiAirQualityModel',
    m.metric_name,
    ROUND((m.base + m.noise * RANDOM())::numeric, 4)
FROM generate_series(0, 11) AS n
CROSS JOIN (VALUES
    ('rmse', 38.32, 2.0),
    ('mae',  29.48, 1.5),
    ('r2',   0.192, 0.02)
) AS m(metric_name, base, noise)
ON CONFLICT DO NOTHING;

-- Drift : toutes les 6h, sur pm25/pm10/no2
INSERT INTO drift_metrics (datetime, feature, current_mean, baseline_mean, drift_score, is_drift)
SELECT
    NOW() - (n * 6 || ' hours')::interval,
    f.feature,
    ROUND((f.base_mean + f.variation * SIN(n * 0.5) + f.noise * RANDOM())::numeric, 2),
    f.base_mean,
    ROUND((ABS(f.variation * SIN(n * 0.5) + f.noise * RANDOM()) / f.baseline_std)::numeric, 3),
    ABS(f.variation * SIN(n * 0.5)) / f.baseline_std > 2
FROM generate_series(0, 11) AS n
CROSS JOIN (VALUES
    ('pm25',  150.0, 25.0, 15.0, 12.0),
    ('pm10',  210.0, 35.0, 20.0, 18.0),
    ('no2',    45.0, 10.0,  8.0,  5.0)
) AS f(feature, base_mean, variation, noise, baseline_std)
ON CONFLICT DO NOTHING;

-- Prédictions : 72h horaires
INSERT INTO predictions (datetime, pm25_observe, pm25_predit, aqi_predit, aqi_category, depasse_naqi)
SELECT
    NOW() - (n || ' hours')::interval,
    ROUND((150 + 80 * SIN(RADIANS((n % 24) * 15 - 90)) + 30 * RANDOM())::numeric, 1),
    ROUND((148 + 78 * SIN(RADIANS((n % 24) * 15 - 90)) + 25 * RANDOM())::numeric, 1),
    ROUND((160 + 85 * SIN(RADIANS((n % 24) * 15 - 90)) + 30 * RANDOM())::numeric, 1),
    CASE
        WHEN (150 + 80 * SIN(RADIANS((n % 24) * 15 - 90))) <= 50  THEN 'Good'
        WHEN (150 + 80 * SIN(RADIANS((n % 24) * 15 - 90))) <= 100 THEN 'Satisfactory'
        WHEN (150 + 80 * SIN(RADIANS((n % 24) * 15 - 90))) <= 200 THEN 'Moderate'
        WHEN (150 + 80 * SIN(RADIANS((n % 24) * 15 - 90))) <= 300 THEN 'Poor'
        WHEN (150 + 80 * SIN(RADIANS((n % 24) * 15 - 90))) <= 400 THEN 'Very Poor'
        ELSE 'Severe'
    END,
    (150 + 80 * SIN(RADIANS((n % 24) * 15 - 90))) > 200
FROM generate_series(0, 71) AS n
ON CONFLICT (datetime) DO NOTHING;

-- Pipeline runs
INSERT INTO pipeline_runs (run_at, statut, nouvelles_lignes, pm25_actuel, message) VALUES
    (NOW() - '2 hours'::interval,  'OK',   24, 187.3, 'Ingestion CSV → DuckDB → dbt → 24 mesures'),
    (NOW() - '14 hours'::interval, 'OK',   24, 234.1, 'Ingestion CSV → DuckDB → dbt → 24 mesures'),
    (NOW() - '26 hours'::interval, 'OK',   24, 312.7, 'Ingestion CSV → DuckDB → dbt → 24 mesures'),
    (NOW() - '38 hours'::interval, 'OK',   24, 156.9, 'Ingestion CSV → DuckDB → dbt → 24 mesures'),
    (NOW() - '50 hours'::interval, 'WARN',  0,   NULL, 'Aucune nouvelle donnée source')
ON CONFLICT DO NOTHING;
