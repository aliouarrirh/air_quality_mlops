-- =============================================================
-- init.sql — Delhi Air Quality — schéma + données de démo
-- =============================================================

CREATE TABLE IF NOT EXISTS predictions (
    id           SERIAL PRIMARY KEY,
    datetime     TIMESTAMPTZ NOT NULL UNIQUE,
    pm25_observe FLOAT,
    pm25_predit  FLOAT,
    aqi_predit   FLOAT,
    aqi_category VARCHAR(20),
    depasse_naqi BOOLEAN DEFAULT FALSE,
    inserted_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pred_datetime ON predictions(datetime DESC);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id               SERIAL PRIMARY KEY,
    run_at           TIMESTAMPTZ DEFAULT NOW(),
    statut           VARCHAR(10),
    nouvelles_lignes INT,
    pm25_actuel      FLOAT,
    message          TEXT
);

-- ── Données de démo : 72h (3 jours × 24h) de mesures Delhi ──────────────────
-- Hiver Delhi : PM2.5 typiquement 80-350 µg/m³ (pics le matin et le soir)
INSERT INTO predictions (datetime, pm25_observe, pm25_predit, aqi_predit, aqi_category, depasse_naqi)
SELECT
    NOW() - (n || ' hours')::interval AS datetime,
    -- PM2.5 observé : cycle diurne + bruit
    ROUND((
        150
        + 80 * SIN(RADIANS((n % 24) * 15 - 90))   -- pic à 6h et 20h
        + 40 * RANDOM()
        - 20
    )::numeric, 1) AS pm25_observe,
    -- PM2.5 prédit par XGBoost (légèrement différent)
    ROUND((
        148
        + 78 * SIN(RADIANS((n % 24) * 15 - 90))
        + 30 * RANDOM()
        - 10
    )::numeric, 1) AS pm25_predit,
    -- AQI prédit (corrélé au PM2.5)
    ROUND((
        160
        + 85 * SIN(RADIANS((n % 24) * 15 - 90))
        + 35 * RANDOM()
        - 15
    )::numeric, 1) AS aqi_predit,
    -- Catégorie NAQI
    CASE
        WHEN (150 + 80 * SIN(RADIANS((n % 24) * 15 - 90))) <= 50  THEN 'Good'
        WHEN (150 + 80 * SIN(RADIANS((n % 24) * 15 - 90))) <= 100 THEN 'Satisfactory'
        WHEN (150 + 80 * SIN(RADIANS((n % 24) * 15 - 90))) <= 200 THEN 'Moderate'
        WHEN (150 + 80 * SIN(RADIANS((n % 24) * 15 - 90))) <= 300 THEN 'Poor'
        WHEN (150 + 80 * SIN(RADIANS((n % 24) * 15 - 90))) <= 400 THEN 'Very Poor'
        ELSE 'Severe'
    END AS aqi_category,
    -- Dépasse NAQI si > 200 µg/m³
    (150 + 80 * SIN(RADIANS((n % 24) * 15 - 90))) > 200 AS depasse_naqi
FROM generate_series(0, 71) AS n
ON CONFLICT (datetime) DO NOTHING;

-- ── Logs pipeline ─────────────────────────────────────────────────────────────
INSERT INTO pipeline_runs (run_at, statut, nouvelles_lignes, pm25_actuel, message) VALUES
    (NOW() - '2 hours'::interval,  'OK',    24, 187.3, 'Ingestion CSV → DuckDB → dbt → 24 nouvelles mesures'),
    (NOW() - '14 hours'::interval, 'OK',    24, 234.1, 'Ingestion CSV → DuckDB → dbt → 24 nouvelles mesures'),
    (NOW() - '26 hours'::interval, 'OK',    24, 312.7, 'Ingestion CSV → DuckDB → dbt → 24 nouvelles mesures'),
    (NOW() - '38 hours'::interval, 'OK',    24, 156.9, 'Ingestion CSV → DuckDB → dbt → 24 nouvelles mesures'),
    (NOW() - '50 hours'::interval, 'WARN',   0,   NULL, 'Aucune nouvelle donnée source')
ON CONFLICT DO NOTHING;
