-- =============================================================
-- init.sql — Tables du projet Delhi Air Quality
-- La DB delhi_aq est déjà créée par POSTGRES_DB dans Docker
-- On se connecte directement et on crée les tables
-- =============================================================

-- Table 1 : observations réelles (après ETL temps réel)
CREATE TABLE IF NOT EXISTS observations_reelles (
    id          SERIAL PRIMARY KEY,
    datetime    TIMESTAMPTZ NOT NULL UNIQUE,
    pm2p5       FLOAT,
    pm10        FLOAT,
    no2         FLOAT,
    so2         FLOAT,
    co          FLOAT,
    o3          FLOAT,
    temp_c      FLOAT,
    vent_kmh    FLOAT,
    blh_m       FLOAT,
    inserted_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_obs_datetime ON observations_reelles(datetime DESC);

-- Table 2 : prédictions + AQI NAQI
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

-- Table 3 : journal des runs pipeline
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id              SERIAL PRIMARY KEY,
    run_at          TIMESTAMPTZ DEFAULT NOW(),
    statut          VARCHAR(10),
    nouvelles_lignes INT,
    pm25_actuel     FLOAT,
    message         TEXT
);
