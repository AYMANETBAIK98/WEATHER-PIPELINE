-- =============================================================
-- Weather ETL - warehouse schema
-- Layers:
--   raw        : API responses stored untouched (JSONB) -> replayable
--   analytics  : cleaned, typed, modeled tables (dimension + fact)
--   mart       : business-ready views
--   monitoring : pipeline run log + data quality results
-- The script is idempotent: safe to run on every deployment.
-- =============================================================

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS mart;
CREATE SCHEMA IF NOT EXISTS monitoring;

-- ---------- RAW ----------------------------------------------
CREATE TABLE IF NOT EXISTS raw.open_meteo_responses (
    response_id   BIGSERIAL PRIMARY KEY,
    city_code     TEXT        NOT NULL,
    run_date      DATE        NOT NULL,          -- the day the data describes
    request_url   TEXT        NOT NULL,
    payload       JSONB       NOT NULL,
    extracted_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_raw_city_date
    ON raw.open_meteo_responses (city_code, run_date);

-- ---------- ANALYTICS ----------------------------------------
CREATE TABLE IF NOT EXISTS analytics.dim_city (
    city_id     SERIAL PRIMARY KEY,
    city_code   TEXT UNIQUE NOT NULL,
    city_name   TEXT        NOT NULL,
    country     TEXT        NOT NULL,
    latitude    NUMERIC(8,4) NOT NULL,
    longitude   NUMERIC(8,4) NOT NULL,
    timezone    TEXT        NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS analytics.fact_weather_hourly (
    city_id              INT          NOT NULL REFERENCES analytics.dim_city(city_id),
    observed_at_utc      TIMESTAMPTZ  NOT NULL,
    observed_date_utc    DATE         NOT NULL,
    temperature_c        NUMERIC(5,2),
    apparent_temp_c      NUMERIC(5,2),
    relative_humidity    SMALLINT,
    precipitation_mm     NUMERIC(6,2),
    wind_speed_kmh       NUMERIC(6,2),
    weather_code         SMALLINT,
    loaded_at            TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (city_id, observed_at_utc)      -- natural key => idempotent upserts
);
CREATE INDEX IF NOT EXISTS ix_fact_date
    ON analytics.fact_weather_hourly (observed_date_utc);

-- ---------- MONITORING ---------------------------------------
CREATE TABLE IF NOT EXISTS monitoring.pipeline_runs (
    run_id        BIGSERIAL PRIMARY KEY,
    run_date      DATE        NOT NULL,
    step          TEXT        NOT NULL,
    status        TEXT        NOT NULL,          -- success / failed
    rows_affected INT,
    message       TEXT,
    logged_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS monitoring.dq_results (
    check_id      BIGSERIAL PRIMARY KEY,
    run_date      DATE        NOT NULL,
    check_name    TEXT        NOT NULL,
    severity      TEXT        NOT NULL,          -- error (blocks) / warn (logs only)
    passed        BOOLEAN     NOT NULL,
    failing_rows  INT         NOT NULL,
    details       TEXT,
    checked_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------- MART ---------------------------------------------
CREATE OR REPLACE VIEW mart.daily_city_weather AS
SELECT
    c.city_name,
    c.country,
    f.observed_date_utc                     AS day,
    ROUND(MIN(f.temperature_c), 1)          AS temp_min_c,
    ROUND(MAX(f.temperature_c), 1)          AS temp_max_c,
    ROUND(AVG(f.temperature_c), 1)          AS temp_avg_c,
    ROUND(AVG(f.relative_humidity), 0)      AS humidity_avg_pct,
    ROUND(SUM(f.precipitation_mm), 1)       AS precipitation_total_mm,
    ROUND(MAX(f.wind_speed_kmh), 1)         AS wind_max_kmh,
    COUNT(*)                                AS hours_recorded
FROM analytics.fact_weather_hourly f
JOIN analytics.dim_city c USING (city_id)
GROUP BY c.city_name, c.country, f.observed_date_utc;

CREATE OR REPLACE VIEW mart.city_temperature_ranking AS
SELECT
    day,
    city_name,
    temp_avg_c,
    RANK() OVER (PARTITION BY day ORDER BY temp_avg_c DESC) AS warmest_rank
FROM mart.daily_city_weather;
