-- =====================================================================================
-- Serving-layer schema (database: fleet).  Executed once, when the Postgres volume is created.
-- =====================================================================================

-- ---- Simulated clock: single source of truth shared by every container -------------
CREATE TABLE sim_clock (
    id          INT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    epoch_unix  DOUBLE PRECISION NOT NULL,      -- real unix time == simulated start_date 00:00
    day_seconds INT  NOT NULL,                  -- real seconds per simulated day
    start_date  DATE NOT NULL
);
INSERT INTO sim_clock (epoch_unix, day_seconds, start_date)
VALUES (EXTRACT(EPOCH FROM now()), 300, DATE '2026-01-01');   -- 1 simulated day = 5 real minutes

-- =====================================================================================
-- SPEED LAYER views (written continuously by the Spark Structured Streaming job)
-- =====================================================================================

-- Latest known state of every vehicle (one row per vehicle, upserted each micro-batch)
CREATE TABLE vehicle_state (
    vehicle_id TEXT PRIMARY KEY,
    driver_id  TEXT,
    status     TEXT NOT NULL,                   -- idle | enroute | on_trip
    lat        DOUBLE PRECISION,
    lon        DOUBLE PRECISION,
    speed      DOUBLE PRECISION,
    zone       TEXT,
    last_ts    TIMESTAMPTZ NOT NULL,            -- event time of the newest event
    idle_since TIMESTAMPTZ,                     -- start of the current continuous idle run
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 1-minute tumbling window per zone ("right now" view)
CREATE TABLE zone_metrics_1m (
    window_start   TIMESTAMPTZ NOT NULL,
    zone           TEXT NOT NULL,
    events         BIGINT NOT NULL DEFAULT 0,
    idle_events    BIGINT NOT NULL DEFAULT 0,
    enroute_events BIGINT NOT NULL DEFAULT 0,
    on_trip_events BIGINT NOT NULL DEFAULT 0,
    trips          BIGINT NOT NULL DEFAULT 0,   -- completed trips
    earnings       NUMERIC(14,2) NOT NULL DEFAULT 0,
    PRIMARY KEY (window_start, zone)
);

-- Time-of-day view: simulated date + simulated hour + zone
CREATE TABLE zone_hourly (
    sim_date    DATE NOT NULL,
    sim_hour    INT  NOT NULL,
    zone        TEXT NOT NULL,
    events      BIGINT NOT NULL DEFAULT 0,
    idle_events BIGINT NOT NULL DEFAULT 0,
    trips       BIGINT NOT NULL DEFAULT 0,
    earnings    NUMERIC(14,2) NOT NULL DEFAULT 0,
    PRIMARY KEY (sim_date, sim_hour, zone)
);

-- Per-vehicle running totals per simulated day (the speed layer's approximate answer)
CREATE TABLE vehicle_daily_rt (
    sim_date       DATE NOT NULL,
    vehicle_id     TEXT NOT NULL,
    events         BIGINT NOT NULL DEFAULT 0,
    idle_events    BIGINT NOT NULL DEFAULT 0,
    on_trip_events BIGINT NOT NULL DEFAULT 0,
    trips          BIGINT NOT NULL DEFAULT 0,
    earnings       NUMERIC(14,2) NOT NULL DEFAULT 0,
    PRIMARY KEY (sim_date, vehicle_id)
);

-- Alerts (idle vehicles). Only ONE open alert per (type, vehicle) thanks to the partial index.
CREATE TABLE alerts (
    alert_id    BIGSERIAL PRIMARY KEY,
    alert_type  TEXT NOT NULL,
    vehicle_id  TEXT NOT NULL,
    severity    TEXT NOT NULL DEFAULT 'warning',
    message     TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at TIMESTAMPTZ
);
CREATE UNIQUE INDEX one_open_alert_per_vehicle
    ON alerts (alert_type, vehicle_id) WHERE resolved_at IS NULL;

-- Ledger that makes micro-batch processing idempotent (exactly-once *effect*):
-- if Spark replays a batch after a crash, the batch_id is already here and is skipped.
CREATE TABLE processed_batches (
    query_name   TEXT NOT NULL,
    batch_id     BIGINT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (query_name, batch_id)
);

-- Cumulative counters per pipeline stage (for the health API and the report)
CREATE TABLE pipeline_stats (
    stage         TEXT PRIMARY KEY,
    records_in    BIGINT NOT NULL DEFAULT 0,
    records_ok    BIGINT NOT NULL DEFAULT 0,
    records_bad   BIGINT NOT NULL DEFAULT 0,
    last_batch_at TIMESTAMPTZ,
    last_event_ts TIMESTAMPTZ
);

-- =====================================================================================
-- BATCH LAYER tables (written by the Airflow DAG once per simulated day)
-- =====================================================================================

CREATE TABLE expenses_clean (
    sim_date         DATE NOT NULL,
    vehicle_id       TEXT NOT NULL,
    fuel_cost        NUMERIC(12,2) NOT NULL,
    maintenance_cost NUMERIC(12,2) NOT NULL,
    distance_covered NUMERIC(10,2) NOT NULL,
    service_flag     BOOLEAN NOT NULL,
    source_file      TEXT NOT NULL,
    loaded_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (sim_date, vehicle_id)
);

CREATE TABLE expenses_rejected (
    id          BIGSERIAL PRIMARY KEY,
    sim_date    DATE NOT NULL,
    source_file TEXT NOT NULL,
    raw_row     JSONB NOT NULL,
    reason      TEXT NOT NULL,
    rejected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Batch view: earnings recomputed from the immutable Parquet master dataset
CREATE TABLE vehicle_daily_batch (
    sim_date    DATE NOT NULL,
    vehicle_id  TEXT NOT NULL,
    events      BIGINT NOT NULL,
    idle_events BIGINT NOT NULL,
    trips       BIGINT NOT NULL,
    earnings    NUMERIC(14,2) NOT NULL,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (sim_date, vehicle_id)
);

-- FINAL business output: per-vehicle daily profitability reconciliation
CREATE TABLE daily_profitability (
    sim_date             DATE NOT NULL,
    vehicle_id           TEXT NOT NULL,
    trips                BIGINT,
    earnings             NUMERIC(14,2),
    fuel_cost            NUMERIC(12,2),
    maintenance_cost     NUMERIC(12,2),
    total_cost           NUMERIC(12,2),
    profit               NUMERIC(14,2),
    margin               NUMERIC(8,3),
    distance_covered     NUMERIC(10,2),
    cost_per_km          NUMERIC(10,2),
    service_flag         BOOLEAN,
    idle_ratio           NUMERIC(6,3),
    prev_profit_change   NUMERIC(14,2),
    declining            BOOLEAN NOT NULL DEFAULT FALSE,
    status               TEXT NOT NULL,   -- PROFITABLE | AT_RISK | UNPROFITABLE | NO_EXPENSE_DATA | NO_STREAM_DATA
    speed_layer_earnings NUMERIC(14,2),
    drift                NUMERIC(14,2),   -- batch earnings minus speed-layer earnings
    generated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (sim_date, vehicle_id)
);

CREATE TABLE daily_reports (
    sim_date     DATE PRIMARY KEY,
    html         TEXT NOT NULL,
    csv          TEXT NOT NULL,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One row per processed simulated day: the audit trail / metrics source for the batch layer
CREATE TABLE batch_runs (
    id            BIGSERIAL PRIMARY KEY,
    sim_date      DATE NOT NULL,
    status        TEXT NOT NULL,          -- success | failed
    rows_loaded   INT,
    rows_rejected INT,
    vehicles      INT,
    unprofitable  INT,
    max_abs_drift NUMERIC(14,2),
    message       TEXT,
    started_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at   TIMESTAMPTZ
);
CREATE INDEX batch_runs_day ON batch_runs (sim_date, status);
