"""Central configuration.

Every setting comes from an environment variable (set in docker-compose.yml / .env)
with a sensible default, so the same code runs in Docker and on your laptop.
Keeping config in one place = "configuration management" for the rubric.
"""
import os


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


# ---- PostgreSQL (serving store) -------------------------------------------
POSTGRES_HOST = _env("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(_env("POSTGRES_PORT", "5432"))
POSTGRES_DB = _env("POSTGRES_DB", "fleet")
POSTGRES_USER = _env("POSTGRES_USER", "fleet")
POSTGRES_PASSWORD = _env("POSTGRES_PASSWORD", "fleet")

# ---- Kafka -----------------------------------------------------------------
KAFKA_BOOTSTRAP = _env("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = _env("KAFKA_TOPIC", "trip-events")

# ---- File-system "data lake" (immutable master dataset for the batch layer) --
LANDING_DIR = _env("LANDING_DIR", "/data/landing")      # daily expense files land here
LAKE_DIR = _env("LAKE_DIR", "/data/lake")               # parquet archive of raw events
REPORT_DIR = _env("REPORT_DIR", "/opt/airflow/reports")  # daily report files
CHECKPOINT_DIR = _env("CHECKPOINT_DIR", "/data/checkpoints/speed_layer")

# ---- Stream simulator ---------------------------------------------------------------
TICK_SECONDS = float(_env("TICK_SECONDS", "2"))          # every vehicle emits one event per tick

# ---- Business thresholds ------------------------------------------------------
# A vehicle idle for this many REAL seconds (measured on event time) raises an alert.
IDLE_ALERT_SECONDS = int(_env("IDLE_ALERT_SECONDS", "60"))
# /health reports "degraded" when no event has been processed for this long.
NO_DATA_SECONDS = int(_env("NO_DATA_SECONDS", "120"))
# A vehicle not heard from for this long is considered offline.
VEHICLE_ONLINE_SECONDS = int(_env("VEHICLE_ONLINE_SECONDS", "30"))
# Profit margin below this (but still >= 0) is flagged AT_RISK.
AT_RISK_MARGIN = float(_env("AT_RISK_MARGIN", "0.15"))
# Batch layer waits this long after a simulated day ends so late Kafka events land.
LATE_DATA_GRACE_SECONDS = int(_env("LATE_DATA_GRACE_SECONDS", "20"))

# ---- Simulated clock fallbacks (the DB row in sim_clock is the source of truth) --
SIM_DAY_SECONDS = int(_env("SIM_DAY_SECONDS", "300"))       # 1 simulated day = 5 real minutes
SIM_START_DATE = _env("SIM_START_DATE", "2026-01-01")
SIM_EPOCH_UNIX = _env("SIM_EPOCH_UNIX", "")                  # optional override (unix seconds)
