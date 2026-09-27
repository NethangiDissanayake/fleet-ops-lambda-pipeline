"""SPEED LAYER - Spark Structured Streaming: Kafka  ->  clean / dedupe / enrich / aggregate  ->  PostgreSQL

Flow of one 10-second micro-batch
---------------------------------
  Kafka topic "trip-events"
     |  parse_events()       JSON -> typed columns, every row gets reject_reason (NULL = valid)
     |  watermark 2 min + dropDuplicates(event_key, event_time)      (producer retries -> duplicates)
     v
  foreachBatch(process_batch)
     |- invalid rows  -> counted per reason, archived in /data/lake/dead_letter   (never silently lost)
     |- valid rows    -> enrich() (zone, sim date/hour, earned)
     |- valid rows    -> Parquet master dataset  /data/lake/trip_events/...      (input for the BATCH layer)
     |- aggregates    -> PostgreSQL (one transaction, idempotent - see sinks.py)
     '- metrics/logs  -> Prometheus (:8003) + JSON logs

Run inside Docker (see docker-compose.yml, service `spark-streaming`).
"""
import os
import time
from datetime import timezone

os.environ["TZ"] = "UTC"     # Spark returns naive datetimes in the process time zone -> force UTC
time.tzset()

from prometheus_client import Counter, Gauge, Histogram, start_http_server
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common import config
from common.db import connect
from common.logging_utils import error, get_logger, info, warning
from streaming import sinks, transforms

SPARK_KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
TRIGGER_SECONDS = int(os.environ.get("TRIGGER_SECONDS", "10"))
MAX_OFFSETS = os.environ.get("MAX_OFFSETS_PER_TRIGGER", "20000")
METRICS_PORT = int(os.environ.get("METRICS_PORT", "8003"))

log = get_logger("spark-streaming", stage="processing")
log_sink = get_logger("spark-streaming", stage="storage")

# ---- Prometheus metrics ----------------------------------------------------------------------
EVENTS_RECEIVED = Counter("spark_events_received_total", "Rows read from Kafka (after de-duplication)")
EVENTS_VALID = Counter("spark_events_valid_total", "Rows that passed validation")
EVENTS_INVALID = Counter("spark_events_invalid_total", "Rows rejected by validation", ["reason"])
BATCHES = Counter("spark_batches_processed_total", "Micro-batches committed")
BATCHES_SKIPPED = Counter("spark_batches_replayed_total", "Micro-batches skipped by the idempotency ledger")
BATCH_FAILURES = Counter("spark_batch_failures_total", "Micro-batches that raised an exception")
ALERTS_RAISED = Counter("spark_idle_alerts_raised_total", "Idle-vehicle alerts raised")
BATCH_SECONDS = Histogram("spark_batch_duration_seconds", "Wall time to process one micro-batch",
                          buckets=(0.5, 1, 2, 4, 8, 15, 30, 60))
LAST_BATCH_TS = Gauge("spark_last_batch_timestamp_seconds", "Unix time the last batch was committed")
LAST_EVENT_TS = Gauge("spark_last_event_timestamp_seconds", "Event-time (unix) of the newest event processed")
LAST_BATCH_ROWS = Gauge("spark_last_batch_rows", "Rows in the last micro-batch")


def _rows(df):
    return [r.asDict() for r in df.collect()]


def process_batch(batch_df, batch_id: int):
    """Called by Spark for every micro-batch. Any exception fails the query; Docker restarts the
    container and Spark resumes from its checkpoint (the DB ledger prevents double counting)."""
    started = time.time()
    try:
        _process_batch(batch_df, batch_id, started)
    except Exception as exc:  # noqa: BLE001 - we log and re-raise on purpose
        BATCH_FAILURES.inc()
        log.error("micro-batch failed", exc_info=True, extra={"fields": {"batch_id": batch_id, "error": str(exc)}})
        raise


def _process_batch(batch_df, batch_id, started):
    batch_df.persist()
    try:
        total = batch_df.count()
        if total == 0:
            return
        valid = transforms.enrich(batch_df.filter(F.col("reject_reason").isNull()))
        bad = batch_df.filter(F.col("reject_reason").isNotNull())
        reasons = {r["reject_reason"]: r["count"] for r in _rows(bad.groupBy("reject_reason").count())}
        n_bad = sum(reasons.values())
        n_valid = total - n_bad

        # --- 1. archive to the data lake (immutable master dataset). Writing to a folder named after
        #        the batch id with mode=overwrite makes a replayed batch overwrite itself (idempotent).
        if n_valid:
            (valid.select(*transforms.LAKE_COLUMNS).write.mode("overwrite").partitionBy("sim_date")
                  .parquet(f"{config.LAKE_DIR}/trip_events/batch_id={batch_id}"))
        if n_bad:
            (bad.select("raw_json", "reject_reason", "kafka_ts").write.mode("overwrite")
                .parquet(f"{config.LAKE_DIR}/dead_letter/batch_id={batch_id}"))
            sample = [r["raw_json"][:120] for r in _rows(bad.select("raw_json").limit(2))]
            warning(log, "invalid events rejected", batch_id=batch_id, count=n_bad, reasons=reasons, sample=sample)

        # --- 2. aggregations (small result sets -> collect to the driver) ---------------------------
        zone_1m = _rows(transforms.aggregate_zone_1m(valid)) if n_valid else []
        zone_hourly = _rows(transforms.aggregate_zone_hourly(valid)) if n_valid else []
        vehicle_daily = _rows(transforms.aggregate_vehicle_daily(valid)) if n_valid else []
        vehicles = _rows(transforms.summarise_vehicles(valid)) if n_valid else []
        max_ts = valid.agg(F.max("event_time")).collect()[0][0] if n_valid else None

        # --- 3. transactional, idempotent write to PostgreSQL ----------------------------------------
        query_id = next((str(q.id) for q in batch_df.sparkSession.streams.active), "local")
        conn = connect()
        try:
            result = sinks.apply_batch(
                conn, batch_id, zone_1m, zone_hourly, vehicle_daily, vehicles,
                {"records_in": total, "records_ok": n_valid, "records_bad": n_bad, "max_event_ts": max_ts},
                query_name=f"{sinks.QUERY_NAME}:{query_id}")
        finally:
            conn.close()

        duration = time.time() - started
        if result["skipped"]:
            BATCHES_SKIPPED.inc()
            info(log_sink, "batch already processed - skipped (idempotent replay)", batch_id=batch_id)
            return

        # --- 4. metrics + structured log ---------------------------------------------------------------
        EVENTS_RECEIVED.inc(total)
        EVENTS_VALID.inc(n_valid)
        for reason, cnt in reasons.items():
            EVENTS_INVALID.labels(reason=reason).inc(cnt)
        ALERTS_RAISED.inc(result["new_alerts"])
        BATCHES.inc()
        BATCH_SECONDS.observe(duration)
        LAST_BATCH_TS.set(time.time())
        LAST_BATCH_ROWS.set(total)
        if max_ts is not None:
            LAST_EVENT_TS.set(max_ts.replace(tzinfo=timezone.utc).timestamp())
        info(log_sink, "batch committed", batch_id=batch_id, rows=total, valid=n_valid, invalid=n_bad,
             vehicles=result["vehicles"], new_idle_alerts=result["new_alerts"],
             resolved_idle_alerts=result["resolved_alerts"], duration_s=round(duration, 2))
    finally:
        batch_df.unpersist()


def build_stream(spark):
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
           .option("subscribe", config.KAFKA_TOPIC)
           .option("startingOffsets", "earliest")          # only used on the very first run
           .option("maxOffsetsPerTrigger", MAX_OFFSETS)
           .option("failOnDataLoss", "false")
           .load())
    events = transforms.parse_events(raw)
    # Producers may retry -> duplicates. Keep state for 2 minutes of event time, then forget it.
    return (events.withWatermark("event_time", "2 minutes")
                  .dropDuplicates(["event_key", "event_time"]))


def main():
    start_http_server(METRICS_PORT)
    LAST_BATCH_TS.set(time.time())      # so the "no data" alert does not fire during start-up
    LAST_EVENT_TS.set(time.time())

    spark = (SparkSession.builder.appName("fleet-speed-layer").master("local[2]")
             .config("spark.jars.packages", SPARK_KAFKA_PACKAGE)
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.driver.bindAddress", "0.0.0.0")
             .config("spark.driver.host", "127.0.0.1")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    query = (build_stream(spark).writeStream
             .foreachBatch(process_batch)
             .option("checkpointLocation", config.CHECKPOINT_DIR)
             .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
             .queryName("speed_layer")
             .start())
    info(log, "streaming query started", query_id=str(query.id), topic=config.KAFKA_TOPIC,
         trigger_seconds=TRIGGER_SECONDS, checkpoint=config.CHECKPOINT_DIR)

    while query.isActive:
        query.awaitTermination(30)
        p = query.lastProgress
        if p:
            info(log, "query progress", batch=p.get("batchId"), input_rows=p.get("numInputRows"),
                 input_rows_per_s=p.get("inputRowsPerSecond"), processed_rows_per_s=p.get("processedRowsPerSecond"))
    exc = query.exception()
    if exc:
        error(log, "streaming query terminated", error=str(exc))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
