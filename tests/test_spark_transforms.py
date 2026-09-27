"""Spark transformations on a small static DataFrame (local Spark, no Kafka needed).
Skipped automatically when pyspark or Java is not installed."""
import json
import os
import shutil
import time
from datetime import datetime

os.environ["TZ"] = "UTC"      # Spark hands naive datetimes to Python in the process time zone;
time.tzset()                  # the Docker image sets TZ=UTC too, so tests mirror production

import pytest

pytest.importorskip("pyspark")
if shutil.which("java") is None:
    pytest.skip("Java not installed", allow_module_level=True)

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from common.validation import event_problem  # noqa: E402
from common.zones import zone_expr, zone_for  # noqa: E402
from streaming import transforms as T  # noqa: E402

KAFKA_TS = datetime(2026, 3, 1, 10, 0, 5)


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder.master("local[1]").appName("tests").config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "2").config("spark.sql.session.timeZone", "UTC").getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def ev(i=1, vid="V001", status="idle", ts="2026-03-01T10:00:%02d.000Z", lat=6.9, lon=79.85, speed=0.0, fare=0.0,
       done=False, sim="2026-01-01T08:30:00"):
    return {"event_id": f"e{i}", "trip_id": None, "driver_id": "D001", "vehicle_id": vid, "lat": lat, "lon": lon,
            "speed": speed, "status": status, "fare": fare, "trip_completed": done,
            "timestamp": ts % i if "%" in ts else ts, "sim_time": sim}


def frame(spark, payloads):
    rows = [(p if isinstance(p, str) else json.dumps(p), KAFKA_TS) for p in payloads]
    return spark.createDataFrame(rows, ["value", "timestamp"])


def test_reject_reasons_match_the_python_rules(spark):
    cases = {
        None: ev(1),
        "missing_gps": ev(2, lat=None),
        "bad_speed": ev(3, speed=-12.5),
        "bad_status": ev(4, status="teleporting"),
        "gps_out_of_range": ev(5, lat=51.5),
        "bad_fare": ev(6, fare=-1.0),
    }
    df = T.parse_events(frame(spark, list(cases.values()) + ['{"vehicle_id": "V001", "lat": ']))
    got = {r["event_id"]: r["reject_reason"] for r in df.collect() if r["event_id"]}
    for (reason, payload) in cases.items():
        assert got[payload["event_id"]] == reason
        assert event_problem(payload) == reason                       # Spark and Python rules agree
    garbage = [r for r in df.collect() if r["event_id"] is None][0]
    assert garbage["reject_reason"] == "unparseable_json" and garbage["event_time"] == KAFKA_TS   # falls back to Kafka time
    assert garbage["event_key"] is not None                            # hash key so de-duplication still works


def test_zone_expression_matches_python_function(spark):
    pts = [(6.85 + i * 0.0155, 79.82 + j * 0.0105) for i in range(-1, 11) for j in range(-1, 11)]
    df = spark.createDataFrame(pts, ["lat", "lon"]).withColumn("zone", zone_expr(F.col("lat"), F.col("lon")))
    assert all(r["zone"] == zone_for(r["lat"], r["lon"]) for r in df.collect())


def enriched(spark, events):
    parsed = T.parse_events(frame(spark, events))
    return T.enrich(parsed.filter(F.col("reject_reason").isNull()))


def test_enrich_and_aggregations(spark):
    events = [
        ev(1, "V001", "idle", "2026-03-01T10:00:01.000Z", 6.86, 79.83),
        ev(2, "V001", "on_trip", "2026-03-01T10:00:10.000Z", 6.86, 79.83, 30.0, 400.0, False),
        ev(3, "V001", "on_trip", "2026-03-01T10:00:20.000Z", 6.86, 79.83, 30.0, 900.0, True),      # completed: earns 900
        ev(4, "V002", "enroute", "2026-03-01T10:01:05.000Z", 6.99, 79.91, 30.0),                    # Z9, next minute
        ev(5, "V002", "on_trip", "2026-03-01T10:01:15.000Z", 6.99, 79.91, 30.0, 1100.0, True),      # completed: earns 1100
    ]
    df = enriched(spark, events)
    row = df.filter("event_id = 'e1'").collect()[0]
    assert row["zone"] == "Z1" and str(row["sim_date"]) == "2026-01-01" and row["sim_hour"] == 8

    z1m = {(str(r["window_start"]), r["zone"]): r for r in T.aggregate_zone_1m(df).collect()}
    z1 = z1m[("2026-03-01 10:00:00", "Z1")]
    assert (z1["events"], z1["idle_events"], z1["on_trip_events"], z1["trips"], z1["earnings"]) == (3, 1, 2, 1, 900.0)
    assert z1m[("2026-03-01 10:01:00", "Z9")]["earnings"] == 1100.0

    hourly = T.aggregate_zone_hourly(df).collect()
    assert sum(r["earnings"] for r in hourly) == 2000.0 and sum(r["trips"] for r in hourly) == 2

    daily = {r["vehicle_id"]: r for r in T.aggregate_vehicle_daily(df).collect()}
    assert daily["V001"]["earnings"] == 900.0 and daily["V002"]["trips"] == 1


def test_vehicle_summary_tracks_the_trailing_idle_run(spark):
    events = [   # V001: idle, trip, idle, idle  -> idle run starts at 10:00:30 and a non-idle event exists in the batch
        ev(1, "V001", "idle", "2026-03-01T10:00:00.000Z"),
        ev(2, "V001", "on_trip", "2026-03-01T10:00:10.000Z", speed=30.0, fare=300.0),
        ev(3, "V001", "idle", "2026-03-01T10:00:30.000Z"),
        ev(4, "V001", "idle", "2026-03-01T10:00:40.000Z"),
        # V002: only idle events -> run may continue from an earlier batch
        ev(5, "V002", "idle", "2026-03-01T10:00:05.000Z"),
        ev(6, "V002", "idle", "2026-03-01T10:00:25.000Z"),
    ]
    s = {r["vehicle_id"]: r for r in T.summarise_vehicles(enriched(spark, events)).collect()}
    assert s["V001"]["status"] == "idle" and s["V001"]["had_non_idle"] == 1
    assert s["V001"]["batch_idle_start"] == datetime(2026, 3, 1, 10, 0, 30)
    assert s["V001"]["last_ts"] == datetime(2026, 3, 1, 10, 0, 40)
    assert s["V002"]["had_non_idle"] == 0 and s["V002"]["batch_idle_start"] == datetime(2026, 3, 1, 10, 0, 5)


def test_streaming_dedup_and_watermark_plan_compiles(spark, tmp_path):
    """Feed duplicates through a real streaming query (file source stands in for Kafka)."""
    src = tmp_path / "in"
    src.mkdir()
    payloads = [ev(1), ev(1), ev(2, ts="2026-03-01T10:00:09.000Z")]           # e1 delivered twice
    (src / "a.json").write_text("\n".join(json.dumps(p) for p in payloads))
    raw = (spark.readStream.format("text").load(str(src))
           .withColumn("timestamp", F.current_timestamp()))
    parsed = T.parse_events(raw)
    deduped = parsed.withWatermark("event_time", "2 minutes").dropDuplicates(["event_key", "event_time"])
    out = []
    q = (deduped.writeStream.foreachBatch(lambda df, i: out.extend(r["event_id"] for r in df.collect()))
         .option("checkpointLocation", str(tmp_path / "ckpt")).trigger(availableNow=True).start())
    q.awaitTermination(60)
    assert sorted(out) == ["e1", "e2"]


def test_process_batch_end_to_end_speed_layer_to_batch_layer(spark, pg_conn, pg_params, tmp_path, monkeypatch):
    """Whole micro-batch path: parse -> lake (Parquet) + PostgreSQL, replay is idempotent, and the
    batch layer can read the very Parquet files Spark wrote."""
    from common import batch_tasks, config
    monkeypatch.setattr(config, "LAKE_DIR", str(tmp_path / "lake"))
    for key, attr in (("host", "POSTGRES_HOST"), ("dbname", "POSTGRES_DB"), ("user", "POSTGRES_USER")):
        monkeypatch.setattr(config, attr, pg_params[key])
    monkeypatch.setattr(config, "POSTGRES_PORT", int(pg_params["port"]))
    monkeypatch.setattr(config, "POSTGRES_PASSWORD", pg_params["password"] or "")
    from streaming import spark_job

    events = [
        ev(1, "V001", "idle", "2026-03-01T10:00:01.000Z"),
        ev(2, "V001", "on_trip", "2026-03-01T10:00:10.000Z", speed=30.0, fare=900.0, done=True),
        ev(3, "V002", "idle", "2026-03-01T10:00:02.000Z"),
        ev(4, "V002", "idle", "2026-03-01T10:01:30.000Z"),                     # idle for 88 s  -> alert
        ev(5, "V003", "bad_status_here", "2026-03-01T10:00:03.000Z"),          # invalid
        '{"vehicle_id": "V009", "lat": ',                                       # garbage
    ]
    batch = T.parse_events(frame(spark, events))
    spark_job._process_batch(batch, 0, 0.0)

    def scalar(sql):
        with pg_conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchone()[0]
    pg_conn.rollback()
    assert scalar("SELECT records_in FROM pipeline_stats") == 6 and scalar("SELECT records_bad FROM pipeline_stats") == 2
    assert float(scalar("SELECT earnings FROM vehicle_daily_rt WHERE vehicle_id='V001'")) == 900.0
    assert scalar("SELECT count(*) FROM alerts WHERE vehicle_id='V002' AND resolved_at IS NULL") == 1
    assert (tmp_path / "lake" / "dead_letter" / "batch_id=0").exists()

    spark_job._process_batch(batch, 0, 0.0)                                       # replay the same batch id
    pg_conn.rollback()
    assert float(scalar("SELECT earnings FROM vehicle_daily_rt WHERE vehicle_id='V001'")) == 900.0   # unchanged
    assert scalar("SELECT records_in FROM pipeline_stats") == 6

    day_events, n_files = batch_tasks.read_lake_day(str(tmp_path / "lake"), "2026-01-01")
    assert n_files >= 1
    view = batch_tasks.compute_batch_view(day_events)
    assert view["V001"]["earnings"] == 900.0 and view["V001"]["trips"] == 1 and view["V002"]["idle_events"] == 2
