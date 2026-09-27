"""PostgreSQL sink for the speed layer (plain psycopg2 - no Spark needed, so it is unit-testable).

`apply_batch` writes ONE micro-batch inside ONE database transaction:

    1. insert batch_id into the `processed_batches` ledger      (if it is already there -> skip)
    2. additive upserts of the aggregates (zone/minute, zone/hour, vehicle/day)
    3. merge vehicle states, raise / resolve idle alerts
    4. bump the pipeline counters
    COMMIT

Because of the ledger the write is IDEMPOTENT: Spark's guarantee is "at least once" (after a crash it
may re-run the last batch), and the ledger turns that into an "exactly-once EFFECT" in the database.
"""
from datetime import timezone

from psycopg2.extras import execute_values

from common import config
from common.state import idle_seconds, next_vehicle_state

QUERY_NAME = "speed_layer"


def _utc(ts):
    """Spark returns naive datetimes (driver TZ = UTC); make them timezone-aware."""
    if ts is not None and ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts


def apply_batch(conn, batch_id, zone_1m, zone_hourly, vehicle_daily, vehicle_summaries, stats,
                query_name=QUERY_NAME):
    """Returns a dict describing what happened (used for logs and metrics).

    zone_1m / zone_hourly / vehicle_daily / vehicle_summaries : lists of dicts (from Spark .collect())
    stats : {"records_in", "records_ok", "records_bad", "max_event_ts"}
    query_name : ledger key. The job passes "speed_layer:<streaming-query-id>" so that wiping the Spark
                 checkpoint (batch ids restart at 0) can never be confused with an old run.
    """
    result = {"skipped": False, "new_alerts": 0, "resolved_alerts": 0, "vehicles": len(vehicle_summaries)}
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO processed_batches (query_name, batch_id) VALUES (%s, %s) "
                "ON CONFLICT DO NOTHING", (query_name, batch_id))
            if cur.rowcount == 0:                       # replayed batch -> nothing to do
                conn.rollback()
                result["skipped"] = True
                return result

            if zone_1m:
                execute_values(cur, """
                    INSERT INTO zone_metrics_1m
                        (window_start, zone, events, idle_events, enroute_events, on_trip_events, trips, earnings)
                    VALUES %s
                    ON CONFLICT (window_start, zone) DO UPDATE SET
                        events         = zone_metrics_1m.events + EXCLUDED.events,
                        idle_events    = zone_metrics_1m.idle_events + EXCLUDED.idle_events,
                        enroute_events = zone_metrics_1m.enroute_events + EXCLUDED.enroute_events,
                        on_trip_events = zone_metrics_1m.on_trip_events + EXCLUDED.on_trip_events,
                        trips          = zone_metrics_1m.trips + EXCLUDED.trips,
                        earnings       = zone_metrics_1m.earnings + EXCLUDED.earnings
                    """, [(_utc(r["window_start"]), r["zone"], r["events"], r["idle_events"],
                           r["enroute_events"], r["on_trip_events"], r["trips"], r["earnings"])
                          for r in zone_1m])

            if zone_hourly:
                execute_values(cur, """
                    INSERT INTO zone_hourly (sim_date, sim_hour, zone, events, idle_events, trips, earnings)
                    VALUES %s
                    ON CONFLICT (sim_date, sim_hour, zone) DO UPDATE SET
                        events      = zone_hourly.events + EXCLUDED.events,
                        idle_events = zone_hourly.idle_events + EXCLUDED.idle_events,
                        trips       = zone_hourly.trips + EXCLUDED.trips,
                        earnings    = zone_hourly.earnings + EXCLUDED.earnings
                    """, [(r["sim_date"], r["sim_hour"], r["zone"], r["events"], r["idle_events"],
                           r["trips"], r["earnings"]) for r in zone_hourly])

            if vehicle_daily:
                execute_values(cur, """
                    INSERT INTO vehicle_daily_rt
                        (sim_date, vehicle_id, events, idle_events, on_trip_events, trips, earnings)
                    VALUES %s
                    ON CONFLICT (sim_date, vehicle_id) DO UPDATE SET
                        events         = vehicle_daily_rt.events + EXCLUDED.events,
                        idle_events    = vehicle_daily_rt.idle_events + EXCLUDED.idle_events,
                        on_trip_events = vehicle_daily_rt.on_trip_events + EXCLUDED.on_trip_events,
                        trips          = vehicle_daily_rt.trips + EXCLUDED.trips,
                        earnings       = vehicle_daily_rt.earnings + EXCLUDED.earnings
                    """, [(r["sim_date"], r["vehicle_id"], r["events"], r["idle_events"],
                           r["on_trip_events"], r["trips"], r["earnings"]) for r in vehicle_daily])

            if vehicle_summaries:
                _merge_vehicle_states(cur, vehicle_summaries, result)

            cur.execute("""
                INSERT INTO pipeline_stats (stage, records_in, records_ok, records_bad, last_batch_at, last_event_ts)
                VALUES (%s, %s, %s, %s, now(), %s)
                ON CONFLICT (stage) DO UPDATE SET
                    records_in    = pipeline_stats.records_in  + EXCLUDED.records_in,
                    records_ok    = pipeline_stats.records_ok  + EXCLUDED.records_ok,
                    records_bad   = pipeline_stats.records_bad + EXCLUDED.records_bad,
                    last_batch_at = now(),
                    last_event_ts = GREATEST(pipeline_stats.last_event_ts, EXCLUDED.last_event_ts)
                """, (QUERY_NAME, stats["records_in"], stats["records_ok"], stats["records_bad"],
                      _utc(stats.get("max_event_ts"))))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return result


def _merge_vehicle_states(cur, summaries, result):
    ids = [s["vehicle_id"] for s in summaries]
    cur.execute("SELECT vehicle_id, driver_id, status, lat, lon, speed, zone, last_ts, idle_since "
                "FROM vehicle_state WHERE vehicle_id = ANY(%s)", (ids,))
    cols = ["vehicle_id", "driver_id", "status", "lat", "lon", "speed", "zone", "last_ts", "idle_since"]
    prior = {row[0]: dict(zip(cols, row)) for row in cur.fetchall()}

    new_states = []
    for s in summaries:
        s = dict(s)
        s["last_ts"] = _utc(s["last_ts"])
        s["batch_idle_start"] = _utc(s.get("batch_idle_start"))
        s["had_non_idle"] = bool(s.get("had_non_idle"))
        new_states.append(next_vehicle_state(prior.get(s["vehicle_id"]), s))

    execute_values(cur, """
        INSERT INTO vehicle_state (vehicle_id, driver_id, status, lat, lon, speed, zone, last_ts, idle_since)
        VALUES %s
        ON CONFLICT (vehicle_id) DO UPDATE SET
            driver_id = EXCLUDED.driver_id, status = EXCLUDED.status, lat = EXCLUDED.lat,
            lon = EXCLUDED.lon, speed = EXCLUDED.speed, zone = EXCLUDED.zone,
            last_ts = EXCLUDED.last_ts, idle_since = EXCLUDED.idle_since, updated_at = now()
        """, [(st["vehicle_id"], st["driver_id"], st["status"], st["lat"], st["lon"], st["speed"],
               st["zone"], st["last_ts"], st["idle_since"]) for st in new_states])

    # ---- threshold alert: idle for too long --------------------------------------------------
    for st in new_states:
        secs = idle_seconds(st)
        if secs >= config.IDLE_ALERT_SECONDS:
            cur.execute("""
                INSERT INTO alerts (alert_type, vehicle_id, severity, message)
                VALUES ('IDLE_VEHICLE', %s, 'warning', %s)
                ON CONFLICT (alert_type, vehicle_id) WHERE resolved_at IS NULL DO NOTHING
                """, (st["vehicle_id"],
                      f"Vehicle {st['vehicle_id']} idle for {secs:.0f}s "
                      f"(threshold {config.IDLE_ALERT_SECONDS}s) in zone {st['zone']}"))
            result["new_alerts"] += cur.rowcount

    working = [st["vehicle_id"] for st in new_states if st["status"] != "idle"]
    if working:
        cur.execute("UPDATE alerts SET resolved_at = now() WHERE alert_type = 'IDLE_VEHICLE' "
                    "AND resolved_at IS NULL AND vehicle_id = ANY(%s)", (working,))
        result["resolved_alerts"] += cur.rowcount
