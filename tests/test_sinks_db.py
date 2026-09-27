"""Speed-layer sink against a real PostgreSQL (throw-away database)."""
from datetime import date, datetime, timedelta, timezone

from streaming.sinks import apply_batch

T0 = datetime(2026, 3, 1, 10, 0, 0, tzinfo=timezone.utc)
STATS = {"records_in": 10, "records_ok": 9, "records_bad": 1, "max_event_ts": T0}


def agg(events=10, trips=1, earnings=500.0):
    return dict(window_start=T0, zone="Z5", events=events, idle_events=2, enroute_events=3, on_trip_events=5,
                trips=trips, earnings=earnings, sim_date=date(2026, 1, 1), sim_hour=9, vehicle_id="V001")


def summary(status, last, had_non_idle=False, idle_start=None, vid="V001"):
    return {"vehicle_id": vid, "driver_id": "D001", "status": status, "lat": 6.9, "lon": 79.85, "speed": 0.0,
            "zone": "Z5", "last_ts": last, "had_non_idle": int(had_non_idle), "batch_idle_start": idle_start}


def scalar(conn, sql, *args):
    with conn.cursor() as cur:
        cur.execute(sql, args)
        return cur.fetchone()[0]


def test_aggregates_are_additive_across_batches(pg_conn):
    for batch_id in (0, 1):
        r = apply_batch(pg_conn, batch_id, [agg()], [agg()], [agg()], [summary("on_trip", T0, True)], STATS)
        assert r["skipped"] is False
    assert scalar(pg_conn, "SELECT events FROM zone_metrics_1m") == 20
    assert float(scalar(pg_conn, "SELECT earnings FROM zone_hourly")) == 1000.0
    assert scalar(pg_conn, "SELECT trips FROM vehicle_daily_rt WHERE vehicle_id='V001'") == 2
    assert scalar(pg_conn, "SELECT records_in FROM pipeline_stats WHERE stage='speed_layer'") == 20


def test_replayed_batch_is_skipped_exactly_once_effect(pg_conn):
    before = scalar(pg_conn, "SELECT events FROM zone_metrics_1m")
    replay = apply_batch(pg_conn, 1, [agg()], [agg()], [agg()], [summary("on_trip", T0, True)], STATS)
    assert replay["skipped"] is True
    assert scalar(pg_conn, "SELECT events FROM zone_metrics_1m") == before          # nothing double-counted


def test_failed_batch_rolls_back_completely(pg_conn):
    bad = agg()
    bad["zone"] = None                                     # violates NOT NULL -> whole transaction must roll back
    try:
        apply_batch(pg_conn, 50, [bad], [], [], [], STATS)
        raise AssertionError("expected an exception")
    except Exception as exc:                               # noqa: BLE001
        assert "AssertionError" not in type(exc).__name__
    assert scalar(pg_conn, "SELECT count(*) FROM processed_batches WHERE batch_id = 50") == 0
    good = apply_batch(pg_conn, 50, [agg()], [], [], [], STATS)                       # retry works
    assert good["skipped"] is False


def test_idle_alert_raised_once_and_resolved_when_vehicle_works(pg_conn):
    vid = "V777"
    r1 = apply_batch(pg_conn, 100, [], [], [], [summary("idle", T0, idle_start=T0, vid=vid)], STATS)
    assert r1["new_alerts"] == 0                                                       # just became idle
    r2 = apply_batch(pg_conn, 101, [], [], [], [summary("idle", T0 + timedelta(seconds=75), vid=vid)], STATS)
    assert r2["new_alerts"] == 1                                                       # idle 75 s >= 60 s threshold
    r3 = apply_batch(pg_conn, 102, [], [], [], [summary("idle", T0 + timedelta(seconds=90), vid=vid)], STATS)
    assert r3["new_alerts"] == 0                                                       # no duplicate open alert
    assert scalar(pg_conn, "SELECT count(*) FROM alerts WHERE vehicle_id=%s AND resolved_at IS NULL", vid) == 1
    r4 = apply_batch(pg_conn, 103, [], [], [], [summary("enroute", T0 + timedelta(seconds=100), True, vid=vid)], STATS)
    assert r4["resolved_alerts"] == 1
    assert scalar(pg_conn, "SELECT count(*) FROM alerts WHERE vehicle_id=%s AND resolved_at IS NULL", vid) == 0
    assert scalar(pg_conn, "SELECT idle_since IS NULL FROM vehicle_state WHERE vehicle_id=%s", vid) is True
