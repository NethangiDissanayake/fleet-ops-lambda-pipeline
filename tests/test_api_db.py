"""Serving layer: FastAPI endpoints against a real PostgreSQL seeded with known numbers."""
import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")
from fastapi.testclient import TestClient  # noqa: E402

from common import config  # noqa: E402


@pytest.fixture(scope="module")
def client(pg_conn, pg_params):
    mp = pytest.MonkeyPatch()
    mp.setattr(config, "POSTGRES_HOST", pg_params["host"])
    mp.setattr(config, "POSTGRES_PORT", int(pg_params["port"]))
    mp.setattr(config, "POSTGRES_DB", pg_params["dbname"])
    mp.setattr(config, "POSTGRES_USER", pg_params["user"])
    mp.setattr(config, "POSTGRES_PASSWORD", pg_params["password"] or "")
    from api import main
    main._pool, main._clock = None, None

    with pg_conn.cursor() as cur:
        cur.execute("""INSERT INTO vehicle_state (vehicle_id, status, zone, last_ts, idle_since) VALUES
            ('V001','on_trip','Z1', now(), NULL), ('V002','enroute','Z1', now(), NULL),
            ('V003','idle','Z2', now(), now() - interval '90 seconds'), ('V004','idle','Z2', now(), now()),
            ('V005','idle','Z3', now() - interval '10 minutes', now() - interval '10 minutes')""")   # V005 offline
        cur.execute("""INSERT INTO zone_metrics_1m (window_start, zone, events, idle_events, trips, earnings) VALUES
            (date_trunc('minute', now()), 'Z1', 100, 10, 4, 3200), (date_trunc('minute', now()), 'Z2', 100, 60, 1, 700)""")
        cur.execute("INSERT INTO zone_hourly VALUES ('2026-01-01', 8, 'Z1', 100, 10, 4, 3200), ('2026-01-01', 9, 'Z1', 50, 25, 1, 700)")
        cur.execute("INSERT INTO alerts (alert_type, vehicle_id, message) VALUES ('IDLE_VEHICLE','V003','idle 90s')")
        cur.execute("INSERT INTO pipeline_stats VALUES ('speed_layer', 1000, 990, 10, now(), now())")
        cur.execute("""INSERT INTO daily_profitability (sim_date, vehicle_id, trips, earnings, total_cost, profit, status)
                       VALUES ('2026-01-01','V001',5,3000,2000,1000,'PROFITABLE'), ('2026-01-01','V002',1,500,2500,-2000,'UNPROFITABLE')""")
        cur.execute("INSERT INTO daily_reports (sim_date, html, csv) VALUES ('2026-01-01','<h1>report</h1>','vehicle_id\\nV001')")
        cur.execute("INSERT INTO batch_runs (sim_date, status, rows_loaded, rows_rejected, vehicles, unprofitable, max_abs_drift, finished_at) "
                    "VALUES ('2026-01-01','success',28,2,30,1,0,now())")
    pg_conn.commit()
    with TestClient(main.app) as c:
        yield c
    mp.undo()


def test_utilization_realtime_metrics(client):
    body = client.get("/metrics/utilization?window_minutes=5").json()
    assert body["fleet"] == {"vehicles_online": 4, "active_vehicles": 2, "idle_vehicles": 2, "idle_ratio": 0.5}
    assert body["throughput"]["completed_trips"] == 5
    assert body["throughput"]["earnings_lkr"] == 3900.0
    assert body["throughput"]["trips_per_sim_hour"] == round(5 / 24, 2)        # 5 min = 24 simulated hours
    zones = {z["zone"]: z for z in body["by_zone"]}
    assert zones["Z1"]["earnings_lkr"] == 3200.0 and zones["Z1"]["active_vehicles_now"] == 2
    assert zones["Z2"]["idle_ratio"] == 0.6 and zones["Z2"]["idle_vehicles_now"] == 2


def test_time_of_day(client):
    body = client.get("/metrics/time-of-day").json()
    assert body["sim_date"] == "2026-01-01"
    assert [h["sim_hour"] for h in body["by_hour"]] == [8, 9]
    assert body["by_hour"][1]["idle_ratio"] == 0.5 and body["by_zone"][0]["earnings_lkr"] == 3900.0


def test_vehicles_and_alerts(client):
    assert [v["vehicle_id"] for v in client.get("/vehicles?status=idle").json()] == ["V003", "V004", "V005"]
    assert client.get("/vehicles?status=flying").status_code == 422
    alerts = client.get("/alerts").json()
    assert alerts["count"] == 1 and alerts["alerts"][0]["vehicle_id"] == "V003"


def test_profitability_report_formats(client):
    body = client.get("/report/profitability").json()
    assert body["vehicles"] == 2 and body["unprofitable"] == 1 and body["rows"][0]["vehicle_id"] == "V002"  # worst first
    assert client.get("/report/profitability?status=PROFITABLE").json()["rows"][0]["vehicle_id"] == "V001"
    assert client.get("/report/profitability?format=html").text == "<h1>report</h1>"
    assert client.get("/report/profitability?format=csv").headers["content-type"].startswith("text/csv")
    assert client.get("/report/profitability?sim_date=2030-01-01").status_code == 404
    assert client.get("/report/dates").json() == ["2026-01-01"]


def test_health_ok_then_degraded(client, pg_conn):
    assert client.get("/health").status_code == 200
    with pg_conn.cursor() as cur:
        cur.execute("UPDATE pipeline_stats SET last_event_ts = now() - interval '10 minutes'")
    pg_conn.commit()
    res = client.get("/health")
    assert res.status_code == 503 and res.json()["status"] == "degraded" and "no data for" in res.json()["problems"][0]


def test_prometheus_endpoint_exposes_pipeline_and_api_metrics(client):
    client.get("/alerts")
    text = client.get("/metrics").text
    for name in ["fleet_vehicles_active 2.0", "fleet_idle_ratio 0.5", "fleet_open_idle_alerts 1.0", "pipeline_db_up 1.0",
                 'pipeline_records_total{kind="invalid"} 10.0', 'batch_days_processed_total{result="success"} 1.0',
                 "batch_last_unprofitable_vehicles 1.0", 'fleet_zone_earnings_5m_lkr{zone="Z1"} 3200.0',
                 'api_requests_total{method="GET",path="/alerts",status="200"}']:
        assert name in text, name
