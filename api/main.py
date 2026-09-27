"""SERVING LAYER - FastAPI service.

  GET /metrics/utilization        real-time fleet utilization (active vehicles, idle ratio, trips/hour, earnings by zone)
  GET /metrics/time-of-day        earnings / trips / idle ratio by simulated hour and by zone
  GET /vehicles                   latest state of every vehicle
  GET /alerts                     idle-vehicle threshold alerts (open or all)
  GET /report/profitability       daily per-vehicle profitability reconciliation (json | csv | html)
  GET /report/dates               simulated days that have a report
  GET /pipeline/status            per-stage counters + last batch runs
  GET /health                     200 = healthy, 503 = no data for NO_DATA_SECONDS or DB down
  GET /metrics                    Prometheus exposition (pipeline + API metrics)
  GET /docs                       interactive Swagger UI

All figures are read from PostgreSQL, which is written by the speed layer (Spark) and the batch layer (Airflow).
"""
import threading
import time
from contextlib import contextmanager
from datetime import date

from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
from prometheus_client import CONTENT_TYPE_LATEST, REGISTRY, Counter, Histogram, generate_latest
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool

from common import config
from common.logging_utils import error, get_logger, info
from common.simclock import SimClock

log = get_logger("api", stage="serving")
app = FastAPI(title="Fleet Operations API", version="1.0",
              description="Real-time utilization (speed layer) + daily profitability (batch layer)")

# ------------------------------------------------------------------------------ DB access
_pool = None
_pool_lock = threading.Lock()
_clock = None


def _get_pool():
    global _pool
    with _pool_lock:
        if _pool is None:
            _pool = ThreadedConnectionPool(
                1, 10, host=config.POSTGRES_HOST, port=config.POSTGRES_PORT, dbname=config.POSTGRES_DB,
                user=config.POSTGRES_USER, password=config.POSTGRES_PASSWORD, connect_timeout=5)
        return _pool


@contextmanager
def cursor():
    pool = _get_pool()
    conn = pool.getconn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)


def q(sql, params=None):
    with cursor() as cur:
        cur.execute(sql, params or ())
        return cur.fetchall()


def q1(sql, params=None):
    rows = q(sql, params)
    return rows[0] if rows else None


def get_clock() -> SimClock:
    global _clock
    if _clock is None:
        r = q1("SELECT epoch_unix, day_seconds, start_date FROM sim_clock WHERE id = 1")
        _clock = SimClock(float(r["epoch_unix"]), int(r["day_seconds"]), r["start_date"])
    return _clock


def _f(v):
    return float(v) if v is not None else None


# ------------------------------------------------------------------------------ API metrics
REQUESTS = Counter("api_requests_total", "HTTP requests", ["method", "path", "status"])
LATENCY = Histogram("api_request_duration_seconds", "HTTP request latency", ["path"])


@app.middleware("http")
async def observe(request: Request, call_next):
    start = time.time()
    status = 500
    try:
        response = await call_next(request)
        status = response.status_code
        return response
    finally:
        route = request.scope.get("route")
        path = getattr(route, "path", request.url.path)
        REQUESTS.labels(request.method, path, str(status)).inc()
        LATENCY.labels(path).observe(time.time() - start)
        if path not in ("/metrics", "/health"):
            info(log, "request", method=request.method, path=request.url.path, status=status,
                 duration_ms=round((time.time() - start) * 1000, 1))


@app.exception_handler(Exception)
async def unhandled(request: Request, exc: Exception):
    error(log, "unhandled error", path=request.url.path, error=str(exc))
    return JSONResponse(status_code=503, content={"detail": f"service unavailable: {exc}"})


# ------------------------------------------------------------------------------ real-time endpoints
@app.get("/metrics/utilization", tags=["real-time"])
def utilization(window_minutes: int = Query(5, ge=1, le=120, description="look-back window (real minutes)")):
    clock = get_clock()
    online_s = config.VEHICLE_ONLINE_SECONDS
    fleet = q1("""
        SELECT count(*) AS online,
               count(*) FILTER (WHERE status <> 'idle') AS active,
               count(*) FILTER (WHERE status = 'idle')  AS idle
        FROM vehicle_state WHERE last_ts > now() - make_interval(secs => %s)""", (online_s,))
    per_zone_now = {r["zone"]: r for r in q("""
        SELECT zone, count(*) FILTER (WHERE status <> 'idle') AS active, count(*) FILTER (WHERE status = 'idle') AS idle
        FROM vehicle_state WHERE last_ts > now() - make_interval(secs => %s) GROUP BY zone""", (online_s,))}
    zones = q("""
        SELECT zone, sum(events) AS events, sum(idle_events) AS idle_events, sum(trips) AS trips, sum(earnings) AS earnings
        FROM zone_metrics_1m WHERE window_start >= now() - make_interval(mins => %s) GROUP BY zone ORDER BY zone""",
              (window_minutes,))
    trips = sum(int(z["trips"]) for z in zones)
    earnings = sum(float(z["earnings"]) for z in zones)
    sim_hours_in_window = window_minutes * 60 / clock.sim_hour_seconds
    online = int(fleet["online"])
    return {
        "as_of": q1("SELECT now() AS t")["t"].isoformat(),
        "sim_time": clock.sim_datetime().isoformat(timespec="minutes"),
        "window_minutes": window_minutes,
        "fleet": {"vehicles_online": online, "active_vehicles": int(fleet["active"]),
                  "idle_vehicles": int(fleet["idle"]),
                  "idle_ratio": round(int(fleet["idle"]) / online, 3) if online else None},
        "throughput": {"completed_trips": trips,
                       "trips_per_sim_hour": round(trips / sim_hours_in_window, 2),
                       "earnings_lkr": round(earnings, 2),
                       "avg_fare_lkr": round(earnings / trips, 2) if trips else None},
        "by_zone": [{
            "zone": z["zone"],
            "active_vehicles_now": int(per_zone_now.get(z["zone"], {}).get("active", 0)),
            "idle_vehicles_now": int(per_zone_now.get(z["zone"], {}).get("idle", 0)),
            "idle_ratio": round(int(z["idle_events"]) / int(z["events"]), 3) if int(z["events"]) else None,
            "trips": int(z["trips"]), "earnings_lkr": round(float(z["earnings"]), 2)} for z in zones],
    }


@app.get("/metrics/time-of-day", tags=["real-time"])
def time_of_day(sim_date: date | None = None):
    if sim_date is None:
        latest = q1("SELECT max(sim_date) AS d FROM zone_hourly")
        if not latest or latest["d"] is None:
            raise HTTPException(404, "no data yet")
        sim_date = latest["d"]
    by_hour = q("""SELECT sim_hour, sum(trips) AS trips, sum(earnings) AS earnings,
                          sum(idle_events)::float / nullif(sum(events), 0) AS idle_ratio
                   FROM zone_hourly WHERE sim_date = %s GROUP BY sim_hour ORDER BY sim_hour""", (sim_date,))
    by_zone = q("""SELECT zone, sum(trips) AS trips, sum(earnings) AS earnings,
                          sum(idle_events)::float / nullif(sum(events), 0) AS idle_ratio
                   FROM zone_hourly WHERE sim_date = %s GROUP BY zone ORDER BY zone""", (sim_date,))
    fmt = lambda r: {**{k: v for k, v in r.items() if k not in ("trips", "earnings", "idle_ratio")},
                     "trips": int(r["trips"]), "earnings_lkr": _f(r["earnings"]),
                     "idle_ratio": round(_f(r["idle_ratio"]), 3) if r["idle_ratio"] is not None else None}
    return {"sim_date": sim_date.isoformat(), "by_hour": [fmt(r) for r in by_hour], "by_zone": [fmt(r) for r in by_zone]}


@app.get("/vehicles", tags=["real-time"])
def vehicles(status: str | None = Query(None, pattern="^(idle|enroute|on_trip)$")):
    rows = q("""SELECT vehicle_id, driver_id, status, zone, lat, lon, speed, last_ts, idle_since,
                       GREATEST(extract(epoch FROM (last_ts - idle_since)), 0) AS idle_seconds,
                       last_ts > now() - make_interval(secs => %s) AS online
                FROM vehicle_state WHERE (%s::text IS NULL OR status = %s::text) ORDER BY vehicle_id""",
             (config.VEHICLE_ONLINE_SECONDS, status, status))
    return [{**r, "idle_seconds": _f(r["idle_seconds"])} for r in rows]


@app.get("/alerts", tags=["alerts"])
def alerts(status: str = Query("open", pattern="^(open|all)$"), limit: int = Query(50, ge=1, le=500)):
    where = "WHERE resolved_at IS NULL" if status == "open" else ""
    rows = q(f"SELECT alert_id, alert_type, vehicle_id, severity, message, created_at, resolved_at "
             f"FROM alerts {where} ORDER BY created_at DESC LIMIT %s", (limit,))
    return {"threshold_seconds": config.IDLE_ALERT_SECONDS, "count": len(rows), "alerts": rows}


# ------------------------------------------------------------------------------ batch-layer reports
@app.get("/report/dates", tags=["reports"])
def report_dates():
    return [r["sim_date"].isoformat() for r in q("SELECT sim_date FROM daily_reports ORDER BY sim_date DESC")]


@app.get("/report/profitability", tags=["reports"])
def profitability(sim_date: date | None = None, format: str = Query("json", pattern="^(json|csv|html)$"),
                  status: str | None = None):
    if sim_date is None:
        latest = q1("SELECT max(sim_date) AS d FROM daily_profitability")
        if not latest or latest["d"] is None:
            raise HTTPException(404, "no daily report yet - the batch layer runs after the first simulated day ends")
        sim_date = latest["d"]
    if format in ("csv", "html"):
        rep = q1("SELECT html, csv FROM daily_reports WHERE sim_date = %s", (sim_date,))
        if not rep:
            raise HTTPException(404, f"no report for {sim_date}")
        return HTMLResponse(rep["html"]) if format == "html" else PlainTextResponse(rep["csv"], media_type="text/csv")
    rows = q("""SELECT vehicle_id, trips, earnings, fuel_cost, maintenance_cost, total_cost, profit, margin,
                       distance_covered, cost_per_km, idle_ratio, prev_profit_change, declining, status,
                       speed_layer_earnings, drift
                FROM daily_profitability WHERE sim_date = %s AND (%s::text IS NULL OR status = %s::text)
                ORDER BY profit ASC NULLS LAST""", (sim_date, status, status))
    if not rows:
        raise HTTPException(404, f"no report for {sim_date}")
    rows = [{k: (_f(v) if hasattr(v, "as_tuple") else v) for k, v in r.items()} for r in rows]
    return {"sim_date": sim_date.isoformat(), "vehicles": len(rows),
            "unprofitable": sum(1 for r in rows if r["status"] == "UNPROFITABLE"),
            "at_risk": sum(1 for r in rows if r["status"] == "AT_RISK"), "rows": rows}


# ------------------------------------------------------------------------------ health / status
def _status():
    stats = q1("SELECT *, extract(epoch FROM (now() - last_event_ts)) AS seconds_since_event "
               "FROM pipeline_stats WHERE stage = 'speed_layer'")
    runs = q("SELECT sim_date, status, rows_loaded, rows_rejected, vehicles, unprofitable, max_abs_drift, finished_at "
             "FROM batch_runs ORDER BY id DESC LIMIT 10")
    return stats, runs


@app.get("/pipeline/status", tags=["observability"])
def pipeline_status():
    stats, runs = _status()
    return {"speed_layer": stats, "recent_batch_runs": runs}


@app.get("/health", tags=["observability"])
def health():
    problems = []
    try:
        stats, _ = _status()
        if not stats or stats["seconds_since_event"] is None:
            problems.append("no events processed yet")
            since = None
        else:
            since = float(stats["seconds_since_event"])
            if since > config.NO_DATA_SECONDS:
                problems.append(f"no data for {since:.0f}s (limit {config.NO_DATA_SECONDS}s)")
    except Exception as exc:  # noqa: BLE001
        return JSONResponse(status_code=503, content={"status": "down", "problems": [f"database: {exc}"]})
    body = {"status": "ok" if not problems else "degraded", "seconds_since_last_event": since, "problems": problems}
    return JSONResponse(status_code=200 if not problems else 503, content=body)


# ------------------------------------------------------------------------------ Prometheus
class PipelineCollector:
    """Reads pipeline state from PostgreSQL at scrape time and exposes it as Prometheus metrics."""

    def collect(self):
        up = GaugeMetricFamily("pipeline_db_up", "1 if PostgreSQL is reachable")
        try:
            fleet = q1("""SELECT count(*) AS online,
                                 count(*) FILTER (WHERE status <> 'idle') AS active,
                                 count(*) FILTER (WHERE status = 'idle') AS idle
                          FROM vehicle_state WHERE last_ts > now() - make_interval(secs => %s)""",
                       (config.VEHICLE_ONLINE_SECONDS,))
            open_alerts = q1("SELECT count(*) AS n FROM alerts WHERE resolved_at IS NULL AND alert_type = 'IDLE_VEHICLE'")
            stats = q1("SELECT *, extract(epoch FROM (now() - last_event_ts)) AS since FROM pipeline_stats WHERE stage='speed_layer'")
            zones = q("SELECT zone, sum(earnings) AS e FROM zone_metrics_1m WHERE window_start >= now() - interval '5 minutes' GROUP BY zone")
            runs = q1("""SELECT count(*) FILTER (WHERE status='success') AS ok, count(*) FILTER (WHERE status='failed') AS failed,
                                extract(epoch FROM max(finished_at) FILTER (WHERE status='success')) AS last_ok
                         FROM batch_runs""")
            last = q1("SELECT unprofitable, rows_rejected, max_abs_drift FROM batch_runs WHERE status='success' ORDER BY id DESC LIMIT 1")
            up.add_metric([], 1)
        except Exception:  # noqa: BLE001
            up.add_metric([], 0)
            yield up
            return
        yield up
        online = int(fleet["online"])
        for name, doc, val in [("fleet_vehicles_online", "Vehicles seen recently", online),
                               ("fleet_vehicles_active", "Vehicles enroute or on a trip", int(fleet["active"])),
                               ("fleet_vehicles_idle", "Online vehicles that are idle", int(fleet["idle"])),
                               ("fleet_idle_ratio", "idle / online vehicles", int(fleet["idle"]) / online if online else 0),
                               ("fleet_open_idle_alerts", "Unresolved idle-vehicle alerts", int(open_alerts["n"]))]:
            g = GaugeMetricFamily(name, doc)
            g.add_metric([], val)
            yield g
        g = GaugeMetricFamily("fleet_zone_earnings_5m_lkr", "Earnings per zone, last 5 minutes", labels=["zone"])
        for z in zones:
            g.add_metric([z["zone"]], float(z["e"]))
        yield g
        g = GaugeMetricFamily("pipeline_seconds_since_last_event", "Seconds since the newest event that was processed")
        g.add_metric([], float(stats["since"]) if stats and stats["since"] is not None else -1)
        yield g
        if stats:
            c = CounterMetricFamily("pipeline_records", "Records seen by the speed layer", labels=["kind"])
            c.add_metric(["received"], int(stats["records_in"]))
            c.add_metric(["valid"], int(stats["records_ok"]))
            c.add_metric(["invalid"], int(stats["records_bad"]))
            yield c
        c = CounterMetricFamily("batch_days_processed", "Simulated days processed by the batch layer", labels=["result"])
        c.add_metric(["success"], int(runs["ok"]))
        c.add_metric(["failed"], int(runs["failed"]))
        yield c
        g = GaugeMetricFamily("batch_last_success_timestamp_seconds", "Unix time of the last successful batch run")
        g.add_metric([], float(runs["last_ok"]) if runs["last_ok"] else 0)
        yield g
        if last:
            for name, doc, val in [("batch_last_unprofitable_vehicles", "Unprofitable vehicles in the last report", last["unprofitable"]),
                                   ("batch_last_rejected_rows", "Expense rows rejected in the last run", last["rows_rejected"]),
                                   ("batch_last_max_abs_drift_lkr", "Max |batch - speed layer| earnings drift, last run", last["max_abs_drift"])]:
                g = GaugeMetricFamily(name, doc)
                g.add_metric([], float(val or 0))
                yield g


REGISTRY.register(PipelineCollector())


@app.get("/metrics", tags=["observability"], include_in_schema=False)
def metrics():
    return Response(generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST)
