"""BATCH LAYER task implementations (called by the Airflow DAG, but Airflow-free so they can be tested).

Every function is IDEMPOTENT: re-running a simulated day deletes and rewrites that day's rows, so
retries and manual re-runs never create duplicates.

Sequence per simulated day D (see airflow/dags/daily_reconciliation.py):
    find_pending_days -> ingest_expenses -> recompute_batch_view -> reconcile_day -> publish_report
"""
import csv
import glob
import json
import os
import re
import time
from datetime import date, timedelta

import pandas as pd
from psycopg2.extras import Json, execute_values

from common import config
from common.batch_logic import clean_expenses, reconcile
from common.logging_utils import error, get_logger, info, warning
from common.report import render_csv, render_html
from common.simclock import load_clock

log = get_logger("airflow-batch", stage="orchestration")
_FILE_RE = re.compile(r"^expenses_(\d{4}-\d{2}-\d{2})\.csv$")


# ------------------------------------------------------------------ 1. discover work
def find_pending_days(conn, clock, landing_dir=None, now=None, grace=None, reprocess=None):
    """Simulated days whose expense file has arrived, whose day is over (+ grace for late Kafka
    events) and which have not been processed successfully yet."""
    landing_dir = landing_dir or config.LANDING_DIR
    grace = config.LATE_DATA_GRACE_SECONDS if grace is None else grace
    now = time.time() if now is None else now
    reprocess = set(reprocess or [])

    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT sim_date FROM batch_runs WHERE status = 'success'")
        done = {r[0].isoformat() for r in cur.fetchall()}
    conn.commit()

    pending = []
    if os.path.isdir(landing_dir):
        for name in sorted(os.listdir(landing_dir)):
            m = _FILE_RE.match(name)
            if not m:
                continue                                  # ignores *.tmp (file still being written)
            day_str = m.group(1)
            day = date.fromisoformat(day_str)
            forced = day_str in reprocess
            day_over = now >= clock.day_end_real_ts(day) + grace
            if forced or (day_str not in done and day_over):
                pending.append(day_str)
    info(log, "scanned landing zone", pending=pending, already_done=len(done))
    return pending


# ------------------------------------------------------------------ 2. ingest + clean
def ingest_expenses(conn, day_str, landing_dir=None):
    landing_dir = landing_dir or config.LANDING_DIR
    path = os.path.join(landing_dir, f"expenses_{day_str}.csv")
    with open(path, newline="") as fh:
        rows = list(csv.DictReader(fh))

    with conn.cursor() as cur:
        cur.execute("SELECT vehicle_id FROM vehicle_state")
        known = {r[0] for r in cur.fetchall()} or None    # vehicle master data = vehicles seen on the stream
    clean, rejected = clean_expenses(rows, known)

    with conn.cursor() as cur:
        cur.execute("DELETE FROM expenses_clean WHERE sim_date = %s", (day_str,))
        cur.execute("DELETE FROM expenses_rejected WHERE sim_date = %s", (day_str,))
        if clean:
            execute_values(cur, """
                INSERT INTO expenses_clean (sim_date, vehicle_id, fuel_cost, maintenance_cost,
                                            distance_covered, service_flag, source_file) VALUES %s""",
                           [(day_str, c["vehicle_id"], c["fuel_cost"], c["maintenance_cost"],
                             c["distance_covered"], c["service_flag"], os.path.basename(path)) for c in clean])
        if rejected:
            execute_values(cur, """
                INSERT INTO expenses_rejected (sim_date, source_file, raw_row, reason) VALUES %s""",
                           [(day_str, os.path.basename(path), Json(r["raw_row"]), r["reason"]) for r in rejected])
    conn.commit()
    reasons = {}
    for r in rejected:
        reasons[r["reason"]] = reasons.get(r["reason"], 0) + 1
    info(log, "expenses ingested", sim_date=day_str, file=os.path.basename(path), rows_in=len(rows),
         rows_loaded=len(clean), rows_rejected=len(rejected), reject_reasons=reasons)
    return {"rows_in": len(rows), "rows_loaded": len(clean), "rows_rejected": len(rejected)}


# ------------------------------------------------------------------ 3. batch view from the master dataset
def read_lake_day(lake_dir, day_str):
    files = sorted(glob.glob(os.path.join(lake_dir, "trip_events", "batch_id=*", f"sim_date={day_str}", "*.parquet")))
    frames = [pd.read_parquet(f, columns=["event_id", "vehicle_id", "status", "fare", "trip_completed"])
              for f in files]
    if not frames:
        return pd.DataFrame(columns=["event_id", "vehicle_id", "status", "fare", "trip_completed"]), 0
    return pd.concat(frames, ignore_index=True), len(files)


def compute_batch_view(events: pd.DataFrame) -> dict:
    """Recompute per-vehicle totals from raw events. De-duplicates by event_id first."""
    if events.empty:
        return {}
    events = events.drop_duplicates(subset="event_id")
    events = events.assign(
        is_idle=(events["status"] == "idle").astype(int),
        trips_done=events["trip_completed"].astype(int),
        earned=events["fare"].where(events["trip_completed"], 0.0),
    )
    grouped = events.groupby("vehicle_id").agg(
        events=("event_id", "count"), idle_events=("is_idle", "sum"),
        trips=("trips_done", "sum"), earnings=("earned", "sum"))
    return {vid: {"events": int(r.events), "idle_events": int(r.idle_events),
                  "trips": int(r.trips), "earnings": round(float(r.earnings), 2)}
            for vid, r in grouped.iterrows()}


def recompute_batch_view(conn, day_str, lake_dir=None):
    lake_dir = lake_dir or config.LAKE_DIR
    events, n_files = read_lake_day(lake_dir, day_str)
    view = compute_batch_view(events)
    if not view:
        warning(log, "no events in the master dataset for this day", sim_date=day_str, files=n_files)
    with conn.cursor() as cur:
        cur.execute("DELETE FROM vehicle_daily_batch WHERE sim_date = %s", (day_str,))
        if view:
            execute_values(cur, """
                INSERT INTO vehicle_daily_batch (sim_date, vehicle_id, events, idle_events, trips, earnings)
                VALUES %s""", [(day_str, vid, v["events"], v["idle_events"], v["trips"], v["earnings"])
                               for vid, v in view.items()])
    conn.commit()
    info(log, "batch view recomputed from parquet master dataset", sim_date=day_str, parquet_files=n_files,
         raw_events=len(events), unique_events=int(events["event_id"].nunique()) if len(events) else 0,
         vehicles=len(view))
    return {"vehicles": len(view), "events": len(events)}


# ------------------------------------------------------------------ 4. join batch + expenses
def reconcile_day(conn, day_str):
    day = date.fromisoformat(day_str)
    prev_day = (day - timedelta(days=1)).isoformat()
    with conn.cursor() as cur:
        cur.execute("SELECT vehicle_id, events, idle_events, trips, earnings FROM vehicle_daily_batch WHERE sim_date=%s", (day_str,))
        batch_view = {r[0]: {"events": r[1], "idle_events": r[2], "trips": r[3], "earnings": float(r[4])}
                      for r in cur.fetchall()}
        cur.execute("SELECT vehicle_id, fuel_cost, maintenance_cost, distance_covered, service_flag FROM expenses_clean WHERE sim_date=%s", (day_str,))
        expenses = {r[0]: {"fuel_cost": float(r[1]), "maintenance_cost": float(r[2]),
                           "distance_covered": float(r[3]), "service_flag": r[4]} for r in cur.fetchall()}
        cur.execute("SELECT vehicle_id, earnings FROM vehicle_daily_rt WHERE sim_date=%s", (day_str,))
        speed_view = {r[0]: {"earnings": float(r[1])} for r in cur.fetchall()}
        cur.execute("SELECT vehicle_id, profit FROM daily_profitability WHERE sim_date=%s", (prev_day,))
        prev_profit = {r[0]: (float(r[1]) if r[1] is not None else None) for r in cur.fetchall()}

    rows = reconcile(batch_view, expenses, speed_view, prev_profit)
    with conn.cursor() as cur:
        cur.execute("DELETE FROM daily_profitability WHERE sim_date = %s", (day_str,))
        if rows:
            execute_values(cur, """
                INSERT INTO daily_profitability (sim_date, vehicle_id, trips, earnings, fuel_cost, maintenance_cost,
                    total_cost, profit, margin, distance_covered, cost_per_km, service_flag, idle_ratio,
                    prev_profit_change, declining, status, speed_layer_earnings, drift) VALUES %s""",
                           [(day_str, r["vehicle_id"], r["trips"], r["earnings"], r["fuel_cost"], r["maintenance_cost"],
                             r["total_cost"], r["profit"], r["margin"], r["distance_covered"], r["cost_per_km"],
                             r["service_flag"], r["idle_ratio"], r["profit_change"], r["declining"], r["status"],
                             r["speed_layer_earnings"], r["drift"]) for r in rows])
    conn.commit()
    drifts = [abs(r["drift"]) for r in rows if r["drift"] is not None]
    summary = {"vehicles": len(rows),
               "unprofitable": sum(1 for r in rows if r["status"] == "UNPROFITABLE"),
               "max_abs_drift": max(drifts) if drifts else 0.0}
    info(log, "profitability reconciled", sim_date=day_str, **summary,
         at_risk=sum(1 for r in rows if r["status"] == "AT_RISK"),
         no_expense=sum(1 for r in rows if r["status"] == "NO_EXPENSE_DATA"))
    return summary


# ------------------------------------------------------------------ 5. report
def publish_report(conn, day_str, report_dir=None):
    report_dir = report_dir or config.REPORT_DIR
    cols = ["vehicle_id", "trips", "earnings", "fuel_cost", "maintenance_cost", "total_cost", "profit",
            "margin", "distance_covered", "cost_per_km", "idle_ratio", "prev_profit_change", "declining",
            "status", "speed_layer_earnings", "drift"]
    with conn.cursor() as cur:
        cur.execute(f"SELECT {', '.join(cols)} FROM daily_profitability WHERE sim_date=%s ORDER BY profit ASC NULLS LAST", (day_str,))
        rows = [dict(zip(cols, [float(v) if hasattr(v, "as_tuple") else v for v in rec])) for rec in cur.fetchall()]
        cur.execute("SELECT sim_hour, SUM(earnings), SUM(trips), SUM(idle_events)::float / NULLIF(SUM(events),0) "
                    "FROM zone_hourly WHERE sim_date=%s GROUP BY sim_hour ORDER BY sim_hour", (day_str,))
        hourly = [{"sim_hour": h, "earnings": float(e), "trips": int(t), "idle_ratio": float(i) if i is not None else None}
                  for h, e, t, i in cur.fetchall()]
        cur.execute("SELECT reason, COUNT(*) FROM expenses_rejected WHERE sim_date=%s GROUP BY reason ORDER BY 2 DESC", (day_str,))
        rejected = {r: int(c) for r, c in cur.fetchall()}
        cur.execute("SELECT COALESCE(SUM(events), 0), COUNT(*) FROM vehicle_daily_batch WHERE sim_date=%s", (day_str,))
        n_events, n_vehicles = cur.fetchone()
    # data coverage: how much of the expected telemetry (vehicles x day length / tick) actually arrived
    expected = n_vehicles * load_clock(conn).day_seconds / config.TICK_SECONDS
    coverage = float(n_events) / expected if expected else 0.0
    if coverage < 0.9:
        warning(log, "partial day - telemetry coverage below 90%", sim_date=day_str, coverage=round(coverage, 3))
    html_doc = render_html(day_str, rows, hourly, rejected, coverage)
    csv_doc = render_csv(rows)

    with conn.cursor() as cur:
        cur.execute("""INSERT INTO daily_reports (sim_date, html, csv, generated_at) VALUES (%s,%s,%s, now())
                       ON CONFLICT (sim_date) DO UPDATE SET html=EXCLUDED.html, csv=EXCLUDED.csv, generated_at=now()""",
                    (day_str, html_doc, csv_doc))
    conn.commit()

    written = []
    try:                                                   # file copy is a convenience - DB is the source of truth
        os.makedirs(report_dir, exist_ok=True)
        for ext, body in (("html", html_doc), ("csv", csv_doc)):
            p = os.path.join(report_dir, f"profitability_{day_str}.{ext}")
            with open(p, "w", encoding="utf-8") as fh:
                fh.write(body)
            written.append(p)
    except OSError as exc:
        warning(log, "could not write report files (report is still available via the API)", error=str(exc))
    info(log, "report published", sim_date=day_str, files=written, rows=len(rows))
    return written


def record_run(conn, day_str, status, started_at=None, message=None, **stats):
    with conn.cursor() as cur:
        cur.execute("""INSERT INTO batch_runs (sim_date, status, rows_loaded, rows_rejected, vehicles, unprofitable,
                                               max_abs_drift, message, started_at, finished_at)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s, COALESCE(to_timestamp(%s), now()), now())""",
                    (day_str, status, stats.get("rows_loaded"), stats.get("rows_rejected"), stats.get("vehicles"),
                     stats.get("unprofitable"), stats.get("max_abs_drift"), message, started_at))
    conn.commit()
