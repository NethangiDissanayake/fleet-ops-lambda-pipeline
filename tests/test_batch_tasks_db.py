"""Batch layer end-to-end: landing CSV + Parquet master dataset + speed-layer view -> profitability report."""
import csv
import time
from datetime import date, datetime, timezone

import pandas as pd
import pytest

from common import batch_tasks as bt
from common.simclock import SimClock

DAY = "2026-01-02"
CLOCK = SimClock(epoch_unix=1_000_000.0, day_seconds=300, start_date=date(2026, 1, 1))


def write_lake(lake_dir, day=DAY):
    """Two Spark micro-batches worth of events (+ one duplicate event to prove de-duplication)."""
    def ev(eid, vid, status, fare, done):
        return {"event_id": eid, "vehicle_id": vid, "status": status, "fare": fare, "trip_completed": done}
    b0 = [ev("a1", "V001", "idle", 0.0, False), ev("a2", "V001", "on_trip", 900.0, True),
          ev("b1", "V002", "idle", 0.0, False), ev("b2", "V002", "idle", 0.0, False)]
    b1 = [ev("a2", "V001", "on_trip", 900.0, True),            # DUPLICATE of a2 (replay) -> must not double count
          ev("a3", "V001", "on_trip", 1100.0, True), ev("b3", "V002", "enroute", 0.0, False)]
    for i, rows in enumerate((b0, b1)):
        d = f"{lake_dir}/trip_events/batch_id={i}/sim_date={day}"
        import os
        os.makedirs(d, exist_ok=True)
        pd.DataFrame(rows).to_parquet(f"{d}/part-0.parquet")


def write_expenses(landing, day=DAY, rows=None):
    rows = rows or [
        {"vehicle_id": "V001", "fuel_cost": "1200", "maintenance_cost": "300", "distance_covered": "60", "service_flag": "false"},
        {"vehicle_id": "V002", "fuel_cost": "1500", "maintenance_cost": "900", "distance_covered": "40", "service_flag": "true"},
        {"vehicle_id": "V001", "fuel_cost": "1200", "maintenance_cost": "300", "distance_covered": "60", "service_flag": "false"},  # dup
        {"vehicle_id": "V999", "fuel_cost": "10", "maintenance_cost": "10", "distance_covered": "1", "service_flag": "false"},     # unknown
    ]
    with open(f"{landing}/expenses_{day}.csv", "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


def q(conn, sql, *args):
    with conn.cursor() as cur:
        cur.execute(sql, args)
        return cur.fetchall()


def test_full_batch_flow(pg_conn, tmp_path):
    landing, lake, reports = tmp_path / "landing", tmp_path / "lake", tmp_path / "reports"
    landing.mkdir()
    write_lake(str(lake))
    write_expenses(str(landing))
    (landing / "expenses_2026-01-09.csv.tmp").write_text("half written")             # must be ignored
    with pg_conn.cursor() as cur:                                                    # vehicle master data + speed layer view
        cur.execute("INSERT INTO vehicle_state (vehicle_id,status,last_ts) VALUES ('V001','idle',now()),('V002','idle',now())")
        cur.execute("INSERT INTO vehicle_daily_rt (sim_date,vehicle_id,earnings,trips) VALUES (%s,'V001',1900,2),(%s,'V002',0,0)", (DAY, DAY))
        cur.execute("INSERT INTO zone_hourly (sim_date,sim_hour,zone,events,idle_events,trips,earnings) VALUES (%s,8,'Z1',10,4,2,2000)", (DAY,))
    pg_conn.commit()

    # --- discovery: day is only pending once it is over (+ grace)
    day_end = CLOCK.day_end_real_ts(date(2026, 1, 2))
    assert bt.find_pending_days(pg_conn, CLOCK, str(landing), now=day_end - 1, grace=20) == []
    assert bt.find_pending_days(pg_conn, CLOCK, str(landing), now=day_end + 30, grace=20) == [DAY]

    # --- ingest
    res = bt.ingest_expenses(pg_conn, DAY, str(landing))
    assert res == {"rows_in": 4, "rows_loaded": 2, "rows_rejected": 2}
    assert sorted(r[0] for r in q(pg_conn, "SELECT reason FROM expenses_rejected")) == ["duplicate_row", "unknown_vehicle"]

    # --- batch view from parquet (duplicate a2 removed): V001 earned 900 + 1100
    bt.recompute_batch_view(pg_conn, DAY, str(lake))
    view = {r[0]: (r[1], float(r[2])) for r in q(pg_conn, "SELECT vehicle_id, trips, earnings FROM vehicle_daily_batch")}
    assert view == {"V001": (2, 2000.0), "V002": (0, 0.0)}

    # --- reconciliation
    summary = bt.reconcile_day(pg_conn, DAY)
    assert summary["vehicles"] == 2 and summary["unprofitable"] == 1
    prof = {r[0]: r[1:] for r in q(pg_conn, "SELECT vehicle_id, profit, status, drift FROM daily_profitability")}
    assert float(prof["V001"][0]) == 500.0 and prof["V001"][1] == "PROFITABLE" and float(prof["V001"][2]) == 100.0
    assert float(prof["V002"][0]) == -2400.0 and prof["V002"][1] == "UNPROFITABLE"

    # --- report + audit row
    files = bt.publish_report(pg_conn, DAY, str(reports))
    html_text = (reports / f"profitability_{DAY}.html").read_text()
    assert len(files) == 2 and "V002" in html_text
    assert "Partial day" in html_text                      # 7 events << 2 vehicles x 150 expected -> flagged as partial
    assert (reports / f"profitability_{DAY}.csv").read_text().startswith("vehicle_id")
    bt.record_run(pg_conn, DAY, "success", started_at=time.time(), **res, **summary)
    assert bt.find_pending_days(pg_conn, CLOCK, str(landing), now=day_end + 30, grace=20) == []   # done -> not pending
    assert bt.find_pending_days(pg_conn, CLOCK, str(landing), now=day_end + 30, grace=20, reprocess=[DAY]) == [DAY]

    # --- idempotency: run the whole chain again -> identical numbers, no duplicate rows
    bt.ingest_expenses(pg_conn, DAY, str(landing)); bt.recompute_batch_view(pg_conn, DAY, str(lake)); bt.reconcile_day(pg_conn, DAY)
    assert q(pg_conn, "SELECT count(*) FROM daily_profitability")[0][0] == 2
    assert q(pg_conn, "SELECT count(*) FROM expenses_clean")[0][0] == 2


def test_trend_uses_previous_day(pg_conn, tmp_path):
    prev, day = "2026-01-05", "2026-01-06"
    with pg_conn.cursor() as cur:
        cur.execute("INSERT INTO daily_profitability (sim_date, vehicle_id, profit, status) VALUES (%s,'V050',100,'PROFITABLE')", (prev,))
        cur.execute("INSERT INTO vehicle_daily_batch VALUES (%s,'V050',10,0,1,1000)", (day,))
        cur.execute("INSERT INTO expenses_clean (sim_date,vehicle_id,fuel_cost,maintenance_cost,distance_covered,service_flag,source_file) "
                    "VALUES (%s,'V050',900,80,50,false,'x')", (day,))
    pg_conn.commit()
    bt.reconcile_day(pg_conn, day)
    row = q(pg_conn, "SELECT profit, margin, status, prev_profit_change, declining FROM daily_profitability WHERE sim_date=%s", day)[0]
    assert float(row[0]) == 20.0 and row[2] == "AT_RISK" and float(row[3]) == -80.0 and row[4] is True
