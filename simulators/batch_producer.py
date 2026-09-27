"""DAILY-BATCH SOURCE - garage + fuel-partner expense file, dropped once per SIMULATED day.

    * When a simulated day ends (every SIM_DAY_SECONDS real seconds) this script writes
      /data/landing/expenses_<sim_date>.csv  containing costs for the day that just finished.
    * The file is written to a temp name and atomically renamed, so Airflow never sees a
      half-written file.
    * ~6 % of rows are deliberately dirty (missing/negative/non-numeric values, unknown vehicle,
      exact duplicate row, garbage service flag) so the cleaning step has real work to do.

Manual use (handy for demos / testing):
    python -m simulators.batch_producer --drop-now            # drop a file for the CURRENT sim day
    python -m simulators.batch_producer --drop-now --date 2026-01-05
Prometheus metrics are served on :8002/metrics.
"""
import argparse
import csv
import os
import random
import signal
import time
from datetime import date, timedelta

from prometheus_client import Counter, Gauge, start_http_server

from common import config
from common.logging_utils import get_logger, info
from common.simclock import load_clock
from simulators.fleet import profile_for, vehicle_ids

log = get_logger("batch-producer", stage="ingestion")

DIRTY_RATE = float(os.environ.get("DIRTY_ROW_RATE", "0.06"))
METRICS_PORT = int(os.environ.get("METRICS_PORT", "8002"))
COLUMNS = ["vehicle_id", "fuel_cost", "maintenance_cost", "distance_covered", "service_flag"]

FILES_DROPPED = Counter("batch_files_dropped_total", "Expense files written to the landing zone")
ROWS_WRITTEN = Counter("batch_rows_written_total", "Rows written across all expense files")
DIRTY_ROWS = Counter("batch_dirty_rows_injected_total", "Deliberately corrupted rows")
LAST_DROP_TS = Gauge("batch_last_drop_timestamp_seconds", "Unix time of the last file drop")

_running = True


def _stop(*_):
    global _running
    _running = False


def expense_row(vehicle_id: str, rng: random.Random) -> dict:
    """Costs for one vehicle for one simulated day (LKR)."""
    profile = profile_for(vehicle_id)
    if profile == "lazy":
        distance = rng.uniform(15, 40)
        maintenance = rng.uniform(300, 900)
    elif profile == "costly":
        distance = rng.uniform(50, 90)
        maintenance = rng.uniform(2200, 4500)          # frequent servicing / repairs
    else:
        distance = rng.uniform(45, 85)
        maintenance = rng.uniform(200, 900)
    fuel = distance * rng.uniform(32, 42)              # LKR per km
    service = maintenance > 2000 or rng.random() < 0.05
    return {"vehicle_id": vehicle_id, "fuel_cost": round(fuel, 2),
            "maintenance_cost": round(maintenance, 2),
            "distance_covered": round(distance, 1), "service_flag": str(service).lower()}


def dirty(row: dict, rng: random.Random) -> list[dict]:
    """Return one or two rows that break a data-quality rule."""
    kind = rng.choice(["missing_fuel", "negative_maint", "non_numeric", "unknown_vehicle",
                       "duplicate", "bad_flag"])
    bad = dict(row)
    if kind == "missing_fuel":
        bad["fuel_cost"] = ""
    elif kind == "negative_maint":
        bad["maintenance_cost"] = -abs(float(row["maintenance_cost"]))
    elif kind == "non_numeric":
        bad["distance_covered"] = "N/A"
    elif kind == "unknown_vehicle":
        bad["vehicle_id"] = "V999"
    elif kind == "bad_flag":
        bad["service_flag"] = "maybe"
    elif kind == "duplicate":
        return [row, dict(row)]
    return [bad]


def write_expense_file(day: date, rng: random.Random, out_dir: str = None) -> str:
    out_dir = out_dir or config.LANDING_DIR
    os.makedirs(out_dir, exist_ok=True)
    rows, dirty_count = [], 0
    for vid in vehicle_ids():
        row = expense_row(vid, rng)
        if rng.random() < DIRTY_RATE:
            rows.extend(dirty(row, rng))
            dirty_count += 1
        else:
            rows.append(row)
    rng.shuffle(rows)

    final = os.path.join(out_dir, f"expenses_{day.isoformat()}.csv")
    tmp = final + ".tmp"
    with open(tmp, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    os.replace(tmp, final)                              # atomic publish

    FILES_DROPPED.inc()
    ROWS_WRITTEN.inc(len(rows))
    DIRTY_ROWS.inc(dirty_count)
    LAST_DROP_TS.set(time.time())
    info(log, "expense file dropped", file=final, sim_date=day.isoformat(), rows=len(rows),
         dirty_rows=dirty_count)
    return final


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--drop-now", action="store_true", help="write one file immediately and exit")
    parser.add_argument("--date", help="simulated date for --drop-now (default: current sim date)")
    args = parser.parse_args()

    clock = load_clock()
    rng = random.Random()

    if args.drop_now:
        day = date.fromisoformat(args.date) if args.date else clock.sim_date()
        write_expense_file(day, rng)
        return

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    start_http_server(METRICS_PORT)
    last_dropped = clock.day_index() - 1                # the day in progress is not finished yet
    info(log, "batch producer started", sim_day_seconds=clock.day_seconds,
         current_sim_date=str(clock.sim_date()), landing_dir=config.LANDING_DIR)

    while _running:
        finished_through = clock.day_index() - 1        # last fully finished simulated day
        while last_dropped < finished_through:
            last_dropped += 1
            write_expense_file(clock.start_date + timedelta(days=last_dropped), rng)
        time.sleep(1)


if __name__ == "__main__":
    main()
