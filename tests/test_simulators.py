import csv
import random
from datetime import date, datetime, timezone

from common.batch_logic import clean_expenses
from common.validation import event_problem
from simulators import batch_producer
from simulators.fleet import LAZY, vehicle_ids
from simulators.vehicle import Vehicle


def run(vehicle_id, ticks=3000, seed=1):
    v = Vehicle(vehicle_id, random.Random(seed))
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    return [v.step(9, datetime(2026, 1, 1, 9), now) for _ in range(ticks)]


def test_every_event_is_valid_and_follows_the_state_machine():
    events = run("V001")
    assert all(event_problem(e) is None for e in events)
    for e in events:
        if e["status"] == "idle":
            assert e["trip_id"] is None and e["fare"] == 0.0 and not e["trip_completed"]
        else:
            assert e["trip_id"]
    # allowed transitions only
    allowed = {("idle", "idle"), ("idle", "enroute"), ("enroute", "enroute"), ("enroute", "on_trip"),
               ("on_trip", "on_trip"), ("on_trip", "idle")}
    seq = [e["status"] for e in events]
    assert all((a, b) in allowed for a, b in zip(seq, seq[1:]))


def test_completed_trip_carries_the_final_fare_and_trips_are_counted_once():
    events = run("V001")
    completed = [e for e in events if e["trip_completed"]]
    assert len(completed) > 20
    assert all(e["status"] == "on_trip" and e["fare"] >= 250 for e in completed)
    assert len({e["trip_id"] for e in completed}) == len(completed)      # one completion per trip id


def test_lazy_vehicle_idles_much_longer_and_earns_less():
    def stats(vid):
        ev = run(vid, 2000)
        return sum(e["status"] == "idle" for e in ev) / len(ev), sum(e["fare"] for e in ev if e["trip_completed"])
    lazy_idle, lazy_earn = stats(sorted(LAZY)[0])
    norm_idle, norm_earn = stats("V001")
    assert lazy_idle > 0.8 > norm_idle and lazy_earn < norm_earn / 2


def test_expense_file_is_written_atomically_and_mostly_clean(tmp_path):
    random.seed(0)
    path = batch_producer.write_expense_file(date(2026, 1, 2), random.Random(3), str(tmp_path))
    assert path.endswith("expenses_2026-01-02.csv")
    assert list(tmp_path.glob("*.tmp")) == []
    rows = list(csv.DictReader(open(path)))
    assert list(rows[0].keys()) == ["vehicle_id", "fuel_cost", "maintenance_cost", "distance_covered", "service_flag"]
    clean, rejected = clean_expenses(rows, set(vehicle_ids()))
    assert len(clean) >= 24                                            # dirty rows are the minority
    assert len(clean) + len(rejected) >= 30
