from datetime import datetime, timedelta, timezone

from common.state import idle_seconds, next_vehicle_state

T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


def summ(status, last, had_non_idle=False, idle_start=None):
    return {"vehicle_id": "V001", "driver_id": "D001", "status": status, "lat": 6.9, "lon": 79.85, "speed": 0.0,
            "zone": "Z5", "last_ts": last, "had_non_idle": had_non_idle, "batch_idle_start": idle_start}


def test_new_idle_vehicle_starts_idle_run():
    st = next_vehicle_state(None, summ("idle", T0 + timedelta(seconds=10), idle_start=T0))
    assert st["idle_since"] == T0 and idle_seconds(st) == 10


def test_idle_run_continues_across_batches():
    first = next_vehicle_state(None, summ("idle", T0 + timedelta(seconds=10), idle_start=T0))
    second = next_vehicle_state(first, summ("idle", T0 + timedelta(seconds=80), idle_start=T0 + timedelta(seconds=20)))
    assert second["idle_since"] == T0 and idle_seconds(second) == 80


def test_working_resets_and_new_idle_run_restarts():
    idle = next_vehicle_state(None, summ("idle", T0 + timedelta(seconds=50), idle_start=T0))
    busy = next_vehicle_state(idle, summ("on_trip", T0 + timedelta(seconds=60), had_non_idle=True))
    assert busy["idle_since"] is None and idle_seconds(busy) == 0
    # idle -> trip -> idle inside ONE batch: the idle run must start after the trip
    again = next_vehicle_state(idle, summ("idle", T0 + timedelta(seconds=90), had_non_idle=True,
                                          idle_start=T0 + timedelta(seconds=70)))
    assert again["idle_since"] == T0 + timedelta(seconds=70)


def test_out_of_order_batch_ignored():
    newer = next_vehicle_state(None, summ("idle", T0 + timedelta(seconds=100), idle_start=T0))
    result = next_vehicle_state(newer, summ("on_trip", T0 + timedelta(seconds=5), had_non_idle=True))
    assert result is newer
