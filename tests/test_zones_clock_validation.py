from datetime import date, datetime

from common.simclock import SimClock
from common.validation import event_problem
from common.zones import LAT_MAX, LAT_MIN, LON_MAX, LON_MIN, zone_for


# ------------------------------------------------------------------ zones
def test_zone_corners_and_centre():
    assert zone_for(LAT_MIN, LON_MIN) == "Z1"            # south-west
    assert zone_for(LAT_MIN, LON_MAX) == "Z3"            # south-east
    assert zone_for(LAT_MAX, LON_MIN) == "Z7"            # north-west
    assert zone_for(LAT_MAX, LON_MAX) == "Z9"            # north-east (edge is clamped, not Z10+)
    assert zone_for((LAT_MIN + LAT_MAX) / 2, (LON_MIN + LON_MAX) / 2) == "Z5"


def test_zone_outside_box_is_clamped():
    assert zone_for(0.0, 0.0) == "Z1"
    assert zone_for(90.0, 180.0) == "Z9"


# ------------------------------------------------------------------ simulated clock
def test_clock_day_and_hour():
    clock = SimClock(epoch_unix=1000.0, day_seconds=300, start_date=date(2026, 1, 1))
    assert clock.day_index(1000.0) == 0
    assert clock.sim_date(1000.0 + 299) == date(2026, 1, 1)
    assert clock.sim_date(1000.0 + 300) == date(2026, 1, 2)
    assert clock.sim_hour(1000.0 + 150) == 12                      # half a day -> noon
    assert clock.sim_datetime(1000.0 + 75) == datetime(2026, 1, 1, 6, 0, 0)
    assert clock.sim_hour_seconds == 12.5


def test_clock_day_boundaries_roundtrip():
    clock = SimClock(2000.0, 300, date(2026, 1, 1))
    d = date(2026, 1, 4)
    assert clock.day_start_real_ts(d) == 2000.0 + 3 * 300
    assert clock.day_end_real_ts(d) == clock.day_start_real_ts(date(2026, 1, 5))


def test_clock_never_negative():
    clock = SimClock(5000.0, 300, date(2026, 1, 1))
    assert clock.day_index(10.0) == 0


# ------------------------------------------------------------------ validation rules
def good():
    return {"event_id": "e1", "vehicle_id": "V001", "timestamp": "2026-01-01T00:00:00Z", "status": "idle",
            "lat": 6.9, "lon": 79.85, "speed": 0.0, "fare": 0.0}


def test_valid_event_passes():
    assert event_problem(good()) is None


def test_invalid_events_are_named():
    cases = {"missing_event_id": {"event_id": None}, "bad_status": {"status": "flying"},
             "missing_gps": {"lat": None}, "gps_out_of_range": {"lat": 50.0}, "bad_speed": {"speed": -1.0},
             "bad_fare": {"fare": -5.0}}
    for reason, patch in cases.items():
        assert event_problem({**good(), **patch}) == reason
    assert event_problem({**good(), "speed": 999.0}) == "bad_speed"
    assert event_problem("not a dict") == "not_an_object"
