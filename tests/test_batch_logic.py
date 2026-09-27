import pytest

from common.batch_logic import classify, clean_expenses, reconcile

KNOWN = {"V001", "V002", "V003"}


def row(vid="V001", fuel="1000", maint="200", dist="60", flag="false"):
    return {"vehicle_id": vid, "fuel_cost": fuel, "maintenance_cost": maint, "distance_covered": dist,
            "service_flag": flag}


def test_clean_accepts_good_rows_and_normalises():
    clean, rejected = clean_expenses([row(" v001 ", flag="TRUE")], KNOWN)
    assert rejected == []
    assert clean == [{"vehicle_id": "V001", "fuel_cost": 1000.0, "maintenance_cost": 200.0,
                      "distance_covered": 60.0, "service_flag": True}]


@pytest.mark.parametrize("bad,reason", [
    (row(fuel=""), "invalid_fuel_cost"),
    (row(maint="-5"), "negative_value"),
    (row(dist="N/A"), "invalid_distance_covered"),
    (row(vid="V999"), "unknown_vehicle"),
    (row(vid=""), "missing_vehicle_id"),
    (row(flag="maybe"), "invalid_service_flag"),
])
def test_clean_rejects_with_reason(bad, reason):
    clean, rejected = clean_expenses([bad], KNOWN)
    assert clean == []
    assert rejected[0]["reason"] == reason
    assert rejected[0]["raw_row"] == bad          # raw row kept for audit


def test_exact_duplicate_rejected_but_partner_rows_are_combined():
    clean, rejected = clean_expenses([row(), row()], KNOWN)
    assert len(clean) == 1 and rejected[0]["reason"] == "duplicate_row"

    fuel_partner = row(fuel="800", maint="0", dist="70", flag="false")
    garage = row(fuel="0", maint="1500", dist="0", flag="true")
    clean, rejected = clean_expenses([fuel_partner, garage], KNOWN)
    assert rejected == []
    assert clean[0]["fuel_cost"] == 800 and clean[0]["maintenance_cost"] == 1500
    assert clean[0]["distance_covered"] == 70 and clean[0]["service_flag"] is True


def test_classify_thresholds():
    assert classify(-1, None, 0.15) == "UNPROFITABLE"
    assert classify(50, 0.05, 0.15) == "AT_RISK"
    assert classify(500, 0.5, 0.15) == "PROFITABLE"


def test_reconcile_profit_and_status():
    batch = {"V001": {"events": 100, "idle_events": 20, "trips": 5, "earnings": 5000.0},
             "V002": {"events": 100, "idle_events": 90, "trips": 1, "earnings": 700.0}}
    exp = {"V001": {"fuel_cost": 2000.0, "maintenance_cost": 500.0, "distance_covered": 60.0, "service_flag": False},
           "V002": {"fuel_cost": 1500.0, "maintenance_cost": 900.0, "distance_covered": 30.0, "service_flag": True}}
    out = {r["vehicle_id"]: r for r in reconcile(batch, exp, {"V001": {"earnings": 4900.0}}, {"V001": 3000.0, "V002": 100.0})}
    assert out["V001"]["profit"] == 2500.0 and out["V001"]["margin"] == 0.5
    assert out["V001"]["status"] == "PROFITABLE" and out["V001"]["drift"] == 100.0
    assert out["V001"]["profit_change"] == -500.0 and out["V001"]["declining"] is False   # still profitable
    assert out["V002"]["profit"] == -1700.0 and out["V002"]["status"] == "UNPROFITABLE"
    assert out["V002"]["declining"] is True and out["V002"]["idle_ratio"] == 0.9


def test_reconcile_missing_sources():
    out = {r["vehicle_id"]: r for r in reconcile(
        {"V001": {"events": 10, "idle_events": 0, "trips": 1, "earnings": 900.0}},
        {"V002": {"fuel_cost": 100.0, "maintenance_cost": 50.0, "distance_covered": 10.0, "service_flag": False}})}
    assert out["V001"]["status"] == "NO_EXPENSE_DATA" and out["V001"]["profit"] is None
    assert out["V002"]["status"] == "NO_STREAM_DATA" and out["V002"]["profit"] == -150.0
