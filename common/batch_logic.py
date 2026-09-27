"""Pure business logic for the BATCH layer (no Spark/Airflow/DB imports -> easy to unit-test).

Three steps used by the Airflow DAG:
    1. clean_expenses()   - validate / normalise the daily garage+fuel CSV
    2. reconcile()        - join yesterday's earnings (from the stream) with yesterday's costs
    3. classify()         - decide PROFITABLE / AT_RISK / UNPROFITABLE
"""
from common import config

EXPECTED_COLUMNS = ["vehicle_id", "fuel_cost", "maintenance_cost", "distance_covered", "service_flag"]

_TRUE = {"true", "1", "yes", "y", "t"}
_FALSE = {"false", "0", "no", "n", "f", ""}


def _to_float(value):
    """Return float or None when the value is empty / not numeric."""
    if value is None:
        return None
    text = str(value).strip()
    if text == "" or text.lower() in {"n/a", "na", "nan", "null", "none"}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _to_bool(value):
    """Return True/False, or None when the flag is garbage."""
    text = "" if value is None else str(value).strip().lower()
    if text in _TRUE:
        return True
    if text in _FALSE:
        return False
    return None


def clean_expenses(rows, known_vehicles=None):
    """Validate raw CSV rows (list of dicts of strings).

    Returns (clean, rejected):
      clean    - one dict per vehicle: vehicle_id, fuel_cost, maintenance_cost, distance_covered,
                 service_flag  (repeat rows for a vehicle - e.g. a fuel partner row plus a garage
                 row - are combined: costs summed, distance = max, service_flag = any)
      rejected - dicts {raw_row, reason} for rows that failed a rule (kept for audit)
    """
    accepted, rejected, seen_exact = [], [], set()

    for raw in rows:
        vehicle_id = (raw.get("vehicle_id") or "").strip().upper()
        reason = None
        fuel = maint = dist = None
        flag = None

        if not vehicle_id:
            reason = "missing_vehicle_id"
        elif known_vehicles is not None and vehicle_id not in known_vehicles:
            reason = "unknown_vehicle"
        else:
            fuel = _to_float(raw.get("fuel_cost"))
            maint = _to_float(raw.get("maintenance_cost"))
            dist = _to_float(raw.get("distance_covered"))
            flag = _to_bool(raw.get("service_flag"))
            if fuel is None:
                reason = "invalid_fuel_cost"
            elif maint is None:
                reason = "invalid_maintenance_cost"
            elif dist is None:
                reason = "invalid_distance_covered"
            elif fuel < 0 or maint < 0 or dist < 0:
                reason = "negative_value"
            elif flag is None:
                reason = "invalid_service_flag"

        if reason is None:
            fingerprint = (vehicle_id, fuel, maint, dist, flag)
            if fingerprint in seen_exact:
                reason = "duplicate_row"
            else:
                seen_exact.add(fingerprint)

        if reason:
            rejected.append({"raw_row": dict(raw), "reason": reason})
        else:
            accepted.append(
                {"vehicle_id": vehicle_id, "fuel_cost": fuel, "maintenance_cost": maint,
                 "distance_covered": dist, "service_flag": flag}
            )

    merged = {}
    for row in accepted:
        cur = merged.get(row["vehicle_id"])
        if cur is None:
            merged[row["vehicle_id"]] = dict(row)
        else:
            cur["fuel_cost"] += row["fuel_cost"]
            cur["maintenance_cost"] += row["maintenance_cost"]
            cur["distance_covered"] = max(cur["distance_covered"], row["distance_covered"])
            cur["service_flag"] = cur["service_flag"] or row["service_flag"]
    return list(merged.values()), rejected


def classify(profit, margin, at_risk_margin=None):
    at_risk_margin = config.AT_RISK_MARGIN if at_risk_margin is None else at_risk_margin
    if profit < 0:
        return "UNPROFITABLE"
    if margin is not None and margin < at_risk_margin:
        return "AT_RISK"
    return "PROFITABLE"


def reconcile(batch_view, expenses, speed_view=None, prev_profit=None):
    """Join per-vehicle earnings with per-vehicle costs for ONE simulated day.

    batch_view : {vehicle_id: {"trips", "earnings", "events", "idle_events"}}  (recomputed from the lake)
    expenses   : {vehicle_id: cleaned expense dict}
    speed_view : {vehicle_id: {"earnings": ...}} what the speed layer had accumulated (for drift)
    prev_profit: {vehicle_id: profit of the previous simulated day} (for the trend flag)
    """
    speed_view = speed_view or {}
    prev_profit = prev_profit or {}
    out = []
    for vid in sorted(set(batch_view) | set(expenses)):
        b = batch_view.get(vid)
        e = expenses.get(vid)
        earnings = round(b["earnings"], 2) if b else 0.0
        trips = b["trips"] if b else 0
        events = b["events"] if b else 0
        idle_ratio = round(b["idle_events"] / events, 3) if b and events else None
        rt = speed_view.get(vid, {}).get("earnings")
        drift = round(earnings - float(rt), 2) if rt is not None else None

        row = {
            "vehicle_id": vid, "trips": trips, "earnings": earnings, "idle_ratio": idle_ratio,
            "speed_layer_earnings": float(rt) if rt is not None else None, "drift": drift,
            "fuel_cost": None, "maintenance_cost": None, "total_cost": None,
            "distance_covered": None, "service_flag": None, "profit": None, "margin": None,
            "cost_per_km": None, "profit_change": None, "declining": False, "status": None,
        }
        if e is None:
            row["status"] = "NO_EXPENSE_DATA"
        elif b is None:
            row.update(fuel_cost=e["fuel_cost"], maintenance_cost=e["maintenance_cost"],
                       distance_covered=e["distance_covered"], service_flag=e["service_flag"],
                       total_cost=round(e["fuel_cost"] + e["maintenance_cost"], 2))
            row["profit"] = round(-row["total_cost"], 2)
            row["status"] = "NO_STREAM_DATA"
        else:
            total = round(e["fuel_cost"] + e["maintenance_cost"], 2)
            profit = round(earnings - total, 2)
            margin = round(profit / earnings, 3) if earnings > 0 else None
            row.update(
                fuel_cost=e["fuel_cost"], maintenance_cost=e["maintenance_cost"], total_cost=total,
                distance_covered=e["distance_covered"], service_flag=e["service_flag"],
                profit=profit, margin=margin,
                cost_per_km=round(total / e["distance_covered"], 2) if e["distance_covered"] else None,
                status=classify(profit, margin),
            )
        if row["profit"] is not None and vid in prev_profit and prev_profit[vid] is not None:
            row["profit_change"] = round(row["profit"] - float(prev_profit[vid]), 2)
            row["declining"] = row["profit_change"] < 0 and row["status"] != "PROFITABLE"
        out.append(row)
    return out
