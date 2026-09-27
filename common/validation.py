"""Data-quality rules for streaming events (single source of truth).

The pure-Python `event_problem` is used by tests and documentation; the Spark job applies
the same bounds as column expressions (see streaming/spark_job.py -> `valid_condition`).
"""
from common.zones import LAT_MAX, LAT_MIN, LON_MAX, LON_MIN

VALID_STATUSES = ("idle", "enroute", "on_trip")
MAX_SPEED_KMH = 150.0
# allow the GPS to be slightly outside the operating box before we call it corrupt
GEO_MARGIN = 0.02


def event_problem(ev: dict) -> str | None:
    """Return None when the event is valid, otherwise a short reason string."""
    if not isinstance(ev, dict):
        return "not_an_object"
    for field in ("event_id", "vehicle_id", "timestamp", "status"):
        if not ev.get(field):
            return f"missing_{field}"
    if ev["status"] not in VALID_STATUSES:
        return "bad_status"
    lat, lon, speed, fare = ev.get("lat"), ev.get("lon"), ev.get("speed"), ev.get("fare")
    if lat is None or lon is None:
        return "missing_gps"
    if not (LAT_MIN - GEO_MARGIN <= lat <= LAT_MAX + GEO_MARGIN
            and LON_MIN - GEO_MARGIN <= lon <= LON_MAX + GEO_MARGIN):
        return "gps_out_of_range"
    if speed is None or not (0 <= speed <= MAX_SPEED_KMH):
        return "bad_speed"
    if fare is None or fare < 0:
        return "bad_fare"
    return None
