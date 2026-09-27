"""Per-vehicle "current state" logic used by the speed layer (pure Python, unit-tested).

The Spark job reduces each micro-batch to ONE summary row per vehicle. `next_vehicle_state`
merges that summary with what we already stored, keeping track of how long a vehicle has been
continuously idle (`idle_since`) - this is what drives the "idle too long" alert.
"""


def next_vehicle_state(prior, summary):
    """
    prior   : previous stored state dict (or None for a new vehicle)
    summary : {vehicle_id, driver_id, status, lat, lon, speed, zone, last_ts,
               had_non_idle (bool)  - did the batch contain any non-idle event?,
               batch_idle_start     - first idle event of the trailing idle run in this batch}
    """
    # Ignore batches that are older than what we already know (out-of-order protection).
    if prior and prior.get("last_ts") and summary["last_ts"] < prior["last_ts"]:
        return prior

    if summary["status"] != "idle":
        idle_since = None
    elif (prior and prior.get("status") == "idle" and prior.get("idle_since")
          and not summary["had_non_idle"]):
        idle_since = prior["idle_since"]              # still the same idle run
    else:
        idle_since = summary["batch_idle_start"] or summary["last_ts"]  # a new idle run began

    return {
        "vehicle_id": summary["vehicle_id"], "driver_id": summary["driver_id"],
        "status": summary["status"], "lat": summary["lat"], "lon": summary["lon"],
        "speed": summary["speed"], "zone": summary["zone"], "last_ts": summary["last_ts"],
        "idle_since": idle_since,
    }


def idle_seconds(state) -> float:
    if state["status"] != "idle" or not state.get("idle_since"):
        return 0.0
    return (state["last_ts"] - state["idle_since"]).total_seconds()
