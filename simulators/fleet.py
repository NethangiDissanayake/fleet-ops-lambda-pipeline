"""Definition of the simulated fleet, shared by both simulators.

Profiles create realistic variety so the report has something interesting to say:
    normal  - works steadily, earns a decent amount
    lazy    - stays idle for very long stretches -> triggers idle alerts, low earnings
    costly  - works normally but has high maintenance / fuel costs -> becomes unprofitable
"""
NUM_VEHICLES = 30

# vehicles V001..V030; the last 4 are "lazy", 3 before that are "costly"
LAZY = {f"V{i:03d}" for i in range(27, 31)}
COSTLY = {f"V{i:03d}" for i in range(24, 27)}


def vehicle_ids(n: int = NUM_VEHICLES):
    return [f"V{i:03d}" for i in range(1, n + 1)]


def driver_for(vehicle_id: str) -> str:
    return "D" + vehicle_id[1:]


def profile_for(vehicle_id: str) -> str:
    if vehicle_id in LAZY:
        return "lazy"
    if vehicle_id in COSTLY:
        return "costly"
    return "normal"
