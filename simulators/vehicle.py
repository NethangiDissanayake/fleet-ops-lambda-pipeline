"""One simulated vehicle = a tiny state machine:

        idle ──> enroute (driving to the passenger) ──> on_trip ──> (trip completed) ──> idle

Each call to `step()` = one "tick" (default every 2 real seconds) and returns ONE telemetry event.
The fare is metered while on_trip; the final event of a trip has trip_completed = True and carries
the final fare. Earnings therefore = SUM(fare) over events where trip_completed is true.

Lazy drivers stay idle for a long time (this is what triggers the idle alert).
"""
import random
import uuid
from datetime import datetime, timezone

from common.zones import LAT_MAX, LAT_MIN, LON_MAX, LON_MIN
from simulators.fleet import driver_for, profile_for

# Demand by simulated hour: busier in the morning/evening rush -> shorter idle gaps.
_DEMAND = {h: 0.6 for h in range(24)}
_DEMAND.update({7: 1.4, 8: 1.6, 9: 1.3, 12: 1.1, 13: 1.0, 17: 1.5, 18: 1.7, 19: 1.4, 20: 1.1})


def demand(sim_hour: int) -> float:
    return _DEMAND.get(sim_hour, 0.8)


class Vehicle:
    def __init__(self, vehicle_id: str, rng: random.Random):
        self.vehicle_id = vehicle_id
        self.driver_id = driver_for(vehicle_id)
        self.profile = profile_for(vehicle_id)
        self.rng = rng
        self.lat = rng.uniform(LAT_MIN, LAT_MAX)
        self.lon = rng.uniform(LON_MIN, LON_MAX)
        self.status = "idle"
        self.ticks_left = rng.randint(1, 4)
        self.trip_id = None
        self.trip_ticks = 0
        self.fare = 0.0
        self._target = (self.lat, self.lon)
        self._steps_total = 1

    # ---- helpers ---------------------------------------------------------------------
    def _random_point(self):
        return (self.rng.uniform(LAT_MIN, LAT_MAX), self.rng.uniform(LON_MIN, LON_MAX))

    def _plan_move(self, ticks):
        self._target = self._random_point()
        self._steps_total = max(ticks, 1)

    def _move(self):
        remaining = max(self.ticks_left, 1)
        self.lat += (self._target[0] - self.lat) / remaining
        self.lon += (self._target[1] - self.lon) / remaining

    def _idle_duration(self, sim_hour: int) -> int:
        if self.profile == "lazy":
            return self.rng.randint(60, 120)                # 2-4 minutes -> triggers alerts
        base = self.rng.randint(2, 6)                       # 4-12 s
        return max(1, round(base / demand(sim_hour)))

    # ---- one tick --------------------------------------------------------------------
    def step(self, sim_hour: int, sim_time: datetime, now: datetime | None = None) -> dict:
        now = now or datetime.now(timezone.utc)
        trip_completed = False
        speed = 0.0

        if self.status == "idle":
            speed = 0.0
            self.fare = 0.0
            self.ticks_left -= 1
            if self.ticks_left <= 0:
                self.status = "enroute"
                self.trip_id = f"T{uuid.uuid4().hex[:10]}"
                self.ticks_left = self.rng.randint(3, 5)
                self._plan_move(self.ticks_left)
        elif self.status == "enroute":
            speed = round(self.rng.uniform(20, 45), 1)
            self._move()
            self.ticks_left -= 1
            if self.ticks_left <= 0:
                self.status = "on_trip"
                self.trip_ticks = self.rng.randint(8, 20)
                self.ticks_left = self.trip_ticks
                self.fare = 250.0                              # base fare (LKR)
                self._plan_move(self.ticks_left)
        else:  # on_trip
            speed = round(self.rng.uniform(25, 60), 1)
            self._move()
            self.ticks_left -= 1
            self.fare = round(self.fare + self.rng.uniform(25, 45), 2)  # metered fare
            if self.ticks_left <= 0:
                trip_completed = True

        event = {
            "event_id": uuid.uuid4().hex,
            "trip_id": self.trip_id if self.status != "idle" else None,
            "driver_id": self.driver_id,
            "vehicle_id": self.vehicle_id,
            "lat": round(self.lat, 6),
            "lon": round(self.lon, 6),
            "speed": speed,
            "status": self.status,
            "fare": self.fare if self.status == "on_trip" else 0.0,
            "trip_completed": trip_completed,
            "timestamp": now.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "sim_time": sim_time.isoformat(timespec="seconds"),
        }

        if trip_completed:  # go back to idle for the next tick
            self.status = "idle"
            self.trip_id = None
            self.ticks_left = self._idle_duration(sim_hour)
        return event
