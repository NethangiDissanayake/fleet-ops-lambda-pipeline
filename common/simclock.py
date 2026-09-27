"""Simulated clock.

We compress time so a "day" can be demonstrated quickly:

    1 simulated day  =  SIM_DAY_SECONDS real seconds  (default 300 s = 5 minutes)
    1 simulated hour =  SIM_DAY_SECONDS / 24          (default 12.5 real seconds)

All services must agree on "what simulated day is it?", so the anchor (epoch) is stored ONCE
in the Postgres table `sim_clock` when the database is first created. Every container reads it
at start-up, which means there is nothing to configure by hand.

    sim_time(real_ts) = start_date 00:00  +  (real_ts - epoch) * (86400 / day_seconds)
"""
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta


@dataclass(frozen=True)
class SimClock:
    epoch_unix: float          # real unix time that corresponds to simulated start_date 00:00
    day_seconds: int = 300     # real seconds per simulated day
    start_date: date = date(2026, 1, 1)

    # -- conversions ---------------------------------------------------------
    def elapsed(self, real_ts: float | None = None) -> float:
        real_ts = time.time() if real_ts is None else real_ts
        return max(real_ts - self.epoch_unix, 0.0)

    def day_index(self, real_ts: float | None = None) -> int:
        """0 for the first simulated day, 1 for the second, ..."""
        return int(self.elapsed(real_ts) // self.day_seconds)

    def sim_date(self, real_ts: float | None = None) -> date:
        return self.start_date + timedelta(days=self.day_index(real_ts))

    def sim_datetime(self, real_ts: float | None = None) -> datetime:
        scale = 86400 / self.day_seconds
        base = datetime.combine(self.start_date, datetime.min.time())
        return base + timedelta(seconds=self.elapsed(real_ts) * scale)

    def sim_hour(self, real_ts: float | None = None) -> int:
        return self.sim_datetime(real_ts).hour

    def day_start_real_ts(self, day: date) -> float:
        """Real unix time at which simulated `day` begins."""
        return self.epoch_unix + (day - self.start_date).days * self.day_seconds

    def day_end_real_ts(self, day: date) -> float:
        return self.day_start_real_ts(day) + self.day_seconds

    @property
    def sim_hour_seconds(self) -> float:
        return self.day_seconds / 24


def load_clock(conn=None) -> SimClock:
    """Build the clock. Precedence: SIM_EPOCH_UNIX env var, else the sim_clock table."""
    from common import config

    start = date.fromisoformat(config.SIM_START_DATE)
    if config.SIM_EPOCH_UNIX:
        return SimClock(float(config.SIM_EPOCH_UNIX), config.SIM_DAY_SECONDS, start)

    from common.db import connect

    own = conn is None
    conn = conn or connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT epoch_unix, day_seconds, start_date FROM sim_clock WHERE id = 1")
            epoch, day_seconds, start_date = cur.fetchone()
        return SimClock(float(epoch), int(day_seconds), start_date)
    finally:
        if own:
            conn.close()
