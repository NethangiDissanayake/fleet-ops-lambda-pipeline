"""Geo enrichment: map a GPS point to a city "zone".

The simulated fleet drives inside a bounding box around Colombo. We split the box into a
3x3 grid -> zones Z1..Z9 (row-major, south-west = Z1). A real system would use polygons
(e.g. PostGIS) - a grid is enough to demonstrate enrichment and per-area aggregation.

`zone_for` (pure Python) is used by the simulators and tests.
`zone_expr` builds the *same* logic as a Spark column expression so the streaming job
stays consistent with it (there is a unit test that checks both agree).
"""
LAT_MIN, LAT_MAX = 6.85, 7.00
LON_MIN, LON_MAX = 79.82, 79.92
GRID = 3  # GRID x GRID zones

ZONE_NAMES = [f"Z{i}" for i in range(1, GRID * GRID + 1)]


def _cell(value: float, lo: float, hi: float) -> int:
    idx = int((value - lo) / (hi - lo) * GRID)
    return min(max(idx, 0), GRID - 1)  # clamp so points on the edge stay in the grid


def zone_for(lat: float, lon: float) -> str:
    row = _cell(lat, LAT_MIN, LAT_MAX)
    col = _cell(lon, LON_MIN, LON_MAX)
    return f"Z{row * GRID + col + 1}"


def zone_expr(lat_col, lon_col):
    """Spark column expression equivalent of zone_for (imports pyspark lazily)."""
    from pyspark.sql import functions as F

    def cell(c, lo, hi):
        raw = F.floor((c - F.lit(lo)) / F.lit(hi - lo) * F.lit(GRID)).cast("int")
        return F.least(F.greatest(raw, F.lit(0)), F.lit(GRID - 1))

    row = cell(lat_col, LAT_MIN, LAT_MAX)
    col = cell(lon_col, LON_MIN, LON_MAX)
    return F.concat(F.lit("Z"), (row * F.lit(GRID) + col + F.lit(1)).cast("string"))
