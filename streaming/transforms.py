"""Spark DataFrame transformations for the speed layer.

They only use ordinary DataFrame operations, so the same code works on a streaming DataFrame
(in production) and on a static DataFrame (in the unit tests).

    raw Kafka rows --parse_events--> typed rows + reject_reason
                   --enrich------->  + zone, sim_date, sim_hour, earned, trip flags
                   --aggregate_*-->  small result sets that the sink writes to PostgreSQL
"""
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from common.validation import GEO_MARGIN, MAX_SPEED_KMH, VALID_STATUSES
from common.zones import LAT_MAX, LAT_MIN, LON_MAX, LON_MIN, zone_expr

EVENT_SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("trip_id", T.StringType()),
    T.StructField("driver_id", T.StringType()),
    T.StructField("vehicle_id", T.StringType()),
    T.StructField("lat", T.DoubleType()),
    T.StructField("lon", T.DoubleType()),
    T.StructField("speed", T.DoubleType()),
    T.StructField("status", T.StringType()),
    T.StructField("fare", T.DoubleType()),
    T.StructField("trip_completed", T.BooleanType()),
    T.StructField("timestamp", T.StringType()),
    T.StructField("sim_time", T.StringType()),
    T.StructField("_corrupt_record", T.StringType()),     # filled by from_json when the text is not valid JSON
])

LAKE_COLUMNS = ["event_id", "trip_id", "driver_id", "vehicle_id", "lat", "lon", "speed", "status",
                "fare", "trip_completed", "event_time", "sim_time", "sim_hour", "zone", "sim_date"]


def parse_events(raw):
    """raw needs columns: value (bytes/string JSON) and timestamp (Kafka ingest time).

    Adds reject_reason (NULL = valid). Bad records are KEPT (not silently dropped) so they can be
    counted, logged and archived in a dead-letter folder.
    """
    df = (raw.select(F.col("value").cast("string").alias("raw_json"),
                     F.col("timestamp").alias("kafka_ts"))
             .withColumn("j", F.from_json("raw_json", EVENT_SCHEMA,
                                          {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"})))
    df = df.select(
        "raw_json", "kafka_ts",
        (F.col("j").isNull() | F.col("j._corrupt_record").isNotNull()).alias("_unparseable"),
        F.col("j.event_id").alias("event_id"),
        F.col("j.trip_id").alias("trip_id"),
        F.col("j.driver_id").alias("driver_id"),
        F.col("j.vehicle_id").alias("vehicle_id"),
        F.col("j.lat").alias("lat"),
        F.col("j.lon").alias("lon"),
        F.col("j.speed").alias("speed"),
        F.col("j.status").alias("status"),
        F.col("j.fare").alias("fare"),
        F.coalesce(F.col("j.trip_completed"), F.lit(False)).alias("trip_completed"),
        F.col("j.timestamp").cast("timestamp").alias("_parsed_ts"),
        F.col("j.sim_time").cast("timestamp").alias("sim_time"),
    )
    # event_time is NEVER null (falls back to Kafka's ingest time) so watermarking always works.
    df = df.withColumn("event_time", F.coalesce("_parsed_ts", "kafka_ts"))
    df = df.withColumn("event_key", F.coalesce("event_id", F.sha2("raw_json", 256)))

    reason = (
        F.when(F.col("_unparseable"), "unparseable_json")
        .when(F.col("event_id").isNull() | F.col("vehicle_id").isNull(), "missing_key_field")
        .when(F.col("_parsed_ts").isNull(), "bad_timestamp")
        .when(~F.col("status").isin(*VALID_STATUSES) | F.col("status").isNull(), "bad_status")
        .when(F.col("lat").isNull() | F.col("lon").isNull(), "missing_gps")
        .when(~F.col("lat").between(LAT_MIN - GEO_MARGIN, LAT_MAX + GEO_MARGIN)
              | ~F.col("lon").between(LON_MIN - GEO_MARGIN, LON_MAX + GEO_MARGIN), "gps_out_of_range")
        .when(F.col("speed").isNull() | ~F.col("speed").between(0.0, MAX_SPEED_KMH), "bad_speed")
        .when(F.col("fare").isNull() | (F.col("fare") < 0), "bad_fare")
    )
    return df.withColumn("reject_reason", reason).drop("_unparseable", "_parsed_ts")


def enrich(valid):
    """Add business columns to VALID events: zone (geo), simulated date/hour, earned amount."""
    sim_ts = F.coalesce(F.col("sim_time"), F.col("event_time"))
    return (valid
            .withColumn("zone", zone_expr(F.col("lat"), F.col("lon")))
            .withColumn("sim_date", F.to_date(sim_ts))
            .withColumn("sim_hour", F.hour(sim_ts))
            .withColumn("trips_done", F.col("trip_completed").cast("int"))
            .withColumn("earned", F.when(F.col("trip_completed"), F.col("fare")).otherwise(F.lit(0.0))))


def _measures(with_enroute=False, with_on_trip=False):
    m = [F.count(F.lit(1)).alias("events"),
         F.sum((F.col("status") == "idle").cast("int")).alias("idle_events")]
    if with_enroute:
        m.append(F.sum((F.col("status") == "enroute").cast("int")).alias("enroute_events"))
    if with_on_trip:
        m.append(F.sum((F.col("status") == "on_trip").cast("int")).alias("on_trip_events"))
    m += [F.sum("trips_done").alias("trips"), F.round(F.sum("earned"), 2).alias("earnings")]
    return m


def aggregate_zone_1m(valid):
    """Tumbling 1-minute window per zone (event time)."""
    return (valid.groupBy(F.window("event_time", "1 minute").getField("start").alias("window_start"), "zone")
                 .agg(*_measures(with_enroute=True, with_on_trip=True)))


def aggregate_zone_hourly(valid):
    """Time-of-day view: simulated date + hour + zone."""
    return valid.groupBy("sim_date", "sim_hour", "zone").agg(*_measures())


def aggregate_vehicle_daily(valid):
    return valid.groupBy("sim_date", "vehicle_id").agg(*_measures(with_on_trip=True))


def summarise_vehicles(valid):
    """ONE row per vehicle: its newest position/status in the batch + facts needed to track idling."""
    w = Window.partitionBy("vehicle_id")
    v = valid.withColumn("_last_non_idle",
                         F.max(F.when(F.col("status") != "idle", F.col("event_time"))).over(w))
    trailing_idle = (F.col("status") == "idle") & (
        F.col("_last_non_idle").isNull() | (F.col("event_time") > F.col("_last_non_idle")))
    return v.groupBy("vehicle_id").agg(
        F.expr("max_by(driver_id, event_time)").alias("driver_id"),
        F.expr("max_by(status, event_time)").alias("status"),
        F.expr("max_by(lat, event_time)").alias("lat"),
        F.expr("max_by(lon, event_time)").alias("lon"),
        F.expr("max_by(speed, event_time)").alias("speed"),
        F.expr("max_by(zone, event_time)").alias("zone"),
        F.max("event_time").alias("last_ts"),
        F.max(F.col("_last_non_idle").isNotNull().cast("int")).alias("had_non_idle"),
        F.min(F.when(trailing_idle, F.col("event_time"))).alias("batch_idle_start"),
    )
