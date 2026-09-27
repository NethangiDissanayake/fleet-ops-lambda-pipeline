"""STREAMING SOURCE - simulated GPS/telemetry from a ride-hailing fleet -> Apache Kafka.

Every TICK_SECONDS (default 2 s) each of the NUM_VEHICLES vehicles emits one JSON event to the
Kafka topic. The vehicle_id is the message KEY, so all events of one vehicle go to the same
partition and keep their order (needed for correct "idle since" tracking).

To make the pipeline's cleaning / de-duplication logic meaningful we inject faults:
    * BAD_EVENT_RATE   (default 1 %)   corrupt events (missing GPS, negative speed, unknown status,
                                       or a non-JSON payload)
    * DUPLICATE_RATE   (default 0.5 %) the same event re-sent (like a producer retry)

Prometheus metrics are served on :8001/metrics.
"""
import json
import os
import random
import signal
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError
from prometheus_client import Counter, Gauge, start_http_server

from common import config
from common.logging_utils import error, get_logger, info, warning
from common.simclock import load_clock
from simulators.fleet import NUM_VEHICLES, vehicle_ids
from simulators.vehicle import Vehicle

log = get_logger("stream-producer", stage="ingestion")

TICK_SECONDS = config.TICK_SECONDS
BAD_EVENT_RATE = float(os.environ.get("BAD_EVENT_RATE", "0.01"))
DUPLICATE_RATE = float(os.environ.get("DUPLICATE_RATE", "0.005"))
METRICS_PORT = int(os.environ.get("METRICS_PORT", "8001"))
SEED = os.environ.get("RANDOM_SEED")

EVENTS_PRODUCED = Counter("stream_events_produced_total", "Events successfully sent to Kafka", ["kind"])
PRODUCE_ERRORS = Counter("stream_produce_errors_total", "Failed sends to Kafka")
LAST_EVENT_TS = Gauge("stream_last_event_timestamp_seconds", "Unix time of the last event produced")
VEHICLES = Gauge("stream_vehicles_simulated", "Number of simulated vehicles")

_running = True


def _stop(*_):
    global _running
    _running = False


def corrupt(event: dict, rng: random.Random):
    """Return a damaged copy of the event (or raw bytes that are not JSON at all)."""
    kind = rng.choice(["no_gps", "neg_speed", "huge_speed", "bad_status", "garbage"])
    if kind == "garbage":
        return b'{"vehicle_id": "V001", "lat": '   # truncated JSON
    bad = dict(event)
    if kind == "no_gps":
        bad["lat"] = None
    elif kind == "neg_speed":
        bad["speed"] = -12.5
    elif kind == "huge_speed":
        bad["speed"] = 999.0
    else:
        bad["status"] = "teleporting"
    return bad


def make_producer() -> KafkaProducer:
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=config.KAFKA_BOOTSTRAP,
                acks="all",                 # wait for the broker to confirm -> no silent loss
                retries=5,
                linger_ms=50,               # small batching for throughput
                key_serializer=lambda k: k.encode("utf-8"),
                value_serializer=lambda v: v if isinstance(v, bytes) else json.dumps(v).encode("utf-8"),
            )
        except KafkaError as exc:
            warning(log, "Kafka not ready, retrying in 3s", error=str(exc))
            time.sleep(3)


def main():
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    start_http_server(METRICS_PORT)

    clock = load_clock()
    rng = random.Random(int(SEED)) if SEED else random.Random()
    fleet = [Vehicle(v, rng) for v in vehicle_ids(NUM_VEHICLES)]
    VEHICLES.set(len(fleet))
    producer = make_producer()
    info(log, "stream producer started", topic=config.KAFKA_TOPIC, vehicles=len(fleet),
         tick_seconds=TICK_SECONDS, sim_day_seconds=clock.day_seconds,
         bad_event_rate=BAD_EVENT_RATE, duplicate_rate=DUPLICATE_RATE)

    sent_total, last_report = 0, time.time()
    while _running:
        loop_start = time.time()
        now = datetime.now(timezone.utc)
        sim_time = clock.sim_datetime(now.timestamp())
        sim_hour = sim_time.hour

        for vehicle in fleet:
            event = vehicle.step(sim_hour, sim_time, now)
            payloads = [("good", event)]
            if rng.random() < DUPLICATE_RATE:
                payloads.append(("duplicate", event))          # exact re-send
            if rng.random() < BAD_EVENT_RATE:
                payloads = [("bad", corrupt(event, rng))]       # replace with a broken one
            for kind, payload in payloads:
                try:
                    producer.send(config.KAFKA_TOPIC, key=vehicle.vehicle_id, value=payload)
                    EVENTS_PRODUCED.labels(kind=kind).inc()
                    sent_total += 1
                except KafkaError as exc:
                    PRODUCE_ERRORS.inc()
                    error(log, "send failed", vehicle_id=vehicle.vehicle_id, error=str(exc))
        LAST_EVENT_TS.set(time.time())

        if time.time() - last_report >= 30:
            info(log, "producer heartbeat", events_sent=sent_total, sim_time=sim_time.isoformat(),
                 sim_date=str(clock.sim_date(now.timestamp())))
            last_report = time.time()

        time.sleep(max(0.0, TICK_SECONDS - (time.time() - loop_start)))

    info(log, "shutting down - flushing", events_sent=sent_total)
    producer.flush(10)
    producer.close()


if __name__ == "__main__":
    main()
