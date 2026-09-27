# Report guide (8-15 pages) - Use Case 1, Ride-Hailing Fleet Operations

Write this in **your own words**; the text below is raw material and reasoning you can adapt. Only claim things you
have run and observed. Marks: Architecture 20, Tech stack 10, Ingestion 15, Processing 15, Serving 10, Observability 10,
Report 15, Code quality 5.

## 1. Use case and requirements (1 page)
Business question: *fleet utilization and earnings by area/time-of-day right now, and which vehicles become unprofitable once
yesterday's fuel/maintenance costs are included.* Requirements you derived:

| Requirement | Latency / property | Consequence |
|---|---|---|
| Live utilization (active, idle ratio, trips/hour, earnings by zone) | seconds | streaming path |
| Alert when a vehicle is idle too long | seconds | stateful stream logic |
| Daily per-vehicle profitability | once per day, must be *correct* and auditable | batch recompute + join |
| Cost data arrives once per day, dirty, can be late/corrected | reprocessable | keep raw data, idempotent jobs |
| Observable, reproducible | - | metrics/logs/alerts, Docker Compose |

State assumptions: simulated clock (1 day = 5 min), Spark local mode, single broker, currency LKR, added fields
(`event_id`, `trip_completed`, `sim_time`), 30 vehicles, first day partial.

## 2. Architecture decision: Lambda vs Kappa (2 pages, worth 20 marks)
**Choice: Lambda.** Arguments (tie each to a requirement):

1. **Two sources with different natures.** A continuous stream and a small daily file. In Kappa the daily file would have to be
   turned into a stream/log and joined statefully; for a 30-row file that is more machinery than a plain daily batch join.
2. **Latency needs differ per output.** Utilization/alerts need seconds (speed layer). The profitability report needs
   completeness and correctness once a day (batch layer). One code path would compromise one of them.
3. **Replay/correction.** Cost providers can send corrections; late events happen. The batch layer recomputes a whole day from the
   immutable Parquet master dataset, so a fix means re-running one DAG day (`{"reprocess": [...]}`), not rewinding a stream.
   The Parquet archive is exactly Lambda's "master dataset".
4. **Consistency check built in.** The speed layer's running totals (`vehicle_daily_rt`) are compared with the recomputed batch
   totals (`vehicle_daily_batch`); the daily report contains `drift`. This is only possible because both paths exist.
5. **Cost.** Batch work is a cheap once-a-day job; the streaming job only keeps small aggregates (no long-term stream state).

**Rejected alternative: Kappa** (single streaming path, reprocess by replaying Kafka). Honest trade-offs to write:
* Kappa is **simpler**: one code base, no duplicated logic, no drift between two implementations. Our Lambda duplicates the
  "earnings = sum of fare where trip_completed" rule in Spark and pandas - the price of Lambda (mitigated by tests + drift metric).
* Kappa replay needs **long Kafka retention** (or tiered storage) and reprocessing jobs of the *whole* history compete with live
  processing; at our tiny volume (15 events/s ~ 1.3 M events/day) this would in fact be feasible - Kappa would be a defensible choice at
  this scale. We chose Lambda because of the daily reconciliation semantics and the auditability of a batch view, not because Kappa cannot work.
* Kafka log retention is time-limited; the Parquet archive is independent of it.
Mention what would change your mind: no daily authoritative report -> Kappa; very high volume of daily data -> Lambda even more.

Also explain the **delivery guarantees**: producer `acks=all` (at-least-once) -> possible duplicates -> Spark watermark + `dropDuplicates`
(2 min) -> idempotent, transactional sink (`processed_batches` ledger) -> exactly-once *effect*; Parquet written to `batch_id=` folders with
overwrite; batch view de-duplicates again by `event_id`.

## 3. Architecture diagram (1 page)
Use `docs/architecture.png` (or redraw in draw.io). Add a second small diagram: the micro-batch flow (Kafka -> parse -> dedupe ->
foreachBatch -> {Parquet, Postgres transaction}) and a third: the Airflow DAG task graph (screenshot from Airflow).
Explain each arrow: what data, what format, which protocol.

## 4. Technology stack and justification (1.5 pages, 10 marks) - tie to *this* use case
| Component | Why it fits this scenario | Alternative considered / why not |
|---|---|---|
| Kafka (3 partitions, key = vehicle_id) | durable buffer decoupling producers from processing; per-vehicle ordering is required for "idle since"; partitions allow parallel consumers as fleet grows | RabbitMQ: no replayable log; direct DB writes: no back-pressure/replay |
| Spark Structured Streaming | event-time windows, watermarks and dedup built in; same DataFrame API used in tests and batch; micro-batch (10 s) is enough for "seconds" latency | Storm/Flink: lower latency not needed; Storm has no built-in windows/exactly-once helpers of the same maturity for this scope |
| Airflow | daily DAG with dependencies, retries, backfill/reprocess, run history, UI for the demo | cron: no retries/visibility; Prefect/Dagster: not required by the brief |
| PostgreSQL | small aggregate results, needs upserts, transactions (ledger), SQL joins for reconciliation, ad-hoc queries by API and Grafana | Cassandra: strengths (huge write throughput) unneeded, weak joins; Parquet-only: no low-latency point queries |
| Parquet on a shared volume | immutable, columnar, cheap master dataset for recompute (S3/HDFS in production) | keep everything in Postgres: mixes raw + serving data |
| FastAPI | typed endpoints + free Swagger docs; easy Prometheus integration | Flask: no built-in validation/docs |
| Prometheus + Grafana | pull-based metrics, alert rules, dashboards on both Postgres and Prometheus | ELK: heavier, logs not metrics |
| Docker Compose | one-command reproducibility | Kubernetes: overkill for the demo |

## 5. Implementation summary (2 pages)
* **Ingestion:** simulator design (state machine, fault injection: 1 % bad, 0.5 % duplicates; batch file dirty rows, atomic write); Kafka topic config.
* **Processing:** validation rules with `reject_reason`; enrichment (zone grid, simulated date/hour); 1-minute tumbling windows per zone; per-vehicle state and
  idle-run tracking; alert lifecycle (one open alert per vehicle, auto-resolve); dead-letter Parquet for rejected rows.
* **Batch:** CSV cleaning rules (list them and the reasons), combination of partner rows, recompute from Parquet, join, classification
  (`UNPROFITABLE` profit < 0, `AT_RISK` margin < 15 %), trend vs previous day, drift, report generation.
* **Serving:** endpoints table; data model (which table serves which endpoint).
* **Idempotency & failure handling:** ledger, transactions, retries (Airflow), restart policies, checkpointing.

## 6. Observability design (1 page, 10 marks): what, how, why
| Signal | How | Why |
|---|---|---|
| Structured logs | JSON, `service` + `stage` on every line | trace a failure to ingestion / processing / storage / orchestration / serving |
| Ingestion metrics | `stream_events_produced_total{kind}`, `stream_produce_errors_total`, `batch_files_dropped_total` | is the source alive, is it sending faults |
| Processing metrics | `spark_events_received_total`, `spark_events_invalid_total{reason}`, `spark_batch_duration_seconds`, `spark_batch_failures_total`, `spark_last_batch_timestamp_seconds` | throughput, data quality, latency, failure |
| Storage/serving | `pipeline_seconds_since_last_event`, `pipeline_records_total`, `pipeline_db_up`, API request count/latency | end-to-end freshness measured *at the store* |
| Batch metrics | `batch_days_processed_total{result}`, `batch_last_success_timestamp_seconds`, `batch_last_rejected_rows`, `batch_last_max_abs_drift_lkr` | is the daily job succeeding, data quality, speed-vs-batch consistency |
| Alert rules | see `monitoring/alert_rules.yml` (no data >2 min, invalid rate >5 %, batch failed/stale, DB down, idle vehicles) | detect and diagnose failures |
| Health check | `GET /health` (503 if no data for 120 s) | load-balancer/uptime probe |
| Business alerts | `alerts` table via `/alerts` | operations reaction |
| Audit | `batch_runs`, `expenses_rejected`, `processed_batches` | traceability |
Include the screenshots from Part D and *explain the failure test* (stop the producer -> alert fires within ~2.5 min).
For "tracing" (rubric mentions it): each event carries `event_id`/`trip_id`; log lines carry `batch_id`/`sim_date` - state honestly that
distributed tracing (OpenTelemetry) was not implemented.

## 7. Results (2 pages)
Screenshots + interpretation: utilization by zone, time-of-day pattern (rush hours 7-9 and 17-19 have more trips / lower idle),
alerts for V027-V030, profitability report (which vehicles are UNPROFITABLE and why: costly maintenance vs lazy idling), drift value
(should be ~0; explain any non-zero value), rejected-row reasons, Spark throughput.
**Do not invent numbers** - copy them from your runs.

## 8. Limitations, trade-offs, production-scale changes (1 page) - be honest
* Time compression makes trips unrealistically short; earnings by "hour" is coarse (12.5 s per simulated hour).
* Spark local mode / single Kafka broker / single Postgres = no fault tolerance; production: Spark on Kubernetes/YARN, 3+ brokers with
  replication 3, Postgres HA or a columnar store (ClickHouse/Druid) for high-volume aggregates.
* Per-vehicle state is folded in Python after `collect()` (fine for hundreds of vehicles); production: `applyInPandasWithState` or Flink keyed state.
* Small Parquet files (one folder per micro-batch) -> compaction / Delta Lake or Iceberg; S3 instead of a Docker volume.
* Logic duplicated between speed and batch layers (Lambda's known cost); drift metric mitigates.
* Grid zones instead of geospatial polygons (PostGIS/H3).
* No Alertmanager, no distributed tracing, no authentication on the API, credentials in compose file (use secrets).
* Late data older than the 2-minute watermark is not de-duplicated by the speed layer (the batch layer still corrects it).
* Schema registry / Avro not used; JSON with a schema enforced in Spark.

## 9. Group contribution statement (if applicable), references, appendix (repo link, how to run).

---
## Viva questions (prepare short answers)
1. *Why Lambda?* -> section 2. *Would Kappa work here?* -> yes at this scale; trade-offs above.
2. *Why is `vehicle_id` the Kafka key?* Same vehicle -> same partition -> ordering per vehicle -> correct idle-run tracking.
3. *What does `acks=all` do and what can go wrong?* Waits for all in-sync replicas; still at-least-once (retries can duplicate) -> dedup downstream.
4. *What is a watermark?* Spark's estimate of how late data can be (2 min) - lets it drop old dedup state; late events beyond it are not de-duplicated.
5. *How do you avoid double counting after a crash?* Checkpoint gives at-least-once; `processed_batches` ledger in the same DB transaction makes the write idempotent.
6. *Why `foreachBatch` and psycopg2 instead of the JDBC sink?* Need upserts/additive updates and a transaction with the ledger; JDBC append cannot.
7. *How is earnings computed and why the `trip_completed` flag?* Sum of `fare` where the trip completes; on-trip events carry a running fare so summing all events would over-count.
8. *What is `drift`?* Batch earnings (recomputed from Parquet) minus speed-layer earnings; non-zero means late/duplicate/lost events.
9. *What happens if the expense file is late/missing?* The DAG only picks days whose file exists and are finished (+20 s grace); vehicles without cost data are `NO_EXPENSE_DATA`; without telemetry `NO_STREAM_DATA`. Re-run with `reprocess`.
10. *What happens if Airflow re-runs a day?* All tasks delete-and-insert that day's rows -> idempotent.
11. *How would you scale 100x?* More Kafka partitions + consumers, Spark cluster, columnar serving store, compaction, aggregate earlier.
12. *How does the idle alert avoid flooding?* Partial unique index: one open alert per (type, vehicle); auto-resolved when it works again.
13. *What does the 1-minute window do?* Tumbling event-time windows per zone give the "right now" view; late data upserts additively into the right window.
14. *Where could data be lost?* Producer failure before ack (counted in `stream_produce_errors_total`); invalid events (archived in dead-letter Parquet, counted); Kafka retention vs. very long outages (Parquet is independent).
