# Step-by-step guide (beginner, 2 days)

The code is already written. Your job in two days: **get it running, understand it well enough to defend it,
collect evidence (screenshots + video), and write the report.** Do the steps in order.

---
## PART A - Set up your computer (about 1 hour, mostly waiting for downloads)

### A1. What to install
| Tool | Why | How |
|---|---|---|
| **Docker Desktop** | runs Kafka, Spark, Airflow, Postgres, etc. as containers | https://www.docker.com/products/docker-desktop (Windows: it will ask to enable **WSL 2** - say yes and restart) |
| **Git** | version control / submission | https://git-scm.com/downloads |
| **VS Code** | to read the code | https://code.visualstudio.com (extensions: Python, Docker) |
| **Python 3.11** (optional) | only to run the unit tests on your laptop | https://www.python.org/downloads (tick "Add to PATH") |
| A screen recorder | demo video | Windows: `Win+G` (Xbox Game Bar) or OBS Studio |

### A2. Give Docker enough memory
Docker Desktop -> Settings -> Resources: **Memory >= 6 GB (8 GB is better), CPUs >= 4**. Apply & restart.
(If your laptop has 8 GB total RAM, close Chrome/other heavy apps while the stack runs.)

### A3. Check the install
Open PowerShell (Windows) or a terminal:
```
docker --version
docker compose version
git --version
```
All three must print a version.

### A4. Get the project
Unzip `fleet-pipeline.zip` (e.g. to `C:\Projects\fleet-pipeline`), then open a terminal **inside that folder**
(VS Code: File -> Open Folder, then Terminal -> New Terminal).
Run `git init` and `git add . && git commit -m "initial version"` so you have your own history (the submission may be a repo link).

---
## PART B - Run the whole system (Day 1, morning)

### B1. Start everything
```
docker compose up --build -d
```
* The first run downloads images and builds 4 containers: **10-20 minutes** on a normal connection. Later starts take seconds.
* Check: `docker compose ps`. You want `Up` for most services and `Exited (0)` for `kafka-init` and `airflow-init` (they are one-off jobs).
* If something is `Restarting`: `docker compose logs <service-name>` and read the last lines.

### B2. Confirm each layer works (do these in order - this is also your demo checklist)
1. **Ingestion:** `docker compose logs --tail 5 stream-producer` -> JSON lines like `"msg": "producer heartbeat"`.
   See the messages in Kafka:
   `docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic trip-events --max-messages 5`
   Show the 3 partitions: `docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic trip-events`
2. **Speed layer:** `docker compose logs --tail 5 spark-streaming` -> `"msg": "batch committed"` every ~10 s. (First batch takes ~1 min while Spark starts.)
3. **Serving / real-time API:** open http://localhost:8000/docs -> `GET /metrics/utilization` -> Try it out -> Execute.
   You should see active vehicles, idle ratio, trips per simulated hour and earnings by zone.
4. **Alerts:** wait 3 minutes, then `GET /alerts`. Vehicles V027-V030 are "lazy" and will be reported idle > 60 s.
5. **Batch layer:** open http://localhost:8080 (admin/admin). The DAG `daily_fleet_reconciliation` runs every 5 minutes.
   After ~10 minutes: `GET /report/profitability` (or `?format=html` in the browser).
6. **Dashboard:** http://localhost:3000 (admin/admin) - dashboard "Fleet Operations - Lambda pipeline".
7. **Monitoring:** http://localhost:9090/alerts shows the alert rules (green = OK).

### B3. What "one simulated day = 5 minutes" means for you
* Every 5 real minutes a simulated day ends -> the batch producer drops `expenses_<date>.csv` -> Airflow processes it.
* The **first** day after a fresh start is *partial* (the stream began mid-day). The report shows a "Partial day" banner.
  Use the **second and later** reports for screenshots. So let the system run **at least 15-20 minutes** before you capture results.
* Leave it running for 30-40 minutes for richer charts (several days of history).

---
## PART C - Understand it (Day 1, afternoon) - needed for the viva

Read the files in this order (each has a docstring at the top explaining it). Aim for the "what" and "why" of each:

1. `docs/architecture.png` - the big picture.
2. `simulators/vehicle.py` - the state machine (idle -> enroute -> on_trip -> idle) and how fares/`trip_completed` work.
3. `simulators/stream_producer.py` - Kafka producer, key = vehicle_id, `acks=all`, fault injection.
4. `streaming/transforms.py` - the processing logic: parse, `reject_reason`, zone, windows, per-vehicle summary.
5. `streaming/spark_job.py` - read from Kafka, watermark + `dropDuplicates`, `foreachBatch`, Parquet, metrics.
6. `streaming/sinks.py` - the transaction, the `processed_batches` ledger, idle alerts.
7. `common/batch_logic.py` and `common/batch_tasks.py` - cleaning, reconciliation, drift.
8. `airflow/dags/daily_reconciliation.py` - the DAG.
9. `api/main.py` - endpoints and Prometheus collector.
10. `monitoring/alert_rules.yml` - the alert rules.

Be ready to answer (short answers are in `docs/REPORT_GUIDE.md`, section "Viva questions").

Try things yourself - this is the fastest way to understand:
* Kill Spark and restart it (`docker compose kill spark-streaming`, then `docker compose up -d spark-streaming`) and watch the numbers stay correct.
* Query the database: `docker compose exec postgres psql -U fleet -d fleet -c "select * from vehicle_state order by vehicle_id limit 5;"`
* Change `IDLE_ALERT_SECONDS` and see the alerts change.

---
## PART D - Evidence for the report (Day 2, morning)

Capture **full-window screenshots with the date/time visible** of:
1. `docker compose ps` (all services up).
2. Kafka topic description (3 partitions) and a few consumed messages.
3. Grafana dashboard (with data) - once when healthy, once during an alert.
4. Swagger `/metrics/utilization` response, `/alerts` response.
5. Airflow: DAG graph view with all tasks green, and one run's task list.
6. The HTML profitability report (a complete day) and the CSV opened in Excel.
7. Prometheus `/alerts` page: healthy, and firing (`docker compose stop stream-producer`, wait ~2 min, screenshot, then `docker compose start stream-producer`).
8. A few JSON log lines (`docker compose logs --tail 20 spark-streaming`).
9. Spark UI (http://localhost:4040 -> Structured Streaming tab).
10. Test run: `pytest` output (all green).

### Demo video (5-10 minutes) - suggested script
| Min | Show |
|---|---|
| 0-1 | The problem + architecture diagram (30 s each) |
| 1-3 | `docker compose ps`, Kafka messages, Spark logs, API real-time endpoint, Grafana |
| 3-5 | Airflow DAG run + the profitability report (explain reconciliation and `drift`) |
| 5-8 | Observability: stop the producer -> alert fires + `/health` 503 -> restart -> recovers. Show JSON logs. Optionally raise `BAD_EVENT_RATE` |
| 8-9 | Crash recovery (kill Spark, restart, numbers consistent) |
| 9-10 | Limitations and what you'd change at production scale |

---
## PART E - Write the report (Day 2, afternoon)
Follow `docs/REPORT_GUIDE.md` - it has the full outline (8-15 pages), pre-written reasoning for Lambda vs Kappa, a technology
justification table, the observability design, limitations, and viva Q&A. **Rewrite everything in your own words** and only
claim what you have verified by running the system. Export to PDF.

## PART F - Submit
* Repo link or zip: `git add . && git commit -m "final"`, push to GitHub (private repo + share with the lecturer) or zip the folder
  (delete `.venv` first). Include the README.
* Report PDF, demo video (or be ready for a live demo), and for a group: the individual-contributions statement.
* Before submitting, run the final check: `docker compose down -v && docker compose up --build -d`, wait 15 min, confirm a report appears.
  (This proves your submission is reproducible.)

---
## Two-day timetable
| Slot | Task |
|---|---|
| Day 1, 0:00-1:30 | Part A (install) - start `docker compose up --build` while you read |
| Day 1, 1:30-3:00 | Part B (verify every layer) |
| Day 1, 3:00-8:00 | Part C (read + experiment); write your own notes for each component |
| Day 2, 0:00-2:00 | Let it run 30+ min; Part D screenshots; record the video |
| Day 2, 2:00-7:00 | Part E report writing (diagrams are provided) |
| Day 2, last 1 h | Part F: clean re-run test, export PDF, submit |

## If something breaks
| Symptom | Fix |
|---|---|
| `docker compose up` fails on pull | check internet/proxy, retry; Docker Desktop must be running |
| Build of `spark-streaming` is slow | normal (downloads Spark JARs once) |
| No report after 15 min | `docker compose logs airflow-scheduler`, check DAG not paused in the UI; check `docker compose logs batch-producer` shows "expense file dropped" |
| Grafana panels empty | wait 2 min; check Data sources -> both "Prometheus" and "Fleet DB" -> Save & test |
| Everything weird | `docker compose down -v` and start again |
