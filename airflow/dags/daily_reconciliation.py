"""BATCH LAYER orchestration - Apache Airflow DAG.

Runs every 5 real minutes (= once per simulated day). Each run processes every simulated day whose
expense file has landed and which is finished (+ a grace period for late Kafka events):

    find_pending_days -> ingest_expenses -> recompute_batch_view -> reconcile_profitability -> publish_report

* find_pending_days       ShortCircuit: stops the run early when there is nothing new to do
* ingest_expenses         CSV -> clean -> PostgreSQL (rejected rows are stored with a reason)
* recompute_batch_view    re-derive per-vehicle earnings from the immutable Parquet master dataset
* reconcile_profitability JOIN earnings (stream) with costs (batch file); classify each vehicle
* publish_report          HTML + CSV report (files in ./reports, and served by the API)

Manual re-run of specific days:  Trigger DAG w/ config  {"reprocess": ["2026-01-03"]}
"""
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator

from common import batch_tasks as bt
from common.db import connect
from common.logging_utils import error, get_logger, info
from common.simclock import load_clock

log = get_logger("airflow-dag", stage="orchestration")


def _days(ti):
    return ti.xcom_pull(task_ids="find_pending_days") or []


def _find_pending(dag_run=None, **_):
    conn = connect()
    try:
        reprocess = (dag_run.conf or {}).get("reprocess", []) if dag_run else []
        return bt.find_pending_days(conn, load_clock(conn), reprocess=reprocess)  # [] -> short-circuit
    finally:
        conn.close()


def _for_each_day(step):
    """Wrap a per-day step so it runs for every pending day and stores its stats in XCom."""
    def runner(ti, **_):
        conn = connect()
        try:
            results = {}
            for day in _days(ti):
                results[day] = step(conn, day)
            return results
        finally:
            conn.close()
    return runner


def _publish_and_record(ti, **_):
    conn = connect()
    try:
        ingest = ti.xcom_pull(task_ids="ingest_expenses") or {}
        recon = ti.xcom_pull(task_ids="reconcile_profitability") or {}
        for day in _days(ti):
            files = bt.publish_report(conn, day)
            stats = {**ingest.get(day, {}), **recon.get(day, {})}
            bt.record_run(conn, day, "success", message=f"report files: {files}", **stats)
            info(log, "batch run finished", sim_date=day, **stats)
    finally:
        conn.close()


def _on_failure(context):
    """Structured error log + an audit row so the failure shows up in the API / Prometheus."""
    ti = context["task_instance"]
    error(log, "batch task failed", task=ti.task_id, run_id=context.get("run_id"),
          error=str(context.get("exception")))
    try:
        conn = connect()
        for day in (ti.xcom_pull(task_ids="find_pending_days") or []):
            bt.record_run(conn, day, "failed", message=f"{ti.task_id}: {context.get('exception')}")
        conn.close()
    except Exception:  # noqa: BLE001 - never let the callback itself crash
        pass


with DAG(
    dag_id="daily_fleet_reconciliation",
    description="Daily per-vehicle profitability reconciliation (Lambda architecture: batch layer)",
    start_date=datetime(2026, 1, 1),
    schedule="*/5 * * * *",           # 1 simulated day = 5 real minutes
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(seconds=20), "on_failure_callback": _on_failure},
    tags=["fleet", "batch-layer"],
    doc_md=__doc__,
) as dag:
    find_pending_days = ShortCircuitOperator(task_id="find_pending_days", python_callable=_find_pending)
    ingest_expenses = PythonOperator(task_id="ingest_expenses", python_callable=_for_each_day(bt.ingest_expenses))
    recompute_batch_view = PythonOperator(task_id="recompute_batch_view",
                                          python_callable=_for_each_day(bt.recompute_batch_view))
    reconcile_profitability = PythonOperator(task_id="reconcile_profitability",
                                             python_callable=_for_each_day(bt.reconcile_day))
    publish_report = PythonOperator(task_id="publish_report", python_callable=_publish_and_record)

    find_pending_days >> ingest_expenses >> recompute_batch_view >> reconcile_profitability >> publish_report
