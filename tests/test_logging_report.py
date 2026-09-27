import io
import json
import logging

from common.logging_utils import JsonFormatter, get_logger, info
from common.report import render_csv, render_html


def test_json_log_line_has_service_stage_and_fields():
    logger = get_logger("unit-test", "processing")
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(JsonFormatter("unit-test", "processing"))
    logger.handlers = [handler]
    info(logger, "batch committed", batch_id=7, rows=120)
    record = json.loads(stream.getvalue())
    assert record["service"] == "unit-test" and record["stage"] == "processing"
    assert record["msg"] == "batch committed" and record["batch_id"] == 7 and record["rows"] == 120
    assert record["level"] == "INFO" and "ts" in record


ROW = {"vehicle_id": "V024", "trips": 5, "earnings": 3000.0, "fuel_cost": 2000.0, "maintenance_cost": 3000.0,
       "total_cost": 5000.0, "profit": -2000.0, "margin": -0.667, "distance_covered": 60.0, "cost_per_km": 83.33,
       "idle_ratio": 0.2, "prev_profit_change": -100.0, "declining": True, "status": "UNPROFITABLE",
       "speed_layer_earnings": 3000.0, "drift": 0.0}


def test_report_renderers():
    html_doc = render_html("2026-01-02", [ROW], [{"sim_hour": 8, "earnings": 900.0, "trips": 2, "idle_ratio": 0.3}],
                           {"negative_value": 1})
    assert "V024" in html_doc and "UNPROFITABLE" in html_doc and "08:00" in html_doc and "negative_value: 1" in html_doc
    csv_doc = render_csv([ROW])
    assert csv_doc.splitlines()[0].startswith("vehicle_id,trips,earnings") and "V024" in csv_doc
