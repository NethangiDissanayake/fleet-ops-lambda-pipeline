"""Render the daily per-vehicle profitability report (HTML + CSV). Pure functions, no DB access."""
import csv
import html
import io

STATUS_COLOURS = {"PROFITABLE": "#1b7f3b", "AT_RISK": "#b7791f", "UNPROFITABLE": "#c53030",
                  "NO_EXPENSE_DATA": "#4a5568", "NO_STREAM_DATA": "#4a5568"}


def _money(v):
    return "-" if v is None else f"{v:,.2f}"


def _pct(v):
    return "-" if v is None else f"{v * 100:.1f}%"


def render_csv(rows) -> str:
    buf = io.StringIO()
    if not rows:
        return ""
    writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue()


def render_html(day_str, rows, hourly, rejected, coverage=None) -> str:
    known = [r for r in rows if r.get("profit") is not None]
    total_earn = sum(r["earnings"] or 0 for r in known)
    total_cost = sum(r["total_cost"] or 0 for r in known)
    n_unprof = sum(1 for r in rows if r["status"] == "UNPROFITABLE")
    n_risk = sum(1 for r in rows if r["status"] == "AT_RISK")
    declining = [r["vehicle_id"] for r in rows if r.get("declining")]

    body_rows = []
    for r in rows:
        colour = STATUS_COLOURS.get(r["status"], "#000")
        flag = " &#8595;" if r.get("declining") else ""
        body_rows.append(
            "<tr>" + "".join(f"<td>{c}</td>" for c in [
                html.escape(r["vehicle_id"]), r["trips"] if r["trips"] is not None else "-",
                _money(r["earnings"]), _money(r["fuel_cost"]), _money(r["maintenance_cost"]),
                _money(r["profit"]), _pct(r["margin"]), _money(r["cost_per_km"]), _pct(r["idle_ratio"]),
                _money(r["drift"]),
            ]) + f'<td style="color:{colour};font-weight:600">{r["status"]}{flag}</td></tr>')

    hourly_rows = "".join(
        f"<tr><td>{h['sim_hour']:02d}:00</td><td>{h['trips']}</td><td>{_money(h['earnings'])}</td>"
        f"<td>{_pct(h['idle_ratio'])}</td></tr>" for h in hourly)
    rejected_txt = ", ".join(f"{html.escape(k)}: {v}" for k, v in rejected.items()) or "none"

    banner = ""
    if coverage is not None and coverage < 0.9:
        banner = (f'<p style="background:#fffaf0;border:1px solid #f6ad55;padding:8px 12px;border-radius:6px">'
                  f'<b>Partial day:</b> only {coverage * 100:.0f}% of the expected telemetry was received for this simulated day '
                  f'(the stream started mid-day or was interrupted), so earnings are understated and profit figures are '
                  f'indicative only.</p>')
    return f"""<!doctype html><html><head><meta charset="utf-8">
<title>Fleet profitability {day_str}</title>
<style>body{{font-family:Segoe UI,Arial,sans-serif;margin:24px;color:#1a202c}}
table{{border-collapse:collapse;margin:12px 0}}th,td{{border:1px solid #cbd5e0;padding:4px 10px;text-align:right}}
th{{background:#edf2f7}}td:first-child,th:first-child{{text-align:left}}
.cards div{{display:inline-block;margin-right:16px;padding:10px 16px;background:#f7fafc;border:1px solid #e2e8f0;border-radius:6px}}
</style></head><body>
<h1>Daily fleet profitability reconciliation &ndash; simulated day {day_str}</h1>
{banner}
<div class="cards">
<div><b>Fleet earnings</b><br>LKR {total_earn:,.2f}</div>
<div><b>Fuel + maintenance</b><br>LKR {total_cost:,.2f}</div>
<div><b>Net profit</b><br>LKR {total_earn - total_cost:,.2f}</div>
<div><b>Unprofitable vehicles</b><br>{n_unprof}</div>
<div><b>At risk (thin margin)</b><br>{n_risk}</div></div>
<p>Vehicles with a worsening profit vs. the previous day: {', '.join(declining) or 'none'}.
Rejected expense rows: {rejected_txt}.</p>
<h2>Per-vehicle reconciliation (worst first)</h2>
<table><tr><th>Vehicle</th><th>Trips</th><th>Earnings</th><th>Fuel</th><th>Maintenance</th><th>Profit</th>
<th>Margin</th><th>Cost/km</th><th>Idle ratio</th><th>Batch-vs-speed drift</th><th>Status</th></tr>
{''.join(body_rows)}</table>
<h2>Earnings by simulated hour of day</h2>
<table><tr><th>Hour</th><th>Trips</th><th>Earnings (LKR)</th><th>Idle ratio</th></tr>{hourly_rows}</table>
<p style="color:#718096">Earnings come from the batch layer (recomputed from the Parquet master dataset); costs come from
the daily garage/fuel-partner file. Drift = batch earnings &minus; what the speed layer had accumulated.</p>
</body></html>"""
