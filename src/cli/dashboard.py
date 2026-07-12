"""Generate operational SLA & freshness dashboard in reports/dashboard.html."""

from __future__ import annotations

import logging
import zoneinfo
from datetime import datetime
from pathlib import Path

from src.db.engine import SessionLocal
from src.models.sla_metrics import SlaMetrics

logger = logging.getLogger(__name__)


def generate_dashboard() -> None:
    """Query SLA metrics from DB and build reports/dashboard.html with premium aesthetics."""
    logger.info("📊 Generating operational SLA & freshness dashboard...")

    with SessionLocal() as session:
        # Query last 100 SLA check metrics
        metrics = (
            session.query(SlaMetrics).order_by(SlaMetrics.check_time.desc(), SlaMetrics.id.desc()).limit(100).all()
        )

    # Compute quick stats
    total_checks = len(metrics)
    violations = sum(1 for m in metrics if m.is_violation)
    pass_rate = ((total_checks - violations) / total_checks * 100.0) if total_checks > 0 else 100.0

    avg_delay = (sum(m.actual_delay_hours for m in metrics) / total_checks) if total_checks > 0 else 0.0

    # Build metrics rows HTML
    rows_html = []
    for m in metrics:
        status_class = "violation" if m.is_violation else "pass"
        status_text = "Violation" if m.is_violation else "Pass"
        check_time_str = m.check_time.strftime("%Y-%m-%d %H:%M:%S") if m.check_time else "N/A"
        notes_str = m.notes or "-"

        rows_html.append(
            f"""
            <tr>
                <td>{check_time_str}</td>
                <td><span class="badge cat-{m.category}">{m.category.upper()}</span></td>
                <td>{m.sla_threshold_hours} hrs</td>
                <td>{m.actual_delay_hours} hrs</td>
                <td><span class="status-indicator {status_class}">{status_text}</span></td>
                <td class="notes-cell" title="{notes_str}">{notes_str}</td>
            </tr>
            """
        )

    violation_color = "var(--violation-color)" if violations > 0 else "var(--pass-color)"
    empty_rows_html = (
        '<tr><td colspan="6" style="text-align: center; color: var(--text-secondary);">'
        "No telemetry log entries found. Run freshness check first.</td></tr>"
    )
    font_url = (
        "https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;800"
        "&family=Plus+Jakarta+Sans:wght@300;400;600;700&display=swap"
    )
    generated_at = datetime.now(zoneinfo.ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d %H:%M:%S")

    # HTML template with premium glassmorphism dark theme
    html_content = f"""<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>KBO Crawler SLA Dashboard</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="{font_url}" rel="stylesheet">
    <style>
        :root {{
            --bg-gradient: radial-gradient(circle at top right, #1a162b, #0d0b14);
            --panel-bg: rgba(22, 19, 39, 0.45);
            --panel-border: rgba(255, 255, 255, 0.06);
            --glow-color: rgba(147, 51, 234, 0.15);
            --text-primary: #f3f1f6;
            --text-secondary: #9a94a6;
            --pass-color: #10b981;
            --pass-glow: rgba(16, 185, 129, 0.15);
            --violation-color: #ef4444;
            --violation-glow: rgba(239, 68, 68, 0.15);
            --accent-purple: #a855f7;
            --cat-game: #3b82f6;
            --cat-relay: #eab308;
            --cat-analysis: #ec4899;
        }}

        * {{
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }}

        body {{
            background: var(--bg-gradient);
            color: var(--text-primary);
            font-family: 'Plus Jakarta Sans', sans-serif;
            min-height: 100vh;
            padding: 2.5rem 1.5rem;
            display: flex;
            flex-direction: column;
            align-items: center;
        }}

        .container {{
            max-width: 1200px;
            width: 100%;
        }}

        /* Header block */
        header {{
            margin-bottom: 2.5rem;
            text-align: center;
        }}

        h1 {{
            font-family: 'Outfit', sans-serif;
            font-size: 2.5rem;
            font-weight: 800;
            background: linear-gradient(135deg, #f3f1f6, #a855f7);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            margin-bottom: 0.5rem;
            letter-spacing: -0.5px;
        }}

        .subtitle {{
            color: var(--text-secondary);
            font-size: 1rem;
            font-weight: 400;
        }}

        /* KPI Cards Grid */
        .kpi-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
            gap: 1.5rem;
            margin-bottom: 2.5rem;
        }}

        .card {{
            background: var(--panel-bg);
            border: 1px solid var(--panel-border);
            border-radius: 20px;
            padding: 1.75rem;
            backdrop-filter: blur(16px);
            -webkit-backdrop-filter: blur(16px);
            box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.37);
            position: relative;
            overflow: hidden;
            transition: transform 0.3s ease, border-color 0.3s ease;
        }}

        .card:hover {{
            transform: translateY(-5px);
            border-color: rgba(168, 85, 247, 0.3);
        }}

        .card::before {{
            content: '';
            position: absolute;
            top: -50%;
            left: -50%;
            width: 200%;
            height: 200%;
            background: radial-gradient(circle, var(--glow-color) 0%, transparent 70%);
            pointer-events: none;
            transition: opacity 0.3s ease;
        }}

        .card-title {{
            font-size: 0.875rem;
            color: var(--text-secondary);
            text-transform: uppercase;
            letter-spacing: 1px;
            margin-bottom: 0.75rem;
        }}

        .card-value {{
            font-family: 'Outfit', sans-serif;
            font-size: 2.25rem;
            font-weight: 800;
            color: var(--text-primary);
            line-height: 1;
        }}

        .card-desc {{
            font-size: 0.8rem;
            color: var(--text-secondary);
            margin-top: 0.5rem;
        }}

        /* Table section */
        .table-section {{
            background: var(--panel-bg);
            border: 1px solid var(--panel-border);
            border-radius: 24px;
            padding: 2rem;
            backdrop-filter: blur(16px);
            -webkit-backdrop-filter: blur(16px);
            box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.37);
            overflow-x: auto;
        }}

        .section-title {{
            font-family: 'Outfit', sans-serif;
            font-size: 1.5rem;
            font-weight: 600;
            margin-bottom: 1.5rem;
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }}

        table {{
            width: 100%;
            border-collapse: collapse;
            text-align: left;
        }}

        th {{
            color: var(--text-secondary);
            font-weight: 600;
            font-size: 0.85rem;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            padding: 1rem;
            border-bottom: 1px solid rgba(255, 255, 255, 0.08);
        }}

        td {{
            padding: 1.25rem 1rem;
            border-bottom: 1px solid rgba(255, 255, 255, 0.04);
            font-size: 0.95rem;
        }}

        tr:hover td {{
            background: rgba(255, 255, 255, 0.02);
        }}

        /* Badges & Status */
        .badge {{
            display: inline-block;
            padding: 0.25rem 0.6rem;
            border-radius: 12px;
            font-size: 0.75rem;
            font-weight: 700;
            letter-spacing: 0.5px;
        }}

        .cat-game {{
            background: rgba(59, 130, 246, 0.15);
            color: var(--cat-game);
        }}

        .cat-relay {{
            background: rgba(234, 179, 8, 0.15);
            color: var(--cat-relay);
        }}

        .cat-analysis {{
            background: rgba(236, 72, 153, 0.15);
            color: var(--cat-analysis);
        }}

        .status-indicator {{
            display: inline-flex;
            align-items: center;
            gap: 0.375rem;
            font-size: 0.85rem;
            font-weight: 600;
        }}

        .status-indicator::before {{
            content: '';
            width: 8px;
            height: 8px;
            border-radius: 50%;
        }}

        .status-indicator.pass {{
            color: var(--pass-color);
        }}

        .status-indicator.pass::before {{
            background: var(--pass-color);
            box-shadow: 0 0 10px var(--pass-color);
        }}

        .status-indicator.violation {{
            color: var(--violation-color);
        }}

        .status-indicator.violation::before {{
            background: var(--violation-color);
            box-shadow: 0 0 10px var(--violation-color);
        }}

        .notes-cell {{
            max-width: 300px;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
            color: var(--text-secondary);
        }}

        footer {{
            margin-top: 3rem;
            text-align: center;
            font-size: 0.8rem;
            color: var(--text-secondary);
        }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>KBO Scraper SLA Dashboard</h1>
            <p class="subtitle">Operational SLA & Data Freshness Monitoring System</p>
        </header>

        <section class="kpi-grid">
            <div class="card" style="--glow-color: rgba(16, 185, 129, 0.12)">
                <div class="card-title">SLA Pass Rate</div>
                <div class="card-value" style="color: var(--pass-color)">{pass_rate:.1f}%</div>
                <div class="card-desc">Target threshold compliance rate</div>
            </div>

            <div class="card" style="--glow-color: rgba(239, 68, 68, 0.12)">
                <div class="card-title">Total Violations</div>
                <div class="card-value" style="color: {violation_color}">{violations}</div>
                <div class="card-desc">Active SLA violations in past 100 checks</div>
            </div>

            <div class="card">
                <div class="card-title">Average Delay</div>
                <div class="card-value">{avg_delay:.2f}h</div>
                <div class="card-desc">Avg delay hours relative to game finish</div>
            </div>

            <div class="card">
                <div class="card-title">Total Checks</div>
                <div class="card-value">{total_checks}</div>
                <div class="card-desc">Total telemetry records loaded</div>
            </div>
        </section>

        <section class="table-section">
            <h2 class="section-title">📊 SLA Telemetry Log</h2>
            <table>
                <thead>
                    <tr>
                        <th>Check Time</th>
                        <th>Category</th>
                        <th>SLA Threshold</th>
                        <th>Actual Delay</th>
                        <th>Status</th>
                        <th>Notes / Issues</th>
                    </tr>
                </thead>
                <tbody>
                    {"".join(rows_html) if rows_html else empty_rows_html}
                </tbody>
            </table>
        </section>

        <footer>
            <p>© 2026 KBO Playwright Scraper Platform. Generated on {generated_at} KST.</p>
        </footer>
    </div>
</body>
</html>
"""

    reports_dir = Path("reports")
    reports_dir.mkdir(parents=True, exist_ok=True)
    dest = reports_dir / "dashboard.html"
    dest.write_text(html_content, encoding="utf-8")
    logger.info("✅ Dashboard generated successfully at: %s", dest.resolve())


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    generate_dashboard()
