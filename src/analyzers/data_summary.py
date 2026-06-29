"""data summary 모듈."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import text

from src.db.engine import SessionLocal


def _fmt(val: object) -> str:
    if val is None:
        return "-"
    return str(val)


def _int(val: object) -> int:
    if val is None:
        return 0
    if isinstance(val, int):
        return val
    return int(str(val))


def analyze_events() -> list[dict[str, Any]]:
    """
    Handle the analyze events operation.

    Returns:
        List of results.

    """
    with SessionLocal() as session:
        rows = session.execute(
            text("""
                SELECT team_id, event_type, COUNT(*) AS cnt,
                       MAX(published_at) AS last_event
                FROM team_events
                GROUP BY team_id, event_type
                ORDER BY team_id, cnt DESC
            """),
        ).fetchall()
        total = session.execute(text("SELECT COUNT(*) FROM team_events")).scalar()
        recent = session.execute(
            text("SELECT COUNT(*) FROM team_events WHERE published_at >= :cutoff"),
            {"cutoff": datetime.now(UTC).replace(tzinfo=None) - timedelta(days=30)},
        ).scalar()
    return [
        {"section": "Events", "total": _int(total), "recent_30d": _int(recent), "by_team": {}},
        *[{"team": r.team_id, "type": r.event_type, "count": _int(r.cnt), "last": _fmt(r.last_event)} for r in rows],
    ]


def analyze_roster() -> list[dict[str, Any]]:
    """
    Handle the analyze roster operation.

    Returns:
        List of results.

    """
    with SessionLocal() as session:
        total = session.execute(text("SELECT COUNT(*) FROM roster_transactions")).scalar()
        by_action = session.execute(
            text("""
                SELECT action, COUNT(*) AS cnt
                FROM roster_transactions GROUP BY action
            """),
        ).fetchall()
        by_team = session.execute(
            text("""
                SELECT team_id, COUNT(*) AS cnt,
                       MAX(transaction_date) AS last_txn
                FROM roster_transactions
                GROUP BY team_id ORDER BY team_id
            """),
        ).fetchall()
        recent = session.execute(
            text("SELECT COUNT(*) FROM roster_transactions WHERE transaction_date >= :cutoff"),
            {"cutoff": (datetime.now(UTC).replace(tzinfo=None) - timedelta(days=30)).date()},
        ).scalar()
    result = [{"section": "Roster", "total": _int(total), "recent_30d": _int(recent)}]
    result.extend({"action": r.action, "count": _int(r.cnt)} for r in by_action)
    result.extend({"team": r.team_id, "count": _int(r.cnt), "last": _fmt(r.last_txn)} for r in by_team)
    return result


def analyze_tickets() -> list[dict[str, Any]]:
    """
    Handle the analyze tickets operation.

    Returns:
        List of results.

    """
    with SessionLocal() as session:
        total = session.execute(text("SELECT COUNT(*) FROM ticket_prices")).scalar()
        by_team = session.execute(
            text("""
                SELECT team_id, COUNT(*) AS cnt,
                       MIN(price) AS min_price, MAX(price) AS max_price,
                       season
                FROM ticket_prices
                GROUP BY team_id, season ORDER BY team_id
            """),
        ).fetchall()
        rules = session.execute(text("SELECT COUNT(*) FROM ticket_open_rules")).scalar()
    result = [{"section": "Ticket", "total": _int(total), "open_rules": _int(rules)}]
    result.extend(
        {
            "team": r.team_id,
            "count": _int(r.cnt),
            "min": _int(r.min_price),
            "max": _int(r.max_price),
            "season": _fmt(r.season),
        }
        for r in by_team
    )
    return result


def analyze_seats() -> list[dict[str, Any]]:
    """
    Handle the analyze seats operation.

    Returns:
        List of results.

    """
    with SessionLocal() as session:
        total = session.execute(text("SELECT COUNT(*) FROM stadium_seat_sections")).scalar()
        by_stadium = session.execute(
            text("""
                SELECT stadium_id, COUNT(*) AS cnt,
                       COUNT(DISTINCT seat_grade) AS grades
                FROM stadium_seat_sections
                GROUP BY stadium_id ORDER BY stadium_id
            """),
        ).fetchall()
    result = [{"section": "Seats", "total": _int(total)}]
    result.extend({"stadium": r.stadium_id, "sections": _int(r.cnt), "grades": _int(r.grades)} for r in by_stadium)
    return result


def analyze_parking() -> list[dict[str, Any]]:
    """
    Handle the analyze parking operation.

    Returns:
        List of results.

    """
    with SessionLocal() as session:
        total = session.execute(text("SELECT COUNT(*) FROM parking_lots")).scalar()
        fees = session.execute(text("SELECT COUNT(*) FROM parking_fee_rules")).scalar()
        by_stadium = session.execute(
            text("""
                SELECT l.stadium_id, COUNT(*) AS cnt
                FROM parking_lots l
                GROUP BY l.stadium_id ORDER BY l.stadium_id
            """),
        ).fetchall()
    result = [{"section": "Parking", "lots": _int(total), "fee_rules": _int(fees)}]
    result.extend({"stadium": r.stadium_id, "lots": _int(r.cnt)} for r in by_stadium)
    return result


def analyze_food() -> list[dict[str, Any]]:
    """
    Handle the analyze food operation.

    Returns:
        List of results.

    """
    with SessionLocal() as session:
        vendors = session.execute(text("SELECT COUNT(*) FROM stadium_food_vendors")).scalar()
        menus = session.execute(text("SELECT COUNT(*) FROM stadium_food_menu_items")).scalar()
        by_stadium = session.execute(
            text("""
                SELECT v.stadium_id, COUNT(DISTINCT v.id) AS vendors,
                       COUNT(m.id) AS menu_items
                FROM stadium_food_vendors v
                LEFT JOIN stadium_food_menu_items m ON m.vendor_id = v.id
                GROUP BY v.stadium_id ORDER BY v.stadium_id
            """),
        ).fetchall()
    result = [{"section": "Food", "vendors": _int(vendors), "menu_items": _int(menus)}]
    result.extend(
        {
            "stadium": r.stadium_id,
            "vendors": _int(r.vendors),
            "menu_items": _int(r.menu_items),
        }
        for r in by_stadium
    )
    return result


def generate_report() -> str:
    """
    Generate generate report.

    Returns:
        String result.

    """
    lines = []

    lines.append("# KBO Pipeline Data Summary")
    lines.append(f"Generated: {datetime.now(UTC).replace(tzinfo=None).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    lines.append("")

    sections = [
        ("Events", analyze_events),
        ("Roster Transactions", analyze_roster),
        ("Ticket Prices", analyze_tickets),
        ("Seat Sections", analyze_seats),
        ("Parking", analyze_parking),
        ("Food & Beverage", analyze_food),
    ]

    for title, fn in sections:
        data = fn()
        lines.append(f"## {title}")
        summary = data[0]
        details = data[1:]

        parts = [f"{k}={v}" for k, v in summary.items() if k != "section"]
        lines.append(f"- Summary: {', '.join(parts)}")

        if details:
            keys = list(details[0].keys())
            col_widths = {k: max(len(k), *(len(str(d.get(k, ""))) for d in details)) for k in keys}
            header = "  " + " | ".join(k.ljust(col_widths[k]) for k in keys)
            sep = "  " + "-+-".join("-" * col_widths[k] for k in keys)
            lines.append("")
            lines.append(header)
            lines.append(sep)
            for d in details:
                row = "  " + " | ".join(str(d.get(k, "")).ljust(col_widths[k]) for k in keys)
                lines.append(row)
        lines.append("")

    return "\n".join(lines)
