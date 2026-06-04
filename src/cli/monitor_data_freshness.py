from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta
from typing import Sequence
from zoneinfo import ZoneInfo

from sqlalchemy import text

from src.db.engine import SessionLocal
from src.repositories.source_registry_repository import DataSourceRepository
from src.services.p0_readiness import build_p0_readiness, format_p0_readiness_summary
from src.utils.alerting import SlackWebhookClient
from src.utils.safe_print import safe_print as print

logger = logging.getLogger(__name__)
KST = ZoneInfo("Asia/Seoul")

STALE_THRESHOLD_HOURS = {
    "daily": 30,
    "weekly": 192,
    "seasonal": 720,
    "ondemand": 720,
}

DOMAIN_TABLE_CHECKS = {
    "event": ("team_events", "last_seen_at"),
    "roster": ("roster_transactions", "transaction_date"),
    "ticket": ("ticket_prices", "season"),
    "seat": ("stadium_seat_sections", "updated_at"),
    "parking": ("parking_lots", "updated_at"),
    "food": ("stadium_food_vendors", "last_verified_at"),
}


def _get_stale_threshold_hours(source) -> int:
    freq = source.crawl_frequency or "daily"
    return STALE_THRESHOLD_HOURS.get(freq, 48)


def check_freshness(dry_run: bool = False) -> list[str]:
    alerts = []
    with SessionLocal() as session:
        ds_repo = DataSourceRepository(session)
        all_active = ds_repo.get_all_active()
        now = datetime.utcnow()

        for source in all_active:
            if source.last_success_at is None:
                msg = f"[STALE] {source.source_key}: never crawled (type={source.source_type}, domain={source.target_domain})"
                logger.warning(msg)
                if not dry_run:
                    alerts.append(msg)
                continue

            threshold = _get_stale_threshold_hours(source)
            cutoff = now - timedelta(hours=threshold)
            if source.last_success_at < cutoff:
                hours_since = (now - source.last_success_at).total_seconds() / 3600
                msg = (
                    f"[STALE] {source.source_key}: {hours_since:.0f}h since last success "
                    f"(threshold={threshold}h, domain={source.target_domain})"
                )
                logger.warning(msg)
                if not dry_run:
                    alerts.append(msg)

    return alerts


def check_table_completeness(dry_run: bool = False) -> list[str]:
    alerts = []
    with SessionLocal() as session:
        for domain, (table, date_col) in DOMAIN_TABLE_CHECKS.items():
            try:
                row = session.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
                if row == 0:
                    msg = f"[EMPTY] Table {table} (domain={domain}) has 0 rows"
                    logger.warning(msg)
                    if not dry_run:
                        alerts.append(msg)
                else:
                    recent = session.execute(text(f"SELECT MAX({date_col}) FROM {table}")).scalar()
                    msg = f"[OK] {table}: {row} rows, latest {date_col}={recent}"
                    logger.info(msg)
            except Exception as e:
                msg = f"[ERROR] Table check failed for {table}: {e}"
                logger.error(msg)
                if not dry_run:
                    alerts.append(msg)
    return alerts


def check_p0_readiness(dry_run: bool = False) -> list[str]:
    alerts = []
    target_date = (datetime.now(KST).date() - timedelta(days=1)).strftime("%Y%m%d")
    with SessionLocal() as session:
        readiness = build_p0_readiness(session, target_date=target_date, lookback_days=0, lookahead_days=1)

    logger.info("P0 readiness: %s", format_p0_readiness_summary(readiness))
    for failure in readiness.get("failures", []):
        if failure.get("severity") != "critical":
            continue
        msg = (
            "[P0] "
            f"{failure.get('dataset')} "
            f"{failure.get('game_date') or '-'} "
            f"{failure.get('game_id') or '-'} "
            f"{failure.get('reason')}"
        )
        logger.warning(msg)
        if not dry_run:
            alerts.append(msg)
    return alerts


def run_monitor(alert: bool = True, dry_run: bool = False) -> dict[str, list[str]]:
    stale = check_freshness(dry_run=dry_run)
    empty = check_table_completeness(dry_run=dry_run)
    p0_issues = check_p0_readiness(dry_run=dry_run)
    all_issues = stale + empty + p0_issues

    if all_issues:
        summary = f"[MONITOR] {len(stale)} stale sources, {len(empty)} table issues, {len(p0_issues)} P0 issues"
        logger.warning(summary)
        if alert and not dry_run:
            header = "<b>🧹 KBO Data Freshness Monitor</b>\n"
            body = "\n".join(f"• {a}" for a in all_issues[:20])
            SlackWebhookClient.send_alert(header + "\n" + body)
        print(summary)
        for issue in all_issues:
            print(f"  {issue}")
    else:
        print("[MONITOR] All data sources and tables look healthy.")

    return {"stale": stale, "table_issues": empty, "p0_issues": p0_issues}


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Monitor KBO data freshness and completeness")
    parser.add_argument("--no-alert", action="store_true", help="Suppress Slack/Telegram alerts")
    parser.add_argument("--dry-run", action="store_true", help="Scan only, do not alert")
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    run_monitor(alert=not args.no_alert, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
