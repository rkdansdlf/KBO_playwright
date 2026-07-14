"""CLI 명령: monitor data freshness."""

from __future__ import annotations

import argparse
import logging
from datetime import UTC, date, datetime, time, timedelta
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from src.db.engine import SessionLocal
from src.repositories.source_registry_repository import DataSourceRepository
from src.services.p0_readiness import P0ReadinessOptions, build_p0_readiness, format_p0_readiness_summary
from src.utils.alerting import SlackWebhookClient

if TYPE_CHECKING:
    from collections.abc import Sequence

    from src.models.source_registry import DataSource

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
    "ticket_open_rule": ("ticket_open_rules", "updated_at"),
    "seat": ("stadium_seat_sections", "updated_at"),
    "parking": ("parking_lots", "updated_at"),
    "food": ("stadium_food_vendors", "last_verified_at"),
    "transit": ("stadium_transit_times", "measured_at"),
    "congestion": ("stadium_congestion", "measured_at"),
    "operation_notice": ("stadium_operation_notices", "published_at"),
}

TABLE_STALE_THRESHOLD_HOURS = {
    "event": 192,
    "roster": 48,
    "ticket_open_rule": 720,
    "seat": 720,
    "parking": 720,
    "food": 720,
    "transit": 192,
    "congestion": 48,
    "operation_notice": 192,
}
PRESEASON_GRACE_MONTHS = {1, 2}


def _get_stale_threshold_hours(source: DataSource) -> int:
    freq = source.crawl_frequency or "daily"
    return STALE_THRESHOLD_HOURS.get(freq, 48)


def _table_staleness_message(
    *,
    domain: str,
    table: str,
    date_column: str,
    latest_value: object,
    now: datetime,
) -> str | None:
    if latest_value is None:
        return f"[STALE] Table {table} (domain={domain}) has no {date_column} value"
    if date_column == "season":
        return _season_staleness_message(domain=domain, table=table, latest_value=latest_value, now=now)
    return _timestamp_staleness_message(
        domain=domain,
        table=table,
        date_column=date_column,
        latest_value=latest_value,
        now=now,
    )


def _season_staleness_message(*, domain: str, table: str, latest_value: object, now: datetime) -> str | None:
    try:
        latest_season = int(latest_value)
    except (TypeError, ValueError):
        return f"[STALE] Table {table} (domain={domain}) has invalid season={latest_value!r}"
    required_season = now.year - 1 if now.month in PRESEASON_GRACE_MONTHS else now.year
    if latest_season < required_season:
        return f"[STALE] Table {table} (domain={domain}) latest season={latest_season} (required>={required_season})"
    return None


def _timestamp_staleness_message(
    *,
    domain: str,
    table: str,
    date_column: str,
    latest_value: object,
    now: datetime,
) -> str | None:
    latest_at = _as_kst_datetime(latest_value)
    if latest_at is None:
        return f"[STALE] Table {table} (domain={domain}) has invalid {date_column}={latest_value!r}"
    threshold = TABLE_STALE_THRESHOLD_HOURS[domain]
    age_hours = (now - latest_at).total_seconds() / 3600
    if age_hours > threshold:
        return (
            f"[STALE] Table {table} (domain={domain}) latest {date_column} is "
            f"{age_hours:.0f}h old (threshold={threshold}h)"
        )
    return None


def _as_kst_datetime(value: object) -> datetime | None:
    if isinstance(value, datetime):
        timestamp = value
    elif isinstance(value, date):
        timestamp = datetime.combine(value, time.min, tzinfo=KST)
    elif isinstance(value, str):
        try:
            timestamp = datetime.fromisoformat(value)
        except ValueError:
            return None
    else:
        return None
    if timestamp.tzinfo is None:
        return timestamp.replace(tzinfo=KST)
    return timestamp.astimezone(KST)


def check_freshness(*, dry_run: bool = False) -> list[str]:
    """Check freshness.

    Args:
        dry_run: If True, returns findings without sending alerts.

    Returns:
        List of results.

    """
    alerts = []
    if dry_run:
        logger.debug("Freshness dry-run returns findings without alert delivery")

    with SessionLocal() as session:
        ds_repo = DataSourceRepository(session)
        all_active = ds_repo.get_all_active()
        now = datetime.now(UTC).replace(tzinfo=None)

        for source in all_active:
            if source.last_success_at is None:
                msg = (
                    f"[STALE] {source.source_key}: never crawled "
                    f"(type={source.source_type}, domain={source.target_domain})"
                )
                logger.warning(msg)
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
                alerts.append(msg)

    return alerts


def check_table_completeness(*, dry_run: bool = False) -> list[str]:
    """Check table completeness.

    Args:
        dry_run: If True, returns findings without sending alerts.

    Returns:
        List of results.

    """
    alerts = []
    if dry_run:
        logger.debug("Table freshness dry-run returns findings without alert delivery")
    now = datetime.now(KST)

    with SessionLocal() as session:
        for domain, (table, date_col) in DOMAIN_TABLE_CHECKS.items():
            try:
                row = session.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()  # noqa: S608
                if row == 0:
                    msg = f"[EMPTY] Table {table} (domain={domain}) has 0 rows"
                    logger.warning(msg)
                    alerts.append(msg)
                else:
                    latest = session.execute(text(f"SELECT MAX({date_col}) FROM {table}")).scalar()  # noqa: S608
                    msg = _table_staleness_message(
                        domain=domain,
                        table=table,
                        date_column=date_col,
                        latest_value=latest,
                        now=now,
                    )
                    if msg:
                        logger.warning(msg)
                        alerts.append(msg)
                    else:
                        logger.info("[OK] %s: %s rows, latest %s=%s", table, row, date_col, latest)
            except SQLAlchemyError as e:
                msg = f"[ERROR] Table check failed for {table}: {e}"
                logger.exception("[ERROR] Table check failed for %s", table)
                alerts.append(msg)
    return alerts


def check_p0_readiness(*, dry_run: bool = False) -> list[str]:
    """Check p0 readiness.

    Args:
        dry_run: If True, returns findings without sending alerts.

    Returns:
        List of results.

    """
    alerts = []
    if dry_run:
        logger.debug("P0 dry-run returns findings without alert delivery")

    target_date = (datetime.now(KST).date() - timedelta(days=1)).strftime("%Y%m%d")
    with SessionLocal() as session:
        readiness = build_p0_readiness(
            session,
            P0ReadinessOptions(target_date=target_date, lookback_days=0, lookahead_days=1),
        )

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
        alerts.append(msg)
    return alerts


def run_monitor(*, alert: bool = True, dry_run: bool = False) -> dict[str, list[str]]:
    """Run monitor.

    Args:
        alert: Alert.
        dry_run: If True, performs a dry run without persisting changes.

    Returns:
        Dictionary result.

    """
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
        logger.info(summary)
        for issue in all_issues:
            logger.info("  %s", issue)
    else:
        logger.info("[MONITOR] All data sources and tables look healthy.")

    return {"stale": stale, "table_issues": empty, "p0_issues": p0_issues}


def build_arg_parser() -> argparse.ArgumentParser:
    """Build arg parser.

    Returns:
        The result of the operation.

    """
    parser = argparse.ArgumentParser(description="Monitor KBO data freshness and completeness")

    parser.add_argument("--no-alert", action="store_true", help="Suppress Slack/Telegram alerts")
    parser.add_argument("--dry-run", action="store_true", help="Scan only, do not alert")
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    """Run the main entry point for this CLI command.

    Args:
        argv: Argv.

    """
    parser = build_arg_parser()

    args = parser.parse_args(argv)
    run_monitor(alert=not args.no_alert, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
