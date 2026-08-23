"""Comprehensive Platform Unified Health Diagnostics CLI."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import UTC, datetime
from http import HTTPStatus
from typing import TYPE_CHECKING, Any

from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from src.api.app import app
from src.constants import KST
from src.db.engine import SessionLocal
from src.models.rag_chunk import RagChunk
from src.repositories.source_registry_repository import DataSourceRepository

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)
DATA_SOURCE_STALE_AFTER_HOURS = 48

TABLE_CHECKS = [
    ("game", "game_date"),
    ("game_batting_stats", "game_id"),
    ("game_pitching_stats", "game_id"),
    ("kbo_press_releases", "created_at"),
    ("player_milestones", "updated_at"),
    ("futures_game_schedules", "game_date"),
    ("player_splits_stats", "updated_at"),
    ("player_draft_histories", "created_at"),
    ("rag_chunks", "created_at"),
    ("team_events", "last_seen_at"),
    ("roster_transactions", "transaction_date"),
    ("ticket_prices", "season"),
    ("ticket_open_rules", "team_id"),
    ("stadium_seat_sections", "updated_at"),
    ("parking_lots", "updated_at"),
    ("parking_fee_rules", "parking_lot_id"),
    ("stadium_food_vendors", "last_verified_at"),
    ("stadium_food_menu_items", "vendor_id"),
    ("team_standings_daily", "standings_date"),
    ("stadium_transit_times", "measured_at"),
    ("stadium_congestion", "measured_at"),
    ("stadium_operation_notices", "published_at"),
    ("team_rivalries", "intensity"),
    ("cheer_songs", "introduction_year"),
]


def _check_datasource_health(session: Session) -> list[dict[str, Any]]:
    ds_repo = DataSourceRepository(session)
    rows = []
    for ds in ds_repo.get_all_active():
        stale = ""
        if ds.last_success_at:
            hours_since = (datetime.now(UTC).replace(tzinfo=None) - ds.last_success_at).total_seconds() / 3600
            stale = (
                f"STALE ({hours_since:.0f}h)"
                if hours_since > DATA_SOURCE_STALE_AFTER_HOURS
                else f"ok ({hours_since:.0f}h ago)"
            )
        else:
            stale = "NEVER"
        rows.append(
            {
                "key": ds.source_key,
                "domain": ds.target_domain,
                "freq": ds.crawl_frequency or "-",
                "stale": stale,
                "hash": (ds.last_content_hash or "-")[:12],
            },
        )
    return rows


def _check_table_health(session: Session) -> list[dict[str, Any]]:
    rows = []
    for table, date_col in TABLE_CHECKS:
        try:
            count = session.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()  # noqa: S608
            latest = session.execute(text(f"SELECT MAX({date_col}) FROM {table}")).scalar()  # noqa: S608
            rows.append(
                {
                    "table": table,
                    "rows": count,
                    "latest": str(latest or "-")[:20],
                },
            )
        except SQLAlchemyError as e:
            logger.warning("Health check table %s query failed: %s", table, e)
            rows.append({"table": table, "rows": "ERR", "latest": str(e)[:40]})
    return rows


_OPTIONAL_TABLES = frozenset(
    {
        "kbo_press_releases",
        "futures_game_schedules",
        "player_draft_histories",
        "player_milestones",
        "player_splits_stats",
        "stadium_congestion",
        "stadium_transit_times",
        "stadium_operation_notices",
        "cheer_songs",
        "team_rivalries",
    },
)


def _table_issue_counts(rows: list[dict[str, Any]]) -> tuple[int, int]:
    """Return (core_issue_count, optional_issue_count)."""
    core_issues = 0
    optional_issues = 0
    for r in rows:
        val = r.get("rows")
        is_issue = val in {0, "ERR"} or isinstance(val, str)
        if is_issue:
            if r.get("table") in _OPTIONAL_TABLES:
                optional_issues += 1
            else:
                core_issues += 1
    return core_issues, optional_issues


def _check_rag_chunks_health(session: Session) -> dict[str, Any]:
    """Check RAG chunks count and category breakdown."""
    total_chunks = session.query(RagChunk).count()
    pr_chunks = session.query(RagChunk).filter(RagChunk.meta.contains("press_release")).count()
    ms_chunks = session.query(RagChunk).filter(RagChunk.meta.contains("milestone")).count()
    fut_chunks = session.query(RagChunk).filter(RagChunk.meta.contains("futures_schedule")).count()
    spl_chunks = session.query(RagChunk).filter(RagChunk.meta.contains("player_splits")).count()

    healthy = total_chunks > 0
    return {
        "healthy": healthy,
        "total_chunks": total_chunks,
        "categories": {
            "press_release": pr_chunks,
            "milestone": ms_chunks,
            "futures_schedule": fut_chunks,
            "player_splits": spl_chunks,
        },
    }


def _check_telegram_bot_health() -> dict[str, Any]:
    """Check Telegram bot configuration and client status."""
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    enabled = bool(token and chat_id)

    return {
        "healthy": True,  # Non-fatal if token is not configured locally
        "token_configured": bool(token),
        "chat_id_configured": bool(chat_id),
        "client_enabled": enabled,
    }


def _check_api_routers_health() -> dict[str, Any]:
    """Check 8 core FastAPI REST API router endpoints using TestClient."""
    client = TestClient(app)
    endpoints = [
        ("GET", "/api/v1/health"),
        ("GET", "/api/v1/notices"),
        ("GET", "/api/v1/milestones"),
        ("GET", "/api/v1/futures/schedule"),
        ("GET", "/api/v1/players"),
        ("GET", "/api/v1/players/78224/splits"),
        ("GET", "/api/v1/players/drafts"),
        ("POST", "/api/v1/rag/hybrid-search"),
    ]

    results = []
    all_ok = True
    for method, path in endpoints:
        try:
            res = client.get(path) if method == "GET" else client.post(path, json={"query": "올스타전", "top_k": 2})

            status_ok = res.status_code == HTTPStatus.OK
            if not status_ok:
                all_ok = False
            results.append({"method": method, "path": path, "status": res.status_code, "ok": status_ok})
        except (AttributeError, KeyError, OSError, RuntimeError, SQLAlchemyError, TypeError, ValueError) as e:
            all_ok = False
            results.append({"method": method, "path": path, "status": 500, "error": str(e), "ok": False})

    return {
        "healthy": all_ok,
        "total_endpoints": len(endpoints),
        "successful_endpoints": sum(1 for r in results if r["ok"]),
        "details": results,
    }


def run_health_check(*, json_format: bool = False) -> dict[str, Any]:
    """Run comprehensive platform health diagnostics."""
    with SessionLocal() as session:
        ds_rows = _check_datasource_health(session)
        table_rows = _check_table_health(session)
        rag_health = _check_rag_chunks_health(session)

    telegram_health = _check_telegram_bot_health()
    api_health = _check_api_routers_health()

    stale_count = sum(1 for r in ds_rows if r["stale"].startswith("STALE"))
    never_count = sum(1 for r in ds_rows if r["stale"] == "NEVER")
    source_freshness_issue_count = stale_count + never_count
    core_issue_count, optional_issue_count = _table_issue_counts(table_rows)
    empty_count = core_issue_count + optional_issue_count

    overall_healthy = (
        source_freshness_issue_count == 0 and core_issue_count == 0 and rag_health["healthy"] and api_health["healthy"]
    )

    report_payload = {
        "timestamp": datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
        "overall_healthy": overall_healthy,
        "datasources": {
            "active": len(ds_rows),
            "stale": stale_count,
            "never_crawled": never_count,
            "freshness_issue_count": source_freshness_issue_count,
            "freshness_healthy": source_freshness_issue_count == 0,
            "rows": ds_rows,
        },
        "tables": {
            "total_checked": len(table_rows),
            "issue_count": empty_count,
            "required_issue_count": core_issue_count,
            "optional_issue_count": optional_issue_count,
            "rows": table_rows,
        },
        "rag_chunks": rag_health,
        "telegram_bot": telegram_health,
        "api_routers": api_health,
    }

    if json_format:
        sys.stdout.write(json.dumps(report_payload, indent=2, ensure_ascii=False) + "\n")
        return report_payload

    logger.info("=" * 60)
    logger.info(" KBO Platform Unified Health Diagnostics")
    logger.info("=" * 60)
    logger.info(" Timestamp: %s", report_payload["timestamp"])
    logger.info(" Overall Status: %s", "✅ HEALTHY" if overall_healthy else "⚠️ WARNING / ISSUES")
    logger.info("")

    logger.info("--- 1. DataSources & Pipeline ---")
    logger.info(" DataSources: %s active (%s stale, %s never crawled)", len(ds_rows), stale_count, never_count)
    logger.info(" Tables: %s checked (%s issues)", len(table_rows), empty_count)

    logger.info("")
    logger.info("--- 2. RAG Knowledge Base ---")
    logger.info(
        " Total Chunks: %d (PR: %d, Milestones: %d, Futures: %d, Splits: %d)",
        rag_health["total_chunks"],
        rag_health["categories"]["press_release"],
        rag_health["categories"]["milestone"],
        rag_health["categories"]["futures_schedule"],
        rag_health["categories"]["player_splits"],
    )

    logger.info("")
    logger.info("--- 3. Telegram Bot Messenger ---")
    logger.info(
        " Token Configured: %s | Chat ID Configured: %s | Client Enabled: %s",
        telegram_health["token_configured"],
        telegram_health["chat_id_configured"],
        telegram_health["client_enabled"],
    )

    logger.info("")
    logger.info("--- 4. REST API Routers (8 Endpoints) ---")
    for detail in api_health["details"]:
        icon = "✅" if detail["ok"] else "❌"
        logger.info("  %s %-4s %-35s Status %d", icon, detail["method"], detail["path"], detail["status"])

    logger.info("")
    logger.info("=" * 60)
    return report_payload


def build_arg_parser() -> argparse.ArgumentParser:
    """Build arg parser for health check CLI."""
    parser = argparse.ArgumentParser(description="KBO platform unified health check")
    parser.add_argument("--json", action="store_true", help="Output health check results in JSON format")
    parser.add_argument(
        "--exit-code",
        action="store_true",
        help="Exit with non-zero status code on health check issues",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    """Run CLI entrypoint."""
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO)
    report = run_health_check(json_format=args.json)

    if args.exit_code and not report["overall_healthy"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
