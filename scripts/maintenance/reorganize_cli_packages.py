"""Script to reorganize flat src/cli modules into domain subpackages with 100% backward compatibility wrappers."""

from __future__ import annotations

from pathlib import Path

CLI_DIR = Path("src/cli")

MAPPING: dict[str, str] = {
    # 1. Collection (38)
    "collect_games": "collection",
    "collect_profiles": "collection",
    "collect_rosters": "collection",
    "crawl_awards": "collection",
    "crawl_congestion": "collection",
    "crawl_external_stats": "collection",
    "crawl_futures": "collection",
    "crawl_futures_schedule": "collection",
    "crawl_historical_seasons": "collection",
    "crawl_kbo_official_events": "collection",
    "crawl_legacy_reviews": "collection",
    "crawl_milestones": "collection",
    "crawl_operation_notices": "collection",
    "crawl_p0_data": "collection",
    "crawl_parking": "collection",
    "crawl_phase1_extra": "collection",
    "crawl_player_drafts": "collection",
    "crawl_player_splits": "collection",
    "crawl_press_releases": "collection",
    "crawl_retire": "collection",
    "crawl_roster_transactions": "collection",
    "crawl_schedule": "collection",
    "crawl_seat_sections": "collection",
    "crawl_stadium_food": "collection",
    "crawl_staff_register": "collection",
    "crawl_team_events": "collection",
    "crawl_text_relay": "collection",
    "crawl_ticket_info": "collection",
    "crawl_transit_time": "collection",
    "discover_historical_players": "collection",
    "fetch_kbo_pbp": "collection",
    "ingest_historical_archive": "collection",
    "ingest_historical_season": "collection",
    "ingest_mock_game_html": "collection",
    "ingest_schedule_html": "collection",
    "seed_data_sources": "collection",
    "seed_p1_data": "collection",
    "seed_relay_validation_metrics": "collection",
    # 2. Pipelines (13)
    "daily_highlight_batch": "pipelines",
    "daily_preview_batch": "pipelines",
    "daily_review_batch": "pipelines",
    "daily_story_batch": "pipelines",
    "pipeline_dashboard": "pipelines",
    "run_advanced_daily": "pipelines",
    "run_all_crawlers": "pipelines",
    "run_daily_update": "pipelines",
    "run_periodic_extras": "pipelines",
    "run_pipeline_demo": "pipelines",
    "run_weekly_maintenance": "pipelines",
    # 3. Backfill (14)
    "auto_healer": "backfill",
    "backfill_1982_pilot": "backfill",
    "backfill_advanced_stats": "backfill",
    "backfill_historical_details": "backfill",
    "backfill_pregame_previews": "backfill",
    "backfill_rag_index_identity": "backfill",
    "backfill_starting_pitchers_from_stats": "backfill",
    "fix_player_names": "backfill",
    "rebuild_relay_events": "backfill",
    "reconcile_postgame": "backfill",
    "regenerate_game_stories": "backfill",
    "regenerate_review_summaries": "backfill",
    "repair_game_stats": "backfill",
    "retry_daily_failures": "backfill",
    # 4. Calc (13)
    "calculate_matchups": "calc",
    "calculate_projections": "calc",
    "calculate_rankings": "calc",
    "calculate_sabermetrics": "calc",
    "calculate_standings": "calc",
    "monthly_pa_audit": "calc",
    "monthly_team_audit": "calc",
    "monthly_unified_audit": "calc",
    "recalc_milestones": "calc",
    "recalc_player_game_stats": "calc",
    "recalc_player_stats": "calc",
    "recalc_season_stats": "calc",
    "recalc_team_stats": "calc",
    # 5. Reports (25)
    "audit_awards": "reports",
    "audit_historical_lake": "reports",
    "check_data_status": "reports",
    "crawler_live_smoke": "reports",
    "crawler_selector_gate": "reports",
    "dashboard_report": "reports",
    "data_integrity_checker": "reports",
    "data_quality_regression_pack": "reports",
    "data_quality_report": "reports",
    "db_healthcheck": "reports",
    "diagnose_coach_pitching": "reports",
    "diagnose_crawler_failure": "reports",
    "freshness_gate": "reports",
    "gap_report": "reports",
    "generate_quality_report": "reports",
    "health_check": "reports",
    "historical_boxscore_import": "reports",
    "historical_coverage_report": "reports",
    "historical_import": "reports",
    "monitor_data_freshness": "reports",
    "morning_pbp_report": "reports",
    "quality_dashboard": "reports",
    "quality_gate_check": "reports",
    "smart_polling_gate": "reports",
    "compare_crawl_evidence": "reports",
    # 6. RAG (11)
    "audit_rag_index": "rag",
    "bootstrap_rag_eval_corpus": "rag",
    "build_rag_index": "rag",
    "evaluate_rag_retrieval": "rag",
    "evaluate_rag_routing": "rag",
    "index_rag_knowledge": "rag",
    "inventory_rag_corpus": "rag",
    "propagate_rag_index": "rag",
    "query_rag_knowledge": "rag",
    "tombstone_rag_chunks": "rag",
    "verify_chunk_quality": "rag",
    # 7. Sync (10)
    "apply_oracle_migrations": "sync",
    "apply_postgres_migrations": "sync",
    "load_text_relay": "sync",
    "lookup_official_players": "sync",
    "refresh_source_snapshots": "sync",
    "run_pgvector_migration": "sync",
    "sqlite_integrity_guard": "sync",
    "stage_official_season_stats": "sync",
    "supersede_award_snapshots": "sync",
    "sync_sqlite_to_oci": "sync",
    # 8. Alerts (4)
    "send_milestone_daily_summary": "alerts",
    "send_postgame_wpa_alerts": "alerts",
    "send_pregame_alerts": "alerts",
    "send_today_pregame_alerts": "alerts",
    # 9. Live & Utilities (6)
    "analyze_data": "live",
    "api_server": "live",
    "dashboard": "live",
    "generate_game_preview": "live",
    "live_boxscore": "live",
    "live_crawler": "live",
    "run_api_server": "live",
}


def _init_packages(domains: set[str]) -> None:
    for domain in sorted(domains):
        pkg_dir = CLI_DIR / domain
        pkg_dir.mkdir(parents=True, exist_ok=True)
        init_file = pkg_dir / "__init__.py"
        if not init_file.exists():
            init_file.write_text(
                f'"""CLI {domain} subpackage."""\n\nfrom __future__ import annotations\n',
                encoding="utf-8",
            )
            print(f"Created package {domain}/__init__.py")


def _fix_dashboard() -> None:
    dashboard_src = CLI_DIR / "dashboard.py"
    if dashboard_src.exists():
        content = dashboard_src.read_text(encoding="utf-8")
        if "def main(" not in content:
            content = content.replace(
                'if __name__ == "__main__":\n    logging.basicConfig(level=logging.INFO, format="%(message)s")\n    generate_dashboard()',
                'def main() -> int:\n    """CLI entrypoint."""\n    generate_dashboard()\n    return 0\n\n\nif __name__ == "__main__":\n    logging.basicConfig(level=logging.INFO, format="%(message)s")\n    sys.exit(main())',
            )
            dashboard_src.write_text(content, encoding="utf-8")


def _move_module(mod_name: str, domain: str) -> bool:
    src_file = CLI_DIR / f"{mod_name}.py"
    dest_file = CLI_DIR / domain / f"{mod_name}.py"

    if not src_file.exists():
        if dest_file.exists():
            print(f"Already moved: {mod_name} -> {domain}")
        else:
            print(f"WARN: Source file not found: {src_file}")
        return False

    if not dest_file.exists():
        content = src_file.read_text(encoding="utf-8")
        dest_file.write_text(content, encoding="utf-8")
    elif "Compatibility wrapper for src.cli" in dest_file.read_text(encoding="utf-8"):
        content = src_file.read_text(encoding="utf-8")
        if "Compatibility wrapper for src.cli" not in content:
            dest_file.write_text(content, encoding="utf-8")

    wrapper_content = f'''"""Compatibility wrapper for src.cli.{domain}.{mod_name}."""

from __future__ import annotations

import sys

from src.cli.{domain} import {mod_name} as _target_module

# Re-export all symbols and alias module in sys.modules so imports and patches work seamlessly
globals().update({{k: v for k, v in _target_module.__dict__.items() if not (k.startswith("__") and k.endswith("__"))}})
sys.modules[__name__] = _target_module

if __name__ == "__main__":
    if hasattr(_target_module, "main"):
        sys.exit(_target_module.main())
'''

    src_file.write_text(wrapper_content, encoding="utf-8")
    return True


def main() -> None:
    """Reorganize flat CLI modules into domain packages."""
    domains = set(MAPPING.values())
    _init_packages(domains)
    _fix_dashboard()

    moved_count = sum(_move_module(mod_name, domain) for mod_name, domain in sorted(MAPPING.items()))
    print(f"Successfully reorganized {moved_count} CLI modules into {len(domains)} domain packages!")


if __name__ == "__main__":
    main()
