"""Unified KBO Platform Master CLI and Subcommand Router."""

from __future__ import annotations

import argparse
import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence


def _add_core_subparsers(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """Add workflow, diagnose, and report subparsers."""
    # 1. Workflow
    p_workflow = subparsers.add_parser("workflow", help="Execute multi-stage DAG workflow pipelines.")
    p_workflow.add_argument(
        "--workflow",
        type=str,
        default="daily_sync",
        choices=["daily_sync", "historical_recovery", "bulk_load"],
    )
    p_workflow.add_argument("--date", type=str, default=None, help="Target date (YYYYMMDD).")
    p_workflow.add_argument("--dry-run", action="store_true", help="Simulate execution.")
    p_workflow.add_argument("--json", action="store_true", help="Output report as JSON.")

    # 2. Diagnose
    p_diag = subparsers.add_parser("diagnose", help="Run multi-subsystem diagnostics & self-healing.")
    p_diag.add_argument(
        "--subsystem",
        type=str,
        default="all",
        choices=["all", "database", "scheduler", "crawler", "pipeline", "rag_vector"],
    )
    p_diag.add_argument("--fix", action="store_true", help="Attempt automated self-healing.")
    p_diag.add_argument("--json", action="store_true", help="Output report as JSON.")

    # 3. Report
    p_rep = subparsers.add_parser("report", help="Generate quality, gap, freshness, scouting, or executive reports.")
    p_rep.add_argument(
        "--category",
        type=str,
        default="all",
        choices=["all", "quality", "gap", "freshness", "executive", "scouting"],
    )
    p_rep.add_argument("--player", "-p", type=str, default="김도영", help="Target player name/ID for scouting.")
    p_rep.add_argument("--year", type=int, default=None, help="Target season year.")
    p_rep.add_argument("--format", type=str, default="markdown", choices=["markdown", "json", "html"])
    p_rep.add_argument("--output", type=str, default=None, help="Output file path.")


def _add_maintenance_and_config_subparsers(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """Add maintenance, config, and notify subparsers."""
    # 4. Maintenance
    p_maint = subparsers.add_parser("maintenance", help="Run database maintenance tasks and integrity fixes.")
    p_maint.add_argument(
        "--task",
        type=str,
        default="all",
        choices=["all", "pa_audit", "null_player_ids", "cleanup", "checkpoint"],
    )
    p_maint.add_argument("--year", type=int, default=None, help="Target season year.")
    p_maint.add_argument("--apply", action="store_true", help="Apply changes (default is dry-run).")
    p_maint.add_argument("--json", action="store_true", help="Output report as JSON.")

    # 5. Config
    p_conf = subparsers.add_parser("config", help="Audit and validate platform configuration & secrets.")
    p_conf.add_argument(
        "--env",
        type=str,
        default="production",
        choices=["local", "development", "staging", "production", "ci"],
    )
    p_conf.add_argument("--strict", action="store_true", help="Fail on warnings.")
    p_conf.add_argument("--json", action="store_true", help="Output report as JSON.")

    # 6. Notify
    p_notif = subparsers.add_parser("notify", help="Dispatch system notifications to Telegram, Slack, or Console.")
    p_notif.add_argument("--channel", type=str, default="console", choices=["all", "telegram", "slack", "console"])
    p_notif.add_argument("--title", type=str, required=True, help="Notification title.")
    p_notif.add_argument("--body", type=str, required=True, help="Notification message body.")
    p_notif.add_argument("--priority", type=str, default="normal", choices=["low", "normal", "high", "critical"])
    p_notif.add_argument("--dry-run", action="store_true", help="Simulate dispatch.")
    p_notif.add_argument("--json", action="store_true", help="Output report as JSON.")


def _add_data_and_ops_subparsers(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """Add migrate, seed, detect, and sync subparsers."""
    # 7. Migrate
    p_mig = subparsers.add_parser("migrate", help="Run and inspect database schema migrations.")
    p_mig.add_argument("--dialect", type=str, default="oracle", choices=["oracle", "sqlite", "postgresql", "pgvector"])
    p_mig.add_argument("--db-url", type=str, default=None, help="Target database connection URL.")
    p_mig.add_argument("--dry-run", action="store_true", help="Preview SQL statements without executing.")
    p_mig.add_argument("--status", action="store_true", help="Show migration status report without applying.")
    p_mig.add_argument("--json", action="store_true", help="Output report as JSON.")

    # 8. Seed
    p_seed = subparsers.add_parser("seed", help="Generate and seed synthetic KBO scenario data.")
    p_seed.add_argument("--season", type=int, default=2026, help="Target season year.")
    p_seed.add_argument("--games-per-team", type=int, default=2, help="Number of games per team.")
    p_seed.add_argument("--players-per-team", type=int, default=9, help="Number of players per team.")
    p_seed.add_argument("--db-url", type=str, default=None, help="Target database connection URL.")
    p_seed.add_argument("--json", action="store_true", help="Output generation summary as JSON.")

    # 9. Detect
    p_det = subparsers.add_parser("detect", help="Detect statistical and operational platform anomalies.")
    p_det.add_argument("--metric", type=str, default="all", help="Specific metric to audit.")
    p_det.add_argument("--sensitivity", type=str, default="medium", choices=["low", "medium", "high"])
    p_det.add_argument("--json", action="store_true", help="Output report as JSON.")

    # 10. Sync
    p_sync = subparsers.add_parser("sync", help="Synchronize SQLite data to Oracle Autonomous Database (OCI).")
    p_sync.add_argument("--source-url", type=str, default="sqlite:///./data/kbo_dev.db", help="Source SQLite URL.")
    p_sync.add_argument("--target-url", type=str, default=None, help="Target Oracle database URL.")
    p_sync.add_argument("--dry-run", action="store_true", help="Preview sync row counts without writing.")
    p_sync.add_argument("--apply", action="store_true", help="Apply synchronization.")
    p_sync.add_argument("--verify", action="store_true", help="Verify row counts between source and target.")
    p_sync.add_argument("--mode", type=str, default="full", choices=["full", "incremental"])


def _add_rag_and_sim_subparsers(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """Add RAG and simulation subparsers."""
    # 11. RAG
    p_rag = subparsers.add_parser("rag", help="Query knowledge base or benchmark RAG retrieval performance.")
    rag_subs = p_rag.add_subparsers(dest="rag_command", help="RAG action to execute")

    p_rag_q = rag_subs.add_parser("query", help="Query KBO hybrid vector/BM25 knowledge base.")
    p_rag_q.add_argument("query", nargs="?", type=str, default="", help="Search query string.")
    p_rag_q.add_argument("--query", "-q", dest="query_opt", type=str, default="", help="Search query string.")
    p_rag_q.add_argument("--top-k", "-k", type=int, default=5, help="Top K results.")
    p_rag_q.add_argument("--category", "-c", type=str, default=None, help="Category filter.")
    p_rag_q.add_argument("--json", action="store_true", help="Output JSON format.")

    p_rag_eval = rag_subs.add_parser("evaluate", help="Benchmark RAG retrieval against golden queries.")
    p_rag_eval.add_argument(
        "--golden-path",
        "-g",
        type=str,
        default="Docs/references/rag_golden_queries.json",
        help="Path to golden queries JSON.",
    )
    p_rag_eval.add_argument("--top-k", "-k", type=int, default=5, help="Top K evaluation depth.")
    p_rag_eval.add_argument("--limit", "-n", type=int, default=None, help="Max queries to evaluate.")
    p_rag_eval.add_argument("--min-recall", type=float, default=0.85, help="Minimum Recall@K target.")
    p_rag_eval.add_argument("--min-mrr", type=float, default=0.70, help="Minimum MRR target.")
    p_rag_eval.add_argument("--max-p95-ms", type=float, default=500.0, help="Maximum p95 latency ms target.")
    p_rag_eval.add_argument("--strict", action="store_true", help="Exit 1 if SLA is violated.")
    p_rag_eval.add_argument("--json", action="store_true", help="Output JSON format.")

    p_rag_census = rag_subs.add_parser("census", help="Run read-only R2 RAG identity census & write manifest.")
    p_rag_census.add_argument(
        "--source",
        action="append",
        choices=None,
        help="Limit the census to one source table; repeat for multiple",
    )
    p_rag_census.add_argument("--season", type=int, help="Limit source records to one season/year")
    p_rag_census.add_argument("--sample", type=int, default=20, help="Number of unsafe entries to print in JSON output")
    p_rag_census.add_argument("--output", type=str, help="Write the complete apply-gated JSON manifest to this path")
    p_rag_census.add_argument("--dry-run", action="store_true", help="Explicitly confirm read-only mode")
    p_rag_census.add_argument(
        "--fail-on-unsafe",
        action="store_true",
        help="Return exit code 1 when orphan, collision, or target-conflict rows exist",
    )
    p_rag_census.add_argument("--json", action="store_true", help="Render the summary as JSON")

    # 12. Simulate

    # 12. Simulate
    p_sim = subparsers.add_parser("simulate", help="Simulate live KBO game event stream & real-time WPA.")
    p_sim.add_argument("--game-id", type=str, default="20260401LGHT0", help="Game ID identifier.")
    p_sim.add_argument("--home-team", type=str, default="KIA", help="Home team code.")
    p_sim.add_argument("--away-team", type=str, default="LG", help="Away team code.")
    p_sim.add_argument("--innings", type=int, default=9, help="Max regulation innings.")
    p_sim.add_argument("--speed", type=float, default=0.0, help="Simulation speed multiplier.")
    p_sim.add_argument("--notify", action="store_true", help="Dispatch hot moment alerts.")
    p_sim.add_argument("--seed", type=int, default=None, help="RNG seed for deterministic playback.")
    p_sim.add_argument("--json", action="store_true", help="Output summary as JSON.")


def _add_advanced_subparsers(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """Add schema drift, serve, predict, and bulk load subparsers."""
    _add_rag_and_sim_subparsers(subparsers)

    # 13. Schema Drift
    p_drift = subparsers.add_parser(
        "drift",
        aliases=["detect-drift"],
        help="Detect database schema drift against ORM models & generate DDL.",
    )
    p_drift.add_argument(
        "--dialect",
        "-d",
        type=str,
        default=None,
        help="Target SQL dialect (oracle, sqlite, postgresql).",
    )
    p_drift.add_argument("--db-url", type=str, default=None, help="Database connection URL.")
    p_drift.add_argument("--tables", "-t", type=str, default=None, help="Comma-separated tables to inspect.")
    p_drift.add_argument("--apply", action="store_true", help="Apply generated remediation DDL.")
    p_drift.add_argument("--output-file", "-o", type=str, default=None, help="Save generated DDL to file.")
    p_drift.add_argument("--strict", action="store_true", help="Exit 1 if any drift is detected.")
    p_drift.add_argument("--json", action="store_true", help="Output report as JSON.")

    # 14. Serve
    p_srv = subparsers.add_parser("serve", help="Start FastAPI REST & WebSocket server gateway.")
    p_srv.add_argument("--host", type=str, default="127.0.0.1", help="Network host interface to bind.")
    p_srv.add_argument("--port", "-p", type=int, default=8000, help="Port to listen on.")
    p_srv.add_argument("--reload", action="store_true", help="Enable auto-reloading on code changes.")
    p_srv.add_argument("--workers", "-w", type=int, default=1, help="Number of worker processes.")

    # 15. Predict
    p_pred = subparsers.add_parser("predict", help="Predict KBO game win probabilities & expected scores.")
    p_pred.add_argument("--game-id", "-g", type=str, default=None, help="Target Game ID identifier.")
    p_pred.add_argument("--home", type=str, default=None, help="Home team code.")
    p_pred.add_argument("--away", type=str, default=None, help="Away team code.")
    p_pred.add_argument("--starter-home", type=str, default=None, help="Home starter pitcher name.")
    p_pred.add_argument("--starter-away", type=str, default=None, help="Away starter pitcher name.")
    p_pred.add_argument("--year", "--season", type=int, default=2024, help="Season year (default: 2024).")
    p_pred.add_argument("--date", type=str, default=None, help="Game date (YYYY-MM-DD).")
    p_pred.add_argument(
        "--format",
        choices=["text", "ascii", "markdown", "json"],
        default="text",
        help="Output format.",
    )
    p_pred.add_argument("--json", action="store_true", help="Output JSON format.")

    # 16. Bulk Load
    p_bulk = subparsers.add_parser(
        "bulk-load",
        aliases=["bulk_load"],
        help="High-throughput parallel chunk loader with atomic checkpoints for KBO data.",
    )
    p_bulk.add_argument(
        "--category",
        type=str,
        default="pbp",
        choices=["pbp", "boxscore", "season_stats", "schedule", "all"],
        help="Target data category (default: pbp).",
    )
    p_bulk.add_argument("--start-year", type=int, default=2020, help="Start season year (default: 2020).")
    p_bulk.add_argument("--end-year", type=int, default=2024, help="End season year (default: 2024).")
    p_bulk.add_argument("--concurrency", type=int, default=4, help="Worker concurrency (default: 4).")
    p_bulk.add_argument("--chunk-size", type=int, default=1, help="Years per chunk (default: 1).")
    p_bulk.add_argument("--resume", action="store_true", help="Resume from existing checkpoint.")
    p_bulk.add_argument("--reset-checkpoint", action="store_true", help="Delete checkpoint before run.")
    p_bulk.add_argument("--json", action="store_true", help="Output result as JSON.")

    # 17. Compare
    p_cmp = subparsers.add_parser(
        "compare",
        help="Search for similar KBO players or perform 1:1 head-to-head sabermetric comparisons.",
    )
    p_cmp.add_argument("--player1", "-p1", type=str, default=None, help="Player 1 name or ID.")
    p_cmp.add_argument("--player2", "-p2", type=str, default=None, help="Player 2 name or ID.")
    p_cmp.add_argument("--year1", "--season1", type=int, default=None, help="Season year for Player 1.")
    p_cmp.add_argument("--year2", "--season2", type=int, default=None, help="Season year for Player 2.")
    p_cmp.add_argument("--find-similar", "-s", type=str, default=None, help="Find similar players for name/ID.")
    p_cmp.add_argument("--top-k", "-k", type=int, default=5, help="Number of similar players (default: 5).")
    p_cmp.add_argument(
        "--format",
        choices=["text", "ascii", "markdown", "json"],
        default="ascii",
        help="Output format.",
    )
    p_cmp.add_argument("--json", action="store_true", help="Output result in JSON format.")


def _add_cert_and_lineage_subparsers(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """Add certification and lineage inspection subparsers."""
    # 18. Certify
    p_cert = subparsers.add_parser(
        "certify",
        help="Execute end-to-end KBO Platform Production Certification Gate.",
    )
    p_cert.add_argument("--production", action="store_true", help="Run full production certification.")
    p_cert.add_argument("--local", action="store_true", help="Run local certification mode.")
    p_cert.add_argument(
        "--json-out",
        type=str,
        default="data/certification/report.json",
        help="Path to output report JSON artifact.",
    )
    p_cert.add_argument("--gate", type=str, default=None, help="Execute specific gate.")
    p_cert.add_argument("--fail-fast", action="store_true", help="Stop on first failure.")
    p_cert.add_argument("--json", action="store_true", help="Output JSON report to stdout.")

    # 19. Lineage
    p_lin = subparsers.add_parser(
        "lineage",
        help="Inspect data provenance, transformation DAGs, and entity lineage.",
    )
    p_lin.add_argument("subcommand", choices=["game", "player", "audit"], help="Target lineage entity to inspect.")
    p_lin.add_argument("identifier", nargs="?", default=None, help="Target game ID or player name/ID.")
    p_lin.add_argument("--season", "-s", type=int, default=None, help="Season year.")
    p_lin.add_argument("--metric", "-m", type=str, default="hits", help="Target metric name (hits, hr, avg, etc.).")
    p_lin.add_argument(
        "--format",
        choices=["tree", "mermaid", "json"],
        default="tree",
        help="Output visualization format.",
    )
    p_lin.add_argument("--json", action="store_true", help="Output result in JSON format.")

    # 20. Formula
    p_form = subparsers.add_parser(
        "formula",
        help="Inspect sabermetric formulas, linear weights, and reproducibility audits.",
    )
    p_form.add_argument(
        "subcommand",
        choices=["list", "explain", "eval", "audit"],
        help="Formula subcommand to execute.",
    )
    p_form.add_argument("metric", nargs="?", default=None, help="Target metric identifier (e.g. wOBA, OPS_PLUS, FIP).")
    p_form.add_argument("--player", "-p", type=str, default=None, help="Player name or ID.")
    p_form.add_argument("--season", "-s", type=int, default=None, help="Season year.")
    p_form.add_argument("--category", "-c", type=str, default=None, help="Metric category.")
    p_form.add_argument("--sample", type=int, default=None, help="Sample count limit.")
    p_form.add_argument("--save-artifact", type=str, default=None, help="Path to save report JSON.")
    p_form.add_argument("--json", action="store_true", help="Output result in JSON format.")


def build_master_parser() -> argparse.ArgumentParser:
    """Build root command-line parser with all platform subcommands."""
    parser = argparse.ArgumentParser(
        prog="kbo",
        description="Unified KBO Playwright Data & Analytics Platform Master CLI.",
    )
    subparsers = parser.add_subparsers(dest="command", help="Platform command to execute")

    _add_core_subparsers(subparsers)
    _add_maintenance_and_config_subparsers(subparsers)
    _add_data_and_ops_subparsers(subparsers)
    _add_advanced_subparsers(subparsers)
    _add_cert_and_lineage_subparsers(subparsers)

    return parser


def _get_dispatcher_map() -> dict[str, Callable[[list[str]], int]]:
    """Return map of subcommand strings to their respective module main entrypoints."""
    from src.cli.bulk_load import main as bulk_main
    from src.cli.certify import main as cert_main
    from src.cli.compare_players import main as cmp_main
    from src.cli.detect_anomalies import main as det_main
    from src.cli.detect_schema_drift import main as drift_main
    from src.cli.diagnose_system import main as diag_main
    from src.cli.formula import main as form_main
    from src.cli.generate_reports import main as rep_main
    from src.cli.lineage import main as lin_main
    from src.cli.predict_matchups import main as pred_main
    from src.cli.run_maintenance import main as maint_main
    from src.cli.run_migrations import main as mig_main
    from src.cli.run_workflow import main as wf_main
    from src.cli.seed_synthetic_data import main as seed_main
    from src.cli.send_notification import main as notif_main
    from src.cli.serve_api import main as srv_main
    from src.cli.simulate_game import main as sim_main
    from src.cli.sync_sqlite_to_oci import main as sync_main
    from src.cli.validate_config import main as conf_main

    def _rag_dispatcher(sub_args: list[str]) -> int:
        if not sub_args:
            print("Usage: kbo rag <query|evaluate|census> [options]")  # noqa: T201
            return 1
        subcmd = sub_args[0]
        rest = sub_args[1:]
        if subcmd == "query":
            from src.cli.rag.query import main as q_main

            return q_main(rest)
        if subcmd == "evaluate":
            from src.cli.rag.evaluate import main as eval_main

            return eval_main(rest)
        if subcmd == "census":
            from src.cli.rag.census_rag_identity import main as census_main

            return census_main(rest)
        print(f"Unknown rag subcommand: {subcmd}. Use 'query', 'evaluate', or 'census'.")  # noqa: T201
        return 1

    return {
        "workflow": wf_main,
        "diagnose": diag_main,
        "report": rep_main,
        "maintenance": maint_main,
        "config": conf_main,
        "notify": notif_main,
        "migrate": mig_main,
        "seed": seed_main,
        "detect": det_main,
        "sync": sync_main,
        "rag": _rag_dispatcher,
        "simulate": sim_main,
        "drift": drift_main,
        "detect-drift": drift_main,
        "serve": srv_main,
        "predict": pred_main,
        "bulk-load": bulk_main,
        "bulk_load": bulk_main,
        "compare": cmp_main,
        "certify": cert_main,
        "lineage": lin_main,
        "formula": form_main,
    }


def main(argv: Sequence[str] | None = None) -> int:
    """CLI execution entrypoint and subcommand dispatcher."""
    parser = build_master_parser()
    args, _remaining = parser.parse_known_args(argv)

    if not args.command:
        parser.print_help()
        return 0

    sub_args = list(argv[1:]) if argv and len(argv) > 1 else []
    dispatch_map = _get_dispatcher_map()
    handler = dispatch_map.get(args.command)

    if handler is not None:
        return handler(sub_args)

    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
