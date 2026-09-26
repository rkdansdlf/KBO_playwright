"""Maintenance, calculation, audit, and recovery jobs for KBO scheduler."""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import UTC, datetime, timedelta

from tenacity import retry, stop_after_attempt, wait_exponential

from src.cli.collection.crawl_retire import main as crawl_retire_main
from src.db.engine import SessionLocal, get_db_session
from src.notifications.alert_dto import AlertEvent, AlertSeverity, AlertSource
from src.scheduler.alerting import alert_failure, alert_success, alert_warning
from src.scheduler.config import (
    KST,
    SCHEDULER_JOB_EXCEPTIONS,
    _env_int,
)
from src.scheduler.jobs.live import _previous_day_kst
from src.scheduler.locks import (
    MAINTENANCE_LOCK,
    _scheduler_job_lock,
    _with_lock_skip_guard,
)

logger = logging.getLogger("src.scheduler.jobs.maintenance")

# Write-intent gates required by build_rag_index._write_target_errors for the
# production Oracle RAG target. Scoped to this job only so manual CLI builds
# stay guarded by default.
_RAG_INCREMENTAL_WRITE_ENV = {
    "RAG_TARGET_ENV": "production",
    "RAG_INDEX_ALLOW_WRITE": "1",
    "RAG_INDEX_ALLOW_PRODUCTION_WRITE": "1",
}

#: Upper bound for the integrity recheck window. Each extra day re-runs the whole
#: check suite, so an unbounded window would let one job monopolise the database.
_INTEGRITY_RECHECK_LOOKBACK_MAX = 7


def _rag_vector_backend_configured() -> bool:
    """Return whether the scheduled RAG job has a supported dense target."""
    database_url = os.getenv("DATABASE_URL", "")
    return database_url.startswith("oracle") or bool(os.getenv("PGVECTOR_URL") or os.getenv("PGVECTOR_TEST_URL"))


@_with_lock_skip_guard
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=120, max=600),
    retry_error_callback=alert_failure,
)
def crawl_retired_players_job(limit: int | None = None) -> None:
    """Monthly job: Crawl retired/inactive player statistics. Runs on 1st of month at 02:00 KST."""
    with _scheduler_job_lock(MAINTENANCE_LOCK):
        logger.info("=== Starting Monthly Retired Player Crawl ===")
        try:
            current_year = datetime.now(KST).year
            start_year = 1982
            end_year = current_year - 1

            logger.info("Crawling retired players from %d to %d (active_year=%d)", start_year, end_year, current_year)
            args = [
                "--start-year",
                str(start_year),
                "--end-year",
                str(end_year),
                "--active-year",
                str(current_year),
                "--concurrency",
                "2",
                "--delay",
                "2.0",
            ]
            if limit is not None:
                args.extend(["--limit", str(limit)])

            crawl_retire_main(args)

            logger.info("=== Monthly Retired Player Crawl Completed Successfully ===")
            alert_success("crawl_retired_players_job")

        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("Retired player crawl attempt failed")
            raise


@_with_lock_skip_guard
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=60, max=300),
    retry_error_callback=alert_failure,
)
def _crawl_team_info_history() -> None:
    """Weekly job: Refresh team info and team history data."""
    with _scheduler_job_lock(MAINTENANCE_LOCK):
        logger.info("=== Starting Team Info/History Refresh ===")
        try:
            from src.crawlers.team_history_crawler import TeamHistoryCrawler
            from src.crawlers.team_info_crawler import TeamInfoCrawler

            crawler_info = TeamInfoCrawler()
            data_info = asyncio.run(crawler_info.crawl(save=True))
            asyncio.run(crawler_info.save(data_info))

            crawler_hist = TeamHistoryCrawler()
            data_hist = asyncio.run(crawler_hist.crawl())
            asyncio.run(crawler_hist.save(data_hist))
            logger.info("=== Team Info/History Refresh Completed ===")
        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("Team info/history refresh failed")


@_with_lock_skip_guard
def weekly_sla_report_job() -> None:
    """Weekly SLA report job: computes past 7 days SLA and alerts. Runs Monday 06:00 KST."""
    from src.monitoring.sla_tracker import SlaTracker

    with _scheduler_job_lock(MAINTENANCE_LOCK):
        logger.info("=== Starting Weekly SLA Report Generation ===")
        with SessionLocal() as session:
            tracker = SlaTracker(session)
            tracker.send_weekly_sla_report()
        logger.info("=== Weekly SLA Report Generation Completed ===")


@_with_lock_skip_guard
def compute_standings_job() -> None:
    """Compute daily standings with home/away splits, recent 10, weekly trends."""
    with _scheduler_job_lock(MAINTENANCE_LOCK):
        logger.info("=== Starting Standings Computation ===")
        try:
            from src.cli.calc.calculate_standings import StandingsCalculator

            current_year = datetime.now(KST).year
            with get_db_session() as session:
                StandingsCalculator(session).calculate_year(current_year)
            logger.info("=== Standings Computation Completed ===")
        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("Standings computation failed")


@_with_lock_skip_guard
def aggregate_team_defense_job() -> None:
    """Aggregate daily team defense statistics (SB, CS, CS%, PB, WP). Runs daily at 03:45 KST."""
    with _scheduler_job_lock(MAINTENANCE_LOCK):
        logger.info("=== Starting Team Defense Aggregation ===")
        try:
            from src.aggregators.team_defense_aggregator import aggregate_team_defense

            current_year = datetime.now(KST).year
            aggregate_team_defense(current_year)
            logger.info("=== Team Defense Aggregation Completed ===")
        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("Team defense aggregation failed")


@_with_lock_skip_guard
def compute_rankings_job() -> None:
    """Compute daily player rankings across all categories. Runs daily at 04:00 KST."""
    with _scheduler_job_lock(MAINTENANCE_LOCK):
        logger.info("=== Starting Rankings Computation ===")
        try:
            from src.cli.calc.calculate_rankings import rebuild_rankings

            current_year = datetime.now(KST).year
            rebuild_rankings(current_year)
            logger.info("=== Rankings Computation Completed ===")
        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("Rankings computation failed")


@_with_lock_skip_guard
def auto_heal_games_job() -> None:
    """Auto-Healer: scan for stuck SCHEDULED/UNRESOLVED games and score sum mismatches."""
    with _scheduler_job_lock(MAINTENANCE_LOCK):
        logger.info("=== Starting Auto-Healer (Stuck & Inconsistent Games) ===")
        try:
            from src.cli.backfill.auto_healer import run_healer_async

            unresolved_count = asyncio.run(run_healer_async(dry_run=False))
            if unresolved_count == 0:
                logger.info("=== Auto-Healer Completed (0 unresolved) ===")
            else:
                logger.warning("=== Auto-Healer Completed with unresolved games (count=%d) ===", unresolved_count)
        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("Auto-Healer job failed")


@_with_lock_skip_guard
def heal_unverified_pbp_job() -> None:
    """PBP Healer: scan for unverified PBP games and re-crawl from KBO official site."""
    with _scheduler_job_lock(MAINTENANCE_LOCK):
        logger.info("=== Starting PBP Auto-Healer ===")
        try:
            import os

            from src.cli.backfill.auto_healer import run_pbp_healer

            lookback = os.getenv("PBP_HEALER_LOOKBACK_DAYS", "3")
            exit_code = run_pbp_healer(["--lookback-days", lookback])
            if exit_code == 0:
                logger.info("=== PBP Auto-Healer Completed (no failures) ===")
            else:
                logger.warning("=== PBP Auto-Healer Completed with some failures (exit_code=%d) ===", exit_code)
        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("PBP Auto-Healer job failed")


def _integrity_recheck_lookback_days() -> int:
    """Return how many prior days the integrity job re-evaluates."""
    raw = os.getenv("INTEGRITY_RECHECK_LOOKBACK_DAYS", "2").strip()
    try:
        return max(0, min(int(raw), _INTEGRITY_RECHECK_LOOKBACK_MAX))
    except ValueError:
        return 2


def _integrity_target_dates() -> list[str]:
    """Return the previous KST day plus a bounded recheck window, newest first.

    Incident keys are date-scoped (``integrity:<check>:<YYYYMMDD>``) and the job
    only ever evaluated the previous day. A check that failed transiently -- for
    example when it ran before the 03:00 crawl landed, or against a briefly stale
    database -- therefore had no later chance to pass, so its incident stayed OPEN
    forever and the dashboard never cleared. Re-checking a short trailing window
    re-evaluates those keys so a genuine recovery resolves the incident by itself.

    Args:
        None.

    Returns:
        Compact ``YYYYMMDD`` dates, newest first, always at least one entry.

    """
    from src.utils.date_helpers import parse_date_str_lenient

    newest = parse_date_str_lenient(_previous_day_kst())
    lookback = _integrity_recheck_lookback_days()
    return [(newest - timedelta(days=offset)).strftime("%Y%m%d") for offset in range(lookback + 1)]


@_with_lock_skip_guard
def data_integrity_check_job() -> None:
    """Run post-crawl data integrity validation (daily at 04:45 KST)."""
    with _scheduler_job_lock(MAINTENANCE_LOCK):
        logger.info("=== Starting Data Integrity Check ===")
        try:
            from src.cli.reports.data_integrity_checker import run_integrity_checks
            from src.notifications.bridge import apply_incidents

            events: list[AlertEvent] = []
            resolve_keys: list[str] = []
            for target_date in _integrity_target_dates():
                try:
                    report = run_integrity_checks(target_date)
                except SCHEDULER_JOB_EXCEPTIONS:
                    # Isolate the fault: a date we could not evaluate must not block
                    # the rest of the window, and its incidents stay untouched because
                    # their state is unknown rather than healthy.
                    logger.exception("Data Integrity Check raised for %s; skipping its incidents", target_date)
                    continue
                for result in report.results:
                    key = f"integrity:{result.name}:{target_date}"
                    if result.passed:
                        resolve_keys.append(key)
                    else:
                        events.append(_integrity_alert_event(result, target_date, key))
                if report.failed_checks:
                    logger.warning(
                        "=== Data Integrity Check Failed for %s (%d/%d checks failed) ===",
                        target_date,
                        report.failed_checks,
                        report.total_checks,
                    )
                else:
                    logger.info(
                        "=== Data Integrity Check Passed for %s (%d checks) ===",
                        target_date,
                        report.total_checks,
                    )

            apply_incidents(events, resolve_keys=resolve_keys)
        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("Data Integrity Check job failed")


def _integrity_alert_event(result: object, target_date: str, key: str) -> AlertEvent:
    """Build an alert event for one failed integrity check."""
    name = str(getattr(result, "name", "unknown"))
    message = str(getattr(result, "message", "integrity check failed"))
    return AlertEvent(
        source=AlertSource.INTEGRITY,
        component=name,
        severity=AlertSeverity.ERROR,
        title=f"데이터 무결성 검사 실패: {name}",
        message=message,
        incident_key=key,
        remediation=(f"python3 -m src.cli.data_integrity_checker --date {target_date}",),
        metadata={"target_date": target_date},
    )


@_with_lock_skip_guard
def sync_rag_incremental_job() -> None:
    """RAG Vector DB Incremental Sync Job: sync latest season data into the Oracle RAG index."""
    with _scheduler_job_lock(MAINTENANCE_LOCK):
        if not _rag_vector_backend_configured():
            logger.warning("=== RAG Vector DB Incremental Sync skipped: vector backend is not configured ===")
            return
        logger.info("=== Starting RAG Vector DB Incremental Sync ===")
        try:
            from src.cli.rag.build_rag_index import main as build_rag_index_main

            previous_env = {key: os.environ.get(key) for key in _RAG_INCREMENTAL_WRITE_ENV}
            os.environ.update(_RAG_INCREMENTAL_WRITE_ENV)
            try:
                current_year = datetime.now(KST).year
                build_rag_index_main(
                    [
                        "--source",
                        "all",
                        "--season",
                        str(current_year),
                        "--skip-existing",
                    ]
                )
            finally:
                for key, value in previous_env.items():
                    if value is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = value
            logger.info("=== RAG Vector DB Incremental Sync Completed Successfully ===")
        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("RAG Vector DB Incremental Sync job failed")


@_with_lock_skip_guard
def sparse_terms_catchup_job() -> None:
    """Sparse postings catch-up for chunks published after the last build (daily 05:40 KST)."""
    with _scheduler_job_lock(MAINTENANCE_LOCK):
        logger.info("=== Starting Sparse Terms Catch-up ===")
        try:
            from src.cli.rag.build_oracle_sparse_index import main as sparse_main

            previous_env = {key: os.environ.get(key) for key in _RAG_INCREMENTAL_WRITE_ENV}
            os.environ.update(_RAG_INCREMENTAL_WRITE_ENV)
            try:
                exit_code = sparse_main(["--apply", "--catch-up", "--batch-size", "40", "--json"])
            finally:
                for key, value in previous_env.items():
                    if value is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = value
            if exit_code == 0:
                logger.info("=== Sparse Terms Catch-up Completed Successfully ===")
            else:
                logger.warning("=== Sparse Terms Catch-up Failed (exit %s) ===", exit_code)
        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("Sparse terms catch-up job failed")


@_with_lock_skip_guard
def backup_db_job() -> None:
    """Weekly SQLite Backup & Integrity Check Job (Sunday 02:00 KST)."""
    with _scheduler_job_lock(MAINTENANCE_LOCK):
        logger.info("=== Starting Weekly SQLite Backup & Integrity Check ===")
        try:
            from scripts.maintenance.backup_db import run_backup

            res = run_backup()
            if res:
                logger.info("=== Weekly SQLite Backup Completed: %s ===", res)
            else:
                logger.warning("=== Weekly SQLite Backup Failed ===")
        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("Weekly SQLite Backup job failed")


@_with_lock_skip_guard
def compute_park_factor_job() -> None:
    """Compute park factor for all stadiums (Sunday 05:30 KST)."""
    with _scheduler_job_lock(MAINTENANCE_LOCK):
        logger.info("=== Starting Park Factor Computation ===")
        try:
            from src.aggregators.park_factor_calculator import ParkFactorCalculator

            current_year = datetime.now(KST).year
            with SessionLocal() as session:
                calc = ParkFactorCalculator(session)
                results = calc.calculate(current_year)
                logger.info("Park Factor computed for %d stadiums", len(results))
        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("Park Factor computation failed")


@_with_lock_skip_guard
def recalc_milestones_and_rag_job() -> None:
    """Recalculate player milestones and index RAG knowledge chunks."""
    with _scheduler_job_lock(MAINTENANCE_LOCK):
        logger.info("=== Starting Milestone Recalculation and RAG Indexing ===")
        try:
            from src.cli.calc.recalc_milestones import main as recalc_main
            from src.cli.rag.index_rag_knowledge import main as index_main

            recalc_main([])
            index_main([])
            logger.info("=== Milestone Recalculation and RAG Indexing Completed ===")
        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("Milestone recalculation and RAG indexing failed")


@_with_lock_skip_guard
def crawl_fan_culture_job() -> None:
    """Fan culture data job: crawl cheer songs, chants, and rivalries from Namuwiki."""
    with _scheduler_job_lock(MAINTENANCE_LOCK):
        logger.info("[FanCulture] Starting fan culture data crawl")
        try:
            from src.crawlers.fan_culture_crawler import FanCultureCrawler

            asyncio.run(FanCultureCrawler().run(save=True))
            logger.info("[FanCulture] Fan culture data crawl completed")
        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("Fan culture job failed")


@_with_lock_skip_guard
def cleanup_stale_data_job() -> None:
    """Clean up stale temp files, expired manifests, empty logs, and old backups."""
    with _scheduler_job_lock(MAINTENANCE_LOCK):
        logger.info("=== Starting Stale Data Cleanup Job ===")
        try:
            from scripts.maintenance.cleanup_data import archive_data

            results = archive_data()
            total_cleaned = sum(len(v) for v in results.values())
            logger.info("=== Stale Data Cleanup Completed (%d files cleaned) ===", total_cleaned)
        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("Stale data cleanup job failed")


@_with_lock_skip_guard
def notification_retention_job() -> None:
    """Weekly prune of delivery audit rows and recovered incidents (Sunday 03:00 KST)."""
    with _scheduler_job_lock(MAINTENANCE_LOCK):
        logger.info("=== Starting Notification Retention Job ===")
        try:
            from src.notifications.retention import (
                prune_delivery_history,
                prune_incident_history,
            )

            now = datetime.now(UTC).replace(tzinfo=None)
            delivery_days = _env_int("NOTIFICATION_DELIVERY_RETENTION_DAYS", 90)
            incident_days = _env_int("NOTIFICATION_INCIDENT_RETENTION_DAYS", 30)
            with SessionLocal() as session:
                deliveries = prune_delivery_history(session, before=now - timedelta(days=delivery_days))
                incidents = prune_incident_history(session, before=now - timedelta(days=incident_days))
                session.commit()
            logger.info(
                "=== Notification Retention Completed (deliveries=%d > %dd, incidents=%d > %dd) ===",
                deliveries,
                delivery_days,
                incidents,
                incident_days,
            )
        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("Notification retention job failed")


def trim_scheduler_logs_job() -> None:
    """Trim scheduler log files to prevent unbounded growth (weekly)."""
    logger.info("=== Starting Scheduler Log Trim ===")
    try:
        from pathlib import Path

        from scripts.maintenance.trim_scheduler_log import trim_log

        log_path = Path("logs/scheduler.launchd.err.log")
        if log_path.exists():
            result = trim_log(log_path, keep_bytes=16 * 1024 * 1024)
            logger.info("=== Scheduler Log Trim Completed: %s ===", result)
        else:
            logger.info("=== No scheduler log file to trim ===")
    except SCHEDULER_JOB_EXCEPTIONS:
        logger.exception("Scheduler log trim job failed")


@_with_lock_skip_guard
def rag_identity_drift_job() -> None:
    """Daily RAG identity drift detection: census legacy vs natural keys, alert on unsafe drift."""
    with _scheduler_job_lock(MAINTENANCE_LOCK):
        logger.info("=== Starting RAG Identity Drift Detection ===")
        try:
            import json
            import os
            import subprocess
            import sys

            env = os.environ.copy()
            env.update(
                {
                    "RAG_INDEX_ALLOW_WRITE": "1",
                    "RAG_INDEX_ALLOW_PRODUCTION_WRITE": "1",
                }
            )

            # Run census with fail-on-unsafe
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "src.cli.kbo",
                    "rag",
                    "census",
                    "--dry-run",
                    "--json",
                    "--fail-on-unsafe",
                ],
                capture_output=True,
                text=True,
                env=env,
                timeout=1800,
                check=False,
            )

            if result.returncode != 0:
                census_data = json.loads(result.stdout) if result.stdout else {}
                unsafe_count = census_data.get("unsafe_entry_count", "unknown")
                logger.warning("RAG identity drift detected: %s unsafe entries (legacy rekey needed)", unsafe_count)
                from src.notifications.bridge import apply_incidents

                apply_incidents(
                    [
                        AlertEvent(
                            source=AlertSource.RAG,
                            component="identity",
                            severity=AlertSeverity.WARNING,
                            title="RAG identity drift 감지",
                            message=f"{unsafe_count}건의 unsafe identity entry (legacy rekey 필요)",
                            incident_key="rag:identity",
                            remediation=("python3 -m src.cli.kbo rag census --fail-on-unsafe",),
                            metadata={"unsafe_entry_count": unsafe_count},
                        ),
                    ],
                )
            else:
                logger.info("RAG identity drift check passed: no unsafe entries")
                from src.notifications.bridge import apply_incidents

                apply_incidents([], resolve_keys=["rag:identity"])

        except Exception:
            logger.exception("RAG identity drift detection failed")


@_with_lock_skip_guard
def schema_drift_check_job() -> None:
    """Daily schema drift detection: alert when ORM metadata and the live DB diverge."""
    with _scheduler_job_lock(MAINTENANCE_LOCK):
        logger.info("=== Starting Schema Drift Check ===")
        try:
            from src.db.drift_detector import SchemaDriftDetector
            from src.db.engine import Engine
            from src.notifications.bridge import apply_incidents

            report = SchemaDriftDetector(Engine).detect_drift()
            if report.drift_count == 0:
                logger.info("=== Schema Drift Check Passed (0 drifts, %d tables) ===", report.total_tables_checked)
                apply_incidents([], resolve_keys=["drift:schema"])
                return

            severity = _drift_alert_severity(report.drifts)
            counts: dict[str, int] = {}
            for drift in report.drifts:
                key = drift.drift_type.value
                counts[key] = counts.get(key, 0) + 1
            summary = ", ".join(f"{name}={count}" for name, count in sorted(counts.items()))
            logger.warning(
                "=== Schema Drift Detected: %d drift(s) across %d tables (%s) ===",
                report.drift_count,
                report.total_tables_checked,
                summary,
            )
            apply_incidents(
                [
                    AlertEvent(
                        source=AlertSource.DRIFT,
                        component="schema",
                        severity=severity,
                        title=f"스키마 드리프트 {report.drift_count}건 감지",
                        message=summary or "schema drift detected",
                        incident_key="drift:schema",
                        remediation=tuple(report.generated_ddl[:5]),
                        metadata={"drift_count": report.drift_count, "dialect": report.dialect},
                    ),
                ],
            )
        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("Schema drift check failed")


def _drift_alert_severity(drifts: list[object]) -> AlertSeverity:
    """Map the worst ``DriftSeverity`` onto an alert severity."""
    from src.db.drift_dto import DriftSeverity

    severities = {getattr(drift, "severity", DriftSeverity.LOW) for drift in drifts}
    if DriftSeverity.HIGH in severities:
        return AlertSeverity.ERROR
    if DriftSeverity.MEDIUM in severities:
        return AlertSeverity.WARNING
    return AlertSeverity.INFO


@_with_lock_skip_guard
def relay_state_cleanup_job() -> None:
    """Weekly relay source state audit job.

    Runs Sunday 02:15 KST to audit relay source states.
    All remediation actions run in dry-run mode (observation only, no writes).
    Uses --sample-size 10000 for performance on large databases.
    """
    from scripts.maintenance.fix_relay_state import (
        audit_relay_source_states,
        fix_source_mismatch,
        fix_unclassified_events,
        fix_unknown_sources,
        print_summary,
        remove_redundant_sources,
    )

    with _scheduler_job_lock(MAINTENANCE_LOCK):
        logger.info("=== Starting Relay State Cleanup ===")
        try:
            summary = audit_relay_source_states(sample_size=10000)
            print_summary(summary)

            logger.info("Audited %d games, found %d issues", summary.total_games, len(summary.issues))

            if summary.unknown_source_games > 0:
                result = fix_unknown_sources(dry_run=True, sample_size=10000)
                logger.info("fix_unknown_sources: %s", result)

            if summary.source_mismatch_games > 0:
                result = fix_source_mismatch(dry_run=True, sample_size=10000)
                logger.info("fix_source_mismatch: %s", result)

            if summary.redundant_source_games > 0:
                result = remove_redundant_sources(dry_run=True, sample_size=10000)
                logger.info("remove_redundant_sources: %s", result)

            if summary.unclassified_event_games > 0:
                result = fix_unclassified_events(dry_run=True)
                logger.info("fix_unclassified_events: %s", result)

            alert_success("relay_state_cleanup", "Relay state cleanup completed")
            logger.info("=== Relay State Cleanup Completed ===")

        except Exception:
            logger.exception("Relay state cleanup failed")
            alert_warning("relay_state_cleanup", "Relay state cleanup failed")


@_with_lock_skip_guard
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=120, max=600),
    retry_error_callback=alert_failure,
)
def crawl_dead_letter_recovery_job() -> None:
    """Recover dead letters stranded in ``retrying`` after a crash."""
    with _scheduler_job_lock(MAINTENANCE_LOCK):
        logger.info("=== Starting Dead Letter Recovery ===")
        try:
            from src.services.crawl_dead_letter_recovery import recover_stuck_retrying

            results = recover_stuck_retrying()
            if not results:
                logger.info("=== Dead Letter Recovery: nothing stuck ===")
                return

            counts: dict[str, int] = {}
            for result in results:
                counts[result.action] = counts.get(result.action, 0) + 1
            summary = ", ".join(f"{action}={count}" for action, count in sorted(counts.items()))
            logger.info("=== Dead Letter Recovery processed %d (%s) ===", len(results), summary)

            if counts.get("exhausted") or counts.get("finalized_interrupted") or counts.get("failed"):
                alert_warning("crawl_dead_letter_recovery", f"DLQ recovery: {summary}")

        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("Dead letter recovery failed")
            raise


@_with_lock_skip_guard
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=120, max=600),
    retry_error_callback=alert_failure,
)
def crawl_dead_letter_retry_job() -> None:
    """Retry due ``pending`` dead letters through their replay handlers."""
    with _scheduler_job_lock(MAINTENANCE_LOCK):
        logger.info("=== Starting Dead Letter Retry ===")
        try:
            from src.services.crawl_dead_letter_worker import retry_due_dead_letters

            summary = retry_due_dead_letters()
            if summary.attempted == 0:
                logger.info("=== Dead Letter Retry: nothing due ===")
                return

            logger.info("=== Dead Letter Retry processed: %s ===", summary.to_dict())
            if summary.exhausted or summary.errored:
                alert_warning("crawl_dead_letter_retry", f"DLQ retry: {summary.to_dict()}")

        except SCHEDULER_JOB_EXCEPTIONS:
            logger.exception("Dead letter retry failed")
            raise
