"""Automated repair and backfill service for missing or inconsistent KBO games.

Discovers discrepancy and failure candidates from:
1. data_integrity_checker (boxscore totals sum mismatches in DB)
2. crawl_dead_letters (DLQ queue items for games)
3. crawl_failures.log (logged permanent crawl failures)

Re-crawls failed games, verifies boxscore sums, UPSERTs into the DB, and
resolves DLQ records.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from sqlalchemy import select, text

from src.db.engine import SessionLocal
from src.models.crawl_dead_letter import CrawlDeadLetter, DlqStatus
from src.models.game import Game
from src.utils.crawler_failure_logger import DEFAULT_FAILURE_LOG_PATH
from src.utils.date_helpers import parse_date_str
from src.validators.boxscore_verifier import verify_boxscore_sums, verify_game_db_boxscore_integrity

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

KBO_DATE_LEN = 8
KBO_GAME_ID_LEN = 13
FAILURE_LOG_PARTS_MIN = 6


@dataclass
class RepairCandidate:
    """A game identified as needing repair."""

    game_id: str
    game_date: str
    source: str
    reasons: list[str] = field(default_factory=list)


@dataclass
class RepairOutcome:
    """Outcome of attempting to repair one game."""

    game_id: str
    game_date: str
    status: str  # "repaired", "discrepancy_remaining", "crawl_failed", "skipped_dry_run"
    errors: list[str] = field(default_factory=list)
    persisted: bool = False


@dataclass
class RepairBatchReport:
    """Overall summary of a repair batch execution."""

    total_candidates: int = 0
    repaired: int = 0
    failed: int = 0
    discrepancies: int = 0
    dry_run: bool = True
    outcomes: list[RepairOutcome] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert batch report to JSON-serializable dictionary."""
        return {
            "total_candidates": self.total_candidates,
            "repaired": self.repaired,
            "failed": self.failed,
            "discrepancies": self.discrepancies,
            "dry_run": self.dry_run,
            "outcomes": [
                {
                    "game_id": o.game_id,
                    "game_date": o.game_date,
                    "status": o.status,
                    "errors": o.errors,
                    "persisted": o.persisted,
                }
                for o in self.outcomes
            ],
        }


def _date_from_game_id(game_id: str) -> str:
    """Extract YYYY-MM-DD string from standard 13-character KBO game_id."""
    clean = str(game_id).strip()
    if len(clean) >= KBO_DATE_LEN and clean[:KBO_DATE_LEN].isdigit():
        return f"{clean[:4]}-{clean[4:6]}-{clean[6:8]}"
    return datetime.now(UTC).strftime("%Y-%m-%d")


def discover_candidates_from_integrity(
    session: Session,
    target_date: str | date | None = None,
    limit: int = 100,
) -> list[RepairCandidate]:
    """Find games with boxscore discrepancies in DB using integrity verification."""
    candidates: list[RepairCandidate] = []

    if target_date is not None:
        parsed_date = parse_date_str(str(target_date).replace("-", "")) if isinstance(target_date, str) else target_date
        games = session.query(Game).filter(Game.game_date == parsed_date).all()
        for game in games:
            is_valid, errors = verify_game_db_boxscore_integrity(session, game.game_id)
            if not is_valid:
                game_date_str = (
                    game.game_date.strftime("%Y-%m-%d") if game.game_date else _date_from_game_id(game.game_id)
                )
                candidates.append(
                    RepairCandidate(
                        game_id=game.game_id,
                        game_date=game_date_str,
                        source="integrity_checker",
                        reasons=errors,
                    )
                )
        return candidates

    discrepancy_sql = text("""
        SELECT g.game_id
        FROM game g
        JOIN game_batting_stats b ON g.game_id = b.game_id
        WHERE g.game_status IN ('COMPLETED', 'FINAL', '종료')
        GROUP BY g.game_id, g.game_date, g.away_score, g.home_score
        HAVING (g.away_score IS NOT NULL AND g.away_score != SUM(CASE WHEN b.team_side = 'away' THEN b.runs ELSE 0 END))
            OR (g.home_score IS NOT NULL AND g.home_score != SUM(CASE WHEN b.team_side = 'home' THEN b.runs ELSE 0 END))
        ORDER BY g.game_date DESC
        LIMIT :limit
    """)
    try:
        candidate_ids = [row[0] for row in session.execute(discrepancy_sql, {"limit": limit}).all()]
    except Exception:
        logger.warning("Fast SQL discrepancy query failed; returning empty candidates list", exc_info=True)
        candidate_ids = []

    for gid in candidate_ids:
        is_valid, errors = verify_game_db_boxscore_integrity(session, gid)
        if not is_valid:
            candidates.append(
                RepairCandidate(
                    game_id=gid,
                    game_date=_date_from_game_id(gid),
                    source="integrity_checker",
                    reasons=errors,
                )
            )

    return candidates


def discover_candidates_from_dlq(
    session: Session,
    limit: int = 100,
) -> list[RepairCandidate]:
    """Find failed game crawl entries from CrawlDeadLetter queue."""
    candidates: list[RepairCandidate] = []
    stmt = (
        select(CrawlDeadLetter)
        .where(
            CrawlDeadLetter.status.in_([DlqStatus.PENDING.value, DlqStatus.RETRYING.value]),
            (CrawlDeadLetter.target_type == "game") | (CrawlDeadLetter.game_id.is_not(None)),
        )
        .limit(limit)
    )
    letters = session.scalars(stmt).all()
    for letter in letters:
        gid = letter.game_id or letter.target_id
        if gid:
            candidates.append(
                RepairCandidate(
                    game_id=gid,
                    game_date=_date_from_game_id(gid),
                    source="dlq",
                    reasons=[letter.error_message or letter.error_code],
                )
            )
    return candidates


def discover_candidates_from_log(
    log_path: Path | str = DEFAULT_FAILURE_LOG_PATH,
) -> list[RepairCandidate]:
    """Find failed game crawl entries from local crawl_failures.log."""
    candidates: list[RepairCandidate] = []
    path = Path(log_path)
    if not path.is_file():
        return candidates

    seen: set[str] = set()
    try:
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                parts = line.strip().split("\t")
                if len(parts) >= FAILURE_LOG_PARTS_MIN:
                    target_type, identifier, reason = parts[1], parts[2], parts[5]
                    is_game = target_type == "game" or (len(identifier) == KBO_GAME_ID_LEN and identifier[:4].isdigit())
                    if is_game and identifier not in seen:
                        seen.add(identifier)
                        candidates.append(
                            RepairCandidate(
                                game_id=identifier,
                                game_date=_date_from_game_id(identifier),
                                source="failure_log",
                                reasons=[reason],
                            )
                        )
    except OSError:
        logger.warning("Could not read failure log at %s", log_path, exc_info=True)

    return candidates


def gather_repair_candidates(
    session: Session,
    *,
    source: str = "all",
    target_date: str | date | None = None,
    specific_game_id: str | None = None,
    log_path: Path | str = DEFAULT_FAILURE_LOG_PATH,
) -> list[RepairCandidate]:
    """Gather unique candidate games from requested sources."""
    if specific_game_id:
        return [
            RepairCandidate(
                game_id=specific_game_id,
                game_date=_date_from_game_id(specific_game_id),
                source="manual",
                reasons=["User requested specific repair"],
            )
        ]

    candidate_map: dict[str, RepairCandidate] = {}

    if source in ("all", "integrity"):
        for c in discover_candidates_from_integrity(session, target_date=target_date):
            candidate_map[c.game_id] = c

    if source in ("all", "dlq"):
        for c in discover_candidates_from_dlq(session):
            if c.game_id in candidate_map:
                candidate_map[c.game_id].reasons.extend(c.reasons)
            else:
                candidate_map[c.game_id] = c

    if source in ("all", "log"):
        for c in discover_candidates_from_log(log_path=log_path):
            if c.game_id in candidate_map:
                candidate_map[c.game_id].reasons.extend(c.reasons)
            else:
                candidate_map[c.game_id] = c

    return list(candidate_map.values())


async def _crawl_and_repair_single(
    candidate: RepairCandidate,
    *,
    dry_run: bool = True,
    session: Session | None = None,
) -> RepairOutcome:
    """Crawl a single game, verify boxscore, and save if not dry-run."""
    game_id = candidate.game_id
    game_date = candidate.game_date

    if dry_run:
        return RepairOutcome(
            game_id=game_id,
            game_date=game_date,
            status="pending_repair",
            errors=candidate.reasons,
            persisted=False,
        )

    from src.crawlers.game_detail_crawler import GameDetailCrawler
    from src.repositories.crawl_dead_letter_repository import CrawlDeadLetterRepository
    from src.repositories.game_save import save_game_detail

    crawler = GameDetailCrawler()

    logger.info("🔧 [REPAIR] Crawling game %s (%s)...", game_id, game_date)
    try:
        payload = await crawler.crawl_game(game_id, game_date, lightweight=False)
    except Exception as exc:
        logger.exception("❌ [REPAIR] Exception while re-crawling %s", game_id)
        return RepairOutcome(
            game_id=game_id,
            game_date=game_date,
            status="crawl_failed",
            errors=[str(exc)],
            persisted=False,
        )

    if not payload:
        return RepairOutcome(
            game_id=game_id,
            game_date=game_date,
            status="crawl_failed",
            errors=["Crawler returned empty payload"],
            persisted=False,
        )

    is_valid, discrepancies = verify_boxscore_sums(payload)

    # Persist via UPSERT
    owns_session = session is None
    active_session = session if session is not None else SessionLocal()
    try:
        save_success = save_game_detail(
            payload,
            session=active_session,
            source_stage="repair",
            source_crawler="BoxscoreRepairService",
            source_reason="auto_repair_boxscore",
        )
        if save_success and is_valid:
            # Resolve any matching pending DLQ entries
            dlq_repo = CrawlDeadLetterRepository(active_session)
            stmt = select(CrawlDeadLetter).where(
                (CrawlDeadLetter.game_id == game_id) | (CrawlDeadLetter.target_id == game_id),
                CrawlDeadLetter.status.in_([DlqStatus.PENDING.value, DlqStatus.RETRYING.value]),
            )
            for letter in active_session.scalars(stmt).all():
                dlq_repo.mark_resolved(letter)
                logger.info("📬 [REPAIR] Resolved DLQ letter %s for game %s", letter.dlq_id, game_id)

        if owns_session:
            active_session.commit()
        else:
            active_session.flush()

        status = "repaired" if (save_success and is_valid) else "discrepancy_remaining"
        return RepairOutcome(
            game_id=game_id,
            game_date=game_date,
            status=status,
            errors=discrepancies,
            persisted=save_success,
        )
    except Exception as exc:
        if owns_session:
            active_session.rollback()
        logger.exception("❌ [REPAIR] Failed to persist repaired game %s", game_id)
        return RepairOutcome(
            game_id=game_id,
            game_date=game_date,
            status="save_failed",
            errors=[str(exc)],
            persisted=False,
        )
    finally:
        if owns_session:
            active_session.close()


async def repair_games_async(
    candidates: list[RepairCandidate],
    *,
    dry_run: bool = True,
    session: Session | None = None,
) -> RepairBatchReport:
    """Asynchronously execute repair workflow across candidates."""
    report = RepairBatchReport(
        total_candidates=len(candidates),
        dry_run=dry_run,
    )

    for candidate in candidates:
        outcome = await _crawl_and_repair_single(candidate, dry_run=dry_run, session=session)
        report.outcomes.append(outcome)
        if outcome.status == "repaired":
            report.repaired += 1
        elif outcome.status in {"crawl_failed", "save_failed"}:
            report.failed += 1
        elif outcome.status in {"discrepancy_remaining", "pending_repair"}:
            report.discrepancies += 1

    return report


def run_boxscore_repair(
    *,
    source: str = "all",
    target_date: str | date | None = None,
    specific_game_id: str | None = None,
    apply: bool = False,
    session: Session | None = None,
) -> RepairBatchReport:
    """Run synchronous boxscore repair pipeline."""
    owns_session = session is None
    active_session = session if session is not None else SessionLocal()

    try:
        candidates = gather_repair_candidates(
            active_session,
            source=source,
            target_date=target_date,
            specific_game_id=specific_game_id,
        )
        logger.info("🔍 [REPAIR] Discovered %s candidate game(s) for repair.", len(candidates))
        return asyncio.run(
            repair_games_async(
                candidates,
                dry_run=not apply,
                session=active_session if not owns_session else None,
            )
        )
    finally:
        if owns_session:
            active_session.close()
