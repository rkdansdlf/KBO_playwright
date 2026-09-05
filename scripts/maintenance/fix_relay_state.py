"""유지보수 스크립트: 릴레이 소스 상태 정리 및 이상 감지.

릴레이 소스 어댑터의 상태를 감사하고, 잘못된 상태의 소스를 정리합니다.
- ALLOWED_SOURCE_TYPES에 없는 소스 이름 감지
- 소스 이름 불일치 감지 (provider_log_id vs source_name)
- 중복/redundant 소스 감지
- 미분류(unknown/unclassified) 이벤트 카운트 기준 정리

사용법:
    python3 scripts/maintenance/fix_relay_state.py --dry-run
    python3 scripts/maintenance/fix_relay_state.py --apply
    python3 scripts/maintenance/fix_relay_state.py --fix-unknown --dry-run
    python3 scripts/maintenance/fix_relay_state.py --fix-mismatch --dry-run
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from collections import Counter
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from sqlalchemy import func, select

from src.db.engine import SessionLocal
from src.models.game import GameEvent, GamePlayByPlay, GameValidationMetrics
from src.sources.relay.base import ALLOWED_SOURCE_TYPES

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

ALLOWED_SOURCE_TYPES_LOWER = {s.lower() for s in ALLOWED_SOURCE_TYPES}

KNOWN_REDUNDANT_PREFIXES = frozenset(
    {
        "jumper",
        "jump",
        "jmp",
        "redirect",
        "r2",
        "r3",
    }
)

UNCLASSIFIED_EVENT_TYPES = frozenset({"unknown", "unclassified", "other"})


@dataclass
class RelayStateIssue:
    issue_type: str
    source_name: str
    count: int
    games: list[str] = field(default_factory=list)
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class RelayStateSummary:
    total_games: int = 0
    total_pbp_rows: int = 0
    total_events: int = 0
    allowed_source_games: int = 0
    unknown_source_games: int = 0
    unclassified_event_games: int = 0
    source_mismatch_games: int = 0
    redundant_source_games: int = 0
    issues: list[RelayStateIssue] = field(default_factory=list)
    source_breakdown: dict[str, int] = field(default_factory=dict)
    game_level_issues: dict[str, list[str]] = field(default_factory=dict)


def _collect_game_ids_with_pbp(session: Session) -> list[str]:
    rows = (
        session.query(GamePlayByPlay.game_id)
        .group_by(GamePlayByPlay.game_id)
        .with_entities(GamePlayByPlay.game_id)
        .all()
    )
    return [r[0] for r in rows]


def _analyze_game_sources(session: Session, game_id: str) -> tuple[set[str], bool, bool, bool]:
    rows = session.execute(
        select(
            GamePlayByPlay.source_name,
            GamePlayByPlay.event_type,
            GamePlayByPlay.provider_log_id,
        ).where(GamePlayByPlay.game_id == game_id)
    ).all()

    source_names: set[str] = set()
    has_unclassified = False
    has_source_mismatch = False
    has_redundant_prefix = False

    provider_sources: dict[str, str] = {}
    for row in rows:
        src = (row.source_name or "none").lower()
        source_names.add(src)

        if row.event_type and row.event_type.lower() in UNCLASSIFIED_EVENT_TYPES:
            has_unclassified = True

        if row.provider_log_id:
            prefix = row.provider_log_id.split("_", 1)[0].lower()
            if prefix:
                provider_sources[prefix] = src

    unique_provider_sources = set(provider_sources.values())
    if len(unique_provider_sources) > 1:
        has_source_mismatch = True

    if any(p in s for p in KNOWN_REDUNDANT_PREFIXES for s in source_names):
        has_redundant_prefix = True

    return source_names, has_unclassified, has_source_mismatch, has_redundant_prefix


def audit_relay_source_states() -> RelayStateSummary:
    summary = RelayStateSummary()

    with SessionLocal() as session:
        summary.total_pbp_rows = session.query(func.count(GamePlayByPlay.id)).scalar() or 0
        summary.total_events = session.query(func.count(GameEvent.id)).scalar() or 0

        game_ids = _collect_game_ids_with_pbp(session)
        summary.total_games = len(game_ids)

        source_counts: Counter[str] = Counter()
        unknown_source_game_ids: set[str] = set()
        unclassified_game_ids: set[str] = set()
        mismatch_game_ids: set[str] = set()
        redundant_game_ids: set[str] = set()
        game_issues: dict[str, list[str]] = {}

        for game_id in game_ids:
            sources, has_unclsf, has_mismatch, has_redundant = _analyze_game_sources(session, game_id)

            for name in sources:
                source_counts[name] += 1

            unknown_names = {n for n in sources if n not in ALLOWED_SOURCE_TYPES_LOWER and n != "none"}
            if unknown_names:
                unknown_source_game_ids.add(game_id)
                summary.issues.append(
                    RelayStateIssue(
                        issue_type="unknown_source",
                        source_name=", ".join(sorted(unknown_names)),
                        count=len(unknown_names),
                        games=[game_id],
                    )
                )
                game_issues[game_id] = game_issues.get(game_id, [])
                game_issues[game_id].append(f"unknown_source:{','.join(sorted(unknown_names))}")

            if has_unclsf:
                unclassified_game_ids.add(game_id)

            if has_mismatch:
                mismatch_game_ids.add(game_id)
                game_issues[game_id] = game_issues.get(game_id, [])
                game_issues[game_id].append("source_mismatch")

            if has_redundant:
                redundant_game_ids.add(game_id)
                game_issues[game_id] = game_issues.get(game_id, [])
                game_issues[game_id].append("redundant")

        summary.source_breakdown = dict(source_counts)
        summary.unknown_source_games = len(unknown_source_game_ids)
        summary.unclassified_event_games = len(unclassified_game_ids)
        summary.source_mismatch_games = len(mismatch_game_ids)
        summary.redundant_source_games = len(redundant_game_ids)
        summary.game_level_issues = game_issues
        summary.allowed_source_games = (
            summary.total_games
            - summary.unknown_source_games
            - summary.redundant_source_games
            - summary.source_mismatch_games
        )

    return summary


def _get_games_by_criterion(summary: RelayStateSummary, criterion: str) -> list[str]:
    if criterion == "unknown_source":
        return [gid for gid, issues in summary.game_level_issues.items() if any("unknown_source" in i for i in issues)]
    if criterion == "unclassified":
        return [
            gid
            for gid, issues in summary.game_level_issues.items()
            if not any("unknown_source" in i or "source_mismatch" in i for i in issues)
        ]
    if criterion == "source_mismatch":
        return [gid for gid, issues in summary.game_level_issues.items() if any("source_mismatch" in i for i in issues)]
    if criterion == "redundant":
        return [gid for gid, issues in summary.game_level_issues.items() if any("redundant" in i for i in issues)]
    if criterion == "all":
        return list(summary.game_level_issues.keys())
    return []


def print_summary(summary: RelayStateSummary) -> None:
    logger.info("=" * 60)
    logger.info("릴레이 소스 상태 감사 요약")
    logger.info("=" * 60)
    logger.info(f"총 게임 수:           {summary.total_games}")
    logger.info(f"PBP 행 수:            {summary.total_pbp_rows}")
    logger.info(f"이벤트 행 수:         {summary.total_events}")
    logger.info("-" * 60)
    logger.info(f"허용 소스 게임:       {summary.allowed_source_games}")
    logger.info(f"알 수 없는 소스 게임:   {summary.unknown_source_games}  (정리 필요)")
    logger.info(f"미분류 이벤트 게임:    {summary.unclassified_event_games}  (참고용)")
    logger.info(f"소스 불일치 게임:      {summary.source_mismatch_games}  (정리 필요)")
    logger.info(f"중복/Redundant 게임:   {summary.redundant_source_games}  (정리 필요)")
    logger.info("-" * 60)
    logger.info("소스별 분포 (상위 15개):")
    for source, count in sorted(summary.source_breakdown.items(), key=lambda x: -x[1])[:15]:
        marker = " !" if source not in ALLOWED_SOURCE_TYPES_LOWER and source != "none" else ""
        logger.info(f"  {source:20s}: {count:6d}{marker}")
    logger.info("-" * 60)
    if summary.issues:
        logger.info("상세 이슈 (상위 10개):")
        for issue in sorted(summary.issues, key=lambda x: -x.count)[:10]:
            logger.info(f"  [{issue.issue_type}] {issue.source_name}: {issue.count}개 게임")


def fix_unknown_sources(dry_run: bool = True) -> dict[str, Any]:
    summary = audit_relay_source_states()
    fixable = _get_games_by_criterion(summary, "unknown_source")

    if not fixable:
        logger.info("정리할 알 수 없는 소스 게임이 없습니다.")
        return {"action": "none", "games": [], "dry_run": dry_run}

    logger.info(f"{'[DRY-RUN] ' if dry_run else ''}{len(fixable)}개 게임의 알 수 없는 소스를 정리합니다.")
    logger.info(f"  미리보기: {fixable[:5]}...")

    results: dict[str, Any] = {
        "action": "fix_unknown_sources",
        "games": fixable,
        "dry_run": dry_run,
        "affected_rows": 0,
    }

    if dry_run:
        return results

    with SessionLocal() as session:
        unknown_names = [n for n in summary.source_breakdown if n not in ALLOWED_SOURCE_TYPES_LOWER and n != "none"]
        if unknown_names:
            affected = session.execute(
                GamePlayByPlay.__table__.update()
                .where(GamePlayByPlay.source_name.in_(unknown_names))
                .values(source_name="none")
            )
            results["affected_rows"] = affected.rowcount
            session.commit()
            logger.info(f"  {affected.rowcount}개 행의 source_name을 'none'으로 변경했습니다.")

    return results


def fix_source_mismatch(dry_run: bool = True) -> dict[str, Any]:
    summary = audit_relay_source_states()
    mismatch_games = _get_games_by_criterion(summary, "source_mismatch")

    if not mismatch_games:
        logger.info("소스 불일치 게임이 없습니다.")
        return {"action": "none", "games": [], "dry_run": dry_run}

    logger.info(f"{'[DRY-RUN] ' if dry_run else ''}{len(mismatch_games)}개 게임의 소스 불일치를 감지했습니다.")
    logger.info("  참고: provider_log_id prefix와 source_name 간 불일치 - 수동 검토 필요")
    return {
        "action": "fix_source_mismatch",
        "games": mismatch_games,
        "dry_run": dry_run,
        "note": "수동 검토 필요 - 프로바이더 로그 ID와 소스 이름 간 불일치",
    }


def remove_redundant_sources(dry_run: bool = True) -> dict[str, Any]:
    summary = audit_relay_source_states()
    redundant_games = _get_games_by_criterion(summary, "redundant")

    if not redundant_games:
        logger.info("중복/Redundant 소스 게임이 없습니다.")
        return {"action": "none", "games": [], "dry_run": dry_run}

    logger.info(f"{'[DRY-RUN] ' if dry_run else ''}{len(redundant_games)}개 게임에서 중복 소스 접두사를 감지했습니다.")
    logger.info(f"  감지된 접두사: {KNOWN_REDUNDANT_PREFIXES}")
    return {
        "action": "remove_redundant",
        "games": redundant_games,
        "dry_run": dry_run,
        "note": "중복 소스 어댑터 감지 - 소스 선택 로직 검토 필요",
    }


def fix_unclassified_events(
    dry_run: bool = True,
    game_ids: list[str] | None = None,
) -> dict[str, Any]:
    with SessionLocal() as session:
        query = session.query(GamePlayByPlay).filter(GamePlayByPlay.event_type.in_(UNCLASSIFIED_EVENT_TYPES))
        if game_ids:
            query = query.filter(GamePlayByPlay.game_id.in_(game_ids))
        rows = query.all()
        affected_count = len(rows)

        if affected_count == 0:
            logger.info("미분류 이벤트가 없습니다.")
            return {"action": "none", "affected_rows": 0, "dry_run": dry_run}

        logger.info(f"{'[DRY-RUN] ' if dry_run else ''}{affected_count}개 미분류 이벤트를 감지했습니다.")
        game_ids_affected = sorted({r.game_id for r in rows})
        logger.info(f"  미리보기: {game_ids_affected[:5]}...")

        if dry_run:
            return {
                "action": "fix_unclassified",
                "affected_rows": affected_count,
                "games": game_ids_affected,
                "dry_run": dry_run,
            }

        for row in rows:
            row.event_type = "noise"
        session.commit()
        logger.info(f"  {affected_count}개 행의 event_type을 'noise'로 변경했습니다.")

        return {
            "action": "fix_unclassified",
            "affected_rows": affected_count,
            "games": game_ids_affected,
            "dry_run": dry_run,
        }


def update_validation_metrics(dry_run: bool = True) -> dict[str, Any]:
    from src.utils.relay_validation import VALIDATION_SOURCE_UNAVAILABLE

    with SessionLocal() as session:
        rows = (
            session.query(GameValidationMetrics.game_id)
            .filter(GameValidationMetrics.validation_status == VALIDATION_SOURCE_UNAVAILABLE)
            .all()
        )
    game_ids = [r[0] for r in rows]

    if not game_ids:
        logger.info("갱신할 검증 지표 게임이 없습니다.")
        return {"action": "none", "games": [], "dry_run": dry_run}

    logger.info(f"{'[DRY-RUN] ' if dry_run else ''}{len(game_ids)}개 게임의 검증 지표를 감사합니다.")

    summary = audit_relay_source_states()
    fixable = [gid for gid in game_ids if gid in summary.game_level_issues]

    if not fixable:
        logger.info("모든 검증 지표 게임이 이미 정리되었습니다.")
        return {"action": "none", "games": game_ids, "already_clean": True, "dry_run": dry_run}

    logger.info(f"  {len(fixable)}개 게임에 문제가 감지되었습니다.")
    return {
        "action": "update_validation_metrics",
        "games": game_ids,
        "fixable": fixable,
        "dry_run": dry_run,
        "note": "검증 상태 SOURCE_UNAVAILABLE 유지, 상세 감사 필요",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="릴레이 소스 상태 정리 및 감사")
    parser.add_argument("--dry-run", action="store_true", default=True, help="실제 변경 없이 미리보기 (기본값)")
    parser.add_argument("--apply", dest="dry_run", action="store_false", help="실제 변경 적용")
    parser.add_argument("--fix-unknown", action="store_true", help="알 수 없는 소스 정리")
    parser.add_argument("--fix-mismatch", action="store_true", help="소스 불일치 해결 (참고용)")
    parser.add_argument("--fix-redundant", action="store_true", help="중복 소스 제거 (참고용)")
    parser.add_argument("--fix-unclassified", action="store_true", help="미분류 이벤트 처리")
    parser.add_argument("--audit-only", action="store_true", help="감사만 수행 (변경 없음)")
    parser.add_argument("--game-ids", type=str, help="쉼표로 구분된 게임 ID 목록")
    args = parser.parse_args()

    logger.info("릴레이 소스 상태 감사를 시작합니다...")

    summary = audit_relay_source_states(sample_size=args.sample_size)
    print_summary(summary)

    if args.audit_only:
        logger.info("\n감사만 수행합니다. --apply 또는 --fix-* 옵션을 사용하세요.")
        return

    results: list[dict[str, Any]] = []

    if args.fix_unknown:
        results.append(fix_unknown_sources(dry_run=args.dry_run, sample_size=args.sample_size))

    if args.fix_mismatch:
        results.append(fix_source_mismatch(dry_run=args.dry_run, sample_size=args.sample_size))

    if args.fix_redundant:
        results.append(remove_redundant_sources(dry_run=args.dry_run, sample_size=args.sample_size))

    if args.fix_unclassified:
        game_ids = args.game_ids.split(",") if args.game_ids else None
        results.append(fix_unclassified_events(dry_run=args.dry_run, game_ids=game_ids))

    if not any([args.fix_unknown, args.fix_mismatch, args.fix_redundant, args.fix_unclassified]):
        logger.info("\n--fix-* 옵션을 지정하세요. 예: --fix-unknown --apply")
        logger.info("또는 --audit-only로 감사만 수행합니다.")

    logger.info("\n요약:")
    for result in results:
        action = result.get("action", "none")
        dry = "[DRY-RUN] " if result.get("dry_run") else ""
        games = result.get("games", [])
        rows = result.get("affected_rows", 0)
        if action != "none":
            logger.info(f"  {dry}{action}: {len(games)}개 게임, {rows}개 행 영향")

    if args.dry_run:
        logger.info("\n[DRY-RUN] 완료. 실제 적용하려면 --apply 옵션을 사용하세요.")


if __name__ == "__main__":
    main()
