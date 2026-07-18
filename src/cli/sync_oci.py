"""로컬 SQLite 데이터베이스의 데이터를 원격 OCI/Postgres 데이터베이스와 동기화하는 스크립트.

OCISync 클래스와 전용 동기화 메서드를 사용하여 테이블별로 특화된 UPSERT/COPY 로직을
수행합니다. `--truncate` 옵션을 사용하면 대상 테이블의 데이터를 삭제한 후 새로 삽입할 수 있습니다.

"""

from __future__ import annotations

import argparse
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from src.db.engine import SessionLocal, create_engine_for_url, get_oci_url
from src.models.game import Game
from src.models.matchup import BatterTeamSplit
from src.models.player import PlayerSeasonBatting
from src.models.rankings import StatRanking
from src.models.standings import TeamStandingsDaily
from src.sync.oci_sync import OCISync

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

OCI_CLI_EXCEPTIONS = (SQLAlchemyError, RuntimeError, ValueError, TypeError, OSError)


def run_parallel_sync(
    sync_fn: Callable[[OCISync, Any], None],
    target_url: str,
    years: list[int],
    workers: int,
    **kwargs: object,
) -> None:
    """연도별로 병렬 동기화 작업을 수행합니다.

    Args:
        sync_fn: Sync Fn.
        target_url: Target URL.
        years: Years.
        workers: Workers.
        kwargs: Keyword arguments to pass through.

    """
    logger.info("🚀 Starting parallel sync with %s workers for years: %s", workers, years)

    def sync_worker(year: int) -> None:
        """Sync worker.

        Args:
            year: Season year.
            year: Season year.

        """
        logger.info("🧵 Worker started for year %s", year)

        with SessionLocal() as session:
            try:
                syncer = OCISync(target_url, session)
                syncer.concurrency = 1
                sync_fn(syncer, year, **kwargs)
                logger.info("✅ Worker finished for year %s", year)
            except OCI_CLI_EXCEPTIONS:
                logger.exception("❌ Worker failed for year %s", year)

    with ThreadPoolExecutor(max_workers=workers) as executor:
        executor.map(sync_worker, years)


def _parse_game_ids(value: str | None) -> list[str]:
    if not value:
        return []
    return list(dict.fromkeys(token.strip() for token in value.split(",") if token.strip()))


def _add_basic_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--source-url",
        type=str,
        default=os.getenv("SOURCE_DATABASE_URL", "sqlite:///./data/kbo_dev.db"),
        help="원본 데이터베이스 URL (기본값: 로컬 SQLite)",
    )
    parser.add_argument(
        "--target-url",
        type=str,
        default=get_oci_url(),
        help="대상 데이터베이스 URL (OCI/Postgres)",
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="데이터 삽입 전 대상 테이블의 모든 데이터를 삭제합니다.",
    )
    parser.add_argument(
        "--year",
        type=int,
        help="특정 연도의 데이터를 동기화합니다. (e.g., 2018)",
    )
    parser.add_argument(
        "--days",
        type=int,
        help="최근 N일간의 경기 데이터만 동기화합니다.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="동기화할 최대 행 수를 지정합니다.",
    )
    parser.add_argument(
        "--unsynced-only",
        action="store_true",
        help="OCI에 없거나 로컬 업데이트가 더 최근인 미동기화/수정된 데이터만 선별하여 동기화합니다.",
    )
    parser.add_argument(
        "--compare",
        action="store_true",
        help="Compare OCI and local DB record counts and optionally apply sync.",
    )
    parser.add_argument(
        "--direction",
        choices=["oci-to-local", "local-to-oci", "bidirectional"],
        default="bidirectional",
        help="Direction for sync when using --compare.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply the synchronization after comparison (use with --compare).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="시그니처 비교를 건너뛰고 강제로 동기화를 진행합니다.",
    )


def _add_game_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--game-details",
        action="store_true",
        help="경기 상세 데이터(박스스코어, 라인업, PBP 등)를 동기화합니다.",
    )
    parser.add_argument(
        "--game-ids",
        type=str,
        help="특정 game_id만 동기화합니다. --game-details와 함께 사용합니다.",
    )
    parser.add_argument(
        "--games-only",
        action="store_true",
        help="game 테이블만 경량 동기화합니다. --year와 함께 사용 가능합니다.",
    )


def _add_entity_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--teams",
        action="store_true",
        help="프랜차이즈 및 팀 정보를 동기화합니다.",
    )
    parser.add_argument(
        "--player-basic",
        action="store_true",
        help="선수 기본 정보(Player Basic)를 동기화합니다.",
    )
    parser.add_argument(
        "--players",
        action="store_true",
        help="마스터 선수 레코드(Players 테이블)를 동기화합니다. (사진, 상세 프로필 포함)",
    )
    parser.add_argument(
        "--daily-roster",
        action="store_true",
        help="일별 1군 등록 현황(Daily Roster)을 동기화합니다.",
    )
    parser.add_argument(
        "--roster-date",
        type=str,
        help="--daily-roster 대상 날짜를 지정합니다. YYYYMMDD 또는 YYYY-MM-DD.",
    )
    parser.add_argument(
        "--roster-start-date",
        type=str,
        help="--daily-roster 시작 날짜를 지정합니다. YYYYMMDD 또는 YYYY-MM-DD.",
    )
    parser.add_argument(
        "--roster-end-date",
        type=str,
        help="--daily-roster 종료 날짜를 지정합니다. YYYYMMDD 또는 YYYY-MM-DD.",
    )
    parser.add_argument(
        "--player-movements",
        action="store_true",
        help="선수 이동 현황(Trade, FA 등)을 동기화합니다.",
    )
    parser.add_argument(
        "--fa-contracts",
        action="store_true",
        help="FA 계약 상세 정보(fa_contracts)를 동기화합니다.",
    )
    parser.add_argument(
        "--awards",
        action="store_true",
        help="수상 내역(Awards)을 동기화합니다.",
    )


def _add_stat_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--season-stats",
        action="store_true",
        help="선수 시즌 누적 스탯(타자, 투수)을 고속 동기화합니다.",
    )
    parser.add_argument(
        "--player-game-stats",
        action="store_true",
        help="선수-경기 스탯(타자, 투수)을 동기화합니다.",
    )
    parser.add_argument(
        "--standings",
        action="store_true",
        help="일별 팀 순위(Standings) 스냅샷 테이블을 고속 동기화합니다.",
    )
    parser.add_argument(
        "--matchups",
        action="store_true",
        help="계산된 상대 전적(Matchup Splits) 테이블을 동기화합니다.",
    )
    parser.add_argument(
        "--rankings",
        action="store_true",
        help="계산된 stat_rankings 테이블을 동기화합니다.",
    )


def _add_misc_table_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--crawl-runs",
        action="store_true",
        help="크롤링 실행 기록(Crawl Runs)을 동기화합니다.",
    )
    parser.add_argument(
        "--rag-chunks",
        action="store_true",
        help="RAG 텍스트 청크 데이터를 동기화합니다.",
    )
    parser.add_argument(
        "--ticket-schedules",
        action="store_true",
        help="경기 예매 오픈 일정 데이터를 동기화합니다.",
    )
    parser.add_argument(
        "--stadium-foods",
        action="store_true",
        help="구장별 먹거리 추천 데이터(stadium_foods)를 동기화합니다.",
    )
    parser.add_argument(
        "--kbo-season",
        action="store_true",
        help="KBO 시즌 기준 정보(kbo_seasons)를 동기화합니다.",
    )


def _add_phase1_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--broadcasts",
        action="store_true",
        help="Phase1: 경기 중계 정보(game_broadcasts)를 동기화합니다.",
    )
    parser.add_argument(
        "--mvps",
        action="store_true",
        help="Phase1: 경기 MVP(game_mvps)를 동기화합니다.",
    )
    parser.add_argument(
        "--stadiums",
        action="store_true",
        help="Phase1: 구장 정보(stadium_info, stadium_regulations)를 동기화합니다.",
    )
    parser.add_argument(
        "--team-events",
        action="store_true",
        help="구단 이벤트/뉴스 정보(team_events)를 동기화합니다.",
    )
    parser.add_argument(
        "--injuries",
        action="store_true",
        help="Phase1: 부상자 정보(injury_entries)를 동기화합니다.",
    )
    parser.add_argument(
        "--foreign-players",
        action="store_true",
        help="Phase1: 외국인 선수 변동(foreign_player_changes)을 동기화합니다.",
    )
    parser.add_argument(
        "--managers",
        action="store_true",
        help="Phase1: 감독 변동(manager_changes)을 동기화합니다.",
    )
    parser.add_argument(
        "--fan-culture",
        action="store_true",
        help="Phase1: 팬 문화 데이터(team_rivalries, cheer_songs, cheer_chants)를 동기화합니다.",
    )
    parser.add_argument(
        "--phase1-all",
        action="store_true",
        help="모든 Phase 1 테이블을 한 번에 동기화합니다.",
    )


def _add_stadium_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--transit-times",
        action="store_true",
        help="잠실구장 실측 이동 시간(stadium_transit_times)을 동기화합니다.",
    )
    parser.add_argument(
        "--congestion",
        action="store_true",
        help="잠실구장 실시간 혼잡도(stadium_congestion)를 동기화합니다.",
    )
    parser.add_argument(
        "--operation-notices",
        action="store_true",
        help="구단 운영 공지(stadium_operation_notices)를 동기화합니다.",
    )
    parser.add_argument(
        "--stadium-realtime-all",
        action="store_true",
        help="이동 시간 + 혼잡도 + 운영 공지 3종을 한 번에 동기화합니다.",
    )
    parser.add_argument(
        "--realtime-game-date",
        type=str,
        default=None,
        metavar="YYYYMMDD",
        help="--transit-times/--congestion/--operation-notices 동기화 시 게임일 필터 (선택)",
    )


def _add_perf_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="일반 UPSERT 작업 시 배치 크기 (기본값: 500)",
    )
    parser.add_argument(
        "--copy-batch-size",
        type=int,
        default=5000,
        help="고속 COPY 작업 시 배치 크기 (기본값: 5000)",
    )
    parser.add_argument(
        "--parallel",
        "-p",
        action="store_true",
        help="연도별 병렬 동기화를 활성화합니다.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="병렬 작업 시 워커(스레드) 수 (기본값: 4)",
    )
    parser.add_argument(
        "--reset-sequences",
        action="store_true",
        help=(
            "동기화 후 OCI 시퀀스 식별자를 재설정합니다. (기본값: 해제 — per-table reset이 이미 sync_games에서 처리됨)"
        ),
    )


def build_arg_parser() -> argparse.ArgumentParser:
    """CLI 인자 파서를 생성합니다."""
    parser = argparse.ArgumentParser(description="Sync local SQLite data to OCI/Postgres")
    _add_basic_args(parser)
    _add_game_args(parser)
    _add_entity_args(parser)
    _add_stat_args(parser)
    _add_misc_table_args(parser)
    _add_phase1_args(parser)
    _add_stadium_args(parser)
    _add_perf_args(parser)
    return parser


_ALLOWED_YEAR_COLUMNS = frozenset(
    {
        "season",
        "season_year",
        "strftime('%Y', game_date)",
        "strftime('%Y', standings_date)",
    },
)


def get_available_years(session: Session, model: type[object], column_name: str = "season") -> list[int]:
    """대상 테이블에서 사용 가능한 연도 목록을 가져옵니다.

    Args:
        session: Session.
        model: Model.
        column_name: Column Name.

    """
    if column_name not in _ALLOWED_YEAR_COLUMNS:
        msg = f"Disallowed column expression: {column_name}"
        raise ValueError(msg)
    query = session.query(text(f"DISTINCT {column_name}")).select_from(model)
    years = [int(row[0]) for row in query.all() if row[0]]
    return sorted(years, reverse=True)


@dataclass
class SyncRunConfig:
    """SyncRunConfig class."""

    parallel_support: bool = False
    header: str | None = None
    years_getter: Callable[[Session], list[int]] | None = None
    completion_msg: str | None = None


def _run_sync(
    args: argparse.Namespace,
    sync_fn: Callable[..., object],
    *,
    config: SyncRunConfig,
) -> None:
    if config.header:
        logger.info(config.header)

    target_years = [args.year] if args.year else []

    if config.parallel_support and args.parallel and config.years_getter:
        if not target_years:
            with SessionLocal() as session:
                target_years = config.years_getter(session)
        run_parallel_sync(
            sync_fn,  # type: ignore[arg-type]
            args.target_url,
            target_years,
            args.workers,
            days=args.days,
            unsynced_only=getattr(args, "unsynced_only", None),
            batch_size=args.batch_size,
            copy_batch_size=args.copy_batch_size,
            truncate=args.truncate,
            force=getattr(args, "force", False),
        )
    else:
        with SessionLocal() as session:
            syncer = OCISync(args.target_url, session)
            syncer.concurrency = args.workers
            try:
                years = [args.year] if args.year else [None]
                for year in years:
                    sync_fn(
                        syncer,
                        year,
                        days=args.days,
                        unsynced_only=getattr(args, "unsynced_only", None),
                        batch_size=args.batch_size,
                        copy_batch_size=args.copy_batch_size,
                        truncate=args.truncate,
                        requested_game_ids=_parse_game_ids(getattr(args, "game_ids", None)),
                        force=getattr(args, "force", False),
                    )
            finally:
                syncer.close()

    if config.completion_msg:
        logger.info(config.completion_msg)


def _validate_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    if args.game_ids and not args.game_details:
        parser.error("--game-ids can only be used with --game-details")
    if args.game_ids and args.parallel:
        parser.error("--game-ids cannot be combined with --parallel")
    if args.roster_date and (args.roster_start_date or args.roster_end_date):
        parser.error("--roster-date cannot be combined with --roster-start-date or --roster-end-date")
    if (args.roster_date or args.roster_start_date or args.roster_end_date) and not args.daily_roster:
        parser.error("--roster-date/--roster-start-date/--roster-end-date can only be used with --daily-roster")
    if not args.target_url:
        msg = "TARGET_DATABASE_URL must be provided via flag or environment variable"
        raise SystemExit(msg)


def _build_sync_dispatch() -> dict[str, tuple]:
    return {
        "game_details": (
            lambda s, y, **kw: (
                s.sync_game_details(
                    year=y,
                    days=kw.get("days"),
                    unsynced_only=kw.get("unsynced_only"),
                    batch_size=kw.get("copy_batch_size"),
                )
                if not kw.get("requested_game_ids")
                else logger.info(
                    "   Explicit game sync result: %s",
                    s.sync_game_details_for_ids(
                        kw["requested_game_ids"],
                        batch_size=kw.get("copy_batch_size"),
                    ),
                )
            ),
            "🚀 Syncing Game Details using specialized OCISync...",
            True,
            lambda sess: get_available_years(sess, Game, "strftime('%Y', game_date)"),
            "✅ Game Details Sync Finished",
        ),
        "games_only": (
            lambda s, y, **kw: logger.info(
                "   [%s] Synced %s rows",
                y,
                s.sync_games(
                    filters=[Game.game_id.like(f"{y}%")] if y else None,
                    batch_size=kw.get("copy_batch_size"),
                ),
            ),
            "🚀 Syncing game table only using specialized OCISync...",
            True,
            lambda sess: get_available_years(sess, Game, "strftime('%Y', game_date)"),
            "✅ Game Table Sync Finished",
        ),
        "season_stats": (
            _sync_season_stats,
            "🚀 Syncing Season Stats using specialized OCISync...",
            True,
            lambda sess: get_available_years(sess, PlayerSeasonBatting, "season"),
            "✅ Season Stats Sync Finished",
        ),
        "standings": (
            lambda s, y, **kw: logger.info(
                "   [%s] Synced %s rows",
                y,
                s.sync_standings(
                    days=kw.get("days"),
                    year=y,
                    batch_size=kw.get("copy_batch_size"),
                ),
            ),
            "🚀 Syncing Daily Standings using specialized OCISync...",
            True,
            lambda sess: get_available_years(sess, TeamStandingsDaily, "strftime('%Y', standings_date)"),
            "✅ Daily Standings Sync Finished",
        ),
        "matchups": (
            lambda s, y, **kw: s.sync_matchups(year=y, batch_size=kw.get("copy_batch_size")),
            "🚀 Syncing Matchup Splits using specialized OCISync...",
            True,
            lambda sess: get_available_years(sess, BatterTeamSplit, "season_year"),
            "✅ Matchup Splits Sync Finished",
        ),
        "rankings": (
            lambda s, y, **kw: logger.info(
                "   [%s] Synced %s rows",
                y,
                s.sync_stat_rankings(year=y, batch_size=kw.get("copy_batch_size")),
            ),
            "🚀 Syncing Stat Rankings using specialized OCISync...",
            True,
            lambda sess: get_available_years(sess, StatRanking, "season"),
            "✅ Stat Rankings Sync Finished",
        ),
        "player_game_stats": (
            _sync_player_game_stats,
            "🚀 Syncing Player Game Stats...",
            True,
            lambda sess: get_available_years(sess, Game, "strftime('%Y', game_date)"),
            "✅ Player Game Stats Sync Finished",
        ),
    }


def _build_simple_flags() -> dict[str, tuple[str, str]]:
    return {
        "kbo_season": ("sync_kbo_seasons", "🚀 Syncing KBO Seasons reference table using OCISync..."),
        "daily_roster": ("sync_daily_rosters", "🚀 Syncing Daily Rosters using specialized OCISync..."),
        "player_basic": ("sync_player_basic", "🚀 Syncing Player Basic using specialized OCISync..."),
        "players": ("sync_players", "🚀 Syncing Master Players using specialized OCISync..."),
        "player_movements": ("sync_player_movements", "🚀 Syncing Player Movements using specialized OCISync..."),
        "fa_contracts": ("sync_fa_contracts", "🚀 Syncing FA Contracts using specialized OCISync..."),
        "teams": ("sync_teams", "🚀 Syncing Franchises & Teams using specialized OCISync..."),
        "awards": ("sync_awards", "🚀 Syncing Awards using specialized OCISync..."),
        "crawl_runs": ("sync_crawl_runs", "🚀 Syncing Crawl Runs using specialized OCISync..."),
        "rag_chunks": ("sync_rag_chunks", "🚀 Syncing RAG Chunks using specialized OCISync..."),
        "ticket_schedules": ("sync_ticket_schedules", "🚀 Syncing Ticket Schedules using specialized OCISync..."),
        "stadium_foods": ("sync_stadium_foods", "🚀 Syncing Stadium Foods using specialized OCISync..."),
        "broadcasts": ("sync_game_broadcasts", "🚀 Syncing Game Broadcasts (Phase 1)..."),
        "mvps": ("sync_game_mvps", "🚀 Syncing Game MVPs (Phase 1)..."),
        "stadiums": ("sync_stadiums", "🚀 Syncing Stadium Info & Regulations (Phase 1)..."),
        "injuries": ("sync_injury_entries", "🚀 Syncing Injury Entries (Phase 1)..."),
        "foreign_players": ("sync_foreign_player_changes", "🚀 Syncing Foreign Player Changes (Phase 1)..."),
        "managers": ("sync_manager_changes", "🚀 Syncing Manager Changes (Phase 1)..."),
        "team_events": ("sync_team_events", "🚀 Syncing Team Events/News..."),
        "fan_culture": ("sync_fan_culture", "🚀 Syncing Fan Culture Data (Phase 1)..."),
        "phase1_all": ("sync_phase1_all", "🚀 Syncing ALL Phase 1 Tables..."),
        "transit_times": ("sync_transit_times", "🚀 Syncing Stadium Transit Times (JAMSIL)..."),
        "congestion": ("sync_congestion", "🚀 Syncing Stadium Congestion (JAMSIL)..."),
        "operation_notices": ("sync_operation_notices", "🚀 Syncing Stadium Operation Notices..."),
        "stadium_realtime_all": ("sync_stadium_realtime_all", "🚀 Syncing ALL Stadium Real-Time Tables..."),
    }


def _log_simple_result(header_str: str, result: object) -> None:
    header_label = header_str.replace("🚀 ", "")
    if isinstance(result, int):
        logger.info("✅ %s Finished (%d rows)", header_label, result)
    else:
        logger.info("✅ %s Finished", header_label)


def _run_special_simple_flag(syncer: OCISync, args: argparse.Namespace, flag: str, header_str: str) -> bool:
    if flag == "stadiums":
        logger.info(header_str)
        synced_info = syncer.sync_stadium_info()
        synced_reg = syncer.sync_stadium_regulations()
        logger.info("✅ Stadium Info (%s) + Regulations (%s) Sync Finished", synced_info, synced_reg)
        return True
    if flag == "fan_culture":
        logger.info(header_str)
        synced_r = syncer.sync_team_rivalries()
        synced_s = syncer.sync_cheer_songs()
        synced_c = syncer.sync_cheer_chants()
        logger.info("✅ Fan Culture Sync Finished (Rivalries=%s, Songs=%s, Chants=%s)", synced_r, synced_s, synced_c)
        return True
    if flag == "teams":
        logger.info(header_str)
        syncer.sync_franchises()
        syncer.sync_teams()
        syncer.sync_team_history()
        syncer.sync_team_code_map()
        logger.info("✅ Franchises & Teams Sync Finished")
        return True
    if flag == "players":
        logger.info(header_str)
        syncer.sync_player_basic()
        syncer.sync_players()
        logger.info("✅ Master Players Sync Finished")
        return True
    if flag == "player_basic":
        logger.info("🚀 Syncing Player Basic using specialized OCISync (limit=%s)...", args.limit)
        result = syncer.sync_player_basic(limit=args.limit)
        _log_simple_result("🚀 Player Basic Sync", result)
        return True
    return False


def _run_bulk_simple_flag(syncer: OCISync, args: argparse.Namespace, flag: str, header_str: str) -> bool:
    if flag == "phase1_all":
        logger.info(header_str)
        results = syncer.sync_phase1_all()
    elif flag == "stadium_realtime_all":
        logger.info(header_str)
        results = syncer.sync_stadium_realtime_all(game_date=getattr(args, "realtime_game_date", None))
    else:
        return False

    for table, count in results.items():
        logger.info("  %s: %s rows", table, count)
    logger.info("✅ %s Finished", header_str.replace("🚀 Syncing ", ""))
    return True


def _run_simple_flag(args: argparse.Namespace, flag: str, method_name: str, header_str: str) -> None:
    with SessionLocal() as session:
        syncer = OCISync(args.target_url, session)
        syncer.concurrency = args.workers
        try:
            if _run_special_simple_flag(syncer, args, flag, header_str):
                return
            if _run_bulk_simple_flag(syncer, args, flag, header_str):
                return
            logger.info(header_str)
            method = getattr(syncer, method_name)
            if flag in ("transit_times", "congestion", "operation_notices"):
                result = method(game_date=getattr(args, "realtime_game_date", None))
            elif flag == "daily_roster":
                result = method(
                    start_date=args.roster_date or args.roster_start_date,
                    end_date=args.roster_date or args.roster_end_date,
                )
            else:
                result = method()
            _log_simple_result(header_str, result)
        finally:
            syncer.close()


def _reset_sequences_if_requested(args: argparse.Namespace) -> None:
    if not getattr(args, "reset_sequences", False):
        return
    logger.info("\n🚀 Resetting Sequence Identifiers on Target DB...")
    try:
        from scripts.maintenance.reset_oci_sequences import reset_sequences

        reset_sequences(args.target_url)
    except (ImportError, *OCI_CLI_EXCEPTIONS):
        logger.exception("⚠️ Failed to call reset_sequences")


def main(argv: Iterable[str] | None = None) -> None:
    """스크립트의 메인 실행 함수.

    Args:
        argv: Argv.

    """
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    load_dotenv()
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    _validate_args(parser, args)

    # New compare handling
    if getattr(args, "compare", False):
        logger.info("🔍 Comparing record counts between local DB and OCI...")
        from sqlalchemy.orm import sessionmaker

        # Local session
        local_session = SessionLocal()
        # OCI session
        oci_engine = create_engine_for_url(args.target_url)
        oci_session = sessionmaker(bind=oci_engine)()

        try:
            local_batting = local_session.execute(text("SELECT COUNT(*) FROM player_season_batting")).scalar_one()
            local_pitching = local_session.execute(text("SELECT COUNT(*) FROM player_season_pitching")).scalar_one()
            oci_batting = oci_session.execute(text("SELECT COUNT(*) FROM player_season_batting")).scalar_one()
            oci_pitching = oci_session.execute(text("SELECT COUNT(*) FROM player_season_pitching")).scalar_one()
            logger.info(
                "📊 Data counts comparison:\n   Local batting: %s, OCI batting: %s\n"
                "   Local pitching: %s, OCI pitching: %s",
                local_batting,
                oci_batting,
                local_pitching,
                oci_pitching,
            )
            if getattr(args, "apply", False):
                direction = getattr(args, "direction", "bidirectional")
                logger.info("⚙️ Apply sync in direction: %s", direction)
                # Placeholder: invoke existing sync flow or custom logic
        finally:
            local_session.close()
            oci_session.close()
            oci_engine.dispose()
        return

    sync_dispatch = _build_sync_dispatch()
    simple_flags = _build_simple_flags()
    flag = _detect_active_flag(args, list(sync_dispatch.keys()) + list(simple_flags.keys()))

    if flag is None:
        logger.warning("⚠️  No recognized sync flag provided. Use --help to see available flags.")
        logger.info("   Tip: --game-details, --season-stats, --teams, --player-basic, --kbo-season")
        _reset_sequences_if_requested(args)
        return

    if flag in sync_dispatch:
        sync_fn, header_str, parallel_ok, year_getter, completion_msg = sync_dispatch[flag]
        _run_sync(
            args,
            sync_fn,
            config=SyncRunConfig(
                parallel_support=parallel_ok,
                header=header_str,
                years_getter=year_getter,
                completion_msg=completion_msg,
            ),
        )
    elif flag in simple_flags:
        method_name, header_str = simple_flags[flag]
        _run_simple_flag(args, flag, method_name, header_str)
    else:
        logger.warning("⚠️  No recognized sync flag provided. Use --help to see available flags.")
        logger.info("   Tip: --game-details, --season-stats, --teams, --player-basic, --kbo-season")
        return

    _reset_sequences_if_requested(args)


def _detect_active_flag(args: argparse.Namespace, all_flags: list[str]) -> str | None:
    for flag_name in all_flags:
        if getattr(args, flag_name.replace("-", "_"), False):
            return flag_name
    return None


def _maybe_purge(syncer: OCISync, year: int | None, truncate: object) -> None:
    if truncate and year:
        syncer.purge_season_stats(year)


def _sync_season_stats(syncer: OCISync, year: int | None, **kw: object) -> object:
    _maybe_purge(syncer, year, kw.get("truncate"))
    force = bool(kw.get("force"))
    logger.info("  - [%s] Syncing Player Batting stats...", year)
    syncer.sync_player_season_batting(year=year, batch_size=kw.get("batch_size"), force=force)  # type: ignore[arg-type]
    logger.info("  - [%s] Syncing Player Pitching stats...", year)
    syncer.sync_player_season_pitching(year=year, batch_size=kw.get("batch_size"), force=force)  # type: ignore[arg-type]
    logger.info("  - [%s] Syncing Team Batting stats...", year)
    syncer.sync_team_season_batting(year=year, batch_size=kw.get("batch_size"), force=force)  # type: ignore[arg-type]
    logger.info("  - [%s] Syncing Team Pitching stats...", year)
    syncer.sync_team_season_pitching(year=year, batch_size=kw.get("batch_size"), force=force)  # type: ignore[arg-type]
    logger.info("  - [%s] Syncing Fielding stats...", year)
    syncer.sync_fielding_stats(year=year, batch_size=kw.get("copy_batch_size"), force=force)  # type: ignore[arg-type]
    logger.info("  - [%s] Syncing Baserunning stats...", year)
    syncer.sync_baserunning_stats(year=year, batch_size=kw.get("copy_batch_size"), force=force)  # type: ignore[arg-type]
    logger.info("  - [%s] Syncing Team Fielding stats...", year)
    syncer.sync_team_season_fielding(year=year, batch_size=kw.get("batch_size"), force=force)  # type: ignore[arg-type]
    logger.info("  - [%s] Syncing Team Baserunning stats...", year)
    return syncer.sync_team_season_baserunning(year=year, batch_size=kw.get("batch_size"), force=force)  # type: ignore[arg-type]


def _sync_player_game_stats(syncer: OCISync, year: int | None, **kw: object) -> object:
    logger.info("  - [%s] Syncing Player Game Batting...", year)
    syncer.sync_player_game_batting(year=year, batch_size=kw.get("copy_batch_size", 5000))  # type: ignore[arg-type]
    logger.info("  - [%s] Syncing Player Game Pitching...", year)
    return syncer.sync_player_game_pitching(year=year, batch_size=kw.get("copy_batch_size", 5000))  # type: ignore[arg-type]


if __name__ == "__main__":  # pragma: no cover
    main()
