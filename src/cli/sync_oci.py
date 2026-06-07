"""로컬 SQLite 데이터베이스의 데이터를 원격 OCI/Postgres 데이터베이스와 동기화하는 스크립트.

OCISync 클래스와 전용 동기화 메서드를 사용하여 테이블별로 특화된 UPSERT/COPY 로직을
수행합니다. `--truncate` 옵션을 사용하면 대상 테이블의 데이터를 삭제한 후 새로 삽입할 수 있습니다.
"""

from __future__ import annotations

import argparse
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Iterable

from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.orm import Session

from src.db.engine import SessionLocal, get_oci_url
from src.models.game import Game
from src.models.matchup import BatterTeamSplit
from src.models.player import PlayerSeasonBatting
from src.models.rankings import StatRanking
from src.models.standings import TeamStandingsDaily
from src.sync.oci_sync import OCISync

logger = logging.getLogger(__name__)


def run_parallel_sync(
    sync_fn: Callable[[OCISync, Any], None], target_url: str, years: list[int], workers: int, **kwargs
) -> None:
    """연도별로 병렬 동기화 작업을 수행합니다."""
    logger.info(f"🚀 Starting parallel sync with {workers} workers for years: {years}")

    def sync_worker(year: int):
        logger.info(f"🧵 Worker started for year {year}")
        with SessionLocal() as session:
            try:
                syncer = OCISync(target_url, session)
                sync_fn(syncer, year, **kwargs)
                logger.info(f"✅ Worker finished for year {year}")
            except Exception:
                logger.exception(f"❌ Worker failed for year {year}")

    with ThreadPoolExecutor(max_workers=workers) as executor:
        executor.map(sync_worker, years)


def _parse_game_ids(value: str | None) -> list[str]:
    if not value:
        return []
    return list(dict.fromkeys(token.strip() for token in value.split(",") if token.strip()))


def build_arg_parser() -> argparse.ArgumentParser:
    """CLI 인자 파서를 생성합니다."""
    parser = argparse.ArgumentParser(description="Sync local SQLite data to OCI/Postgres")
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
        "--teams",
        action="store_true",
        help="프랜차이즈 및 팀 정보를 동기화합니다.",
    )
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
    parser.add_argument(
        "--days",
        type=int,
        help="최근 N일간의 경기 데이터만 동기화합니다.",
    )
    parser.add_argument(
        "--unsynced-only",
        action="store_true",
        help="OCI에 없거나 로컬 업데이트가 더 최근인 미동기화/수정된 데이터만 선별하여 동기화합니다. schedule-only 부모 game 행은 자동 제외됩니다.",
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
        "--kbo-season",
        action="store_true",
        help="KBO 시즌 기준 정보(kbo_seasons)를 동기화합니다.",
    )
    parser.add_argument(
        "--year",
        type=int,
        help="특정 연도의 데이터를 동기화합니다. (e.g., 2018)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="동기화할 최대 행 수를 지정합니다.",
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
    # ─── Phase 1 sync flags ────────────────────────────────────────────────
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
        "--team-events",
        action="store_true",
        help="구단 이벤트/뉴스 정보(team_events)를 동기화합니다.",
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
    # ─── Stadium Real-Time Data sync flags ──────────────────────────────────
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
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="일반 UPSERT 작업 시 배치 크기 (기본값: 500)",
    )
    parser.add_argument(
        "--copy-batch-size",
        type=int,
        default=10000,
        help="고속 COPY 작업 시 배치 크기 (기본값: 10000)",
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
        help="동기화 후 OCI 시퀀스 식별자를 재설정합니다. (기본값: 해제 — per-table reset이 이미 sync_games에서 처리됨)",
    )
    return parser


_ALLOWED_YEAR_COLUMNS = frozenset({
    "season",
    "season_year",
    "strftime('%Y', game_date)",
    "strftime('%Y', standings_date)",
})


def get_available_years(session: Session, model: Any, column_name: str = "season") -> list[int]:
    """대상 테이블에서 사용 가능한 연도 목록을 가져옵니다."""
    if column_name not in _ALLOWED_YEAR_COLUMNS:
        raise ValueError(f"Disallowed column expression: {column_name}")
    query = session.query(text(f"DISTINCT {column_name}")).select_from(model)
    years = [int(row[0]) for row in query.all() if row[0]]
    return sorted(years, reverse=True)


def _run_sync(args, sync_fn, *, parallel_support=False, header=None, years_getter=None, completion_msg=None):
    if header:
        logger.info(header)

    target_years = [args.year] if args.year else []

    if parallel_support and args.parallel and years_getter:
        if not target_years:
            with SessionLocal() as session:
                target_years = years_getter(session)
        run_parallel_sync(
            sync_fn,
            args.target_url,
            target_years,
            args.workers,
            days=args.days,
            unsynced_only=getattr(args, "unsynced_only", None),
            batch_size=args.batch_size,
            copy_batch_size=args.copy_batch_size,
            truncate=args.truncate,
        )
    else:
        with SessionLocal() as session:
            syncer = OCISync(args.target_url, session)
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
                    )
            finally:
                syncer.close()

    if completion_msg:
        logger.info(completion_msg)


def main(argv: Iterable[str] | None = None) -> None:
    """스크립트의 메인 실행 함수."""
    load_dotenv()
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    if args.game_ids and not args.game_details:
        parser.error("--game-ids can only be used with --game-details")
    if args.game_ids and args.parallel:
        parser.error("--game-ids cannot be combined with --parallel")
    if args.roster_date and (args.roster_start_date or args.roster_end_date):
        parser.error("--roster-date cannot be combined with --roster-start-date or --roster-end-date")
    if (args.roster_date or args.roster_start_date or args.roster_end_date) and not args.daily_roster:
        parser.error("--roster-date/--roster-start-date/--roster-end-date can only be used with --daily-roster")

    if not args.target_url:
        raise SystemExit("TARGET_DATABASE_URL must be provided via flag or environment variable")

    # fmt: off
    SYNC_DISPATCH: dict[str, tuple] = {
        "game_details":   (lambda s, y, **kw: s.sync_game_details(year=y, days=kw.get("days"), unsynced_only=kw.get("unsynced_only"), batch_size=kw.get("copy_batch_size")) if not kw.get("requested_game_ids")
                           else [logger.info("   [%s] %s", gid, s.sync_specific_game(gid)) for gid in kw["requested_game_ids"]],
                           "🚀 Syncing Game Details using specialized OCISync...", True,
                           lambda sess: get_available_years(sess, Game, "strftime('%Y', game_date)"),
                           "✅ Game Details Sync Finished"),
        "games_only":     (lambda s, y, **kw: logger.info("   [%s] Synced %s rows", y, s.sync_games(filters=[Game.game_id.like(f'{y}%')] if y else None, batch_size=kw.get('copy_batch_size'))),
                           "🚀 Syncing game table only using specialized OCISync...", True,
                           lambda sess: get_available_years(sess, Game, "strftime('%Y', game_date)"),
                           "✅ Game Table Sync Finished"),
        "season_stats":   (lambda s, y, **kw: (_maybe_purge(s, y, kw.get("truncate")),
                           logger.info("  - [%s] Syncing Player Batting stats...", y), s.sync_player_season_batting(year=y, batch_size=kw.get("batch_size")),
                           logger.info("  - [%s] Syncing Player Pitching stats...", y), s.sync_player_season_pitching(year=y, batch_size=kw.get("batch_size")),
                           logger.info("  - [%s] Syncing Team Batting stats...", y), s.sync_team_season_batting(year=y, batch_size=kw.get("batch_size")),
                           logger.info("  - [%s] Syncing Team Pitching stats...", y), s.sync_team_season_pitching(year=y, batch_size=kw.get("batch_size")),
                           logger.info("  - [%s] Syncing Fielding stats...", y), s.sync_fielding_stats(year=y, batch_size=kw.get("copy_batch_size")),
                           logger.info("  - [%s] Syncing Baserunning stats...", y), s.sync_baserunning_stats(year=y, batch_size=kw.get("copy_batch_size")),
                           logger.info("  - [%s] Syncing Team Fielding stats...", y), s.sync_team_season_fielding(year=y, batch_size=kw.get("batch_size")),
                           logger.info("  - [%s] Syncing Team Baserunning stats...", y), s.sync_team_season_baserunning(year=y, batch_size=kw.get("batch_size")))[-1],
                           "🚀 Syncing Season Stats using specialized OCISync...", True,
                           lambda sess: get_available_years(sess, PlayerSeasonBatting, "season"),
                           "✅ Season Stats Sync Finished"),
        "standings":      (lambda s, y, **kw: logger.info("   [%s] Synced %s rows", y, s.sync_standings(days=kw.get('days'), year=y, batch_size=kw.get('copy_batch_size'))),
                           "🚀 Syncing Daily Standings using specialized OCISync...", True,
                           lambda sess: get_available_years(sess, TeamStandingsDaily, "strftime('%Y', standings_date)"),
                           "✅ Daily Standings Sync Finished"),
        "matchups":       (lambda s, y, **kw: s.sync_matchups(year=y, batch_size=kw.get("copy_batch_size")),
                           "🚀 Syncing Matchup Splits using specialized OCISync...", True,
                           lambda sess: get_available_years(sess, BatterTeamSplit, "season_year"),
                           "✅ Matchup Splits Sync Finished"),
        "rankings":       (lambda s, y, **kw: logger.info("   [%s] Synced %s rows", y, s.sync_stat_rankings(year=y, batch_size=kw.get('copy_batch_size'))),
                            "🚀 Syncing Stat Rankings using specialized OCISync...", True,
                            lambda sess: get_available_years(sess, StatRanking, "season"),
                            "✅ Stat Rankings Sync Finished"),
        "player_game_stats": (lambda s, y, **kw: (logger.info("  - [%s] Syncing Player Game Batting...", y), s.sync_player_game_batting(), logger.info("  - [%s] Syncing Player Game Pitching...", y), s.sync_player_game_pitching())[-1],
                            "🚀 Syncing Player Game Stats...", True,
                            lambda sess: get_available_years(sess, Game, "strftime('%Y', game_date)"),
                            "✅ Player Game Stats Sync Finished"),
    }
    # fmt: on

    SIMPLE_FLAGS = {
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
        # Stadium Real-Time Data
        "transit_times": ("sync_transit_times", "🚀 Syncing Stadium Transit Times (JAMSIL)..."),
        "congestion": ("sync_congestion", "🚀 Syncing Stadium Congestion (JAMSIL)..."),
        "operation_notices": ("sync_operation_notices", "🚀 Syncing Stadium Operation Notices..."),
        "stadium_realtime_all": ("sync_stadium_realtime_all", "🚀 Syncing ALL Stadium Real-Time Tables..."),
    }

    flag = _detect_active_flag(
        args, list(SYNC_DISPATCH.keys()) + list(SIMPLE_FLAGS.keys()) + ["phase1_all", "stadium_realtime_all"]
    )

    if flag in SYNC_DISPATCH:
        sync_fn, header_str, parallel_ok, year_getter, completion_msg = SYNC_DISPATCH[flag]
        _run_sync(
            args,
            sync_fn,
            parallel_support=parallel_ok,
            header=header_str,
            years_getter=year_getter,
            completion_msg=completion_msg,
        )
    elif flag in SIMPLE_FLAGS:
        method_name, header_str = SIMPLE_FLAGS[flag]
        with SessionLocal() as session:
            syncer = OCISync(args.target_url, session)
            try:
                if flag == "stadiums":
                    logger.info(header_str)
                    synced_info = syncer.sync_stadium_info()
                    synced_reg = syncer.sync_stadium_regulations()
                    logger.info(f"✅ Stadium Info ({synced_info}) + Regulations ({synced_reg}) Sync Finished")
                elif flag == "fan_culture":
                    logger.info(header_str)
                    synced_r = syncer.sync_team_rivalries()
                    synced_s = syncer.sync_cheer_songs()
                    synced_c = syncer.sync_cheer_chants()
                    logger.info(
                        f"✅ Fan Culture Sync Finished (Rivalries={synced_r}, Songs={synced_s}, Chants={synced_c})"
                    )
                elif flag == "teams":
                    logger.info(header_str)
                    syncer.sync_franchises()
                    syncer.sync_teams()
                    syncer.sync_team_history()
                    syncer.sync_team_code_map()
                    logger.info("✅ Franchises & Teams Sync Finished")
                elif flag == "players":
                    logger.info(header_str)
                    syncer.sync_player_basic()
                    syncer.sync_players()
                    logger.info("✅ Master Players Sync Finished")
                elif flag == "player_basic":
                    logger.info(f"🚀 Syncing Player Basic using specialized OCISync (limit={args.limit})...")
                    synced = syncer.sync_player_basic(limit=args.limit)
                    if isinstance(synced, int):
                        logger.info("✅ Player Basic Sync Finished (%d rows)", synced)
                    else:
                        logger.info("✅ Player Basic Sync Finished")
                elif flag == "phase1_all":
                    logger.info(header_str)
                    results = syncer.sync_phase1_all()
                    for table, count in results.items():
                        logger.info(f"  {table}: {count} rows")
                    logger.info("✅ All Phase 1 Tables Sync Finished")
                elif flag == "stadium_realtime_all":
                    logger.info(header_str)
                    game_date = getattr(args, "realtime_game_date", None)
                    results = syncer.sync_stadium_realtime_all(game_date=game_date)
                    for table, count in results.items():
                        logger.info(f"  {table}: {count} rows")
                    logger.info("✅ Stadium Real-Time All Sync Finished")
                elif flag in ("transit_times", "congestion", "operation_notices"):
                    logger.info(header_str)
                    game_date = getattr(args, "realtime_game_date", None)
                    method = getattr(syncer, method_name)
                    result = method(game_date=game_date)
                    header_label = header_str.replace("🚀 ", "")
                    if isinstance(result, int):
                        logger.info("✅ %s Finished (%d rows)", header_label, result)
                    else:
                        logger.info("✅ %s Finished", header_label)
                elif flag == "daily_roster":
                    logger.info(header_str)
                    roster_start_date = args.roster_date or args.roster_start_date
                    roster_end_date = args.roster_date or args.roster_end_date
                    result = syncer.sync_daily_rosters(start_date=roster_start_date, end_date=roster_end_date)
                    header_label = header_str.replace("🚀 ", "")
                    if isinstance(result, int):
                        logger.info("✅ %s Finished (%d rows)", header_label, result)
                    else:
                        logger.info("✅ %s Finished", header_label)
                else:
                    logger.info(header_str)
                    method = getattr(syncer, method_name)
                    result = method()
                    header_label = header_str.replace("🚀 ", "")
                    if isinstance(result, int):
                        logger.info("✅ %s Finished (%d rows)", header_label, result)
                    else:
                        logger.info("✅ %s Finished", header_label)
            finally:
                syncer.close()
    else:
        logger.warning("⚠️  No recognized sync flag provided. Use --help to see available flags.")
        logger.info("   Tip: --game-details, --season-stats, --teams, --player-basic, --kbo-season")
        return

    if getattr(args, "reset_sequences", False):
        logger.info("\n🚀 Resetting Sequence Identifiers on Target DB...")
        try:
            from scripts.legacy.maintenance.reset_oci_sequences import reset_sequences

            reset_sequences(args.target_url)
        except Exception:
            logger.exception("⚠️ Failed to call reset_sequences")


def _detect_active_flag(args, all_flags: list[str]) -> str | None:
    for flag_name in all_flags:
        if getattr(args, flag_name.replace("-", "_"), False):
            return flag_name
    return None


def _maybe_purge(syncer, year, truncate):
    if truncate and year:
        syncer.purge_season_stats(year)


if __name__ == "__main__":  # pragma: no cover
    main()
