"""
퓨처스리그 선수들의 연도별 타격 기록을 수집하는 CLI 스크립트.

이 스크립트는 다음 단계를 수행합니다:
1. 특정 시즌의 모든 현역 선수 ID 목록을 가져옵니다.
2. 각 선수에 대해 퓨처스리그 기록 페이지로 이동하여 연도별 타격 데이터를 크롤링하고 파싱합니다.
3. 파싱된 데이터를 데이터베이스에 UPSERT(존재하면 업데이트, 없으면 삽입)하여 저장합니다.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from collections import Counter
from datetime import datetime
from typing import Sequence

from src.crawlers.futures.futures_batting import fetch_and_parse_futures_batting
from src.crawlers.futures.futures_pitching import fetch_and_parse_futures_pitching
from src.crawlers.player_list_crawler import PlayerListCrawler
from src.db.engine import SessionLocal
from src.models.player import PlayerBasic
from src.parsers.player_profile_parser import PlayerProfileParsed
from src.repositories.player_repository import PlayerRepository
from src.repositories.player_season_pitching_repository import save_pitching_stats_to_db
from src.repositories.save_futures_batting import save_futures_batting
from src.utils.player_validation import normalize_player_id
from src.utils.playwright_pool import AsyncPlaywrightPool
from src.utils.safe_print import safe_print as print


def _has_player_basic(player_id: str) -> bool:
    try:
        player_id_db = int(str(player_id).strip())
    except (TypeError, ValueError):
        return False

    with SessionLocal() as session:
        return (
            session.query(PlayerBasic.player_id).filter(PlayerBasic.player_id == player_id_db).scalar_one_or_none()
            is not None
        )


async def gather_active_player_ids(season_year: int, delay: float) -> dict[str, dict[str, str]]:
    """지정된 시즌의 모든 현역 선수 ID와 메타정보(포지션, 이름)를 수집합니다."""
    print(f"Gathering active player list for {season_year}...")
    crawler = PlayerListCrawler(request_delay=delay)
    result = await crawler.crawl_all_players(season_year=season_year)

    player_positions: dict[str, dict[str, str]] = {}
    for player in result.get("hitters", []):
        pid = player.get("player_id")
        name = player.get("player_name") or ""
        if pid:
            player_positions[pid] = {"position": "hitter", "name": name}

    for player in result.get("pitchers", []):
        pid = player.get("player_id")
        name = player.get("player_name") or ""
        if pid:
            if pid in player_positions:
                player_positions[pid] = {"position": "both", "name": name}
            else:
                player_positions[pid] = {"position": "pitcher", "name": name}

    hitters_cnt = sum(1 for m in player_positions.values() if m["position"] == "hitter")
    pitchers_cnt = sum(1 for m in player_positions.values() if m["position"] == "pitcher")
    both_cnt = sum(1 for m in player_positions.values() if m["position"] == "both")
    print(
        f"Found {len(player_positions)} active players (hitters: {hitters_cnt}, pitchers: {pitchers_cnt}, both: {both_cnt})"
    )
    return player_positions


async def process_player(
    player_id: str,
    position: str,
    player_name: str,
    repository: PlayerRepository,
    delay: float,
    pool,
) -> tuple[str, int]:
    """
    단일 선수의 퓨처스리그 기록을 크롤링하고 데이터베이스에 저장합니다.

    Returns:
        (player_id, 저장된 시즌 기록 수)
    """
    result = await process_player_result(player_id, position, player_name, repository, delay, pool)
    return (str(result["player_id"]), int(result["saved"]))


async def process_player_result(
    player_id: str,
    position: str,
    player_name: str,
    repository: PlayerRepository,
    delay: float,
    pool,
) -> dict:
    normalized_id = normalize_player_id(player_id)
    if normalized_id is None:
        return {
            "player_id": player_id,
            "status": "failed",
            "saved": 0,
            "failure_reason": "invalid_player_id",
        }

    player_id = str(normalized_id)

    # Resolve player name if not provided
    if not player_name:
        with SessionLocal() as session:
            basic = session.query(PlayerBasic).filter(PlayerBasic.player_id == int(player_id)).first()
            if basic:
                player_name = basic.name
            else:
                player_name = "Unknown"

    if not _has_player_basic(player_id):
        return {
            "player_id": player_id,
            "status": "skipped",
            "saved": 0,
            "failure_reason": "missing_player_basic",
        }

    batting_rows = []
    pitching_rows = []

    # 1. Hitter stats
    if position in ("hitter", "both"):
        hitter_url = f"https://www.koreabaseball.com/Futures/Player/HitterTotal.aspx?playerId={player_id}"
        try:
            batting_rows = await fetch_and_parse_futures_batting(player_id, hitter_url, pool=pool)
        except Exception as exc:
            print(f"[ERROR] Exception crawling batting stats for player {player_id}: {exc}")

    # 2. Pitcher stats
    if position in ("pitcher", "both"):
        pitcher_url = f"https://www.koreabaseball.com/Futures/Player/PitcherTotal.aspx?playerId={player_id}"
        try:
            pitching_rows = await fetch_and_parse_futures_pitching(player_id, pitcher_url, pool=pool)
        except Exception as exc:
            print(f"[ERROR] Exception crawling pitching stats for player {player_id}: {exc}")

    if not batting_rows and not pitching_rows:
        return {
            "player_id": player_id,
            "status": "skipped",
            "saved": 0,
            "failure_reason": "futures_empty",
        }

    # 데이터베이스에 선수 정보가 없으면 새로 생성
    player = await asyncio.to_thread(
        repository.upsert_player_profile, player_id, PlayerProfileParsed(is_active=True, player_name=player_name)
    )

    if not player:
        print(f"[WARN] Could not create player record for {player_id}")
        return {
            "player_id": player_id,
            "status": "failed",
            "saved": 0,
            "failure_reason": "profile_upsert_failed",
        }

    saved = 0

    # Save Hitter stats if any
    if batting_rows:
        try:
            saved_batting = await asyncio.to_thread(save_futures_batting, player_id, batting_rows)
            saved += saved_batting
        except Exception as exc:
            print(f"[ERROR] Exception saving batting stats for player {player_id}: {exc}")

    # Save Pitcher stats if any
    if pitching_rows:
        try:
            payloads = []
            for row in pitching_rows:
                payloads.append(
                    {
                        "player_id": int(player_id),
                        "player_name": player_name,
                        "season": row.get("season"),
                        "league": "FUTURES",
                        "level": "KBO2",
                        "source": "PROFILE",
                        "team_code": row.get("team_code"),
                        "games": row.get("games"),
                        "complete_games": row.get("complete_games"),
                        "shutouts": row.get("shutouts"),
                        "wins": row.get("wins"),
                        "losses": row.get("losses"),
                        "saves": row.get("saves"),
                        "holds": row.get("holds"),
                        "innings_pitched": row.get("innings_pitched"),
                        "innings_outs": row.get("innings_outs"),
                        "hits_allowed": row.get("hits_allowed"),
                        "runs_allowed": row.get("runs_allowed"),
                        "earned_runs": row.get("earned_runs"),
                        "home_runs_allowed": row.get("home_runs_allowed"),
                        "walks_allowed": row.get("walks_allowed"),
                        "hit_batters": row.get("hit_batters"),
                        "strikeouts": row.get("strikeouts"),
                        "era": row.get("era"),
                        "tbf": row.get("tbf"),
                    }
                )
            saved_pitching = await asyncio.to_thread(save_pitching_stats_to_db, payloads)
            saved += saved_pitching
        except Exception as exc:
            print(f"[ERROR] Exception saving pitching stats for player {player_id}: {exc}")

    if saved > 0:
        return {
            "player_id": player_id,
            "status": "success",
            "saved": saved,
            "failure_reason": None,
        }

    return {
        "player_id": player_id,
        "status": "failed",
        "saved": 0,
        "failure_reason": "save_failed",
    }


async def crawl_futures(args: argparse.Namespace) -> dict:
    """퓨처스리그 크롤링 메인 로직."""
    print("\n=== Futures League Stats Crawler ===")
    print(f"Season: {args.season}")
    print(f"Concurrency: {args.concurrency}")
    print(f"Delay: {args.delay}s\n")

    # 1단계: 크롤링 대상 선수 ID 목록 수집
    if getattr(args, "player_ids", None):
        pids = [pid.strip() for pid in args.player_ids.split(",") if pid.strip()]
        player_positions = {}
        for pid in pids:
            player_positions[pid] = {"position": "both", "name": ""}
        print(f"Using target player IDs from CLI: {pids}\n")
    else:
        player_positions = await gather_active_player_ids(args.season, args.delay)

    if args.limit:
        limited_pids = sorted(player_positions.keys())[: args.limit]
        player_positions = {pid: player_positions[pid] for pid in limited_pids}
        print(f"Limited to {len(player_positions)} players\n")

    summary = {
        "ok": False,
        "season": args.season,
        "processed": 0,
        "success_count": 0,
        "total_saved": 0,
        "failure_counts": {},
        "results": [],
    }

    if not player_positions:
        print("No players to process")
        summary["failure_counts"] = {"player_list_empty": 1}
        if getattr(args, "json_summary", False):
            print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
        return summary

    # 2단계: 각 선수를 병렬로 처리
    print(f"Processing {len(player_positions)} players...\n")

    repository = PlayerRepository()
    pool = AsyncPlaywrightPool(
        max_pages=args.concurrency,
        context_kwargs={"locale": "ko-KR"},
    )
    semaphore = asyncio.Semaphore(args.concurrency)  # 동시 요청 수 제어

    results: list[dict] = []
    failure_counts: Counter = Counter()

    async def runner(pid: str, meta: dict):
        async with semaphore:
            pos = meta["position"]
            name = meta["name"]
            try:
                result = await process_player_result(pid, pos, name, repository, args.delay, pool)
            except Exception as exc:
                print(f"[ERROR] Unhandled exception for player {pid} ({pos}): {exc}")
                result = {
                    "player_id": pid,
                    "status": "failed",
                    "saved": 0,
                    "failure_reason": "exception",
                }
            results.append(result)

            player_id = result["player_id"]
            saved = result["saved"]
            failure_reason = result.get("failure_reason")
            if result["status"] == "success":
                print(f"[OK] {player_id} ({pos}): {saved} seasons")
            elif failure_reason == "futures_empty":
                failure_counts[failure_reason] += 1
                print(f"[SKIP] {player_id} ({pos}): no Futures data")
            else:
                failure_counts[failure_reason or "exception"] += 1
                print(f"[ERROR] {player_id} ({pos}): {failure_reason}")

    async with pool:
        await asyncio.gather(*(runner(pid, meta) for pid, meta in sorted(player_positions.items())))

    # 3단계: 결과 요약
    print("\n=== Summary ===")
    total_saved = sum(result["saved"] for result in results)
    success_count = sum(1 for result in results if result["status"] == "success")

    print(f"Total players processed: {len(results)}")
    print(f"Players with Futures data: {success_count}")
    print(f"Total seasons saved: {total_saved}")
    print(f"Failures/skips: {sum(failure_counts.values())}")

    if failure_counts:
        print("\nFailure reasons:")
        for reason, count in sorted(failure_counts.items()):
            print(f"  {reason}: {count}")

    summary.update(
        {
            "ok": not any(result["status"] == "failed" for result in results),
            "processed": len(results),
            "success_count": success_count,
            "total_saved": total_saved,
            "failure_counts": dict(failure_counts),
            "results": results,
        }
    )
    if getattr(args, "json_summary", False):
        print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return summary


def build_arg_parser() -> argparse.ArgumentParser:
    """CLI 인자 파서를 생성합니다."""
    parser = argparse.ArgumentParser(description="Crawl year-by-year Futures batting stats for active players")
    parser.add_argument(
        "--season",
        type=int,
        default=datetime.now().year,
        help="기준 시즌 (기본값: 현재 연도)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=3,
        help="동시 요청 수 (기본값: 3)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=2.0,
        help="요청 간 지연 시간(초) (기본값: 2.0)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="처리할 최대 선수 수 (테스트용)",
    )
    parser.add_argument(
        "--player-ids",
        type=str,
        default=None,
        help="쉼표로 구분된 특정 선수 ID 목록 (지정 시 active player list 수집을 건너뜀)",
    )
    parser.add_argument(
        "--json-summary",
        action="store_true",
        help="Print a machine-readable summary after the run",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    asyncio.run(crawl_futures(args))


if __name__ == "__main__":
    main()
