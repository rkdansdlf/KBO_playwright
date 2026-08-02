"""Smart polling gate for KBO daily data collection.

Layer 1 lightweight checker: queries the Naver Sports schedule API to determine
whether all of today's KBO games have finished. Exits with code 0 if crawling
should proceed (all games finished or no games today), code 1 if games are
still in progress (skip this polling cycle to save CI minutes).

"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import date, datetime, timedelta
from http import HTTPStatus
from typing import TYPE_CHECKING, Any

import httpx

from src.constants import KST
from src.utils.game_state import derive_lifecycle_from_naver_status

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)

NAVER_SCHEDULE_API = "https://api-gw.sports.naver.com/schedule/today-games"
NAVER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://m.sports.naver.com",
}

TERMINAL_LIFECYCLE_STATES = {"cancelled", "final"}
ACTIVE_LIFECYCLE_STATES = {"before", "running", "delayed", "suspended", "result_pending_stabilization"}


def get_kst_today_str() -> str:
    """Return today's date as YYYYMMDD in KST."""
    return datetime.now(KST).strftime("%Y%m%d")


def get_kst_today_date() -> date:
    """Return today's date in KST."""
    return datetime.now(KST).date()


def _build_query_params(date_str: str) -> dict[str, str]:
    """Build Naver Sports API query parameters for a given date.

    Note: seasonYear uses the calendar year of the game date. For KBO games
    in Jan-Feb (which may belong to the previous season), this still works
    because the Naver API accepts the game date and resolves the season
    server-side. If this ever changes, use: year_part = str(int(date_str[:4]) - 1)
    when date_str[4:6] in ("01", "02").

    Args:
        date_str: Date Str.

    """
    year_part = date_str[:4]

    return {
        "sectionId": "kbaseball",
        "categoryId": "kbo",
        "seasonYear": year_part,
        "date": f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}",
    }


async def _fetch_naver_games(
    client: httpx.AsyncClient,
    date_str: str,
) -> list[dict[str, Any]]:
    """Fetch today's game list from Naver Sports API.

    Return empty list on any error or non-200 response.
    Network errors are logged at ERROR level to distinguish from "no games."

    Args:
        client: Client.
        date_str: Date Str.

    """
    params = _build_query_params(date_str)

    try:
        response = await client.get(
            NAVER_SCHEDULE_API,
            params=params,
            headers=NAVER_HEADERS,
            timeout=10.0,
        )
        if response.status_code != HTTPStatus.OK:
            logger.error(
                "[GATE] Naver API returned status %d for date %s — treating as error",
                response.status_code,
                date_str,
            )
            return []
        payload = response.json()
        if payload.get("error") is not None:
            logger.error(
                "[GATE] Naver API returned error envelope for date %s: %s",
                date_str,
                payload.get("error"),
            )
            return []
        games = list((payload.get("result") or {}).get("games") or [])
        if games:
            logger.info(
                "[GATE] Naver API returned %d games for date %s",
                len(games),
                date_str,
            )
        else:
            logger.info("[GATE] No games found for date %s (confirmed by API)", date_str)
    except (httpx.HTTPError, ValueError, TypeError, KeyError):
        logger.exception("[GATE] Failed to fetch/parse Naver schedule")
        return []
    else:
        return games


def _extract_game_status(game: dict[str, Any]) -> str | None:
    """Extract the lifecycle-relevant status string from a Naver game object.

    Check multiple possible field names in order of reliability.

    Args:
        game: Game.

    """
    for field in ("status", "gameStatus", "gameState", "progressState"):
        value = game.get(field)
        if value and isinstance(value, str):
            return value.strip().upper()
    return None


def _classify_games(
    games: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """Classify games into terminal, active, and unknown categories.

    Return (terminal_games, active_games, unknown_games).

    Args:
        games: Games.

    """
    terminal: list[dict[str, Any]] = []

    active: list[dict[str, Any]] = []
    unknown: list[dict[str, Any]] = []

    for game in games:
        raw_status = _extract_game_status(game)
        lifecycle = derive_lifecycle_from_naver_status(raw_status)
        if lifecycle is None:
            unknown.append(game)
        elif lifecycle in TERMINAL_LIFECYCLE_STATES:
            terminal.append(game)
        elif lifecycle in ACTIVE_LIFECYCLE_STATES:
            active.append(game)
        else:
            unknown.append(game)

    return terminal, active, unknown


def _format_game_label(game: dict[str, Any]) -> str:
    """Format a human-readable label for a game dict.

    Args:
        game: Game.

    """
    away = game.get("awayTeamName") or game.get("awayTeamCode") or "?"

    home = game.get("homeTeamName") or game.get("homeTeamCode") or "?"
    stadium = game.get("stadiumName") or game.get("stadium") or ""
    return f"{away} vs {home} ({stadium})" if stadium else f"{away} vs {home}"


def _build_details(reason: str, **extras: object) -> dict[str, Any]:
    return {"reason": reason, **extras}


def _unknown_games_are_today_or_future(unknown: list[dict[str, Any]], today_date: date) -> bool:
    for game in unknown:
        game_date_str = game.get("gameDate") or game.get("date") or ""
        if not game_date_str:
            continue
        try:
            game_date = datetime.strptime(game_date_str[:10], "%Y-%m-%d").replace(tzinfo=KST).date()
            if game_date >= today_date:
                logger.info("  ⏳ Unknown game is today or future: %s", _format_game_label(game))
                return True
        except (ValueError, TypeError):
            return True
    return False


async def _handle_no_games(
    today_str: str,
    today_date: date,
    client: httpx.AsyncClient,
) -> tuple[bool, bool, dict[str, Any]] | None:
    yesterday = today_date - timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y%m%d")

    yesterday_games = await _fetch_naver_games(client, yesterday_str)

    if not yesterday_games:
        logger.info("[GATE] No games today or yesterday — likely rest day, skip")
        return (False, False, _build_details("no_games_today", today_games=0, yesterday_active=0))

    _, yesterday_active, _ = _classify_games(yesterday_games)
    if yesterday_active:
        logger.info(
            "[GATE] Yesterday (%s) still has %d active games — proceeding to crawl",
            yesterday_str,
            len(yesterday_active),
        )
        return (
            True,
            True,
            _build_details("yesterday_games_still_active", yesterday_active=len(yesterday_active), today_games=0),
        )

    logger.info("[GATE] No games today (%s), yesterday (%s) all terminal — skip", today_str, yesterday_str)
    return (False, False, _build_details("no_games_today", today_games=0, yesterday_active=0))


def _is_cleaning_time(game: dict[str, Any]) -> bool:
    """Check if the game is currently in cleaning time (5th inning end break)."""
    text_sources = [
        str(game.get(k) or "")
        for k in (
            "status",
            "statusInfo",
            "statusDescription",
            "statusName",
            "progressState",
            "gameStatus",
            "gameState",
        )
    ]
    relay = game.get("relay") or {}
    if isinstance(relay, dict):
        text_sources.extend([str(relay.get(k) or "") for k in ("status", "statusInfo", "description")])

    combined = " ".join(text_sources).lower()
    keywords = ["클리닝타임", "클리닝 타임", "cleaning_time", "cleaning time", "5회말 종료", "5회 종료"]
    return any(kw in combined for kw in keywords)


def diagnose_game_detail(game: dict[str, Any], now_kst: datetime | None = None) -> dict[str, Any]:  # noqa: C901
    """Diagnose fine-grained status and lifecycle for a single game.

    Returns a dict with keys: label, raw_status, lifecycle, diagnosed_state, is_cleaning_time.
    """
    raw_status = _extract_game_status(game)
    lifecycle = derive_lifecycle_from_naver_status(raw_status)
    label = _format_game_label(game)
    cleaning = _is_cleaning_time(game)

    if lifecycle in TERMINAL_LIFECYCLE_STATES:
        diagnosed_state = "TERMINAL"
    elif cleaning:
        diagnosed_state = "CLEANING_TIME"
    elif lifecycle in ("suspended", "delayed"):
        diagnosed_state = "SUSPENDED"
    elif lifecycle == "result_pending_stabilization":
        diagnosed_state = "RESULT_PENDING"
    elif lifecycle == "running":
        diagnosed_state = "RUNNING"
    elif lifecycle == "before":
        game_start_time_str = game.get("startTime") or game.get("gameStartTime") or ""
        is_pregame = False
        if game_start_time_str and now_kst:
            try:
                parts = game_start_time_str.split(":")
                if len(parts) >= 2:  # noqa: PLR2004
                    h, m = int(parts[0]), int(parts[1])
                    game_dt = now_kst.replace(hour=h, minute=m, second=0, microsecond=0)
                    diff = (game_dt - now_kst).total_seconds()
                    if 0 <= diff <= 900:  # noqa: PLR2004
                        is_pregame = True
            except (ValueError, TypeError):
                pass
        diagnosed_state = "PREGAME" if is_pregame else "BEFORE"
    else:
        diagnosed_state = "UNKNOWN"

    return {
        "label": label,
        "raw_status": raw_status,
        "lifecycle": lifecycle,
        "diagnosed_state": diagnosed_state,
        "is_cleaning_time": cleaning,
    }


def diagnose_today_games(  # noqa: PLR0911
    games: list[dict[str, Any]],
    now_kst: datetime | None = None,
) -> tuple[str, int, list[dict[str, Any]]]:
    """Diagnose today's overall games state and compute recommended polling interval in seconds.

    Returns:
        (overall_diagnosed_state, recommended_interval_seconds, games_summary)

    """
    if now_kst is None:
        now_kst = datetime.now(KST)

    if not games:
        return "NO_GAMES", 1800, []

    summaries = [diagnose_game_detail(g, now_kst) for g in games]
    states = [s["diagnosed_state"] for s in summaries]

    if "RUNNING" in states:
        return "GAMES_IN_PROGRESS", 10, summaries
    if "CLEANING_TIME" in states:
        return "CLEANING_TIME", 60, summaries
    if "SUSPENDED" in states:
        return "GAME_SUSPENDED", 60, summaries
    if "RESULT_PENDING" in states:
        return "COOLDOWN", 60, summaries
    if "PREGAME" in states:
        return "PREGAME", 60, summaries
    if "BEFORE" in states:
        return "BEFORE_GAMES", 300, summaries
    if all(s == "TERMINAL" for s in states):
        return "ALL_TERMINAL", 1800, summaries

    return "GAMES_IN_PROGRESS", 30, summaries


async def check_all_games_finished() -> tuple[bool, bool, dict[str, Any]]:
    """Check if all of today's KBO games have reached a terminal state.

    Returns:
        (should_proceed, has_games_today, details) where:
        - should_proceed: True if Layer 2 crawling should run
        - has_games_today: True if there were any games scheduled today
        - details: dict with counts and game info for logging

    """
    today_str = get_kst_today_str()

    today_date = get_kst_today_date()

    logger.info("[GATE] Checking game status for %s (KST)", today_str)

    async with httpx.AsyncClient() as client:
        games = await _fetch_naver_games(client, today_str)

        if not games:
            logger.info("[GATE] No games found for %s — checking if truly no games", today_str)
            result = await _handle_no_games(today_str, today_date, client)
            if result is not None:
                should_p, has_g, d = result
                d.update(
                    {
                        "diagnosed_state": "NO_GAMES",
                        "recommended_polling_interval_seconds": 1800,
                        "games_summary": [],
                    }
                )
                return (should_p, has_g, d)
            return (
                False,
                False,
                _build_details(
                    "no_games_today",
                    today_games=0,
                    yesterday_active=0,
                    diagnosed_state="NO_GAMES",
                    recommended_polling_interval_seconds=1800,
                    games_summary=[],
                ),
            )

    terminal, active, unknown = _classify_games(games)
    now_kst = datetime.now(KST)
    diagnosed_state, rec_interval, games_summary = diagnose_today_games(games, now_kst)

    logger.info(
        "[GAME] Classification: %d terminal, %d active, %d unknown out of %d total (diagnosed=%s, rec_interval=%ds)",
        len(terminal),
        len(active),
        len(unknown),
        len(games),
        diagnosed_state,
        rec_interval,
    )
    for game in games:
        label = _format_game_label(game)
        raw_status = _extract_game_status(game)
        lifecycle = derive_lifecycle_from_naver_status(raw_status)
        logger.info("  ⚾ %s | raw=%s | lifecycle=%s", label, raw_status, lifecycle)

    if active:
        logger.info(
            "[GATE] ⏳ %d game(s) still in progress — skipping this cycle (rec_interval=%ds)",
            len(active),
            rec_interval,
        )
        return (
            False,
            True,
            _build_details(
                "games_in_progress",
                active_count=len(active),
                terminal_count=len(terminal),
                unknown_count=len(unknown),
                active_games=[_format_game_label(g) for g in active],
                diagnosed_state=diagnosed_state,
                recommended_polling_interval_seconds=rec_interval,
                games_summary=games_summary,
            ),
        )

    if unknown and _unknown_games_are_today_or_future(unknown, today_date):
        logger.info("[GATE] ⏳ Unknown-status game(s) are today or future — skipping")
        return (
            False,
            True,
            _build_details(
                "unknown_status_games_today",
                unknown_count=len(unknown),
                active_count=0,
                diagnosed_state=diagnosed_state,
                recommended_polling_interval_seconds=rec_interval,
                games_summary=games_summary,
            ),
        )

    logger.info(
        "[GATE] ✅ All %d game(s) are terminal — proceeding to crawl",
        len(games),
    )
    return (
        True,
        True,
        _build_details(
            "all_games_finished",
            total_games=len(games),
            terminal_count=len(terminal),
            diagnosed_state=diagnosed_state,
            recommended_polling_interval_seconds=rec_interval,
            games_summary=games_summary,
        ),
    )


def build_arg_parser() -> argparse.ArgumentParser:
    """Build arg parser.

    Returns:
        The result of the operation.

    """
    parser = argparse.ArgumentParser(
        description="Smart polling gate: check if today's KBO games are finished",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output result as JSON (for CI artifact capture)",
    )
    return parser


async def main_async(argv: Sequence[str] | None = None) -> int:
    """Run the main entry point for this CLI command.

    Args:
        argv: Argv.

    """
    parser = build_arg_parser()

    args = parser.parse_args(argv)

    should_proceed, has_games, details = await check_all_games_finished()

    if args.json:
        import json

        result = {
            "should_proceed": should_proceed,
            "has_games_today": has_games,
            "timestamp_kst": datetime.now(KST).isoformat(),
            **details,
        }
        sys.stdout.write(json.dumps(result, ensure_ascii=False, indent=2) + "\n")

    if should_proceed:
        return 0
    return 1


def main(argv: Sequence[str] | None = None) -> None:
    """Run the main entry point for this CLI command.

    Args:
        argv: Argv.

    """
    exit_code = asyncio.run(main_async(argv))

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
