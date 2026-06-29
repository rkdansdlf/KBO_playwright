"""Shared helpers for schedule persistence workflows."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from src.repositories.game_repository import save_schedule_game
from src.services.game_write_contract import GameWriteContract
from src.utils.schedule_validation import validate_schedule_game_payload

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

logger = logging.getLogger(__name__)


@dataclass
class ScheduleSaveResult:
    """ScheduleSaveResult class."""

    games: list[dict[str, Any]]
    saved_games: list[dict[str, Any]]
    failed_games: list[dict[str, Any]]
    filtered_games: list[dict[str, Any]]
    saved: int = 0
    failed: int = 0
    filtered: int = 0

    @property
    def discovered(self) -> int:
        """
        Handle the discovered operation.

        Returns:
            Integer result.

        """
        return len(self.games)


def save_schedule_games(
    games: Iterable[dict[str, Any]],
    *,
    log: Callable[[str], None] | None = logger.info,
    write_contract: GameWriteContract | None = None,
    source_crawler: str = "ScheduleCrawler",
    source_reason: str = "schedule_refresh",
) -> ScheduleSaveResult:
    """
    Save schedule games.

    Args:
        games: Games.
        log: Logger instance.
        write_contract: Write Contract.
        source_crawler: Source Crawler.
        source_reason: Source Reason.
        games: Games.
        log: Logger instance.
        write_contract: Write Contract.
        source_crawler: Source Crawler.
        source_reason: Source Reason.
        games: Games.

    Returns:
        ScheduleSaveResult instance.

    """
    game_list = list(games)

    result = ScheduleSaveResult(games=game_list, saved_games=[], failed_games=[], filtered_games=[])
    contract = write_contract or GameWriteContract(run_label="schedule_collection", log=log)
    for game in game_list:
        is_valid, failure_reason = validate_schedule_game_payload(game)
        if not is_valid:
            result.failed += 1
            result.filtered += 1
            filtered_game = dict(game)
            filtered_game["failure_reason"] = failure_reason or "schedule_payload_filtered"
            result.failed_games.append(filtered_game)
            result.filtered_games.append(filtered_game)
            if log:
                log(
                    "[WARN] Filtered schedule game: "
                    f"{game.get('game_id') or '<missing>'} "
                    f"reason={filtered_game['failure_reason']}",
                )
            continue

        if save_schedule_game(
            game,
            write_contract=contract,
            source_stage="schedule",
            source_crawler=source_crawler,
            source_reason=source_reason,
        ):
            result.saved += 1
            result.saved_games.append(game)
        else:
            result.failed += 1
            result.failed_games.append(game)
            if log:
                log(f"[WARN] Failed to save schedule game: {game.get('game_id')}")
    if log and write_contract is None:
        log(contract.summary())
    return result
