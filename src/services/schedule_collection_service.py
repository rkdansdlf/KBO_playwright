"""Shared helpers for schedule persistence workflows."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Iterable, List

from src.repositories.game_repository import save_schedule_game
from src.utils.safe_print import safe_print as print


@dataclass
class ScheduleSaveResult:
    games: List[dict[str, Any]]
    saved_games: List[dict[str, Any]]
    failed_games: List[dict[str, Any]]
    saved: int = 0
    failed: int = 0

    @property
    def discovered(self) -> int:
        return len(self.games)


def save_schedule_games(
    games: Iterable[dict[str, Any]],
    *,
    log: Callable[[str], None] | None = print,
) -> ScheduleSaveResult:
    game_list = list(games)
    result = ScheduleSaveResult(games=game_list, saved_games=[], failed_games=[])
    for game in game_list:
        if save_schedule_game(game):
            result.saved += 1
            result.saved_games.append(game)
        else:
            result.failed += 1
            result.failed_games.append(game)
            if log:
                log(f"[WARN] Failed to save schedule game: {game.get('game_id')}")
    return result
