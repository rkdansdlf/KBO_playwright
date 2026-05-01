"""Run-scoped write contract for overlapping game collection jobs."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Optional


@dataclass(frozen=True)
class GameWriteSource:
    stage: str
    crawler: str
    reason: str = ""

    def label(self) -> str:
        parts = [self.stage, self.crawler]
        if self.reason:
            parts.append(self.reason)
        return "/".join(parts)


class GameWriteContract:
    """Track one run's field/table write claims by game and collection stage."""

    def __init__(
        self,
        *,
        run_label: Optional[str] = None,
        log: Optional[Callable[[str], None]] = None,
        log_duplicate_fields: bool = False,
    ) -> None:
        self.run_label = run_label or f"game-write:{datetime.utcnow():%Y%m%dT%H%M%SZ}"
        self.log = log
        self.log_duplicate_fields = log_duplicate_fields
        self.claimed_games: dict[str, set[GameWriteSource]] = {}
        self.field_claims: dict[tuple[str, str], GameWriteSource] = {}
        self.updated_fields = 0
        self.duplicate_fields = 0
        self.replaced_datasets = 0
        self.duplicate_datasets = 0

    def claim_game(self, game_id: str, source: GameWriteSource) -> None:
        claims = self.claimed_games.setdefault(game_id, set())
        if source in claims:
            return

        if claims:
            previous = ", ".join(sorted(claim.label() for claim in claims))
            self._emit(
                f"[OVERLAP] run={self.run_label} game={game_id} "
                f"previous={previous} current={source.label()}"
            )
        claims.add(source)
        self._emit(
            f"[CLAIM] run={self.run_label} game={game_id} "
            f"stage={source.stage} crawler={source.crawler} reason={source.reason or 'unspecified'}"
        )

    def field_updated(self, game_id: str, source: GameWriteSource, field: str, old: Any, new: Any) -> None:
        self.updated_fields += 1
        previous = self.field_claims.get((game_id, field))
        if previous and previous != source:
            self._emit(
                f"[FIELD-OVERLAP] run={self.run_label} game={game_id} field={field} "
                f"previous={previous.label()} current={source.label()}"
            )
        self.field_claims[(game_id, field)] = source
        self._emit(
            f"[WRITE] run={self.run_label} game={game_id} stage={source.stage} "
            f"crawler={source.crawler} field={field} old={_format_value(old)} new={_format_value(new)}"
        )

    def field_duplicate(self, game_id: str, source: GameWriteSource, field: str, value: Any) -> None:
        self.duplicate_fields += 1
        if self.log_duplicate_fields:
            self._emit(
                f"[SKIP] run={self.run_label} game={game_id} stage={source.stage} "
                f"crawler={source.crawler} field={field} duplicate={_format_value(value)}"
            )

    def dataset_replaced(self, game_id: str, source: GameWriteSource, dataset: str, rows: int) -> None:
        self.replaced_datasets += 1
        self._emit(
            f"[WRITE] run={self.run_label} game={game_id} stage={source.stage} "
            f"crawler={source.crawler} dataset={dataset} rows={rows}"
        )

    def dataset_duplicate(self, game_id: str, source: GameWriteSource, dataset: str, rows: int) -> None:
        self.duplicate_datasets += 1
        self._emit(
            f"[SKIP] run={self.run_label} game={game_id} stage={source.stage} "
            f"crawler={source.crawler} dataset={dataset} duplicate_rows={rows}"
        )

    def summary(self) -> str:
        return (
            f"[WRITE-SUMMARY] run={self.run_label} games={len(self.claimed_games)} "
            f"field_updates={self.updated_fields} field_duplicates={self.duplicate_fields} "
            f"dataset_replacements={self.replaced_datasets} dataset_duplicates={self.duplicate_datasets}"
        )

    def _emit(self, message: str) -> None:
        if self.log:
            self.log(message)


def _format_value(value: Any) -> str:
    text = repr(value)
    if len(text) > 80:
        return text[:77] + "..."
    return text
