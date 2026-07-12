"""Run-scoped write contract for overlapping game collection jobs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable


MAX_VALUE_REPR_LENGTH = 80
TRUNCATED_VALUE_REPR_LENGTH = 77


@dataclass(frozen=True)
class GameWriteSource:
    """GameWriteSource class."""

    stage: str
    crawler: str
    reason: str = ""

    def label(self) -> str:
        """Handle the label operation.

        Returns:
            String result.

        """
        parts = [self.stage, self.crawler]

        if self.reason:
            parts.append(self.reason)
        return "/".join(parts)


class GameWriteContract:
    """Track one run's field/table write claims by game and collection stage."""

    def __init__(
        self,
        *,
        run_label: str | None = None,
        log: Callable[[str], None] | None = None,
        log_duplicate_fields: bool = False,
    ) -> None:
        """Initialize a new instance.

        Args:
            run_label: Run Label.
            log: Logger instance.
            log_duplicate_fields: Log Duplicate Fields.
            run_label: Run Label.
            log: Logger instance.
            log_duplicate_fields: Log Duplicate Fields.

        """
        self.run_label = run_label or f"game-write:{datetime.now(UTC).replace(tzinfo=None):%Y%m%dT%H%M%SZ}"

        self.log = log
        self.log_duplicate_fields = log_duplicate_fields
        self.claimed_games: dict[str, set[GameWriteSource]] = {}
        self.field_claims: dict[tuple[str, str], GameWriteSource] = {}
        self.updated_fields = 0
        self.duplicate_fields = 0
        self.replaced_datasets = 0
        self.duplicate_datasets = 0

    def claim_game(self, game_id: str, source: GameWriteSource) -> None:
        """Handle the claim game operation.

        Args:
            game_id: Game ID.
            source: Source.
            game_id: Game ID.
            source: Source.
            game_id: Game ID.
            source: Source.

        """
        claims = self.claimed_games.setdefault(game_id, set())

        if source in claims:
            return

        if claims:
            previous = ", ".join(sorted(claim.label() for claim in claims))
            self._emit(f"[OVERLAP] run={self.run_label} game={game_id} previous={previous} current={source.label()}")
        claims.add(source)
        self._emit(
            f"[CLAIM] run={self.run_label} game={game_id} "
            f"stage={source.stage} crawler={source.crawler} reason={source.reason or 'unspecified'}",
        )

    def field_updated(self, game_id: str, source: GameWriteSource, field: str, old: object, new: object) -> None:
        """Handle the field updated operation.

        Args:
            game_id: Game ID.
            source: Source.
            field: Field.
            old: Old.
            new: New.
            game_id: Game ID.
            source: Source.
            field: Field.
            old: Old.
            new: New.
            game_id: Game ID.
            source: Source.
            field: Field.
            old: Old.
            new: New.

        """
        self.updated_fields += 1

        previous = self.field_claims.get((game_id, field))
        if previous and previous != source:
            self._emit(
                f"[FIELD-OVERLAP] run={self.run_label} game={game_id} field={field} "
                f"previous={previous.label()} current={source.label()}",
            )
        self.field_claims[(game_id, field)] = source
        self._emit(
            f"[WRITE] run={self.run_label} game={game_id} stage={source.stage} "
            f"crawler={source.crawler} field={field} old={_format_value(old)} new={_format_value(new)}",
        )

    def field_duplicate(self, game_id: str, source: GameWriteSource, field: str, value: object) -> None:
        """Handle the field duplicate operation.

        Args:
            game_id: Game ID.
            source: Source.
            field: Field.
            value: Value.
            game_id: Game ID.
            source: Source.
            field: Field.
            value: Value.
            game_id: Game ID.
            source: Source.
            field: Field.
            value: Value.

        """
        self.duplicate_fields += 1

        if self.log_duplicate_fields:
            self._emit(
                f"[SKIP] run={self.run_label} game={game_id} stage={source.stage} "
                f"crawler={source.crawler} field={field} duplicate={_format_value(value)}",
            )

    def dataset_replaced(self, game_id: str, source: GameWriteSource, dataset: str, rows: int) -> None:
        """Handle the dataset replaced operation.

        Args:
            game_id: Game ID.
            source: Source.
            dataset: Dataset.
            rows: Rows.
            game_id: Game ID.
            source: Source.
            dataset: Dataset.
            rows: Rows.
            game_id: Game ID.
            source: Source.
            dataset: Dataset.
            rows: Rows.

        """
        self.replaced_datasets += 1

        self._emit(
            f"[WRITE] run={self.run_label} game={game_id} stage={source.stage} "
            f"crawler={source.crawler} dataset={dataset} rows={rows}",
        )

    def dataset_duplicate(self, game_id: str, source: GameWriteSource, dataset: str, rows: int) -> None:
        """Handle the dataset duplicate operation.

        Args:
            game_id: Game ID.
            source: Source.
            dataset: Dataset.
            rows: Rows.
            game_id: Game ID.
            source: Source.
            dataset: Dataset.
            rows: Rows.
            game_id: Game ID.
            source: Source.
            dataset: Dataset.
            rows: Rows.

        """
        self.duplicate_datasets += 1

        self._emit(
            f"[SKIP] run={self.run_label} game={game_id} stage={source.stage} "
            f"crawler={source.crawler} dataset={dataset} duplicate_rows={rows}",
        )

    def summary(self) -> str:
        """Handle the summary operation.

        Returns:
            String result.

        """
        return (
            f"[WRITE-SUMMARY] run={self.run_label} games={len(self.claimed_games)} "
            f"field_updates={self.updated_fields} field_duplicates={self.duplicate_fields} "
            f"dataset_replacements={self.replaced_datasets} dataset_duplicates={self.duplicate_datasets}"
        )

    def _emit(self, message: str) -> None:
        if self.log:
            self.log(message)


def _format_value(value: object) -> str:
    text = repr(value)
    if len(text) > MAX_VALUE_REPR_LENGTH:
        return text[:TRUNCATED_VALUE_REPR_LENGTH] + "..."
    return text
