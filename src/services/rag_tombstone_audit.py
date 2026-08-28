"""Classify RAG tombstones caused by historical source-identity rekeys."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

from src.services.rag_index_identity import RETRIEVABLE_INDEX_STATUSES

if TYPE_CHECKING:
    from collections.abc import Iterable

    from sqlalchemy.orm import Session


LEGACY_TEAM_CODE_MAP = {
    "BE": "HH",
    "HT": "KIA",
    "MBC": "LG",
    "NX": "KH",
    "OB": "DB",
    "SK": "SSG",
    "WO": "KH",
}
_REKEY_SOURCE_TABLES = frozenset({"player_season_batting", "player_season_pitching"})
_DELETED_INDEX_STATUSES = frozenset({"DELETED", "TOMBSTONED"})
_SEASON_IDENTITY_PATTERN = re.compile(r"(?P<player>[^_]+)_(?P<season>\d{4})_(?P<team>[^_]+)_(?P<league>.+)")


@dataclass(frozen=True, slots=True)
class TombstoneIdentityAudit:
    """Summarize deleted identities and their active canonical replacements."""

    deleted_keys: tuple[str, ...]
    expected_rekey_keys: tuple[str, ...] = ()
    unexplained_keys: tuple[str, ...] = ()

    @property
    def classification(self) -> str:
        """Return the aggregate tombstone classification."""
        if not self.deleted_keys:
            return "NO_DELETED_ROWS"
        if not self.unexplained_keys:
            return "EXPECTED_IDENTITY_REKEY"
        if not self.expected_rekey_keys:
            return "UNEXPLAINED"
        return "MIXED"

    @property
    def is_consistent(self) -> bool:
        """Return whether every deleted identity has a recognized rekey replacement."""
        return not self.unexplained_keys

    def to_dict(self) -> dict[str, object]:
        """Serialize the audit without changing the indexed data."""
        return {
            "deleted": len(self.deleted_keys),
            "expected_identity_rekey": len(self.expected_rekey_keys),
            "unexplained": len(self.unexplained_keys),
            "classification": self.classification,
            "consistent": self.is_consistent,
            "deleted_keys": list(self.deleted_keys),
            "expected_identity_rekey_keys": list(self.expected_rekey_keys),
            "unexplained_keys": list(self.unexplained_keys),
        }


def _expected_rekey_key(source_key: str) -> str | None:
    """Return the canonical replacement key for one legacy season-stat identity."""
    source_table, separator, source_row_id = source_key.partition(":")
    if not separator or source_table not in _REKEY_SOURCE_TABLES:
        return None
    match = _SEASON_IDENTITY_PATTERN.fullmatch(source_row_id)
    if match is None:
        return None
    legacy_team = match.group("team").upper()
    canonical_team = LEGACY_TEAM_CODE_MAP.get(legacy_team)
    if canonical_team is None:
        return None
    canonical_row_id = f"{match.group('player')}_{match.group('season')}_{canonical_team}_{match.group('league')}"
    return f"{source_table}:{canonical_row_id}"


def classify_tombstone_identity_rekeys(
    deleted_keys: Iterable[str],
    active_keys: Iterable[str],
) -> TombstoneIdentityAudit:
    """Classify deleted season-stat identities against active canonical keys."""
    deleted = tuple(sorted(set(deleted_keys)))
    active = set(active_keys)
    expected: list[str] = []
    unexplained: list[str] = []
    for deleted_key in deleted:
        replacement = _expected_rekey_key(deleted_key)
        if replacement is not None and replacement in active:
            expected.append(deleted_key)
        else:
            unexplained.append(deleted_key)
    return TombstoneIdentityAudit(tuple(deleted), tuple(expected), tuple(unexplained))


def audit_tombstone_session(session: Session) -> TombstoneIdentityAudit:
    """Audit deleted Oracle rows against active source identities without writing."""
    from sqlalchemy import select

    from src.models.rag_chunk import RagChunk

    deleted_rows = session.execute(
        select(RagChunk.source_table, RagChunk.source_row_id).where(
            RagChunk.index_status.in_(tuple(_DELETED_INDEX_STATUSES))
        )
    ).all()
    active_rows = session.execute(
        select(RagChunk.source_table, RagChunk.source_row_id).where(
            RagChunk.index_status.in_(tuple(RETRIEVABLE_INDEX_STATUSES))
        )
    ).all()
    deleted_keys = (f"{source_table}:{source_row_id}" for source_table, source_row_id in deleted_rows)
    active_keys = (f"{source_table}:{source_row_id}" for source_table, source_row_id in active_rows)
    return classify_tombstone_identity_rekeys(deleted_keys, active_keys)
