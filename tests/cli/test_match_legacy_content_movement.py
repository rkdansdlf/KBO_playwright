"""The movement index must key on the canonical club code, like the generator does.

A re-key swaps one identity for another, so the matcher and the builder have to
agree on the key byte for byte. ``player_movements.team_code`` holds Korean names
(``롯데``) and era codes (``HT``) as well as canonical codes, so reading that
column here would compute keys the index never contains.
"""

from __future__ import annotations

from datetime import date

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from src.cli.rag.match_legacy_content import _movement_index
from src.models.player import PlayerMovement


def _session() -> Session:
    engine = create_engine("sqlite://")
    PlayerMovement.__table__.create(engine)
    return Session(engine)


def _movement(**overrides) -> PlayerMovement:
    values = {
        "movement_date": date(2024, 7, 18),
        "section": "부상자 명단",
        "team_code": "기아",
        "player_name": "이우성(외야수)",
    }
    values.update(overrides)
    return PlayerMovement(**values)


def test_the_index_key_uses_the_canonical_code() -> None:
    """The raw column holds ``기아``; the key must say ``KIA``."""
    session = _session()
    session.add(_movement(canonical_team_id="KIA"))
    session.commit()

    index = _movement_index(session)
    rows = next(iter(index.values()))

    assert "_KIA_" in rows[0].identity
    assert "기아" not in rows[0].identity


def test_the_index_row_carries_the_canonical_code_as_its_team() -> None:
    """Team agreement is compared on this field, so it must be the canonical one."""
    session = _session()
    session.add(_movement(canonical_team_id="KIA"))
    session.commit()

    rows = next(iter(_movement_index(session).values()))

    assert rows[0].team_code == "KIA"


def test_a_row_without_a_canonical_code_falls_back_to_the_raw_column() -> None:
    session = _session()
    session.add(_movement(canonical_team_id=None))
    session.commit()

    rows = next(iter(_movement_index(session).values()))

    assert "_기아_" in rows[0].identity
    assert rows[0].team_code == "기아"


def test_the_index_row_carries_the_player_name_not_the_team_code() -> None:
    """The team is already in ``team_code``; putting it here would break name matching."""
    session = _session()
    session.add(_movement(canonical_team_id="KIA"))
    session.commit()

    rows = next(iter(_movement_index(session).values()))

    assert rows[0].player_name == "이우성(외야수)"
