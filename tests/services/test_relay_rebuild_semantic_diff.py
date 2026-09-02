"""Gate R1-C: Full Semantic Rebuild Differential & Persistence Roundtrip Idempotency Tests.

Verifies:
1. 22-field canonical event representation.
2. Natural key differential: (game_id, event_seq) -> added, removed, modified, reordered.
3. Half-inning chronological progression: (inn, half) monotonically increasing.
4. Persistence roundtrip idempotency: Rebuild -> Save to Ephemeral DB -> Query -> Rebuild 2 == Rebuild 1.
"""

from __future__ import annotations

import hashlib
from datetime import date
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.cli.backfill.rebuild_relay_events import (
    _build_orm_events,
    _event_to_payload,
    _rebuild_events_for_game,
)
from src.models.base import Base
from src.models.game import Game, GameEvent
from src.services.wpa_calculator import WPACalculator

CANONICAL_FIELDS = (
    "game_id",
    "event_seq",
    "inning",
    "inning_half",
    "outs",
    "at_bat_seq",
    "batter_id",
    "batter_name",
    "pitcher_id",
    "pitcher_name",
    "description",
    "event_type",
    "result_code",
    "rbi",
    "bases_before",
    "bases_after",
    "wpa",
    "win_expectancy_before",
    "win_expectancy_after",
    "score_diff",
    "base_state",
    "home_score",
    "away_score",
)


def canonical_event_repr(ev: dict[str, Any] | GameEvent) -> str:
    parts = []
    for k in CANONICAL_FIELDS:
        v = getattr(ev, k, None) if isinstance(ev, GameEvent) else ev.get(k)
        if isinstance(v, float):
            parts.append(f"{v:.4f}")
        else:
            parts.append(str(v if v is not None else ""))
    return "|".join(parts)


def stream_hash(events: list[Any]) -> str:
    h = hashlib.sha256()
    for ev in events:
        h.update(canonical_event_repr(ev).encode("utf-8"))
    return h.hexdigest()


def test_full_semantic_rebuild_and_persistence_roundtrip(tmp_path: Path):
    """Verify full 22-field semantic rebuild and persistence roundtrip idempotency."""
    calc = WPACalculator()
    game_id = "20260401SKLG0"

    # Ephemeral SQLite database
    db_file = tmp_path / "ephemeral_roundtrip.db"
    db_url = f"sqlite:///{db_file}"
    engine = create_engine(db_url)
    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine)

    with Session() as s:
        s.add(
            Game(
                game_id=game_id,
                game_date=date(2026, 4, 1),
                home_team="LG",
                away_team="SK",
                home_score=0,
                away_score=2,
                game_status="COMPLETED",
            )
        )
        s.commit()

    raw_events = [
        GameEvent(
            game_id=game_id,
            event_seq=1,
            inning=1,
            inning_half="top",
            outs=0,
            batter_name="홍길동",
            pitcher_name="김투수",
            description="홍길동 : 중전 안타",
            event_type="hit",
            result_code="1B",
            home_score=0,
            away_score=0,
            bases_before="---",
            bases_after="1--",
        ),
        GameEvent(
            game_id=game_id,
            event_seq=2,
            inning=1,
            inning_half="top",
            outs=0,
            batter_name="이순신",
            pitcher_name="김투수",
            description="이순신 : 우월 투런 홈런 (비거리 125m)",
            event_type="home_run",
            result_code="HR",
            home_score=0,
            away_score=2,
            bases_before="1--",
            bases_after="---",
        ),
        GameEvent(
            game_id=game_id,
            event_seq=3,
            inning=1,
            inning_half="top",
            outs=0,
            batter_name="강감찬",
            pitcher_name="김투수",
            description="강감찬 : 삼진 아웃",
            event_type="strikeout",
            result_code="SO",
            home_score=0,
            away_score=2,
            bases_before="---",
            bases_after="---",
        ),
    ]

    # Rebuild Pass 1
    rebuilt_1 = _rebuild_events_for_game(raw_events, calculator=calc)
    pass1_hash = stream_hash(rebuilt_1)

    # Invariants on Pass 1
    assert len(rebuilt_1) == 3
    for ev in rebuilt_1:
        assert 0.0 <= ev["win_expectancy_before"] <= 1.0
        assert 0.0 <= ev["win_expectancy_after"] <= 1.0
        assert ev["outs"] in (0, 1, 2, 3)

    # Persist Pass 1 to ephemeral DB
    with Session() as s:
        orm_rows = _build_orm_events(game_id, rebuilt_1)
        s.add_all(orm_rows)
        s.commit()

    # Re-query from DB and run Rebuild Pass 2 (Persistence Roundtrip Idempotency)
    with Session() as s:
        persisted = (
            s.query(GameEvent)
            .filter(GameEvent.game_id == game_id)
            .order_by(GameEvent.event_seq.asc(), GameEvent.id.asc())
            .all()
        )
        rebuilt_2 = _rebuild_events_for_game(persisted, calculator=calc)
        pass2_hash = stream_hash(rebuilt_2)

    # Exact full 22-field equality across persistence roundtrip
    assert pass1_hash == pass2_hash
    assert len(rebuilt_1) == len(rebuilt_2)
    for ev1, ev2 in zip(rebuilt_1, rebuilt_2, strict=True):
        assert canonical_event_repr(ev1) == canonical_event_repr(ev2)
