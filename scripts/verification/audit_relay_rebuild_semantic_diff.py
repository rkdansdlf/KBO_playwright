"""Gate R1-C: Comprehensive Full Semantic Rebuild Differential & Persistence Idempotency Analyzer.

Verifies:
1. Full 22-field canonical contract representation & SHA-256 hash.
2. Natural key-based differential: (game_id, event_seq) -> added, removed, modified, reordered.
3. Chronological half-inning progression: (1, 'top') < (1, 'bottom') < (2, 'top') ...
4. Half-inning out non-decreasing & boundary reset invariants.
5. Score non-decreasing monotonic transitions.
6. Win Expectancy bounds (0.0 <= WE <= 1.0) & WPA balance invariants.
7. Persistence roundtrip idempotency through ephemeral SQLite DB.
8. Diverse game coverage: Standard, Extra-innings, Walk-off, High-scoring, Rain-shortened, Doubleheader.
"""

from __future__ import annotations

import hashlib
import json
import os
from datetime import date
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

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
    """Format an event into its complete 22-field canonical representation."""
    parts = []
    for k in CANONICAL_FIELDS:
        v = getattr(ev, k, None) if isinstance(ev, GameEvent) else ev.get(k)
        if isinstance(v, float):
            parts.append(f"{v:.4f}")
        else:
            parts.append(str(v if v is not None else ""))
    return "|".join(parts)


def stream_sha256(events: list[Any]) -> str:
    h = hashlib.sha256()
    for ev in events:
        h.update(canonical_event_repr(ev).encode("utf-8"))
    return h.hexdigest()


def half_inning_order(inning: int | None, half: str | None) -> int:
    inn = inning or 1
    h = 0 if (half or "top").lower() == "top" else 1
    return inn * 2 + h


def audit_game_semantic_diff(  # noqa: C901
    game_id: str,
    category: str,
    session: Session,
    calc: WPACalculator,
    tmp_path: Path,
) -> dict[str, Any]:
    game = session.query(Game).filter(Game.game_id == game_id).one_or_none()
    old_events = (
        session.query(GameEvent)
        .filter(GameEvent.game_id == game_id)
        .order_by(GameEvent.event_seq.asc(), GameEvent.id.asc())
        .all()
    )

    if not old_events:
        return {"game_id": game_id, "category": category, "error": "no_events"}

    old_payloads = [_event_to_payload(e) for e in old_events]
    old_hash = stream_sha256(old_payloads)

    # Rebuild Pass 1
    rebuilt_1 = _rebuild_events_for_game(old_events, calculator=calc)
    pass1_hash = stream_sha256(rebuilt_1)

    # Key-based event differential
    old_map = {e["event_seq"]: e for e in old_payloads}
    new_map = {e["event_seq"]: e for e in rebuilt_1}

    old_keys = set(old_map.keys())
    new_keys = set(new_map.keys())

    added = len(new_keys - old_keys)
    removed = len(old_keys - new_keys)
    modified = sum(
        1 for k in (old_keys & new_keys) if canonical_event_repr(old_map[k]) != canonical_event_repr(new_map[k])
    )
    identity_collisions = len(rebuilt_1) - len(new_keys)

    # Monotonic & Transition Invariants
    half_order_monotonic = True
    outs_valid = True
    score_monotonic = True
    we_bounded = True

    prev_half_idx = -1
    prev_outs = 0
    prev_home = 0
    prev_away = 0

    for ev in rebuilt_1:
        # Half inning progression
        current_half_idx = half_inning_order(ev.get("inning"), ev.get("inning_half"))
        if current_half_idx < prev_half_idx:
            half_order_monotonic = False

        outs = ev.get("outs")
        if outs is None or outs < 0 or outs > 3:
            outs_valid = False

        # Inning boundary reset & non-decreasing outs within half
        if current_half_idx != prev_half_idx:
            prev_outs = 0
        elif outs is not None and outs < prev_outs:
            outs_valid = False

        if outs is not None:
            prev_outs = outs
        prev_half_idx = current_half_idx

        # Score monotonic progression
        h_score = ev.get("home_score") or 0
        a_score = ev.get("away_score") or 0
        if h_score < prev_home or a_score < prev_away:
            score_monotonic = False
        prev_home, prev_away = h_score, a_score

        # Win Expectancy bounds
        we_b = ev.get("win_expectancy_before")
        we_a = ev.get("win_expectancy_after")
        if we_b is not None and not (0.0 <= we_b <= 1.0):
            we_bounded = False
        if we_a is not None and not (0.0 <= we_a <= 1.0):
            we_bounded = False

    wpa_sum = round(sum(ev.get("wpa") or 0.0 for ev in rebuilt_1), 4)

    # Persistence Roundtrip Idempotency through ephemeral SQLite DB
    ephemeral_db_path = tmp_path / f"ephemeral_rebuild_{game_id}.db"
    e_url = f"sqlite:///{ephemeral_db_path}"
    e_engine = create_engine(e_url)
    Base.metadata.create_all(bind=e_engine)
    EphemeralSession = sessionmaker(bind=e_engine)

    with EphemeralSession() as es:
        if game:
            es.add(
                Game(
                    game_id=game.game_id,
                    game_date=game.game_date or date(2026, 4, 1),
                    home_team=game.home_team,
                    away_team=game.away_team,
                    game_status=game.game_status,
                    home_score=game.home_score,
                    away_score=game.away_score,
                )
            )
        orm_rows = _build_orm_events(game_id, rebuilt_1)
        es.add_all(orm_rows)
        es.commit()

    # Re-query from ephemeral DB and run Rebuild Pass 2
    with EphemeralSession() as es:
        persisted_events = (
            es.query(GameEvent)
            .filter(GameEvent.game_id == game_id)
            .order_by(GameEvent.event_seq.asc(), GameEvent.id.asc())
            .all()
        )
        rebuilt_2 = _rebuild_events_for_game(persisted_events, calculator=calc)
        pass2_hash = stream_sha256(rebuilt_2)

    persistence_roundtrip_idempotent = pass1_hash == pass2_hash

    return {
        "game_id": game_id,
        "category": category,
        "final_score": f"{game.away_score}:{game.home_score}" if game else "N/A",
        "old_event_count": len(old_payloads),
        "new_event_count": len(rebuilt_1),
        "added": added,
        "removed": removed,
        "modified": modified,
        "identity_collisions": identity_collisions,
        "old_sha256": old_hash,
        "pass1_sha256": pass1_hash,
        "pass2_sha256": pass2_hash,
        "full_contract_hash_equal": (old_hash == pass1_hash),
        "persistence_roundtrip_idempotent": persistence_roundtrip_idempotent,
        "half_order_monotonic": half_order_monotonic,
        "outs_valid": outs_valid,
        "score_monotonic": score_monotonic,
        "we_bounded": we_bounded,
        "wpa_sum": wpa_sum,
    }


def main() -> None:
    db_url = os.getenv("DATABASE_URL", "sqlite:///./data/kbo_dev.db")
    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    calc = WPACalculator()

    import tempfile

    tmp_dir = Path(tempfile.mkdtemp(prefix="gate_r1_audit_"))

    targets = [
        ("20260822HTWO0", "Standard 9-inning"),
        ("20260821KTSK0", "Extra innings (11th, DRAW)"),
        ("20260820OBNC0", "Walk-off"),
        ("20260821LGHH0", "High-scoring (11:15)"),
        ("20110402SSHT0", "Rain-shortened (5 innings)"),
        ("20250918LGKT1", "Doubleheader DH1"),
    ]

    results = []
    with Session() as s:
        for game_id, category in targets:
            res = audit_game_semantic_diff(game_id, category, s, calc, tmp_dir)
            results.append(res)

    print(json.dumps(results, indent=2))

    # Save to canonical evidence path
    evidence_dir = Path("Docs/certification/phase-106/gate-106f-relay")
    evidence_dir.mkdir(parents=True, exist_ok=True)
    out_file = evidence_dir / "semantic-diff-results.json"
    out_file.write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(f"\n[INFO] Semantic diff results saved to {out_file}")


if __name__ == "__main__":
    main()
