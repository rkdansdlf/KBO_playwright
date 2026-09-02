"""Gate R1: Semantic Rebuild Differential Analyzer.

Audits semantic equivalence, transitions, WPA delta, and idempotency across representative game types.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

from src.cli.backfill.rebuild_relay_events import (
    _event_to_payload,
    _rebuild_events_for_game,
)
from src.db.engine import SessionLocal
from src.models.game import Game, GameEvent
from src.services.wpa_calculator import WPACalculator


def canonical_event_repr(ev: dict[str, Any]) -> str:
    """Generate deterministic string for an event."""
    keys = [
        "inning",
        "inning_half",
        "outs",
        "batter_id",
        "pitcher_id",
        "event_type",
        "result_code",
        "home_score",
        "away_score",
        "base_state",
        "wpa",
    ]
    vals = [str(ev.get(k)) for k in keys]
    return "|".join(vals)


def canonical_stream_hash(events: list[dict[str, Any]]) -> str:
    """Hash the entire sequence of events."""
    h = hashlib.sha256()
    for ev in events:
        h.update(canonical_event_repr(ev).encode("utf-8"))
    return h.hexdigest()


def analyze_game_semantic_diff(game_id: str, calculator: WPACalculator) -> dict[str, Any]:
    with SessionLocal() as session:
        game = session.query(Game).filter(Game.game_id == game_id).first()
        raw_events = (
            session.query(GameEvent)
            .filter(GameEvent.game_id == game_id)
            .order_by(GameEvent.event_seq.asc(), GameEvent.id.asc())
            .all()
        )
        if not raw_events:
            return {"game_id": game_id, "error": "no_events_found"}

        old_payloads = [_event_to_payload(e) for e in raw_events]
        old_hash = canonical_stream_hash(old_payloads)

        # First rebuild pass
        rebuilt_pass1 = _rebuild_events_for_game(raw_events, calculator=calculator)
        pass1_hash = canonical_stream_hash(rebuilt_pass1)

        # Invariant checks on pass1
        inning_monotonic = True
        outs_valid = True
        prev_inn = 0
        wpa_sum = 0.0

        for ev in rebuilt_pass1:
            inn = int(ev.get("inning") or 0)
            if inn < prev_inn:
                inning_monotonic = False
            prev_inn = inn

            outs = ev.get("outs")
            if outs is not None and (outs < 0 or outs > 3):
                outs_valid = False

            wpa = ev.get("wpa")
            if wpa is not None:
                wpa_sum += float(wpa)

        # Idempotency check: Second rebuild pass on pass1
        # Mock GameEvent objects from pass1 dicts to pass to _rebuild_events_for_game
        mock_events = [GameEvent(**{k: v for k, v in ev.items() if hasattr(GameEvent, k)}) for ev in rebuilt_pass1]
        rebuilt_pass2 = _rebuild_events_for_game(mock_events, calculator=calculator)
        pass2_hash = canonical_stream_hash(rebuilt_pass2)

        idempotent = (pass1_hash == pass2_hash) and (len(rebuilt_pass1) == len(rebuilt_pass2))

        # Diff metrics
        added = max(0, len(rebuilt_pass1) - len(old_payloads))
        removed = max(0, len(old_payloads) - len(rebuilt_pass1))
        content_exact = sum(
            1
            for o, r in zip(old_payloads, rebuilt_pass1, strict=True)
            if canonical_event_repr(o) == canonical_event_repr(r)
        )

        return {
            "game_id": game_id,
            "final_score": f"{game.away_score}:{game.home_score}" if game else "unknown",
            "old_event_count": len(old_payloads),
            "new_event_count": len(rebuilt_pass1),
            "identity_exact": content_exact,
            "ordered_hash_equal": (old_hash == pass1_hash),
            "old_hash": old_hash[:16],
            "pass1_hash": pass1_hash[:16],
            "pass2_hash": pass2_hash[:16],
            "second_rebuild_idempotent": idempotent,
            "added": added,
            "removed_noise": removed,
            "inning_monotonic": inning_monotonic,
            "outs_valid": outs_valid,
            "wpa_sum": round(wpa_sum, 4),
        }


def main():
    calc = WPACalculator()
    target_games = [
        ("Standard 9-inning", "20260822HTWO0"),
        ("Extra innings (11th, DRAW)", "20260821KTSK0"),
        ("Walk-off", "20260820OBNC0"),
        ("High-scoring (11:15)", "20260821LGHH0"),
        ("Doubleheader DH1", "20250918LGKT1"),
        ("Doubleheader DH2", "20250918LGKT2"),
    ]

    results = []
    for label, gid in target_games:
        analysis = analyze_game_semantic_diff(gid, calc)
        analysis["category"] = label
        results.append(analysis)

    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
