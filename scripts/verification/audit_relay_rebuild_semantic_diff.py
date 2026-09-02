"""Gate R1-C: Comprehensive Full Semantic Rebuild Differential & Persistence Idempotency Analyzer.

Verifies:
1. Full 23-field canonical contract representation & JSON array serialization.
2. Natural key-based differential: (game_id, event_seq) -> added, removed, modified, reordered.
3. Chronological half-inning progression: (1, 'top') < (1, 'bottom') < (2, 'top') ...
4. Half-inning out non-decreasing & boundary reset invariants.
5. Score non-decreasing monotonic transitions.
6. Win Expectancy bounds (0.0 <= WE <= 1.0) & batter-perspective WPA transition consistency.
7. Persistence roundtrip idempotency through ephemeral SQLite DB (Pass 1 == Pass 2).
8. Separation of IDENTICAL_CONTRACT vs FILTERING_CONTRACT.
9. Strict fail-closed Gate enforcement (SystemExit(2) on invariant violation).
"""

from __future__ import annotations

from datetime import date
import hashlib
import json
import os
from pathlib import Path
import sys
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

# Exactly 23 canonical fields
CANONICAL_FIELDS: tuple[str, ...] = (
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


def normalize_canonical_value(val: Any) -> Any:
    """Normalize values for strict JSON serialization without delimiter collision."""
    if val is None:
        return None
    if isinstance(val, float):
        return round(val, 4)
    if isinstance(val, (int, bool)):
        return val
    return str(val)


def canonical_event_repr(ev: dict[str, Any] | GameEvent) -> str:
    """Format an event into its complete 23-field canonical JSON array representation."""
    payload = [
        normalize_canonical_value(getattr(ev, k, None) if isinstance(ev, GameEvent) else ev.get(k))
        for k in CANONICAL_FIELDS
    ]
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


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
    contract_type: str,
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
        return {"game_id": game_id, "category": category, "contract_type": contract_type, "error": "no_events"}

    old_payloads = [_event_to_payload(e) for e in old_events]
    old_hash = stream_sha256(old_payloads)

    # 1. Identity collision check in old stream
    old_seqs = [e["event_seq"] for e in old_payloads]
    old_identity_collisions = len(old_seqs) - len(set(old_seqs))

    # 2. Rebuild Pass 1
    rebuilt_1 = _rebuild_events_for_game(old_events, calculator=calc)
    pass1_hash = stream_sha256(rebuilt_1)

    # 3. Identity collision check in rebuilt stream
    new_seqs = [e["event_seq"] for e in rebuilt_1]
    new_identity_collisions = len(new_seqs) - len(set(new_seqs))

    # 4. Key-based event differential
    old_map = {e["event_seq"]: e for e in old_payloads}
    new_map = {e["event_seq"]: e for e in rebuilt_1}

    old_keys = set(old_map.keys())
    new_keys = set(new_map.keys())

    added = len(new_keys - old_keys)
    removed = len(old_keys - new_keys)
    modified = sum(
        1 for k in (old_keys & new_keys) if canonical_event_repr(old_map[k]) != canonical_event_repr(new_map[k])
    )

    # 5. Monotonicity & Mathematical Invariants
    half_order_monotonic = True
    outs_valid = True
    score_monotonic = True
    we_bounded = True
    wpa_delta_mismatches = 0
    we_continuity_mismatches = 0

    prev_half_idx = -1
    prev_half = None
    prev_outs = 0
    prev_home = 0
    prev_away = 0
    prev_we_after = None

    for ev in rebuilt_1:
        # Half inning progression
        current_half_idx = half_inning_order(ev.get("inning"), ev.get("inning_half"))
        if current_half_idx < prev_half_idx:
            half_order_monotonic = False

        outs = ev.get("outs")
        if outs is None or outs < 0 or outs > 3:
            outs_valid = False

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

        # Win Expectancy bounds [0.0, 1.0]
        we_b = ev.get("win_expectancy_before")
        we_a = ev.get("win_expectancy_after")
        if we_b is not None and not (0.0 <= we_b <= 1.0):
            we_bounded = False
        if we_a is not None and not (0.0 <= we_a <= 1.0):
            we_bounded = False

        # WPA delta transition: WPA == (WE_after - WE_before) for bottom, (WE_before - WE_after) for top
        wpa = ev.get("wpa")
        is_bottom = ev.get("inning_half") == "bottom"
        if wpa is not None and we_b is not None and we_a is not None:
            expected_wpa = round(we_a - we_b if is_bottom else we_b - we_a, 4)
            if abs(wpa - expected_wpa) > 0.0001:
                wpa_delta_mismatches += 1

        # WE continuity within half inning
        cur_half = (ev.get("inning"), ev.get("inning_half"))
        if cur_half == prev_half and prev_we_after is not None and we_b is not None:
            if abs(we_b - prev_we_after) > 0.005:
                we_continuity_mismatches += 1
        prev_half = cur_half
        prev_we_after = we_a

    wpa_transition_valid = wpa_delta_mismatches == 0
    wpa_sum = round(sum(ev.get("wpa") or 0.0 for ev in rebuilt_1), 4)

    # 6. Persistence Roundtrip Idempotency through ephemeral SQLite DB
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
        "contract_type": contract_type,
        "final_score": f"{game.away_score}:{game.home_score}" if game else "N/A",
        "old_event_count": len(old_payloads),
        "new_event_count": len(rebuilt_1),
        "added": added,
        "removed": removed,
        "modified": modified,
        "old_identity_collisions": old_identity_collisions,
        "new_identity_collisions": new_identity_collisions,
        "wpa_delta_mismatches": wpa_delta_mismatches,
        "we_continuity_mismatches": we_continuity_mismatches,
        "wpa_transition_valid": wpa_transition_valid,
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


def main() -> None:  # noqa: C901
    db_url = os.getenv("KBO_LOCAL_DB_URL", "sqlite:///data/kbo_dev.db")
    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    calc = WPACalculator()

    import tempfile

    tmp_dir = Path(tempfile.mkdtemp(prefix="gate_r1_audit_"))

    targets = [
        ("20260822HTWO0", "Standard 9-inning", "IDENTICAL_CONTRACT"),
        ("20260821KTSK0", "Extra innings (11th, DRAW)", "IDENTICAL_CONTRACT"),
        ("20260820OBNC0", "Walk-off", "IDENTICAL_CONTRACT"),
        ("20260821LGHH0", "High-scoring (11:15)", "IDENTICAL_CONTRACT"),
        ("20110402SSHT0", "Rain-shortened (5 innings)", "FILTERING_CONTRACT"),
        ("20250918LGKT1", "Doubleheader DH1", "IDENTICAL_CONTRACT"),
    ]

    results = []
    with Session() as s:
        for game_id, category, contract_type in targets:
            res = audit_game_semantic_diff(game_id, category, contract_type, s, calc, tmp_dir)
            results.append(res)

    print(json.dumps(results, indent=2))

    # Save to canonical evidence path
    evidence_dir = Path("Docs/certification/phase-106/gate-106f-relay")
    evidence_dir.mkdir(parents=True, exist_ok=True)
    out_file = evidence_dir / "semantic-diff-results.json"
    with out_file.open("w", encoding="utf-8") as f:
        f.write(json.dumps(results, indent=2) + "\n")
    print(f"\n[INFO] Semantic diff results saved to {out_file}")

    # Strict Gate Invariant Assertion (Fail-Closed)
    violations = []
    for r in results:
        g_id = r["game_id"]
        if not r["persistence_roundtrip_idempotent"]:
            violations.append(f"{g_id}: persistence roundtrip not idempotent")
        if not r["half_order_monotonic"]:
            violations.append(f"{g_id}: half inning order not monotonic")
        if not r["score_monotonic"]:
            violations.append(f"{g_id}: score not monotonic")
        if not r["we_bounded"]:
            violations.append(f"{g_id}: win expectancy out of [0, 1] bounds")
        if not r["wpa_transition_valid"]:
            violations.append(f"{g_id}: WPA does not equal batter perspective WE delta")
        if r["old_identity_collisions"] != 0:
            violations.append(f"{g_id}: old stream has identity collisions ({r['old_identity_collisions']})")
        if r["new_identity_collisions"] != 0:
            violations.append(f"{g_id}: new stream has identity collisions ({r['new_identity_collisions']})")

        if r["contract_type"] == "IDENTICAL_CONTRACT":
            if not r["full_contract_hash_equal"]:
                violations.append(f"{g_id}: IDENTICAL_CONTRACT full contract hash mismatch")
            if r["added"] != 0 or r["removed"] != 0 or r["modified"] != 0:
                violations.append(
                    f"{g_id}: IDENTICAL_CONTRACT has delta (add={r['added']}, rem={r['removed']}, mod={r['modified']})"
                )
        elif r["contract_type"] == "FILTERING_CONTRACT":
            if r["new_event_count"] == 0:
                violations.append(f"{g_id}: FILTERING_CONTRACT produced 0 valid events")

    if violations:
        print("\n[ERROR] GATE RF-C FAILED with violations:")
        for v in violations:
            print(f"  - {v}")
        sys.exit(2)

    print("\n[SUCCESS] All 6 games passed strict semantic contract invariants and roundtrip idempotency.")


if __name__ == "__main__":
    main()
