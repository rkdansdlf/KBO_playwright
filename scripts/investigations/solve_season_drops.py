"""Search DROP combinations that make a season's merged result match its anchors.

Usage:
    python3 scripts/investigations/solve_season_drops.py --year 1990 [--max-drop 2]

Tries every conflicting/twin raw box (and small combinations) through the real
pipeline (drop -> merge -> fixes -> finalize -> record check) and reports the
combinations that satisfy all Wikipedia anchors.
"""

from __future__ import annotations

import argparse
import io
import json
import sys
from contextlib import redirect_stdout
from itertools import combinations
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.historical.namu_season_boxscores import (  # noqa: E402
    ANCHORS,
    RAW_DIR,
    apply_score_fixes,
    apply_stadium_fixes,
    finalize_games,
    merge_games,
)


def _records_ok(games: list[dict], year: int, anchors: dict) -> bool:
    """Return whether team W-L-D matches anchors (null fields wildcarded)."""
    tr: dict[str, list[int]] = {}
    for g in games:
        for t in (g["home_team"], g["away_team"]):
            tr.setdefault(t, [0, 0, 0])
        hs, as_ = g["home_score"], g["away_score"]
        if hs == as_:
            tr[g["home_team"]][2] += 1
            tr[g["away_team"]][2] += 1
            continue
        win, lose = (
            (g["home_team"], g["away_team"]) if hs > as_ else (g["away_team"], g["home_team"])
        )
        tr[win][0] += 1
        tr[lose][1] += 1
    for code, v in tr.items():
        a = anchors.get(code)
        if not a:
            continue
        exp_w, exp_l, exp_d = a.get("w"), a.get("l"), a.get("d")
        if exp_w is not None and v[0] != exp_w:
            return False
        if exp_l is not None and v[1] != exp_l:
            return False
        if exp_d is not None and v[2] != exp_d:
            return False
    return True


def _build(raw: list[dict], year: int) -> list[dict]:
    """Run raw boxes through merge and fixes without printing."""
    with redirect_stdout(io.StringIO()):
        games = merge_games(raw)
        final = finalize_games(apply_stadium_fixes(apply_score_fixes(games, year), year), year)
    return final


def _apply_ops(raw: list[dict], ops: list[tuple[dict, str]]) -> list[dict]:
    """Apply (box, mode) operations: 'drop' removes, 'draw' forces a tie."""
    ids = {id(b): mode for b, mode in ops}
    out = []
    for b in raw:
        mode = ids.get(id(b))
        if mode == "drop":
            continue
        g = dict(b)
        if mode == "draw":
            mid = (g["score1"] + g["score2"]) // 2
            g["score1"] = mid
            g["score2"] = mid
        out.append(g)
    return out


def main(argv: list[str] | None = None) -> int:
    """Run the drop-combination solver CLI."""
    parser = argparse.ArgumentParser(description="Solve DROP combinations for a season")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--max-drop", type=int, default=2)
    parser.add_argument(
        "--include-single",
        action="store_true",
        help="한쪽 문서에만 존재하는 박스도 후보에 포함 (초과 시즌의 단일문서 유령 탐지)",
    )
    args = parser.parse_args(argv)

    anchors = json.loads(ANCHORS.read_text(encoding="utf-8")).get(str(args.year), {})
    raw_path = RAW_DIR / f"{args.year}_namu_raw.json"
    raw = json.loads(raw_path.read_text(encoding="utf-8"))

    # 후보 박스 식별: 같은 (date,pair)에서 문서 간 버션이 어긋나는 박스들
    from collections import defaultdict

    grouped: dict[tuple, dict[str, list[dict]]] = defaultdict(lambda: defaultdict(list))
    for g in raw:
        key = (g["date"], tuple(sorted((g["team1"], g["team2"]))))
        grouped[key][g["team_doc"]].append(g)

    candidates: list[dict] = []
    for _key, docs in grouped.items():
        sig_sets = [frozenset((b["team1"], b["score1"], b["score2"], b["team2"], b["stadium"]) for b in v) for v in docs.values()]
        if len(docs) > 1 and len(set(sig_sets)) > 1:
            for v in docs.values():
                candidates.extend(v)
        elif args.include_single:
            candidates.extend(next(iter(docs.values())))

    baseline_ok = _records_ok(_build(raw, args.year), args.year, anchors)
    if baseline_ok:
        sys.stdout.write("baseline already OK\n")
        return 0

    found: list[tuple[str, ...]] = []
    for r in range(1, args.max_drop + 1):
        for combo in combinations(candidates, r):
            ids = [id(b) for b in combo]
            trimmed = [b for b in raw if id(b) not in ids]
            if _records_ok(_build(trimmed, args.year), args.year, anchors):
                label = tuple(
                    f"{b['date']}|{b['team_doc']}|{b['team1']}{b['score1']}:{b['score2']}{b['team2']}"
                    for b in combo
                )
                found.append(label)
        if found:
            break

    if not found:
        # 드롭만으로 불가하면 무승부 변환을 혼합한 3연산 탐색
        mismatched = set()
        base_final = _build(raw, args.year)
        for g in base_final:
            pass
        tr: dict[str, list[int]] = {}
        for g in base_final:
            for t in (g["home_team"], g["away_team"]):
                tr.setdefault(t, [0, 0, 0])
            hs, as_ = g["home_score"], g["away_score"]
            if hs == as_:
                tr[g["home_team"]][2] += 1
                tr[g["away_team"]][2] += 1
                continue
            win, lose = (
                (g["home_team"], g["away_team"]) if hs > as_ else (g["away_team"], g["home_team"])
            )
            tr[win][0] += 1
            tr[lose][1] += 1
        for code, v in tr.items():
            a = anchors.get(code) or {}
            if (a.get("w") is not None and v[0] != a["w"]) or (a.get("l") is not None and v[1] != a["l"]) or (
                a.get("d") is not None and v[2] != a["d"]
            ):
                mismatched.add(code)
        ops: list[tuple[dict, str]] = []
        for b in candidates:
            if mismatched and b["team1"] not in mismatched and b["team2"] not in mismatched:
                continue
            ops.append((b, "drop"))
            if b["score1"] != b["score2"]:
                ops.append((b, "draw"))
        import itertools as _it

        hits = []
        for trio in _it.combinations(ops, 3):
            trial = _apply_ops(raw, list(trio))
            if _records_ok(_build(trial, args.year), args.year, anchors):
                label = tuple(
                    f"{b['date']}|{b['team_doc']}|{mode}:{b['team1']}{b['score1']}:{b['score2']}{b['team2']}"
                    for b, mode in trio
                )
                hits.append(label)
                if len(hits) >= 5:
                    break
        if not hits:
            sys.stdout.write(
                f"NO SOLUTION (drop<=2 / mixed-3) — 외부 증거 필요 (ops={len(ops)})\n"
            )
            return 1
        for solution in hits:
            for entry in solution:
                sys.stdout.write(f"OP {entry}\n")
            sys.stdout.write("---\n")
        return 0

    for solution in found[:10]:
        for entry in solution:
            sys.stdout.write(f"DROP {entry}\n")
        sys.stdout.write("---\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
