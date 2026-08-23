"""Scan a season's namu raw boxes for inter-doc conflicts and date-typo twins.

Usage:
    python3 scripts/investigations/probe_season_conflicts.py --year 1992

Prints, per matchup:
    - CONFLICT: same date+matchup reported by two docs with different scores
      (score-typo / copy-forward candidates — merge counts them as extra games)
    - CROSS-DATE TWIN: identical box signature under different dates across docs
      (date-typo ghosts)
    - MULTI: dates whose box count exceeds one-per-doc (real doubleheaders keep
      both docs in agreement)
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

RAW_DIR = Path("data/archives")


def main(argv: list[str] | None = None) -> int:
    """Run the conflict scanner CLI."""
    parser = argparse.ArgumentParser(description="Scan namu raw boxes for conflicts/twins")
    parser.add_argument("--year", type=int, required=True)
    args = parser.parse_args(argv)

    raw = json.loads((RAW_DIR / f"{args.year}_namu_raw.json").read_text(encoding="utf-8"))
    by_date_pair: dict[tuple[str, tuple[str, str]], dict[str, set[tuple]]] = defaultdict(lambda: defaultdict(set))
    sig_boxes: dict[tuple, list[dict]] = defaultdict(list)

    def pair_of(g: dict) -> tuple[str, str]:
        return tuple(sorted((g["team1"], g["team2"])))

    for g in raw:
        p = pair_of(g)
        sig = (g["team1"], g["score1"], g["score2"], g["team2"], g["stadium"])
        by_date_pair[(g["date"], p)][g["team_doc"]].add(sig)
        sig_boxes[(p,) + sig].append(g)

    # CONFLICT: 같은 날짜·매치업에서 문서별 버전 집합이 서로 다른 경우
    # 분류: STADIUM(구장 명칭 변형만), SWAP(팀 라벨 교차), SCORE(스코어 불일치),
    #       DOCS(한쪽 문서 박스 수 부족)
    conflicts = []
    for (date, p), docs in sorted(by_date_pair.items()):
        versions = [frozenset(b) for b in docs.values()]
        if len(versions) > 1 and len(set(versions)) > 1:
            conflicts.append((date, p, docs))
    for date, p, docs in conflicts:
        union = set().union(*docs.values())
        all_stadia = {v[4] for v in union}
        scores_per_doc = [frozenset((v[1], v[2]) for v in docs[d]) for d in docs]
        teams_canonical = {tuple(sorted((v[0], v[3]))) for v in union}
        if len(all_stadia) > 1 and len(scores_per_doc) == 1 and len(teams_canonical) == 1 and all(
            len(s) == len(docs[d]) for s, d in zip(scores_per_doc, docs)
        ):
            kind = "STADIUM"
            sys.stdout.write(f"STADIUM {args.year}-{date} {'-'.join(p)} :: {sorted(all_stadia)}\n")
            continue
        kinds = []
        if len(set(scores_per_doc)) == 1 and len(teams_canonical) == 1:
            pass  # 스코어·조합 동일 → 아래 박스수 편차로 판단
        else:
            label_sets = [frozenset((v[0], v[3]) for v in docs[d]) for d in docs]
            swapped = (
                len(label_sets) == 2
                and len(label_sets[0]) == 1
                and label_sets[0] == label_sets[1]
                and len(set(scores_per_doc)) == 1
            )
            kinds.append("SWAP" if swapped else "SCORE")
        counts = {d: len(docs[d]) for d in docs}
        if len(set(counts.values())) > 1:
            kinds.append(f"DOCS{sorted(counts.items())}")
        tag = "+".join(kinds) or "DOCS-EQ"
        sys.stdout.write(f"{tag} {args.year}-{date} {'-'.join(p)}\n")
        for doc in sorted(docs):
            for v in sorted(docs[doc]):
                sys.stdout.write(f"   [{doc}] {v[0]} {v[1]}:{v[2]} {v[3]} @ {v[4]}\n")

    # TWIN(날짜 오기 유령 후보): 동일 시그니처 박스가 정확히 2개이면서
    # 서로 다른 문서·다른 날짜에 하나씩만 존재하는 경우
    twins = []
    for (p, t1, s1, s2, t2, stadium), boxes in sig_boxes.items():
        if len(boxes) != 2:
            continue
        dts = {b["date"] for b in boxes}
        docs = {b["team_doc"] for b in boxes}
        if len(dts) == 2 and len(docs) == 2:
            twins.append((sorted(dts), "-".join(p), boxes))
    for dts, pair_name, boxes in sorted(twins):
        detail = " | ".join(f"{b['date']}({b['team_doc']})" for b in boxes)
        sys.stdout.write(f"TWIN {'/'.join(dts)} {pair_name} :: {t1} {s1}:{s2} {t2} @ {stadium} [{detail}]\n")

    sys.stdout.write(
        f"\nsummary: conflicts={len(conflicts)} twins={len(twins)} boxes={len(raw)}\n"
    )
    # SINGLE: 한쪽 문서에만 존재하는 날짜·매치업 (상대 월문서 결손이면 정상)
    single_docs = defaultdict(list)
    for (date, p), docs in by_date_pair.items():
        if len(docs) == 1:
            (doc,) = docs.keys()
            single_docs[doc].append(date)
    for doc, dates in sorted(single_docs.items()):
        sys.stdout.write(f"SINGLE {doc}: {len(dates)}건 {sorted(dates)[:8]}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
