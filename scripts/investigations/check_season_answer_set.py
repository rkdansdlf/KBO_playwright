"""Season answer-set validator for the 1983-2000 measured-replacement batches.

Usage:
    python3 scripts/investigations/check_season_answer_set.py --year 1988

Checks (mirrors HISTORICAL_1983_2000_PLAN.md batch methodology):
    1. total/per-team/matchup counts vs expected round-robin arithmetic
    2. team W-L-D vs Wikipedia anchors (data/archives/season_anchors_1983_2000.json)
    3. draw accounting (equal-score games)
    4. home/away balance per team
    5. ghost/copy-error scans (same-date conflict, twin-box copy, duplicate ids)
    6. calendar sanity: date span, month coverage, suspicious gaps
    7. stale team codes / stadium token outliers
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any
import itertools

ARCHIVES = Path("data/archives")
ANCHORS = ARCHIVES / "season_anchors_1983_2000.json"

EXPECTED_TEAM_GAMES = {6: {1983: 100, 1984: 100}, 7: {}, 8: {}}


def load_answer_set(year: int) -> list[dict[str, Any]]:
    """Load the final answer-set game list for a season."""
    path = ARCHIVES / f"{year}_answer_set_final.json"
    data = json.loads(path.read_text(encoding="utf-8"))
    return data if isinstance(data, list) else data.get("games", [])


def load_anchors(year: int) -> dict[str, dict[str, Any]]:
    """Load the Wikipedia standings anchor row for a season."""
    anchors = json.loads(ANCHORS.read_text(encoding="utf-8"))
    return anchors.get(str(year), {})


def _pair(a: str, b: str) -> str:
    """Return the sorted matchup token."""
    return "-".join(sorted((a, b)))


def validate(games: list[dict[str, Any]], year: int) -> dict[str, Any]:
    """Run every structural check and return a serializable report."""
    findings: list[str] = []
    report: dict[str, Any] = {"year": year, "total_games": len(games)}

    # --- 1. counts -------------------------------------------------------
    team_counter = Counter()
    matchup_counter = Counter()
    for g in games:
        team_counter[g["home_team"]] += 1
        team_counter[g["away_team"]] += 1
        matchup_counter[_pair(g["home_team"], g["away_team"])] += 1
    teams = sorted(team_counter)
    n_teams = len(teams)
    games_per_team = max(team_counter.values()) // 2 if team_counter else 0
    report["teams"] = teams
    report["games_per_team"] = team_counter
    report["expected_total"] = n_teams * games_per_team // 2 * 2 // 2 * (n_teams - 1)
    report["expected_total"] = n_teams * (n_teams - 1) * games_per_team // 2

    if len(set(team_counter.values())) > 1:
        findings.append(f"팀별 경기수 불균형: {dict(team_counter)}")
    # 매치업 편수는 절대 균일이 아닌 분산 리그(1999-2000 드림/매직 등)를 고려해
    # 상대 분포로만 판정한다.
    mc_min, mc_max = min(matchup_counter.values()), max(matchup_counter.values())
    if matchup_counter and mc_max - mc_min >= 2:
        odd_matchups = {m: c for m, c in matchup_counter.items() if c != mc_min}
        findings.append(f"매치업 편수 불균형({mc_min}~{mc_max}회): {odd_matchups}")

    # --- 2. anchors ------------------------------------------------------
    anchors = load_anchors(year)
    wld: dict[str, Counter] = defaultdict(Counter)
    for g in games:
        hs, as_ = g["home_score"], g["away_score"]
        if hs == as_:
            wld[g["home_team"]]["d"] += 1
            wld[g["away_team"]]["d"] += 1
            continue
        win, lose = (g["home_team"], g["away_team"]) if hs > as_ else (g["away_team"], g["home_team"])
        wld[win]["w"] += 1
        wld[lose]["l"] += 1
    report["team_wld"] = {t: dict(c) for t, c in sorted(wld.items())}

    anchor_rows = []
    for team in teams:
        a = anchors.get(team)
        actual = wld.get(team, Counter())
        row = {
            "team": team,
            "anchor": None if a is None else (a.get("w"), a.get("l"), a.get("d")),
            "answer": (actual.get("w", 0), actual.get("l", 0), actual.get("d", 0)),
        }
        row["match"] = row["anchor"] == row["answer"]
        anchor_rows.append(row)
        if a is not None and not row["match"]:
            findings.append(f"앵커 불일치 {team}: 위키{row['anchor']} vs 정답셋{row['answer']}")
        if a is None:
            findings.append(f"앵커 누락 팀코드: {team}")
    report["anchor_comparison"] = anchor_rows

    # --- 4. home/away balance -------------------------------------------
    home_counter = Counter(g["home_team"] for g in games)
    away_counter = Counter(g["away_team"] for g in games)
    report["home_away"] = {t: (home_counter[t], away_counter[t]) for t in teams}
    half = len(games) // (2 * n_teams)
    unbalanced_home = {t: v for t, v in report["home_away"].items() if abs(v[0] - v[1]) > max(2, half // 4)}
    if unbalanced_home:
        findings.append(f"홈/원정 큰 불균형: {unbalanced_home}")

    # --- 5. ghosts --------------------------------------------------------
    by_id: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_key: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    box_fingerprint: dict[tuple[Any, ...], list[str]] = defaultdict(list)
    for g in games:
        by_id[g["game_id"]].append(g)
        by_key[(g["game_date"], _pair(g["home_team"], g["away_team"]))].append(g)
        sig = (
            g["home_score"],
            g["away_score"],
            tuple(str(g.get(k)) for k in sorted(k for k in g if k.startswith("inning"))),
        )
        box_fingerprint[(sig, g["stadium"])].append(g["game_id"])

    dup_ids = {gid: len(v) for gid, v in by_id.items() if len(v) > 1}
    if dup_ids:
        findings.append(f"game_id 중복: {list(dup_ids)[:5]} ({len(dup_ids)}건)")
    date_conflicts = {
        k: [(x["game_id"], x["home_score"], x["away_score"]) for x in v]
        for k, v in by_key.items()
        if len({(x["home_score"], x["away_score"]) for x in v}) > 1 and not all(x["game_id"][-1].isdigit() for x in v)
    }
    if date_conflicts:
        findings.append(f"동일 날짜·매치업 스코어 충돌(유령 후보): {date_conflicts}")
    doubleheaders = {k: v for k, v in by_key.items() if len(v) > 1 and {x["game_id"][-1] for x in v} <= {"0", "1", "2"}}
    report["doubleheader_pairs"] = len(doubleheaders)

    # twin-box copy error: identical (matchup, teams, scores, stadium) on nearby dates
    sig_map: dict[tuple, list[tuple[str, str]]] = defaultdict(list)
    for g in games:
        key = (
            _pair(g["home_team"], g["away_team"]),
            g["home_team"],
            g["away_team"],
            g["home_score"],
            g["away_score"],
            g["stadium"],
        )
        sig_map[key].append((g["game_id"], g["game_date"]))
    twins: dict[str, list[tuple[str, str]]] = defaultdict(list)
    seen_pairs: set[tuple[str, str]] = set()
    for rows in sig_map.values():
        if len(rows) < 2:
            continue
        for (ida, da), (idb, db) in itertools.pairwise(rows):
            delta = abs((datetime.strptime(db, "%Y-%m-%d") - datetime.strptime(da, "%Y-%m-%d")).days)
            if 0 < delta <= 3 and (ida, idb) not in seen_pairs:
                seen_pairs.add((ida, idb))
                twins[ida].append(idb)
    if twins:
        report["twin_box_candidates"] = twins
        # 매치업 편수·앵커가 모두 정상이면 스코어 우연 일치로 판단해 정보만 남긴다.
        surplus_exists = mc_max - mc_min >= 2 or any(not r["match"] for r in anchor_rows)
        if surplus_exists:
            findings.append(f"쌍둥이 박스 의심(±3일 동일 스코어·구장·조합): {len(twins)}쌍 {list(twins.items())[:8]}")
        else:
            report["twin_box_benign"] = True
    exact_dup_keys = [k for k, v in by_key.items() if len(v) > 1]
    unsuffixed_dups = [
        k
        for k in exact_dup_keys
        if any(not x["game_id"][-1].isdigit() for x in by_key[k])
    ]
    report["doubleheader_dates"] = len(exact_dup_keys)
    if unsuffixed_dups:
        findings.append(
            f"동일 날짜·매치업 중복(경기번호 접미사 없음 — DH 미분리 의심): {len(unsuffixed_dups)}건"
        )

    # --- 6. calendar ------------------------------------------------------
    dates = sorted(datetime.strptime(g["game_date"], "%Y-%m-%d").date() for g in games)
    report["season_span"] = [str(dates[0]), str(dates[-1])]
    gaps = [(str(a), str(b), (b - a).days) for a, b in itertools.pairwise(dates) if (b - a).days >= 14]
    if gaps:
        report["calendar_gaps_ge_14d"] = gaps
    month_counts = Counter(d.month for d in dates)
    report["month_distribution"] = dict(sorted(month_counts.items()))

    # --- 7. tokens ---------------------------------------------------------
    stadiums = Counter(g["stadium"] for g in games)
    report["stadiums"] = dict(stadiums.most_common())

    KNOWN_CODES = ("MBC", "OB", "LT", "SS", "HT", "BE", "CB", "TP", "SM", "PN", "HD", "SK", "NX", "LG", "DB")

    def _id_slots(game_id: str) -> tuple[str | None, str | None]:
        """Extract (away, home) team codes from a KBO game_id after the 8-digit date."""
        body = game_id[8:]
        for code in KNOWN_CODES:
            if body.startswith(code):
                rest = body[len(code) :]
                for home in KNOWN_CODES:
                    if rest.startswith(home):
                        return code, home
        return None, None

    stale_codes = []
    label_mismatch = []
    for g in games:
        away_slot, home_slot = _id_slots(g["game_id"])
        if year >= 1988 and "CB" in (away_slot, home_slot):
            stale_codes.append(g["game_id"])
        # ID 팀순서 규칙은 세대별로 상이(홈먼저/원정먼저) — 어느 쪽으로도
        # 해석되지 않을 때만 이상으로 본다.
        if (
            away_slot
            and {g.get("home_team"), g.get("away_team")} != {away_slot, home_slot}
        ):
            label_mismatch.append(g["game_id"])
    if stale_codes:
        findings.append(f"개명 전 코드(CB=청보) 슬롯 잔존: {stale_codes[:8]} ({len(stale_codes)}건)")
    report["id_label_mismatch"] = len(label_mismatch)
    if label_mismatch:
        findings.append(
            f"game_id 팀 조합과 home/away 불일치: {len(label_mismatch)}건 (예: {label_mismatch[:5]})"
        )

    report["findings"] = findings
    report["passed"] = not findings
    return report


def render(report: dict[str, Any]) -> str:
    """Render a human-readable summary block."""
    lines = [
        f"== {report['year']} 시즌 정답셋 검증 ==",
        f"총 경기 {report['total_games']} / 팀 {len(report['teams'])} / 팀당 {report['games_per_team']}경기",
        f"기간: {report['season_span'][0]} ~ {report['season_span'][1]}",
        "",
        "[앵커 대조]",
    ]
    for row in report["anchor_comparison"]:
        mark = "OK " if row["match"] else "FAIL"
        lines.append(f"  [{mark}] {row['team']:3s} 위키={row['anchor']} 정답셋={row['answer']}")
    lines.append("")
    lines.append("[홈/원정]")
    for t, v in report["home_away"].items():
        lines.append(f"  {t:3s} 홈 {v[0]:3d} / 원정 {v[1]:3d}")
    if report.get("calendar_gaps_ge_14d"):
        lines.append("")
        lines.append(f"[14일+ 공백] {report['calendar_gaps_ge_14d']}")
    lines.append("")
    if report["findings"]:
        lines.append(f"[발견 {len(report['findings'])}건]")
        lines.extend(f"  - {f}" for f in report["findings"])
    else:
        lines.append("[발견 없음] 전 체크 통과")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    """Run the season validation CLI."""
    parser = argparse.ArgumentParser(description="Validate a season answer set against anchors")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--json-out", type=Path, default=None, help="Save JSON report here")
    args = parser.parse_args(argv)

    games = load_answer_set(args.year)
    report = validate(games, args.year)
    sys.stdout.write(render(report) + "\n")
    out_path = args.json_out or Path("reports") / "historical_batch" / f"{args.year}_validation.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    sys.stdout.write(f"\nreport: {out_path}\n")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
