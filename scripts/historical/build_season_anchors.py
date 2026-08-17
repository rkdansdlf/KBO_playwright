"""1983-2000 KBO 시즌 앵커 빌더 (위키백과 팀 문서 시즌별 성적 표).

위키백과 팀 문서의 시즌별 성적 표는 문서/연도에 따라 행마다 셀 수가 다르다
(팀명/순위 셀의 rowspan 병합, 전기/후기 표기 등). 열 인덱스 대신 셀 내용으로
파싱한다:

  - 헤더 행: "연도 + 승 + 패"를 포함한 행.
  - 데이터 행: 첫 셀이 숫자(연도, 1982~2000).
  - 승률 셀: 소수 "0.xxx" 셀 — 승률 바로 앞의 연속 정수 셀을 오른쪽에서부터
    (패, 무, 승) 순으로 매핑하고, 4번째 정수가 있으면 경기수로 본다.
  - 예외: 쌍방울 1990 행은 2군리그 표이므로 제외 (1군 1991부터).

용례
----
  python3 -m scripts.historical.build_season_anchors            # 전체 수집·저장
  python3 -m scripts.historical.build_season_anchors --json     # 기계 판독 요약
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
import urllib.parse
import urllib.request

logger = logging.getLogger(__name__)
from pathlib import Path

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
# (문서명, 대상 연도 범위) — 태평양/현대 문서는 동일 표(리다이렉트)이므로
# 브랜드 시절에 맞게 연도 범위를 나눈다.
DOCS = [
    ("두산 베어스", (1982, 2000)),
    ("LG 트윈스", (1982, 2000)),
    ("KIA 타이거즈", (1982, 2000)),
    ("삼성 라이온즈", (1982, 2000)),
    ("롯데 자이언츠", (1982, 2000)),
    ("한화 이글스", (1986, 2000)),
    ("현대 유니콘스", (1996, 2000)),
    ("태평양 돌핀스", (1982, 1995)),
    ("쌍방울 레이더스", (1991, 2000)),
    ("SK 와이번스", (2001, 2000)),  # 시즌표가 2021+ — 2000 앵커는 시즌 문서로 보완
]
# SK 2000 보완: 시즌 문서 순위표 (팀별 순위표만 존재하는 시즌)
SEASON_DOC_SUPPLEMENT = {
    "2000": "2000년 한국프로야구",
}
REQUEST_DELAY_S = 1.2
OUTPUT = Path("data/archives/season_anchors_1983_2000.json")

PCT_RE = re.compile(r"0\.\d{1,3}")
INT_RE = re.compile(r"^(\d+)$")


def fetch(url: str) -> str:
    """위키백과 문서 HTML 수신."""
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.read().decode("utf-8")


def strip_tags(s: str) -> str:
    """HTML 태그/엔티티 제거."""
    s = re.sub(r"<[^>]+>", "", s)
    return s.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">").strip()


def parse_tables(html: str) -> list[list[list[str]]]:
    """2행 이상 표 파싱."""
    out = []
    for t in re.findall(r"<table[^>]*>(.*?)</table>", html, re.S):
        rows = []
        for r in re.findall(r"<tr[^>]*>(.*?)</tr>", t, re.S):
            cells = [strip_tags(c) for c in re.findall(r"<t[hd][^>]*>(.*?)</t[hd]>", r, re.S)]
            if cells:
                rows.append(cells)
        if rows:
            out.append(rows)
    return out


def _as_int(cell: str) -> int | None:
    """'43 (22/21)' 등 셀에서 첫 정수를 추출."""
    m = INT_RE.match(cell.strip())
    if m:
        return int(m.group(1))
    m = re.match(r"(\d+)\s*\(", cell.strip())
    return int(m.group(1)) if m else None


def extract_rows(t: list[list[str]], *, teamless: bool = False) -> list[dict]:  # noqa: C901
    """시즌 성적 표에서 (연도, 팀명 셀, 승/무/패/경기) 행들을 추출.

    teamless=True: 연도 컬럼이 없고 (순위, 팀명, ...) 형태인 시즌 문서 순위표.
    """
    head_idx: int | None = None
    for i, r in enumerate(t):
        joined = "|".join(r)
        if "승" in joined and "패" in joined and (teamless or "연도" in joined):
            head_idx = i
            break
    if head_idx is None:
        return []
    out: list[dict] = []
    for r in t[head_idx + 1 :]:
        if not r:
            continue
        # 연도 셀: 정규형이면 첫 셀, teamless는 "순위" 뒤 첫 숫자가 아니므로 0으로 표시 후 시즌 병합
        y_cell = _as_int(r[0]) if not teamless else None
        if not teamless and (not y_cell or not (1982 <= y_cell <= 2000)):
            continue
        # 승률 소수 셀 탐색 → 앞쪽 정수 셀 (패,무,승[,경기]) 매핑
        pct_idx = None
        for j, cell in enumerate(r):
            if PCT_RE.search(cell):
                pct_idx = j
                break
        if pct_idx is None:
            continue
        ints: list[int] = []
        for j in range(pct_idx - 1, 0, -1):
            v = _as_int(r[j])
            if v is None:
                break
            ints.append(v)
        if len(ints) < 3:
            continue
        if teamless:
            # 팀명 셀 (구단) 탐색: 승률 앞 정수 셀들 밖에서 알려진 팀명 찾기
            team_cell = ""
            for _j, cell in enumerate(r):
                c = cell.strip()
                if c in TEAM_CODE_BY_NAME:
                    team_cell = c
                    break
            rec = {"year": 0, "doc": "", "team_cell": team_cell}
        else:
            team_cell = r[1].strip() if len(r) > 1 else ""
            rec = {
                "year": y_cell,
                "doc": "",
                "team_cell": team_cell if team_cell in TEAM_CODE_BY_NAME else "",
            }
        if len(ints) >= 4:
            rec["games"] = ints[3]
        rec["w"] = ints[2]
        rec["d"] = ints[1]
        rec["l"] = ints[0]
        out.append(rec)
    return out


TEAM_CODE_BY_NAME = {
    "OB": "OB",
    "OB 베어스": "OB",
    "두산": "DB",
    "두산 베어스": "DB",
    "MBC": "MBC",
    "MBC 청룡": "MBC",
    "LG": "LG",
    "LG 트윈스": "LG",
    "해태": "HT",
    "해태 타이거즈": "HT",
    "KIA": "HT",
    "KIA 타이거즈": "HT",
    "삼성": "SS",
    "삼성 라이온즈": "SS",
    "롯데": "LT",
    "롯데 자이언츠": "LT",
    "삼미": "SM",
    "삼미 슈퍼스타즈": "SM",
    "청보": "CB",
    "청보 핀토스": "CB",
    "태평양": "TP",
    "태평양 돌핀스": "TP",
    "빙그레": "BE",
    "빙그레 이글스": "BE",
    "한화": "HH",
    "한화 이글스": "HH",
    "현대": "HU",
    "현대 유니콘스": "HU",
    "쌍방울": "SL",
    "쌍방울 레이더스": "SL",
    "SK": "SK",
    "SK 와이번스": "SK",
}


def season_code(team_name: str, year: int) -> str | None:
    """시즌 브랜드 코드 (OB↔두산 1999, MBC↔LG 1990, 빙그레↔한화 1994 등)."""
    code = TEAM_CODE_BY_NAME.get(team_name)
    if code is None:
        return None
    if code in ("OB", "DB"):
        return "DB" if year >= 1999 else "OB"
    if code in ("MBC", "LG"):
        return "LG" if year >= 1990 else "MBC"
    if code in ("BE", "HH"):
        return "HH" if year >= 1994 else "BE"
    if code in ("SM", "CB", "TP"):
        if year < 1986:
            return "SM"
        return "TP" if year >= 1988 else "CB"
    if code == "SL" and year < 1991:
        return None
    return code


def main(argv: list[str] | None = None) -> int:  # noqa: C901
    """CLI: 앵커 수집·정규화·저장."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    parser.add_argument("--json", action="store_true", help="기계 판독 출력")
    args = parser.parse_args(argv)

    raw: dict[str, list[dict]] = {}
    for doc, (y0, y1) in DOCS:
        if y1 < y0:
            continue  # SK 2001+ 범위는 사용 안 함 (2000 보완)
        url = "https://ko.wikipedia.org/wiki/" + urllib.parse.quote(doc)
        try:
            html = fetch(url)
        except Exception as exc:
            logger.exception("fetch error %s", doc)
            print(f"{doc}: fetch error {exc}")
            continue
        rows: list[dict] = []
        for t in parse_tables(html):
            for rec in extract_rows(t):
                if y0 <= rec["year"] <= y1:
                    rows.append(rec)
        raw[doc] = rows
        print(f"{doc}: {len(rows)} season rows")
        time.sleep(REQUEST_DELAY_S)

    # 시즌 문서 보완 (SK 2000 등 팀 문서에 없는 팀)
    for year_str, season_doc in SEASON_DOC_SUPPLEMENT.items():
        url = "https://ko.wikipedia.org/wiki/" + urllib.parse.quote(season_doc)
        try:
            html = fetch(url)
        except Exception as exc:
            logger.exception("fetch error %s", season_doc)
            print(f"{season_doc}: fetch error {exc}")
            continue
        supp: list[dict] = []
        for t in parse_tables(html):
            supp.extend(extract_rows(t, teamless=True))
        raw[f"{season_doc}#{year_str}"] = [dict(r, year=int(year_str)) for r in supp if r["team_cell"]]
        print(f"{season_doc}: {len(supp)} season rows (supplement)")
        time.sleep(REQUEST_DELAY_S)

    normalized: dict[str, dict] = {}
    for doc, rows in raw.items():
        for rec in rows:
            team_name = rec.get("team_cell") or doc
            code = season_code(team_name, rec["year"])
            if code is None:
                continue
            year_str = str(rec["year"])
            if code not in normalized.setdefault(year_str, {}):
                normalized[year_str][code] = {
                    "year": rec["year"],
                    "team": code,
                    "games": rec.get("games"),
                    "w": rec["w"],
                    "d": rec["d"],
                    "l": rec["l"],
                }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(normalized, ensure_ascii=False, indent=1, sort_keys=True),
        encoding="utf-8",
    )
    print(f"saved {args.output}")
    summary: dict[str, dict] = {}
    for year_str in sorted(normalized):
        teams = normalized[year_str]
        n = len(teams)
        games_sum = sum(v.get("games") or (v["w"] + v["d"] + v["l"]) for v in teams.values())
        summary[year_str] = {
            "teams": n,
            "games_sum": games_sum,
            "records": {c: [v["w"], v["d"], v["l"], v.get("games")] for c, v in sorted(teams.items())},
        }
        print(f"{year_str}: {n} teams, games_sum {games_sum}")
        print(
            "  "
            + ", ".join(
                f"{c}{v['w']}W-{v['d']}D-{v['l']}L{(gv and '(' + str(gv) + ')') or ''}"
                for c, v in sorted(teams.items())
                for gv in [v.get("games")]
            )
        )
    if args.json:
        print(json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
