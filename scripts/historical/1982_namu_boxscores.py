"""1982 KBO 1군 시즌 실측 경기결과 수집 파이프라인 (나무위키 팀별 월별 문서).

개요
----
1982 시즌(팀당 80경기, 6팀)의 경기별 스코어는 KBO 공식 채널(2000년 이전)과
스탯티즈/위키백과에서 확인할 수 없어, 나무위키 팀별 월별 문서의 박스스코어를
단일 실측 소스로 사용한다.

  - 문서: https://namu.moe/w/{팀 문서명}/1982년/{3~4월|5월|6월|7월|8월|9~10월}
  - 예외: OB 베어스는 "1982년/9~10월" 문서가 없어 "1982년/9월" 단일 문서 사용.
  - 문서당 표 4~: "3월 27일, 동대문야구장" 헤더의 박스스코어 (이닝별 + R/H/E/B).
  - 캘린더 표(표 3)는 날짜 시프트(박스 대비 1일 이르게)와 스코어 방향 혼재로
    신뢰하지 않으며, 박스스코어의 날짜/스코어를 진실로 취급한다.

알려진 원본 오기와 처리
------------------------
1. 해태 4/8 박스(10:2 구덕)는 4/14 동대문 경기의 이닝 라인이 복사된 오기.
   실측은 삼미 박스가 보존한 "4/8 구덕 해태 7:4"(방수원 1-1 승). → `GHOSTS` 제거.
2. 8/5 무승부(7:7, MB:HT 무등)의 재경기 박스는 헤더가 "8월 5일, 8월 18일,
   동대문야구장"으로 복수 날짜 표기. → `REPLAY_CORRECTIONS`로 8/18로 이동.

사용법
------
  python3 -m scripts.historical.1982_namu_boxscores --crawl   # 네트워크 전체 수집
  python3 -m scripts.historical.1982_namu_boxscores            # 로컬 raw로 재현/검증
  python3 -m scripts.historical.1982_namu_boxscores --verify-only
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
from collections import Counter, defaultdict
from pathlib import Path

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
TEAMS = {
    "OB": "OB 베어스",
    "MB": "MBC 청룡",
    "HT": "해태 타이거즈",
    "SS": "삼성 라이온즈",
    "LT": "롯데 자이언츠",
    "SM": "삼미 슈퍼스타즈",
}
MONTH_DOCS = ["3~4월", "5월", "6월", "7월", "8월", "9~10월"]
OB_SEPTEMBER_ONLY = True  # OB "9~10월" 404 → "9월" 단일 문서
REQUEST_DELAY_S = 1.5

STADIUM_RE = re.compile(r"([가-힣A-Za-z0-9]+(?:\s+[가-힣A-Za-z0-9]+)*(?:야구장|구장))")
DATE_RE = re.compile(r"(\d{1,2})월 (\d{1,2})일")

# 재경기 박스: 헤더 "8월 5일, 8월 18일, 동대문야구장"에서 첫 날짜만 파싱됨.
# 첫 날짜는 원 경기(무승부), 둘째가 재경기 날짜. 무승부 박스(무등 7:7)는 유지.
REPLAY_CORRECTIONS = {
    "08-05": ("08-18", "MB", "HT", 8, 7, "동대문야구장"),  # 8/5 무승부 7:7의 재경기
}
# 해태 4/8 박스가 4/14 경기 복사 오기인 유령 경기 (date, team1, score1, team2, score2)
GHOSTS = [
    {"date": "04-08", "team1": "HT", "score1": 10, "team2": "SM", "score2": 2},
]

DEFAULT_RAW = Path("data/archives/1982_namu_raw.json")
DEFAULT_OUTPUT = Path("data/archives/1982_answer_set_final.json")

# 위키백과 1982 최종 순위 앵커 (승/패, 무승부는 순위표에 미기재)
WIKI_WINS = {"OB": 56, "SS": 54, "MB": 46, "HT": 38, "LT": 31, "SM": 15}
WIKI_LOSSES = {"OB": 24, "SS": 26, "MB": 34, "HT": 42, "LT": 49, "SM": 65}

# 홈/원정 판별: 경기장 소유 팀. 순회(중립) 경기장은 매치업 순서를 그대로 유지.
HOME_BY_STADIUM = {
    "구덕 야구장": "LT",
    "대구시민운동장 야구장": "SS",
    "무등 야구장": "HT",
    "숭의야구장": "SM",
    "춘천공설운동장 야구장": "SM",
    "전주종합경기장 야구장": "HT",
    "마산 야구장": "LT",
    "청주 야구장": "OB",
    "한밭 야구장": "OB",
    "한밭종합운동장 야구장": "OB",
    "동대문야구장": "MB",
    "서울종합운동장 야구장": "OB",
}
STADIUM_SHORT = {
    "구덕 야구장": "부산",
    "대구시민운동장 야구장": "대구",
    "무등 야구장": "광주",
    "숭의야구장": "인천",
    "춘천공설운동장 야구장": "춘천",
    "전주종합경기장 야구장": "전주",
    "마산 야구장": "마산",
    "청주 야구장": "청주",
    "한밭 야구장": "대전",
    "한밭종합운동장 야구장": "대전",
    "동대문야구장": "동대문",
    "서울종합운동장 야구장": "잠실",
}


def fetch(url: str) -> str:
    """나무위키 문서 HTML을 받아온다."""
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.read().decode("utf-8")


def doc_url(team_doc: str, month: str) -> str:
    """팀별 월별 문서 URL."""
    return "https://namu.moe/w/" + urllib.parse.quote(f"{team_doc}/1982년/{month}")


def try_doc_url(team_doc: str, month: str) -> str:
    """OB는 9~10월 문서 대신 9월 단일 문서를 쓴다."""
    if OB_SEPTEMBER_ONLY and team_doc == "OB 베어스" and month == "9~10월":
        return doc_url(team_doc, "9월")
    return doc_url(team_doc, month)


def strip_tags(s: str) -> str:
    """HTML 태그 제거 + 기본 엔티티 디코드."""
    s = re.sub(r"<[^>]+>", "", s)
    return s.replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&").strip()


def parse_tables(html: str) -> list[list[list[str]]]:
    """문서의 2행 이상 표들을 파싱."""
    tables = []
    for t in re.findall(r"<table[^>]*>(.*?)</table>", html, re.S):
        rows = []
        for r in re.findall(r"<tr[^>]*>(.*?)</tr>", t, re.S):
            cells = [strip_tags(c) for c in re.findall(r"<td[^>]*>(.*?)</td>", r, re.S)]
            if cells:
                rows.append(cells)
        if len(rows) >= 2:
            tables.append(rows)
    return tables


def code_of(name: str) -> str | None:
    """박스의 팀 셀 문자열 → 팀 코드 (짧은 이름 포함 매칭)."""
    for k, v in TEAMS.items():
        if name == v or name in v or v in name:
            return k
    return None


def parse_boxes(tables: list[list[list[str]]], month_label: str) -> list[dict]:
    """박스스코어 표들에서 (date, stadium, team1/2, score1/2) 추출."""
    games = []
    for rows in tables:
        if len(rows) < 3:
            continue
        head = rows[0][0]
        dm = DATE_RE.search(head)
        if not dm:
            continue
        header = rows[1]
        if "R" not in header:
            continue
        r_idx = header.index("R")
        entries = []
        for r in rows[2:]:
            code = code_of(r[0])
            if code is None or r_idx >= len(r):
                continue
            val = r[r_idx].replace("X", "").replace("-", "").strip()
            if not re.fullmatch(r"\d+", val):
                continue
            entries.append({"team": code, "runs": int(val)})
        if len(entries) != 2:
            continue
        sm = STADIUM_RE.search(head)
        games.append(
            {
                "date": f"{int(dm.group(1)):02d}-{int(dm.group(2)):02d}",
                "month_doc": month_label,
                "stadium": sm.group(1) if sm else "",
                "team1": entries[0]["team"],
                "team2": entries[1]["team"],
                "score1": entries[0]["runs"],
                "score2": entries[1]["runs"],
            }
        )
    return games


def crawl_all() -> list[dict]:
    """모든 팀 월별 문서를 수집해 raw 박스 리스트 반환."""
    all_games: list[dict] = []
    missing: list[str] = []
    for team_doc, code in [(v, k) for k, v in TEAMS.items()]:
        for month in MONTH_DOCS:
            url = try_doc_url(team_doc, month)
            try:
                html = fetch(url)
            except Exception as exc:
                logger.exception("fetch failed %s/%s", team_doc, month)
                missing.append(f"{team_doc}/{month}: {exc}")
                continue
            if "해당 문서는 존재하지 않습니다" in html or "문서가 없습니다" in html:
                missing.append(f"{team_doc}/{month}: 404")
                continue
            games = parse_boxes(parse_tables(html), month)
            for g in games:
                g["team_doc"] = code
            all_games.extend(games)
            print(f"{team_doc}/{month}: {len(games)} boxscores", flush=True)
            time.sleep(REQUEST_DELAY_S)
    print(f"total boxscores: {len(all_games)}")
    for m in missing:
        print(f"MISSING {m}")
    return all_games


def drop_ghosts(raw: list[dict]) -> list[dict]:
    """원본 오기 유령 경기 제거 (해태 4/8 = 4/14 복사본)."""
    out = []
    for g in raw:
        if any(
            g["date"] == gh["date"]
            and g["team1"] == gh["team1"]
            and g["score1"] == gh["score1"]
            and g["team2"] == gh["team2"]
            and g["score2"] == gh["score2"]
            for gh in GHOSTS
        ):
            continue
        out.append(g)
    return out


def apply_replay_corrections(raw: list[dict]) -> list[dict]:
    """재경기 박스의 날짜를 재경기 날짜로 교정.

    복수 날짜 헤더("8월 5일, 8월 18일, 동대문야구장")의 박스는 파서가 첫 날짜만
    캡처하므로, (date, team1, team2, score1, score2, stadium) 전부 일치할 때만
    재경기 날짜로 이동한다. 무승부 경기(같은 날짜 다른 경기장/스코어)는 유지.
    """
    out = []
    for g in raw:
        spec = REPLAY_CORRECTIONS.get(g["date"])
        if spec and (g["team1"], g["team2"], g["score1"], g["score2"], g["stadium"]) == spec[1:]:
            out.append(dict(g, date=spec[0], replayed=True))
            continue
        out.append(g)
    return out


def merge_games(raw: list[dict]) -> list[dict]:
    """양 팀 문서 중복 박스를 1경기로 병합 (같은 날짜+매치업+스코어)."""
    by = defaultdict(list)
    for g in raw:
        by[(g["date"], tuple(sorted([g["team1"], g["team2"]])))].append(g)
    games = []
    for _k, v in by.items():
        seen = set()
        for g in v:
            sig = (g["score1"], g["score2"])
            if sig in seen:
                continue
            seen.add(sig)
            games.append(g)
    return games


def finalize_games(games: list[dict]) -> list[dict]:
    """game_id/홈-원정/경기장 단축명을 부여한 최종 정답 세트."""
    out = []
    cnt: Counter = Counter()
    for g in games:
        home = HOME_BY_STADIUM.get(g["stadium"])
        a, b = g["team1"], g["team2"]
        if home == a:
            ht, at, hs, a_s = a, b, g["score1"], g["score2"]
        elif home == b:
            ht, at, hs, a_s = b, a, g["score2"], g["score1"]
        else:  # 순회(중립) 경기장: 박스 순서 유지
            ht, at, hs, a_s = a, b, g["score1"], g["score2"]
        key = (g["date"], ht, at)
        out.append(
            {
                "game_id": f"1982{g['date'].replace('-', '')}{ht}{at}{cnt[key]}",
                "game_date": f"1982-{g['date']}",
                "stadium": STADIUM_SHORT.get(g["stadium"], g["stadium"]),
                "home_team": ht,
                "away_team": at,
                "home_score": hs,
                "away_score": a_s,
            }
        )
        cnt[key] += 1
    return out


def verify(games: list[dict]) -> bool:
    """위키 최종순위/매치업 균형/무승부 카운트 검증."""
    tr = defaultdict(lambda: [0, 0, 0])
    mc = defaultdict(int)
    draws = 0
    for g in games:
        a, b = g["home_team"], g["away_team"]
        s1, s2 = g["home_score"], g["away_score"]
        mc["-".join(sorted([a, b]))] += 1
        if s1 > s2:
            tr[a][0] += 1
            tr[b][1] += 1
        elif s2 > s1:
            tr[b][0] += 1
            tr[a][1] += 1
        else:
            tr[a][2] += 1
            tr[b][2] += 1
            draws += 1
    ok = True
    print("team records (W-L-D):")
    for t in ["OB", "SS", "MB", "HT", "LT", "SM"]:
        wins, losses, draws_t = tr[t]
        expected = (WIKI_WINS[t], WIKI_LOSSES[t])
        match = wins == expected[0] and losses == expected[1]
        ok &= match
        print(
            f"  {t}: {wins}W-{losses}L-{draws_t}D = {wins + losses + draws_t}  "
            f"wiki {expected[0]}-{expected[1]}  {'OK' if match else '<-- MISMATCH'}"
        )
    mm = min(mc.values())
    mx = max(mc.values())
    ok &= mm >= 16 and mx <= 18
    print(f"matchups: min {mm} max {mx} (expect 16-17)")
    print(f"draws: {draws} (expect 1)")
    print(f"total games: {len(games)} (expect 241 = 240 + 1 draw)")
    ok &= draws == 1 and len(games) == 241
    return ok


def main(argv: list[str] | None = None) -> int:
    """CLI 진입점."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--crawl", action="store_true", help="네트워크로 모든 문서를 새로 수집 (기본: 로컬 archive 재현)"
    )
    parser.add_argument("--verify-only", action="store_true", help="기존 answer set만 검증 (재산출 없음)")
    parser.add_argument("--raw", type=Path, default=DEFAULT_RAW)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args(argv)

    if args.crawl:
        raw = crawl_all()
        args.raw.parent.mkdir(parents=True, exist_ok=True)
        args.raw.write_text(json.dumps(raw, ensure_ascii=False, indent=1), encoding="utf-8")
        print(f"saved {args.raw}")
    else:
        raw = json.loads(args.raw.read_text(encoding="utf-8"))

    raw = drop_ghosts(apply_replay_corrections(raw))
    games = merge_games(raw)
    final = finalize_games(games)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(final, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"saved {args.output}")

    ok = verify(final if not args.verify_only else json.loads(args.output.read_text(encoding="utf-8")))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
