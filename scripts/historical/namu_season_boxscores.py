"""1983-2000 KBO 시즌 실측 경기결과 수집 파이프라인 (나무위키 팀별 월별 문서).

1982 파이프라인(scripts/historical/1982_namu_boxscores.py)을 일반화한 버전.
시즌마다 문서 구성/팀 구성이 달라 다음을 매개변수로 받는다:

  - SEASONS: 시즌별 (팀 코드 → 나무위키 문서명) + 경기장 홈 소유 매핑.
  - 월 문서명: 팀 시즌 부모 문서(`{팀}/{연도}년`)의 하위 문서 목록에서 자동
    수집 (1982년: "3~4월", 1983년: "4월", ... 시즌마다 다름).
  - 앵커: data/archives/season_anchors_1983_2000.json (위키백과 팀 문서
    시즌별 성적 표 기반) — 팀별 최종 승/무/패 대조로 검증.

사용법
------
  python3 -m scripts.historical.namu_season_boxscores --year 1983 --crawl
  python3 -m scripts.historical.namu_season_boxscores --year 1983
  python3 -m scripts.historical.namu_season_boxscores --year 1983 --verify-only
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
REQUEST_DELAY_S = 1.5
RAW_DIR = Path("data/archives")
ANCHORS = Path("data/archives/season_anchors_1983_2000.json")
CANDIDATE_MONTHS = [f"{m}월" for m in range(3, 11)]

# 시즌 중 개명으로 두 문서가 공존하는 프랜차이즈 — 크롤한 뒤 통일 코드로 정규화.
# 1985: 삼미 슈퍼스타즈(전기, SM) → 청보 핀토스(후기, CB). 위키백과 최종순위와
# team_history 코드는 CB이므로 파싱 결과를 CB로 통일한다.
SEASON_TEAM_ALIASES: dict[int, dict[str, str]] = {
    1985: {"SM": "CB"},
}

SEASONS: dict[int, dict[str, str]] = {
    1983: {
        "OB": "OB 베어스",
        "MBC": "MBC 청룡",
        "HT": "해태 타이거즈",
        "SS": "삼성 라이온즈",
        "LT": "롯데 자이언츠",
        "SM": "삼미 슈퍼스타즈",
    },
    1984: {
        "OB": "OB 베어스",
        "MBC": "MBC 청룡",
        "HT": "해태 타이거즈",
        "SS": "삼성 라이온즈",
        "LT": "롯데 자이언츠",
        "SM": "삼미 슈퍼스타즈",
    },
    1985: {
        "OB": "OB 베어스",
        "MBC": "MBC 청룡",
        "HT": "해태 타이거즈",
        "SS": "삼성 라이온즈",
        "LT": "롯데 자이언츠",
        "SM": "삼미 슈퍼스타즈",
        "CB": "청보 핀토스",
    },
    1986: {
        "OB": "OB 베어스",
        "MBC": "MBC 청룡",
        "HT": "해태 타이거즈",
        "SS": "삼성 라이온즈",
        "LT": "롯데 자이언츠",
        "BE": "빙그레 이글스",
        "CB": "청보 핀토스",
    },
    1987: {
        "OB": "OB 베어스",
        "MBC": "MBC 청룡",
        "HT": "해태 타이거즈",
        "SS": "삼성 라이온즈",
        "LT": "롯데 자이언츠",
        "BE": "빙그레 이글스",
        "CB": "청보 핀토스",
    },
    1988: {
        "OB": "OB 베어스",
        "MBC": "MBC 청룡",
        "HT": "해태 타이거즈",
        "SS": "삼성 라이온즈",
        "LT": "롯데 자이언츠",
        "BE": "빙그레 이글스",
        "TP": "태평양 돌핀스",
    },
    1989: {
        "OB": "OB 베어스",
        "MBC": "MBC 청룡",
        "HT": "해태 타이거즈",
        "SS": "삼성 라이온즈",
        "LT": "롯데 자이언츠",
        "BE": "빙그레 이글스",
        "TP": "태평양 돌핀스",
    },
    1990: {
        "OB": "OB 베어스",
        "LG": "LG 트윈스",
        "HT": "해태 타이거즈",
        "SS": "삼성 라이온즈",
        "LT": "롯데 자이언츠",
        "BE": "빙그레 이글스",
        "TP": "태평양 돌핀스",
    },
    1991: {
        "OB": "OB 베어스",
        "LG": "LG 트윈스",
        "HT": "해태 타이거즈",
        "SS": "삼성 라이온즈",
        "LT": "롯데 자이언츠",
        "BE": "빙그레 이글스",
        "TP": "태평양 돌핀스",
        "SL": "쌍방울 레이더스",
    },
    1992: {
        "OB": "OB 베어스",
        "LG": "LG 트윈스",
        "HT": "해태 타이거즈",
        "SS": "삼성 라이온즈",
        "LT": "롯데 자이언츠",
        "BE": "빙그레 이글스",
        "TP": "태평양 돌핀스",
        "SL": "쌍방울 레이더스",
    },
    1993: {
        "OB": "OB 베어스",
        "LG": "LG 트윈스",
        "HT": "해태 타이거즈",
        "SS": "삼성 라이온즈",
        "LT": "롯데 자이언츠",
        "BE": "빙그레 이글스",
        "TP": "태평양 돌핀스",
        "SL": "쌍방울 레이더스",
    },
    1994: {
        "OB": "OB 베어스",
        "LG": "LG 트윈스",
        "HT": "해태 타이거즈",
        "SS": "삼성 라이온즈",
        "LT": "롯데 자이언츠",
        "HH": "한화 이글스",
        "TP": "태평양 돌핀스",
        "SL": "쌍방울 레이더스",
    },
    1995: {
        "OB": "OB 베어스",
        "LG": "LG 트윈스",
        "HT": "해태 타이거즈",
        "SS": "삼성 라이온즈",
        "LT": "롯데 자이언츠",
        "HH": "한화 이글스",
        "TP": "태평양 돌핀스",
        "SL": "쌍방울 레이더스",
    },
    1996: {
        "OB": "OB 베어스",
        "LG": "LG 트윈스",
        "HT": "해태 타이거즈",
        "SS": "삼성 라이온즈",
        "LT": "롯데 자이언츠",
        "HH": "한화 이글스",
        "HU": "현대 유니콘스",
        "SL": "쌍방울 레이더스",
    },
    1997: {
        "OB": "OB 베어스",
        "LG": "LG 트윈스",
        "HT": "해태 타이거즈",
        "SS": "삼성 라이온즈",
        "LT": "롯데 자이언츠",
        "HH": "한화 이글스",
        "HU": "현대 유니콘스",
        "SL": "쌍방울 레이더스",
    },
    1998: {
        "OB": "OB 베어스",
        "LG": "LG 트윈스",
        "HT": "해태 타이거즈",
        "SS": "삼성 라이온즈",
        "LT": "롯데 자이언츠",
        "HH": "한화 이글스",
        "HU": "현대 유니콘스",
        "SL": "쌍방울 레이더스",
    },
    1999: {
        "DB": "두산 베어스",
        "LG": "LG 트윈스",
        "HT": "해태 타이거즈",
        "SS": "삼성 라이온즈",
        "LT": "롯데 자이언츠",
        "HH": "한화 이글스",
        "HU": "현대 유니콘스",
        "SL": "쌍방울 레이더스",
    },
    2000: {
        "DB": "두산 베어스",
        "LG": "LG 트윈스",
        "HT": "해태 타이거즈",
        "SS": "삼성 라이온즈",
        "LT": "롯데 자이언츠",
        "HH": "한화 이글스",
        "HU": "현대 유니콘스",
        "SK": "SK 와이번스",
    },
}

# 문서 박스 스코어 교정 (재현 가능한 수동 교정). 1982 파이프라인의
# REPLAY_CORRECTIONS에 대응. 키: (연도, 날짜 MM-DD) → {팀코드: 최종득점}.
# 박스 내 팀 순서와 무관하게 팀코드 기준으로 스코어를 교체한다.
# 원칙: 위키 앵커/문서 내부 일관성(헤더·누적표·캘린더)이 박스 R열보다
# 신뢰할 때만 등록. r_mismatch(이닝 합 불일치)는 파서가 자동 처리하므로
# 여기엔 R열이 일관된데 사실이 다른 경우만 남긴다.
SCORE_FIXES: dict[tuple[int, str], dict[str, int]] = {
    # 1983-09-24 OB-SM: 양 문서 박스 R열이 OB 4-3으로 일관(이닝 합도 일치)했으나
    # 문서 헤더 "1패"·캘린더(삼미 4:3)·양팀 누적표·위키 앵커가 모두 삼미 승을 지지.
    # 6개 신호 교차검증으로 OB 3-4 확정. (Docs/references/HISTORICAL_1983_2000_PLAN.md)
    (1983, "09-24"): {"OB": 3, "SM": 4},
    # 1985-06-18/19 LT-MBC (구덕): MBC 문서 박스가 팀 라벨 스왑 오기 — 이닝 라인은
    # LT 문서와 동일하나 팀명만 뒤집혀 "LT 3:1 MBC"로 기록됨. 양 문서 섹션 헤더가 모두
    # "피스윕"(롯데 기준) / "스윕"(MBC 기준)으로 MBC 2승을 지지. 팀코드 기준 스코어 교정.
    (1985, "06-18"): {"LT": 1, "MBC": 3},
    (1985, "06-19"): {"LT": 2, "MBC": 4},
}

# 같은 날짜·매치업에 스코어가 다른 박스가 각 문서에서 1개씩 나올 때,
# 실제 더블헤더가 아니라 한쪽 문서의 스코어 오기인 경우 — 폐기할 (date, team_doc).
# (merge_games가 스코어 시그니처로 중복 판별하므로 다른 스코어는 DH로 오인됨)
DROP_BOXES: dict[int, set[tuple[str, str]]] = {
    # 1985-07-14 CB-OB (동대문): OB 문서가 7:5, 청보 문서가 7:6. 당일 스케줄·청보
    # 문서 헤더("위닝 시리즈")는 단일 경기. 청보 문서가 선발/승패까지 상세하므로 채택.
    1985: {("07-14", "OB")},
}

# 경기장 소유 홈 팀 (순회/공동 홈 경기장은 제외: 잠실 1990+, 마산 등은 박스 순서 유지).
# 다년도 코드는 stadium_home()에서 시즌별로 보정:
#   OT(숭의/인천): SM→CB→TP→HU→SK, BE(한밭/청주): BE→HH, 잠실: OB→공동, 동대문: MBC
HOME_BY_STADIUM: dict[str, str] = {
    "구덕 야구장": "LT",
    "구덕운동장 야구장": "LT",
    "구덕종합운동장 야구장": "LT",
    "대구시민운동장 야구장": "SS",
    "대구시민구장": "SS",
    "대구 야구장": "SS",
    "무등 야구장": "HT",
    "무등종합운동장 야구장": "HT",
    "광주야구장": "HT",
    "광주무등경기장 야구장": "HT",
    "숭의야구장": "OT",  # SM/CB/TP/HU/SK — 시즌별 코드로 보정
    "춘천공설운동장 야구장": "SM",
    "춘천야구장": "SM",
    "동대문야구장": "MBC",
    "서울종합운동장 야구장": "OB",  # 1990+ 잠실은 공동 홈 → 박스 순서 유지
    "잠실야구장": "OB",
    "한밭 야구장": "BE",
    "한밭종합운동장 야구장": "BE",
    "한밭구장": "BE",
    "전주종합경기장 야구장": "HT",
    "전주야구장": "HT",
    "청주 야구장": "BE",
    "청주종합운동장 야구장": "BE",
    "청주야구장": "BE",
    "수원 야구장": "SL",
    "수원종합운동장 야구장": "SL",
    "인천야구장": "OT",
    "인천시민운동장 야구장": "OT",
    "인천문학경기장 야구장": "OT",
}
STADIUM_SHORT = {
    "구덕 야구장": "부산",
    "구덕운동장 야구장": "부산",
    "구덕종합운동장 야구장": "부산",
    "대구시민운동장 야구장": "대구",
    "대구시민구장": "대구",
    "대구 야구장": "대구",
    "무등 야구장": "광주",
    "무등종합운동장 야구장": "광주",
    "광주야구장": "광주",
    "광주무등경기장 야구장": "광주",
    "숭의야구장": "인천",
    "인천야구장": "인천",
    "인천시민운동장 야구장": "인천",
    "인천문학경기장 야구장": "문학",
    "춘천공설운동장 야구장": "춘천",
    "춘천야구장": "춘천",
    "동대문야구장": "동대문",
    "서울종합운동장 야구장": "잠실",
    "잠실야구장": "잠실",
    "한밭 야구장": "대전",
    "한밭종합운동장 야구장": "대전",
    "한밭구장": "대전",
    "전주종합경기장 야구장": "전주",
    "전주야구장": "전주",
    "청주 야구장": "청주",
    "청주종합운동장 야구장": "청주",
    "청주야구장": "청주",
    "마산 야구장": "마산",
    "제주종합경기장 야구장": "제주",
    "수원 야구장": "수원",
    "수원종합운동장 야구장": "수원",
}

STADIUM_RE = re.compile(r"([가-힣A-Za-z0-9]+(?:\s+[가-힣A-Za-z0-9]+)*(?:야구장|구장))")
DATE_RE = re.compile(r"(\d{1,2})월 (\d{1,2})일")


def fetch(url: str) -> str:
    """나무위키 문서 HTML 수신."""
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.read().decode("utf-8")


def strip_tags(s: str) -> str:
    """HTML 태그/엔티티 제거."""
    s = re.sub(r"<[^>]+>", "", s)
    return s.replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&").strip()


def parse_tables(html: str) -> list[list[list[str]]]:
    """2행 이상 표 파싱."""
    out = []
    for t in re.findall(r"<table[^>]*>(.*?)</table>", html, re.S):
        rows = []
        for r in re.findall(r"<tr[^>]*>(.*?)</tr>", t, re.S):
            cells = [strip_tags(c) for c in re.findall(r"<td[^>]*>(.*?)</td>", r, re.S)]
            if cells:
                rows.append(cells)
        if len(rows) >= 2:
            out.append(rows)
    return out


def discover_month_docs(team_doc: str, year: int) -> list[str]:
    """부모 문서(`{팀}/{연도}년`)의 하위 월 문서명 목록.

    부모 문서가 없으면(404) 표준 월명("3월"~"10월") 문서를 직접 프로브한다 —
    삼성 라이온즈/1984년 사례(시즌 문서 부재 + 월별 문서 존재).
    """
    url = "https://namu.moe/w/" + urllib.parse.quote(f"{team_doc}/{year}년")
    try:
        html = fetch(url)
    except Exception as exc:
        logger.warning("parent season doc fetch failed, probing months: %s (%s)", url, exc)
        return _probe_standard_month_docs(team_doc, year)
    text = re.sub(r"<[^>]+>", " ", html)
    pattern = re.compile(re.escape(f"{team_doc}/{year}년/") + r"([0-9~가-힣]+?)(?:\s|#|&|\||\]|$)")
    names = sorted(set(pattern.findall(text)))
    return [n for n in names if n != ""]


def _probe_standard_month_docs(team_doc: str, year: int) -> list[str]:
    """부모 문서 없는 시즌 — 표준 월 문서("3월"~"10월") 존재 여부 직접 확인."""
    found: list[str] = []
    for month in CANDIDATE_MONTHS:
        url = "https://namu.moe/w/" + urllib.parse.quote(f"{team_doc}/{year}년/{month}")
        try:
            html = fetch(url)
        except Exception as exc:
            logger.warning("probe miss %s: %s", url, exc)
            continue
        if "해당 문서는 존재하지 않습니다" in html or "문서가 없습니다" in html:
            continue
        found.append(month)
        time.sleep(REQUEST_DELAY_S)
    return found


def parse_boxes(  # noqa: C901
    tables: list[list[list[str]]], teams: dict[str, str], month_label: str
) -> list[dict]:
    """박스스코어 표들에서 (date, stadium, team1/2, score1/2) 추출.

    R열뿐 아니라 이닝 라인 합을 함께 계산해 R열 오기(합계 칸만 틀린 박스)를
    r_mismatch로 표시한다 — 1983년 4/30 HT-MBC 사례(이닝 합 4인데 R열 2로 기록)
    감지를 위한 규칙.
    """
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
            code = None
            for k, v in teams.items():
                name = r[0]
                if name == v or name in v or v in name:
                    code = k
                    break
            if code is None or r_idx >= len(r):
                continue
            val = r[r_idx].replace("X", "").replace("-", "").strip()
            if not re.fullmatch(r"\d+", val):
                continue
            runs = int(val)
            inning_sum = 0
            for cell in r[1:r_idx]:
                num = re.fullmatch(r"(\d+)X?", cell.strip())
                if num:
                    inning_sum += int(num.group(1))
            entries.append(
                {
                    "team": code,
                    "runs": runs,
                    "inning_sum": inning_sum,
                }
            )
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
                "inning_sum1": entries[0]["inning_sum"],
                "inning_sum2": entries[1]["inning_sum"],
                "r_mismatch": (
                    entries[0]["inning_sum"] != entries[0]["runs"] or entries[1]["inning_sum"] != entries[1]["runs"]
                ),
            }
        )
    return games


def crawl_year(year: int) -> list[dict]:
    """시즌의 모든 팀 월별 문서 수집."""
    teams = SEASONS[year]
    alias = SEASON_TEAM_ALIASES.get(year, {})
    all_games: list[dict] = []
    missing: list[str] = []
    for code, team_doc in teams.items():
        months = discover_month_docs(team_doc, year)
        if not months:
            missing.append(f"{team_doc}: no month docs")
            continue
        for month in months:
            url = "https://namu.moe/w/" + urllib.parse.quote(f"{team_doc}/{year}년/{month}")
            try:
                html = fetch(url)
            except Exception as exc:
                logger.exception("fetch failed %s/%s", team_doc, month)
                missing.append(f"{team_doc}/{month}: {exc}")
                continue
            if "해당 문서는 존재하지 않습니다" in html or "문서가 없습니다" in html:
                missing.append(f"{team_doc}/{month}: 404")
                continue
            games = parse_boxes(parse_tables(html), teams, month)
            for g in games:
                g["team_doc"] = code
                g["team1"] = alias.get(g["team1"], g["team1"])
                g["team2"] = alias.get(g["team2"], g["team2"])
            all_games.extend(games)
            print(f"{team_doc}/{month}: {len(games)} boxscores", flush=True)
            time.sleep(REQUEST_DELAY_S)
    print(f"year {year}: total boxscores {len(all_games)}")
    for m in missing:
        print(f"MISSING {m}")
    return all_games


def drop_box_fixes(raw: list[dict], year: int) -> list[dict]:
    """DROP_BOXES 등록 박스를 병합 전에 폐기.

    같은 날짜·매치업에 스코어가 다른 박스가 각 문서에서 1개씩 나올 때, 실제로는
    단일 경기인데 한쪽 문서의 스코어가 오기인 경우를 처리한다. 해당 (date, team_doc)
    박스를 제거하면 상대 문서 박스가 merge에서 단일 경기로 남는다.
    만약 지정된 박스가 없어 폐기가 불가능하면 경고를 남긴다.
    """
    drops = DROP_BOXES.get(year, set())
    if not drops:
        return raw
    kept: list[dict] = []
    for g in raw:
        key = (g["date"], g["team_doc"])
        if key in drops:
            print(
                f"DROP {year}-{g['date']} {g['team_doc']} box "
                f"({g['team1']} {g['score1']}:{g['score2']} {g['team2']}): "
                f"동일 날짜 타 문서와 스코어 상이 — 상대 문서 채택",
                flush=True,
            )
            continue
        kept.append(g)
    dropped = {key for key in drops if key in {(g["date"], g["team_doc"]) for g in raw}}
    if dropped != drops:
        for key in drops - dropped:
            print(f"WARN drop target {year}-{key[0]} {key[1]} 없음 (이미 폐기됨?)", flush=True)
    return kept


def merge_games(raw: list[dict]) -> list[dict]:
    """양 팀 문서 중복 박스를 1경기로 병합 (같은 날짜+매치업+스코어).

    스코어가 서로 다른 박스가 같은 날짜·매치업에 존재하면:
      1. R열 오기(r_mismatch=True) 박스는 이닝 합이 신뢰 가능한 박스와 다를 때
         유령 중복으로 간주해 폐기한다 (1983년 4/30 HT-MBC 사례).
      2. R열이 정상인 박스가 여럿이면 실제 더블헤더로 보고 유지한다.
    """
    by: dict[tuple, list[dict]] = defaultdict(list)
    for g in raw:
        key = (g["date"], tuple(sorted([g["team1"], g["team2"]])))
        by[key].append(g)
    games = []
    for _k, v in by.items():
        seen = set()
        healthy: list[dict] = []
        for g in v:
            if g.get("r_mismatch"):
                continue
            sig = (g["score1"], g["score2"])
            if sig in seen:
                continue
            seen.add(sig)
            healthy.append(g)
        if healthy:
            games.extend(healthy)
            continue
        # 전부 R열 오기 — 원본 보존 + 경고 (스코어 자체가 오기일 수 있음)
        seen = set()
        kept = 0
        for g in v:
            sig = (g["score1"], g["score2"])
            if sig in seen:
                continue
            seen.add(sig)
            games.append(g)
            kept += 1
        if kept > 1:
            print(f"WARN multiple R-mismatched boxes on {_k[0]} {_k[1]}: {sorted(seen)}", flush=True)
    return games


def stadium_home(stadium: str, year: int) -> str | None:
    """경기장 홈 팀 (시즌별 코드 보정: 숭의/한밭/잠실/동대문)."""
    code = HOME_BY_STADIUM.get(stadium)
    if code == "OT":  # 숭의/인천 — 연도별 홈 구단
        if year <= 1984:
            return "SM"  # 삼미 (1982-84)
        if year <= 1987:
            return "CB"  # 청보 (1985-87, 삼미→청보 개명이 1985 후반기)
        if year <= 1995:
            return "TP"
        if year <= 1999:
            return "HU"
        return "SK"  # 2000+
    if code == "BE":  # 한밭/청주 — 빙그레(1986-93) → 한화(1994+)
        if year < 1986:
            return None
        return "HH" if year >= 1994 else "BE"
    if code == "OB" and stadium in ("서울종합운동장 야구장", "잠실야구장") and year >= 1990:
        return None  # 잠실 공동 홈 (LG/OB) — 박스 순서 유지
    if code == "MBC" and year >= 1990:
        return None  # 동대문은 MBC 시절만 홈
    return code


def apply_score_fixes(games: list[dict], year: int) -> list[dict]:
    """SCORE_FIXES 등록 교정을 병합 결과에 적용 (재현 가능한 수동 교정).

    박스 팀 순서와 무관하게 팀코드 기준으로 득점을 교체한다.
    적용 시 로그를 남겨 교정 적용 여부를 투명하게 만든다.
    """
    applied = 0
    for g in games:
        fix = SCORE_FIXES.get((year, g["date"]))
        if not fix or g["team1"] not in fix or g["team2"] not in fix:
            continue
        s1, s2 = fix[g["team1"]], fix[g["team2"]]
        if (g["score1"], g["score2"]) == (s1, s2):
            continue
        print(
            f"FIX {year}-{g['date']} {g['team1']}-{g['team2']}: {g['score1']}:{g['score2']} -> {s1}:{s2}",
            flush=True,
        )
        g["score1"], g["score2"] = s1, s2
        applied += 1
    print(f"score fixes applied: {applied}")
    return games


def finalize_games(games: list[dict], year: int) -> list[dict]:
    """game_id/홈-원정/경기장 단축명 부여."""
    out = []
    cnt: Counter = Counter()
    unknown_stadiums: Counter = Counter()
    for g in games:
        home = stadium_home(g["stadium"], year)
        if not home:
            unknown_stadiums[g["stadium"] or "<none>"] += 1
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
                "game_id": f"{year}{g['date'].replace('-', '')}{ht}{at}{cnt[key]}",
                "game_date": f"{year}-{g['date']}",
                "stadium": STADIUM_SHORT.get(g["stadium"], g["stadium"]),
                "home_team": ht,
                "away_team": at,
                "home_score": hs,
                "away_score": a_s,
            }
        )
        cnt[key] += 1
    if unknown_stadiums:
        print("unknown stadiums:", dict(unknown_stadiums))
    return out


def _print_head_to_head(head: dict[tuple[str, str], list[int]]) -> None:
    """매치업별 상호전적 출력 (verify 불일치 진단)."""
    for pair in sorted(head):
        w, losses, d = head[pair]
        print(f"  head-to-head {pair[0]}-{pair[1]}: {w}W-{losses}L-{d}D (={w + losses + d})")


def _print_team_records(tr: dict[str, list[int]], anchors: dict) -> list[str]:
    """팀별 성적/앵커 대조 출력, 불일치 팀 목록 반환."""
    mismatch: list[str] = []
    for code, v in sorted(tr.items()):
        wins, losses, draws_t = v
        anchor = anchors.get(code) or {}
        exp_w, exp_l, exp_d = anchor.get("w"), anchor.get("l"), anchor.get("d")
        match = exp_w is not None and wins == exp_w and losses == exp_l and (exp_d is None or draws_t == exp_d)
        print(
            f"  {code}: {wins}W-{losses}L-{draws_t}D = {wins + losses + draws_t}  "
            f"anchor {exp_w}-{exp_l}-{exp_d}  {'OK' if match else '<-- MISMATCH'}"
        )
        if not match:
            mismatch.append(code)
    return mismatch


def verify(games: list[dict], year: int, anchors: dict) -> bool:
    """앵커(위키 최종순위) 대조 + 매치업 균형 + 경기 수.

    불일치 시 수정 후보를 좁히기 위해 매치업별 승패와 무승부 경기 목록,
    승패가 반대로 어긋난 팀 쌍(flip 후보)을 출력한다.
    """
    tr: dict[str, list[int]] = defaultdict(lambda: [0, 0, 0])
    head: dict[tuple[str, str], list[int]] = defaultdict(lambda: [0, 0, 0])
    mc: Counter = Counter()
    draw_games: list[str] = []
    for g in games:
        a, b = g["home_team"], g["away_team"]
        s1, s2 = g["home_score"], g["away_score"]
        pair = tuple(sorted([a, b]))
        mc["-".join(pair)] += 1
        if s1 > s2:
            winner = a
            tr[a][0] += 1
            tr[b][1] += 1
        elif s2 > s1:
            winner = b
            tr[b][0] += 1
            tr[a][1] += 1
        else:
            winner = None
            tr[a][2] += 1
            tr[b][2] += 1
            draw_games.append(f"{g['game_date']} {a} {g['home_score']}:{g['away_score']} {b}")
        if winner == pair[0]:
            head[pair][0] += 1
        elif winner == pair[1]:
            head[pair][1] += 1
        else:
            head[pair][2] += 1
    ok = True
    print(f"== verify {year} ==")
    mismatch_teams = _print_team_records(tr, anchors)
    ok &= not mismatch_teams
    if mc:
        mm, mx = min(mc.values()), max(mc.values())
        print(f"matchups: min {mm} max {mx}")
        ok &= mx - mm <= 2
    print(f"draws: {len(draw_games)}")
    for d in sorted(draw_games):
        print(f"  DRAW {d}")
    if mismatch_teams:
        print("-- mismatch diagnostics --")
        for code in mismatch_teams:
            v = tr[code]
            anchor = anchors.get(code) or {}
            dw = v[0] - (anchor.get("w") or 0)
            dl = v[1] - (anchor.get("l") or 0)
            dd = v[2] - (anchor.get("d") or 0)
            print(f"  {code}: delta W{dw:+d} L{dl:+d} D{dd:+d}")
        _print_head_to_head(head)
        print(
            "  tip: 승패 delta가 서로 반대 부호인 두 팀의 상호전(head-to-head)에서 "
            "단일 경기 스코어 반전 후보를 찾을 것 (1983년 9/24 OB-SM 사례)."
        )
    print(f"total games: {len(games)}")
    return ok


def main(argv: list[str] | None = None) -> int:
    """CLI 진입점."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--crawl", action="store_true")
    parser.add_argument("--verify-only", action="store_true")
    args = parser.parse_args(argv)

    year = args.year
    if year not in SEASONS:
        print(f"unsupported year {year}; supported: {sorted(SEASONS)}")
        return 2
    anchors = json.loads(ANCHORS.read_text(encoding="utf-8")).get(str(year), {})
    raw_path = RAW_DIR / f"{year}_namu_raw.json"
    out_path = RAW_DIR / f"{year}_answer_set_final.json"

    if args.verify_only:
        # 저장된 answer set을 재검증만 — 파일을 절대 덮어쓰지 않는다.
        existing = json.loads(out_path.read_text(encoding="utf-8"))
        return 0 if verify(existing, year, anchors) else 1

    if args.crawl:
        raw = crawl_year(year)
        RAW_DIR.mkdir(parents=True, exist_ok=True)
        raw_path.write_text(json.dumps(raw, ensure_ascii=False, indent=1), encoding="utf-8")
        print(f"saved {raw_path}")
    else:
        raw = json.loads(raw_path.read_text(encoding="utf-8"))

    games = merge_games(drop_box_fixes(raw, year))
    final = finalize_games(apply_score_fixes(games, year), year)
    ok = verify(final, year, anchors)
    # 검증 통과한 경우에만 answer set 교체 (실패 상태로 파일을 덮어쓰지 않음).
    if ok:
        out_path.write_text(json.dumps(final, ensure_ascii=False, indent=1), encoding="utf-8")
        print(f"saved {out_path}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
