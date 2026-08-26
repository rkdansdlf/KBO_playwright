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
}

# 박스 구장 표기 오기 교정. 키: (연도, 날짜 MM-DD, matchup) → 올바른 구장명.
# matchup은 정렬된 팀코드 "-" 연결 — 같은 날짜 타 경기를 보호하기 위해 필요.
STADIUM_FIXES: dict[tuple[int, str, str], str] = {
    # 1985-05-04 CB-OB: 양 문서 헤더 모두 "5월 3~6일 VS 삼미/OB (동대문) 동률 시리즈",
    # 전후 경기(05-03)도 동대문인데 이 박스만 숭의로 복사 오기.
    (1985, "05-04", "CB-OB"): "동대문야구장",
    # 1987-08-16 HT-LT: 양 문서 헤더 "8월 15~16일 VS 롯데/해태 (사직)" — LT 문서는
    # 사직으로 정상 기재, HT 문서만 무등으로 오기. 스윕(HT 2승)도 스코어와 부합.
    (1987, "08-16", "HT-LT"): "사직 야구장",
}

# 같은 날짜·매치업에 스코어가 다른 박스가 각 문서에서 1개씩 나올 때,
# 실제 더블헤더가 아니라 한쪽 문서의 스코어 오기인 경우 — 폐기할 (date, team_doc, matchup).
# matchup은 정렬된 팀코드를 "-"로 연결 ("BE-CB"). 날짜 오기 유령은 동일 박스가 문서마다
# 다른 날짜로 기재되므로, 같은 날짜의 정상 박스 보호를 위해 매치업까지 지정한다.
DROP_BOXES: dict[int, set[tuple[str, str, str]]] = {
    1985: {
        # 07-14 CB-OB (동대문): OB 문서가 7:5, 청보 문서가 7:6. 당일 스케줄·청보
        # 문서 헤더("위닝 시리즈")는 단일 경기. 청보 문서가 선발/승패까지 상세하므로 채택.
        ("07-14", "OB", "CB-OB"),
        # 06-18/19 LT-MBC (구덕): MBC 문서 박스가 팀 라벨 스왑 오기 — 이닝 라인은 LT 문서와
        # 동일하나 팀명만 뒤집혀 "LT 3:1 MBC"로 기록됨. 양 문서 섹션 헤더가 모두 MBC 스윕을
        # 지지하며, LT 문서 버전(team1=MBC)이 원래 올바른 순서이므로 MBC 문서 박스를 폐기.
        ("06-18", "MBC", "LT-MBC"),
        ("06-19", "MBC", "LT-MBC"),
    },
    # 날짜 오기 유령 — 동일 박스(스코어·구장 동일)가 문서마다 다른 날짜로 기재된 케이스.
    # 1986: 진짜 날짜는 각주 참조 (Docs/references/HISTORICAL_1983_2000_PLAN.md).
    1986: {
        ("04-12", "CB", "BE-CB"),  # 진짜 04-06 (BE doc). CB는 4/12에 OB전 (헤더 "4/12~13 VS OB")
        ("05-25", "CB", "CB-MBC"),  # 춘천 경기 진짜 05-24 (MBC doc, 헤더 "5/24~25 스윕")
        ("09-10", "OB", "LT-OB"),  # 진짜 05-10 (LT doc, 헤더 "5/10~11 동률"). OB는 9/10에 청보전
    },
    1987: {
        # 09-01 OB doc CB-OB → 진짜 09-02: 양 문서 헤더 모두 "9월 2일 VS 청보/OB
        # (인천)". 동일 박스(OB 4:13, 숭의)를 OB 문서만 9/1로 기재.
        ("09-01", "OB", "CB-OB"),
    },
    1988: {
        # 07-07/07-08 LT-MBC (잠실): 동일 박스(LT 4:0)를 MBC 문서는 07-07 단독,
        # LT 문서는 07-08에서 6:6(무승부)과 함께 DH로 기재. MBC 문서는 같은 날
        # 6:6을 07-08 단독으로 정확히 보유하므로 2일 시리즈가 맞고, LT 문서의
        # 07-08 4:0 박스가 07-07 경기의 이중 계입. → LT 문서 박스 폐기.
        ("07-08", "LT", "LT-MBC"),
        # 09-02/09-03 OB-SS (대구): 동일 박스(OB 0:1 SS)를 SS 문서는 09-02,
        # OB 문서는 09-03으로 기재한 날짜 오기 유령. 홈팀 문서(SS) 기재를 채택해
        # OB 문서의 09-03 박스를 폐기. (진짜 날짜 헤더 확인은 후속 과제)
        ("09-03", "OB", "OB-SS"),
        # 09-04 MBC-TP (춘천): 단일 경기를 두 문서가 다른 스코어로 기재 —
        # MBC 문서 4:1은 전일(09-03 숭의) 스코어의 복사 오기, TP 문서 3:1 채택.
        # 스코어가 달라 merge에서 2경기로 치부되어 초과 계입된 케이스.
        ("09-04", "MBC", "MBC-TP"),
    },
    1991: {
        # 06-01 LT-TP (사직): 팀 라벨 스왑 오기 — LT 문서 'LT 0:5 TP'(홈=TP),
        # TP 문서 'TP 0:5 LT'(홈=LT). 이닝 라인 동일, 사직은 LT 홈이므로
        # TP 문서 버전(홈=LT)이 원래 올바른 순서 → LT 문서 박스 폐기.
        # (1985년 06-18/19 구덕 LT-MBC 사례와 동일 패턴)
        ("06-01", "LT", "LT-TP"),
    },
    1989: {
        # 06-21/22 LT-TP (숭의): 스코어 충돌 — LT 문서 06-21 'LT 2:1 TP',
        # TP 문서는 06-21 'LT 0:10 TP'와 06-22 'LT 2:1 TP' 보유. 앵커 방향상
        # 승자는 TP이므로 TP 문서의 0:10을 채택하고 LT 문서의 06-21 박스 폐기.
        ("06-21", "LT", "LT-TP"),
    },
    1990: {
        # 07-05 BE-LT (한밭): 스코어 불일치 복사본 (BE 10:4 vs 15:4, 승자 BE 동일) -> LT 문서 박스 폐기
        ("07-05", "LT", "BE-LT"),
        # 08-22 OB-TP (잠실 DH2): OB 문서 'TP 4:6 OB'(OB 승) vs TP 문서 'TP 6:4 OB'(TP 승) -> 앵커 방향상 TP 승이 정답, OB 문서 박스 폐기
        ("08-22", "OB", "OB-TP"),
        # 09-20 HT-LT (무등): 스코어 불일치 복사본 (HT 3:0 vs 6:0, 승자 HT 동일) -> LT 문서 박스 폐기
        ("09-20", "LT", "HT-LT"),
    },
    1993: {
        # 08-13~15 LT-OB (사직 3연전): OB 문서가 팀 라벨을 교차 기재
        # ('LT 1:7 OB' 등) — LT 문서 버전('OB 1:7 LT', 홈=LT)이 구장 규칙 부합.
        # 3경기 중 앵커 교정에 필요한 1경기(08-13)만 폐기해 단일 사본으로 병합.
        ("08-13", "OB", "LT-OB"),
    },
    1994: {
        # 06-01/02 HT-LT: HT 문서 기재 채택 시 앵커 불일치가 전체 시리즈에서
        # 발생 — 솔버 검증 결과 두 박스 폐기 시 LT 문서 기재와 정합.
        ("06-01", "HT", "HT-LT"),
        ("06-02", "HT", "HT-LT"),
        # 09-14 HH-SS (대구): 스코어 충돌 복사본(HH 3:5 vs 3:4) — 승자(SS)는
        # 동일하나 이중 계입된 초과분. 원격팀(HH) 문서 박스 폐기.
        ("09-14", "HH", "HH-SS"),
    },
    1995: {
        # 06-25 HH-TP (숭의): 스코어 충돌 복사본 (HH 4:3 vs 4:2, 승자 HH 동일) -> TP 문서 박스 폐기
        ("06-25", "TP", "HH-TP"),
        # 07-20 HT-LG (잠실): 스코어 충돌 복사본 (HT 4:2 vs 3:2, 승자 HT 동일) -> LG 문서 박스 폐기
        ("07-20", "LG", "HT-LG"),
        # 08-27 HH-HT (한밭): 스코어 충돌 복사본 (HT 1:0 vs 2:1, 승자 HT 동일) -> HH 문서 박스 폐기
        ("08-27", "HH", "HH-HT"),
    },
    1996: {
        # 06-04 / 06-06 HU-OB (동대문): OB 문서가 팀 라벨을 스왑해 'HU 3:8 OB', 'HU 3:4 OB'로 기재 (HU 승 지지하는 HU 문서 채택)
        ("06-04", "OB", "HU-OB"),
        ("06-06", "OB", "HU-OB"),
    },
    1998: {
        # 06-24 HH-HT: 8월 문서에 수록된 6/24 재경기 오기 박스 (HH 4:2 HT) 폐기 (6월 문서의 진짜 06-24 HH 4:1 HT 보존)
        ("06-24", "HH", "HH-HT", "8월"),
        ("06-24", "HT", "HH-HT", "8월"),
    },
    1999: {
        # 06-21 HU-LG: 서스펜디드 2:2 무승부 박스 폐기 (당일 완봉 재경기 10:3 박스 보존)
        ("06-21", "HU", "HU-LG", (2, 2)),
        ("06-21", "LG", "HU-LG", (2, 2)),
    },
    2000: {
        # 05-11 DB-HT (무등): 스코어 충돌 복사본(DB 11:2 vs 8:2, 승자 DB 동일).
        # 홈팀(HT) 문서 기재 채택, DB 문서 박스 폐기.
        ("05-11", "DB", "DB-HT"),
        # 10-04 HH-HU (한밭): HH 문서 'HU 1:2 HH'(HH 승) vs HU 문서
        # 'HU 11:7 HH'(HU 승) — 앵커 방향상 HU 승이 정답. HH 문서 박스 폐기.
        ("10-04", "HH", "HH-HU"),
    },
}

# 경기장 소유 홈 팀 — 단일 구단 전용 구장만 등록 (크로스체크용).
# 공동/순회 구장(잠실 1986+ MBC·OB, 동대문 82-84 MBC·85 OB, 마산 등)은 박스 순서
# 규칙(홈=team2)이 처리하므로 미등록.
# 다년도 코드는 stadium_home()에서 시즌별로 보정:
#   OT(숭의/인천): SM→CB→TP→HU→SK, BE(한밭/청주): BE→HH, CC(춘천): SM→CB
HOME_BY_STADIUM: dict[str, str] = {
    "구덕 야구장": "LT",
    "구덕운동장 야구장": "LT",
    "구덕종합운동장 야구장": "LT",
    "사직 야구장": "LT",  # 1985 개장 — 롯데 홈 (구덕 대체)
    "사직야구장": "LT",
    "대구시민운동장 야구장": "SS",
    "대구시민구장": "SS",
    "대구 야구장": "SS",
    "무등 야구장": "HT",
    "무등종합운동장 야구장": "HT",
    "광주야구장": "HT",
    "광주무등경기장 야구장": "HT",
    "숭의야구장": "OT",  # SM/CB/TP/HU/SK — 시즌별 코드로 보정
    "춘천공설운동장 야구장": "CC",  # 삼미(≤1984)/청보(1985-87) 순회 홈 — 시즌별 보정
    "춘천야구장": "CC",
    "한밭 야구장": "BE",
    "한밭종합운동장 야구장": "BE",
    "한밭구장": "BE",
    "전주종합경기장 야구장": "HT",
    "전주야구장": "HT",
    "청주 야구장": "BE",
    "청주종합운동장 야구장": "BE",
    "청주종합경기장 야구장": "BE",
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
    "사직 야구장": "부산",
    "사직야구장": "부산",
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
    "청주종합경기장 야구장": "청주",
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
    단일 경기인데 한쪽 문서의 스코어 오기인 경우와 동일 박스가 문서마다 다른
    날짜로 기재된 날짜 오기 유령을 처리한다. 해당 (date, team_doc, matchup)
    또는 (date, team_doc, matchup, month_doc) 박스를 제거하면 상대 문서 박스가
    merge에서 단일 경기로 남는다.
    """
    drops = DROP_BOXES.get(year, set())
    if not drops:
        return raw
    kept: list[dict] = []
    matched_drops = set()
    for g in raw:
        key3 = (g["date"], g.get("team_doc"), "-".join(sorted([g["team1"], g["team2"]])))
        key4 = (g["date"], g.get("team_doc"), "-".join(sorted([g["team1"], g["team2"]])), g.get("month_doc"))
        key_score = (
            g["date"],
            g.get("team_doc"),
            "-".join(sorted([g["team1"], g["team2"]])),
            (g["score1"], g["score2"]),
        )
        if key3 in drops or key4 in drops or key_score in drops:
            matched_drops.add(key3 if key3 in drops else (key4 if key4 in drops else key_score))
            print(
                f"DROP {year}-{g['date']} {g.get('team_doc')} box "
                f"({g['team1']} {g['score1']}:{g['score2']} {g['team2']} @ {g.get('stadium', '')}): "
                f"날짜/스코어 오기 — 상대 문서 채택",
                flush=True,
            )
            continue
        kept.append(g)

    if matched_drops != drops:
        for key in drops - matched_drops:
            print(f"WARN drop target {year}-{key[0]} {key[1]} {key[2]} 없음 (이미 폐기됨?)", flush=True)

    return kept


def merge_games(raw: list[dict]) -> list[dict]:
    """양 팀 문서 중복 박스를 1경기로 병합 (같은 날짜+매치업+스코어).

    스코어가 서로 다른 박스가 같은 날짜·매치업에 존재하면:
      1. R열 오기(r_mismatch=True) 박스는 이닝 합이 신뢰 가능한 박스와 다를 때
         유령 중복으로 간주해 폐기한다 (1983년 4/30 HT-MBC 사례).
      2. R열이 정상인 박스가 여럿이면 실제 더블헤더로 보고 유지한다.
      3. 동일 스코어의 더블헤더(단일 문서 내 복수 박스)를 올바르게 보존한다.
    """
    by: dict[tuple, list[dict]] = defaultdict(list)
    for g in raw:
        key = (g["date"], tuple(sorted([g["team1"], g["team2"]])))
        by[key].append(g)
    games = []
    for _k, v in by.items():
        by_doc: dict[str, list[dict]] = defaultdict(list)
        for g in v:
            if not g.get("r_mismatch"):
                doc_key = g.get("team_doc", g.get("team1"))
                by_doc[doc_key].append(g)

        if not by_doc:
            for g in v:
                doc_key = g.get("team_doc", g.get("team1"))
                by_doc[doc_key].append(g)

        max_boxes = max(len(boxes) for boxes in by_doc.values())
        if max_boxes == 1:
            first_doc = next(iter(by_doc))
            games.append(by_doc[first_doc][0])
        else:
            doc_with_max = next(d for d in by_doc if len(by_doc[d]) == max_boxes)
            selected = by_doc[doc_with_max]
            seen = set()
            unique_selected = []
            for b in selected:
                sig = (b["score1"], b["score2"])
                if sig in seen and len(by_doc) == 1 and all(g.get("team_doc") is None for g in v):
                    continue
                seen.add(sig)
                unique_selected.append(b)
            games.extend(unique_selected)

    return games


_OT_SEASON_HOME: list[tuple[int, str]] = [(1984, "SM"), (1987, "CB"), (1995, "TP"), (1999, "HU")]
_CC_SEASON_HOME: list[tuple[int, str]] = [(1984, "SM"), (1987, "CB")]


def _year_bounded_home(boundaries: list[tuple[int, str]], default: str | None, year: int) -> str | None:
    """연도 상한 구간표로 시즌별 홈 구단을 반환한다."""
    for max_year, code in boundaries:
        if year <= max_year:
            return code
    return default


def stadium_home(stadium: str, year: int) -> str | None:
    """경기장 홈 팀 (시즌별 코드 보정: 숭의/한밭/춘천)."""
    code = HOME_BY_STADIUM.get(stadium)
    if code == "OT":  # 숭의/인천 — 연도별 홈 구단
        return _year_bounded_home(_OT_SEASON_HOME, "SK", year)
    if code == "CC":  # 춘천 — 삼미(≤1984) → 청보(1985-87), 이후 기록 없음
        return _year_bounded_home(_CC_SEASON_HOME, None, year)
    if code == "BE":  # 한밭/청주 — 빙그레(1986-93) → 한화(1994+)
        if year < 1986:
            return None
        return _year_bounded_home([(1993, "BE")], "HH", year)
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


def apply_stadium_fixes(games: list[dict], year: int) -> list[dict]:
    """STADIUM_FIXES 등록 구장 오기를 병합 결과에 교체 (재현 가능한 수동 교정)."""
    applied = 0
    for g in games:
        key = (year, g["date"], "-".join(sorted([g["team1"], g["team2"]])))
        fix = STADIUM_FIXES.get(key)
        if not fix or g["stadium"] == fix:
            continue
        print(f"STADIUM FIX {year}-{g['date']} {g['team1']}-{g['team2']}: {g['stadium']} -> {fix}", flush=True)
        g["stadium"] = fix
        applied += 1
    if applied:
        print(f"stadium fixes applied: {applied}")
    return games


def finalize_games(games: list[dict], year: int) -> list[dict]:
    """game_id/홈-원정/경기장 단축명 부여.

    나무위키 박스는 항상 원정팀이 첫 행, 홈팀이 둘째 행 (1986 데이터 258/258
    검증, 문서별 전체 일관). 따라서 항상 team2를 홈으로 삼고, stadium_home의
    기대 코드와 다르면 크로스체크 경고만 남긴다 — 공동 홈(잠실 등)·순회
    구장도 매핑 없이 올바르게 라벨링된다.
    """
    out = []
    cnt: Counter = Counter()
    home_mismatch: Counter = Counter()
    for g in games:
        ht, at = g["team2"], g["team1"]
        hs, a_s = g["score2"], g["score1"]
        expected_home = stadium_home(g["stadium"], year)
        if expected_home and expected_home != ht:
            home_mismatch[f"{g['date']} {g['stadium']}:{expected_home}≠{ht}"] += 1
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
    if home_mismatch:
        print("stadium-home crosscheck mismatches:", dict(home_mismatch))
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
    final = finalize_games(apply_stadium_fixes(apply_score_fixes(games, year), year), year)
    ok = verify(final, year, anchors)
    # 검증 통과한 경우에만 answer set 교체 (실패 상태로 파일을 덮어쓰지 않음).
    if ok:
        out_path.write_text(json.dumps(final, ensure_ascii=False, indent=1), encoding="utf-8")
        print(f"saved {out_path}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
