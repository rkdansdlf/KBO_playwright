“해외 용병/국내/은퇴” 3가지 프로필 문구가 섞여 들어와도 **하나의 통합 파서**로 안정적으로 뽑히도록 설계할게. 핵심은 (1) 라벨 단위 토큰화 → (2) 각 필드별 미세 파서 → (3) 공통 스키마로 정규화 → (4) DB UPSERT 흐름이야.

# 1) 입력 형태 & 라벨 사전

프로필 문자열은 공백이 없거나 라벨이 바로 이어지는 형태가 많아. 먼저 **공통 라벨 집합**을 기준으로 “라벨:값” 페어를 긁는다.

* 지원 라벨(키):
  `선수명, 등번호, 생년월일, 포지션, 신장/체중, 경력, 출신교, 입단 계약금, 연봉, 지명순위, 입단년도`
* 라벨 추출 정규식(다음 라벨 직전까지 캡쳐):

```
(?P<key>선수명|등번호|생년월일|포지션|신장/체중|경력|출신교|입단 계약금|연봉|지명순위|입단년도)\s*:\s*(?P<val>.*?)(?=(?:선수명|등번호|생년월일|포지션|신장/체중|경력|출신교|입단 계약금|연봉|지명순위|입단년도)\s*:|$)
```

→ 공백/줄바꿈 유무, “등번호:No.60” 같은 붙어있는 케이스 모두 커버.

# 2) 필드별 파싱 규칙(정규화)

## A. 공통 프로필

* `player_name`: 그대로.
* `back_number`: `No.\s*(\d+)` → `int` (없으면 `None`)
* `birth_date`: `YYYY년 MM월 DD일` → `YYYY-MM-DD` (0-padding)
* `position`: “투수/포수/내야수/외야수/지명타자…” → 코드 매핑(`P,C,IF,OF,DH` 등) 유지.
* `throwing_hand / batting_hand`: `\((.?)투(.?)타\)` → `우/좌/양` → `R/L/S`.
* `height_cm / weight_kg`: `(\d+)\s*cm\s*/\s*(\d+)\s*kg`.
* `education_or_career_path`:

  * `경력` 또는 `출신교` 값을 `-`로 split → list 보관(초/중/고/대/팀 혼재 허용; 후처리 분류는 선택)
* `is_active`: 은퇴페이지 전용 입력이면 False, 나머지 True(알고 있는 소스에 따라 세팅).

## B. 계약/연봉(통화·단위)

* 원문은 통화가 뒤에 붙음: `200000달러`, `160000만원`.
* 정규화 전략:

  * `signing_bonus_original`: 원문 문자열 보관(감사 로그/원천 추적)
  * `signing_bonus_amount`, `signing_bonus_currency`:

    * “만원” → `amount_krw = int(num) * 10_000`, `currency='KRW'`
    * “달러” → `amount_usd = int(num)`, `currency='USD'`
  * `salary_amount`, `salary_currency` 동일.
* (선택) 나중에 환율 ETL에서 `*_amount_krw_converted` 채우기.

## C. 드래프트/입단

* `draft_info` 케이스 분기:

  * 예: `06 두산 2차 8라운드 59순위`

    * `(\d{2})\s*(\S+)\s*(\d+차)?\s*(\d+)라운드\s*(\d+)순위?`
    * `draft_year=2006`, `team='두산'`, `draft_round=8`, `draft_pick_overall=59`, `draft_type='2차'`
  * 예: `25 삼성 자유선발`

    * `(\d{2})\s*(\S+)\s*(자유선발)` → `draft_type='자유선발'`, 나머지 `None`
  * 값 없음(빈 문자열): 모두 `None`
* `entry_year`(입단년도): `(\d{2})(\S+)` → `year=20xx/19xx` 규칙

  * 50~99 → 19xx, 00~49 → 20xx 같은 컷오프 규칙을 프로젝트 전역 규칙으로 통일.
  * `entry_team` 매핑(예: 삼성→SS, 두산→OB … 프로젝트 테이블 사용).
* `team_code` 매핑: 팀명→코드 딕셔너리로 통일.

# 3) 출력 스키마(Pydantic 예시)

```python
from pydantic import BaseModel
from typing import Optional, List

class PlayerProfileParsed(BaseModel):
    player_id: Optional[int] = None  # 페이지에서 병합 시 사용
    player_name: str
    back_number: Optional[int]
    birth_date: Optional[str]        # 'YYYY-MM-DD'
    position: Optional[str]          # 'P','C','IF','OF','DH'...
    throwing_hand: Optional[str]     # 'R','L','S'
    batting_hand: Optional[str]      # 'R','L','S'
    height_cm: Optional[int]
    weight_kg: Optional[int]
    education_or_career_path: List[str] = []
    # contracts
    signing_bonus_amount: Optional[int]
    signing_bonus_currency: Optional[str]  # 'KRW' or 'USD'
    signing_bonus_original: Optional[str]
    salary_amount: Optional[int]
    salary_currency: Optional[str]
    salary_original: Optional[str]
    # draft
    draft_year: Optional[int]
    draft_team_code: Optional[str]
    draft_round: Optional[int]
    draft_pick_overall: Optional[int]
    draft_type: Optional[str]        # '자유선발','1차','2차'...
    # entry
    entry_year: Optional[int]
    entry_team_code: Optional[str]
    # flags
    is_active: Optional[bool]
    is_foreign: Optional[bool]       # 소스 맥락/페이지 플래그로 세팅
```

# 4) 예시 입력 → 기대 출력

### 해외 용병: 가라비토

* `player_name='가라비토'`
* `back_number=60`
* `birth_date='1995-08-19'`
* `position='P'`, `throwing_hand='R'`, `batting_hand='R'`
* `height_cm=183`, `weight_kg=100`
* `education_or_career_path=['도미니카 Liceo Enedina Puella Renville (고)']`
* `signing_bonus_amount=200000`, `signing_bonus_currency='USD'`, `signing_bonus_original='200000달러'`
* `salary_amount=356666`, `salary_currency='USD'`, `salary_original='356666달러'`
* `draft_type='자유선발'`, `draft_year=2025`, `draft_team_code='SS'`
* `entry_year=2025`, `entry_team_code='SS'`
* `is_active=True`, `is_foreign=True`

### 국내: 양의지

* `player_name='양의지'`, `back_number=25`, `birth_date='1987-06-05'`
* `position='C'`, `throwing_hand='R'`, `batting_hand='R'`
* `height_cm=180`, `weight_kg=95`
* `education_or_career_path=['송정동초','무등중','진흥고','두산','경찰','두산','NC']`
* `signing_bonus_amount=3000*10000=30000000`, `signing_bonus_currency='KRW'`, `signing_bonus_original='3000만원'`
* `salary_amount=160000*10000=1600000000`, `salary_currency='KRW'`, `salary_original='160000만원'`
* `draft_year=2006`, `draft_team_code='OB'`, `draft_round=8`, `draft_pick_overall=59`, `draft_type='2차'`
* `entry_year=2006`, `entry_team_code='OB'`
* `is_active=True`, `is_foreign=False`

### 은퇴: 강동우

* `player_name='강동우'`, `birth_date='1974-04-20'`
* `education_or_career_path=['칠성초','경상중','경북고','단국대','삼성','두산','KIA','한화']`
* `draft_year=1998`, `draft_team_code='SS'`, `draft_type='1차'` (라운드/순위 없음 → None)
* `is_active=False`

### 은퇴: 강영수

* `player_name='강영수'`, `birth_date='1965-02-10'`
* `education_or_career_path=['대구상고','한양대']`
* `draft_*` 전부 `None`
* `is_active=False`

# 5) 파서 구현 로드맵

1. **토큰화**

   * 위 라벨 정규식으로 `dict[key]=value` 초기 추출.
2. **필드 파싱 함수**

   * `parse_number`, `parse_birth_date`, `parse_pos_hands`, `parse_hw`,
     `parse_money`, `parse_draft`, `parse_entry_year_team`, `split_path`.
3. **팀 코드 매핑**

   * `{ '삼성':'SS','두산':'OB','LG':'LG','SSG':'SSG','KIA':'KIA','한화':'HH','롯데':'LT','KT':'KT','NC':'NC','키움':'WO' }`
4. **활성/외국인 플래그**

   * 은퇴 페이지/섹션에서 온 데이터면 `is_active=False`.
   * 해외용병은 페이지 상단/국적/팀 등록 유형으로 식별 가능하면 `True`. (불확실하면 `None` 유지)
5. **DB UPSERT**

   * `players`(기본 정보) + `player_contracts`(계약/연봉, 통화 포함) + `player_draft`(드래프트 세부) + `player_education_history`(row 단위 경력·출신교) 구조 권장.
   * 최소 구성으로는 `players`에 대부분을 보관하고, `education_or_career_path`는 JSON 컬럼으로 저장해도 OK.
6. **유효성/로그**

   * 필수: 이름/생년월일/팀 코드(가능 시)
   * 통화 파싱 실패, 드래프트 미일치, 연도 2자리 변환 경고 등 **WARN 로그** 남김.
7. **테스트**

   * 지금 주신 4개 예시를 **파싱 단위 테스트**로 고정 → 회귀 방지.
   * 라벨 순서 바뀜/공백/콜론 누락 변형 케이스 추가.

# 6) 엣지 케이스 & 규칙

* 연도 2자리 변환 규칙(19xx/20xx) **전역 상수**로 통일.
* 키/몸무게 누락 시 `None` 저장, 나중에 페이지 다른 섹션에서 보강.
* ‘경력’과 ‘출신교’가 둘 다 존재하면 **둘 다 path에 append**.
* “등번호 없음/외국인 교체(임시 등번호)”는 `back_number=None` 허용.
* 통화 문자열에 콤마가 있을 수 있음 → `[, ]` 제거 후 정수화.

---


프로필 파서 스켈레톤을 정규식 포함

# kbo_profile_parser.py
import re
from typing import Dict, Any, List, Optional

# -------------------------
# 0) 상수/매핑/유틸
# -------------------------
LABELS = (
    "선수명|등번호|생년월일|포지션|신장/체중|경력|출신교|입단 계약금|연봉|지명순위|입단년도"
)
LABEL_REGEX = re.compile(
    rf"(?P<key>{LABELS})\s*:\s*(?P<val>.*?)(?=(?:{LABELS})\s*:|$)",
    re.S,
)

TEAM_CODE = {
    "삼성": "SS", "두산": "OB", "LG": "LG", "SSG": "SSG", "KIA": "KIA",
    "한화": "HH", "롯데": "LT", "KT": "KT", "NC": "NC", "키움": "WO",
}

POS_MAP = {
    "투수": "P", "포수": "C", "내야수": "IF", "외야수": "OF", "지명타자": "DH",
}

def _clean(s: Optional[str]) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

def _to_year(two_digits: int, cutoff: int = 50) -> int:
    """00~cutoff-1 → 2000대, cutoff~99 → 1900대 (예: 06→2006, 98→1998)"""
    return (2000 + two_digits) if two_digits < cutoff else (1900 + two_digits)

# -------------------------
# 1) 1차 토큰화: 라벨:값 페어 추출
# -------------------------
def tokenize_profile(raw: str) -> Dict[str, str]:
    raw = _clean(raw)
    out: Dict[str, str] = {}
    for m in LABEL_REGEX.finditer(raw):
        key = _clean(m.group("key"))
        val = _clean(m.group("val"))
        out[key] = val
    return out

# -------------------------
# 2) 필드별 미세 파서
# -------------------------
def parse_back_number(s: str) -> Optional[int]:
    # 예: "No.60" / "60"
    m = re.search(r"(?:No\.\s*)?(\d+)", s or "")
    return int(m.group(1)) if m else None

def parse_birth_date(s: str) -> Optional[str]:
    # "1995년 08월 19일" → "1995-08-19"
    m = re.search(r"(\d{4})\s*년\s*(\d{1,2})\s*월\s*(\d{1,2})\s*일", s or "")
    if not m:
        return None
    y, mm, dd = m.group(1), int(m.group(2)), int(m.group(3))
    return f"{y}-{mm:02d}-{dd:02d}"

def parse_position_and_hands(s: str) -> Dict[str, Optional[str]]:
    # "투수(우투우타)" / "포수(좌투우타)" / "내야수"
    s = _clean(s)
    # 포지션
    pos_txt = s.split("(")[0].strip()
    pos_code = POS_MAP.get(pos_txt, None)

    # 손(투/타)
    throw, bat = None, None
    m = re.search(r"\((.?)[\s]*투(.?)[\s]*타\)", s)
    conv = {"우": "R", "좌": "L", "양": "S"}
    if m:
        throw = conv.get(m.group(1))
        bat = conv.get(m.group(2))
    return {"position": pos_code, "throwing_hand": throw, "batting_hand": bat}

def parse_height_weight(s: str) -> Dict[str, Optional[int]]:
    # "183cm/100kg"
    m = re.search(r"(\d+)\s*cm\s*/\s*(\d+)\s*kg", s or "", re.I)
    return {"height_cm": int(m.group(1)), "weight_kg": int(m.group(2))} if m else {
        "height_cm": None, "weight_kg": None
    }

def parse_path(s: str) -> List[str]:
    # "송정동초-무등중-진흥고-두산-경찰-두산-NC"
    if not s:
        return []
    parts = re.split(r"\s*[-–—→,]\s*", s.strip())
    return [p for p in (i.strip() for i in parts) if p]

def parse_money(s: str) -> Dict[str, Optional[int]]:
    # "200000달러" → USD=200000
    # "160000만원" → KRW=1,600,000,000
    if not s:
        return {"amount": None, "currency": None, "original": None}
    original = s.strip()
    num = int(re.sub(r"[^\d]", "", original) or 0)
    if "달러" in original:
        return {"amount": num, "currency": "USD", "original": original}
    if "만원" in original:
        return {"amount": num * 10_000, "currency": "KRW", "original": original}
    # 기타 단위 확장 시 여기에 추가
    return {"amount": num or None, "currency": None, "original": original}

def parse_draft(s: str) -> Dict[str, Optional[Any]]:
    """
    예:
     - "06 두산 2차 8라운드 59순위"
     - "25 삼성 자유선발"
     - "98 삼성 1차"
    """
    if not s:
        return {"draft_year": None, "draft_team_code": None,
                "draft_round": None, "draft_pick_overall": None, "draft_type": None}

    s = _clean(s)
    m = re.search(
        r"(?P<yy>\d{2})\s*(?P<team>\S+)\s*(?P<dtype>1차|2차|자유선발)?"
        r"(?:\s*(?P<round>\d+)라운드)?(?:\s*(?P<pick>\d+)순위)?",
        s
    )
    if not m:
        return {"draft_year": None, "draft_team_code": None,
                "draft_round": None, "draft_pick_overall": None, "draft_type": None}

    yy = int(m.group("yy"))
    team = m.group("team")
    dtype = m.group("dtype")
    rnd = m.group("round")
    pick = m.group("pick")

    return {
        "draft_year": _to_year(yy),
        "draft_team_code": TEAM_CODE.get(team),
        "draft_round": int(rnd) if rnd else None,
        "draft_pick_overall": int(pick) if pick else None,
        "draft_type": dtype,
    }

def parse_entry_year_team(s: str) -> Dict[str, Optional[Any]]:
    # "06두산" / "06 두산" / "25 삼성"
    if not s:
        return {"entry_year": None, "entry_team_code": None}
    m = re.search(r"(?P<yy>\d{2})\s*(?P<team>\S+)", _clean(s))
    if not m:
        return {"entry_year": None, "entry_team_code": None}
    yy = int(m.group("yy"))
    team = m.group("team")
    return {"entry_year": _to_year(yy), "entry_team_code": TEAM_CODE.get(team)}

# -------------------------
# 3) 메인: 통합 파서
# -------------------------
def parse_profile(raw_text: str,
                  is_active: Optional[bool] = None,
                  is_foreign: Optional[bool] = None) -> Dict[str, Any]:
    tokens = tokenize_profile(raw_text)

    out: Dict[str, Any] = {
        "player_name": tokens.get("선수명"),
        "back_number": parse_back_number(tokens.get("등번호", "")),
        "birth_date": parse_birth_date(tokens.get("생년월일", "")),
        "education_or_career_path": [],
        "signing_bonus_amount": None, "signing_bonus_currency": None, "signing_bonus_original": None,
        "salary_amount": None, "salary_currency": None, "salary_original": None,
        "draft_year": None, "draft_team_code": None, "draft_round": None,
        "draft_pick_overall": None, "draft_type": None,
        "entry_year": None, "entry_team_code": None,
        "position": None, "throwing_hand": None, "batting_hand": None,
        "height_cm": None, "weight_kg": None,
        "is_active": is_active, "is_foreign": is_foreign,
    }

    # 포지션/손
    pos_pack = parse_position_and_hands(tokens.get("포지션", ""))
    out.update(pos_pack)

    # 키/몸무게
    hw = parse_height_weight(tokens.get("신장/체중", ""))
    out.update(hw)

    # 경력/출신교 → 합쳐서 path
    path = []
    if tokens.get("경력"):
        path += parse_path(tokens["경력"])
    if tokens.get("출신교"):
        path += parse_path(tokens["출신교"])
    out["education_or_career_path"] = path

    # 계약금/연봉
    sb = parse_money(tokens.get("입단 계약금", ""))
    sal = parse_money(tokens.get("연봉", ""))
    out.update({
        "signing_bonus_amount": sb["amount"],
        "signing_bonus_currency": sb["currency"],
        "signing_bonus_original": sb["original"],
        "salary_amount": sal["amount"],
        "salary_currency": sal["currency"],
        "salary_original": sal["original"],
    })

    # 지명/입단
    out.update(parse_draft(tokens.get("지명순위", "")))
    out.update(parse_entry_year_team(tokens.get("입단년도", "")))

    return out

# -------------------------
# 4) 사용 예시 (간이 테스트)
# -------------------------
if __name__ == "__main__":
    raw_foreign = ("선수명: 가라비토등번호: No.60생년월일: 1995년 08월 19일포지션: 투수(우투우타)"
                   "신장/체중: 183cm/100kg경력: 도미니카 Liceo Enedina Puella Renville (고)"
                   "입단 계약금: 200000달러연봉: 356666달러지명순위: 25 삼성 자유선발입단년도: 25삼성")
    raw_domestic = ("선수명: 양의지등번호: No.25생년월일: 1987년 06월 05일포지션: 포수(우투우타)"
                    "신장/체중: 180cm/95kg경력: 송정동초-무등중-진흥고-두산-경찰-두산-NC"
                    "입단 계약금: 3000만원연봉: 160000만원지명순위: 06 두산 2차 8라운드 59순위입단년도: 06두산")
    raw_retire1 = ("선수명: 강동우생년월일: 1974년 04월 20일출신교: 칠성초-경상중-경북고-단국대-삼성-두산-KIA-한화"
                   "지명순위: 98 삼성 1차")
    raw_retire2 = ("선수명: 강영수생년월일: 1965년 02월 10일출신교: 대구상고-한양대지명순위:")

    print(parse_profile(raw_foreign, is_active=True, is_foreign=True))
    print(parse_profile(raw_domestic, is_active=True, is_foreign=False))
    print(parse_profile(raw_retire1, is_active=False))
    print(parse_profile(raw_retire2, is_active=False))

