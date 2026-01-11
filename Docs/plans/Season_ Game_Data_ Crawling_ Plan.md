“게임센터(박스스코어)” DOM 특성/KBO 표기 관행 때문에 몇 군데만 보완하면 훨씬 단단해집니다. 

# 크롤링 로직 점검(수정 포인트)

1. **브라우저 생성/재사용**

* 현재 `crawl_game_detail()`마다 `launch()`→`close()`를 반복합니다. 한 번 띄운 `browser/context`를 여러 경기에서 **재사용**하세요(동시성도 Page 단위로 제한). 이렇게 하면 메모리/시간을 크게 줄입니다.

2. **대기 조건 강화**

* `networkidle`만으로는 동적 테이블이 늦게 붙는 경우가 있습니다. “진짜 필요한 것”을 기다리세요. 예:

  * `.box-score-area`
  * `.tblAwayHitter1, .tblHomeHitter1`
  * `.pitcher-record-area`
* 셀렉터 자체는 프로젝트 레퍼런스와 일치합니다. 그대로 쓰되 “최소 1개 테이블 등장”을 보장하는 식으로 대기하세요. 

3. **스코어보드 파싱 안정화**

* `soup.find('table', class_='tbl')`는 애매합니다. 박스스코어 영역 내부에서 **이닝 헤더(th)가 숫자인 테이블**을 찾아 그 인덱스까지 슬라이스 하세요(연장전 10~12회 변수 대응). 현재처럼 `cells[1:-3]` 고정 슬라이스는 페이지 스펙 바뀌면 금방 깨져요.

4. **타자 테이블 병합/순서**

* `.tblAwayHitter1/2/3`(교체, 대타 등)을 **순서대로 이어붙이는 것**은 좋아요. 다만 “합계/팀합계” 같은 마지막 행이 하나 이상 있을 수 있어 `rows[:-1]` 대신 `th/td` 첫 칸이 “합계”인 행은 **조건 필터로 제외**하는 쪽이 안전합니다. 셀렉터/테이블 분할은 문서 기준과 일치합니다. 
* 컬럼 수가 경기/연도별로 미묘하게 달 수 있으니 **헤더 텍스트→인덱스 매핑**으로 가져가면 내구성이 좋아집니다.

5. **투수 IP·결정(승/패/세/홀드) 파싱**

* KBO는 이닝을 `1⅔`, `1 2/3` 같이 표기하기도 해서 `float()` 변환이 깨집니다. `⅓→0.3333`, `⅔→0.6667` 또는 `X 1/3, X 2/3` 패턴을 **정규식으로 치환**한 뒤 `Decimal`로 저장하세요(스키마 v2에서도 분수 이닝을 소수로 통일하는 컨벤션을 권장). 
* 승패/세/홀드 아이콘은 보통 아이콘/스팬으로 붙습니다. 이름 문자열에서 `'W','L','S','H'`를 단순 치환하면 **선수명 글자까지 지울 수** 있어요. **아이콘 텍스트만 따로 읽고, 이름은 `<a>` 텍스트만** 추출하세요.

6. **playerId 추출 내구성**

* `href.split('playerId=')[1].split('&')[0]` 대신 `urllib.parse.urlparse/parse_qs`로 **파라미터 안전 파싱** 권장(상대경로/클릭 로깅 파라미터 등 예외 대비).

7. **경기 정보 텍스트 파싱**

* “구장 : 문학  관중 : 22,500 …” 처럼 콜론이 여럿이라 `split(':')[-1]`는 취약합니다. `re.search(r'관중\s*:\s*([\d,]+)')` 같은 **명시적 정규식**으로 뽑으세요. 관중이 ‘-’인 경기도 있으니 `None` 허용.

8. **팀명 → team_id 매핑**

* `away_team/home_team`에 **문자열 팀명**을 저장하지 말고 DB `team_id`(LG/SSG/… 코드)로 변환해 넣으세요. 프로젝트에 웹 코드↔DB 코드 표가 있으니 그 매핑을 그대로 사용하세요. 

# DB 저장 설계 점검(키/타입/동일성)

1. **키/무결성**

* `games`: PK=`game_id`, `game_date`+`start_time`는 보조 인덱스. 총합(R/H/E/LOB)도 컬럼으로 보유 추천.
* `inning_scores`: **UNIQUE(game_id, team_type, inning)** 필요(ON DUPLICATE KEY가 동작하려면 키가 있어야 함).
* `game_batting_stats`: **UNIQUE(game_id, player_id)**(교체 타자 여러 번 등장 시 `appearance_seq`까지 포함 고려). `batting_order`는 “선발 오더” 의미와 “출전 순서”가 섞일 수 있어 `is_starter`/`appearance_seq` 2필드 구조 권장.
* `game_pitching_stats`: **UNIQUE(game_id, player_id)**. `innings_pitched`는 **`DECIMAL(5,3)`** 권장(⅓ 단위). 결정(`decision`)은 ENUM('W','L','S','H', NULL)로.
  v2 스키마 가이드의 통일 소수화/체크 제약 아이디어를 그대로 반영하면 깔끔합니다. 

2. **타입/정규화**

* `attendance`는 INT, `start_time/end_time`은 TIME, `game_duration`은 **분 단위 INT**(표기는 `H:MM`라도 저장은 분) 추천.
* 타자/투수 경기 기록은 시즌 누적 계산의 원천이므로 **NULL 보다는 0 기본값**을 일관 적용.

3. **팀 측면 식별**

* `team_type`('home'/'away') + 선수의 실제 소속팀(`team_id`)을 **둘 다** 저장하면, 트레이드 직후 경기 등 특수 케이스를 올바르게 표현 가능합니다.

# 품질/성능/안전성

* **아이템포턴시**: 동일 `game_id` 재실행 시 완전 덮어쓰기 되도록 모든 테이블에 `ON DUPLICATE KEY UPDATE` + 적절한 UNIQUE 키를 보장하세요.
* **동시성 제한**: `Semaphore` 3개 이하는 적절. 스케줄 자동화·주기 설정은 기존 스케줄 문서 플로우에 맞추면 됩니다(일일/주간 잡 구조). 
* **셀렉터/URL 레퍼런스 문서 동기화**: 박스스코어 URL·셀렉터 정의는 이미 프로젝트 문서에 반영되어 있으니(원정/홈 타자 테이블, 투수 영역 등) 코드 주석에 해당 문서 경로를 남겨두세요. 
* **KBO 표 구조 변화 대응**: 타자 상세는 “두 개의 테이블로 분리되는 케이스”가 대표적 이슈입니다(이번엔 박스스코어지만, 같은 접근—헤더 매핑/테이블 병합—을 재사용). 

# 최소 보완 코드 스니펫(핵심만)

* **IP 문자열 → 소수 변환**

```python
import re
from decimal import Decimal

def parse_ip(text: str) -> Decimal:
    s = text.strip().replace('⅓',' 1/3').replace('⅔',' 2/3')
    m = re.match(r'^\s*(\d+)(?:\s+(\d)/3)?\s*$', s)
    if not m:  # 예: "0" 또는 빈값
        return Decimal('0')
    whole = Decimal(m.group(1))
    frac = Decimal(m.group(2))/Decimal(3) if m.group(2) else Decimal('0')
    return whole + frac
```

* **팀명 → team_id 매핑(예시)**

```python
TEAM_MAP = {"LG":"LG","한화":"HH","SSG":"SSG","삼성":"SS","NC":"NC","KT":"KT",
            "롯데":"LT","KIA":"KIA","두산":"OB","키움":"WO"}  # 문서 표에 맞춤
```



---

## 결론

* 전반 구조(Playwright → 파싱 → DB upsert)는 적절합니다.
* 실전에서 가장 많이 터지는 지점이 **이닝/결정 표기 파싱, 스코어 테이블 인덱싱, 팀코드 매핑, UNIQUE 키 설계**인데, 위 보완만 적용하면 안정적으로 굴러갑니다.
* 셀렉터/URL/팀코드·스케줄·스키마 규칙은 프로젝트 문서와 **동기화**해 두세요. (GameCenter 셀렉터/URL 레퍼런스, v2 스키마/이닝 소수화, 스케줄 자동화 가이드)   

필요하시면 `games/inning_scores/game_batting_stats/game_pitching_stats`의 추천 DDL도 바로 잡아 드릴게요.
