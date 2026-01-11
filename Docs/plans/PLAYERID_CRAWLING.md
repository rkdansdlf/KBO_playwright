기존 players 테이블을 삭제하고 player_basic이라는 테이블로 변경하는 계획

@player_search_crawler.py
파일에서 선수 조회

“선수 조회”에서 `value='%'`(= `searchWord=%25`)로 전체 검색을 열고, 각 페이지의 표를 돌면서
`playerId, 등번호, 선수명, 팀명, 포지션, 생년월일, 체격(키/몸무게), 출신교(→ DB에 career로 저장)`를 추출해 **SQLite에 UPSERT**하는 방식으로 정리해드릴게요.

아래 예시는 **Playwright(비동기)** + **SQLAlchemy** 기준이며, 프로젝트에 바로 넣어도 되는 수준으로 작성했습니다.

---

# 1) 크롤러: 전체 페이지 순회 + 행 파싱

* 페이지네이션은 ASP.NET WebForms의 `__doPostBack` 기반이라 “다음(▶)” 버튼을 `click()` 하고,
  숨은필드 `hfPage` 값이 바뀌었는지로 전환 성공/끝 페이지를 판정합니다.
* 한 페이지의 표는 `table.tEx > tbody > tr`로 잡히고, 각 `tr`의 `td` 순서는 아래처럼 가정합니다.
  `0=선수명(링크에 playerId) 1=등번호 2=팀명 3=포지션 4=생년월일 5=체격 6=출신교`
* 체격은 `"180cm/80kg"` 같은 문자열이라 **키/몸무게**를 숫자로 파싱합니다.
* 생년월일은 `YYYY-MM-DD` 또는 `YYYY.MM.DD` 등 **여러 포맷**이 섞일 수 있어, 간단 파서를 둡니다.

```python
# src/crawlers/player_search_crawler.py
import asyncio
import re
import time
from dataclasses import dataclass
from datetime import date
from urllib.parse import urlparse, parse_qs

from playwright.async_api import async_playwright

SEARCH_URL = "https://www.koreabaseball.com/Player/Search.aspx?searchWord=%25"
TABLE_ROWS = "table.tEx tbody tr"
NEXT_BTN = "a[id$='ucPager_btnNext']"
HFPAGE = "input[id$='hfPage']"

REQUEST_DELAY_SEC = 1.0
TIMEOUT_MS = 15000

@dataclass
class PlayerRow:
    player_id: int
    uniform_no: str | None
    name: str
    team: str | None
    position: str | None
    birth_date: str | None   # 원문 문자열 그대로 저장(파싱값은 별도 칼럼에 넣고 싶으면 추가)
    height_cm: int | None
    weight_kg: int | None
    career: str | None       # 출신교 → career 필드로 저장

def _parse_height_weight(s: str) -> tuple[int | None, int | None]:
    # 예: "180cm/80kg", "180cm / 80kg", "180/80", "-" 등
    if not s:
        return None, None
    s = s.replace(" ", "")
    m = re.search(r"(\d{2,3})\s*cm?[/ ]?(\d{2,3})\s*kg?", s, re.IGNORECASE)
    if m:
        h = int(m.group(1))
        w = int(m.group(2))
        # 말도 안되는 수치 방지
        if 140 <= h <= 220 and 45 <= w <= 150:
            return h, w
    return None, None

def _extract_player_id(href: str | None) -> int | None:
    if not href:
        return None
    try:
        q = parse_qs(urlparse(href).query)
        pid = q.get("playerId", [None])[0]
        return int(pid) if pid and pid.isdigit() else None
    except Exception:
        m = re.search(r"playerId=(\d+)", href)
        return int(m.group(1)) if m else None

async def _collect_page_rows(page) -> list[PlayerRow]:
    rows = page.locator(TABLE_ROWS)
    count = await rows.count()
    results: list[PlayerRow] = []
    for i in range(count):
        r = rows.nth(i)
        tds = r.locator("td")
        tdc = await tds.count()
        if tdc < 7:
            continue

        # 선수명 + 링크
        name_el = tds.nth(0).locator("a")
        href = await name_el.get_attribute("href")
        name = (await name_el.inner_text()).strip()
        player_id = _extract_player_id(href)

        uniform_no = (await tds.nth(1).inner_text()).strip() or None
        team = (await tds.nth(2).inner_text()).strip() or None
        position = (await tds.nth(3).inner_text()).strip() or None
        birth = (await tds.nth(4).inner_text()).strip() or None
        body = (await tds.nth(5).inner_text()).strip() or ""
        career = (await tds.nth(6).inner_text()).strip() or None

        h, w = _parse_height_weight(body)

        if player_id is None:
            continue
        results.append(PlayerRow(
            player_id=player_id,
            uniform_no=uniform_no if uniform_no != "-" else None,
            name=name,
            team=team if team != "-" else None,
            position=position if position != "-" else None,
            birth_date=birth if birth and birth != "-" else None,
            height_cm=h,
            weight_kg=w,
            career=career if career != "-" else None
        ))
    return results

async def crawl_all_players(max_pages: int | None = None) -> list[PlayerRow]:
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context()
        page = await ctx.new_page()

        await page.goto(SEARCH_URL, wait_until="domcontentloaded")
        await page.wait_for_selector(TABLE_ROWS, timeout=TIMEOUT_MS)

        all_rows: list[PlayerRow] = []
        pages_visited = 0
        while True:
            all_rows += await _collect_page_rows(page)

            # 다음 페이지 이동
            prev = await page.locator(HFPAGE).input_value()
            next_btn = page.locator(NEXT_BTN)
            if not await next_btn.is_visible():
                break

            await next_btn.click()
            try:
                await page.wait_for_function(
                    """(sel, prev) => {
                        const el = document.querySelector(sel);
                        return el && el.value && el.value !== prev;
                    }""",
                    (HFPAGE, prev),
                    timeout=TIMEOUT_MS
                )
            except Exception:
                break

            await page.wait_for_selector(TABLE_ROWS, timeout=TIMEOUT_MS)
            time.sleep(REQUEST_DELAY_SEC)
            pages_visited += 1
            if max_pages is not None and pages_visited >= max_pages:
                break

        await ctx.close()
        await browser.close()
        return all_rows

if __name__ == "__main__":
    players = asyncio.run(crawl_all_players())
    print(f"rows: {len(players)}")
    # for r in players[:3]: print(r)
```

---

# 2) DB 스키마(예: SQLite) + UPSERT

* 테이블: `player_basic`
* 기본키: `player_id`
* 중복 수집 시 **ON CONFLICT … DO UPDATE**로 갱신
* 출신교는 `career` 컬럼으로 저장

```python
# src/db/models.py
from sqlalchemy.orm import declarative_base, Mapped, mapped_column
from sqlalchemy import String, Integer, Date, UniqueConstraint

Base = declarative_base()

class PlayerBasic(Base):
    __tablename__ = "player_basic"
    # playerId는 KBO의 고유 식별자
    player_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=False)

    name: Mapped[str] = mapped_column(String(100), nullable=False)
    uniform_no: Mapped[str | None] = mapped_column(String(10))
    team: Mapped[str | None] = mapped_column(String(50))
    position: Mapped[str | None] = mapped_column(String(50))

    # 원문 생년월일 문자열 보관(원한다면 birth_date_date 같은 파싱칼럼 추가)
    birth_date: Mapped[str | None] = mapped_column(String(20))

    height_cm: Mapped[int | None] = mapped_column(Integer)
    weight_kg: Mapped[int | None] = mapped_column(Integer)

    career: Mapped[str | None] = mapped_column(String(200))  # 출신교
```

```python
# src/db/session.py
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

DB_URL = os.getenv("DATABASE_URL", "sqlite:///./data/kbo_dev.db")

engine = create_engine(DB_URL, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
```

```python
# src/db/init_db.py
from .session import engine
from .models import Base

def init_db():
    Base.metadata.create_all(bind=engine)
```

```python
# src/db/upsert.py
from sqlalchemy import insert
from sqlalchemy.orm import Session
from .models import PlayerBasic

def upsert_players(session: Session, rows: list[dict]):
    """
    rows: dict(player_id, name, uniform_no, team, position, birth_date, height_cm, weight_kg, career)
    """
    if not rows:
        return
    stmt = insert(PlayerBasic).values(rows)
    # SQLite 3.24+ 지원
    update_cols = {c.name: c for c in PlayerBasic.__table__.c if c.name != "player_id"}
    stmt = stmt.on_conflict_do_update(
        index_elements=[PlayerBasic.__table__.c.player_id],
        set_=update_cols
    )
    session.execute(stmt)
```

---

# 3) 수집 → 저장 오케스트레이터

크롤링 결과를 dict로 변환해서 UPSERT 합니다.

```python
# scripts/crawl_players.py
import asyncio
from sqlalchemy.orm import Session

from src.crawlers.player_search_crawler import crawl_all_players
from src.db.session import SessionLocal
from src.db.init_db import init_db
from src.db.upsert import upsert_players

def _to_dict(row) -> dict:
    return {
        "player_id": row.player_id,
        "name": row.name,
        "uniform_no": row.uniform_no,
        "team": row.team,
        "position": row.position,
        "birth_date": row.birth_date,
        "height_cm": row.height_cm,
        "weight_kg": row.weight_kg,
        "career": row.career,
    }

def main():
    init_db()

    players = asyncio.run(crawl_all_players())  # 전 페이지
    print(f"크롤링 완료: {len(players)}명")

    payload = [_to_dict(p) for p in players]

    with SessionLocal() as session:  # type: Session
        upsert_players(session, payload)
        session.commit()
    print("DB 업서트 완료")

if __name__ == "__main__":
    main()
```

---

# 4) 사용법

```bash
# 1) 의존성 설치
pip install playwright sqlalchemy
playwright install chromium

# 2) (선택) .env / 환경변수
export DATABASE_URL=sqlite:///./data/kbo_dev.db

# 3) 실행
python scripts/crawl_players.py

# 4) 검증 (sqlite3 CLI)
sqlite3 ./data/kbo_dev.db "SELECT COUNT(*) FROM player_basic;"
sqlite3 ./data/kbo_dev.db "SELECT * FROM player_basic LIMIT 5;"
```

---

# 5) 실무 팁 & 예외 처리

* **요청 간격**: 서버 보호 차원에서 페이지 전환 사이에 `REQUEST_DELAY_SEC=1.0~1.5s` 유지.
* **빈 값 처리**: `-` 또는 공란은 `None`으로 정규화(누락 데이터와 구분).
* **체격 포맷 다양성**: `"180/80"`, `"180cm / 80kg"`, `"-"` 등 변형 대비 정규식 허용 범위를 넓혔습니다.
* **생년월일**: 원문 문자열을 우선 저장. 필요하면 별도 마이그레이션으로 `birth_date_date DATE` 칼럼을 추가하고 파싱 값을 채워도 좋습니다.
* **재실행 안전성**: `ON CONFLICT(player_id) DO UPDATE`로 중복 걱정 없이 갱신됩니다.
* **부분 장애 대비**: 로컬 개발이나 CI에서는 `crawl_all_players(max_pages=2)`로 빠르게 스모크 테스트 가능.

---

좋습니다. 아래에 **마이그레이션 SQL**, **백필(파싱) 스크립트**, **모델 패치**와 **실행 절차**를 한 번에 드릴게요. (기본 DB는 SQLite 가정, Supabase/Postgres용도 함께 제공합니다.)

---

# 1) SQLite 마이그레이션 SQL

`backend/db/migrations/0002_player_basic_indexes_birthdate.sql`

```sql
-- 0002: player_basic 인덱스 추가 + birth_date_date 컬럼 추가 (SQLite)

PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;

-- 1) 생년월일 파싱 저장용 컬럼 추가 (NULL 허용)
ALTER TABLE player_basic
  ADD COLUMN birth_date_date TEXT;  -- YYYY-MM-DD 형식으로 저장(ISO8601)

-- 2) 조회 최적화를 위한 인덱스
CREATE INDEX IF NOT EXISTS idx_player_basic_name     ON player_basic(name);
CREATE INDEX IF NOT EXISTS idx_player_basic_team     ON player_basic(team);
CREATE INDEX IF NOT EXISTS idx_player_basic_position ON player_basic(position);
CREATE INDEX IF NOT EXISTS idx_player_basic_team_pos ON player_basic(team, position);

-- (참고) name NOT NULL, player_id PK는 0001에서 이미 적용되어 있다고 가정
-- SQLite는 기존 컬럼에 NOT NULL 제약 추가가 어려우므로, 추가 제약이 필요하면
-- 새 테이블을 만들어 옮기는 방식이 필요합니다. (아래 주석 템플릿 참고)

COMMIT;
PRAGMA foreign_keys=ON;

-- --- [선택] 더 강한 제약이 필요할 때(예시) ---
-- BEGIN TRANSACTION;
-- CREATE TABLE player_basic_new (
--   player_id   INTEGER PRIMARY KEY,        -- NOT NULL + PK
--   name        TEXT    NOT NULL,           -- NOT NULL 강제
--   uniform_no  TEXT,
--   team        TEXT,
--   position    TEXT,
--   birth_date  TEXT,
--   height_cm   INTEGER,
--   weight_kg   INTEGER,
--   career      TEXT,
--   birth_date_date TEXT
-- );
-- INSERT INTO player_basic_new
--   (player_id, name, uniform_no, team, position, birth_date, height_cm, weight_kg, career, birth_date_date)
-- SELECT
--   player_id, name, uniform_no, team, position, birth_date, height_cm, weight_kg, career, birth_date_date
-- FROM player_basic
-- WHERE name IS NOT NULL; -- 새 제약 충족
-- DROP TABLE player_basic;
-- ALTER TABLE player_basic_new RENAME TO player_basic;
-- CREATE INDEX idx_player_basic_name      ON player_basic(name);
-- CREATE INDEX idx_player_basic_team      ON player_basic(team);
-- CREATE INDEX idx_player_basic_position  ON player_basic(position);
-- CREATE INDEX idx_player_basic_team_pos  ON player_basic(team, position);
-- COMMIT;
```

---

# 2) Supabase/Postgres 마이그레이션 SQL (선택)

`backend/db/migrations_pg/0002_player_basic_indexes_birthdate.sql`

```sql
-- 0002 for Postgres

BEGIN;

ALTER TABLE public.player_basic
  ADD COLUMN IF NOT EXISTS birth_date_date date;

-- 인덱스
CREATE INDEX IF NOT EXISTS idx_player_basic_name      ON public.player_basic(name);
CREATE INDEX IF NOT EXISTS idx_player_basic_team      ON public.player_basic(team);
CREATE INDEX IF NOT EXISTS idx_player_basic_position  ON public.player_basic(position);
CREATE INDEX IF NOT EXISTS idx_player_basic_team_pos  ON public.player_basic(team, position);

COMMIT;
```

---

# 3) 모델 패치 (SQLAlchemy)

`src/db/models.py` – `PlayerBasic`에 `birth_date_date` 추가

```python
from sqlalchemy import String, Integer, Date  # ← Date 가져오기

class PlayerBasic(Base):
    __tablename__ = "player_basic"

    player_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=False)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    uniform_no: Mapped[str | None] = mapped_column(String(10))
    team: Mapped[str | None] = mapped_column(String(50))
    position: Mapped[str | None] = mapped_column(String(50))
    birth_date: Mapped[str | None] = mapped_column(String(20))
    height_cm: Mapped[int | None] = mapped_column(Integer)
    weight_kg: Mapped[int | None] = mapped_column(Integer)
    career: Mapped[str | None] = mapped_column(String(200))

    # 신규: 파싱된 생년월일(ISO 날짜)
    birth_date_date: Mapped["date | None"] = mapped_column(Date, nullable=True)
```

> 주의: SQLite는 내부적으로 `DATE`를 텍스트로 저장하지만, SQLAlchemy가 파싱/직렬화를 도와주므로 컬럼 타입은 `Date`로 유지하세요.

---

# 4) 생년월일 백필 스크립트 (파싱)

`scripts/backfill_birthdates.py`

```python
import os
from datetime import datetime
from typing import Iterable

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from src.db.session import SessionLocal
from src.db.models import PlayerBasic

# 다양한 표기를 커버하는 포맷들
_FORMATS: tuple[str, ...] = (
    "%Y-%m-%d",
    "%Y.%m.%d",
    "%Y/%m/%d",
    "%Y%m%d",
    "%y-%m-%d",   # 드물지만 대비
    "%y.%m.%d",
    "%y/%m/%d",
)

def _parse_birth_date(raw: str | None) -> "datetime.date | None":
    if not raw:
        return None
    s = raw.strip().replace(" ", "")
    # 숫자/구분자 혼합 정규화(Optional): 여긴 일단 포맷 시도 우선
    for fmt in _FORMAT_S:
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    # 추가: 1990.7.3처럼 0패딩이 없는 경우 처리
    try:
        parts = (
            s.replace("-", ".")
             .replace("/", ".")
             .split(".")
        )
        if len(parts) == 3 and all(p.isdigit() for p in parts):
            y = int(parts[0])
            m = int(parts[1])
            d = int(parts[2])
            if 1900 <= y <= 2100 and 1 <= m <= 12 and 1 <= d <= 31:
                return datetime(y, m, d).date()
    except Exception:
        pass
    return None

def backfill(limit: int | None = None) -> int:
    updated = 0
    with SessionLocal() as session:  # type: Session
        stmt = select(PlayerBasic).where(
            PlayerBasic.birth_date.isnot(None),
            PlayerBasic.birth_date != "",
            PlayerBasic.birth_date != "-",
            (PlayerBasic.birth_date_date.is_(None))
        )
        if limit:
            stmt = stmt.limit(limit)

        for row in session.scalars(stmt):
            dt = _parse_birth_date(row.birth_date)
            if dt:
                session.execute(
                    update(PlayerBasic)
                    .where(PlayerBasic.player_id == row.player_id)
                    .values(birth_date_date=dt)
                )
                updated += 1
        session.commit()
    return updated

if __name__ == "__main__":
    n = backfill()
    print(f"birth_date_date 백필 완료: {n}건")
```

> 필요 시 `limit` 파라미터로 소량 테스트 후 전체 실행하세요.

---

# 5) 실행 절차

```bash
# 0) 가상환경 / 의존성
pip install sqlalchemy

# 1) (SQLite) 마이그레이션 적용
sqlite3 ./data/kbo_dev.db < backend/db/migrations/0002_player_basic_indexes_birthdate.sql

# 2) 모델 패치 반영(코드 변경 반영)
#   - src/db/models.py 수정 저장

# 3) 생년월일 백필
python scripts/backfill_birthdates.py

# 4) 검증
sqlite3 ./data/kbo_dev.db "PRAGMA table_info(player_basic);"
sqlite3 ./data/kbo_dev.db "SELECT COUNT(*) FROM player_basic WHERE birth_date IS NOT NULL;"
sqlite3 ./data/kbo_dev.db "SELECT COUNT(*) FROM player_basic WHERE birth_date_date IS NOT NULL;"
sqlite3 ./data/kbo_dev.db "SELECT player_id, name, birth_date, birth_date_date FROM player_basic WHERE birth_date_date IS NULL AND birth_date IS NOT NULL LIMIT 20;"
```

( Supabase/Postgres 를 병행한다면 1단계에서 `psql`로 PG용 SQL을 적용하고, `backfill_birthdates.py`는 같은 코드로 동작합니다. )

---

# 6) 왜 이렇게 설계했나 (간단 요약)

* **인덱스**: `name`, `team`, `position`, `(team, position)`은 조회/필터 빈도가 높아 질 확률이 큽니다.
* **NOT NULL**: 이미 `name`은 NOT NULL/`player_id`는 PK. 그 외 컬럼은 현행 크롤링 데이터에 공란이 많아 강제하지 않았습니다. 강제 필요 시 **새 테이블 생성→데이터 이관** 패턴(주석 템플릿)으로 적용하세요.
* **birth_date_date**: 원문 문자열(`birth_date`)을 보존하면서 파싱된 날짜 컬럼을 별도로 두면, 검색/통계를 날짜형으로 정확히 처리할 수 있고 원문 유지로 회귀·검증이 용이합니다.

