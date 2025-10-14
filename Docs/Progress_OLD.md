“선수 프로필 페이지에서 퓨처스 리그 시즌 타격 기록(AVG, G, AB, R, H, 2B, 3B, HR, RBI, SB, BB, HBP, SO, SLG, OBP)만 뽑아와서 DB에 넣는” 최소·견고 코드 패턴을 바로 쓸 수 있게 정리했어요.
핵심은 (1) HTML 안전 추출 → (2) 헤더 정규화 → (3) 값 정수/실수 변환 → (4) 누락 파생지표 보완 → (5) SQLAlchemy 업서트 입니다.

0) 요약 사용법
rows = await fetch_and_parse_futures_batting(player_id="77999", profile_url=URL)
save_futures_batting(player_id_db=1234, rows=rows, season_type="FUTURES")

1) 크롤러(+파서): Playwright + BeautifulSoup
# src/crawlers/futures_batting.py
import asyncio
import re
from typing import List, Dict, Optional
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

FUTURES_KEYS = [
    "season","AVG","G","AB","R","H","2B","3B","HR","RBI","SB","BB","HBP","SO","SLG","OBP"
]

HEADER_MAP = {
    # 한국어/영문 혼재 대응 → 표준 키로 통일
    "연도": "season", "년도": "season", "시즌": "season", "year": "season",
    "경기": "G", "g": "G",
    "타수": "AB", "ab": "AB",
    "득점": "R", "r": "R",
    "안타": "H", "h": "H",
    "2루타": "2B", "2b": "2B",
    "3루타": "3B", "3b": "3B",
    "홈런": "HR", "hr": "HR",
    "타점": "RBI", "rbi": "RBI",
    "도루": "SB", "sb": "SB",
    "볼넷": "BB", "bb": "BB",
    "사구": "HBP", "hbp": "HBP", "죽사구": "HBP",
    "삼진": "SO", "so": "SO",
    "타율": "AVG", "avg": "AVG",
    "장타율": "SLG", "slg": "SLG",
    "출루율": "OBP", "obp": "OBP",
}

def _norm_header(txt: str) -> str:
    t = re.sub(r"\s+", "", txt).lower()
    return HEADER_MAP.get(t, txt.strip())

def _to_int(x: Optional[str]) -> Optional[int]:
    if x is None: return None
    t = x.strip().replace(",", "")
    if t in ("", "-", "—"): return None
    return int(re.sub(r"[^\d-]", "", t))

def _to_float(x: Optional[str]) -> Optional[float]:
    if x is None: return None
    t = x.strip().replace(",", "")
    if t in ("", "-", "—"): return None
    # 일부 페이지에서 . 대신 0.000 형식/퍼센트 혼용 방지
    t = re.sub(r"[^\d\.]", "", t)
    return float(t) if t else None

def _compute_missing(row: Dict):
    # 필요한 파생만 보완(가능한 경우에 한해)
    # SLG, OBP는 표에 거의 있지만, 없을 경우 간이 계산(1B를 H-2B-3B-HR로)
    H = row.get("H"); _2B=row.get("2B"); _3B=row.get("3B"); HR=row.get("HR"); AB=row.get("AB")
    BB=row.get("BB"); HBP=row.get("HBP"); SF=row.get("SF")  # 없으면 None
    if "SLG" not in row or row.get("SLG") is None:
        if None not in (H, _2B, _3B, HR, AB) and AB and AB>0:
            _1B = H - sum(v or 0 for v in [_2B, _3B, HR])
            tb = ( _1B or 0 ) + 2*(_2B or 0) + 3*(_3B or 0) + 4*(HR or 0)
            row["SLG"] = round(tb/AB, 3)
    if "OBP" not in row or row.get("OBP") is None:
        denom = (AB or 0) + (BB or 0) + (HBP or 0) + (SF or 0)
        if denom>0:
            row["OBP"] = round(((H or 0) + (BB or 0) + (HBP or 0))/denom, 3)
    return row

def _parse_table(table) -> List[Dict]:
    # thead → 표준 헤더로 매핑
    headers = [ _norm_header(th.get_text(strip=True)) for th in table.select("thead th, thead td") ]
    # tbody → 시즌별 레코드
    out = []
    for tr in table.select("tbody tr"):
        cells = [td.get_text(strip=True) for td in tr.find_all(["td","th"])]
        if not cells or any(tag.get("class")==["totals"] for tag in tr.parents):  # 합계 표시는 건너뛰기
            continue
        row = {}
        for h, v in zip(headers, cells):
            key = _norm_header(h)
            if key == "season":
                # "2023", "2023 (퓨처스)" 등 → 숫자만 추출
                m = re.search(r"\d{4}", v)
                if not m: 
                    row["season"] = None
                else:
                    row["season"] = int(m.group())
            elif key in ("AVG","SLG","OBP"):
                row[key] = _to_float(v)
            elif key in ("G","AB","R","H","2B","3B","HR","RBI","SB","BB","HBP","SO","SF"):
                row[key] = _to_int(v)
            # 불필요 헤더는 무시
        # 필수 시즌/경기수 없는 행은 스킵
        if not row.get("season"): 
            continue
        out.append(_compute_missing(row))
    return out

def _pick_futures_table(soup: BeautifulSoup):
    """
    '퓨처스' 섹션 테이블을 최대한 안전하게 찾는다.
    - 탭/제목에 '퓨처스'가 붙은 섹션의 첫 번째 table
    - 없으면 'AVG','OBP','SLG'를 모두 포함하는 헤더를 가진 테이블 중 '연도/년도' 컬럼이 있는 것
    """
    # 1) '퓨처스' 문자열 근처의 테이블
    label = soup.find(lambda tag: tag.name in ["h2","h3","h4","button","a","li","span"] and "퓨처스" in tag.get_text())
    if label:
        nxt = label.find_next("table")
        if nxt: return nxt
    # 2) 헤더 기반 휴리스틱
    for t in soup.find_all("table"):
        headers = [ _norm_header(th.get_text(strip=True)) for th in t.select("thead th, thead td") ]
        if not headers: 
            continue
        if {"season","AVG","OBP","SLG"}.issubset(set(headers)):
            return t
    return None

async def fetch_and_parse_futures_batting(player_id: str, profile_url: str) -> List[Dict]:
    """
    해당 선수 프로필(혹은 통합 기록) 페이지에서 '퓨처스' 시즌 타격 테이블만 파싱하여
    dict 리스트로 반환. (각 dict는 한 시즌)
    """
    async with async_playwright() as p:
        br = await p.chromium.launch(headless=True)
        page = await br.new_page()
        try:
            # 예) profile_url = f"https://www.koreabaseball.com/Record/Player/HitterDetail.aspx?playerId={player_id}"
            await page.goto(profile_url, wait_until="domcontentloaded", timeout=30000)
            # 탭/버튼으로 '퓨처스' 전환이 필요한 페이지라면, 버튼 클릭 로직을 추가
            # ex) await page.click("text=퓨처스")
            html = await page.content()
        finally:
            await br.close()

    soup = BeautifulSoup(html, "html.parser")
    table = _pick_futures_table(soup)
    if not table:
        return []  # 없을 수 있음(퓨처스 기록이 없는 선수)
    rows = _parse_table(table)

    # 요청된 컬럼만 추려서, 누락은 None 채우기
    trimmed = []
    for r in rows:
        item = {k: r.get(k) for k in FUTURES_KEYS}
        trimmed.append(item)
    return trimmed

2) 저장(업서트): SQLAlchemy Core (SQLite/MySQL 겸용)
# src/repository/save_futures_batting.py
from typing import List, Dict
from sqlalchemy import Table, Column, Integer, Float, String, MetaData, insert
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session
from sqlalchemy import create_engine

# player_season_batting과 동일/유사한 테이블이라고 가정
# PK: (player_id, season, league, level)
metadata = MetaData()
player_season_batting = Table(
    "player_season_batting", metadata,
    Column("player_id", Integer, primary_key=True),
    Column("season", Integer, primary_key=True),
    Column("league", String(16), primary_key=True),   # 'FUTURES'
    Column("level", String(16), primary_key=True, default="KBO2"),
    Column("franchise_id", Integer, nullable=True),
    Column("identity_id", Integer, nullable=True),
    Column("G", Integer), Column("AB", Integer), Column("R", Integer), Column("H", Integer),
    Column("2B", Integer), Column("3B", Integer), Column("HR", Integer), Column("RBI", Integer),
    Column("BB", Integer), Column("HBP", Integer), Column("SO", Integer), Column("SB", Integer),
    Column("AVG", Float), Column("OBP", Float), Column("SLG", Float),
    Column("source", String(16), default="PROFILE")
)

def save_futures_batting(player_id_db: int, rows: List[Dict], engine=None, league="FUTURES", level="KBO2"):
    """rows: fetch_and_parse_futures_batting() 결과"""
    if not rows:
        return 0
    engine = engine or create_engine("sqlite:///./data/kbo_dev.db", pool_pre_ping=True)
    dialect = engine.dialect.name

    with engine.begin() as cx:
        saved = 0
        for r in rows:
            values = {
                "player_id": player_id_db,
                "season": r["season"],
                "league": league,
                "level": level,
                "G": r.get("G"), "AB": r.get("AB"), "R": r.get("R"), "H": r.get("H"),
                "2B": r.get("2B"), "3B": r.get("3B"), "HR": r.get("HR"), "RBI": r.get("RBI"),
                "BB": r.get("BB"), "HBP": r.get("HBP"), "SO": r.get("SO"), "SB": r.get("SB"),
                "AVG": r.get("AVG"), "OBP": r.get("OBP"), "SLG": r.get("SLG"),
                "source": "PROFILE",
            }
            if dialect == "sqlite":
                stmt = sqlite_insert(player_season_batting).values(**values)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["player_id","season","league","level"],
                    set_={
                        k: stmt.excluded[k]
                        for k in ["G","AB","R","H","2B","3B","HR","RBI","BB","HBP","SO","SB","AVG","OBP","SLG","source"]
                    }
                )
            else:
                stmt = mysql_insert(player_season_batting).values(**values).on_duplicate_key_update(
                    **{k: mysql_insert(player_season_batting).inserted[k]
                       for k in ["G","AB","R","H","2B","3B","HR","RBI","BB","HBP","SO","SB","AVG","OBP","SLG","source"]}
                )
            cx.execute(stmt)
            saved += 1
    return saved

3) 엔드투엔드 사용 예시
# run_once.py
import asyncio
from sqlalchemy import create_engine
from src.crawlers.futures_batting import fetch_and_parse_futures_batting
from src.repository.save_futures_batting import save_futures_batting

PLAYER_ID = "77999"  # KBO playerId (문자열)
PROFILE_URL = f"https://www.koreabaseball.com/Record/Player/HitterDetail.aspx?playerId={PLAYER_ID}"

async def main():
    rows = await fetch_and_parse_futures_batting(PLAYER_ID, PROFILE_URL)
    print(f"parsed rows: {rows[:2]} ... total={len(rows)}")
    engine = create_engine("sqlite:///./data/kbo_dev.db", pool_pre_ping=True)
    saved = save_futures_batting(player_id_db=1234, rows=rows, engine=engine)
    print(f"saved: {saved}")

if __name__ == "__main__":
    asyncio.run(main())

4) 실전 팁

셀 구조 변화에 대비해 _pick_futures_table()처럼 “퓨처스” 라벨 근접 테이블 + 헤더 휴리스틱을 같이 씁니다.

값 변환 시 -/공백/콤마 제거를 철저히 하고, AVG/OBP/SLG는 float로.

누락된 SLG/OBP는 간이식으로 보완했지만, 원페이지 값이 있으면 그대로 신뢰하세요.

저장은 UPSERT(멱등) 으로, PK (player_id, season, league, level) 기준.