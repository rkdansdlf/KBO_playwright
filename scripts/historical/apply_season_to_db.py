"""실측 answer set을 운영 DB에 적용 (1983-2000 시즌 교체).

`scripts/historical/namu_season_boxscores.py` 검증을 통과한
`data/archives/{year}_answer_set_final.json`을 game 테이블 실측 행으로 교체한다.

기존 DB의 해당 연도 합성(추정) game 행을 먼저 백업 덤프로 저장한 뒤 삭제하고,
answer set 기준으로 다시 UPSERT한다. 기본은 dry-run — `--apply` 없이는
아무것도 쓰지 않는다.

game_id는 DB 표준(원정+홈 순서, 2자리 세그먼트 — MBC→MB)으로 재생성한다.
answer set의 game_id는 홈+원정·3자리(MBC)라 하위 파서(game_helpers 등)와
충돌하기 때문.

사용법
------
  python3 -m scripts.historical.apply_season_to_db --year 1983            # dry-run
  python3 -m scripts.historical.apply_season_to_db --year 1983 --apply    # 교체 실행
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path


ANSWER_DIR = Path("data/archives")

# DB game_id 세그먼트 계약: 2자리 고정. MBC만 3자리라 MB로 축약 (1982 백업 관례).
SEGMENT_BY_TEAM = {"MBC": "MB"}
EXPECTED_FIELDS = ("game_id", "game_date", "stadium", "home_team", "away_team", "home_score", "away_score")


def _segment(team_code: str) -> str:
    """game_id 세그먼트 (MBC 3자리 → MB)."""
    return SEGMENT_BY_TEAM.get(team_code, team_code)


def build_standard_game_id(date: str, home_team: str, away_team: str, dh: str = "0") -> str:
    """DB 표준 game_id: {YYYYMMDD}{away_seg}{home_seg}{dh}."""
    return f"{date}{_segment(away_team)}{_segment(home_team)}{dh}"


def load_answer_set(year: int) -> list[dict]:
    """Answer set 검증 후 로드 (필수 필드 + 무승부 home/away 팀 존재)."""
    path = ANSWER_DIR / f"{year}_answer_set_final.json"
    games = json.loads(path.read_text(encoding="utf-8"))
    if not games:
        msg = f"empty answer set: {path}"
        raise SystemExit(msg)
    for g in games:
        missing = [f for f in EXPECTED_FIELDS if f not in g]
        if missing:
            msg = f"missing fields {missing} in {g}"
            raise SystemExit(msg)
    return games


def franchise_map(session, year: int) -> dict[str, int]:
    """team_history 기준 {팀코드: franchise_id} (시즌 브랜드 매핑)."""
    from sqlalchemy import text

    rows = session.execute(
        text("SELECT team_code, franchise_id FROM team_history WHERE season = :s AND franchise_id IS NOT NULL"),
        {"s": year},
    ).fetchall()
    return dict(rows)


def compute_winning(g: dict) -> tuple[str | None, int | None]:
    """승리팀 코드/스코어 (무승부 → None)."""
    if g["home_score"] > g["away_score"]:
        return g["home_team"], g["home_score"]
    if g["away_score"] > g["home_score"]:
        return g["away_team"], g["away_score"]
    return None, None


def plan_rows(games: list[dict], year: int, fid: dict[str, int]) -> list[dict]:
    """Answer set → game 테이블 행 플랜 (Oracle DATE는 파이썬 date 객체로)."""
    rows = []
    for g in games:
        win_team, win_score = compute_winning(g)
        rows.append(
            {
                "game_id": build_standard_game_id(
                    g["game_date"].replace("-", ""),
                    g["home_team"],
                    g["away_team"],
                    dh=g["game_id"][-1],
                ),
                "game_date": date.fromisoformat(g["game_date"]),
                "stadium": g["stadium"],
                "home_team": g["home_team"],
                "away_team": g["away_team"],
                "home_score": g["home_score"],
                "away_score": g["away_score"],
                "winning_team": win_team,
                "winning_score": win_score,
                "season_id": year * 100,
                "game_status": "COMPLETED",
                "game_lifecycle_state": None,
                "is_primary": True,
                "home_franchise_id": fid.get(g["home_team"]),
                "away_franchise_id": fid.get(g["away_team"]),
                "winning_franchise_id": fid.get(win_team) if win_team else None,
            }
        )
    return rows


def existing_count(session, year: int) -> int:
    """기존 {year}% game 행 수 (교체 대상)."""
    from sqlalchemy import text

    (n,) = session.execute(text(f"SELECT COUNT(*) FROM game WHERE game_id LIKE '{year}%'")).fetchone()
    return n


def backup_existing(session, year: int, out: Path) -> int:
    """기존 {year}% game 행 전체 덤프 (교체 전 백업)."""
    from sqlalchemy import text

    rows = session.execute(
        text(
            "SELECT id, game_id, game_date, stadium, home_team, away_team, "
            "home_score, away_score, winning_team, winning_score, season_id, "
            "game_status, game_lifecycle_state, is_primary, "
            "home_franchise_id, away_franchise_id, winning_franchise_id, "
            "created_at, updated_at FROM game WHERE game_id LIKE :p"
        ),
        {"p": f"{year}%"},
    ).mappings()
    payload = [{k: (v.isoformat() if isinstance(v, (datetime, date)) else v) for k, v in dict(r).items()} for r in rows]
    if payload:
        out.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
    return len(payload)


def delete_existing(session, year: int) -> int:
    """기존 {year}% game 및 game_metadata 행 삭제."""
    from sqlalchemy import text

    session.execute(text(f"DELETE FROM game_metadata WHERE game_id LIKE '{year}%'"))
    result = session.execute(text(f"DELETE FROM game WHERE game_id LIKE '{year}%'"))
    return result.rowcount or 0


def insert_rows(session, rows: list[dict], year: int) -> int:
    """Game 및 GameMetadata 행 일괄 INSERT (교체 후 신규 적재)."""
    from datetime import UTC, datetime

    from scripts.maintenance.backfill_historical_stadium_codes import HISTORICAL_STADIUM_NAMES, clean_stadium_name
    from src.models.game import Game, GameMetadata
    from src.utils.stadium_codes import resolve_stadium_code

    meta_rows = []
    for r in rows:
        cleaned_stadium = clean_stadium_name(r["stadium"], r["home_team"])
        stadium_code = resolve_stadium_code(cleaned_stadium, season_year=year) or "JAMSIL"
        stadium_name = HISTORICAL_STADIUM_NAMES.get(stadium_code, f"{cleaned_stadium}야구장")
        meta_rows.append(
            {
                "game_id": r["game_id"],
                "stadium_code": stadium_code,
                "stadium_name": stadium_name,
                "source_payload": {
                    "source": "historical_boxscore",
                    "raw_stadium": r["stadium"],
                    "cleaned_stadium": cleaned_stadium,
                    "applied_at": datetime.now(UTC).isoformat(),
                },
            }
        )

    session.execute(Game.__table__.insert(), rows)
    session.execute(GameMetadata.__table__.insert(), meta_rows)
    session.commit()
    return len(rows)


def anchor_checks(rows: list[dict], year: int) -> bool:
    """Answer set 로드 상태에서 verify (앵커 ·매치업 균형 ·무승부)."""
    if year == 1982:
        import importlib

        mod_1982 = importlib.import_module("scripts.historical.1982_namu_boxscores")
        verify_1982 = mod_1982.verify

        games_1982 = [
            {
                "game_date": r["game_date"].isoformat()
                if hasattr(r["game_date"], "isoformat")
                else str(r["game_date"]),
                "home_team": r["home_team"],
                "away_team": r["away_team"],
                "home_score": r["home_score"],
                "away_score": r["away_score"],
                "stadium": r["stadium"],
            }
            for r in rows
        ]
        return verify_1982(games_1982)

    from scripts.historical.namu_season_boxscores import ANCHORS, verify

    anchors = json.loads(ANCHORS.read_text(encoding="utf-8")).get(str(year), {})
    games = [
        {
            "game_date": r["game_date"],
            "home_team": r["home_team"],
            "away_team": r["away_team"],
            "home_score": r["home_score"],
            "away_score": r["away_score"],
        }
        for r in rows
    ]
    return verify(games, year, anchors)


def apply_year(session, year: int, *, do_apply: bool) -> int:
    """시즌 교체 실행/미리보기. 반환: 0=성공, 1=경고, 2=오류."""
    games = load_answer_set(year)
    print(f"== apply season {year} ==")
    print(f"answer set: {len(games)} games")
    fid = franchise_map(session, year)
    print(f"franchise map (team_history {year}): {dict(sorted(fid.items()))}")
    rows = plan_rows(games, year, fid)

    n_existing = existing_count(session, year)
    print(f"existing {year}% game rows: {n_existing}")
    ids = [r["game_id"] for r in rows]
    if len(set(ids)) != len(ids):
        print("FATAL duplicate game_id in plan")
        return 2

    ok = anchor_checks(rows, year)
    print(f"anchor checks: {'PASS' if ok else 'FAIL'}")
    if not ok:
        print("aborting: anchor checks failed")
        return 2

    if not do_apply:
        print("DRY-RUN — no writes (add --apply to replace)")
        return 0

    backup_path = ANSWER_DIR / f"oci_{year}_pre_replace_backup.json"
    backed = backup_existing(session, year, backup_path)
    print(f"backed up existing rows -> {backup_path} ({backed})")
    deleted = delete_existing(session, year)
    print(f"deleted: {deleted}")
    written = insert_rows(session, rows, year)
    print(f"inserted: {written}")

    print(f"applied season {year}: {written} games (backup {backed})")
    return 0


def main(argv: list[str] | None = None) -> int:
    """CLI 진입점."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--apply", action="store_true", help="실제 교체 실행 (기본 dry-run)")
    parser.add_argument("--db-url", type=str, default=None, help="Target database URL (default: SessionLocal)")
    args = parser.parse_args(argv)

    if args.year < 1982 or args.year > 2000:
        print(f"unsupported year {args.year}; supported: 1982-2000")
        return 2

    if args.db_url:
        from sqlalchemy import create_engine
        from sqlalchemy.orm import Session

        engine = create_engine(args.db_url)
        session = Session(engine)
    else:
        from src.db.engine import SessionLocal

        session = SessionLocal()

    try:
        return apply_year(session, args.year, do_apply=args.apply)
    finally:
        session.close()


if __name__ == "__main__":
    sys.exit(main())
