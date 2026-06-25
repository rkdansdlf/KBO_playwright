"""Safe batting data repository with foreign key constraint bypass
타자 데이터를 외래키 제약조건 우회하여 안전하게 저장.
"""

from __future__ import annotations

import logging
from collections import Counter
from typing import TYPE_CHECKING, Any

from sqlalchemy import func, text
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import SQLAlchemyError

from src.db.engine import SessionLocal, get_database_type
from src.models.player import PlayerSeasonBatting
from src.utils.player_season_stat_validation import filter_valid_season_stat_payloads

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)
LAST_FILTER_COUNTS: Counter = Counter()
BATTING_CONFLICT_KEYS = ["player_id", "season", "league", "level"]


def get_last_filter_counts() -> dict[str, int]:
    """Gets last counts.

    Returns:
        Dictionary result.

    """
    return dict(LAST_FILTER_COUNTS)


def _unique_batting_payloads(payloads: list[dict[str, Any]]) -> dict[tuple[Any, Any, Any, Any], dict[str, Any]]:
    unique_payloads = {}
    for payload in payloads:
        key = (
            payload.get("player_id"),
            payload.get("season"),
            payload.get("league"),
            payload.get("level", "KBO1"),
        )
        if key[0] is None or key[1] is None:
            continue
        unique_payloads[key] = payload
    return unique_payloads


def _batting_row(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "player_id": payload.get("player_id"),
        "season": payload.get("season"),
        "league": payload.get("league"),
        "level": payload.get("level", "KBO1"),
        "source": payload.get("source", "CRAWLER"),
        "team_code": payload.get("team_code"),
        "games": payload.get("games"),
        "plate_appearances": payload.get("plate_appearances"),
        "at_bats": payload.get("at_bats"),
        "runs": payload.get("runs"),
        "hits": payload.get("hits"),
        "doubles": payload.get("doubles"),
        "triples": payload.get("triples"),
        "home_runs": payload.get("home_runs"),
        "rbi": payload.get("rbi"),
        "walks": payload.get("walks"),
        "intentional_walks": payload.get("intentional_walks"),
        "hbp": payload.get("hbp"),
        "strikeouts": payload.get("strikeouts"),
        "stolen_bases": payload.get("stolen_bases"),
        "caught_stealing": payload.get("caught_stealing"),
        "sacrifice_hits": payload.get("sacrifice_hits"),
        "sacrifice_flies": payload.get("sacrifice_flies"),
        "gdp": payload.get("gdp"),
        "avg": payload.get("avg"),
        "obp": payload.get("obp"),
        "slg": payload.get("slg"),
        "ops": payload.get("ops"),
        "iso": payload.get("iso"),
        "babip": payload.get("babip"),
        "extra_stats": payload.get("extra_stats"),
    }


def _batting_rows(payloads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [_batting_row(payload) for payload in _unique_batting_payloads(payloads).values()]


def _excluded_update_dict(stmt: object, rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        key: func.coalesce(stmt.excluded[key], getattr(PlayerSeasonBatting, key))
        for key in rows[0]
        if key not in BATTING_CONFLICT_KEYS
    }


def _inserted_update_dict(stmt: object, rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        key: func.coalesce(stmt.inserted[key], getattr(PlayerSeasonBatting, key))
        for key in rows[0]
        if key not in BATTING_CONFLICT_KEYS
    }


def _save_sqlite_rows(session: Session, rows: list[dict[str, Any]]) -> int:
    stmt = sqlite_insert(PlayerSeasonBatting).values(rows)
    stmt = stmt.on_conflict_do_update(index_elements=BATTING_CONFLICT_KEYS, set_=_excluded_update_dict(stmt, rows))
    try:
        session.execute(stmt)
        return len(rows)
    except SQLAlchemyError:
        session.rollback()
        logger.exception("⚠️ 배치 UPSERT 실패, 개별 처리로 전환합니다")
        saved_count = 0
        for data in rows:
            row_stmt = sqlite_insert(PlayerSeasonBatting).values(**data)
            row_stmt = row_stmt.on_conflict_do_update(
                index_elements=BATTING_CONFLICT_KEYS,
                set_=_excluded_update_dict(row_stmt, rows),
            )
            saved_count += _execute_single_upsert(session, row_stmt, data)
        return saved_count


def _save_mysql_rows(session: Session, rows: list[dict[str, Any]]) -> int:
    stmt = mysql_insert(PlayerSeasonBatting).values(rows)
    stmt = stmt.on_duplicate_key_update(_inserted_update_dict(stmt, rows))
    try:
        session.execute(stmt)
        return len(rows)
    except SQLAlchemyError:
        session.rollback()
        logger.exception("⚠️ 배치 UPSERT 실패, 개별 처리로 전환합니다")
        saved_count = 0
        for data in rows:
            row_stmt = mysql_insert(PlayerSeasonBatting).values(**data)
            row_stmt = row_stmt.on_duplicate_key_update(_inserted_update_dict(row_stmt, rows))
            saved_count += _execute_single_upsert(session, row_stmt, data)
        return saved_count


def _save_postgresql_rows(session: Session, rows: list[dict[str, Any]]) -> int:
    stmt = postgresql_insert(PlayerSeasonBatting).values(rows)
    stmt = stmt.on_conflict_do_update(index_elements=BATTING_CONFLICT_KEYS, set_=_excluded_update_dict(stmt, rows))
    try:
        session.execute(stmt)
        return len(rows)
    except SQLAlchemyError:
        session.rollback()
        logger.exception("⚠️ 배치 UPSERT 실패, 개별 처리로 전환합니다")
        saved_count = 0
        for data in rows:
            row_stmt = postgresql_insert(PlayerSeasonBatting).values(**data)
            row_stmt = row_stmt.on_conflict_do_update(
                index_elements=BATTING_CONFLICT_KEYS,
                set_=_excluded_update_dict(row_stmt, rows),
            )
            saved_count += _execute_single_upsert(session, row_stmt, data)
        return saved_count


def _execute_single_upsert(session: Session, stmt: object, data: dict[str, Any]) -> int:
    try:
        session.execute(stmt)
    except SQLAlchemyError:
        logger.exception("⚠️ UPSERT 실패 (player_id=%s)", data.get("player_id"))
        session.rollback()
        return 0
    else:
        return 1


def _save_generic_rows(session: Session, rows: list[dict[str, Any]]) -> int:
    saved_count = 0
    for data in rows:
        existing = (
            session.query(PlayerSeasonBatting)
            .filter_by(
                player_id=data["player_id"],
                season=data["season"],
                league=data["league"],
                level=data["level"],
            )
            .first()
        )
        if existing:
            for key, value in data.items():
                if value is not None:
                    setattr(existing, key, value)
        else:
            session.add(PlayerSeasonBatting(**data))
        saved_count += 1
    return saved_count


def _save_rows_by_database_type(session: Session, rows: list[dict[str, Any]], db_type: str) -> int:
    if db_type == "sqlite":
        return _save_sqlite_rows(session, rows)
    if db_type == "mysql":
        return _save_mysql_rows(session, rows)
    if db_type == "postgresql":
        return _save_postgresql_rows(session, rows)
    return _save_generic_rows(session, rows)


def save_batting_stats_safe(payloads: list[dict[str, Any]]) -> int:
    """타자 시즌 통계를 player_season_batting 테이블에 안전하게 UPSERT 저장
    외래키 제약조건을 임시로 비활성화하여 데이터 저장.

    Args:
        payloads: 타자 데이터 딕셔너리 리스트

    Returns:
        저장된 레코드 수

    """
    global LAST_FILTER_COUNTS
    LAST_FILTER_COUNTS = Counter()
    if not payloads:
        return 0

    payloads, LAST_FILTER_COUNTS = filter_valid_season_stat_payloads(
        payloads,
        stat_type="batting",
    )
    if not payloads:
        return 0

    with SessionLocal() as session:
        db_type = get_database_type()

        try:
            # SQLite의 경우 외래키 제약조건 임시 비활성화
            if db_type == "sqlite":
                session.execute(text("PRAGMA foreign_keys = OFF"))
                logger.info("⚙️ SQLite 외래키 제약조건 임시 비활성화")

            rows = _batting_rows(payloads)
            if not rows:
                return 0

            saved_count = _save_rows_by_database_type(session, rows, db_type)

            session.commit()
            logger.info("✅ 타자 데이터 %s건 저장 완료 (player_season_batting 테이블)", saved_count)

        except SQLAlchemyError:
            session.rollback()
            logger.exception("❌ 타자 데이터 저장 실패")
            return 0
        finally:
            if db_type == "sqlite":
                session.execute(text("PRAGMA foreign_keys = ON"))

        return saved_count


def get_batting_stats_count(session: Session | None = None) -> int:
    """타자 테이블의 레코드 수 조회."""
    if session:
        return session.query(PlayerSeasonBatting).count()
    with SessionLocal() as new_session:
        return new_session.query(PlayerSeasonBatting).count()


def get_batting_stats_by_season(season: int, session: Session | None = None) -> list[PlayerSeasonBatting]:
    """시즌별 타자 데이터 조회."""
    if session:
        return session.query(PlayerSeasonBatting).filter_by(season=season).all()
    with SessionLocal() as new_session:
        return new_session.query(PlayerSeasonBatting).filter_by(season=season).all()


def cleanup_invalid_batting_data(session: Session | None = None) -> int:
    """잘못된 타자 데이터 정리 (예: 필수 필드 누락)."""
    cleanup_session = session or SessionLocal()

    try:
        # player_id나 season이 없는 레코드 삭제
        deleted = (
            cleanup_session.query(PlayerSeasonBatting)
            .filter((PlayerSeasonBatting.player_id.is_(None)) | (PlayerSeasonBatting.season.is_(None)))
            .delete()
        )

        if not session:  # 외부 세션이 아닌 경우만 커밋
            cleanup_session.commit()

    except SQLAlchemyError:
        if not session:
            cleanup_session.rollback()
        logger.exception("⚠️ 타자 데이터 정리 실패")
        return 0
    else:
        return deleted
    finally:
        if not session:
            cleanup_session.close()


def save_futures_batting(player_id_db: int, rows: list[dict], league: str = "FUTURES", level: str = "KBO2") -> int:
    """Saves futures batting.

    Args:
        player_id_db: Player Id Db.
        rows: Rows.
        league: League.
        level: Level.

    Returns:
        Integer result.

    """
    if not rows:
        return 0
    payloads = [
        {
            "player_id": player_id_db,
            "season": r.get("season"),
            "league": league,
            "level": level,
            "games": r.get("G"),
            "at_bats": r.get("AB"),
            "runs": r.get("R"),
            "hits": r.get("H"),
            "doubles": r.get("2B"),
            "triples": r.get("3B"),
            "home_runs": r.get("HR"),
            "rbi": r.get("RBI"),
            "walks": r.get("BB"),
            "hbp": r.get("HBP"),
            "strikeouts": r.get("SO"),
            "stolen_bases": r.get("SB"),
            "avg": r.get("AVG"),
            "obp": r.get("OBP"),
            "slg": r.get("SLG"),
            "source": "PROFILE",
        }
        for r in rows
        if r.get("season")
    ]
    if not payloads:
        return 0
    return save_batting_stats_safe(payloads)
