from __future__ import annotations

import logging

logger = logging.getLogger(__name__)
"""
KBO 팀명 매핑 유틸리티
OCI team_history 테이블과 연동하여 동적 매핑 제공
"""


from typing import TYPE_CHECKING

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from src.utils.team_codes import resolve_team_code

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

TEAM_HISTORY_QUERIES = (
    # 컬럼명 패턴 0 (KBO_playwright 표준)
    """
    SELECT
        team_name,
        team_code,
        season,
        season as end_year,
        team_name as franchise_name
    FROM team_history
    WHERE team_name IS NOT NULL
    AND team_code IS NOT NULL
    ORDER BY season
    """,
    # 컬럼명 패턴 1
    """
    SELECT
        team_name_kor,
        team_code,
        start_year,
        end_year,
        franchise_name
    FROM team_history
    WHERE team_name_kor IS NOT NULL
    AND team_code IS NOT NULL
    ORDER BY start_year
    """,
    # 컬럼명 패턴 2
    """
    SELECT
        name_kor,
        code,
        start_year,
        end_year,
        franchise
    FROM team_history
    WHERE name_kor IS NOT NULL
    AND code IS NOT NULL
    ORDER BY start_year
    """,
    # 컬럼명 패턴 3 (레거시 PostgreSQL 일부 구조)
    """
    SELECT
        team_name,
        team_code,
        start_season,
        end_season,
        team_name as franchise_name
    FROM team_history
    WHERE team_name IS NOT NULL
    AND team_code IS NOT NULL
    ORDER BY start_season
    """,
    # 기본 모든 컬럼 조회
    """
    SELECT * FROM team_history LIMIT 5
    """,
)

HISTORICAL_PATTERNS = {
    "OB": "OB",
    "OB베어스": "OB",
    "두산": "DB",
    "두산베어스": "DB",
    "삼성": "SS",
    "삼성라이온즈": "SS",
    "LG": "LG",
    "LG트윈스": "LG",
    "MBC": "MBC",
    "MBC청룡": "MBC",
    "롯데": "LT",
    "롯데자이언츠": "LT",
    "한화": "HH",
    "한화이글스": "HH",
    "빙그레": "BE",
    "빙그레이글스": "BE",
    "해태": "HT",
    "해태타이거즈": "HT",
    "KIA": "KIA",
    "KIA타이거즈": "KIA",
    "현대": "HU",
    "현대유니콘스": "HU",
    "키움": "KH",
    "키움히어로즈": "KH",
    "넥센": "NX",
    "넥센히어로즈": "NX",
    "SK": "SK",
    "SK와이번스": "SK",
    "SSG": "SSG",
    "SSG랜더스": "SSG",
    "청보": "CB",
    "청보핀토스": "CB",
    "삼미": "SM",
    "삼미슈퍼스타즈": "SM",
    "태평양": "TP",
    "태평양돌핀스": "TP",
    "쌍방울": "SL",
    "쌍방울레이더스": "SL",
    "NC": "NC",
    "NC다이노스": "NC",
    "KT": "KT",
    "KT위즈": "KT",
}


class TeamMapper:
    """팀명 매핑 관리 클래스"""

    def __init__(self) -> None:
        self.static_mapping = self._get_static_mapping()
        self.oci_mapping = {}
        self.year_specific_mapping = {}
        self._oci_loaded = False

    def _get_static_mapping(self) -> dict[str, str]:
        """기본 정적 매핑 (현재 팀들)"""
        return {
            "LG": "LG",
            "NC": "NC",
            "KT": "KT",
            "삼성": "SS",
            "롯데": "LT",
            "두산": "DB",
            "KIA": "KIA",
            "한화": "HH",
            "키움": "KH",
            "SSG": "SSG",
            # 추가 변형들
            "LG트윈스": "LG",
            "NC다이노스": "NC",
            "KT위즈": "KT",
            "삼성라이온즈": "SS",
            "롯데자이언츠": "LT",
            "두산베어스": "DB",
            "KIA타이거즈": "KIA",
            "한화이글스": "HH",
            "키움히어로즈": "KH",
            "SSG랜더스": "SSG",
        }

    def load_oci_mapping(self) -> bool:
        """OCI team_history 테이블에서 매핑 데이터 로드"""
        from src.db.engine import get_oci_url

        oci_url = get_oci_url()
        if not oci_url:
            logger.warning("⚠️ OCI_DB_URL 환경변수가 설정되지 않음. 정적 매핑만 사용.")
            return False

        try:
            results = self._load_team_history_rows(oci_url)
            if not results:
                return False
            self._apply_oci_mapping_rows(results)
            self._oci_loaded = True
        except (SQLAlchemyError, ValueError):
            logger.exception("⚠️ OCI 팀 매핑 로드 실패")
            return False
        else:
            logger.info("✅ OCI에서 %s개 팀 매핑 로드 완료", len(results))
            return True

    def _load_team_history_rows(self, oci_url: str) -> list[Sequence[object]] | None:
        engine = create_engine(oci_url)
        session_maker = sessionmaker(bind=engine)
        session = session_maker()
        try:
            self._log_team_history_columns(session)
            results = self._query_team_history(session)
            if results is None:
                logger.error("❌ 모든 쿼리 패턴 실패")
                return None
            if not results:
                logger.warning("⚠️ team_history 테이블에서 데이터를 찾을 수 없음")
                return None
            return results
        finally:
            session.close()
            engine.dispose()

    @staticmethod
    def _log_team_history_columns(session: Session) -> None:
        try:
            structure_query = text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'team_history'
                ORDER BY ordinal_position
            """)
            columns = session.execute(structure_query).fetchall()
            logger.info("📋 team_history 테이블 컬럼: %s", [col[0] for col in columns])
        except SQLAlchemyError:
            logger.exception("⚠️ 테이블 구조 확인 실패")

    @staticmethod
    def _query_team_history(session: Session) -> list[Sequence[object]] | None:
        for index, query_sql in enumerate(TEAM_HISTORY_QUERIES, start=1):
            try:
                session.rollback()
                query_result = session.execute(text(query_sql)).fetchall()
            except SQLAlchemyError:
                logger.exception("⚠️ 쿼리 패턴 %s 실패", index)
                session.rollback()
            else:
                logger.info("✅ 쿼리 패턴 %s 성공: %s개 행 조회", index, len(query_result))
                return query_result
        return None

    def _apply_oci_mapping_rows(self, rows: Iterable[Sequence[object]]) -> None:
        for row in rows:
            self._apply_oci_mapping_row(row)

    def _apply_oci_mapping_row(self, row: Sequence[object]) -> None:
        team_name = row[0]
        team_code = row[1]
        try:
            start_year = int(row[2])
            end_year = int(row[3]) if row[3] is not None else 9999
        except (ValueError, TypeError):
            return
        self._add_mapping_for_years(team_name, team_code, start_year, end_year)
        franchise = row[4]
        if franchise and franchise != team_name:
            self._add_mapping_for_years(franchise, team_code, start_year, end_year)

    def _add_mapping_for_years(self, team_name: str, team_code: str, start_year: int, end_year: int) -> None:
        self.oci_mapping[team_name] = team_code
        for year in range(start_year, end_year + 1):
            self.year_specific_mapping.setdefault(year, {})[team_name] = team_code

    def get_team_code(self, team_name: str, year: int | None = None) -> str | None:
        """팀명으로 팀 코드 조회 (년도 고려)"""
        if not team_name:
            return None

        team_name = team_name.strip()

        # 1. 년도별 매핑 우선 확인 (OCI 등의 외부 소스)
        if year and self._oci_loaded and year in self.year_specific_mapping:
            year_mapping = self.year_specific_mapping[year]
            if team_name in year_mapping:
                canonical_code = resolve_team_code(team_name, year)
                if canonical_code:
                    return canonical_code
                return year_mapping[team_name]
        return self._resolve_team_code(team_name, year)

    def _resolve_team_code(self, team_name: str, year: int | None = None) -> str | None:
        # 1.5 Standard Resolution via team_codes (Superior to static/fuzzy)
        resolved = resolve_team_code(team_name, year)
        if resolved:
            return resolved
        # 2. OCI 매핑 확인
        if self._oci_loaded and team_name in self.oci_mapping:
            return self.oci_mapping[team_name]
        # 3. 정적 매핑 확인
        if team_name in self.static_mapping:
            return self.static_mapping[team_name]
        # 4. 부분 매칭 시도 (역대 팀명 변화 고려)
        return self._fuzzy_match(team_name, year)

    def _fuzzy_match(self, team_name: str, year: int | None = None) -> str | None:
        """퍼지 매칭으로 팀 코드 찾기"""
        if team_name in HISTORICAL_PATTERNS:
            return HISTORICAL_PATTERNS[team_name]

        partial_match = self._partial_fuzzy_match(team_name)
        if partial_match:
            return partial_match

        return self._year_specific_fuzzy_match(team_name, year)

    @staticmethod
    def _partial_fuzzy_match(team_name: str) -> str | None:
        for pattern, code in HISTORICAL_PATTERNS.items():
            if pattern in team_name or team_name in pattern:
                return code
        return None

    @staticmethod
    def _year_specific_fuzzy_match(team_name: str, year: int | None = None) -> str | None:
        if not year:
            return None
        if year <= 1985:
            return TeamMapper._early_kbo_fuzzy_match(team_name)
        if year <= 1995:
            return TeamMapper._nineties_fuzzy_match(team_name)
        if year <= 2000:
            return TeamMapper._late_nineties_fuzzy_match(team_name)
        return None

    @staticmethod
    def _early_kbo_fuzzy_match(team_name: str) -> str | None:
        if "MBC" in team_name or "청룡" in team_name:
            return "LG"
        if "해태" in team_name or "타이거즈" in team_name:
            return "HT"
        if "삼미" in team_name:
            return "SM"
        if "청보" in team_name:
            return "CB"
        return None

    @staticmethod
    def _nineties_fuzzy_match(team_name: str) -> str | None:
        if "빙그레" in team_name:
            return "BE"
        if "태평양" in team_name:
            return "TP"
        return None

    @staticmethod
    def _late_nineties_fuzzy_match(team_name: str) -> str | None:
        if "현대" in team_name:
            return "HU"
        if "쌍방울" in team_name:
            return "SL"
        return None

    def get_all_teams_for_year(self, year: int) -> dict[str, str]:
        """특정 년도의 모든 팀 매핑 반환 (정적 매핑과 통합)"""
        # 기본적으로 정적 매핑에서 시작
        mapping = self.static_mapping.copy()

        # OCI에서 로드된 년도별 특수 매핑이 있으면 덮어씀
        if year in self.year_specific_mapping:
            mapping.update(self.year_specific_mapping[year])

        return mapping

    def validate_team_code(self, team_code: str, _year: int | None = None) -> bool:
        """팀 코드 유효성 검증"""
        if not team_code:
            return False

        # 현재 유효한 팀 코드들
        valid_codes = {"LG", "NC", "KT", "SS", "LT", "DB", "KIA", "HH", "KH", "SSG"}

        # 역대 팀 코드들 (해체된 팀 포함)
        historical_codes = {"CB", "SM", "TP", "SL", "OB", "HT", "WO", "SK", "NX", "HU", "MBC", "BE"}

        return team_code in valid_codes or team_code in historical_codes


# 전역 인스턴스
_team_mapper = None


def get_team_mapper() -> TeamMapper:
    """TeamMapper 싱글톤 인스턴스 반환"""
    global _team_mapper
    if _team_mapper is None:
        _team_mapper = TeamMapper()
        # 처음 생성시 OCI 매핑 시도
        _team_mapper.load_oci_mapping()
    return _team_mapper


def get_team_code(team_name: str, year: int | None = None) -> str | None:
    """간편 함수: 팀명으로 팀 코드 조회"""
    mapper = get_team_mapper()
    return mapper.get_team_code(team_name, year)


def get_team_mapping_for_year(year: int) -> dict[str, str]:
    """간편 함수: 특정 년도의 팀 매핑 반환"""
    mapper = get_team_mapper()
    return mapper.get_all_teams_for_year(year)


def refresh_oci_mapping() -> bool:
    """OCI 매핑 갱신"""
    mapper = get_team_mapper()
    return mapper.load_oci_mapping()


if __name__ == "__main__":
    # 테스트 코드
    mapper = TeamMapper()
    mapper.load_oci_mapping()

    # 테스트 케이스들
    test_cases = [
        ("삼성", 2025),
        ("해태", 1985),
        ("MBC", 1983),
        ("현대", 1998),
        ("키움", 2020),
        ("SSG", 2021),
        ("삼미", 1983),
        ("청보", 1983),
    ]

    logger.info("🔍 팀 매핑 테스트:")
    for team_name, year in test_cases:
        code = mapper.get_team_code(team_name, year)
        logger.info("  %s년 '%s' → '%s'", year, team_name, code)
