"""연도 필터가 ``LIKE`` 를 쓰면 PostgreSQL 에서 죽는다.

``game.game_date`` 는 ``date`` 타입이다. ``LIKE '2025%'`` 는 SQLite 에서는 통과하지만
PostgreSQL 에서는 ``operator does not exist: date ~~ unknown`` 로 실패한다 —
2026-10-19 에 ``backfill_sh_sf_from_pbp --year 2025 --dry-run`` 이 이 오류로 죽었다.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

from scripts.maintenance.backfill_sh_sf_from_pbp import (
    count_rows_in_need,
    find_candidate_games,
)


def _captured(module_function, year: int) -> tuple[str, dict]:
    with patch("scripts.maintenance.backfill_sh_sf_from_pbp.SessionLocal") as mock_sl:
        session = MagicMock()
        mock_sl.return_value.__enter__.return_value = session
        session.execute.return_value.all.return_value = []
        session.execute.return_value.scalar.return_value = 0
        module_function(session, year)
        args = session.execute.call_args[0]
        return str(args[0]), args[1]


class TestTheYearFilterIsAPortableRange:
    def test_find_candidate_games_does_not_use_like(self):
        sql, params = _captured(find_candidate_games, 2025)
        assert "LIKE" not in sql.upper(), sql
        assert params["year_start"] == date(2025, 1, 1)
        assert params["year_end"] == date(2026, 1, 1)

    def test_count_rows_in_need_does_not_use_like(self):
        sql, params = _captured(count_rows_in_need, 2025)
        assert "LIKE" not in sql.upper(), sql
        assert params["year_start"] == date(2025, 1, 1)
        assert params["year_end"] == date(2026, 1, 1)

    def test_no_year_means_no_filter_and_no_parameters(self):
        sql, params = _captured(find_candidate_games, 0)
        assert "year_start" not in sql
        assert "LIKE" not in sql.upper()
        assert params == {}

    def test_a_distinct_select_orders_by_a_selected_column(self):
        """PostgreSQL 은 DISTINCT 에서 select list 밖 표현식으로 정렬하는 것을 거부한다.

        ``SELECT DISTINCT g.game_id ... ORDER BY g.game_date`` 가
        ``InvalidColumnReference`` 로 죽었다(2026-10-19).
        """
        sql, _ = _captured(find_candidate_games, 2025)
        select_list, _, _rest = sql.partition("FROM")
        assert "DISTINCT" in select_list.upper(), select_list
        assert "g.game_date" in select_list, select_list
