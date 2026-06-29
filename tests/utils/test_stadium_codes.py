from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from sqlalchemy.exc import SQLAlchemyError

from src.utils.stadium_codes import (
    resolve_stadium_code,
    resolve_stadium_code_from_db,
)


class TestResolveStadiumCode:
    def setup_method(self):
        resolve_stadium_code.cache_clear()

    def test_none_input_returns_none(self):
        assert resolve_stadium_code(None) is None

    def test_empty_string_returns_none(self):
        assert resolve_stadium_code("") is None

    def test_whitespace_returns_none(self):
        assert resolve_stadium_code("   ") is None

    def test_short_name_jamsil(self):
        assert resolve_stadium_code("잠실") == "JAMSIL"

    def test_kr_full_name(self):
        assert resolve_stadium_code("잠실야구장") == "JAMSIL"

    def test_daejeon_full_name(self):
        assert resolve_stadium_code("대전 한화생명 이글스 파크") == "HANBAT"

    def test_suwon_short(self):
        assert resolve_stadium_code("수원") == "SUWON"

    def test_unknown_returns_none(self):
        assert resolve_stadium_code("존재하지않는구장") is None

    def test_strips_whitespace(self):
        assert resolve_stadium_code("  잠실  ") == "JAMSIL"

    def test_caching_works(self):
        resolve_stadium_code("잠실")
        resolve_stadium_code("잠실")
        assert resolve_stadium_code.cache_info().hits >= 1


class TestResolveStadiumCodeFromDb:
    def setup_method(self):
        resolve_stadium_code.cache_clear()

    def test_none_input_returns_none(self):
        session = MagicMock()
        assert resolve_stadium_code_from_db(session, None) is None

    def test_static_match_skips_db(self):
        session = MagicMock()
        assert resolve_stadium_code_from_db(session, "잠실") == "JAMSIL"
        session.execute.assert_not_called()

    def test_db_fallback_returns_code(self):
        session = MagicMock()
        row = MagicMock()
        row.__getitem__ = lambda self, idx: "TEST_STADIUM"
        session.execute.return_value.one_or_none.return_value = row

        result = resolve_stadium_code_from_db(session, "알수없는구장")
        assert result == "TEST_STADIUM"
        session.execute.assert_called_once()

    def test_db_error_returns_none(self):
        session = MagicMock()
        session.execute.side_effect = SQLAlchemyError("DB error", None, None)

        result = resolve_stadium_code_from_db(session, "알수없는구장")
        assert result is None

    def test_db_no_match_returns_none(self):
        session = MagicMock()
        session.execute.return_value.one_or_none.return_value = None

        result = resolve_stadium_code_from_db(session, "알수없는구장")
        assert result is None
