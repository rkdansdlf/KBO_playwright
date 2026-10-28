"""``game_date`` 필터는 문자열로 들어와 ``DATE`` 컬럼에 바인딩된다.

컬럼을 추가한 직후 드러난 두 번째 결함이다. 컬럼이 없을 때는
``UndefinedColumn``이었고, 컬럼을 만들자 ``operator does not exist: date =
character varying``이 됐다 — 파라미터가 ``str``로 문서화·타입 지정돼 있는데
컬럼은 ``DATE``이기 때문이다. 두 오류 모두 **검색 전체를 실패시켰고**, 단위
테스트는 ``search_by_cosine``을 mock하므로 어느 쪽도 잡지 못했다. 실제 조회를
돌려야만 드러난다.

그래서 여기서는 변환 자체를 고정한다. 호출 경계는 문자열을 유지한다 —
API가 받는 것이 문자열이고, 변환은 컬럼 옆에 있는 것이 맞다.
"""

from __future__ import annotations

from datetime import date

import pytest

from src.repositories.vector_search_repository import _coerce_game_date


class TestTheFilterValueBecomesADate:
    def test_an_iso_day_is_parsed(self) -> None:
        assert _coerce_game_date("2026-10-25") == date(2026, 10, 25)

    def test_a_timestamp_is_read_as_its_day(self) -> None:
        """Callers that pass a datetime should narrow to that day, not fail."""
        assert _coerce_game_date("2026-10-25T18:30:00") == date(2026, 10, 25)

    def test_a_date_is_passed_through(self) -> None:
        given = date(2026, 10, 25)

        assert _coerce_game_date(given) is given

    def test_the_game_id_date_form_is_accepted(self) -> None:
        """``YYYYMMDD`` is how this repository spells dates in game ids."""
        assert _coerce_game_date("20261025") == date(2026, 10, 25)

    def test_none_stays_none(self) -> None:
        assert _coerce_game_date(None) is None

    def test_blank_is_none(self) -> None:
        assert _coerce_game_date("   ") is None


class TestAnUnusableValueDropsTheFilterInsteadOfFailing:
    """A mistyped date should narrow results, not turn a working query into an error."""

    @pytest.mark.parametrize("value", ["2026-13-45", "not-a-date", "2026/10/25", "25-10-2026"])
    def test_unparsable_values_return_none(self, value: str) -> None:
        assert _coerce_game_date(value) is None
