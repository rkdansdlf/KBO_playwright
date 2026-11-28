"""존재하지 않는 ``document_type``을 필터로 걸면 검색이 0건이 된다.

실측(2026-10-29): "2024년 김도영 홈런은 몇 개였나?"가 `category="milestone"`을
만들었고, 그 유형의 청크는 **DB에 0건**이다. 그래서 벡터·BM25 양쪽이 빈손으로
돌아왔고, 0건이 트리거하는 `enable_indexscan = off` 폴백이 전체 스캔을 돌아
**130초**를 썼다. 데이터는 있었고(시즌 타격 청크가 정확 검색에서 1위),
필터가 없었을 뿐이다.

`CATEGORY_KEYWORDS`는 질문의 낱말로 유형을 **추측**한다. 추측과 실제 스키마
사이를 확인하는 코드가 없으면, 추측이 빗나갈 때마다 검색이 조용히 0건이 된다.
그래서 필터를 걸기 전에 그 값이 이 인덱스에 존재하는지 묻는다.
"""

from __future__ import annotations

import logging

import pytest
from sqlalchemy.exc import OperationalError

from src.services.hybrid_retriever import _document_type_exists


class _FakeSession:
    """Returns whether the asked-for type appears, recording every query."""

    def __init__(self, existing: set[str]) -> None:
        self.existing = existing
        self.queries: list[str] = []

    def execute(self, statement: object, params: dict | None = None) -> object:
        self.queries.append(str(statement))
        asked = (params or {}).get("document_type")
        found = asked in self.existing

        class _Result:
            @staticmethod
            def scalar_one_or_none() -> int | None:
                return 1 if found else None

            @staticmethod
            def fetchone() -> tuple[int] | None:
                return (1,) if found else None

        return _Result()


class TestATypeThatExistsIsKept:
    def test_a_present_type_is_allowed(self) -> None:
        session = _FakeSession({"season_batting"})

        assert _document_type_exists(session, "season_batting") is True

    def test_the_question_is_about_that_exact_type(self) -> None:
        session = _FakeSession({"season_batting"})

        _document_type_exists(session, "season_batting")

        assert any("document_type" in query for query in session.queries)


class TestATypeThatDoesNotExistIsRefused:
    def test_an_absent_type_is_rejected(self) -> None:
        """`milestone` was exactly this case: a plausible word, no rows."""
        session = _FakeSession({"season_batting", "award"})

        assert _document_type_exists(session, "milestone") is False

    def test_the_check_is_a_single_cheap_query(self) -> None:
        """It runs on every search; the column has an index for it."""
        session = _FakeSession({"award"})

        _document_type_exists(session, "milestone")

        assert len(session.queries) == 1

    def test_none_is_not_asked_about(self) -> None:
        """No filter is not a filter that might be wrong."""
        session = _FakeSession(set())

        assert _document_type_exists(session, None) is True
        assert session.queries == []


class TestWhenTheCheckItselfFails:
    def test_a_broken_check_keeps_the_filter(self, caplog: pytest.LogCaptureFixture) -> None:
        """A database error must not silently widen every search.

        Only database errors are caught: a programming error should surface
        rather than be absorbed into a "keep the filter" answer.
        """

        class _Broken:
            @staticmethod
            def execute(statement: object, params: dict | None = None) -> object:
                raise OperationalError("SELECT 1", None, Exception("connection lost"))

        with caplog.at_level(logging.WARNING):
            # 확인할 수 없으면 필터를 유지한다 — 넓히는 쪽이 더 위험하다.
            assert _document_type_exists(_Broken(), "award") is True

    def test_a_programming_error_is_not_absorbed(self) -> None:
        """Absorbing it would turn a bug into a silently widened search."""

        class _Broken:
            @staticmethod
            def execute(statement: object, params: dict | None = None) -> object:
                raise RuntimeError("bug in the caller")

        with pytest.raises(RuntimeError):
            _document_type_exists(_Broken(), "award")


class TestTheFilterIsRemovedFromTheFilterDict:
    """`target_category`만 비우면 dense 검색이 merged_filters에서 다시 읽는다.

    실측: `document_type='milestone'`이 merged_filters에 남아 있어 벡터 검색이
    그 필터로 0건을 만들고, 0건 폴백이 전체 스캔을 돌아 **244초**를 썼다.
    필터를 버린다는 결정은 **그 값을 쓰는 모든 곳**에 적용돼야 한다.
    """

    def test_the_source_uses_the_same_dict_for_both_decisions(self) -> None:
        from pathlib import Path

        source = Path("src/services/hybrid_retriever.py").read_text(encoding="utf-8")
        marker = "_document_type_exists(self.session, target_category)"
        assert marker in source, "the check must stay next to where the filter is chosen"

        after = source.split(marker, 1)[1]
        window = after[:600]
        assert 'merged_filters.pop("document_type"' in window, (
            "dropping the filter must also clear it from merged_filters, or the dense search reads it back"
        )
