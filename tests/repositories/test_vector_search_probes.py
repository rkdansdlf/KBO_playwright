"""IVF 근사 검색은 기본 설정에서 정답을 놓친다.

pgvector의 `ivfflat.probes` 기본값은 **1**이다 — 인덱스를 100개 리스트로
나눠 놓고 그중 하나만 본다. 2026-10-29 실측(37만 행, lists=100):

| probes | 시간 | top-10 안의 정답 |
| --- | --- | --- |
| 1 (기본) | 1,684ms | **없음** |
| 5 | 1,663ms | 3위 |
| 10 | 9,764ms | 3위 |
| 20 | 11,566ms | 1위 |

즉 기본값은 **같은 시간에 정답을 놓치고 있었다**. `probes=5`가 1과 같은
비용으로 정답을 top-3에 올린다.

`SET`은 벡터 확장이 로드된 세션에서만 유효하므로 첫 쿼리에서는 실패할 수
있다. 그 경우 기본값으로 진행하되 **조용히 삼키지 않고** 로그를 남긴다 —
정확도가 떨어졌다는 사실을 나중에 추적할 수 있어야 한다.
"""

from __future__ import annotations

import logging

import pytest
from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError

from src.repositories.vector_search_repository import (
    ANN_PROBES,
    _configure_ann_probes,
)


class _FakeSession:
    def __init__(self, *, raises: bool = False) -> None:
        self.statements: list[str] = []
        self._raises = raises

    def execute(self, statement: object) -> None:
        sql = str(statement)
        self.statements.append(sql)
        if self._raises:
            raise ProgrammingError("unrecognized configuration parameter", None, None)


class TestTheProbeCountIsRaised:
    def test_the_default_of_one_is_replaced(self) -> None:
        """One list out of a hundred is not a search, it is a sample."""
        assert ANN_PROBES > 1

    def test_the_measured_value_is_five(self) -> None:
        """Anchored to the measurement: 5 matched 1's cost and found the answer."""
        assert ANN_PROBES == 5

    def test_the_setting_is_applied_to_the_session(self) -> None:
        session = _FakeSession()

        _configure_ann_probes(session)

        assert any(f"ivfflat.probes = {ANN_PROBES}" in statement for statement in session.statements)

    def test_it_is_local_to_the_transaction(self) -> None:
        """A session-level SET would leak into every later query on the pool."""
        session = _FakeSession()

        _configure_ann_probes(session)

        assert all("SET LOCAL" in statement for statement in session.statements)


class TestWhenTheExtensionIsNotLoadedYet:
    def test_a_failure_is_not_fatal(self, caplog: pytest.LogCaptureFixture) -> None:
        """`SET` before any vector operation raises; the query must still run."""
        session = _FakeSession(raises=True)

        with caplog.at_level(logging.WARNING):
            _configure_ann_probes(session)

        assert session.statements, "the attempt must still be made"

    def test_a_failure_is_logged_rather_than_swallowed(self, caplog: pytest.LogCaptureFixture) -> None:
        """A silent fallback to probes=1 lowers recall with nothing in the logs."""
        session = _FakeSession(raises=True)

        with caplog.at_level(logging.WARNING):
            _configure_ann_probes(session)

        assert any("probes" in record.message.lower() for record in caplog.records)

    def test_the_caller_does_not_see_the_error(self) -> None:
        """The probe count is an optimisation; search must not fail over it."""
        _configure_ann_probes(_FakeSession(raises=True))
