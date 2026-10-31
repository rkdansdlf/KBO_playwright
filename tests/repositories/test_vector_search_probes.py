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
    ANN_EF_SEARCH,
    ANN_PROBES,
    _configure_ann_probes,
)


class _FakeSession:
    """Record statements and fail the ones the test names.

    Two failure modes have to be separable now: the extension may not load at all
    (every statement fails), or it may load while a GUC is still rejected. They are
    different problems and the log has to say which one happened.
    """

    def __init__(self, *, raises: bool = False, raises_on: str | None = None) -> None:
        self.statements: list[str] = []
        self._raises = raises
        self._raises_on = raises_on

    def execute(self, statement: object) -> None:
        sql = str(statement)
        self.statements.append(sql)
        if self._raises or (self._raises_on and self._raises_on in sql):
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

        setters = [statement for statement in session.statements if statement.startswith("SET")]
        assert setters
        assert all("SET LOCAL" in statement for statement in setters)


class TestTheGraphSearchEffortIsRaised:
    """HNSW has its own knob, and its default is as damaging as IVF's.

    Measured 2026-10-31 against the index built that day (m=8, ef_construction=32,
    2.9 GB, 375k rows) on a real query:

    | `hnsw.ef_search` | exact top-15 recovered | time |
    | --- | --- | --- |
    | 40 (pgvector default) | **0 / 15** | 66 ms |
    | 100 | 0 / 15 | -- |
    | **200** | **15 / 15** | 88 ms |
    | 400 | 15 / 15 | -- |

    So a search that leaves this at the default returns the wrong rows quickly. The
    planner picks between IVF and HNSW, which is why both parameters are set from
    the same function rather than only the one whose index happened to be present.
    """

    def test_a_value_above_the_default_is_used(self) -> None:
        assert ANN_EF_SEARCH > 40

    def test_the_measured_value_is_two_hundred(self) -> None:
        """Anchored to the measurement: 200 recovered every exact row."""
        assert ANN_EF_SEARCH == 200

    def test_the_setting_is_applied_to_the_session(self) -> None:
        session = _FakeSession()

        _configure_ann_probes(session)

        assert any(f"hnsw.ef_search = {ANN_EF_SEARCH}" in statement for statement in session.statements)

    def test_both_index_kinds_are_configured(self) -> None:
        """Setting only one leaves whichever index the planner picks untuned."""
        session = _FakeSession()

        _configure_ann_probes(session)

        joined = " ".join(session.statements)
        assert "ivfflat.probes" in joined
        assert "hnsw.ef_search" in joined

    def test_the_extension_is_loaded_before_the_settings(self) -> None:
        """The GUCs do not exist until something vector-typed has been evaluated."""
        session = _FakeSession()

        _configure_ann_probes(session)

        first_setter = next(i for i, s in enumerate(session.statements) if s.startswith("SET"))
        assert any("::vector" in s for s in session.statements[:first_setter])


class TestWhenTheExtensionIsNotLoadedYet:
    def test_a_failure_is_not_fatal(self, caplog: pytest.LogCaptureFixture) -> None:
        """`SET` before any vector operation raises; the query must still run."""
        session = _FakeSession(raises=True)

        with caplog.at_level(logging.WARNING):
            _configure_ann_probes(session)

        assert session.statements, "the attempt must still be made"

    def test_a_failure_is_logged_rather_than_swallowed(self, caplog: pytest.LogCaptureFixture) -> None:
        """A silent fallback lowers recall with nothing in the logs."""
        session = _FakeSession(raises=True)

        with caplog.at_level(logging.WARNING):
            _configure_ann_probes(session)

        assert any("not loadable" in record.message for record in caplog.records)

    def test_a_rejected_setting_names_the_setting(self, caplog: pytest.LogCaptureFixture) -> None:
        """The extension may load while a GUC is still refused -- say which one."""
        session = _FakeSession(raises_on="hnsw.ef_search")

        with caplog.at_level(logging.WARNING):
            _configure_ann_probes(session)

        assert any("hnsw.ef_search" in record.message for record in caplog.records)
        assert any("ivfflat.probes" in statement for statement in session.statements)

    def test_the_caller_does_not_see_the_error(self) -> None:
        """The probe count is an optimisation; search must not fail over it."""
        _configure_ann_probes(_FakeSession(raises=True))
