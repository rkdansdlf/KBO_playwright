"""운영 DB fail-fast 프로브 계약.

스케줄러 잡은 고정 cron으로 돌기 때문에, DB가 죽었을 때 잡이 연결 타임아웃을
기다리며 tier lock을 붙잡고 있으면 같은 락을 쓰는 다른 잡이 전부 굶는다
(2026-10-03 장애: DLQ 잡당 약 150초, maintenance 락 스킵 발생). 그래서 프로브는
짧은 ``connect_timeout``을 강제한다.
"""

from __future__ import annotations

import pytest

from src.db.engine import (
    DB_PROBE_CONNECT_TIMEOUT_SECONDS,
    DB_PROBE_ENGINE_CACHE_SIZE,
    _probe_engine,
    database_reachable,
)

POSTGRES_URL = "postgresql+psycopg2://probe_user:probe_secret@db.example:5432/kbo"

#: More URLs than a build actually opens. The constant is checked against the
#: realistic set above; this one exists to make eviction *happen* if the cache is
#: too small, since a set that happens to fit proves nothing.
MANY_URLS = tuple(f"postgresql://target{i}/kbo" for i in range(12))


def test_probe_engine_injects_a_short_connect_timeout() -> None:
    probe = _probe_engine(POSTGRES_URL, 3)

    assert dict(probe.url.query)["connect_timeout"] == "3"
    # 자격 증명은 엔진에 남지만 로그용 렌더에서는 가려져야 한다.
    assert "probe_secret" not in probe.url.render_as_string(hide_password=True)


def test_probe_engine_preserves_existing_query_parameters() -> None:
    probe = _probe_engine(f"{POSTGRES_URL}?keepalives=1", 3)

    query = dict(probe.url.query)
    assert query["keepalives"] == "1"
    assert query["connect_timeout"] == "3"


def test_probe_engine_leaves_sqlite_alone() -> None:
    probe = _probe_engine("sqlite:///:memory:", 3)

    assert "connect_timeout" not in dict(probe.url.query)


def test_probe_engine_is_cached_per_url() -> None:
    assert _probe_engine(POSTGRES_URL, 3) is _probe_engine(POSTGRES_URL, 3)


def test_the_cache_outlives_a_whole_build_target_set() -> None:
    """A probe engine per URL, and a build can be asked about more than one.

    The cache was sized for a single database, which is all the probe asked about
    until jobs began naming the stores they actually open. Under that load an
    undersized cache evicts silently: every probe builds a fresh engine and its
    connection pool, which is the cost the cache exists to avoid, and the failure
    looks like slowness rather than like a bug.

    Asserted by *behaviour* -- a second pass must build nothing -- rather than by
    reading the size constant, which the decorator could disagree with while the
    constant still reads as correct.
    """
    _probe_engine.cache_clear()
    try:
        for url in MANY_URLS:
            _probe_engine(url, DB_PROBE_CONNECT_TIMEOUT_SECONDS)
        misses_after_fill = _probe_engine.cache_info().misses
        for url in MANY_URLS:
            _probe_engine(url, DB_PROBE_CONNECT_TIMEOUT_SECONDS)

        assert _probe_engine.cache_info().misses == misses_after_fill, (
            "a probe engine was evicted and rebuilt; the cache is too small for the target set"
        )
        assert len(MANY_URLS) > 4, "this proves nothing while the set still fits the old size"
        assert len(MANY_URLS) <= DB_PROBE_ENGINE_CACHE_SIZE
    finally:
        _probe_engine.cache_clear()


def test_one_dead_target_fails_the_whole_gate(monkeypatch) -> None:
    """A live database must not vouch for a dead one.

    This is the hole the ``urls`` parameter exists to close: a RAG job whose
    vector store is unreachable while the operational database is fine would
    otherwise pass the gate, take its tier lock, and discover the dead store by
    timing out on it there.
    """
    import src.monitoring.db_availability as availability

    monkeypatch.setattr(availability, "probe_engine", lambda _engine: (True, 0.01))
    assert database_reachable(urls=["postgresql://live/db", "postgresql://dead/db"]) is True

    def _only_the_second_fails(engine: object) -> tuple[bool, float]:
        return ("dead" not in str(engine.url), 0.01)

    monkeypatch.setattr(availability, "probe_engine", _only_the_second_fails)

    assert database_reachable(urls=["postgresql://live/db", "postgresql://dead/db"]) is False
    assert database_reachable(urls=["postgresql://live/db"]) is True


def test_an_empty_target_list_falls_back_to_the_operational_database(monkeypatch) -> None:
    """Probing nothing would report every gate as passing.

    A job whose resolution found no targets -- a deployment with no dense RAG
    store, say -- reaches the gate with an empty sequence. Reading that as "all
    reachable" would let the job take its lock and fail on the same precondition
    it could have read first.
    """
    seen: list[str] = []
    import src.monitoring.db_availability as availability

    monkeypatch.setattr(
        availability,
        "probe_engine",
        lambda engine: (seen.append(str(engine.url)) is None, 0.01),
    )

    assert database_reachable(urls=()) is True
    assert len(seen) == 1, "an empty target list must still probe something"


def test_database_reachable_reflects_the_probe(monkeypatch) -> None:
    import src.monitoring.db_availability as availability

    monkeypatch.setattr(availability, "probe_engine", lambda _engine: (False, 0.0))
    assert database_reachable() is False

    monkeypatch.setattr(availability, "probe_engine", lambda _engine: (True, 0.01))
    assert database_reachable() is True


def test_default_timeout_stays_short() -> None:
    """게이트가 무의미해지지 않도록 기본 타임아웃은 몇 초를 넘지 않는다."""
    assert 0 < DB_PROBE_CONNECT_TIMEOUT_SECONDS <= 5
