"""운영 DB fail-fast 프로브 계약.

스케줄러 잡은 고정 cron으로 돌기 때문에, DB가 죽었을 때 잡이 연결 타임아웃을
기다리며 tier lock을 붙잡고 있으면 같은 락을 쓰는 다른 잡이 전부 굶는다
(2026-10-03 장애: DLQ 잡당 약 150초, maintenance 락 스킵 발생). 그래서 프로브는
짧은 ``connect_timeout``을 강제한다.
"""

from __future__ import annotations

from src.db.engine import (
    DB_PROBE_CONNECT_TIMEOUT_SECONDS,
    _probe_engine,
    database_reachable,
)

POSTGRES_URL = "postgresql+psycopg2://probe_user:probe_secret@db.example:5432/kbo"


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


def test_database_reachable_reflects_the_probe(monkeypatch) -> None:
    import src.monitoring.db_availability as availability

    monkeypatch.setattr(availability, "probe_engine", lambda _engine: (False, 0.0))
    assert database_reachable() is False

    monkeypatch.setattr(availability, "probe_engine", lambda _engine: (True, 0.01))
    assert database_reachable() is True


def test_default_timeout_stays_short() -> None:
    """게이트가 무의미해지지 않도록 기본 타임아웃은 몇 초를 넘지 않는다."""
    assert 0 < DB_PROBE_CONNECT_TIMEOUT_SECONDS <= 5
