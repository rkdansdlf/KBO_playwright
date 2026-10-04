"""``ConfigManager`` must report the database it is actually pointed at.

The dialect was derived with ``"oracle" in db_url``, so every other backend
labelled "sqlite". This deployment runs on PostgreSQL, which means
``kbo config --env production`` reported the production database as a local
SQLite file -- and, because ``DatabaseConfig.dialect`` is what the report
serialises, nothing downstream could notice.

Measured before the fix, one process per URL so the settings singleton could not
leak between cases:

    postgresql://      -> 'sqlite'
    sqlite:///          -> 'sqlite'   (right by accident)
    oracle+thick://     -> 'oracle'
    mysql://            -> 'sqlite'

``ConfigManager.load_settings`` caches a ``PlatformSettings`` singleton, so every
case here forces a reload; otherwise the first URL wins and the rest measure the
cache rather than the code.
"""

from __future__ import annotations

import pytest

from src.config.manager import ConfigManager


@pytest.fixture(autouse=True)
def _no_settings_singleton() -> None:
    """Keep the cached settings from leaking into or out of these cases."""
    yield
    ConfigManager._instance = None


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("postgresql://kbo:secret@db.internal:5432/kbo", "postgresql"),
        ("postgresql+psycopg2://kbo:secret@db.internal:5432/kbo", "postgresql"),
        ("sqlite:///./data/kbo_dev.db", "sqlite"),
        ("oracle+thick://kbo:secret@db.internal:1521/kbo", "oracle"),
        ("mysql://kbo:secret@db.internal:3306/kbo", "mysql"),
    ],
)
def test_the_configured_dialect_is_reported_honestly(
    url: str,
    expected: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The report must name the backend the URL actually selects."""
    monkeypatch.setenv("DATABASE_URL", url)

    settings = ConfigManager.load_settings(force_reload=True)

    assert settings.database.dialect == expected


def test_an_unparseable_url_degrades_instead_of_raising(monkeypatch: pytest.MonkeyPatch) -> None:
    """A malformed value is a configuration problem to report, not a crash.

    ``kbo config`` exists precisely to describe a broken environment, so raising
    here would remove the tool's ability to say anything at all.
    """
    monkeypatch.setenv("DATABASE_URL", "definitely not a url")

    settings = ConfigManager.load_settings(force_reload=True)

    assert settings.database.dialect  # a usable, non-empty description


def test_the_production_postgres_url_is_not_called_sqlite(monkeypatch: pytest.MonkeyPatch) -> None:
    """Named explicitly, because this is the URL this deployment actually runs."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://kbo:secret@100.81.73.13:5432/kbo")

    settings = ConfigManager.load_settings(force_reload=True)

    assert settings.database.dialect == "postgresql"
