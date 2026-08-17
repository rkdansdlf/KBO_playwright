"""Read-only Oracle connection smoke coverage."""

from __future__ import annotations

import os
from datetime import date

import pytest
from dotenv import load_dotenv
from sqlalchemy import delete, inspect, select, text
from sqlalchemy.orm import sessionmaker


pytestmark = pytest.mark.oci


def _oci_target_url() -> str:
    """Return the explicitly configured OCI URL or skip the smoke test."""
    load_dotenv()
    if os.getenv("KBO_RUN_OCI_INTEGRATION") != "1":
        pytest.skip("Set KBO_RUN_OCI_INTEGRATION=1 to run Oracle smoke tests")
    url = os.getenv("OCI_DB_URL")
    if not url or not url.startswith("oracle"):
        pytest.skip("OCI_DB_URL is not configured with an Oracle URL")
    configured_database_url = os.getenv("DATABASE_URL")
    if configured_database_url and url == configured_database_url:
        pytest.fail("OCI_DB_URL must be separate from DATABASE_URL")
    return url


def _create_oci_engine():
    """Create an engine for the explicitly configured OCI verification target."""
    from src.db.engine import create_engine_for_url

    return create_engine_for_url(
        _oci_target_url(),
        tns_admin=os.getenv("TNS_ADMIN"),
        wallet_password=os.getenv("OCI_WALLET_PASSWORD"),
    )


def test_oracle_oci_read_only_smoke() -> None:
    """Verify Oracle connectivity and the migrated baseline without writes."""
    engine = _create_oci_engine()
    try:
        with engine.connect() as connection:
            assert connection.dialect.name == "oracle"
            current_user = connection.execute(
                text("SELECT SYS_CONTEXT('USERENV', 'CURRENT_USER') FROM dual"),
            ).scalar_one()
            current_schema = connection.execute(
                text("SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM dual"),
            ).scalar_one()
            migration_count = connection.execute(
                text("SELECT COUNT(*) FROM schema_migrations"),
            ).scalar_one()
            table_names = {name.casefold() for name in inspect(connection).get_table_names()}
    finally:
        engine.dispose()

    assert current_user
    assert current_schema
    assert migration_count >= 1
    assert {"game", "awards", "schema_migrations"} <= table_names


def test_oracle_repository_rollback_and_upsert() -> None:
    """Verify repository rollback and idempotent update behavior on ADMIN."""
    from src.models.kbo_press_release import KboPressRelease
    from src.repositories.press_release_repository import KboPressReleaseRepository

    engine = _create_oci_engine()
    session_factory = sessionmaker(bind=engine, expire_on_commit=False)
    notice_id = "OCI_SMOKE_REPOSITORY"
    payload = {
        "notice_id": notice_id,
        "published_date": date(2026, 8, 16),
        "category": "smoke",
        "title": "OCI smoke original",
        "source_url": "https://example.invalid/oci-smoke",
    }
    try:
        with engine.begin() as connection:
            connection.execute(delete(KboPressRelease).where(KboPressRelease.notice_id == notice_id))

        with session_factory() as session:
            KboPressReleaseRepository(session).save_press_release(payload)
            session.rollback()

        with session_factory() as session:
            rolled_back = session.execute(
                select(KboPressRelease).where(KboPressRelease.notice_id == notice_id),
            ).scalar_one_or_none()
            assert rolled_back is None

        with session_factory() as session:
            repository = KboPressReleaseRepository(session)
            repository.save_press_release(payload)
            session.commit()
            repository.save_press_release({**payload, "title": "OCI smoke updated"})
            session.commit()

        with session_factory() as session:
            records = list(
                session.execute(select(KboPressRelease).where(KboPressRelease.notice_id == notice_id)).scalars(),
            )
            assert len(records) == 1
            assert records[0].title == "OCI smoke updated"
    finally:
        with engine.begin() as connection:
            connection.execute(delete(KboPressRelease).where(KboPressRelease.notice_id == notice_id))
        engine.dispose()
