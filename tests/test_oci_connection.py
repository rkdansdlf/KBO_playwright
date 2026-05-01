from __future__ import annotations

import os

import pytest
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


load_dotenv()


@pytest.mark.integration
def test_oci_connection_from_env():
    """Opt-in smoke test for OCI/Postgres connectivity.

    This avoids hard-coded credentials and import-time network calls so the
    default pytest suite remains deterministic.
    """
    db_url = os.getenv("OCI_TEST_DATABASE_URL") or os.getenv("OCI_DB_URL")
    if not db_url:
        pytest.skip("OCI_TEST_DATABASE_URL or OCI_DB_URL is not set")

    engine = create_engine(db_url, echo=False, connect_args={"connect_timeout": 5})
    try:
        with engine.connect() as conn:
            version = conn.execute(text("SELECT version()")).scalar_one()
    finally:
        engine.dispose()

    assert "PostgreSQL" in version
