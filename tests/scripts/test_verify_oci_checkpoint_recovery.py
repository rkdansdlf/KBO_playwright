from __future__ import annotations

import pytest

import scripts.verification.verify_oci_checkpoint_recovery as verify


@pytest.fixture
def _no_oracle_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Clear Oracle configuration so verification takes the SKIPPED path."""
    for var in ("ORACLE_TARGET_URL", "OCI_DB_URL", "TNS_ADMIN", "OCI_WALLET_PASSWORD"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("DATABASE_URL", "sqlite:///:memory:")


def test_run_verification_returns_skipped_without_oracle_config(
    _no_oracle_env: None,
) -> None:
    """Missing Oracle config must return SKIPPED instead of falling through."""
    report = verify.run_verification()

    assert report.status == "SKIPPED_NO_ORACLE_CONFIG"
    assert report.initial_synced_count == 0
    assert report.cleanup_success is True
