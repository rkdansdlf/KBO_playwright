"""Phase 105 Gate 4: Identity Verification Protocol Test Suite.

Certifies the 5-SYS_CONTEXT runtime identity verification:
1. Valid allowlist instantiation and immutability (frozen dataclass).
2. Fail-closed rejection of invalid identifier characters (SQL injection protection).
3. Exact matching of all 5 descriptors (DB_UNIQUE_NAME, SERVICE_NAME, CURRENT_SCHEMA, SESSION_USER, CON_NAME).
4. Detection of individual mismatches, empty values, and case-insensitive canonical matching.
5. Fail-closed behavior on database/connection exceptions.
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from src.services.staging_identity import (
    IDENTITY_DESCRIPTORS,
    StagingIdentityAllowlist,
    probe_runtime_identity,
    verify_staging_identity,
)


@pytest.fixture
def valid_allowlist() -> StagingIdentityAllowlist:
    return StagingIdentityAllowlist(
        db_unique_name="kbo_staging_iad",
        service_name="kbo_staging_high.adb.oraclecloud.com",
        current_schema="KBO_STAGING",
        session_user="KBO_RAG_REHEARSAL_USER",
        con_name="KBO_STAGING_PDB",
    )


@pytest.fixture
def mock_session() -> Session:
    engine = create_engine("sqlite:///:memory:")
    return Session(bind=engine)


class TestStagingIdentityAllowlist:
    """Certify allowlist validation and contract enforcement."""

    def test_valid_allowlist_instantiation(self, valid_allowlist: StagingIdentityAllowlist) -> None:
        """Allowlist instantiates cleanly with valid descriptors."""
        assert valid_allowlist.db_unique_name == "kbo_staging_iad"
        assert valid_allowlist.service_name == "kbo_staging_high.adb.oraclecloud.com"
        assert valid_allowlist.current_schema == "KBO_STAGING"
        assert valid_allowlist.session_user == "KBO_RAG_REHEARSAL_USER"
        assert valid_allowlist.con_name == "KBO_STAGING_PDB"
        assert len(valid_allowlist.to_dict()) == 5

    def test_allowlist_immutability(self, valid_allowlist: StagingIdentityAllowlist) -> None:
        """Allowlist is frozen and cannot be mutated after creation."""
        with pytest.raises(FrozenInstanceError):
            valid_allowlist.current_schema = "MUTATED"  # type: ignore[misc]

    @pytest.mark.parametrize("empty_field", list(IDENTITY_DESCRIPTORS))
    def test_allowlist_rejects_empty_descriptor(self, empty_field: str) -> None:
        """Allowlist fails closed if any descriptor is empty."""
        kwargs = {
            "db_unique_name": "kbo_staging",
            "service_name": "kbo_service",
            "current_schema": "KBO_STAGING",
            "session_user": "KBO_USER",
            "con_name": "KBO_PDB",
        }
        kwargs[empty_field] = ""
        with pytest.raises(ValueError, match="must be a non-empty string"):
            StagingIdentityAllowlist(**kwargs)

    @pytest.mark.parametrize(
        "malicious_value",
        [
            "staging; DROP TABLE users;",
            "staging' OR '1'='1",
            "staging--comment",
            "staging/*comment*/",
            "staging space",
        ],
    )
    def test_allowlist_rejects_malicious_characters(self, malicious_value: str) -> None:
        """Allowlist fails closed if any descriptor contains SQL injection characters."""
        with pytest.raises(ValueError, match="contains invalid characters"):
            StagingIdentityAllowlist(
                db_unique_name=malicious_value,
                service_name="kbo_staging_high.adb.oraclecloud.com",
                current_schema="KBO_STAGING",
                session_user="KBO_RAG_REHEARSAL_USER",
                con_name="KBO_STAGING_PDB",
            )


class TestVerifyStagingIdentity:
    """Certify runtime probe execution and verification matching."""

    def test_all_descriptors_match_passes_cleanly(
        self,
        mock_session: Session,
        valid_allowlist: StagingIdentityAllowlist,
    ) -> None:
        """When all 5 descriptors match exactly, verification passes with 0 mismatches."""
        mock_session._mock_oracle_sys_context = valid_allowlist.to_dict()  # type: ignore[attr-defined]

        result = verify_staging_identity(mock_session, valid_allowlist)
        assert result.passed is True
        assert result.is_clean is True
        assert len(result.mismatches) == 0
        assert result.error is None

    def test_case_insensitive_canonical_match(
        self,
        mock_session: Session,
        valid_allowlist: StagingIdentityAllowlist,
    ) -> None:
        """Oracle uppercase returned names match lowercase expected values safely."""
        oracle_cased = {k: v.upper() for k, v in valid_allowlist.to_dict().items()}
        mock_session._mock_oracle_sys_context = oracle_cased  # type: ignore[attr-defined]

        result = verify_staging_identity(mock_session, valid_allowlist)
        assert result.passed is True
        assert result.is_clean is True

    @pytest.mark.parametrize("mismatched_desc", list(IDENTITY_DESCRIPTORS))
    def test_individual_descriptor_mismatch_fails_closed(
        self,
        mock_session: Session,
        valid_allowlist: StagingIdentityAllowlist,
        mismatched_desc: str,
    ) -> None:
        """Any single mismatched descriptor causes immediate verification failure."""
        runtime_context = valid_allowlist.to_dict()
        runtime_context[mismatched_desc] = "UNAUTHORIZED_PRODUCTION_INSTANCE"
        mock_session._mock_oracle_sys_context = runtime_context  # type: ignore[attr-defined]

        result = verify_staging_identity(mock_session, valid_allowlist)
        assert result.passed is False
        assert result.is_clean is False
        assert len(result.mismatches) == 1
        assert result.mismatches[0].descriptor == mismatched_desc
        assert result.mismatches[0].actual == "UNAUTHORIZED_PRODUCTION_INSTANCE"

    def test_missing_or_empty_context_descriptor_fails(
        self,
        mock_session: Session,
        valid_allowlist: StagingIdentityAllowlist,
    ) -> None:
        """Empty descriptor returned from runtime probe is reported as <NULL_OR_EMPTY>."""
        runtime_context = valid_allowlist.to_dict()
        runtime_context["con_name"] = ""
        mock_session._mock_oracle_sys_context = runtime_context  # type: ignore[attr-defined]

        result = verify_staging_identity(mock_session, valid_allowlist)
        assert result.passed is False
        assert len(result.mismatches) == 1
        assert result.mismatches[0].descriptor == "con_name"
        assert result.mismatches[0].actual == "<NULL_OR_EMPTY>"

    def test_probe_exception_fails_closed(
        self,
        mock_session: Session,
        valid_allowlist: StagingIdentityAllowlist,
    ) -> None:
        """Database or connection exception during probe fails closed with error reported."""
        mock_session.get_bind = MagicMock(side_effect=RuntimeError("Connection dropped"))  # type: ignore[method-assign]

        result = verify_staging_identity(mock_session, valid_allowlist)
        assert result.passed is False
        assert result.error is not None
        assert "Connection dropped" in result.error

    def test_oracle_probe_query_execution_syntax(self) -> None:
        """On Oracle dialect, probe query runs against DUAL and formats mapping."""
        mock_session = MagicMock(spec=Session)
        mock_bind = MagicMock()
        mock_bind.dialect.name = "oracle"
        mock_session.get_bind.return_value = mock_bind

        mock_row = {
            "db_unique_name": "kbo_staging_iad",
            "service_name": "kbo_staging_high.adb.oraclecloud.com",
            "current_schema": "KBO_STAGING",
            "session_user": "KBO_RAG_REHEARSAL_USER",
            "con_name": "KBO_STAGING_PDB",
        }
        mock_session.execute.return_value.mappings.return_value.fetchone.return_value = mock_row

        context = probe_runtime_identity(mock_session)
        assert context["db_unique_name"] == "kbo_staging_iad"
        assert context["session_user"] == "KBO_RAG_REHEARSAL_USER"
        mock_session.execute.assert_called_once()
