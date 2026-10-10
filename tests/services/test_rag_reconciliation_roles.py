"""Which side is asked for a vector, and what an unanswered question means.

The gate compared `embedding_present` on both sides and called a missing vector
a finding on either. That asked the sparse store for a column nothing reads:
PostgreSQL sparse search runs through `to_tsvector` on the operational database,
and dense search reads the pgvector store, so a sparse store without vectors is
the design rather than a defect. Running that version against production reported
221,656 findings that were not drift.

These tests pin the replacement rule, and the rule is not "stop checking". Only
the role that holds vectors is asked; every other comparison -- identity, content
hash, version, status -- still applies to both sides, and a store that does not
declare a role is still asked. That last default is what keeps a silent
exoneration from replacing the false alarm.
"""

from __future__ import annotations

import pytest

from src.services.rag_reconciliation import (
    MANIFEST_ROLES,
    ROLE_DENSE,
    ROLE_SPARSE,
    ManifestEntry,
    entry_from_manifest_row,
    reconcile_manifests,
)

HASH = "a" * 64


def _entry(
    row_id: str = "1",
    *,
    role: str | None = None,
    embedding: bool | None = True,
    content_hash: str | None = HASH,
    index_version: str | None = "rag-v1",
    index_status: str | None = "ACTIVE",
) -> ManifestEntry:
    return ManifestEntry(
        source_table="game",
        source_row_id=row_id,
        content_hash=content_hash,
        index_version=index_version,
        index_status=index_status,
        embedding_present=embedding,
        updated_at=None,
        role=role,
    )


def _findings(left: ManifestEntry, right: ManifestEntry) -> list[str]:
    """Return the issue codes raised for the pair, without their keys.

    ``unexplained_issues`` maps an issue name to the keys that raised it, so the
    codes are the dictionary's keys, not its values.
    """
    report = reconcile_manifests([left], [right])
    return sorted(report.unexplained_issues)


class TestOnlyTheDenseStoreIsAskedForAVector:
    def test_a_sparse_store_without_vectors_passes(self) -> None:
        """The production shape: operational sparse store, pgvector dense store."""
        findings = _findings(_entry(role=ROLE_SPARSE, embedding=False), _entry(role=ROLE_DENSE, embedding=True))

        assert findings == []

    def test_a_dense_store_without_vectors_still_fails(self) -> None:
        """The direction must not be reversible by accident."""
        findings = _findings(_entry(role=ROLE_SPARSE, embedding=True), _entry(role=ROLE_DENSE, embedding=False))

        assert findings == ["EMBEDDING_MISSING"]

    def test_both_without_vectors_fails(self) -> None:
        findings = _findings(_entry(role=ROLE_SPARSE, embedding=False), _entry(role=ROLE_DENSE, embedding=False))

        assert findings == ["EMBEDDING_MISSING"]

    def test_a_declared_role_is_required_to_skip_the_check(self) -> None:
        """An undeclared role keeps the strict reading rather than the lenient one.

        A manifest that does not say what it holds is the one case where skipping
        would hide a real gap. Defaulting to exempt would turn every older
        manifest into a free pass.
        """
        findings = _findings(_entry(role=None, embedding=False), _entry(role=None, embedding=True))

        assert findings == ["EMBEDDING_MISSING"]


class TestAnUnmeasuredVectorIsNotAnAbsentOne:
    def test_unmeasured_is_reported_separately_from_missing(self) -> None:
        """`None` means the question was never answered.

        The old check read `is False`, so an exporter that stopped measuring
        would pass. The two are kept as distinct codes because the fixes differ:
        one is a backfill, the other is a broken measurement.
        """
        findings = _findings(_entry(role=ROLE_SPARSE, embedding=False), _entry(role=ROLE_DENSE, embedding=None))

        assert findings == ["EMBEDDING_UNMEASURED"]
        assert "EMBEDDING_MISSING" not in findings

    def test_an_unmeasured_dense_vector_is_not_a_pass(self) -> None:
        report = reconcile_manifests(
            [_entry(role=ROLE_SPARSE, embedding=False)],
            [_entry(role=ROLE_DENSE, embedding=None)],
        )

        assert report.is_clean is False

    def test_an_unmeasured_sparse_vector_is_ignored_with_the_rest(self) -> None:
        """The exemption covers unmeasured too -- the sparse side is not asked."""
        findings = _findings(_entry(role=ROLE_SPARSE, embedding=None), _entry(role=ROLE_DENSE, embedding=True))

        assert findings == []


class TestTheOtherComparisonsStillApplyToBothSides:
    def test_a_hash_mismatch_is_a_finding_even_when_the_sparse_side_has_no_vector(self) -> None:
        findings = _findings(
            _entry(role=ROLE_SPARSE, embedding=False, content_hash="a" * 64),
            _entry(role=ROLE_DENSE, embedding=True, content_hash="b" * 64),
        )

        assert findings == ["CONTENT_HASH_MISMATCH"]

    def test_a_version_mismatch_is_a_finding_even_when_the_sparse_side_has_no_vector(self) -> None:
        findings = _findings(
            _entry(role=ROLE_SPARSE, embedding=False, index_version="rag-v1"),
            _entry(role=ROLE_DENSE, embedding=True, index_version="rag-v2"),
        )

        assert findings == ["INDEX_VERSION_MISMATCH"]

    def test_a_status_mismatch_is_a_finding_even_when_the_sparse_side_has_no_vector(self) -> None:
        findings = _findings(
            _entry(role=ROLE_SPARSE, embedding=False, index_status="ACTIVE"),
            _entry(role=ROLE_DENSE, embedding=True, index_status="DELETED"),
        )

        assert findings == ["INDEX_STATUS_MISMATCH"]

    def test_a_missing_identity_is_still_a_finding(self) -> None:
        report = reconcile_manifests([_entry("1", role=ROLE_SPARSE, embedding=False)], [])

        assert "MISSING_IN_RIGHT" in report.unexplained_issues


class TestTheRoleIsStrictEnoughToBeWorthDeclaring:
    def test_an_unknown_role_is_refused_rather_than_exonerated(self) -> None:
        """A typo must not become a free pass.

        `"vector"`, `"dense "` and `"DENSE"` are all near-misses someone will
        write. Treating them as unrecognised-and-therefore-exempt would disable
        the check on exactly the store that needs it, and the manifest would look
        healthy while nothing was verified.
        """
        with pytest.raises(ValueError, match="unknown manifest role"):
            entry_from_manifest_row(
                {
                    "source_table": "game",
                    "source_row_id": "1",
                    "role": "vector",
                },
            )

    def test_the_known_roles_are_exactly_these(self) -> None:
        assert {"sparse", "dense"} == MANIFEST_ROLES

    @pytest.mark.parametrize("role", sorted(MANIFEST_ROLES))
    def test_a_declared_role_survives_a_round_trip(self, role: str) -> None:
        entry = _entry(role=role)
        restored = entry_from_manifest_row(entry.to_manifest_dict())

        assert restored.role == role
        assert restored.requires_embeddings == entry.requires_embeddings

    def test_an_absent_role_round_trips_as_absent(self) -> None:
        """The field is additive: manifests written before it still read."""
        restored = entry_from_manifest_row({"source_table": "game", "source_row_id": "1"})

        assert restored.role is None
        assert restored.requires_embeddings is True
