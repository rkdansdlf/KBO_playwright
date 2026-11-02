"""Assemble the rekey manifest that ``apply_rag_rekey`` validates and applies.

Two producers write this format — the identity census and the legacy content
matcher — and one consumer reads it. Keeping the shape in one place is not
tidiness: ``apply_rag_rekey`` refuses a manifest whose ``manifest_sha256`` does
not match its own body, whose ``git_commit_sha`` differs from the current HEAD,
or whose ``expected_entry_count`` disagrees with ``len(entries)``. A producer
that got any of those subtly wrong would be rejected at apply time, after the
work of building it.

The hash covers everything **except** the header, so the header cannot contain
its own digest. :func:`build_manifest` assembles the body first, hashes it, and
only then attaches the header.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from src.constants import KST

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

MANIFEST_SCHEMA_VERSION = "r2-rekey-manifest-v1"
IDENTITY_SCHEMA_VERSION = "r2"
#: ``git`` is resolved through ``PATH`` rather than a fixed ``/usr/bin/git``.
#: The scheduler runs on Windows where that path does not exist, and the
#: committed absolute path made every manifest generated there carry
#: ``git_commit_sha: "unknown"`` — which ``apply_rag_rekey`` then refuses.
_GIT_EXECUTABLE = "git"


def current_git_sha() -> str:
    """Return HEAD's sha, or ``"unknown"`` when git cannot answer.

    ``"unknown"`` is not a fallback the applier accepts: it compares the value to
    the current HEAD, so a manifest built without git is rejected rather than
    applied against the wrong revision.
    """
    try:
        completed = subprocess.run(  # noqa: S603
            [_GIT_EXECUTABLE, "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
            cwd=Path.cwd(),
        )
    except subprocess.CalledProcessError, FileNotFoundError, OSError:
        return "unknown"
    return completed.stdout.strip() or "unknown"


def database_fingerprint() -> str:
    """Return a short digest of the target database, with credentials masked."""
    url = os.getenv("DATABASE_URL", "")
    masked = re.sub(r"://[^:]+:[^@]+@", "://***:***@", url)
    content = f"{masked}|{os.getenv('RAG_INDEX_VERSION', 'rag-v1')}".encode()
    return hashlib.sha256(content).hexdigest()[:16]


def manifest_sha256(body: Mapping[str, object]) -> str:
    """Hash a manifest body the way the applier recomputes it."""
    rendered = json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(rendered).hexdigest()


def build_manifest(  # noqa: PLR0913 (all keyword-only; one per manifest section)
    *,
    manifest_version: str,
    target_index_version: str,
    source_tables: Sequence[str],
    totals: Mapping[str, object],
    unsafe_entry_count: int,
    sources: Sequence[Mapping[str, object]],
    entries: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    """Return a manifest whose header hash covers its whole body.

    The per-disposition counts are derived from ``entries`` rather than accepted
    as an argument: two sources of truth for the same number is how a manifest
    ends up disagreeing with itself.

    Args:
        manifest_version: Producer identifier stored beside the entries.
        target_index_version: Index version the entries are written against.
        source_tables: Source tables the manifest covers.
        totals: Producer's own summary counts.
        unsafe_entry_count: Entries a producer flagged as unsafe to apply.
        sources: Per-source summaries.
        entries: The rekey entries, already serialized.

    Returns:
        The manifest with ``manifest_header`` attached.

    """
    body: dict[str, object] = {
        "manifest_version": manifest_version,
        "target_index_version": target_index_version,
        "read_only": True,
        "source_tables": list(source_tables),
        "totals": dict(totals),
        "unsafe_entry_count": unsafe_entry_count,
        "sources": [dict(source) for source in sources],
        "entries": [dict(entry) for entry in entries],
    }
    header = {
        "manifest_schema_version": MANIFEST_SCHEMA_VERSION,
        "identity_schema_version": IDENTITY_SCHEMA_VERSION,
        "generated_at": datetime.now(KST).isoformat(),
        "database_fingerprint": database_fingerprint(),
        "git_commit_sha": current_git_sha(),
        "manifest_sha256": manifest_sha256(body),
        "expected_entry_count": len(entries),
        "expected_disposition_counts": dict(Counter(str(entry.get("disposition")) for entry in entries)),
    }
    return {"manifest_header": header, **body}
