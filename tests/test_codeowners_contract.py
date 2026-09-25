"""CODEOWNERS contract.

A code owner who lacks repository access is ignored by GitHub *silently*: the
file keeps looking correct and review requirements quietly stop firing. This
repository is single-owner, so a CODEOWNERS entry pointing anywhere else is
always a mistake.

These checks are offline. They compare the handles in CODEOWNERS against the
remote repository owner, which is what actually determines access.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
CODEOWNERS = ROOT / ".github" / "CODEOWNERS"

#: Areas where a wrong owner lets a change reach production without review.
#: Each must have its own entry, not just the `*` fallback.
PROTECTED_PATHS = (
    "/.github/",
    "/src/db/",
    "/src/models/",
    "/src/repositories/",
    "/src/sync/",
    "/src/crawlers/",
    "/src/scheduler/",
    "/migrations/",
    "/tests/",
    "/pyproject.toml",
    "/uv.lock",
    "/Dockerfile",
    "/docker-compose.prod.yml",
)

OWNER_PATTERN = re.compile(r"@([A-Za-z0-9](?:[A-Za-z0-9]|-(?=[A-Za-z0-9])){0,38})")


def _remote_owner() -> str:
    """Return the account that owns the repository, from the git remote."""
    result = subprocess.run(
        ["git", "remote", "get-url", "origin"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    url = result.stdout.strip()
    match = re.search(r"github\.com[:/](?P<owner>[^/]+)/", url)
    if match is None:
        pytest.skip(f"origin remote is not a GitHub URL: {url}")
    return match.group("owner")


def _entries() -> list[tuple[str, list[str]]]:
    """Return (pattern, owners) for each active CODEOWNERS line."""
    parsed: list[tuple[str, list[str]]] = []
    for raw in CODEOWNERS.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        parsed.append((parts[0], parts[1:]))
    return parsed


def test_codeowners_file_exists() -> None:
    assert CODEOWNERS.is_file(), ".github/CODEOWNERS is missing"


def test_every_owner_can_actually_be_reviewed() -> None:
    """An owner without repository access makes the whole file a no-op."""
    owner = _remote_owner()
    strangers = {o for _, owners in _entries() for o in owners if o.lstrip("@") != owner}
    assert not strangers, f"CODEOWNERS references non-collaborators: {sorted(strangers)} (repo owner: {owner})"


def test_every_entry_declares_an_owner() -> None:
    ownerless = [pattern for pattern, owners in _entries() if not owners]
    assert not ownerless, f"CODEOWNERS patterns with no owner: {ownerless}"


def test_default_covers_everything() -> None:
    assert any(pattern == "*" for pattern, _ in _entries()), "CODEOWNERS has no `*` default"


@pytest.mark.parametrize("path", PROTECTED_PATHS)
def test_protected_area_has_an_explicit_owner(path: str) -> None:
    """A protected path must be named, so review can be required for it."""
    patterns = {pattern for pattern, _ in _entries()}
    assert path in patterns, f"CODEOWNERS has no explicit entry for {path}"
