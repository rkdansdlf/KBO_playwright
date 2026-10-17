"""Fetch and vendor external skill adapters for the agent Harness.

Only skills with ``redistribution: allowed`` in ``harness.lock.json`` may be
vended. Skills with ``prohibited_pending_review`` or
``selected_asset_review_required`` are skipped unless explicitly overridden.

Vendor directory: ``vendor/skills/<skill_id>/`` (gitignored).
"""

from __future__ import annotations

import json
import shutil
import subprocess
from dataclasses import asdict, dataclass
from pathlib import Path  # noqa: TC003
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

LOCK_PATH = "harness.lock.json"
VENDOR_DIR = "vendor/skills"
ALLOWED_REDISTRIBUTION = {"allowed"}
_GIT = "git"


@dataclass(frozen=True)
class FetchResult:
    """Outcome of one skill fetch attempt."""

    skill_id: str
    status: str
    detail: str
    vendor_path: str | None = None

    def to_dict(self) -> dict[str, object]:
        """Serialize to JSON-compatible values."""
        return asdict(self)


def _load_lock(root: Path) -> dict[str, object]:
    """Load harness.lock.json."""
    lock_path = root / LOCK_PATH
    if not lock_path.exists():
        msg = f"harness.lock.json not found at {lock_path}"
        raise FileNotFoundError(msg)
    return json.loads(lock_path.read_text(encoding="utf-8"))


def _is_vendable(skill_data: dict[str, object]) -> bool:
    """Check if a skill is eligible for vending."""
    redistribution = skill_data.get("redistribution", "")
    return redistribution in ALLOWED_REDISTRIBUTION


def _vendor_path(root: Path, skill_id: str) -> Path:
    """Return the vendor path for a skill."""
    return root / VENDOR_DIR / skill_id


def _is_cached(root: Path, skill_id: str, revision: str) -> bool:
    """Check if a skill is already vended at the pinned revision."""
    vendor = _vendor_path(root, skill_id)
    revision_file = vendor / ".revision"
    if not vendor.exists() or not revision_file.exists():
        return False
    return revision_file.read_text(encoding="utf-8").strip() == revision


def _copy_license(vendor: Path) -> None:
    """Copy a license file into the vendor directory if present."""
    license_file: Path | None = None
    for candidate in ("LICENSE", "LICENSE.md", "LICENSE.txt", "LICENSE-MIT"):
        candidate_path = vendor / candidate
        if candidate_path.exists():
            license_file = candidate_path
            break
    if license_file is None:
        for f in vendor.iterdir():
            if f.name.lower().startswith("license"):
                license_file = f
                break
    if license_file is not None and license_file.exists():
        dest = vendor / "LICENSE"
        if license_file != dest:
            shutil.copy2(license_file, dest)


def _clone_and_checkout(vendor: Path, repository: str, revision: str) -> str | None:
    """Clone repository and checkout revision. Returns error message or None."""
    try:
        subprocess.run(  # noqa: S603
            [_GIT, "clone", "--depth", "1", "--no-checkout", repository, str(vendor)],
            check=True,
            capture_output=True,
            text=True,
            timeout=120,
        )
        subprocess.run(  # noqa: S603
            [_GIT, "-C", str(vendor), "fetch", "--depth", "1", "origin", revision],
            check=True,
            capture_output=True,
            text=True,
            timeout=120,
        )
        subprocess.run(  # noqa: S603
            [_GIT, "-C", str(vendor), "checkout", revision],
            check=True,
            capture_output=True,
            text=True,
            timeout=60,
        )
    except (subprocess.SubprocessError, OSError) as exc:
        return f"git clone/checkout failed: {exc}"
    return None


def _fetch_skill(
    root: Path,
    skill_id: str,
    skill_data: dict[str, object],
    *,
    force: bool = False,
) -> FetchResult:
    """Fetch and vendor one skill at its pinned revision."""
    revision = str(skill_data.get("revision", ""))
    repository = str(skill_data.get("repository", ""))
    license_name = str(skill_data.get("license", "NOASSERTION"))

    if not _is_vendable(skill_data):
        redistribution = skill_data.get("redistribution", "unknown")
        detail = f"redistribution={redistribution}, not vendable"
        return FetchResult(skill_id=skill_id, status="skipped", detail=detail)

    vendor = _vendor_path(root, skill_id)

    if not force and _is_cached(root, skill_id, revision):
        detail = f"already vended at {revision[:12]}"
        return FetchResult(
            skill_id=skill_id,
            status="cached",
            detail=detail,
            vendor_path=str(vendor),
        )

    if not repository:
        return FetchResult(
            skill_id=skill_id,
            status="failed",
            detail="no repository URL in lock",
        )

    if vendor.exists():
        shutil.rmtree(vendor)
    vendor.mkdir(parents=True, exist_ok=True)

    error = _clone_and_checkout(vendor, repository, revision)
    if error is not None:
        if vendor.exists():
            shutil.rmtree(vendor, ignore_errors=True)
        return FetchResult(skill_id=skill_id, status="failed", detail=error)

    git_dir = vendor / ".git"
    if git_dir.exists():
        shutil.rmtree(git_dir, ignore_errors=True)

    (vendor / ".revision").write_text(revision, encoding="utf-8")
    _copy_license(vendor)

    detail = f"vended at {revision[:12]} (license: {license_name})"
    return FetchResult(
        skill_id=skill_id,
        status="vended",
        detail=detail,
        vendor_path=str(vendor),
    )


def fetch_skills(
    root: Path,
    *,
    skill_ids: Sequence[str] | None = None,
    force: bool = False,
) -> list[FetchResult]:
    """Fetch and vendor skills from harness.lock.json.

    Args:
        root: Repository root path.
        skill_ids: Specific skills to fetch. None means all vendable skills.
        force: Force re-fetch even if cached.

    Returns:
        List of FetchResult for each attempted skill.

    """
    lock = _load_lock(root)
    skills = lock.get("skills", {})
    if not isinstance(skills, dict):
        return []

    results: list[FetchResult] = []
    for sid, data in skills.items():
        if not isinstance(data, dict):
            continue
        if skill_ids and sid not in skill_ids:
            continue
        result = _fetch_skill(root, sid, data, force=force)
        results.append(result)

    return results


__all__ = [
    "ALLOWED_REDISTRIBUTION",
    "VENDOR_DIR",
    "FetchResult",
    "fetch_skills",
]
