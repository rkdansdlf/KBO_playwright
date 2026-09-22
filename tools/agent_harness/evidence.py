"""Persist atomic, redacted Harness evidence bundles."""

from __future__ import annotations

import json
import secrets
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tools.agent_harness.permissions import PermissionPolicy


@dataclass(frozen=True)
class EvidenceStore:
    """Write one run's evidence beneath the configured artifact directory."""

    root: Path
    permissions: PermissionPolicy

    @classmethod
    def create(cls, artifacts_root: Path, permissions: PermissionPolicy) -> EvidenceStore:
        """Create a unique evidence directory for a Harness run."""
        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        run_id = f"{stamp}-{secrets.token_hex(4)}"
        run_root = artifacts_root / run_id
        relative = run_root.relative_to(artifacts_root.parents[1])
        if not permissions.can_write(relative):
            msg = f"Harness policy denied artifact path: {relative}"
            raise PermissionError(msg)
        run_root.mkdir(parents=True, exist_ok=False)
        return cls(root=run_root, permissions=permissions)

    @classmethod
    def open(cls, artifacts_root: Path, run_id: str, permissions: PermissionPolicy) -> EvidenceStore:
        """Open an existing run after constraining the run identifier."""
        if not run_id or Path(run_id).name != run_id:
            msg = "Invalid Harness run ID"
            raise ValueError(msg)
        run_root = artifacts_root / run_id
        if not run_root.is_dir():
            msg = f"Harness run not found: {run_id}"
            raise FileNotFoundError(msg)
        return cls(root=run_root, permissions=permissions)

    @property
    def run_id(self) -> str:
        """Return the evidence directory name used as the run identifier."""
        return self.root.name

    def write_json(self, name: str, payload: object) -> Path:
        """Atomically write a redacted JSON evidence document."""
        target = self.root / name
        rendered = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
        temp = target.with_suffix(f"{target.suffix}.tmp")
        temp.write_text(self.permissions.redact(rendered) + "\n", encoding="utf-8")
        temp.replace(target)
        return target

    def append_jsonl(self, name: str, payload: object) -> Path:
        """Append one redacted JSON event to an evidence stream."""
        target = self.root / name
        rendered = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        with target.open("a", encoding="utf-8") as stream:
            stream.write(self.permissions.redact(rendered) + "\n")
        return target

    def write_text(self, name: str, content: str) -> Path:
        """Atomically write a redacted text artifact."""
        target = self.root / name
        temp = target.with_suffix(f"{target.suffix}.tmp")
        temp.write_text(self.permissions.redact(content), encoding="utf-8")
        temp.replace(target)
        return target


__all__ = ["EvidenceStore"]
