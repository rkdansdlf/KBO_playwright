"""Persist atomic, redacted Harness evidence bundles."""

from __future__ import annotations

import json
import os
import secrets
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from tools.agent_harness.exceptions import PermissionDeniedError

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
        decision = permissions.check_write(run_root, "harness")
        if not decision.allowed:
            msg = f"Harness policy denied artifact path: {run_root}"
            raise PermissionDeniedError(msg)
        run_root.mkdir(parents=True, exist_ok=False)
        return cls(root=run_root, permissions=permissions)

    @classmethod
    def open(cls, artifacts_root: Path, run_id: str, permissions: PermissionPolicy) -> EvidenceStore:
        """Open an existing run after validating its identifier and path."""
        if not run_id or run_id in {".", ".."} or Path(run_id).name != run_id or "/" in run_id or "\\" in run_id:
            msg = "Invalid Harness run ID"
            raise ValueError(msg)
        run_root = artifacts_root / run_id
        decision = permissions.check_write(run_root, "harness")
        if not decision.allowed:
            msg = f"Harness policy denied artifact path: {run_root}"
            raise PermissionDeniedError(msg)
        if not run_root.is_dir():
            msg = f"Harness run not found: {run_id}"
            raise FileNotFoundError(msg)
        return cls(root=run_root, permissions=permissions)

    @property
    def run_id(self) -> str:
        """Return the evidence directory name used as the run identifier."""
        return self.root.name

    def read_text(self, name: str) -> str:
        """Read one redacted evidence artifact through the safe path boundary."""
        return self.permissions.redact(self._target(name).read_text(encoding="utf-8"))

    def read_json(self, name: str) -> dict[str, object]:
        """Read one JSON evidence object through the safe path boundary."""
        payload = json.loads(self.read_text(name))
        if not isinstance(payload, dict):
            msg = f"Expected JSON object in Harness evidence artifact: {name}"
            raise TypeError(msg)
        return payload

    def _upgrade_command_records(
        self,
        records: list[dict[str, object]],
        *,
        final_verification_id: str,
        final_command_count: int,
    ) -> list[dict[str, object]]:
        command_positions = [index for index, record in enumerate(records) if record.get("event") is None]
        final_positions = set(command_positions[-final_command_count:]) if final_command_count else set()
        start_positions = [
            index for index, record in enumerate(records) if record.get("event") == "verification_started"
        ]
        final_start_position = start_positions[-1] if start_positions else -1
        history_id = f"legacy-history-{final_verification_id.removeprefix('legacy-')}"
        upgraded: list[dict[str, object]] = []
        for index, record in enumerate(records):
            verification_id = record.get("verification_id")
            if index in final_positions or index == final_start_position:
                verification_id = final_verification_id
            elif not verification_id:
                verification_id = history_id
            upgraded.append({**record, "verification_id": verification_id})
        return upgraded

    def needs_current_schema_upgrade(self) -> bool:
        """Return whether any structured record lacks current bundle identity metadata."""
        from tools.agent_harness.dto import EVIDENCE_SCHEMA_VERSION

        verification = self.read_json("verification.json")
        for name in ("task.json", "plan.json", "context.json", "verification.json"):
            payload = verification if name == "verification.json" else self.read_json(name)
            if payload.get("schema_version") != EVIDENCE_SCHEMA_VERSION or payload.get("run_id") != self.run_id:
                return True
        final_id = verification.get("verification_id")
        if "passed" in verification and not final_id:
            return True
        records = [json.loads(line) for line in self.read_text("commands.jsonl").splitlines() if line]
        if any(
            record.get("schema_version") != EVIDENCE_SCHEMA_VERSION
            or record.get("run_id") != self.run_id
            or not record.get("verification_id")
            for record in records
        ):
            return True
        return "passed" in verification and not any(
            record.get("event") == "verification_started" and record.get("verification_id") == final_id
            for record in records
        )

    def upgrade_current_schema(self) -> None:
        """Upgrade a readable legacy bundle to resumable current identity metadata."""
        from tools.agent_harness.dto import EVIDENCE_SCHEMA_VERSION

        verification = self.read_json("verification.json")
        final_verification_id = str(verification.get("verification_id") or f"legacy-{secrets.token_hex(8)}")
        final_commands = verification.get("commands", [])
        final_command_count = len(final_commands) if isinstance(final_commands, list) else 0
        for name in ("task.json", "plan.json", "context.json", "verification.json"):
            payload = verification if name == "verification.json" else self.read_json(name)
            if name == "verification.json" and "passed" in payload:
                payload = {**payload, "verification_id": final_verification_id}
            self.write_json(
                name,
                {**payload, "schema_version": EVIDENCE_SCHEMA_VERSION, "run_id": self.run_id},
            )
        for name in ("skill-trace.jsonl", "commands.jsonl"):
            records = [json.loads(line) for line in self.read_text(name).splitlines() if line]
            upgraded = (
                self._upgrade_command_records(
                    records,
                    final_verification_id=final_verification_id,
                    final_command_count=final_command_count,
                )
                if name == "commands.jsonl"
                else records
            )
            rendered = "\n".join(
                json.dumps(
                    {**record, "schema_version": EVIDENCE_SCHEMA_VERSION, "run_id": self.run_id},
                    ensure_ascii=False,
                    sort_keys=True,
                )
                for record in upgraded
            )
            self.write_text(name, rendered + ("\n" if rendered else ""))
        if "passed" in verification:
            records = [json.loads(line) for line in self.read_text("commands.jsonl").splitlines() if line]
            has_start = any(
                record.get("event") == "verification_started" and record.get("verification_id") == final_verification_id
                for record in records
            )
            if not has_start:
                self.append_jsonl(
                    "commands.jsonl",
                    {
                        "schema_version": EVIDENCE_SCHEMA_VERSION,
                        "run_id": self.run_id,
                        "event": "verification_started",
                        "verification_id": final_verification_id,
                        "profile": verification.get("profile", "unknown"),
                    },
                )

    def _target(self, name: str) -> Path:
        """Resolve one simple evidence filename inside the run directory."""
        if not name or name in {".", ".."} or Path(name).name != name or "/" in name or "\\" in name:
            msg = f"Invalid Harness evidence artifact name: {name}"
            raise PermissionDeniedError(msg)
        target = self.root / name
        decision = self.permissions.check_write(target, "harness")
        if not decision.allowed:
            msg = f"Harness policy denied evidence artifact path: {target}"
            raise PermissionDeniedError(msg)
        return target

    def _temp_target(self, target: Path) -> Path:
        """Return an unpredictable same-directory temporary artifact path."""
        return self._target(f".{target.name}.{secrets.token_hex(8)}.tmp")

    def write_json(self, name: str, payload: object) -> Path:
        """Atomically write a redacted JSON evidence document."""
        target = self._target(name)
        rendered = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
        temp = self._temp_target(target)
        with temp.open("x", encoding="utf-8") as stream:
            temp.chmod(0o600)
            stream.write(self.permissions.redact(rendered) + "\n")
        temp.replace(target)
        return target

    def append_jsonl(self, name: str, payload: object) -> Path:
        """Append one redacted JSON event to an evidence stream."""
        target = self._target(name)
        rendered = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        flags = os.O_APPEND | os.O_CREAT | os.O_WRONLY
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = os.open(target, flags, 0o600)
        with os.fdopen(descriptor, "a", encoding="utf-8") as stream:
            stream.write(self.permissions.redact(rendered) + "\n")
        return target

    def write_text(self, name: str, content: str) -> Path:
        """Atomically write a redacted text artifact."""
        target = self._target(name)
        temp = self._temp_target(target)
        with temp.open("x", encoding="utf-8") as stream:
            temp.chmod(0o600)
            stream.write(self.permissions.redact(content))
        temp.replace(target)
        return target


__all__ = ["EvidenceStore"]
