"""Build a local, secret-safe Harness context manifest."""

from __future__ import annotations

import hashlib
import subprocess
from dataclasses import dataclass
from typing import TYPE_CHECKING

from tools.agent_harness.command_runner import CommandRunner
from tools.agent_harness.exceptions import PermissionDeniedError

if TYPE_CHECKING:
    from pathlib import Path

    from tools.agent_harness.permissions import PermissionPolicy
    from tools.agent_harness.registry import HarnessRegistry
    from tools.agent_harness.router import RouteDecision


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


@dataclass(frozen=True)
class ContextBuilder:
    """Create context metadata without handing repository contents to adapters."""

    registry: HarnessRegistry
    permissions: PermissionPolicy

    def build(self, decision: RouteDecision | None = None) -> dict[str, object]:
        """Return hashes and selected engines for reproducible context assembly."""
        root = self.registry.root
        tracked_inputs = ("AGENTS.md", ".agent-harness/harness.yaml", "harness.lock.json")
        files = {
            path: _sha256(root / path)
            for path in tracked_inputs
            if (root / path).is_file() and self.permissions.can_read(path)
        }
        runner = CommandRunner(permissions=self.permissions, root=root)
        try:
            result = runner.run(["git", "rev-parse", "HEAD"], skill_id="harness", timeout_seconds=10)
        except (OSError, subprocess.SubprocessError, PermissionDeniedError):
            result = None
        revision = result.stdout.strip() if result is not None and result.exit_code == 0 else ""
        return {
            "git_revision": revision or "unknown",
            "files": files,
            "context_engines": list(decision.context) if decision else ["graphify"],
            "external_execution": "reference_only",
            "secrets_included": False,
        }


__all__ = ["ContextBuilder"]
