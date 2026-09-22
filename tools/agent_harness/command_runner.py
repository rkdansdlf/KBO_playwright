"""Single subprocess boundary for the agent Harness.

All external processes must pass through :class:`CommandRunner` so the
permission policy cannot be bypassed by direct ``subprocess.run`` calls.
Execution always uses ``shell=False`` with an allowlisted argv.
"""

from __future__ import annotations

import subprocess
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

from tools.agent_harness.dto import PermissionDecision
from tools.agent_harness.exceptions import PermissionDeniedError

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from pathlib import Path

    from tools.agent_harness.permissions import PermissionPolicy
    from tools.agent_harness.verifier import CommandResult


@dataclass(frozen=True)
class CommandRunner:
    """Execute allowlisted commands with a sanitized environment."""

    permissions: PermissionPolicy
    root: Path

    def run(
        self,
        argv: Sequence[str],
        *,
        skill_id: str,
        timeout_seconds: int = 300,
        cwd: Path | None = None,
        env: Mapping[str, str] | None = None,
    ) -> CommandResult:
        """Authorize, sanitize, and run one argv command without a shell."""
        from tools.agent_harness.verifier import CommandResult as _CommandResult

        decision = self.permissions.authorize_command(list(argv), skill_id)
        if decision.decision != PermissionDecision.ALLOW:
            msg = f"Command denied for skill {skill_id}: {decision.reason}"
            raise PermissionDeniedError(msg)
        import os

        base_env: dict[str, str] = dict(env) if env is not None else dict(os.environ)
        clean_env = self.permissions.sanitize_environment(base_env, skill_id)
        start = time.perf_counter()
        completed = subprocess.run(  # noqa: S603 -- argv allowlisted above, shell=False
            list(argv),
            cwd=cwd or self.root,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            env=clean_env,
            shell=False,
        )
        return _CommandResult(
            argv=tuple(argv),
            exit_code=completed.returncode,
            duration_ms=(time.perf_counter() - start) * 1000,
            stdout=self.permissions.redact(completed.stdout),
            stderr=self.permissions.redact(completed.stderr),
        )


__all__ = ["CommandRunner"]
