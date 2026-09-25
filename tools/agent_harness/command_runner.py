"""Single subprocess boundary for the agent Harness.

All external processes must pass through :class:`CommandRunner` so the
permission policy cannot be bypassed by direct ``subprocess.run`` calls.
Execution always uses ``shell=False`` with an allowlisted argv.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import sysconfig
import time
from dataclasses import dataclass, field
from pathlib import Path, PureWindowsPath
from typing import TYPE_CHECKING

from tools.agent_harness.dto import PermissionDecision
from tools.agent_harness.exceptions import PermissionDeniedError

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from tools.agent_harness.permissions import PermissionPolicy
    from tools.agent_harness.verifier import CommandResult

IS_WINDOWS = os.name == "nt"


@dataclass(frozen=True)
class CommandRunner:
    """Execute allowlisted commands with a sanitized environment."""

    permissions: PermissionPolicy
    root: Path
    _trusted_path: str = field(init=False, repr=False)

    def __post_init__(self) -> None:
        """Capture fixed trusted executable roots and normalize the repository root."""
        object.__setattr__(self, "root", self.root.resolve())
        trusted_path = os.pathsep.join(str(path) for path in self._trusted_executable_roots())
        object.__setattr__(self, "_trusted_path", trusted_path)

    @staticmethod
    def _windows_directory(function_name: str) -> Path | None:
        import ctypes

        try:
            buffer = ctypes.create_unicode_buffer(32768)
            function = getattr(ctypes.WinDLL("kernel32", use_last_error=True), function_name)
            if function(buffer, len(buffer)):
                return Path(buffer.value)
        except (AttributeError, OSError):
            return None
        return None

    @classmethod
    def _trusted_executable_roots(cls) -> tuple[Path, ...]:
        scripts = Path(sysconfig.get_path("scripts"))
        roots = [scripts, Path(sys.executable).resolve().parent]
        if IS_WINDOWS:
            windows_root = cls._windows_directory("GetWindowsDirectoryW")
            system_directory = cls._windows_directory("GetSystemDirectoryW")
            if windows_root is not None:
                roots.extend((windows_root, windows_root.parent / "Program Files" / "Git" / "cmd"))
            if system_directory is not None:
                roots.append(system_directory)
        else:
            roots.extend((Path("/opt/homebrew/bin"), Path("/usr/local/bin"), Path("/usr/bin"), Path("/bin")))
        return tuple(dict.fromkeys(roots))

    @classmethod
    def _trusted_executable_targets(cls) -> tuple[Path, ...]:
        roots = list(cls._trusted_executable_roots())
        if not IS_WINDOWS:
            roots.extend((Path("/opt/homebrew/Cellar"), Path("/opt/homebrew/opt")))
        return tuple(dict.fromkeys(root.resolve() for root in roots))

    def _resolve_program(self, program: str) -> str:
        """Resolve an allowlisted program through the runner's trusted PATH."""
        if self.permissions.is_python_launcher(program):
            return sys.executable
        resolved = shutil.which(program, path=self._trusted_path)
        if resolved is None:
            msg = f"Allowlisted executable is unavailable in the trusted PATH: {program}"
            raise PermissionDeniedError(msg)
        resolved_name = PureWindowsPath(resolved).name if IS_WINDOWS else Path(resolved).name
        allowed_names = {program.casefold()} if IS_WINDOWS else {program}
        if IS_WINDOWS:
            allowed_names.update(f"{program.casefold()}{suffix}" for suffix in (".exe", ".cmd", ".bat"))
        name_matches = resolved_name.casefold() in allowed_names if IS_WINDOWS else resolved_name in allowed_names
        if not name_matches:
            msg = f"Trusted executable resolution changed the program name: {program}"
            raise PermissionDeniedError(msg)
        resolved_path = Path(resolved).resolve()
        trusted_targets = self._trusted_executable_targets()
        if not any(resolved_path == root or root in resolved_path.parents for root in trusted_targets):
            msg = f"Trusted executable resolved outside approved roots: {program}"
            raise PermissionDeniedError(msg)
        return str(resolved_path)

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
        canonical_argv = [self._resolve_program(str(argv[0])), *map(str, argv[1:])]
        cwd_decision = self.permissions.check_cwd(cwd or self.root)
        if not cwd_decision.allowed or cwd_decision.matched_rule is None:
            msg = f"Command working directory denied: {cwd_decision.reason}"
            raise PermissionDeniedError(msg)
        working_directory = self.root / cwd_decision.matched_rule
        base_env: dict[str, str] = dict(env) if env is not None else dict(os.environ)
        clean_env = self.permissions.sanitize_environment(base_env, skill_id)
        clean_env["PATH"] = self._trusted_path
        start = time.perf_counter()
        completed = subprocess.run(  # noqa: S603 -- argv allowlisted above, shell=False
            canonical_argv,
            cwd=working_directory,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            env=clean_env,
            shell=False,
        )
        return _CommandResult(
            argv=tuple(canonical_argv),
            exit_code=completed.returncode,
            duration_ms=(time.perf_counter() - start) * 1000,
            stdout=self.permissions.redact(completed.stdout),
            stderr=self.permissions.redact(completed.stderr),
        )


__all__ = ["CommandRunner"]
