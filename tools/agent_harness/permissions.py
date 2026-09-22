"""Enforce Harness path, network, and secret-redaction policy."""

from __future__ import annotations

import fnmatch
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING

from tools.agent_harness.dto import PermissionDecision, PermissionResult
from tools.agent_harness.registry import _mapping, _strings, load_yaml_mapping, project_root

if TYPE_CHECKING:
    from pathlib import Path


def _matches(path: str, patterns: tuple[str, ...]) -> bool:
    normalized = path.replace("\\", "/").removeprefix("./").casefold()
    return any(
        fnmatch.fnmatch(normalized, variant)
        for pattern in patterns
        for variant in {pattern.casefold(), pattern.casefold().replace("**/", "")}
    )


@dataclass(frozen=True)
class PermissionPolicy:
    """Represent fail-closed permissions for third-party skill adapters."""

    deny_read: tuple[str, ...]
    restricted_read: tuple[str, ...]
    write_default: bool
    allowed_write: tuple[str, ...]
    network_default: bool
    network_skills: tuple[str, ...]
    redact_env: tuple[str, ...]
    allowed_executables: tuple[str, ...] = ()
    python_modules: tuple[str, ...] = ()
    forbidden_flags: tuple[str, ...] = ("--shell",)

    @classmethod
    def load(cls, root: Path | None = None) -> PermissionPolicy:
        """Load the repository Harness permission policy."""
        repo_root = (root or project_root()).resolve()
        payload = load_yaml_mapping(repo_root / ".agent-harness" / "policies" / "permissions.yaml")
        read_policy = _mapping(payload.get("read"), "permissions.read")
        write_policy = _mapping(payload.get("write"), "permissions.write")
        network_policy = _mapping(payload.get("network"), "permissions.network")
        commands_policy = _mapping(payload.get("commands", {}), "permissions.commands")
        return cls(
            deny_read=_strings(read_policy.get("deny", []), "permissions.read.deny"),
            restricted_read=_strings(read_policy.get("restricted", []), "permissions.read.restricted"),
            write_default=write_policy.get("default") is True,
            allowed_write=_strings(write_policy.get("allowed", []), "permissions.write.allowed"),
            network_default=network_policy.get("default") is True,
            network_skills=_strings(network_policy.get("allowed_skills", []), "permissions.network.allowed_skills"),
            redact_env=_strings(payload.get("redact_env", []), "permissions.redact_env"),
            allowed_executables=_strings(
                commands_policy.get("allowed_executables", []), "permissions.commands.allowed_executables"
            ),
            python_modules=_strings(commands_policy.get("python_modules", []), "permissions.commands.python_modules"),
            forbidden_flags=_strings(
                commands_policy.get("forbidden_flags", []), "permissions.commands.forbidden_flags"
            ),
        )

    def can_read(self, path: str | Path) -> bool:
        """Return whether a path is available without restricted-data approval."""
        value = str(path)
        return not _matches(value, self.deny_read + self.restricted_read)

    def can_write(self, path: str | Path) -> bool:
        """Return whether generated output may be written to a path."""
        if _matches(str(path), self.allowed_write):
            return True
        return self.write_default

    def can_use_network(self, skill: str) -> bool:
        """Return whether a skill is explicitly allowed external network access."""
        return skill in self.network_skills or self.network_default

    def redact(self, text: str) -> str:
        """Replace configured environment secret values in output text."""
        from src.certification.context import redact_secrets

        redacted = text
        for name in self.redact_env:
            value = os.getenv(name)
            if value:
                redacted = redacted.replace(value, f"[REDACTED:{name}]")
        return redact_secrets(redacted)

    def check_read(self, path: str | Path, skill_id: str) -> PermissionResult:
        """Return the typed read decision for one skill and path."""
        _ = skill_id
        if _matches(str(path), self.deny_read + self.restricted_read):
            return PermissionResult(
                decision=PermissionDecision.DENY,
                reason="path matched deny/restricted read policy",
                matched_rule=str(path),
            )
        return PermissionResult(decision=PermissionDecision.ALLOW, reason="path allowed")

    def check_write(self, path: str | Path, skill_id: str) -> PermissionResult:
        """Return the typed write decision for one skill and path."""
        _ = skill_id
        if _matches(str(path), self.allowed_write):
            return PermissionResult(decision=PermissionDecision.ALLOW, reason="path allowed")
        return PermissionResult(
            decision=PermissionDecision.DENY,
            reason="writes are limited to artifact and plan paths",
            matched_rule=str(path),
        )

    def check_network(self, skill_id: str) -> PermissionResult:
        """Return the typed network decision for one skill."""
        if skill_id in self.network_skills or self.network_default:
            return PermissionResult(decision=PermissionDecision.ALLOW, reason="skill allowed network")
        return PermissionResult(decision=PermissionDecision.DENY, reason="network default denied")

    def authorize_command(self, argv: list[str] | tuple[str, ...], skill_id: str) -> PermissionResult:
        """Authorize an argv command without shell expansion or string matching."""
        _ = skill_id
        tokens = list(argv)
        injection = self._injection_hit(tokens)
        if injection is not None:
            return injection
        if self._is_python_launcher(tokens[0]) if tokens else False:
            return self._authorize_python_module(tokens)
        if not tokens:
            return PermissionResult(decision=PermissionDecision.DENY, reason="empty command")
        return self._authorize_executable(tokens[0])

    @staticmethod
    def _is_python_launcher(program: str) -> bool:
        """Return whether a program token is a Python interpreter."""
        import sys
        from pathlib import Path as _Path

        return _Path(str(program)).name in {"python", "python3", _Path(sys.executable).name}

    def _injection_hit(self, tokens: list[str]) -> PermissionResult | None:
        """Return a DENY result when shell metacharacters or flags appear."""
        if not tokens:
            return PermissionResult(decision=PermissionDecision.DENY, reason="empty command")
        for token in tokens:
            if any(flag in token for flag in (";", "|", "&&", "$(", "`", "\n")):
                return PermissionResult(
                    decision=PermissionDecision.DENY,
                    reason="shell metacharacter in command token",
                    matched_rule=token,
                )
            if token in self.forbidden_flags:
                return PermissionResult(
                    decision=PermissionDecision.DENY,
                    reason="forbidden flag",
                    matched_rule=token,
                )
        return None

    def _authorize_python_module(self, tokens: list[str]) -> PermissionResult:
        """Authorize only allowlisted ``python -m <module>`` invocations."""
        min_python_module_argv = 3
        if len(tokens) < min_python_module_argv or tokens[1] != "-m":
            return PermissionResult(
                decision=PermissionDecision.DENY,
                reason="python must use an allowlisted -m module",
            )
        if tokens[2] not in self.python_modules:
            return PermissionResult(
                decision=PermissionDecision.DENY,
                reason="python module not allowlisted",
                matched_rule=str(tokens[2]),
            )
        return PermissionResult(decision=PermissionDecision.ALLOW, reason="allowlisted python module")

    def _authorize_executable(self, program: str) -> PermissionResult:
        """Authorize a non-Python executable against the allowlist."""
        from pathlib import Path as _Path

        executable = _Path(str(program)).name
        if executable not in self.allowed_executables:
            return PermissionResult(
                decision=PermissionDecision.DENY,
                reason="executable not allowlisted",
                matched_rule=executable,
            )
        return PermissionResult(decision=PermissionDecision.ALLOW, reason="allowlisted executable")

    def sanitize_environment(self, env: dict[str, str], skill_id: str) -> dict[str, str]:
        """Strip configured secret values from a child-process environment."""
        _ = skill_id
        return {key: value for key, value in env.items() if key not in self.redact_env}


__all__ = ["PermissionPolicy"]
