"""Enforce Harness path, network, and secret-redaction policy."""

from __future__ import annotations

import fnmatch
import os
import sys
from dataclasses import dataclass
from pathlib import Path

from tools.agent_harness.dto import PermissionDecision, PermissionResult
from tools.agent_harness.registry import _mapping, _strings, load_yaml_mapping, project_root

SHELL_METACHARACTERS = (";", "&", "|", "<", ">", "\n", "\r", "\x00", "$(", "${", "`")


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
    denied_env: tuple[str, ...] = ()
    denied_env_prefixes: tuple[str, ...] = ()
    root: Path | None = None

    @classmethod
    def load(cls, root: Path | None = None) -> PermissionPolicy:
        """Load the repository Harness permission policy."""
        repo_root = (root or project_root()).resolve()
        payload = load_yaml_mapping(repo_root / ".agent-harness" / "policies" / "permissions.yaml")
        read_policy = _mapping(payload.get("read"), "permissions.read")
        write_policy = _mapping(payload.get("write"), "permissions.write")
        network_policy = _mapping(payload.get("network"), "permissions.network")
        commands_policy = _mapping(payload.get("commands", {}), "permissions.commands")
        environment_policy = _mapping(payload.get("environment", {}), "permissions.environment")
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
            denied_env=_strings(environment_policy.get("denied", []), "permissions.environment.denied"),
            denied_env_prefixes=_strings(
                environment_policy.get("denied_prefixes", []), "permissions.environment.denied_prefixes"
            ),
            root=repo_root,
        )

    @staticmethod
    def _file_link_error(path: Path) -> str | None:
        """Return a hard-link or metadata error for an existing regular file."""
        if not path.is_file():
            return None
        try:
            return "path contains a hard-linked file" if path.stat().st_nlink != 1 else None
        except OSError:
            return "path metadata cannot be inspected"

    def _safe_relative_path(self, path: str | Path) -> tuple[str | None, str | None]:
        root = (self.root or project_root()).resolve()
        value = str(path)
        raw = Path(value)
        if "\x00" in value or ".." in raw.parts:
            reason = "path contains a null byte" if "\x00" in value else "path traversal is not allowed"
            return None, reason
        candidate = raw if raw.is_absolute() else root / raw
        try:
            relative = candidate.relative_to(root)
        except ValueError:
            return None, "path is outside the repository root"
        current = root
        for part in relative.parts:
            current /= part
            if current.is_symlink():
                return None, "path contains a symbolic link"
        link_error = self._file_link_error(candidate)
        if link_error is not None:
            return None, link_error
        try:
            resolved = candidate.resolve()
            resolved_relative = resolved.relative_to(root).as_posix()
        except (OSError, ValueError):
            return None, "path cannot be resolved within the repository root"
        return resolved_relative, None

    def can_read(self, path: str | Path) -> bool:
        """Return whether a path is available without restricted-data approval."""
        relative, reason = self._safe_relative_path(path)
        return reason is None and not _matches(relative or "", self.deny_read + self.restricted_read)

    def can_write(self, path: str | Path) -> bool:
        """Return whether generated output may be written to a path."""
        relative, reason = self._safe_relative_path(path)
        if reason is not None:
            return False
        return _matches(relative or "", self.allowed_write) or self.write_default

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
        relative, path_error = self._safe_relative_path(path)
        if path_error is not None:
            return PermissionResult(decision=PermissionDecision.DENY, reason=path_error, matched_rule=str(path))
        if _matches(relative or "", self.deny_read + self.restricted_read):
            return PermissionResult(
                decision=PermissionDecision.DENY,
                reason="path matched deny/restricted read policy",
                matched_rule=relative,
            )
        return PermissionResult(decision=PermissionDecision.ALLOW, reason="path allowed")

    def check_write(self, path: str | Path, skill_id: str) -> PermissionResult:
        """Return the typed write decision for one skill and path."""
        _ = skill_id
        relative, path_error = self._safe_relative_path(path)
        if path_error is not None:
            return PermissionResult(decision=PermissionDecision.DENY, reason=path_error, matched_rule=str(path))
        if _matches(relative or "", self.allowed_write):
            return PermissionResult(decision=PermissionDecision.ALLOW, reason="path allowed")
        return PermissionResult(
            decision=PermissionDecision.DENY,
            reason="writes are limited to artifact and plan paths",
            matched_rule=relative,
        )

    def check_cwd(self, path: str | Path) -> PermissionResult:
        """Return whether a command working directory is safely contained."""
        relative, path_error = self._safe_relative_path(path)
        if path_error is not None:
            return PermissionResult(decision=PermissionDecision.DENY, reason=path_error, matched_rule=str(path))
        root = (self.root or project_root()).resolve()
        if not (root / (relative or ".")).is_dir():
            return PermissionResult(
                decision=PermissionDecision.DENY,
                reason="command working directory does not exist",
                matched_rule=relative,
            )
        return PermissionResult(
            decision=PermissionDecision.ALLOW,
            reason="working directory allowed",
            matched_rule=relative,
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
        if not tokens:
            return PermissionResult(decision=PermissionDecision.DENY, reason="empty command")
        injection = self._injection_hit(tokens)
        if injection is not None:
            return injection
        if self.is_python_launcher(tokens[0]):
            return self._authorize_python_module(tokens)
        return self._authorize_executable(tokens[0])

    @staticmethod
    def is_python_launcher(program: str) -> bool:
        """Return whether a program token is the trusted Python interpreter."""
        return str(program) in {"python", "python3", sys.executable, Path(sys.executable).name}

    def _injection_hit(self, tokens: list[str]) -> PermissionResult | None:
        """Return a DENY result when shell metacharacters or flags appear."""
        for token in tokens:
            if any(metacharacter in token for metacharacter in SHELL_METACHARACTERS):
                return PermissionResult(
                    decision=PermissionDecision.DENY,
                    reason="shell metacharacter in command token",
                    matched_rule=token,
                )
            if any(token == flag or token.startswith(f"{flag}=") for flag in self.forbidden_flags):
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
        executable = str(program)
        if "/" in executable or "\\" in executable:
            return PermissionResult(
                decision=PermissionDecision.DENY,
                reason="executable paths must be resolved by CommandRunner",
                matched_rule=executable,
            )
        if executable not in self.allowed_executables:
            return PermissionResult(
                decision=PermissionDecision.DENY,
                reason="executable not allowlisted",
                matched_rule=executable,
            )
        return PermissionResult(decision=PermissionDecision.ALLOW, reason="allowlisted executable")

    def sanitize_environment(self, env: dict[str, str], skill_id: str) -> dict[str, str]:
        """Strip configured secrets and execution-control variables from a child environment."""
        _ = skill_id
        denied = {name.casefold() for name in (*self.redact_env, *self.denied_env)}
        prefixes = tuple(prefix.casefold() for prefix in self.denied_env_prefixes)
        return {
            key: value
            for key, value in env.items()
            if key.casefold() not in denied and not any(key.casefold().startswith(prefix) for prefix in prefixes)
        }


__all__ = ["PermissionPolicy"]
