"""Execution context, target metadata, and secret redaction for Certification Runner."""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

SECRET_PATTERNS = [
    re.compile(r"://([^:@]+):([^@]+)@"),  # DB URL credentials: ://user:pass@
    re.compile(r"bot\d+:[A-Za-z0-9_-]+"),  # Telegram bot tokens
    re.compile(r"(api[_-]?key|secret|password|token)\s*=\s*['\"]?[A-Za-z0-9_\-./+=]{6,}['\"]?", re.IGNORECASE),
]


def redact_secrets(text: str) -> str:
    """Mask credentials, passwords, and API tokens from strings and tracebacks."""
    if not text:
        return ""
    sanitized = text
    for pattern in SECRET_PATTERNS:
        sanitized = pattern.sub(r"://***:***@", sanitized)
    return sanitized


def get_git_revision() -> str:
    """Retrieve current Git commit SHA or fallback string."""
    git_bin = shutil.which("git")
    if not git_bin:
        return "UNKNOWN_GIT_SHA"
    try:
        res = subprocess.run(  # noqa: S603
            [git_bin, "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            check=False,
            timeout=2,
        )
        if res.returncode == 0 and res.stdout.strip():
            return res.stdout.strip()
    except (OSError, subprocess.SubprocessError):
        pass
    return "UNKNOWN_GIT_SHA"


@dataclass
class CertificationContext:
    """Central configuration and runtime environment passed to all certification gates."""

    run_id: str = field(default_factory=lambda: f"cert_{uuid.uuid4().hex[:12]}")
    target: str = "production"  # production, local
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    git_revision: str = field(default_factory=get_git_revision)
    artifact_dir: Path = field(default_factory=lambda: Path("data/certification"))
    fail_fast: bool = False
    strict: bool = False
    filter_gate: str | None = None
    expected_vector_dimension: int = 1536
    database_url_redacted: str = ""
    verification_db_url_redacted: str = ""

    def __post_init__(self) -> None:
        """Sanitize URLs and ensure artifact directory exists."""
        self.artifact_dir.mkdir(parents=True, exist_ok=True)
        raw_db = os.getenv("DATABASE_URL", "sqlite:///./data/kbo_dev.db")
        raw_oci = os.getenv("OCI_DB_URL", "")
        self.database_url_redacted = redact_secrets(raw_db)
        self.verification_db_url_redacted = redact_secrets(raw_oci) if raw_oci else "sqlite:///:memory:"

    def redact(self, value: str) -> str:
        """Redact sensitive information using context rules."""
        return redact_secrets(value)


__all__ = [
    "CertificationContext",
    "get_git_revision",
    "redact_secrets",
]
