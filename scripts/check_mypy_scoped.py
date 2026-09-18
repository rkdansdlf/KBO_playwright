"""Scoped mypy gate for certified-clean modules.

Runs mypy over an explicit allowlist of modules that have been verified to
produce zero attributed errors, and fails when any error is attributed to a
listed file. Errors in transitive dependencies are ignored here; they belong
to future cleanup batches.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

# Modules certified to produce zero mypy errors attributed to themselves.
# Add new files only after verifying `mypy <file>` reports no errors for them.
SCOPED_FILES: tuple[str, ...] = (
    "scripts/maintenance/fix_relay_state.py",
    "src/__init__.py",
    "src/analytics/matchup.py",
    "src/api/app.py",
    "src/api/auth.py",
    "src/api/routers/games.py",
    "src/api/routers/pipeline.py",
    "src/api/schemas.py",
    "src/certification/models.py",
    "src/certification/registry.py",
    "src/cli/live/dashboard.py",
    "src/cli/rag/build_rag_index.py",
    "src/cli/rag/evaluate_rag_retrieval.py",
    "src/crawlers/base.py",
    "src/db/drift_detector.py",
    "src/db/engine.py",
    "src/db/types.py",
    "src/models/dto.py",
    "src/models/game.py",
    "src/models/inspector.py",
    "src/models/player.py",
    "src/models/season.py",
    "src/models/team.py",
    "src/scheduler/jobs/daily.py",
    "src/scheduler/jobs/live.py",
    "src/scheduler/jobs/maintenance.py",
    "src/services/relay_recovery_engine.py",
    "src/services/wpa_chart_service.py",
    "src/utils/url_validator.py",
)


def main() -> int:
    """Run mypy on scoped files and fail on any attributed error."""
    repo_root = Path(__file__).resolve().parents[1]
    missing = [f for f in SCOPED_FILES if not (repo_root / f).exists()]
    if missing:
        sys.stderr.write(f"scoped mypy gate: listed files missing: {missing}\n")
        return 2
    proc = subprocess.run(
        [sys.executable, "-m", "mypy", *SCOPED_FILES],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )
    output = (proc.stdout or "") + (proc.stderr or "")
    attributed = [line for line in output.splitlines() if line.startswith(SCOPED_FILES) and "error:" in line]
    if attributed:
        sys.stdout.write("\n".join(attributed) + "\n")
        sys.stdout.write(f"scoped mypy gate: {len(attributed)} error(s) in certified files\n")
        return 1
    sys.stdout.write(f"scoped mypy gate: {len(SCOPED_FILES)} files clean\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
