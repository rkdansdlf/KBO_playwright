"""Contracts for the scheduler launchd restore script.

The script is the one-command recovery path used when the scheduler was
disabled by renaming its plist to ``*.disabled``. It must stay shell-parseable
and must never grow a path that touches the other projects' launchd jobs.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "restore_scheduler_launchd.sh"


def test_script_passes_bash_syntax_check() -> None:
    result = subprocess.run(["bash", "-n", str(SCRIPT)], capture_output=True, text=True, check=False)

    assert result.returncode == 0, result.stderr


def test_help_exits_zero_and_names_the_use_case() -> None:
    result = subprocess.run(["bash", str(SCRIPT), "--help"], capture_output=True, text=True, check=False)

    assert result.returncode == 0
    assert "Restore the KBO scheduler" in result.stdout


def test_script_targets_the_scheduler_label_and_reuses_the_installer() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert 'LABEL="com.kbo-playwright.scheduler"' in text
    assert ".disabled" in text
    assert "install_scheduler_launchd.sh" in text


def test_script_never_touches_the_other_projects_plists() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert "com.kbo.daily_ingest" not in text
    assert "com.kbo.monthly_embed_upgrade" not in text
