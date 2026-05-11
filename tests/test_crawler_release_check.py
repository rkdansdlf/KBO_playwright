from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "verification" / "crawler_release_check.sh"


def test_release_check_skips_live_smoke_by_default():
    true_bin = shutil.which("true")
    assert true_bin

    env = {**os.environ, "CRAWLER_STABILITY_PYTHON": true_bin}
    env.pop("KBO_LIVE_SMOKE", None)
    env.pop("KBO_LIVE_SMOKE_DATE", None)

    result = subprocess.run(
        ["bash", str(SCRIPT)],
        cwd=ROOT,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )

    assert "Skipping live smoke" in result.stdout
    assert "Running opt-in live smoke for" not in result.stdout


def test_release_check_runs_live_smoke_when_explicitly_enabled():
    true_bin = shutil.which("true")
    assert true_bin

    env = {
        **os.environ,
        "CRAWLER_STABILITY_PYTHON": true_bin,
        "CRAWLER_LIVE_SMOKE_PYTHON": true_bin,
        "KBO_LIVE_SMOKE": "1",
        "KBO_LIVE_SMOKE_DATE": "20250101",
    }

    result = subprocess.run(
        ["bash", str(SCRIPT)],
        cwd=ROOT,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )

    assert "Running opt-in live smoke for 20250101" in result.stdout
    assert "Running opt-in crawler live smoke" in result.stdout
