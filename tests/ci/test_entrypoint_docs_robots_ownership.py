"""docker/entrypoint.sh must hand /app/Docs/robots to the remapped appuser.

The image bakes ``/app/Docs/robots`` owned by the UID ``appuser`` had at build time (999).
The entrypoint then remaps ``appuser`` to the bind-mount owner (default 1000), which left the
directory unwritable and made the robots.txt compliance snapshot raise ``PermissionError``
on every run. The root branch cannot run outside a container, so this lifts the one block
out of the script and runs it against a temporary directory with ``stat``/``chown`` stubbed.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

ENTRYPOINT = Path(__file__).resolve().parents[2] / "docker" / "entrypoint.sh"
START_MARKER = '    if [ -d "/app/Docs/robots" ]; then'
END_MARKER = "    # gosu로 appuser 권한으로 재실행"


def _robots_block() -> str:
    text = ENTRYPOINT.read_text(encoding="utf-8")
    start = text.index(START_MARKER)
    end = text.index(END_MARKER)
    assert start < end, "the ownership fix must run before the privilege drop"
    return text[start:end]


def _run_block(tmp_path: Path, *, current_owner: int, target_uid: int = 1000) -> tuple[str, list[str]]:
    robots = tmp_path / "robots"
    robots.mkdir()
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    calls = tmp_path / "chown_calls"
    (bin_dir / "stat").write_text(f"#!/bin/sh\necho {current_owner}\n", encoding="utf-8")
    (bin_dir / "chown").write_text(f'#!/bin/sh\necho "$@" >> "{calls}"\n', encoding="utf-8")
    for tool in ("stat", "chown"):
        (bin_dir / tool).chmod(0o755)

    script = f"TARGET_UID={target_uid}\nTARGET_GID={target_uid}\n" + _robots_block().replace(
        "/app/Docs/robots",
        str(robots),
    )
    result = subprocess.run(
        ["/bin/bash", "-c", script],
        env={"PATH": f"{bin_dir}:{os.environ['PATH']}"},
        capture_output=True,
        text=True,
        check=True,
    )
    recorded = calls.read_text(encoding="utf-8").splitlines() if calls.exists() else []
    return result.stdout, recorded


def test_entrypoint_is_valid_bash() -> None:
    subprocess.run(["/bin/bash", "-n", str(ENTRYPOINT)], check=True)


def test_directory_owned_by_the_old_build_uid_is_handed_to_the_remapped_appuser(tmp_path: Path) -> None:
    output, calls = _run_block(tmp_path, current_owner=999)

    assert len(calls) == 1
    assert calls[0].startswith("-R 1000:1000 ")
    assert calls[0].endswith("robots")
    assert "Chowning" in output


@pytest.mark.parametrize("owner", [1000])
def test_directory_already_owned_by_the_target_uid_is_left_alone(tmp_path: Path, owner: int) -> None:
    output, calls = _run_block(tmp_path, current_owner=owner)

    assert calls == []
    assert "Skipping" in output
