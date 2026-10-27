"""The robots policy directory name must match the one git tracks.

Both the source inventory and the crawler compliance check read per-host
policies from a relative path, and both spelled it ``docs/robots`` while git
tracked ``Docs/robots``. macOS resolves the names to the same directory, so
every local run passed; Linux does not, so inside the container the directory
was absent and every host was reported unreachable.

An unreachable host is not a blocked one, so no incident opened and the policy
read as enforced while enforcing nothing. Nothing in the suite caught it because
every test injects ``policy_dir`` as a parameter, which is exactly the thing the
bug was not in.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]


def _git_tracked_robots_dir() -> str:
    """Return the ``<Docs>/robots`` spelling git stores, or skip.

    git records the name as committed, which is the only spelling that exists on
    a case-sensitive filesystem. Reading the working tree cannot answer this --
    on macOS both spellings resolve.
    """
    try:
        out = subprocess.run(
            ["git", "ls-files", "-z", "--", "*/robots/*"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=True,
        ).stdout
    except OSError, subprocess.CalledProcessError:  # pragma: no cover - no git/no repo
        pytest.skip("git is unavailable")

    for entry in out.split("\0"):
        match = re.match(r"^(.*?/robots)/", entry)
        if match:
            return match.group(1)
    pytest.skip("no robots files are tracked")  # pragma: no cover - fixtures present


def test_the_constant_matches_the_tracked_directory() -> None:
    from src.reporting.source_inventory import ROBOTS_DIR

    tracked = _git_tracked_robots_dir()
    # Compare in git's spelling. ``str(Path)`` uses the host separator, so on
    # Windows ``Path("Docs/robots")`` stringifies as ``Docs\\robots`` and this
    # assertion failed over a separator rather than over the case it exists to
    # check. ``as_posix()`` normalises only a *real* separator: on POSIX a
    # backslash is an ordinary character, so a ``Path("Docs\\robots")`` literal
    # still fails there -- the platform the container runs on.
    assert ROBOTS_DIR.as_posix() == tracked, (
        f"ROBOTS_DIR is {ROBOTS_DIR.as_posix()!r} but git tracks {tracked!r}; "
        "the name is case-sensitive on Linux and the container has no such directory"
    )


def test_the_host_policy_directory_matches_the_tracked_directory() -> None:
    from src.utils.host_policy import POLICY_DIR

    tracked = _git_tracked_robots_dir()
    assert POLICY_DIR.as_posix() == tracked, f"POLICY_DIR is {POLICY_DIR.as_posix()!r} but git tracks {tracked!r}"


def test_the_compliance_snapshot_directory_matches_the_tracked_directory() -> None:
    """The snapshot writer is the third reader, and it was wrong the same way.

    It is checked through the source text rather than through an import because
    the path is built inside a function with no constant to read. A literal that
    only macOS can resolve is the whole defect.
    """
    tracked = _git_tracked_robots_dir()
    body = (REPO_ROOT / "src" / "utils" / "compliance.py").read_text(encoding="utf-8")

    literals = re.findall(r'Path\(\s*"([^"]*robots[^"]*)"\s*\)', body)
    assert literals, "no robots path literal found in compliance.py; this test has gone stale"
    for literal in literals:
        assert literal == tracked, (
            f"compliance.py builds {literal!r} but git tracks {tracked!r}; on Linux that directory does not exist"
        )


class TestTheContainerActuallyCarriesTheDirectory:
    """The path being right is not enough -- the image has to contain it.

    A correct constant pointing into a directory the Dockerfile never copies
    fails exactly the way the misspelling did, and reports the same thing: every
    host unreachable.
    """

    def test_the_runtime_dockerfile_copies_the_robots_directory(self) -> None:
        tracked = _git_tracked_robots_dir()
        body = (REPO_ROOT / "Dockerfile").read_text(encoding="utf-8")

        copies = re.findall(r"^COPY\s+(?:--\S+\s+)*(\S+)\s+(\S+)\s*$", body, re.MULTILINE)
        assert copies, "no COPY instructions found in Dockerfile; this test has gone stale"

        sources = [src.rstrip("/") for src, _ in copies]
        assert tracked in sources, (
            f"Dockerfile does not COPY {tracked!r}; the image has no robots policies "
            f"and every host will be reported unreachable. COPY sources: {sources}"
        )

    def test_the_dockerignore_does_not_exclude_it(self) -> None:
        """COPY cannot undo an ignore rule, and the ignore rule is a prefix match.

        ``Docs/*`` excludes the whole tree up front, so an entry that re-includes
        the robots directory has to come after it. A rule placed before would be
        overridden by the exclusion and the COPY would fail to find the source.
        """
        tracked = _git_tracked_robots_dir()
        lines = [
            line.strip()
            for line in (REPO_ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        ]

        exclude_all = f"{tracked.split('/')[0]}/*"
        assert exclude_all in lines, f"{exclude_all!r} is not excluded; this test has gone stale"
        assert f"!{tracked}/" in lines, f".dockerignore does not re-include {tracked!r}"

        assert lines.index(f"!{tracked}/") > lines.index(exclude_all), (
            f"!{tracked}/ comes before {exclude_all}, so the exclusion wins and COPY finds nothing"
        )
