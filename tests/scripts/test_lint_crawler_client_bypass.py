"""Regression tests for the crawler HTTP client bypass gate.

The gate is the thing that turned issue #26 into a build failure, so what it
refuses matters more than the single violation it caught. Two properties are
pinned here: an unmarked construction fails, and a marked one exempts only
itself. A gate that exempts a whole file is worse than no gate, because it
reports clean while the thing it watches grows.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

from scripts.lint_crawler_client_bypass import (
    ALLOWED_FILES,
    BYPASS_MARKER,
    main,
    scan,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
GATE = REPO_ROOT / "scripts" / "lint_crawler_client_bypass.py"

_DIRECT = """\
import httpx


def fetch():
    return httpx.AsyncClient(timeout=1.0)
"""

_MARKED_LEADING = """\
import httpx


def fetch():
    # crawler-client-bypass: streams a response body the shared client cannot hold
    return httpx.AsyncClient(timeout=1.0)
"""

_MARKED_TRAILING = """\
import httpx


def fetch():
    return httpx.AsyncClient(
        timeout=1.0,
    )  # crawler-client-bypass: streams a response body the shared client cannot hold
"""

_MARKED_WITHOUT_REASON = """\
import httpx


def fetch():
    # crawler-client-bypass
    return httpx.AsyncClient(timeout=1.0)
"""


def _write(tmp_path: Path, source: str, name: str = "probe.py") -> Path:
    path = tmp_path / name
    path.write_text(source, encoding="utf-8")
    return path


class TestTheMarkerExemptsOnlyItsOwnCall:
    def test_an_unmarked_construction_is_reported(self, tmp_path: Path) -> None:
        path = _write(tmp_path, _DIRECT)

        unmarked, _ = scan(path)

        assert unmarked

    def test_a_leading_marker_exempts_the_call(self, tmp_path: Path) -> None:
        path = _write(tmp_path, _MARKED_LEADING)

        unmarked, marked = scan(path)

        assert unmarked == []
        assert marked

    def test_a_trailing_marker_exempts_the_call(self, tmp_path: Path) -> None:
        path = _write(tmp_path, _MARKED_TRAILING)

        unmarked, _ = scan(path)

        assert unmarked == []

    def test_a_marker_without_a_reason_does_not_exempt(self, tmp_path: Path) -> None:
        """A bypass nobody can review is indistinguishable from an oversight.

        This is the exact string the old gate advertised in its own error
        message, and accepting it would hand back the quiet exemption under a
        new name.
        """
        path = _write(tmp_path, _MARKED_WITHOUT_REASON)

        unmarked, _ = scan(path)

        assert unmarked

    def test_a_marker_in_one_function_does_not_exempt_another(self, tmp_path: Path) -> None:
        """The file-wide exemption this replaces: a marked call hides nothing else.

        The previous gate returned an empty list as soon as the marker appeared
        anywhere in the source, so one registered bypass silently licensed every
        later one in that file.
        """
        path = _write(
            tmp_path,
            """\
import httpx


def streaming():
    # crawler-client-bypass: the first call streams and needs a raw client
    return httpx.AsyncClient(timeout=1.0)


def ordinary():
    return httpx.AsyncClient(timeout=1.0)
""",
        )

        unmarked, marked = scan(path)

        assert len(marked) == 1
        assert len(unmarked) == 1
        assert unmarked[0] > marked[0]

    def test_a_marker_in_a_docstring_does_not_exempt(self, tmp_path: Path) -> None:
        """Only a comment exempts a call; prose that merely mentions it does not."""
        path = _write(
            tmp_path,
            f"""\
import httpx

# Historically this needed a {BYPASS_MARKER}: the endpoint needed a raw client.
# That is no longer true.


def ordinary():
    return httpx.AsyncClient(timeout=1.0)
""",
        )

        unmarked, marked = scan(path)

        assert len(unmarked) == 1
        assert marked == []


class TestTheGateReportsAndExits:
    def test_a_violation_exits_non_zero(self, tmp_path: Path) -> None:
        path = _write(tmp_path, _DIRECT)

        code = main([str(path)])

        assert code == 1

    def test_a_marked_call_exits_zero(self, tmp_path: Path, capsys) -> None:
        path = _write(tmp_path, _MARKED_LEADING)

        code = main([str(path)])

        assert code == 0
        assert "No new crawler HTTP client bypasses." in capsys.readouterr().out

    def test_the_error_names_the_marker_format_it_accepts(self, tmp_path: Path, capsys) -> None:
        """The message has to advertise the reason requirement, or nobody complies."""
        path = _write(tmp_path, _DIRECT)

        main([str(path)])

        assert f"# {BYPASS_MARKER}: <reason>" in capsys.readouterr().out


class TestTheRepositoryItselfIsClean:
    """The gate is only trustworthy if it runs against the tree as it stands."""

    def test_running_the_gate_against_the_repository_passes(self) -> None:
        """No crawler builds its own client, so the check stays available.

        A gate nobody runs catches nothing. This is the check that fails first
        when a future crawler reintroduces a raw client.
        """
        result = subprocess.run(
            [sys.executable, str(GATE)],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )

        assert result.returncode == 0, result.stdout + result.stderr

    def test_the_allowlist_holds_no_dead_entries(self) -> None:
        """Every sanctioned file still contains what it was sanctioned for.

        The allowlist is a list of files allowed to break a rule, so an entry
        whose file stopped breaking it is a licence with nobody using it. The
        migration that removed these entries is done: the list is back to the
        two places that genuinely construct the client.
        """
        for relative in sorted(ALLOWED_FILES):
            path = REPO_ROOT / relative
            assert path.is_file(), f"{relative} is allowlisted but missing"
            unmarked, _ = scan(path)
            assert unmarked, f"{relative} is allowlisted but no longer constructs a client"


class TestTheAllowlistIsMinimal:
    def test_only_the_client_and_the_legacy_helper_are_sanctioned(self) -> None:
        """Pin the allowlist so widening it has to be a deliberate edit.

        `CrawlerHttpClient` is the client itself. `base.py` holds
        `BaseHttpCrawler.http_client()`, which merges headers and validates the
        URL but neither throttles nor retries; no crawler under `src/` or
        `scripts/` calls it any more, and it retires with the test that still
        exercises it.
        """
        assert frozenset({"src/crawlers/base.py", "src/crawlers/http_client.py"}) == ALLOWED_FILES


def test_the_gate_runs_without_arguments_as_ci_invokes_it() -> None:
    """CI runs the script with no file arguments, so the default scan is the contract."""
    result = subprocess.run(
        [sys.executable, str(GATE)],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
        env={"PATH": "/usr/bin:/bin:/usr/local/bin", "HOME": str(Path.home())},
    )

    assert result.returncode == 0, result.stdout + result.stderr


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
