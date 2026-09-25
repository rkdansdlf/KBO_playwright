"""Dead-code regression tests for the unreachable-statement lint gate.

Ruff's RET/B012 rules do not flag statements that follow a function's final return or
raise, so the repository relies on `scripts/lint_unreachable_code.py` to catch a second
copy of logic that no test exercises.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from scripts.lint_unreachable_code import main as lint_main
from scripts.lint_unreachable_code import scan_file, unreachable_statements

ROOT = Path(__file__).resolve().parents[2]
HARNESS_DIR = ROOT / "tools" / "agent_harness"
PRODUCTION_DIRS = ("src", "tools")


def _write(tmp_path: Path, body: str) -> Path:
    path = tmp_path / "sample.py"
    path.write_text(body, encoding="utf-8")
    return path


def test_statements_after_a_return_are_reported(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "def route(task):\n    if task:\n        return 'a'\n    return 'b'\n\n    route_spec = object()\n",
    )

    assert unreachable_statements(path) == [(6, "route")]


def test_dead_block_that_ends_in_another_return_is_reported(tmp_path: Path) -> None:
    """Regression for the P18 router shape: a duplicated return after a return.

    The dead block ended in its own `return`, so keying off the *last* terminator missed it.
    """
    path = _write(
        tmp_path,
        "def score(task):\n"
        "    return 'feature', 'default profile'\n"
        "\n"
        "    route = {'feature': 1}\n"
        "    return ('feature', route)\n",
    )

    assert unreachable_statements(path) == [(4, "score"), (5, "score")]


def test_statements_after_a_raise_are_reported(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "def check(value):\n"
        "    if not value:\n"
        "        raise ValueError('empty')\n"
        "    return value\n"
        "    print('never')\n",
    )

    assert unreachable_statements(path) == [(5, "check")]


def test_yield_does_not_terminate_the_body(tmp_path: Path) -> None:
    """Generator execution resumes after `yield`, so trailing code is reachable."""
    path = _write(
        tmp_path,
        "def stream(rows):\n    yield from rows\n    print('resumes per item')\n",
    )

    assert unreachable_statements(path) == []


def test_branch_terminators_without_a_final_terminator_are_not_reported(tmp_path: Path) -> None:
    """A body that only returns inside if/else keeps trailing code reachable-looking."""
    path = _write(
        tmp_path,
        "def pick(flag):\n"
        "    if flag:\n"
        "        return 'a'\n"
        "    else:\n"
        "        return 'b'\n"
        "    print('only reached if the branch above is refactored')\n",
    )

    assert unreachable_statements(path) == []


def test_code_after_a_try_block_is_not_reported(tmp_path: Path) -> None:
    """A raise inside an except handler does not terminate the function body."""
    path = _write(
        tmp_path,
        "def load(path):\n"
        "    try:\n"
        "        return path.read_text()\n"
        "    except OSError:\n"
        "        raise\n"
        "    return ''\n",
    )

    assert unreachable_statements(path) == []


def test_unparsable_files_are_skipped(tmp_path: Path) -> None:
    path = _write(tmp_path, "def broken(:\n")

    assert unreachable_statements(path) == []


def test_investigations_directory_is_excluded(tmp_path: Path) -> None:
    path = tmp_path / "investigations" / "probe.py"
    path.parent.mkdir()
    path.write_text("def probe():\n    return 1\n    return 2\n", encoding="utf-8")

    assert scan_file(path) == []
    assert scan_file(Path("scripts/investigations/anything.py")) == []


@pytest.mark.parametrize("directory", PRODUCTION_DIRS)
def test_production_trees_have_no_unreachable_statements(directory: str) -> None:
    offenders: list[str] = []
    for path in sorted((ROOT / directory).rglob("*.py")):
        for line, function in scan_file(path):
            offenders.append(f"{path.relative_to(ROOT)}:{line} in {function}()")

    assert offenders == []


def test_cli_reports_a_clean_tree(capsys: pytest.CaptureFixture[str]) -> None:
    assert lint_main() == 0
    assert "Total: 0 unreachable statements" in capsys.readouterr().out
