"""Tests for the transaction ownership audit script."""

from __future__ import annotations

from pathlib import Path

import pytest

from scripts.audit_transaction_ownership import audit


def _write(root: Path, relative: str, content: str) -> Path:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return path


CLEAN_REPO = """from sqlalchemy.orm import Session


class PlayerRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def upsert(self, data: dict) -> None:
        self.session.add(data)
        self.session.flush()
"""

VIOLATING_REPO = """from sqlalchemy.orm import Session


class BadRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def save(self) -> None:
        self.session.commit()
"""

CALLER_SCRIPT = """from sqlalchemy.orm import Session


def main() -> None:
    session = Session()
    try:
        session.commit()
    finally:
        session.close()
"""

FACADE_REPO = """from sqlalchemy.orm import Session

from src.db.engine import get_db_session


def save(data: dict, session: Session | None = None) -> bool:
    if session is None:
        with get_db_session() as s:
            return save(data, session=s)
    return True
"""


def test_clean_repository_tree_passes(tmp_path: Path) -> None:
    """A repository tree without lifecycle markers satisfies the contract."""
    _write(tmp_path, "src/repositories/player_repository.py", CLEAN_REPO)
    _write(tmp_path, "src/repositories/team_repository.py", CLEAN_REPO)

    exit_code, lines = audit([tmp_path], verbose=False)

    assert exit_code == 0
    assert "contract satisfied" in "\n".join(lines)


def test_repository_commit_is_strict_violation(tmp_path: Path) -> None:
    """A non-allowlisted repository with commit() fails the audit."""
    _write(tmp_path, "src/repositories/bad_repository.py", VIOLATING_REPO)

    exit_code, lines = audit([tmp_path], verbose=False)

    assert exit_code == 1
    output = "\n".join(lines)
    assert "STRICT_VIOLATION" in output
    assert "bad_repository.py" in output


def test_allowlisted_facade_with_commit_passes(tmp_path: Path) -> None:
    """An allowlisted compatibility facade may keep guarded lifecycle control."""
    _write(tmp_path, "src/repositories/game_save.py", VIOLATING_REPO)

    exit_code, lines = audit([tmp_path], verbose=False)

    assert exit_code == 0
    assert "COMPAT_FACADE" in "\n".join(lines)


def test_caller_layer_commit_is_allowed(tmp_path: Path) -> None:
    """Scripts and services own transaction boundaries and may commit."""
    _write(tmp_path, "scripts/backfill_players.py", CALLER_SCRIPT)

    exit_code, lines = audit([tmp_path], verbose=True)

    assert exit_code == 0
    output = "\n".join(lines)
    assert "CALLER" in output
    assert "STRICT_VIOLATION" not in output


def test_session_optional_facade_is_compat(tmp_path: Path) -> None:
    """A repository with a get_db_session fallback is a compatibility facade."""
    _write(tmp_path, "src/repositories/game_save.py", FACADE_REPO)

    exit_code, lines = audit([tmp_path], verbose=False)

    assert exit_code == 0
    assert "COMPAT_FACADE" in "\n".join(lines)


def test_multiple_roots_are_scanned(tmp_path: Path) -> None:
    """All provided roots are scanned for violations."""
    clean = tmp_path / "clean"
    dirty = tmp_path / "dirty"
    _write(clean, "src/repositories/ok.py", CLEAN_REPO)
    _write(dirty, "src/repositories/bad.py", VIOLATING_REPO)

    exit_code, lines = audit([clean, dirty], verbose=False)

    assert exit_code == 1
    assert "bad.py" in "\n".join(lines)


@pytest.mark.parametrize(
    "content",
    [
        "def broken(:\n",
        "class Broken: pass\n  ",
    ],
)
def test_parse_error_is_tolerated(tmp_path: Path, content: str) -> None:
    """Unparsable files are reported without crashing the audit."""
    _write(tmp_path, "src/repositories/broken.py", content)

    exit_code, _ = audit([tmp_path], verbose=False)

    assert exit_code in (0, 1)
