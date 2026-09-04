"""Static test enforcing single canonical URL generation path for KBO LiveText."""

from __future__ import annotations

from pathlib import Path

SRC_DIR = Path(__file__).resolve().parent.parent / "src"
ALLOWED_EXEMPTIONS = {"utils/kbo_relay_target.py"}


def test_no_manual_livetext_query_assembly_in_src() -> None:
    """Scan all Python source files in src/ for manual LiveText query assembly."""
    violations = []
    for py_path in SRC_DIR.rglob("*.py"):
        rel_path = py_path.relative_to(SRC_DIR).as_posix()
        if rel_path in ALLOWED_EXEMPTIONS:
            continue
        content = py_path.read_text(encoding="utf-8")
        for line_num, line in enumerate(content.splitlines(), start=1):
            stripped = line.strip()
            if stripped.startswith(("#", "* ", '"""')):
                continue
            if "LiveText.aspx?" in line:
                violations.append(f"{rel_path}:{line_num} -> {stripped}")
    msg = f"Manual LiveText query assembly found: {', '.join(violations)}"
    assert not violations, msg
