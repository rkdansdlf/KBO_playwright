"""
Auto-fix silent bare except Exception blocks in crawler files.
Adds logger.exception() before pass/continue/return to prevent silent error swallowing.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

logger = logging.getLogger(__name__)

SRC = Path("src")
FIX_PATTERNS = [
    (
        re.compile(r"(except Exception:)\s*\n\s*(pass|continue)"),
        r'\1\n                    logger.exception("\2 after \1")\n                    \2',
    ),
    (re.compile(r"(except Exception:)\s*pass"), r'\1 logger.exception("silent except")'),
    (
        re.compile(r'(except Exception:)\s*return ""'),
        r'\1 logger.exception("silent except returning empty"); return ""',
    ),
    (re.compile(r"(except Exception:)\s*return None"), r'\1 logger.exception("silent except returning None")'),
    (re.compile(r"(except Exception:)\s*return 0"), r'\1 logger.exception("silent except returning 0")'),
    (re.compile(r"(except Exception:)\s*return 0\.0"), r'\1 logger.exception("silent except returning 0.0")'),
    (re.compile(r"(except Exception:)\s*return"), r'\1 logger.exception("silent except returning")'),
]


def needs_logger(text: str) -> bool:
    return "import logging" not in text and "from logging" not in text


def ensure_logger(text: str) -> str:
    """Add logger import if not present."""
    if needs_logger(text):
        # Find last import line
        lines = text.split("\n")
        last_import = -1
        for i, line in enumerate(lines):
            if line.startswith("import ") or line.startswith("from "):
                last_import = i
        if last_import >= 0:
            lines.insert(last_import + 1, "\nimport logging")
            lines.insert(last_import + 2, "logger = logging.getLogger(__name__)")
        return "\n".join(lines)
    return text


def fix_file(path: Path) -> int:
    text = path.read_text()
    original = text
    for pattern, replacement in FIX_PATTERNS:
        text = pattern.sub(replacement, text)
    if needs_logger(text):
        # Check if there are any fixed patterns that need logger
        if "logger.exception" in text and "logger" not in text.split("\n")[0]:
            pass  # will be caught below
    if "logger.exception" in text and needs_logger(text):
        text = ensure_logger(text)
    if text != original:
        path.write_text(text)
        return 1
    return 0


def main():
    files = list(SRC.rglob("*.py"))
    fixed = 0
    for f in files:
        try:
            fixed += fix_file(f)
        except (OSError, ValueError):
            logger.exception(f"Error fixing {f}")
    logger.info(f"Fixed {fixed} files.")


if __name__ == "__main__":
    main()
