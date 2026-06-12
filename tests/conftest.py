"""
Pytest configuration shared across test modules.
Ensures the repository root is importable so `import src` works consistently.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import logging


class _CurrentStdoutHandler(logging.StreamHandler):
    """StreamHandler that always writes to sys.stdout (even when capsys patches it)."""

    def __init__(self) -> None:
        super().__init__(None)

    @property
    def stream(self):
        return sys.stdout

    @stream.setter
    def stream(self, value) -> None:
        pass


logging.basicConfig(level=logging.DEBUG, format="%(message)s", force=True)
root = logging.getLogger()
if root.handlers:
    root.handlers = [_CurrentStdoutHandler()]
