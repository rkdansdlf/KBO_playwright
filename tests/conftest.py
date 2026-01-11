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
