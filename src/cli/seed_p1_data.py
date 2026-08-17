"""Compatibility wrapper for src.cli.collection.seed_p1_data."""

from __future__ import annotations

import sys

from src.cli.collection import seed_p1_data as _target_module

# Re-export all symbols and alias module in sys.modules so imports and patches work seamlessly
globals().update({k: v for k, v in _target_module.__dict__.items() if not (k.startswith("__") and k.endswith("__"))})
sys.modules[__name__] = _target_module

if __name__ == "__main__":
    if hasattr(_target_module, "main"):
        sys.exit(_target_module.main())
