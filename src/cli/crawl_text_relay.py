"""Compatibility wrapper for src.cli.collection.crawl_text_relay."""

from __future__ import annotations

import sys

from src.cli.collection import crawl_text_relay as _target_module

# Re-export all symbols and alias module in sys.modules so imports and patches work seamlessly
globals().update({k: v for k, v in _target_module.__dict__.items() if not (k.startswith("__") and k.endswith("__"))})
sys.modules[__name__] = _target_module

if __name__ == "__main__":
    if hasattr(_target_module, "main"):
        sys.exit(_target_module.main(sys.argv[1:]))
