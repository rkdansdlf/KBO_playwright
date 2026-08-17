"""Compatibility wrapper for :mod:`src.cli.rag.build_rag_index`."""

from __future__ import annotations

import sys

from src.cli.rag import build_rag_index as _target_module

globals().update({key: value for key, value in _target_module.__dict__.items() if not key.startswith("__")})
sys.modules[__name__] = _target_module

if __name__ == "__main__":
    sys.exit(_target_module.main())
