"""Root CLI compatibility wrapper for run_unified_quality_check."""

from __future__ import annotations

import sys

from src.cli.reports.run_unified_quality_check import main

if __name__ == "__main__":
    sys.exit(main())
