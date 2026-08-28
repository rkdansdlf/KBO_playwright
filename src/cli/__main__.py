"""Root entrypoint when running `python3 -m src.cli`."""

from __future__ import annotations

import sys

from src.cli.kbo import main

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
