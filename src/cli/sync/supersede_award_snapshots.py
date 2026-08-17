"""CLI to supersede award snapshots."""

from __future__ import annotations

import argparse


def main(argv: list[str] | None = None) -> int:
    """Run supersede award snapshots CLI."""
    parser = argparse.ArgumentParser()
    parser.parse_args(argv)
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
