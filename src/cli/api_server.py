"""CLI wrapper to launch the FastAPI REST API server."""

from __future__ import annotations

import argparse
import sys
from typing import TYPE_CHECKING

import uvicorn

if TYPE_CHECKING:
    from collections.abc import Sequence


def main(argv: Sequence[str] | None = None) -> None:
    """Parse arguments and runs the Uvicorn web server."""
    parser = argparse.ArgumentParser(description="Start the KBO Playwright REST API server.")
    parser.add_argument(
        "--host",
        type=str,
        default="127.0.0.1",
        help="Host address to bind to (e.g. 0.0.0.0 for Docker).",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Port number to bind to.",
    )
    parser.add_argument(
        "--reload",
        action="store_true",
        help="Enable auto-reload for local development.",
    )

    args = parser.parse_args(argv if argv is not None else sys.argv[1:])

    uvicorn.run(
        "src.api.app:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
    )


if __name__ == "__main__":
    main()
