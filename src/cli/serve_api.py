"""CLI command for running the FastAPI REST & WebSocket server gateway."""

from __future__ import annotations

import argparse
import logging
import sys
from typing import TYPE_CHECKING

import uvicorn

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)


def main(argv: Sequence[str] | None = None) -> int:
    """Execute FastAPI server gateway CLI."""
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    parser = argparse.ArgumentParser(description="Start KBO Playwright FastAPI REST & WebSocket Server Gateway")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Network host interface (default: 127.0.0.1)")
    parser.add_argument("--port", "-p", type=int, default=8000, help="Port to listen on (default: 8000)")
    parser.add_argument("--reload", action="store_true", help="Enable auto-reloading on code changes")
    parser.add_argument("--workers", "-w", type=int, default=1, help="Number of worker processes (default: 1)")

    args = parser.parse_args(argv)

    print("=" * 75)  # noqa: T201
    print(f"🚀 [KBO FastAPI Server Starting]: http://{args.host}:{args.port}")  # noqa: T201
    print(f"• Swagger API Docs:  http://{args.host}:{args.port}/docs")  # noqa: T201
    print(f"• Redoc Reference:   http://{args.host}:{args.port}/redoc")  # noqa: T201
    print(f"• Live WebSocket:    ws://{args.host}:{args.port}/ws/live/{{game_id}}")  # noqa: T201
    print(f"• Auto-reload:       {'Enabled' if args.reload else 'Disabled'}")  # noqa: T201
    print("=" * 75)  # noqa: T201

    uvicorn.run(
        "src.api.app:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
        workers=args.workers if not args.reload else None,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
