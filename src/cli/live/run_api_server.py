"""CLI entrypoint to run the KBO Playwright FastAPI REST API server."""

from __future__ import annotations

import argparse
import sys

import uvicorn


def parse_args(args: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments for API server."""
    parser = argparse.ArgumentParser(description="Run the KBO Playwright REST API server.")
    parser.add_argument(
        "--host",
        type=str,
        default="127.0.0.1",
        help="Host IP address to bind (default: 127.0.0.1)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Port to bind (default: 8000)",
    )
    parser.add_argument(
        "--reload",
        action="store_true",
        help="Enable auto-reload on code changes",
    )
    return parser.parse_args(args)


def main(args: list[str] | None = None) -> None:
    """Run the KBO Playwright FastAPI REST API server."""
    parsed_args = parse_args(args)
    print(f"Starting KBO Data API server on http://{parsed_args.host}:{parsed_args.port}")  # noqa: T201
    print(f"Swagger API Docs: http://{parsed_args.host}:{parsed_args.port}/docs")  # noqa: T201

    uvicorn.run(
        "src.api.app:app",
        host=parsed_args.host,
        port=parsed_args.port,
        reload=parsed_args.reload,
    )


if __name__ == "__main__":
    main(sys.argv[1:])
