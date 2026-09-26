"""Guarded project environment loading.

A module that calls `load_dotenv()` at import time silently mutates
`os.environ`. Two things go wrong:

1. **Test outcomes stop being a function of the code under test.** Whether a
   given test sees a configured Telegram chat id depends on whether some
   unrelated module happened to be imported first, so failures vary between
   runs of the same commit.
2. **Real credentials enter the test process.** A developer's `.env` holds live
   provider keys, and importing `src.db.engine` alone is enough to load them
   into every pytest worker.

The guard therefore lives here, in the function that performs the load, rather
than in the test that would otherwise have to undo it. Set
`KBO_ENV_FILE_LOADING=0` to make loading a no-op; the default is enabled so
production behaviour is unchanged.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

from dotenv import load_dotenv

logger = logging.getLogger(__name__)

ENV_FILE_LOADING_FLAG = "KBO_ENV_FILE_LOADING"
"""Set to `0`/`false`/`no`/`off` to make `load_project_env()` a no-op."""

PROJECT_ROOT = Path(__file__).resolve().parents[2]
"""Repository root, used to resolve `.env` independently of the current cwd."""

FALSY_VALUES = frozenset({"0", "false", "no", "off"})


def env_file_loading_enabled() -> bool:
    """Return whether automatic `.env` loading is permitted right now.

    Read at call time rather than at import time, so a test can toggle it.
    """
    return os.getenv(ENV_FILE_LOADING_FLAG, "1").strip().lower() not in FALSY_VALUES


def load_project_env(dotenv_path: str | Path | None = None, *, override: bool = False) -> bool:
    """Load the project `.env` into `os.environ` unless loading is disabled.

    Args:
        dotenv_path: Explicit `.env` path. Defaults to `<repo root>/.env`.
        override: Whether `.env` values overwrite already-set variables.
            Keep this `False` so an explicit environment always wins.

    Returns:
        True when a `.env` file was found and applied, False otherwise
        (including when loading is disabled).

    """
    if not env_file_loading_enabled():
        logger.debug("%s is disabled; skipping .env load", ENV_FILE_LOADING_FLAG)
        return False

    target = Path(dotenv_path) if dotenv_path is not None else PROJECT_ROOT / ".env"
    loaded = bool(load_dotenv(target, override=override))
    if not loaded:
        logger.debug("No .env file at %s; continuing with the process environment", target)
    return loaded


__all__ = [
    "ENV_FILE_LOADING_FLAG",
    "FALSY_VALUES",
    "PROJECT_ROOT",
    "env_file_loading_enabled",
    "load_project_env",
]
