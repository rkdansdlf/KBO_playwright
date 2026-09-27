"""Run a coroutine from synchronous code, even inside a running event loop."""

from __future__ import annotations

import asyncio
import threading
from typing import Any


def run_coro_blocking(coro: Any) -> Any:  # noqa: ANN401
    """Execute a coroutine to completion from sync code.

    When a loop is already running in this thread (e.g. under pytest-asyncio),
    the coroutine is executed in a short-lived worker thread instead of calling
    ``asyncio.run`` (which would raise).
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    box: dict[str, Any] = {}

    def _target() -> None:
        try:
            box["result"] = asyncio.run(coro)
        except BaseException as exc:  # noqa: BLE001
            box["error"] = exc

    thread = threading.Thread(target=_target, name="crawl-async-bridge")
    thread.start()
    thread.join()
    if "error" in box:
        raise box["error"]
    return box.get("result")
