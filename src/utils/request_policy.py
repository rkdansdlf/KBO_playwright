"""
Shared request policy for throttling, UA rotation, and retries.
"""
from __future__ import annotations

import asyncio
import os
import random
import time
from typing import Callable, Iterable, Any, Dict, List, Optional

DEFAULT_USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Safari/605.1.15",
]


class RequestPolicy:
    """Centralized throttling and retry policy."""

    def __init__(
        self,
        *,
        min_delay: Optional[float] = None,
        max_delay: Optional[float] = None,
        user_agents: Optional[Iterable[str]] = None,
        max_retries: Optional[int] = None,
        backoff_factor: Optional[float] = None,
    ):
        env_min = float(os.getenv("KBO_REQUEST_DELAY_MIN", min_delay or 1.5))
        env_max = float(os.getenv("KBO_REQUEST_DELAY_MAX", max_delay or 2.5))
        if env_min > env_max:
            env_min, env_max = env_max, env_min

        self.min_delay = env_min
        self.max_delay = env_max
        self.max_retries = int(os.getenv("KBO_REQUEST_MAX_RETRIES", max_retries or 3))
        self.backoff_factor = float(os.getenv("KBO_REQUEST_BACKOFF", backoff_factor or 1.5))
        self.user_agents = self._load_user_agents(user_agents)

    def _load_user_agents(self, override: Optional[Iterable[str]]) -> List[str]:
        if override:
            pool = [ua.strip() for ua in override if ua.strip()]
            if pool:
                return pool
        env_value = os.getenv("KBO_USER_AGENTS")
        if env_value:
            parsed = [ua.strip() for ua in env_value.replace("|", ",").split(",") if ua.strip()]
            if parsed:
                return parsed
        return DEFAULT_USER_AGENTS

    def random_user_agent(self) -> str:
        return random.choice(self.user_agents)

    def build_context_kwargs(self, **overrides) -> Dict[str, Any]:
        kwargs = {"user_agent": self.random_user_agent()}
        kwargs.update(overrides)
        return kwargs

    def _random_delay(self) -> float:
        return random.uniform(self.min_delay, self.max_delay)

    def delay(self):
        time.sleep(self._random_delay())

    async def delay_async(self):
        await asyncio.sleep(self._random_delay())

    def run_with_retry(self, func: Callable, *args, **kwargs):
        last_exc = None
        for attempt in range(1, self.max_retries + 1):
            try:
                return func(*args, **kwargs)
            except Exception as exc:
                last_exc = exc
                if attempt >= self.max_retries:
                    raise
                time.sleep(self._backoff_delay(attempt))
        if last_exc:
            raise last_exc

    async def run_with_retry_async(self, func: Callable, *args, **kwargs):
        last_exc = None
        for attempt in range(1, self.max_retries + 1):
            try:
                return await func(*args, **kwargs)
            except Exception as exc:
                last_exc = exc
                if attempt >= self.max_retries:
                    raise
                await asyncio.sleep(self._backoff_delay(attempt))
        if last_exc:
            raise last_exc

    def _backoff_delay(self, attempt: int) -> float:
        return self.backoff_factor * attempt
