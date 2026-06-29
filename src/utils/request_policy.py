"""Shared request policy for throttling, UA rotation, and retries."""

import asyncio
import logging
import os
import random
import time
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from typing import Any, ParamSpec, TypeVar

from src.utils.throttle import throttle

logger = logging.getLogger(__name__)
P = ParamSpec("P")
R = TypeVar("R")

DEFAULT_USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Safari/605.1.15",
]


@dataclass(frozen=True)
class RequestPolicyConfig:
    """RequestPolicyConfig class."""

    min_delay: float | None = None
    max_delay: float | None = None
    user_agents: Iterable[str] | None = None
    max_retries: int | None = None
    backoff_factor: float | None = None
    retry_exceptions: tuple[type[BaseException], ...] = (Exception,)


class RequestPolicy:
    """Centralized throttling and retry policy."""

    def __init__(self, config: RequestPolicyConfig | None = None, **overrides: object) -> None:
        """
        Initialize a new instance.

        Args:
            config: Configuration object.
            overrides: Overrides.
            config: Configuration object.
            overrides: Overrides.

        """
        if config is None:
            config = RequestPolicyConfig(**overrides)  # type: ignore[arg-type]
        elif overrides:
            msg = "Pass either RequestPolicyConfig or keyword policy fields, not both"
            raise TypeError(msg)
        env_min = float(os.getenv("KBO_REQUEST_DELAY_MIN", config.min_delay or 1.5))
        env_max = float(os.getenv("KBO_REQUEST_DELAY_MAX", config.max_delay or 2.5))
        if env_min > env_max:
            env_min, env_max = env_max, env_min

        self.min_delay = env_min
        self.max_delay = env_max
        self.max_retries = int(os.getenv("KBO_REQUEST_MAX_RETRIES", config.max_retries or 3))
        self.backoff_factor = float(os.getenv("KBO_REQUEST_BACKOFF", config.backoff_factor or 1.5))
        self.user_agents = self._load_user_agents(config.user_agents)
        self.retry_exceptions = config.retry_exceptions

    @classmethod
    def with_delay(cls, min_delay: float | None, max_delay: float | None = None) -> "RequestPolicy":
        """
        Handle the with delay operation.

        Args:
            min_delay: Min Delay.
            max_delay: Max Delay.
            min_delay: Min Delay.
            max_delay: Max Delay.
            min_delay: Min Delay.
            max_delay: Max Delay.

        Returns:
            RequestPolicy instance.

        """
        return cls(RequestPolicyConfig(min_delay=min_delay, max_delay=max_delay))

    def _load_user_agents(self, override: Iterable[str] | None) -> list[str]:
        """
        Load user agents.

        Args:
            override: Override.
            override: Override.
            override: Override.

        Returns:
            List of results.

        """
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
        """
        Handle the random user agent operation.

        Returns:
            String result.

        """
        return random.choice(self.user_agents)

    def build_context_kwargs(self, **overrides: object) -> dict[str, Any]:
        """
        Build context kwargs.

        Args:
            overrides: Overrides.
            overrides: Overrides.

        Returns:
            Dictionary mapping.

        """
        kwargs = {"user_agent": self.random_user_agent()}

        kwargs.update(overrides)  # type: ignore[arg-type]
        return kwargs

    def _random_delay(self) -> float:
        """
        Handle the random delay operation.

        Returns:
            float instance.

        """
        return random.uniform(self.min_delay, self.max_delay)

    def delay(self, host: str = "koreabaseball.com") -> None:
        """
        Handle the delay operation.

        Args:
            host: Host.
            host: Host.
            host: Host.

        """
        throttle.default_delay = self.min_delay  # Dynamic adjustment based on policy

        throttle.wait_sync(host)

    async def delay_async(self, host: str = "koreabaseball.com") -> None:
        """
        Handle the delay async operation.

        Args:
            host: Host.
            host: Host.
            host: Host.

        """
        throttle.default_delay = self.min_delay

        await throttle.wait(host)

    def run_with_retry(self, func: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> R:
        """
        Run with retry.

        Args:
            func: Func.
            args: Positional arguments to pass through.
            kwargs: Keyword arguments to pass through.
            func: Func.
            args: Positional arguments to pass through.
            kwargs: Keyword arguments to pass through.
            func: Func.

        Returns:
            R instance.

        """
        last_exc = None

        for attempt in range(1, self.max_retries + 1):
            try:
                return func(*args, **kwargs)
            except self.retry_exceptions as exc:
                last_exc = exc
                if attempt >= self.max_retries:
                    raise
                logger.info("Retrying function due to error: %s", exc)
                time.sleep(self._backoff_delay(attempt))
        if last_exc:
            raise last_exc
        msg = "Unreachable: all retries exhausted without exception"
        raise RuntimeError(msg)

    async def run_with_retry_async(self, func: Callable[P, Awaitable[R]], *args: P.args, **kwargs: P.kwargs) -> R:
        """
        Run with retry async.

        Args:
            func: Func.
            args: Positional arguments to pass through.
            kwargs: Keyword arguments to pass through.
            func: Func.
            args: Positional arguments to pass through.
            kwargs: Keyword arguments to pass through.
            func: Func.

        Returns:
            R instance.

        """
        last_exc = None

        for attempt in range(1, self.max_retries + 1):
            try:
                return await func(*args, **kwargs)
            except self.retry_exceptions as exc:
                last_exc = exc
                if attempt >= self.max_retries:
                    raise
                logger.info("Retrying async function due to error: %s", exc)
                await asyncio.sleep(self._backoff_delay(attempt))
        if last_exc:
            raise last_exc
        msg = "Unreachable: all retries exhausted without exception"
        raise RuntimeError(msg)

    def _backoff_delay(self, attempt: int) -> float:
        """
        Handle the backoff delay operation.

        Args:
            attempt: Attempt.
            attempt: Attempt.
            attempt: Attempt.

        Returns:
            float instance.

        """
        return self.backoff_factor * attempt
