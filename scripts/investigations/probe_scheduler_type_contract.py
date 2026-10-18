from collections.abc import Callable

from src.scheduler.locks import _with_db_fail_fast_guard

@_with_db_fail_fast_guard
def bare() -> None:
    """A job guarded with the bare spelling."""


@_with_db_fail_fast_guard(urls=lambda: ('postgresql://probe',))
def with_urls() -> None:
    """A job guarded with the argument spelling."""


registry: list[tuple[Callable[..., object], str, int]] = [
    (bare, "bare", 1),
    (with_urls, "urls", 2),
]
