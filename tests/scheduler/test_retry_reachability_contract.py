"""Contract: a tenacity ``@retry`` on a scheduler job must be able to fire.

Four jobs declared a retry policy that could never execute. Each wrapped a body
that catches ``SCHEDULER_JOB_EXCEPTIONS`` and returns without re-raising, so
tenacity never saw an exception and the declared backoff was decoration:

- ``crawl_p1p2_data_job`` (``stop_after_attempt(4)``, ``wait_exponential(min=300)``
  -- a 900s budget that never applied)
- ``_crawl_team_info_history``
- ``crawl_transit_time_job``
- ``_process_pregame_date``

A dead retry is worse than no retry: the code reads as resilient and a later
change can lean on that. So this file pins two things — the exact set of jobs
allowed to carry ``@retry``, and, for each, that its body contains a ``raise``
so an exception can reach tenacity at all.

Adding an entry to ``LIVE_RETRIES`` is a claim that the exception escapes. The
``Raise`` check below is what keeps that claim honest; a function that swallows
everything fails even if it is listed.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

JOBS_DIR = Path("src/scheduler/jobs")

#: job name -> why an exception can escape this one.
LIVE_RETRIES = {
    "crawl_retired_players_job": "re-raises after recording the failure, so a transient DB error can be retried",
    "crawl_dead_letter_recovery_job": "re-raises; the DB gate returns silently instead, so retries only happen on a real failure",
    "crawl_dead_letter_retry_job": "re-raises; the DB gate returns silently instead, so retries only happen on a real failure",
}


def _decorated_with_retry(path: Path) -> dict[str, ast.FunctionDef]:
    """Return the functions in ``path`` carrying ``@retry``, read from the AST."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    found: dict[str, ast.FunctionDef] = {}
    for node in tree.body:
        if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        for decorator in node.decorator_list:
            target = decorator.func if isinstance(decorator, ast.Call) else decorator
            if ast.unparse(target) == "retry":
                found[node.name] = node
    return found


def _all_retry_jobs() -> dict[str, Path]:
    """Map every ``@retry`` job name in the scheduler jobs package to its file."""
    out: dict[str, Path] = {}
    for path in sorted(JOBS_DIR.glob("*.py")):
        for name in _decorated_with_retry(path):
            out[name] = path
    return out


def _reaches_tenacity(node: ast.FunctionDef) -> bool:
    """Return whether the function body contains any ``raise``.

    A body that only logs and returns cannot propagate, so its ``@retry`` can
    never fire. Not a complete proof of reachability for the failure we care
    about, but it separates the observed defect class from a live policy.
    """
    return any(isinstance(child, ast.Raise) for child in ast.walk(node))


def _retry_kwargs(node: ast.FunctionDef) -> ast.Call | None:
    """Return the ``@retry(...)`` call on ``node``, if it carries one."""
    for decorator in node.decorator_list:
        target = decorator.func if isinstance(decorator, ast.Call) else decorator
        if ast.unparse(target) == "retry" and isinstance(decorator, ast.Call):
            return decorator
    return None


def _takes_tier_lock(node: ast.FunctionDef) -> bool:
    """Return whether the body acquires a scheduler tier lock."""
    return any(
        isinstance(child, ast.Call) and ast.unparse(child.func) == "_scheduler_job_lock" for child in ast.walk(node)
    )


def _excludes_lock_skip(call: ast.Call, aliases: dict[str, ast.expr]) -> bool:
    """Return whether the ``retry=`` policy exempts ``_LockSkipped``.

    The policy is normally bound to a module constant for documentation, so the
    value is followed through its assignments rather than matched textually: a
    guard that only accepted the literal call would fail the moment somebody
    named it, which is a good way to get a guard switched off.
    """
    seen: set[str] = set()
    pending: list[ast.expr] = []
    for keyword in call.keywords:
        if keyword.arg == "retry":
            pending.append(keyword.value)
    while pending:
        value = pending.pop()
        if isinstance(value, ast.Call):
            if "retry_if_not_exception_type" in ast.unparse(value) and "_LockSkipped" in ast.unparse(value):
                return True
        elif isinstance(value, ast.Name):
            if value.id in seen:
                continue
            seen.add(value.id)
            target = aliases.get(value.id)
            if target is not None:
                pending.append(target)
    return False


def _module_aliases(path: Path) -> dict[str, ast.expr]:
    """Return module-level ``NAME = <expr>`` bindings, for resolving policy aliases."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    aliases: dict[str, ast.expr] = {}
    for node in tree.body:
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.expr):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    aliases[target.id] = node.value
    return aliases


def test_the_known_live_set_matches_the_code() -> None:
    """No job may add or drop a ``@retry`` without updating the stated set."""
    assert set(_all_retry_jobs()) == set(LIVE_RETRIES)


@pytest.mark.parametrize("name", sorted(LIVE_RETRIES))
def test_every_allowlisted_retry_can_actually_propagate(name: str) -> None:
    """Each allowlisted job must contain a ``raise``, or its retry is decoration."""
    path = _all_retry_jobs()[name]
    node = _decorated_with_retry(path)[name]
    assert _reaches_tenacity(node), f"{name} carries @retry but never raises, so the policy cannot fire"


def test_the_removed_jobs_stay_undecorated() -> None:
    """The four dead policies must not creep back in."""
    removed = {
        "crawl_p1p2_data_job",
        "_crawl_team_info_history",
        "crawl_transit_time_job",
        "_process_pregame_date",
    }
    assert removed.isdisjoint(_all_retry_jobs())


def test_no_tenacity_import_survives_without_a_retry() -> None:
    """A leftover ``from tenacity import retry`` would let a dead policy return."""
    for path in sorted(JOBS_DIR.glob("*.py")):
        source = path.read_text(encoding="utf-8")
        if "from tenacity import" not in source:
            continue
        assert _decorated_with_retry(path), f"{path} imports tenacity but no function uses @retry"


@pytest.mark.parametrize("name", sorted(LIVE_RETRIES))
def test_a_locked_job_never_retries_a_lock_skip(name: str) -> None:
    """A job holding a tier lock must let ``_LockSkipped`` through untouched.

    ``_scheduler_job_lock`` raises ``_LockSkipped`` to mean "another process has
    the lock; leave it to the next cron tick", and ``_with_lock_skip_guard`` sits
    outside ``@retry`` on every such job. A default tenacity policy therefore
    retries the skip three times with 120s and 240s of backoff, fires
    ``alert_failure`` for routine contention, and consumes the exception before the
    guard can log it. Measured on 2026-10-04 before the exemption was added.
    """
    node = _decorated_with_retry(_all_retry_jobs()[name])[name]
    call = _retry_kwargs(node)

    assert call is not None, f"{name} is listed as a live retry but lost its decorator"
    if not _takes_tier_lock(node):
        pytest.skip(f"{name} does not acquire a tier lock")
    assert _excludes_lock_skip(call, _module_aliases(_all_retry_jobs()[name])), (
        f"{name} acquires a tier lock and retries, so it must set retry=retry_if_not_exception_type(_LockSkipped)"
    )
