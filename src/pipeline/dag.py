"""Event-Driven DAG Pipeline Orchestration Engine for KBO Data System."""

from __future__ import annotations

import logging
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger(__name__)


class TaskState(StrEnum):
    """Lifecycle state of a pipeline DAG task."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    RETRYING = "RETRYING"
    SKIPPED = "SKIPPED"


@dataclass
class TaskExecutionResult:
    """Detailed execution result for a single task."""

    task_id: str
    state: TaskState
    started_at: datetime | None = None
    finished_at: datetime | None = None
    elapsed_seconds: float = 0.0
    error: str | None = None
    output: Any = None


@dataclass
class PipelineTask:
    """A single executable node in the pipeline DAG."""

    task_id: str
    execute_fn: Callable[[dict[str, Any]], Any]
    dependencies: set[str] = field(default_factory=set)
    allow_failure: bool = False


class PipelineDAG:
    """Directed Acyclic Graph orchestrator for multi-stage data pipelines."""

    def __init__(self, name: str = "kbo_pipeline") -> None:
        """Initialize empty DAG."""
        self.name = name
        self._tasks: dict[str, PipelineTask] = {}

    def add_task(
        self,
        task_id: str,
        execute_fn: Callable[[dict[str, Any]], Any],
        *,
        dependencies: set[str] | None = None,
        allow_failure: bool = False,
    ) -> None:
        """Register a new task node with explicit dependency task IDs."""
        if task_id in self._tasks:
            msg = f"Duplicate task_id: {task_id}"
            raise ValueError(msg)
        self._tasks[task_id] = PipelineTask(
            task_id=task_id,
            execute_fn=execute_fn,
            dependencies=dependencies or set(),
            allow_failure=allow_failure,
        )

    def get_execution_order(self) -> list[str]:
        """Perform topological sort (Kahn's algorithm) to determine valid task execution order."""
        in_degree: dict[str, int] = dict.fromkeys(self._tasks, 0)
        graph: dict[str, list[str]] = defaultdict(list)

        for task_id, task in self._tasks.items():
            for dep in task.dependencies:
                if dep not in self._tasks:
                    msg = f"Task '{task_id}' depends on non-existent task '{dep}'"
                    raise ValueError(msg)
                graph[dep].append(task_id)
                in_degree[task_id] += 1

        queue: deque[str] = deque([task_id for task_id, deg in in_degree.items() if deg == 0])
        order: list[str] = []

        while queue:
            node = queue.popleft()
            order.append(node)
            for neighbor in graph[node]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        if len(order) != len(self._tasks):
            msg = "Cyclic dependency detected in PipelineDAG"
            raise ValueError(msg)

        return order

    def execute(self, context: dict[str, Any] | None = None) -> dict[str, TaskExecutionResult]:
        """Execute all tasks in topological order, propagating outputs and skipping on upstream failure."""
        ctx = dict(context or {})
        results: dict[str, TaskExecutionResult] = {}
        execution_order = self.get_execution_order()

        for task_id in execution_order:
            task = self._tasks[task_id]
            res = TaskExecutionResult(task_id=task_id, state=TaskState.PENDING)

            # Check if any dependencies failed
            failed_deps = [
                dep
                for dep in task.dependencies
                if dep in results and results[dep].state in (TaskState.FAILED, TaskState.SKIPPED)
            ]
            if failed_deps and not task.allow_failure:
                res.state = TaskState.SKIPPED
                res.error = f"Upstream dependency failed: {', '.join(failed_deps)}"
                results[task_id] = res
                logger.info("[DAG] Skipping task '%s' due to failed dependencies: %s", task_id, failed_deps)
                continue

            # Execute task
            res.state = TaskState.RUNNING
            res.started_at = datetime.now(UTC)
            t0 = datetime.now(UTC).timestamp()

            try:
                out = task.execute_fn(ctx)
                res.output = out
                res.state = TaskState.SUCCESS
                ctx[f"task_{task_id}_output"] = out
            except Exception as exc:
                res.state = TaskState.FAILED
                res.error = str(exc)
                logger.exception("[DAG] Task '%s' failed", task_id)
            finally:
                t1 = datetime.now(UTC).timestamp()
                res.finished_at = datetime.now(UTC)
                res.elapsed_seconds = round(t1 - t0, 4)
                results[task_id] = res

        return results

    async def execute_async(self, context: dict[str, Any] | None = None) -> dict[str, TaskExecutionResult]:
        """Execute all tasks asynchronously in topological order."""
        import inspect

        ctx = dict(context or {})
        results: dict[str, TaskExecutionResult] = {}
        execution_order = self.get_execution_order()

        for task_id in execution_order:
            task = self._tasks[task_id]
            res = TaskExecutionResult(task_id=task_id, state=TaskState.PENDING)

            failed_deps = [
                dep
                for dep in task.dependencies
                if dep in results and results[dep].state in (TaskState.FAILED, TaskState.SKIPPED)
            ]
            if failed_deps and not task.allow_failure:
                res.state = TaskState.SKIPPED
                res.error = f"Upstream dependency failed: {', '.join(failed_deps)}"
                results[task_id] = res
                logger.info("[DAG] Skipping task '%s' due to failed dependencies: %s", task_id, failed_deps)
                continue

            res.state = TaskState.RUNNING
            res.started_at = datetime.now(UTC)
            t0 = datetime.now(UTC).timestamp()

            try:
                if inspect.iscoroutinefunction(task.execute_fn):
                    out = await task.execute_fn(ctx)
                else:
                    out = task.execute_fn(ctx)
                    if inspect.isawaitable(out):
                        out = await out
                res.output = out
                res.state = TaskState.SUCCESS
                ctx[f"task_{task_id}_output"] = out
            except Exception as exc:
                res.state = TaskState.FAILED
                res.error = str(exc)
                logger.exception("[DAG] Task '%s' failed", task_id)
            finally:
                t1 = datetime.now(UTC).timestamp()
                res.finished_at = datetime.now(UTC)
                res.elapsed_seconds = round(t1 - t0, 4)
                results[task_id] = res

        return results


__all__ = ["PipelineDAG", "PipelineTask", "TaskExecutionResult", "TaskState"]
