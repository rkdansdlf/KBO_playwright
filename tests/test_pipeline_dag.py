"""Unit tests for PipelineDAG."""

from __future__ import annotations

import pytest

from src.pipeline.dag import PipelineDAG, TaskState


def test_dag_linear_execution() -> None:
    """Tasks should execute sequentially in dependency order."""
    dag = PipelineDAG("linear_test")
    trace = []

    def task_a(ctx):
        trace.append("A")
        return 1

    def task_b(ctx):
        trace.append("B")
        return ctx["task_a_output"] + 1

    dag.add_task("a", task_a)
    dag.add_task("b", task_b, dependencies={"a"})

    results = dag.execute()
    assert trace == ["A", "B"]
    assert results["a"].state == TaskState.SUCCESS
    assert results["b"].state == TaskState.SUCCESS
    assert results["b"].output == 2


def test_dag_skip_on_upstream_failure() -> None:
    """When task A fails, dependent task B must be marked SKIPPED."""
    dag = PipelineDAG("fail_test")

    def task_fail(ctx):
        msg = "Network timeout"
        raise RuntimeError(msg)

    def task_b(ctx):
        return "Should not run"

    dag.add_task("fail_node", task_fail)
    dag.add_task("downstream", task_b, dependencies={"fail_node"})

    results = dag.execute()
    assert results["fail_node"].state == TaskState.FAILED
    assert results["downstream"].state == TaskState.SKIPPED
    assert "Upstream dependency failed" in results["downstream"].error


def test_dag_detects_cycle() -> None:
    """Cyclic dependency must raise ValueError."""
    dag = PipelineDAG("cycle_test")
    dag.add_task("node1", lambda ctx: None, dependencies={"node2"})
    dag.add_task("node2", lambda ctx: None, dependencies={"node1"})

    with pytest.raises(ValueError, match="Cyclic dependency detected"):
        dag.execute()


@pytest.mark.asyncio
async def test_dag_async_execution() -> None:
    """Async task execution should be handled cleanly."""
    dag = PipelineDAG("async_test")

    async def async_task_a(ctx):
        return "async_result_a"

    def sync_task_b(ctx):
        return ctx["task_a_output"] + "_b"

    dag.add_task("a", async_task_a)
    dag.add_task("b", sync_task_b, dependencies={"a"})

    results = await dag.execute_async()
    assert results["a"].state == TaskState.SUCCESS
    assert results["a"].output == "async_result_a"
    assert results["b"].state == TaskState.SUCCESS
    assert results["b"].output == "async_result_a_b"
