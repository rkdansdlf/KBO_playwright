"""스케줄러 작업 의존성 추적 테스트.

src/scheduler/jobs/daily.py의 JobStatus, JobResult, _JOB_REGISTRY 및
작업 의존성 추적 함수들을 테스트합니다.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from src.scheduler.jobs.daily import (
    JobStatus,
    JobResult,
    _JOB_REGISTRY,
    _register_job,
    _update_job_status,
    _can_run_job,
    get_job_status_summary,
    clear_job_registry,
)


@pytest.fixture(autouse=True)
def reset_registry():
    """Ensure clean registry state for each test."""
    clear_job_registry()
    yield
    clear_job_registry()


# === JobStatus / JobResult dataclass tests ===


def test_job_status_enum_values() -> None:
    """Test JobStatus enum has all expected values."""
    assert JobStatus.SUCCESS.value == "success"
    assert JobStatus.FAILURE.value == "failure"
    assert JobStatus.SKIPPED.value == "skipped"
    assert JobStatus.RUNNING.value == "running"


def test_job_status_count() -> None:
    """Test JobStatus has exactly 4 states."""
    assert len(list(JobStatus)) == 4


def test_job_result_required_fields() -> None:
    """Test JobResult with required fields only."""
    result = JobResult(job_name="test_job", status=JobStatus.SUCCESS)
    assert result.job_name == "test_job"
    assert result.status == JobStatus.SUCCESS
    assert result.message == ""
    assert result.details == {}
    assert result.dependencies == []


def test_job_result_with_dependencies() -> None:
    """Test JobResult with dependencies."""
    result = JobResult(
        job_name="test_job",
        status=JobStatus.SUCCESS,
        dependencies=["dep1", "dep2"],
    )
    assert result.dependencies == ["dep1", "dep2"]


def test_job_result_with_details() -> None:
    """Test JobResult with details dict."""
    result = JobResult(
        job_name="test_job",
        status=JobStatus.FAILURE,
        details={"error": "test error", "code": 500},
    )
    assert result.details == {"error": "test error", "code": 500}


# === _register_job tests ===


def test_register_job_default_status() -> None:
    """Test that newly registered job has RUNNING status."""
    _register_job("test_job")
    assert "test_job" in _JOB_REGISTRY
    assert _JOB_REGISTRY["test_job"].status == JobStatus.RUNNING


def test_register_job_with_dependencies() -> None:
    """Test that registered job stores dependencies."""
    _register_job("test_job", dependencies=["dep1", "dep2"])
    assert _JOB_REGISTRY["test_job"].dependencies == ["dep1", "dep2"]


def test_register_job_no_dependencies() -> None:
    """Test that registered job without dependencies has empty list."""
    _register_job("test_job")
    assert _JOB_REGISTRY["test_job"].dependencies == []


# === _update_job_status tests ===


def test_update_job_status_success() -> None:
    """Test updating job status to SUCCESS."""
    _register_job("test_job")
    _update_job_status("test_job", JobStatus.SUCCESS, "Completed")
    assert _JOB_REGISTRY["test_job"].status == JobStatus.SUCCESS
    assert _JOB_REGISTRY["test_job"].message == "Completed"


def test_update_job_status_failure() -> None:
    """Test updating job status to FAILURE."""
    _register_job("test_job")
    _update_job_status("test_job", JobStatus.FAILURE, "Failed")
    assert _JOB_REGISTRY["test_job"].status == JobStatus.FAILURE


def test_update_job_status_skipped() -> None:
    """Test updating job status to SKIPPED."""
    _register_job("test_job")
    _update_job_status("test_job", JobStatus.SKIPPED, "Skipped due to dependency")
    assert _JOB_REGISTRY["test_job"].status == JobStatus.SKIPPED


def test_update_job_status_with_details() -> None:
    """Test updating job status with details dict."""
    _register_job("test_job")
    _update_job_status("test_job", JobStatus.SUCCESS, "Done", details={"rows": 100})
    assert _JOB_REGISTRY["test_job"].details == {"rows": 100}


# === _can_run_job tests ===


def test_can_run_job_no_dependencies() -> None:
    """Test that a job without dependencies can always run."""
    _register_job("test_job")
    can_run, reason = _can_run_job("test_job")
    assert can_run is True
    assert reason == ""


def test_can_run_job_with_success_dependency() -> None:
    """Test job with successful dependency can run."""
    _register_job("dep_job")
    _update_job_status("dep_job", JobStatus.SUCCESS, "OK")
    _register_job("main_job", dependencies=["dep_job"])

    can_run, reason = _can_run_job("main_job")
    assert can_run is True
    assert reason == ""


def test_can_run_job_with_failed_dependency() -> None:
    """Test job with failed dependency cannot run."""
    _register_job("dep_job")
    _update_job_status("dep_job", JobStatus.FAILURE, "Error")
    _register_job("main_job", dependencies=["dep_job"])

    can_run, reason = _can_run_job("main_job")
    assert can_run is False
    assert "failure" in reason.lower()


def test_can_run_job_with_skipped_dependency() -> None:
    """Test job with skipped dependency cannot run."""
    _register_job("dep_job")
    _update_job_status("dep_job", JobStatus.SKIPPED, "Skipped")
    _register_job("main_job", dependencies=["dep_job"])

    can_run, reason = _can_run_job("main_job")
    assert can_run is False
    assert "skipped" in reason.lower()


def test_can_run_job_with_running_dependency() -> None:
    """Test job with running dependency cannot run yet."""
    _register_job("dep_job")  # default status is RUNNING
    _register_job("main_job", dependencies=["dep_job"])

    can_run, reason = _can_run_job("main_job")
    assert can_run is False
    assert "running" in reason.lower()


def test_can_run_job_with_unknown_dependency() -> None:
    """Test that unknown dependency prevents job from running."""
    _register_job("main_job", dependencies=["nonexistent_dep"])

    can_run, reason = _can_run_job("main_job")
    assert can_run is False
    assert "not registered" in reason.lower()


def test_can_run_job_with_multiple_success_dependencies() -> None:
    """Test job with all successful dependencies can run."""
    _register_job("dep1")
    _update_job_status("dep1", JobStatus.SUCCESS, "OK")
    _register_job("dep2")
    _update_job_status("dep2", JobStatus.SUCCESS, "OK")
    _register_job("main_job", dependencies=["dep1", "dep2"])

    can_run, reason = _can_run_job("main_job")
    assert can_run is True


def test_can_run_job_with_partial_failure() -> None:
    """Test that one failed dependency blocks the job."""
    _register_job("dep1")
    _update_job_status("dep1", JobStatus.SUCCESS, "OK")
    _register_job("dep2")
    _update_job_status("dep2", JobStatus.FAILURE, "Error")
    _register_job("main_job", dependencies=["dep1", "dep2"])

    can_run, reason = _can_run_job("main_job")
    assert can_run is False


def test_can_run_job_not_registered() -> None:
    """Test that an unregistered job returns True (no dependencies to block)."""
    can_run, reason = _can_run_job("not_registered_job")
    assert can_run is True
    assert reason == ""


# === get_job_status_summary tests ===


def test_get_job_status_summary_empty() -> None:
    """Test summary is empty dict when registry is empty."""
    assert get_job_status_summary() == {}


def test_get_job_status_summary_with_jobs() -> None:
    """Test summary contains registered jobs."""
    _register_job("job1")
    _update_job_status("job1", JobStatus.SUCCESS, "OK")
    _register_job("job2", dependencies=["job1"])
    _update_job_status("job2", JobStatus.FAILURE, "Failed")

    summary = get_job_status_summary()
    assert "job1" in summary
    assert "job2" in summary
    assert summary["job1"]["status"] == "success"
    assert summary["job2"]["status"] == "failure"
    assert summary["job2"]["dependencies"] == ["job1"]


def test_get_job_status_summary_includes_message() -> None:
    """Test that summary includes job message."""
    _register_job("test_job")
    _update_job_status("test_job", JobStatus.SUCCESS, "All done!")

    summary = get_job_status_summary()
    assert summary["test_job"]["message"] == "All done!"


def test_get_job_status_summary_includes_details() -> None:
    """Test that summary includes job details."""
    _register_job("test_job")
    _update_job_status("test_job", JobStatus.SUCCESS, "Done", details={"count": 42})

    summary = get_job_status_summary()
    assert summary["test_job"]["details"] == {"count": 42}


# === clear_job_registry tests ===


def test_clear_job_registry() -> None:
    """Test that clear_job_registry empties the registry."""
    _register_job("job1")
    _register_job("job2")
    assert len(_JOB_REGISTRY) == 2

    clear_job_registry()
    assert len(_JOB_REGISTRY) == 0
    assert get_job_status_summary() == {}


def test_clear_job_registry_idempotent() -> None:
    """Test that clear_job_registry is idempotent."""
    clear_job_registry()
    clear_job_registry()
    assert _JOB_REGISTRY == {}


# === Integration scenario tests ===


def test_dependency_chain_success() -> None:
    """Test a complete dependency chain that succeeds."""
    _register_job("step1")
    _update_job_status("step1", JobStatus.SUCCESS, "Step 1 done")
    _register_job("step2", dependencies=["step1"])
    _update_job_status("step2", JobStatus.SUCCESS, "Step 2 done")
    _register_job("step3", dependencies=["step2"])
    _update_job_status("step3", JobStatus.SUCCESS, "Step 3 done")

    can_run, _ = _can_run_job("step3")
    assert can_run is True


def test_dependency_chain_failure_propagates() -> None:
    """Test that a failure in the chain blocks downstream jobs."""
    _register_job("step1")
    _update_job_status("step1", JobStatus.SUCCESS, "OK")
    _register_job("step2", dependencies=["step1"])
    _update_job_status("step2", JobStatus.FAILURE, "Error")
    _register_job("step3", dependencies=["step2"])

    can_run, reason = _can_run_job("step3")
    assert can_run is False


def test_diamond_dependency_pattern() -> None:
    """Test diamond-shaped dependency: A -> B, A -> C, B -> D, C -> D."""
    _register_job("a")
    _update_job_status("a", JobStatus.SUCCESS, "OK")
    _register_job("b", dependencies=["a"])
    _update_job_status("b", JobStatus.SUCCESS, "OK")
    _register_job("c", dependencies=["a"])
    _update_job_status("c", JobStatus.SUCCESS, "OK")
    _register_job("d", dependencies=["b", "c"])

    can_run, _ = _can_run_job("d")
    assert can_run is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
