"""DB 장애 시 스케줄러 잡이 **락을 잡기 전에** 빠르게 포기하는지 검증한다.

2026-10-03 장애에서 DLQ retry/recovery 잡이 ``MAINTENANCE_LOCK``을 붙잡은 채
연결 재시도(tenacity ``wait_exponential(min=120)`` 포함)로 약 150초를 보냈고,
같은 락을 쓰는 다른 유지보수 잡이 60초 대기 후 스킵됐다. 게이트는 락 획득과
재시도 **이전**에 있어야 하며, 실패해도 알림을 발생시키지 않는다(Prometheus
``kbo_db_available`` 게이지가 알림을 담당한다).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.scheduler.jobs import maintenance


class _NullLock:
    """아무것도 획득하지 않는 락 대역(기존 스케줄러 테스트와 동일한 패턴)."""

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        pass

    def __enter__(self) -> _NullLock:
        return self

    def __exit__(self, *_args: object) -> bool:
        return False


def _skip_without_lock(monkeypatch, job) -> MagicMock:
    monkeypatch.setattr(maintenance, "database_reachable", lambda **_kwargs: False)
    lock = MagicMock()
    monkeypatch.setattr(maintenance, "_scheduler_job_lock", lock)

    job()

    lock.assert_not_called()
    return lock


def test_dlq_retry_skips_before_taking_the_lock(monkeypatch) -> None:
    with patch("src.services.crawl_dead_letter_worker.retry_due_dead_letters") as worker:
        _skip_without_lock(monkeypatch, maintenance.crawl_dead_letter_retry_job)

    worker.assert_not_called()


def test_dlq_recovery_skips_before_taking_the_lock(monkeypatch) -> None:
    with patch("src.services.crawl_dead_letter_recovery.recover_stuck_retrying") as recovery:
        _skip_without_lock(monkeypatch, maintenance.crawl_dead_letter_recovery_job)

    recovery.assert_not_called()


def test_snapshot_drift_skips_before_taking_the_lock(monkeypatch) -> None:
    with patch("src.services.snapshot_replay.validate_recent_snapshots") as validate:
        _skip_without_lock(monkeypatch, maintenance.snapshot_drift_check_job)

    validate.assert_not_called()


def test_a_dead_database_returns_without_raising(monkeypatch) -> None:
    """게이트는 예외를 던지지 않는다 — 그래야 tenacity 재시도와 실패 알림이 발동하지 않는다."""
    monkeypatch.setattr(maintenance, "database_reachable", lambda **_kwargs: False)
    monkeypatch.setattr(maintenance, "_scheduler_job_lock", _NullLock)

    for job in (
        maintenance.crawl_dead_letter_retry_job,
        maintenance.crawl_dead_letter_recovery_job,
        maintenance.snapshot_drift_check_job,
    ):
        job()


def test_the_drift_job_proceeds_when_the_database_answers(monkeypatch) -> None:
    monkeypatch.setattr(maintenance, "database_reachable", lambda **_kwargs: True)
    monkeypatch.setattr(maintenance, "_scheduler_job_lock", _NullLock)

    with (
        patch("src.services.snapshot_replay.validate_recent_snapshots", return_value=[]) as validate,
        patch("src.notifications.bridge.apply_incidents") as apply_incidents,
    ):
        maintenance.snapshot_drift_check_job()

    validate.assert_called_once()
    apply_incidents.assert_called_once()
