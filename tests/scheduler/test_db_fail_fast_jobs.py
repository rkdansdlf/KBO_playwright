"""DB 장애 시 스케줄러 잡이 **락을 잡기 전에** 빠르게 포기하는지 검증한다.

2026-10-03 장애에서 DLQ retry/recovery 잡이 ``MAINTENANCE_LOCK``을 붙잡은 채
연결 재시도(tenacity ``wait_exponential(min=120)`` 포함)로 약 150초를 보냈고,
같은 락을 쓰는 다른 유지보수 잡이 60초 대기 후 스킵됐다. 게이트는 락 획득과
재시도 **이전**에 있어야 하며, 실패해도 알림을 발생시키지 않는다(Prometheus
``kbo_db_available`` 게이지가 알림을 담당한다).

게이트는 ``_with_db_fail_fast_guard`` 데코레이터 하나로 모든 잡에 적용되며
``src.scheduler.locks`` 안에서 프로브 결과를 메모이즈한다. 따라서 이 파일은 두
가지를 함께 고정한다: 데코레이터의 순서(락·tenacity보다 바깥), 그리고 **어떤
DB 의존 유지보수 잡도 게이트 없이 추가되지 않는다**는 완결성.
"""

from __future__ import annotations

import ast
import pathlib
import subprocess
import sys
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pytest
from tenacity import retry, stop_after_attempt, wait_none

from src.scheduler import locks
from src.scheduler.config import SCHEDULER_JOB_EXCEPTIONS
from src.scheduler.jobs import daily, maintenance

if TYPE_CHECKING:
    from collections.abc import Iterator


def _job_source(module_path: str, name: str) -> str:
    """Return one job function's source, read from the file rather than imported."""
    source = pathlib.Path(module_path).read_text(encoding="utf-8")
    for node in ast.parse(source).body:
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef) and node.name == name:
            return ast.get_source_segment(source, node) or ""
    msg = f"{name} is not a top-level job in {module_path}"
    raise AssertionError(msg)


#: 게이트를 검증하는 세 잡. 하나는 tenacity가 겹친DLQ retry, 하나는
#: ``MAINTENANCE_LOCK`` 잡, 하나는 인시던트를 여는 drift 체크다.
GATED_PROBES = (
    (maintenance.crawl_dead_letter_retry_job, "src.services.crawl_dead_letter_worker.retry_due_dead_letters"),
    (maintenance.crawl_dead_letter_recovery_job, "src.services.crawl_dead_letter_recovery.recover_stuck_retrying"),
    (maintenance.snapshot_drift_check_job, "src.services.snapshot_replay.validate_recent_snapshots"),
)

#: 유지보수 락 잡 전수 검사에서 게이트가 없어야 하는 잡과 그 사유.
#:
#: 유지보수 락 잡 전부에 게이트를 붙이되 이 둘은 제외한다. ``backup_db_job``는
#: ORM 세션이 아니라 ``sqlite3``로 로컬 파일을 백업하고, ``cleanup_stale_data_job``는
#: 파일만 정리한다. 둘 다 운영 DB가 죽었다고 해서 할 일이 사라지지 않으며, 반대로
#: 운영 DB 프로브로 게이트하면 백업 대상 파일과 무관한 신호에 동작이 좌우된다.
#:
#: ``trim_scheduler_logs_job``는 유지보수 락 잡도 아니므로 여기 들어오지 않는다.
#: 게이트가 필요 없다는 판단과 게이트 대상이 아니라는 판단은 서로 다르다.
DELIBERATELY_UNGATED = {
    "backup_db_job": "sqlite3 file backup; not a SessionLocal reader",
    "cleanup_stale_data_job": "file-only cleanup; the database is irrelevant to it",
}

JOB_MODULES = (maintenance, daily)


class _NullLock:
    """아무것도 획득하지 않는 락 대역(기존 스케줄러 테스트와 동일한 패턴)."""

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        pass

    def __enter__(self) -> _NullLock:
        return self

    def __exit__(self, *_args: object) -> bool:
        return False


@pytest.fixture(autouse=True)
def _reset_gate() -> Iterator[None]:
    """프로브 메모이즈를 매 테스트마다 초기화한다.

    실패 결과는 쿨다운 동안 캐시되므로, 초기화 없이 두 시나리오를 연속 검증하면
    두 번째 검증이 첫 번째의 결과를 재사용해 통과해 버린다.
    """
    locks._reset_db_gate()
    yield
    locks._reset_db_gate()


def _skip_without_lock(monkeypatch: pytest.MonkeyPatch, job) -> MagicMock:
    """DB가 죽었을 때 잡이 락을 건드리지도 않고 본문도 돌리지 않는지 확인."""
    monkeypatch.setattr(locks, "database_reachable", lambda **_kwargs: False)
    monkeypatch.setattr(maintenance, "_scheduler_job_lock", MagicMock())
    monkeypatch.setattr(daily, "_scheduler_job_lock", MagicMock())

    job()

    maintenance._scheduler_job_lock.assert_not_called()
    daily._scheduler_job_lock.assert_not_called()


@pytest.mark.parametrize(("job", "worker_path"), GATED_PROBES)
def test_a_job_skips_before_taking_the_lock(monkeypatch: pytest.MonkeyPatch, job, worker_path: str) -> None:
    with patch(worker_path) as worker:
        _skip_without_lock(monkeypatch, job)

    worker.assert_not_called()


@pytest.mark.parametrize(("job", "worker_path"), GATED_PROBES)
def test_a_dead_database_never_raises(monkeypatch: pytest.MonkeyPatch, job, worker_path: str) -> None:
    """게이트는 예외를 던지지 않는다 — 그래야 tenacity 재시도와 실패 알림이 발동하지 않는다."""
    monkeypatch.setattr(locks, "database_reachable", lambda **_kwargs: False)
    monkeypatch.setattr(maintenance, "_scheduler_job_lock", _NullLock)

    job()


def test_the_drift_job_proceeds_when_the_database_answers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(locks, "database_reachable", lambda **_kwargs: True)
    monkeypatch.setattr(maintenance, "_scheduler_job_lock", _NullLock)

    with (
        patch("src.services.snapshot_replay.validate_recent_snapshots", return_value=[]) as validate,
        patch("src.notifications.bridge.apply_incidents") as apply_incidents,
    ):
        maintenance.snapshot_drift_check_job()

    validate.assert_called_once()
    apply_incidents.assert_called_once()


def test_the_gate_is_outer_to_the_lock_and_the_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    """순서가 계약이다.

    게이트가 ``@retry`` 안쪽에 있으면 DB가 죽었을 때 락을 잡은 채 tenacity가
    exponential backoff을 반복한다. 게이트가 락 안쪽에 있으면 이미 잡은 락을
    쥔 채 락 대기 시간까지 소모한다. 둘 다 2026-10-03에 실제로 일어난 실패이며,
    순서를 바꿔서는 되돌릴 수 없다.
    """
    calls: list[int] = []

    @locks._with_db_fail_fast_guard
    @locks._with_lock_skip_guard
    @retry(stop=stop_after_attempt(3), wait=wait_none())
    def job() -> None:
        calls.append(1)

    monkeypatch.setattr(locks, "database_reachable", lambda **_kwargs: False)

    job()

    assert calls == [], "the job body ran while the database was unreachable"

    # The failed probe is memoised for its cooldown, so recovery is only visible
    # once that window passes. This is the same path the scheduler takes.
    monkeypatch.setattr(locks, "database_reachable", lambda **_kwargs: True)
    locks._reset_db_gate()

    job()

    assert calls == [1], "the gate kept the job out even though the database answered"


class TestTheGateProbesOncePerWindow:
    def test_a_failure_is_not_re_probed_for_every_job(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """장애 중에 잡마다 프로브하면 그 잡들만큼 커넥트 타임아웃을 낸다."""
        probes = []

        def _probe(**_kwargs: object) -> bool:
            probes.append(1)
            return False

        monkeypatch.setattr(locks, "database_reachable", _probe)

        for _ in range(5):
            assert locks._db_gate() is False

        assert len(probes) == 1, "each call re-probed, so the cooldown is not in effect"

    def test_a_success_is_never_carried_forward(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """DB가 돌아온 순간 다음 잡이 곧바로 시작되어야 한다.

        성공을 캐시하면 복구 후 쿨다운만큼 아무도 실행되지 않는데, 장애 판단에만
        쿨다운이 필요한 이유다.
        """
        probes = []

        def _probe(**_kwargs: object) -> bool:
            probes.append(1)
            return True

        monkeypatch.setattr(locks, "database_reachable", _probe)

        assert locks._db_gate() is True
        assert locks._db_gate() is True
        assert len(probes) == 2

    def test_recovery_is_picked_up_once_the_cooldown_expires(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """쿨다운은 장애에만 적용된다. 상태를 뒤집으면 즉시 다시 조회가 난다."""
        answers = iter([False, True])
        monkeypatch.setattr(locks, "database_reachable", lambda **_kwargs: next(answers))
        monkeypatch.setattr(locks._DB_GATE, "cooldown_seconds", 0.0)

        assert locks._db_gate() is False
        assert locks._db_gate() is True

    def test_a_failure_memo_is_not_shared_across_url_sets(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """두 대상 집합의 메모를 공유하면 게이트가 구멍이 된다.

        잡 하나가 죽은 벡터 저장소 때문에 False를 캐시했다면, 운영 DB만 보는
        잡까지 그 결과를 물려받아 건너뛰게 된다. 한쪽 방향의 오탐은 안전하지만,
        이 방향은 조용한 데이터 손실이다.
        """
        probed: list[tuple[str, ...]] = []

        def _probe(*, urls=None, **_kwargs: object) -> bool:
            key = tuple(urls or ("op",))
            probed.append(key)
            return "vector" not in key

        monkeypatch.setattr(locks, "database_reachable", _probe)

        assert locks._db_gate(("op", "vector")) is False
        assert locks._db_gate(("op",)) is True
        assert probed == [("op", "vector"), ("op",)]


class TestTheGateNamesTheRightDatabases:
    """잡이 필요한 저장소를 실제로 본다.

    RAG 빌드는 소스·sparse·벡터로 최대 3개 DB를 열 수 있다. 게이트가
    `DATABASE_URL`만 보면, 운영 DB는 살아 있고 벡터 저장소만 죽었을 때 그 잡이
    락을 잡은 채 대상 DB 타임아웃을 기다린다 — 게이트를 만들려던 바로 그 상황이
    다른 이름으로 남는 것이다.

    환경변수로 실제 해석기를 통과시킨다. `_rag_target_urls`를 스텁하면 데코레이터가
    import 시점에 캡처한 함수 참조를 바꿀 수 없어(그리고 바꿀 필요도 없어) 테스트가
    실제로 고치려는 회귀를 우회하게 된다.
    """

    @pytest.fixture
    def _vector_target(self, monkeypatch: pytest.MonkeyPatch) -> str:
        """Give the deployment a separate dense target, as pgvector deployments do."""
        for name in ("RAG_SOURCE_DB_URL", "RAG_INDEX_DB_URL", "PGVECTOR_URL", "PGVECTOR_TEST_URL"):
            monkeypatch.delenv(name, raising=False)
        monkeypatch.setenv("PGVECTOR_URL", "postgresql://vector/db")
        return "postgresql://vector/db"

    def test_the_rag_job_probes_its_vector_store(
        self,
        _vector_target: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        probed: list[tuple[str, ...]] = []
        monkeypatch.setattr(
            locks,
            "database_reachable",
            lambda *, urls=None, **_kwargs: probed.append(tuple(urls or ())) or True,
        )
        monkeypatch.setattr(maintenance, "_scheduler_job_lock", _NullLock)

        maintenance.sync_rag_incremental_job()

        assert probed and _vector_target in probed[0], probed

    def test_a_dead_vector_store_holds_the_rag_job_before_the_lock(
        self,
        _vector_target: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(
            locks,
            "database_reachable",
            lambda *, urls=None, **_kwargs: _vector_target not in (urls or ()),
        )
        monkeypatch.setattr(maintenance, "_scheduler_job_lock", MagicMock())

        maintenance.sync_rag_incremental_job()

        maintenance._scheduler_job_lock.assert_not_called()

    def test_the_target_list_follows_the_build_precedence(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """`PGVECTOR_TEST_URL`이 `PGVECTOR_URL`보다 우선한다 — 빌드와 같아야 한다.

        게이트가 자기 해석을 따로 들면 빌드가 열지 않는 URL을 열게 되며, 그
        URL이 죽었을 때 잡을 빌드와 무관한 이유로 건너뛴다.
        """
        for name in ("RAG_SOURCE_DB_URL", "RAG_INDEX_DB_URL", "PGVECTOR_URL", "PGVECTOR_TEST_URL"):
            monkeypatch.delenv(name, raising=False)
        monkeypatch.setenv("PGVECTOR_URL", "postgresql://stale/db")
        monkeypatch.setenv("PGVECTOR_TEST_URL", "postgresql://live/db")

        targets = maintenance._rag_target_urls()

        assert "postgresql://live/db" in targets
        assert "postgresql://stale/db" not in targets

    def test_the_operational_database_is_always_in_the_set(
        self,
        _vector_target: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """`BuildTargets`는 sparse가 별도면 `target_db_url`을 담지 않는다.

        빌드는 어쨌든 공유 엔진으로 운영 DB를 열기 때문에, 해석 결과만 믿으면
        잡이 실제로 여는 저장소를 놓친다.
        """
        from src.db.engine import DATABASE_URL

        monkeypatch.setenv("RAG_INDEX_DB_URL", "postgresql://sparse/db")

        targets = maintenance._rag_target_urls()

        assert "postgresql://sparse/db" in targets
        assert DATABASE_URL in targets, "the operational database is opened regardless of targets"


class TestTheBackendPrecheckSeesTheSameTargets:
    """사전 검사와 게이트는 같은 목록을 봐야 한다.

    둘이 다른 env를 읽으면 한쪽은 잡을 돌리고 다른 쪽은 건너뛰는 구성이
    생긴다. 어느 방향이든 조용하다.
    """

    def test_a_separate_sparse_store_counts_as_configured(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """`RAG_INDEX_DB_URL`만 설정된 배포가 조용히 건너뛰어지던 회귀."""
        from src.db.engine import DATABASE_URL

        for name in ("RAG_SOURCE_DB_URL", "RAG_INDEX_DB_URL", "PGVECTOR_URL", "PGVECTOR_TEST_URL"):
            monkeypatch.delenv(name, raising=False)
        monkeypatch.setenv("RAG_INDEX_DB_URL", "postgresql://sparse/db")
        monkeypatch.setenv("PGVECTOR_URL", "postgresql://vector/db")

        assert "postgresql://sparse/db" in maintenance._rag_target_urls()
        assert DATABASE_URL in maintenance._rag_target_urls()
        assert maintenance._rag_vector_backend_configured() is True

    def test_a_deployment_with_no_dense_target_is_not_configured(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """벡터 대상 없이 dense 빌드는 불가능하다 — 조용히 돌다 실패하는 대신 건너뛴다."""
        for name in ("RAG_SOURCE_DB_URL", "RAG_INDEX_DB_URL", "PGVECTOR_URL", "PGVECTOR_TEST_URL"):
            monkeypatch.delenv(name, raising=False)

        assert maintenance._rag_target_urls() == ()
        assert maintenance._rag_vector_backend_configured() is False


class TestAnUnusableStoreIsReportedRatherThanEscaped:
    """벡터 저장소를 못 써도 잡이 그것을 알고 종료해야 한다.

    게이트가 저장소별 도달성을 확인하게 만들어도, 그 확인이 실제로 실패하는
    경로는 별도 문제다. RAG 빌드는 원래 ``sys.exit(1)``로 끝냈고,
    ``SystemExit``은 ``BaseException``이라 ``SCHEDULER_JOB_EXCEPTIONS``에
    잡히지 않는다. 잡이 락을 쥔 채 예외 없이 죽으면 ``finally``는 락을 풀지만
    ``logger.exception``도, 실패 알림도 발동하지 않는다 — 게이트가 막으려던
    실패가 이름만 바뀌어 그대로 남는다.
    """

    def test_the_build_failure_is_catchable_by_the_job_handler(self) -> None:
        """잡의 ``except``가 이 실패를 받을 수 있어야 한다."""
        from src.cli.rag.build_rag_index import RagIndexUnavailableError

        assert issubclass(RagIndexUnavailableError, SCHEDULER_JOB_EXCEPTIONS)
        assert not issubclass(RagIndexUnavailableError, SystemExit), (
            "SystemExit cannot be caught by the job handler; that is the bug this type replaces"
        )

    def test_an_unreachable_vector_store_raises_instead_of_exiting(self, monkeypatch) -> None:
        """빌드는 예외를 던지고, 프로세스 exit은 CLI 경계에서만 만든다."""
        from types import SimpleNamespace

        from src.cli.rag import build_rag_index

        monkeypatch.setattr(build_rag_index, "_is_oracle_url", lambda _url: False)
        monkeypatch.setattr(build_rag_index, "is_pgvector_available", lambda: False, raising=False)
        resolved = SimpleNamespace(
            source_db="sqlite:///x",
            sparse_index_db="postgresql://op/db",
            vector_db="",
            target_environment="local",
            write_enabled=True,
        )
        resolved.display = lambda: {
            "source_db": "sqlite://",
            "sparse_index_db": "pg",
            "vector_db": "pg",
            "target_environment": "local",
            "write_enabled": True,
        }
        monkeypatch.setattr(build_rag_index, "_resolve_build_targets", lambda *a, **k: resolved)

        with pytest.raises(build_rag_index.RagIndexUnavailableError):
            build_rag_index.main(["--dry-run"])

    def test_the_cli_boundary_still_exits_with_the_documented_code(self, monkeypatch) -> None:
        """CLI 계약은 그대로다: 저장소를 못 쓰면 exit 1, 설정 오류면 exit 2."""
        from src.cli.rag import build_rag_index

        def _raise(*_a: object, **_k: object) -> None:
            raise build_rag_index.RagIndexUnavailableError("vector store down")

        monkeypatch.setattr(build_rag_index, "main", _raise)

        assert build_rag_index.cli_main([]) == 1

        def _raise_two(*_a: object, **_k: object) -> None:
            raise build_rag_index.RagIndexUnavailableError("bad target", exit_code=2)

        monkeypatch.setattr(build_rag_index, "main", _raise_two)

        assert build_rag_index.cli_main([]) == 2


class TestEveryDatabaseBoundJobIsGated:
    """완결성의 증명은 게이트 목록이 아니라 전수 검사다.

    새로운 유지보수 잡이 데코레이터 없이 들어오면 이 테스트가 실패해야 한다.
    그렇지 않으면 그 잡은 DB가 죽은 창 동안 조용히 아무 일도 하지 않고, 그것을
    알아낼 방법이 없다 — 게이트가 조용한 것이 의도된 설계이기 때문이다.
    """

    @staticmethod
    def _maintenance_jobs(module_path: str) -> dict[str, str]:
        """Return each maintenance-lock job mapped to whether it declares the gate.

        Read from the AST rather than imported: ``functools.wraps`` collapses the
        decorator stack, so a wrapped function is indistinguishable from a bare
        one, and the source is what actually answers "is the gate written here".
        """
        source = pathlib.Path(module_path).read_text(encoding="utf-8")
        jobs: dict[str, str] = {}
        for node in ast.parse(source).body:
            if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
                continue
            if not node.name.endswith("_job") or node.name.startswith("_"):
                continue
            body = ast.get_source_segment(source, node) or ""
            if "MAINTENANCE_LOCK" not in body:
                continue
            gated = any(
                ast.unparse(d.func if isinstance(d, ast.Call) else d) == "_with_db_fail_fast_guard"
                for d in node.decorator_list
            )
            jobs[node.name] = "gated" if gated else "ungated"
        return jobs

    def test_no_maintenance_job_is_left_ungated_without_a_stated_reason(self) -> None:
        """모든 ``MAINTENANCE_LOCK`` 잡은 게이트를 갖고, 예외는 사유와 함께 든다."""
        ungated: set[str] = set()
        for module in JOB_MODULES:
            ungated |= {name for name, state in self._maintenance_jobs(module.__file__).items() if state == "ungated"}

        assert ungated == set(DELIBERATELY_UNGATED), (
            f"maintenance jobs without a DB gate: {sorted(ungated)}; "
            "add a gate or state the reason in DELIBERATELY_UNGATED"
        )

    def test_every_stated_exception_is_actually_an_exception(self) -> None:
        """화이트리스트는 예외 목록이 아니라 근거다. 근거 없는 항목은 곧 드리프트다."""
        gated: set[str] = set()
        for module in JOB_MODULES:
            gated |= {name for name, state in self._maintenance_jobs(module.__file__).items() if state == "gated"}

        for name in DELIBERATELY_UNGATED:
            assert name not in gated, f"{name} is listed as ungated but now carries the gate"

    def test_the_gate_is_the_outermost_decorator_on_every_gated_job(self) -> None:
        """게이트는 락과 tenacity보다 먼저 돌아야 한다 — 실제 잡 구조로 확인한다.

        순서는 계약이고, 지금까지는 합성 함수로만 검증했다. 그러면 실제로
        ``@retry``가 게이트보다 바깥인 잡이 있어도 테스트는 통과한다.

        실질 피해가 크지 않은 것은 **메모이즈와 조용한 반환** 때문이다: 게이트가
        예외 없이 None을 돌려주므로 tenacity는 재시도하지 않고, 프로브도 쿨다운당
        한 번이다. 그래도 구조는 틀렸고, 여기서는 틀렸다는 사실 자체를 고정한다.
        게이트를 안쪽에 둔 잡이 **착각 없이** 통과할 수 없어야 한다.
        """
        inverted: list[str] = []
        for module in JOB_MODULES:
            for node in ast.parse(pathlib.Path(module.__file__).read_text(encoding="utf-8")).body:
                if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
                    continue
                decorators = [ast.unparse(d.func if isinstance(d, ast.Call) else d) for d in node.decorator_list]
                if "_with_db_fail_fast_guard" not in decorators:
                    continue
                # Decorators are written top-down but applied bottom-up, so the
                # FIRST one listed is the outermost wrapper. The gate has to run
                # before the lock *and* before tenacity, which means outermost --
                # stating it that way leaves no room to read the order backwards.
                if decorators[0] != "_with_db_fail_fast_guard":
                    inverted.append(node.name)

        assert not inverted, (
            f"the DB gate is not the outermost decorator on {sorted(inverted)}: "
            "it must run before the lock and before tenacity"
        )

    def test_the_exceptions_are_real(self) -> None:
        """제외 이유가 코드와 맞는지 확인한다.

        ``backup_db_job``이 ORM 세션을 쓰는 것으로 바뀐다면 운영 DB 프로브로
        게이트하는 편이 맞을 테니, 그때는 예외 목록이 아니라 게이트를 고쳐야 한다.
        """
        backup_script = pathlib.Path("scripts/maintenance/backup_db.py").read_text(encoding="utf-8")
        assert "sqlite3" in backup_script, "backup_db_job is expected to be a file-level sqlite3 backup"
        assert "SessionLocal" not in backup_script, "a sqlite3 backup should not open an ORM session"

        assert "SessionLocal" not in _job_source(maintenance.__file__, "cleanup_stale_data_job")

    def test_the_gate_lives_in_one_place(self) -> None:
        """잡마다 직접 프로브를 부르면 순서 계약을 잃는다.

        인라인 게이트는 곧바로 봉인의 역사가 된다 — 2026-10-03에 DLQ 잡 세 개가
        정확히 그 형태로 작성돼 있었다. 게이트는 데코레이터 하나에만 존재하고,
        잡은 그것을 선언한다.
        """
        for module in JOB_MODULES:
            source = pathlib.Path(module.__file__).read_text(encoding="utf-8")
            assert "database_reachable" not in source, f"{module.__name__} calls the probe directly"


class TestTheGateDoesNotEraseTheJobSignature:
    """``_with_db_fail_fast_guard``는 잡의 시그니처를 지우지 않는다.

    스케줄러 레지스트리는 잡들을 리스트에 모아 ``add_job``에 넘긴다. 그 리스트의
    원소 타입이 하나라도 ``object``로 무너지면 같은 리스트의 모든 잡이 scoped mypy
    게이트를 함께 실패한다. 실제로 두 가지 철자가 서로 다르게 무너진다:

    * ``@_with_db_fail_fast_guard`` (위치 인자) — 데코레이터가 잡을 받아 그대로 돌려준다.
    * ``@_with_db_fail_fast_guard(urls=...)`` (인자형) — 잡이 없는 상태로 호출돼
      데코레이터를 반환해야 하며, 여기서 반환 타입이 무너지면 잡이 ``object``가 된다.

    런타임 동작은 두 철자가 동일하므로(pytest로는 구분되지 않는다) 정적 계약을
    mypy로 확인한다. 그래야 "게이트가 조용히 타입을 망가뜨리는" 회귀가 게이트에서
    잡힌다.
    """

    #: 스코프 mypy 게이트가 실제로 검사하는 목록과 같은 파일만 대상으로 한다.
    SCOPED_FILE = "src/scheduler/locks.py"
    REGISTRY_FILE = "src/scheduler/registry.py"

    def _mypy(self, source: str) -> list[str]:
        """Type-check a snippet and return only the errors in the snippet itself."""
        target = pathlib.Path("src/scheduler/_type_contract_probe.py")
        previous = target.read_text(encoding="utf-8") if target.exists() else None
        target.write_text(source, encoding="utf-8")
        try:
            result = subprocess.run(
                [sys.executable, "-m", "mypy", str(target)],
                capture_output=True,
                text=True,
                check=False,
            )
        finally:
            if previous is None:
                target.unlink(missing_ok=True)
            else:
                target.write_text(previous, encoding="utf-8")
        return [line for line in result.stdout.splitlines() if line.startswith(str(target)) and "error:" in line]

    def test_both_spellings_keep_the_job_typed(self) -> None:
        """위치형과 인자형 모두 잡을 호출 가능한 함수로 유지한다.

        리스트를 ``Callable``이 아니라 ``object``로 선언하면 잡이 무너지는 것이
        조용히 통과한다. 그래서 여기서는 스케줄러 레지스트리가 실제로 쓰는 타입
        (``Callable[..., object]``)을 요구한다 — 잡 하나라도 ``object``가 되면
        리스트에 넣을 수 없다는 오류가 난다.
        """
        errors = self._mypy(
            "from collections.abc import Callable\n"
            "\n"
            "from src.scheduler.locks import _with_db_fail_fast_guard\n"
            "\n"
            "@_with_db_fail_fast_guard\n"
            "def bare() -> None:\n"
            '    """A job guarded with the bare spelling."""\n'
            "\n"
            "\n"
            "@_with_db_fail_fast_guard(urls=lambda: ('postgresql://probe',))\n"
            "def with_urls() -> None:\n"
            '    """A job guarded with the argument spelling."""\n'
            "\n"
            "\n"
            "registry: list[tuple[Callable[..., object], str, int]] = [\n"
            '    (bare, "bare", 1),\n'
            '    (with_urls, "urls", 2),\n'
            "]\n"
        )
        assert not errors, "the DB gate collapsed a job signature:\n" + "\n".join(errors)

    def test_a_signature_is_carried_through_not_replaced(self) -> None:
        """데코레이터는 매개변수와 반환 타입을 그대로 보존한다.

        ``functools.wraps``가 남기는 ``__wrapped__`` 때문에 런타임에는 이미 성립하지만,
        타입 수준에서 지켜지지 않으면 인자 이름 실수(예: ``limit=``)가 잡힌다.
        """
        errors = self._mypy(
            "from src.scheduler.locks import _with_db_fail_fast_guard\n"
            "\n"
            "@_with_db_fail_fast_guard\n"
            "def takes_arguments(limit: int) -> str:\n"
            '    """A job that takes an argument and returns a value."""\n'
            "    return str(limit)\n"
            "\n"
            "\n"
            "typed: str = takes_arguments(1)\n"
        )
        assert not errors, "the DB gate replaced the job signature:\n" + "\n".join(errors)

    def test_the_argument_spelling_does_not_accept_a_bare_decoration(self) -> None:
        """인자형 오버로드는 ``urls`` 없이 호출할 수 없다.

        ``urls``를 지정한 스펙과 지정하지 않은 스펙을 분리하지 않으면, 데코레이터가
        인자를 받는지 조용히 삼키는 회귀가 타입 검사에 잡히지 않는다.
        """
        errors = self._mypy(
            "from src.scheduler.locks import _with_db_fail_fast_guard\n"
            "\n"
            "@_with_db_fail_fast_guard\n"
            "def missing_urls() -> None:\n"
            '    """The argument spelling without a urls callable."""\n'
        )
        assert not errors, "the argument overload accepted a bare decoration:\n" + "\n".join(errors)

    def test_the_gate_declares_an_overload_for_each_spelling(self) -> None:
        """두 철자가 코드에 실제로 존재하는지 확인한다(오버로드가 사라지는 회귀 방지)."""
        source = pathlib.Path(self.SCOPED_FILE).read_text(encoding="utf-8")
        assert source.count("@overload") >= 2, "the DB gate lost one of its overloads"
        assert "def _with_db_fail_fast_guard[**P, R]" in source, "the overloads must preserve the job signature"

    def test_the_registry_collects_the_jobs_into_one_annotated_list(self) -> None:
        """레지스트리는 잡 목록에 타입을 붙인다.

        타입 없는 리스트는 mypy가 첫 항목의 타입을 그대로 전파하므로, 인자형 게이트
        하나가 ``object``로 무너지면 같은 리스트의 나머지 잡이 함께 실패한다.
        """
        registry = pathlib.Path(self.REGISTRY_FILE).read_text(encoding="utf-8")
        assert "tier2_jobs" in registry, "the tier-2 job list is gone"
        assert "_with_db_fail_fast_guard(urls=" in pathlib.Path(maintenance.__file__).read_text(encoding="utf-8"), (
            "no job uses the argument spelling, so the overload has no production caller to protect"
        )
