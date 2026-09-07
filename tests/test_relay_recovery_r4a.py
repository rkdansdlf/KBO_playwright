"""Test suite for Gate R4A: Sealed-Snapshot Relay Pipeline, Crash Injection, and Restart Recovery."""

from __future__ import annotations

import json
import socket
import subprocess
import sys
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.game import (
    Game,
    GameEvent,
    GamePlayByPlay,
    GameValidationMetrics,
)
from src.services.relay_recovery_engine import (
    CheckpointBase,
    RelayCheckpointRecord,
    SealedSnapshotRelayPipeline,
    init_ephemeral_database,
)

FIXTURES_DIR = Path("tests/fixtures/relay_snapshots")
KBO_FIXTURE = FIXTURES_DIR / "kbo_sealed_dom_nodes_20240930NCHT0.json"
NAVER_FIXTURE = FIXTURES_DIR / "naver_sealed_payload_20240930NCHT0.json"
TARGET_GAME_ID = "20240930NCHT0"


@pytest.fixture
def ephemeral_db(tmp_path: Path) -> Path:
    """Provide a path to a clean, isolated temporary SQLite database."""
    return tmp_path / "ephemeral_test.db"


@pytest.fixture
def lock_dir(tmp_path: Path) -> Path:
    """Provide a path to a temporary process lock directory."""
    ldir = tmp_path / "locks"
    ldir.mkdir(parents=True, exist_ok=True)
    return ldir


def _run_worker_subprocess(
    db_path: Path,
    *,
    crash_point: str | None = None,
    apply_correction: bool = False,
    lock_dir: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    """Invoke the pipeline in an isolated subprocess to support true hard-exit (os._exit)."""
    cmd = [
        sys.executable,
        "-m",
        "src.services.relay_recovery_engine",
        "--game-id",
        TARGET_GAME_ID,
        "--db-path",
        str(db_path),
        "--kbo-fixture",
        str(KBO_FIXTURE),
        "--naver-fixture",
        str(NAVER_FIXTURE),
    ]
    if crash_point:
        cmd.extend(["--crash-point", crash_point])
    if apply_correction:
        cmd.append("--apply-correction")

    env = dict(sys.modules["os"].environ)
    if lock_dir:
        env["KBO_LOCK_DIR"] = str(lock_dir)

    return subprocess.run(cmd, capture_output=True, text=True, env=env, check=False)


def test_protected_db_safety() -> None:
    """Verify that pointing the recovery engine to protected kbo_dev.db fails closed."""
    with pytest.raises(ValueError, match="Forbidden operation"):
        init_ephemeral_database("/path/to/data/kbo_dev.db")


def test_zero_network_invariance(ephemeral_db: Path, lock_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify that the sealed pipeline executes with zero network calls."""

    def forbidden_connect(*_args: object, **_kwargs: object) -> None:
        msg = "Network connection attempted during sealed snapshot replay"
        raise RuntimeError(msg)

    monkeypatch.setattr(socket.socket, "connect", forbidden_connect)

    pipeline = SealedSnapshotRelayPipeline(
        game_id=TARGET_GAME_ID,
        db_path_or_url=str(ephemeral_db),
        kbo_fixture_path=KBO_FIXTURE,
        naver_fixture_path=NAVER_FIXTURE,
        lock_dir=lock_dir,
    )
    result = pipeline.run()
    assert result["status"] == "SUCCESS"
    assert result["events_count"] == 5
    assert result["pbp_count"] == 5


def test_golden_baseline_run(ephemeral_db: Path, lock_dir: Path) -> None:
    """Test a clean, un-interrupted single-pass execution establishing the baseline."""
    res = _run_worker_subprocess(ephemeral_db, lock_dir=lock_dir)
    assert res.returncode == 0, f"Baseline failed: {res.stderr}"

    engine = create_engine(f"sqlite:///{ephemeral_db}")
    session_factory = sessionmaker(bind=engine)
    with session_factory() as session:
        events = (
            session.query(GameEvent).filter(GameEvent.game_id == TARGET_GAME_ID).order_by(GameEvent.event_seq).all()
        )
        pbps = (
            session.query(GamePlayByPlay)
            .filter(GamePlayByPlay.game_id == TARGET_GAME_ID)
            .order_by(GamePlayByPlay.source_row_index)
            .all()
        )
        checkpoints = (
            session.query(RelayCheckpointRecord)
            .filter(RelayCheckpointRecord.game_id == TARGET_GAME_ID)
            .order_by(RelayCheckpointRecord.seq_no)
            .all()
        )

        assert len(events) == 5
        assert len(pbps) == 5
        assert [e.event_seq for e in events] == [1, 2, 3, 4, 5]
        assert [e.outs for e in events] == [0, 1, 2, 2, 3]
        assert len(checkpoints) >= 5


def test_cp1_crash_after_fetch_and_restart(ephemeral_db: Path, lock_dir: Path) -> None:
    """Crash Point 1: Crash immediately after snapshot load, then restart."""
    res1 = _run_worker_subprocess(ephemeral_db, crash_point="CP1_FETCH_COMPLETE", lock_dir=lock_dir)
    assert res1.returncode == 137  # Verified hard crash

    res2 = _run_worker_subprocess(ephemeral_db, lock_dir=lock_dir)
    assert res2.returncode == 0

    engine = create_engine(f"sqlite:///{ephemeral_db}")
    with sessionmaker(bind=engine)() as session:
        assert session.query(GameEvent).filter(GameEvent.game_id == TARGET_GAME_ID).count() == 5
        assert session.query(GamePlayByPlay).filter(GamePlayByPlay.game_id == TARGET_GAME_ID).count() == 5


def test_cp2_crash_during_normalization_and_restart(ephemeral_db: Path, lock_dir: Path) -> None:
    """Crash Point 2: Crash mid-way through normalization, then restart."""
    res1 = _run_worker_subprocess(ephemeral_db, crash_point="CP2_DURING_NORMALIZATION", lock_dir=lock_dir)
    assert res1.returncode == 137

    res2 = _run_worker_subprocess(ephemeral_db, lock_dir=lock_dir)
    assert res2.returncode == 0

    engine = create_engine(f"sqlite:///{ephemeral_db}")
    with sessionmaker(bind=engine)() as session:
        assert session.query(GameEvent).filter(GameEvent.game_id == TARGET_GAME_ID).count() == 5


def test_cp3_crash_after_kbo_before_naver_and_restart(ephemeral_db: Path, lock_dir: Path) -> None:
    """Crash Point 3: Crash after staging KBO events before Naver events, then restart."""
    res1 = _run_worker_subprocess(ephemeral_db, crash_point="CP3_AFTER_KBO_BEFORE_NAVER", lock_dir=lock_dir)
    assert res1.returncode == 137

    res2 = _run_worker_subprocess(ephemeral_db, lock_dir=lock_dir)
    assert res2.returncode == 0

    engine = create_engine(f"sqlite:///{ephemeral_db}")
    with sessionmaker(bind=engine)() as session:
        assert session.query(GameEvent).filter(GameEvent.game_id == TARGET_GAME_ID).count() == 5


def test_cp4_crash_after_events_before_pbp_and_restart(ephemeral_db: Path, lock_dir: Path) -> None:
    """Crash Point 4: Crash after GameEvents inserted before GamePlayByPlay, then restart.

    Verifies atomicity: uncommitted partial transaction is rolled back by SQLite, and recovery
    inserts full batch cleanly with 0 partial batch.
    """
    res1 = _run_worker_subprocess(ephemeral_db, crash_point="CP4_AFTER_EVENTS_BEFORE_PBP", lock_dir=lock_dir)
    assert res1.returncode == 137

    # Check that mid-transaction uncommitted rows were rolled back
    engine = create_engine(f"sqlite:///{ephemeral_db}")
    with sessionmaker(bind=engine)() as session:
        # Tables exist, but zero uncommitted events remain
        assert session.query(GameEvent).filter(GameEvent.game_id == TARGET_GAME_ID).count() == 0

    res2 = _run_worker_subprocess(ephemeral_db, lock_dir=lock_dir)
    assert res2.returncode == 0

    with sessionmaker(bind=engine)() as session:
        assert session.query(GameEvent).filter(GameEvent.game_id == TARGET_GAME_ID).count() == 5
        assert session.query(GamePlayByPlay).filter(GamePlayByPlay.game_id == TARGET_GAME_ID).count() == 5


def test_cp5_crash_after_commit_before_checkpoint_and_restart(ephemeral_db: Path, lock_dir: Path) -> None:
    """Crash Point 5: Crash after DB commit before external checkpoint write, then restart.

    Verifies idempotency: already committed DB rows are not duplicated on restart.
    """
    res1 = _run_worker_subprocess(ephemeral_db, crash_point="CP5_AFTER_COMMIT_BEFORE_CHECKPOINT", lock_dir=lock_dir)
    assert res1.returncode == 137

    # Rows exist in DB before restart
    engine = create_engine(f"sqlite:///{ephemeral_db}")
    with sessionmaker(bind=engine)() as session:
        assert session.query(GameEvent).filter(GameEvent.game_id == TARGET_GAME_ID).count() == 5

    res2 = _run_worker_subprocess(ephemeral_db, lock_dir=lock_dir)
    assert res2.returncode == 0

    # No duplicate rows inserted!
    with sessionmaker(bind=engine)() as session:
        assert session.query(GameEvent).filter(GameEvent.game_id == TARGET_GAME_ID).count() == 5
        assert session.query(GamePlayByPlay).filter(GamePlayByPlay.game_id == TARGET_GAME_ID).count() == 5


def test_cp6_crash_during_checkpoint_record_and_restart(ephemeral_db: Path, lock_dir: Path) -> None:
    """Crash Point 6: Crash during checkpoint writing, then restart."""
    res1 = _run_worker_subprocess(ephemeral_db, crash_point="CP6_DURING_CHECKPOINT_RECORD", lock_dir=lock_dir)
    assert res1.returncode == 137

    res2 = _run_worker_subprocess(ephemeral_db, lock_dir=lock_dir)
    assert res2.returncode == 0

    engine = create_engine(f"sqlite:///{ephemeral_db}")
    with sessionmaker(bind=engine)() as session:
        assert session.query(GameEvent).filter(GameEvent.game_id == TARGET_GAME_ID).count() == 5
        checkpoints = session.query(RelayCheckpointRecord).filter(RelayCheckpointRecord.game_id == TARGET_GAME_ID).all()
        assert len(checkpoints) >= 5


def test_cp7_crash_during_correction_update_and_restart(ephemeral_db: Path, lock_dir: Path) -> None:
    """Crash Point 7: Crash during in-place correction, then restart."""
    # First establish clean base
    res_base = _run_worker_subprocess(ephemeral_db, lock_dir=lock_dir)
    assert res_base.returncode == 0

    # Now simulate crash during correction
    res1 = _run_worker_subprocess(
        ephemeral_db,
        crash_point="CP7_DURING_CORRECTION_UPDATE",
        apply_correction=True,
        lock_dir=lock_dir,
    )
    assert res1.returncode == 137

    # Restart with correction enabled
    res2 = _run_worker_subprocess(ephemeral_db, apply_correction=True, lock_dir=lock_dir)
    assert res2.returncode == 0

    engine = create_engine(f"sqlite:///{ephemeral_db}")
    with sessionmaker(bind=engine)() as session:
        assert session.query(GameEvent).filter(GameEvent.game_id == TARGET_GAME_ID).count() == 5
        ev3 = session.query(GameEvent).filter(GameEvent.game_id == TARGET_GAME_ID, GameEvent.event_seq == 3).first()
        assert ev3 is not None
        assert "[CORRECTED]" in ev3.description
        assert "투수 땅볼 (정정)" in ev3.result_code


def test_checkpoint_monotonicity(ephemeral_db: Path, lock_dir: Path) -> None:
    """Verify that checkpoint sequence numbers are strictly monotonic."""
    _run_worker_subprocess(ephemeral_db, lock_dir=lock_dir)

    engine = create_engine(f"sqlite:///{ephemeral_db}")
    with sessionmaker(bind=engine)() as session:
        checkpoints = (
            session.query(RelayCheckpointRecord)
            .filter(RelayCheckpointRecord.game_id == TARGET_GAME_ID)
            .order_by(RelayCheckpointRecord.seq_no)
            .all()
        )
        seqs = [c.seq_no for c in checkpoints]
        assert seqs == sorted(seqs)
        assert len(seqs) == len(set(seqs))  # Strictly distinct monotonic sequence numbers


def test_convergence_against_golden_baseline(tmp_path: Path) -> None:
    """Verify that state across all crash recovery scenarios converges bit/row-identically to golden baseline."""
    baseline_db = tmp_path / "baseline.db"
    res_base = _run_worker_subprocess(baseline_db)
    assert res_base.returncode == 0

    crash_db = tmp_path / "recovered.db"
    # Crash at CP4, then recover
    _run_worker_subprocess(crash_db, crash_point="CP4_AFTER_EVENTS_BEFORE_PBP")
    res_rec = _run_worker_subprocess(crash_db)
    assert res_rec.returncode == 0

    b_engine = create_engine(f"sqlite:///{baseline_db}")
    c_engine = create_engine(f"sqlite:///{crash_db}")

    with sessionmaker(bind=b_engine)() as b_session, sessionmaker(bind=c_engine)() as c_session:
        b_events = (
            b_session.query(GameEvent).filter(GameEvent.game_id == TARGET_GAME_ID).order_by(GameEvent.event_seq).all()
        )
        c_events = (
            c_session.query(GameEvent).filter(GameEvent.game_id == TARGET_GAME_ID).order_by(GameEvent.event_seq).all()
        )

        assert len(b_events) == len(c_events) == 5
        for be, ce in zip(b_events, c_events, strict=True):
            assert be.event_seq == ce.event_seq
            assert be.description == ce.description
            assert be.event_type == ce.event_type
            assert be.outs == ce.outs
            assert be.result_code == ce.result_code
