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
    GameEvent,
    GamePlayByPlay,
    GameValidationMetrics,
)
from src.services.relay_recovery_engine import (
    RelayCheckpointRecord,
    SealedSnapshotRelayPipeline,
    compute_domain_state_hash,
    init_ephemeral_database,
    load_sealed_snapshots,
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
    empty_naver: bool = False,
    no_verify_checksums: bool = False,
    lock_dir: Path | None = None,
    kbo_fixture: Path = KBO_FIXTURE,
    naver_fixture: Path = NAVER_FIXTURE,
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
        str(kbo_fixture),
        "--naver-fixture",
        str(naver_fixture),
    ]
    if crash_point:
        cmd.extend(["--crash-point", crash_point])
    if apply_correction:
        cmd.append("--apply-correction")
    if empty_naver:
        cmd.append("--empty-naver")
    if no_verify_checksums:
        cmd.append("--no-verify-checksums")

    env = dict(sys.modules["os"].environ)
    env["KBO_SEALED_REPLAY_OFFLINE"] = "1"
    if lock_dir:
        env["KBO_LOCK_DIR"] = str(lock_dir)

    return subprocess.run(cmd, capture_output=True, text=True, env=env, check=False)


def test_protected_db_safety() -> None:
    """Verify that pointing the recovery engine to protected kbo_dev.db fails closed."""
    with pytest.raises(ValueError, match="Forbidden operation"):
        init_ephemeral_database("/path/to/data/kbo_dev.db")


def test_zero_network_invariance(ephemeral_db: Path, lock_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify that the sealed pipeline executes with zero network calls in the current process."""

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
    assert result["pbp_count"] == 47
    assert result["source_used"] == "dual_canonical"


def test_load_time_checksum_verification_failure(tmp_path: Path) -> None:
    """Verify that tampered fixture files trigger a checksum verification failure."""
    tampered_kbo = tmp_path / "tampered_kbo.json"
    tampered_kbo.write_text(json.dumps([{"text": "tampered"}]), encoding="utf-8")

    with pytest.raises(ValueError, match="KBO fixture checksum mismatch"):
        load_sealed_snapshots(tampered_kbo, NAVER_FIXTURE, verify_checksums=True)


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
        val = session.query(GameValidationMetrics).filter(GameValidationMetrics.game_id == TARGET_GAME_ID).first()

        assert len(events) == 5
        assert len(pbps) == 47  # Production Naver parser extracts 47 PBP pitch rows
        assert [e.event_seq for e in events] == [1, 2, 3, 4, 5]
        assert [e.outs for e in events] == [0, 1, 2, 2, 3]
        assert val is not None
        assert val.source_used == "dual_canonical"
        assert len(checkpoints) >= 5


def test_dual_source_and_negative_control(ephemeral_db: Path, lock_dir: Path) -> None:
    """Verify dual source behavior and negative control (empty Naver)."""
    # Negative control: empty Naver -> kbo_single
    res_neg = _run_worker_subprocess(ephemeral_db, empty_naver=True, lock_dir=lock_dir)
    assert res_neg.returncode == 0
    neg_data = json.loads(res_neg.stdout)
    assert neg_data["source_used"] == "kbo_single"
    assert neg_data["events_count"] == 5
    assert neg_data["pbp_count"] == 5

    engine = create_engine(f"sqlite:///{ephemeral_db}")
    with sessionmaker(bind=engine)() as session:
        val = session.query(GameValidationMetrics).filter(GameValidationMetrics.game_id == TARGET_GAME_ID).first()
        assert val is not None
        assert val.source_used == "kbo_single"


def test_cp1_crash_after_fetch_and_restart(ephemeral_db: Path, lock_dir: Path) -> None:
    """Crash Point 1: Crash immediately after snapshot load, then restart."""
    res1 = _run_worker_subprocess(ephemeral_db, crash_point="CP1_FETCH_COMPLETE", lock_dir=lock_dir)
    assert res1.returncode == 137  # Verified hard crash

    res2 = _run_worker_subprocess(ephemeral_db, lock_dir=lock_dir)
    assert res2.returncode == 0

    engine = create_engine(f"sqlite:///{ephemeral_db}")
    with sessionmaker(bind=engine)() as session:
        assert session.query(GameEvent).filter(GameEvent.game_id == TARGET_GAME_ID).count() == 5
        assert session.query(GamePlayByPlay).filter(GamePlayByPlay.game_id == TARGET_GAME_ID).count() == 47


def test_cp2_crash_during_normalization_and_restart(ephemeral_db: Path, lock_dir: Path) -> None:
    """Crash Point 2: Crash mid-way through normalization, then restart."""
    res1 = _run_worker_subprocess(ephemeral_db, crash_point="CP2_DURING_NORMALIZATION", lock_dir=lock_dir)
    assert res1.returncode == 137

    res2 = _run_worker_subprocess(ephemeral_db, lock_dir=lock_dir)
    assert res2.returncode == 0

    engine = create_engine(f"sqlite:///{ephemeral_db}")
    with sessionmaker(bind=engine)() as session:
        assert session.query(GameEvent).filter(GameEvent.game_id == TARGET_GAME_ID).count() == 5
        assert session.query(GamePlayByPlay).filter(GamePlayByPlay.game_id == TARGET_GAME_ID).count() == 47


def test_cp3_crash_before_deduplication_and_restart(ephemeral_db: Path, lock_dir: Path) -> None:
    """Crash Point 3: Crash after normalization before deduplication merge, then restart."""
    res1 = _run_worker_subprocess(ephemeral_db, crash_point="CP3_BEFORE_DEDUPLICATION_MERGE", lock_dir=lock_dir)
    assert res1.returncode == 137

    res2 = _run_worker_subprocess(ephemeral_db, lock_dir=lock_dir)
    assert res2.returncode == 0

    engine = create_engine(f"sqlite:///{ephemeral_db}")
    with sessionmaker(bind=engine)() as session:
        assert session.query(GameEvent).filter(GameEvent.game_id == TARGET_GAME_ID).count() == 5
        assert session.query(GamePlayByPlay).filter(GamePlayByPlay.game_id == TARGET_GAME_ID).count() == 47


def test_cp4_crash_in_transaction_during_save_and_restart(ephemeral_db: Path, lock_dir: Path) -> None:
    """Crash Point 4: Crash inside save transaction after flush, before commit.

    Verifies atomicity: uncommitted partial transaction is rolled back by SQLite, and recovery
    inserts full batch cleanly with 0 partial batch.
    """
    res1 = _run_worker_subprocess(ephemeral_db, crash_point="CP4_IN_TRANSACTION_DURING_SAVE", lock_dir=lock_dir)
    assert res1.returncode == 137

    # Check that mid-transaction uncommitted rows were rolled back
    engine = create_engine(f"sqlite:///{ephemeral_db}")
    with sessionmaker(bind=engine)() as session:
        # Tables exist, but zero uncommitted events remain
        assert session.query(GameEvent).filter(GameEvent.game_id == TARGET_GAME_ID).count() == 0
        assert session.query(GamePlayByPlay).filter(GamePlayByPlay.game_id == TARGET_GAME_ID).count() == 0

    res2 = _run_worker_subprocess(ephemeral_db, lock_dir=lock_dir)
    assert res2.returncode == 0

    with sessionmaker(bind=engine)() as session:
        assert session.query(GameEvent).filter(GameEvent.game_id == TARGET_GAME_ID).count() == 5
        assert session.query(GamePlayByPlay).filter(GamePlayByPlay.game_id == TARGET_GAME_ID).count() == 47


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
        assert session.query(GamePlayByPlay).filter(GamePlayByPlay.game_id == TARGET_GAME_ID).count() == 47

    res2 = _run_worker_subprocess(ephemeral_db, lock_dir=lock_dir)
    assert res2.returncode == 0

    # No duplicate rows inserted!
    with sessionmaker(bind=engine)() as session:
        assert session.query(GameEvent).filter(GameEvent.game_id == TARGET_GAME_ID).count() == 5
        assert session.query(GamePlayByPlay).filter(GamePlayByPlay.game_id == TARGET_GAME_ID).count() == 47


def test_cp6_crash_during_checkpoint_record_and_restart(ephemeral_db: Path, lock_dir: Path) -> None:
    """Crash Point 6: Crash inside checkpoint transaction after flush, before commit."""
    res1 = _run_worker_subprocess(ephemeral_db, crash_point="CP6_DURING_CHECKPOINT_RECORD", lock_dir=lock_dir)
    assert res1.returncode == 137

    res2 = _run_worker_subprocess(ephemeral_db, lock_dir=lock_dir)
    assert res2.returncode == 0

    engine = create_engine(f"sqlite:///{ephemeral_db}")
    with sessionmaker(bind=engine)() as session:
        assert session.query(GameEvent).filter(GameEvent.game_id == TARGET_GAME_ID).count() == 5
        assert session.query(GamePlayByPlay).filter(GamePlayByPlay.game_id == TARGET_GAME_ID).count() == 47
        checkpoints = session.query(RelayCheckpointRecord).filter(RelayCheckpointRecord.game_id == TARGET_GAME_ID).all()
        assert len(checkpoints) >= 5


def test_cp7_crash_during_correction_update_and_restart(ephemeral_db: Path, lock_dir: Path) -> None:
    """Crash Point 7: Crash during in-place correction transaction after flush, before commit."""
    # First establish clean base
    res_base = _run_worker_subprocess(ephemeral_db, lock_dir=lock_dir)
    assert res_base.returncode == 0

    # Simulate crash during correction transaction
    res1 = _run_worker_subprocess(
        ephemeral_db,
        crash_point="CP7_DURING_CORRECTION_UPDATE",
        apply_correction=True,
        lock_dir=lock_dir,
    )
    assert res1.returncode == 137

    # Verify uncommitted correction was rolled back
    engine = create_engine(f"sqlite:///{ephemeral_db}")
    with sessionmaker(bind=engine)() as session:
        ev3_before = (
            session.query(GameEvent).filter(GameEvent.game_id == TARGET_GAME_ID, GameEvent.event_seq == 3).first()
        )
        assert ev3_before is not None
        assert "[CORRECTED]" not in (ev3_before.description or "")

    # Restart with correction enabled
    res2 = _run_worker_subprocess(ephemeral_db, apply_correction=True, lock_dir=lock_dir)
    assert res2.returncode == 0

    with sessionmaker(bind=engine)() as session:
        assert session.query(GameEvent).filter(GameEvent.game_id == TARGET_GAME_ID).count() == 5
        ev3 = session.query(GameEvent).filter(GameEvent.game_id == TARGET_GAME_ID, GameEvent.event_seq == 3).first()
        assert ev3 is not None
        assert "[CORRECTED]" in ev3.description
        assert "투수 땅볼 (정정)" in ev3.result_code


def test_correction_idempotency_and_zero_duplicate_tags(ephemeral_db: Path, lock_dir: Path) -> None:
    """Verify that applying the same revision repeatedly results in zero subsequent mutations."""
    # 1. Base run with correction applied
    res1 = _run_worker_subprocess(ephemeral_db, apply_correction=True, lock_dir=lock_dir)
    assert res1.returncode == 0

    engine = create_engine(f"sqlite:///{ephemeral_db}")
    with sessionmaker(bind=engine)() as session:
        ev3_first = (
            session.query(GameEvent).filter(GameEvent.game_id == TARGET_GAME_ID, GameEvent.event_seq == 3).first()
        )
        assert ev3_first is not None
        desc_first = ev3_first.description
        assert desc_first.count("[CORRECTED]") == 1

    # 2. Run correction again on the same DB
    pipeline = SealedSnapshotRelayPipeline(
        game_id=TARGET_GAME_ID,
        db_path_or_url=str(ephemeral_db),
        kbo_fixture_path=KBO_FIXTURE,
        naver_fixture_path=NAVER_FIXTURE,
        lock_dir=lock_dir,
    )
    repeat_res = pipeline.apply_event_correction()
    assert repeat_res["already_applied"] is True
    assert repeat_res["mutations"] == 0

    with sessionmaker(bind=engine)() as session:
        ev3_second = (
            session.query(GameEvent).filter(GameEvent.game_id == TARGET_GAME_ID, GameEvent.event_seq == 3).first()
        )
        assert ev3_second is not None
        # Must still only have 1 [CORRECTED] tag, never duplicated!
        assert ev3_second.description == desc_first
        assert ev3_second.description.count("[CORRECTED]") == 1


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


def test_deep_domain_hash_tamper_detection(ephemeral_db: Path, lock_dir: Path) -> None:
    """Verify that deep domain hash computation detects any tampering in substantive fields."""
    _run_worker_subprocess(ephemeral_db, lock_dir=lock_dir)

    engine = create_engine(f"sqlite:///{ephemeral_db}")
    with sessionmaker(bind=engine)() as session:
        golden_hash = compute_domain_state_hash(session, TARGET_GAME_ID)

        # Mutate outs in an event
        ev1 = session.query(GameEvent).filter(GameEvent.game_id == TARGET_GAME_ID, GameEvent.event_seq == 1).first()
        assert ev1 is not None
        ev1.outs = 2
        session.commit()

        tampered_hash = compute_domain_state_hash(session, TARGET_GAME_ID)
        assert golden_hash != tampered_hash


def test_convergence_against_golden_baseline(tmp_path: Path) -> None:
    """Verify that state across all crash recovery scenarios converges bit/row-identically to golden baseline."""
    baseline_db = tmp_path / "baseline.db"
    res_base = _run_worker_subprocess(baseline_db)
    assert res_base.returncode == 0

    crash_db = tmp_path / "recovered.db"
    # Crash at CP4, then recover
    _run_worker_subprocess(crash_db, crash_point="CP4_IN_TRANSACTION_DURING_SAVE")
    res_rec = _run_worker_subprocess(crash_db)
    assert res_rec.returncode == 0

    b_engine = create_engine(f"sqlite:///{baseline_db}")
    c_engine = create_engine(f"sqlite:///{crash_db}")

    with sessionmaker(bind=b_engine)() as b_session, sessionmaker(bind=c_engine)() as c_session:
        b_hash = compute_domain_state_hash(b_session, TARGET_GAME_ID)
        c_hash = compute_domain_state_hash(c_session, TARGET_GAME_ID)
        assert b_hash == c_hash

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
            assert be.home_score == ce.home_score
            assert be.away_score == ce.away_score
            assert be.bases_before == ce.bases_before
            assert be.bases_after == ce.bases_after
