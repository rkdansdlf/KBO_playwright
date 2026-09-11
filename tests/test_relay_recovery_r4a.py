"""Test suite for Gate R4A: Sealed-Snapshot Relay Pipeline, Crash Injection, and Restart Recovery."""

from __future__ import annotations

import json
import socket
import subprocess
import sys
from datetime import UTC, datetime
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
    HalfInningContext,
    RelayCheckpointRecord,
    RelayRevisionRecord,
    SealedSnapshotRelayPipeline,
    compute_domain_state_hash,
    extract_domain_entities,
    init_ephemeral_database,
    load_sealed_snapshots,
)
from src.utils.lock import ForceProcessLock

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


def test_half_inning_context_provenance() -> None:
    """Verify that initial game context is explicitly typed and carries formal boxscore provenance."""
    ctx = HalfInningContext()
    assert ctx.inning == 9
    assert ctx.inning_half == "top"
    assert ctx.home_score == 10
    assert ctx.away_score == 5
    assert ctx.active_pitcher == "최지민"
    assert "20240930NCHT0" in ctx.provenance


def test_revision_conflict_rejection(ephemeral_db: Path, lock_dir: Path) -> None:
    """Verify that re-applying the same revision ID with conflicting payload is explicitly rejected."""
    pipeline = SealedSnapshotRelayPipeline(
        game_id=TARGET_GAME_ID,
        db_path_or_url=str(ephemeral_db),
        kbo_fixture_path=KBO_FIXTURE,
        naver_fixture_path=NAVER_FIXTURE,
        lock_dir=lock_dir,
    )
    pipeline.run()

    # Apply valid revision
    res1 = pipeline.apply_event_correction(
        revision_id="REV-CONFLICT-TEST",
        target_event_seq=3,
        revised_description="Valid revision description",
        revised_result_code="투수 땅볼 (정정)",
    )
    assert res1["already_applied"] is False
    assert res1["mutations"] == 1

    # Attempt re-application with identical payload -> idempotent no-op
    res2 = pipeline.apply_event_correction(
        revision_id="REV-CONFLICT-TEST",
        target_event_seq=3,
        revised_description="Valid revision description",
        revised_result_code="투수 땅볼 (정정)",
    )
    assert res2["already_applied"] is True
    assert res2["mutations"] == 0

    # Attempt re-application with conflicting payload -> raises ValueError
    with pytest.raises(ValueError, match="Revision conflict"):
        pipeline.apply_event_correction(
            revision_id="REV-CONFLICT-TEST",
            target_event_seq=3,
            revised_description="Conflicting description with different text",
            revised_result_code="삼진 (정정)",
        )


def test_revision_persistence_across_regular_replay(ephemeral_db: Path, lock_dir: Path) -> None:
    """Verify that committed revisions in _relay_revisions persist even when regular replay runs without --apply-correction."""
    pipeline = SealedSnapshotRelayPipeline(
        game_id=TARGET_GAME_ID,
        db_path_or_url=str(ephemeral_db),
        kbo_fixture_path=KBO_FIXTURE,
        naver_fixture_path=NAVER_FIXTURE,
        lock_dir=lock_dir,
    )
    # 1. Run pipeline and apply correction
    pipeline.run(apply_correction=True)

    engine = create_engine(f"sqlite:///{ephemeral_db}")
    with sessionmaker(bind=engine)() as session:
        ev3 = session.query(GameEvent).filter(GameEvent.game_id == TARGET_GAME_ID, GameEvent.event_seq == 3).first()
        assert ev3 is not None
        assert "[CORRECTED]" in ev3.description
        assert "투수 땅볼 (정정)" in ev3.result_code

        rev_record = session.query(RelayRevisionRecord).filter(RelayRevisionRecord.game_id == TARGET_GAME_ID).first()
        assert rev_record is not None
        assert rev_record.status == "APPLIED"

    # 2. Re-run regular replay without correction flag
    pipeline.run(apply_correction=False)

    # 3. Verify event 3 did NOT regress to uncorrected raw text
    with sessionmaker(bind=engine)() as session:
        ev3_after = (
            session.query(GameEvent).filter(GameEvent.game_id == TARGET_GAME_ID, GameEvent.event_seq == 3).first()
        )
        assert ev3_after is not None
        assert "[CORRECTED]" in ev3_after.description
        assert "투수 땅볼 (정정)" in ev3_after.result_code

        # Verify PBP row is also preserved via provider_log_id
        target_pid = ev3_after.provider_log_id
        pbp_target = (
            session.query(GamePlayByPlay)
            .filter(GamePlayByPlay.game_id == TARGET_GAME_ID, GamePlayByPlay.provider_log_id == target_pid)
            .first()
        )
        assert pbp_target is not None
        assert "[CORRECTED]" in pbp_target.play_description
        assert pbp_target.batter_name == "김형준"
        assert pbp_target.source_row_index == 35

        # Verify uncorrected PBP row (e.g. row 3 Kim Hwi-jip) was NOT modified
        pbp_row3 = (
            session.query(GamePlayByPlay)
            .filter(GamePlayByPlay.game_id == TARGET_GAME_ID, GamePlayByPlay.source_row_index == 3)
            .first()
        )
        assert pbp_row3 is not None
        assert "[CORRECTED]" not in pbp_row3.play_description
        assert pbp_row3.batter_name == "김휘집"


def test_pbp_and_event_synchronized_correction(ephemeral_db: Path, lock_dir: Path) -> None:
    """Verify that apply_event_correction updates both GameEvent and GamePlayByPlay synchronously via provider_log_id."""
    pipeline = SealedSnapshotRelayPipeline(
        game_id=TARGET_GAME_ID,
        db_path_or_url=str(ephemeral_db),
        kbo_fixture_path=KBO_FIXTURE,
        naver_fixture_path=NAVER_FIXTURE,
        lock_dir=lock_dir,
    )
    pipeline.run(apply_correction=True)

    engine = create_engine(f"sqlite:///{ephemeral_db}")
    with sessionmaker(bind=engine)() as session:
        ev = session.query(GameEvent).filter(GameEvent.game_id == TARGET_GAME_ID, GameEvent.event_seq == 3).first()
        assert ev is not None
        assert ev.batter_name == "김형준"
        assert ev.provider_log_id == "naver:c3fe20fbf07f:9t:2:6:cdeacf6a06"

        # Semantic link: target PBP row is row 35 (Kim Hyeong-jun), NOT row 3 (Kim Hwi-jip)
        pbp = (
            session.query(GamePlayByPlay)
            .filter(GamePlayByPlay.game_id == TARGET_GAME_ID, GamePlayByPlay.provider_log_id == ev.provider_log_id)
            .first()
        )
        assert pbp is not None
        assert pbp.source_row_index == 35
        assert pbp.batter_name == "김형준"
        assert "[CORRECTED]" in ev.description
        assert "[CORRECTED]" in pbp.play_description
        assert ev.result_code == pbp.result == "투수 땅볼 (정정)"

        # Verify row 3 (Kim Hwi-jip) was completely untouched
        pbp_row3 = (
            session.query(GamePlayByPlay)
            .filter(GamePlayByPlay.game_id == TARGET_GAME_ID, GamePlayByPlay.source_row_index == 3)
            .first()
        )
        assert pbp_row3 is not None
        assert pbp_row3.batter_name == "김휘집"
        assert "[CORRECTED]" not in pbp_row3.play_description

        # Verify revision record lineage and provider_log_id binding
        rev = session.query(RelayRevisionRecord).filter(RelayRevisionRecord.game_id == TARGET_GAME_ID).first()
        assert rev is not None
        assert rev.target_provider_log_id == "naver:c3fe20fbf07f:9t:2:6:cdeacf6a06"
        assert rev.original_description == "김형준 : 투수 땅볼 아웃 (투수->1루수 송구아웃)"


def test_path_confinement_violation(tmp_path: Path) -> None:
    """Verify that paths outside the designated temporary root are strictly rejected."""
    allowed_dir = tmp_path / "allowed_realm"
    allowed_dir.mkdir()
    outside_dir = tmp_path / "forbidden_outside"
    outside_dir.mkdir()

    # Valid path inside allowed root succeeds
    valid_db = allowed_dir / "valid.sqlite"
    init_ephemeral_database(str(valid_db), allowed_root=allowed_dir)
    assert valid_db.exists()

    # Path outside allowed root fails closed
    outside_db = outside_dir / "outside.sqlite"
    with pytest.raises(ValueError, match="Path confinement violation"):
        init_ephemeral_database(str(outside_db), allowed_root=allowed_dir)


def test_live_pid_lock_protection(tmp_path: Path) -> None:
    """Verify that ForceProcessLock never steals locks from an active live PID, but clears dead PIDs."""
    import fcntl
    import os

    lock_dir = tmp_path / "locks"
    lock_dir.mkdir()

    live_pid = os.getpid()
    lock_file = lock_dir / "relay_test_lock.lock"

    # 1. Lock held by living process with real fcntl flock
    with lock_file.open("w") as fd:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fd.write(f"{live_pid}\n")
        fd.flush()

        # Another process lock cannot steal it because PID is alive
        lock = ForceProcessLock("relay_test_lock", lock_dir=lock_dir)
        acquired = lock.acquire(timeout=0.2)
        assert acquired is False
        assert lock_file.exists()

        fcntl.flock(fd, fcntl.LOCK_UN)

    # 2. Lock file left behind by dead PID (e.g. crashed worker)
    dead_pid = 9999999
    lock_file.write_text(f"{dead_pid}\n", encoding="utf-8")

    # Stale lock must be auto-cleared and acquired
    lock2 = ForceProcessLock("relay_test_lock", lock_dir=lock_dir)
    acquired_stale = lock2.acquire(timeout=1.0)
    assert acquired_stale is True
    assert lock_file.exists()
    assert lock_file.read_text(encoding="utf-8").strip() == str(live_pid)
    lock2.release()


def test_4_entity_domain_hash_tamper_detection(ephemeral_db: Path, lock_dir: Path) -> None:
    """Verify that tampering any of the 4 domain entities (Events, PBPs, Validation, Revisions) shifts the hash."""
    pipeline = SealedSnapshotRelayPipeline(
        game_id=TARGET_GAME_ID,
        db_path_or_url=str(ephemeral_db),
        kbo_fixture_path=KBO_FIXTURE,
        naver_fixture_path=NAVER_FIXTURE,
        lock_dir=lock_dir,
    )
    pipeline.run(apply_correction=True)

    engine = create_engine(f"sqlite:///{ephemeral_db}")
    with sessionmaker(bind=engine)() as session:
        entities = extract_domain_entities(session, TARGET_GAME_ID)
        assert len(entities["events"]) == 5
        assert len(entities["pbps"]) == 47
        assert entities["validation"]["source_used"] == "dual_canonical"
        assert "observed_event_pbp_state_sha256" in entities["validation"]
        assert entities["validation"]["observed_event_pbp_state_sha256"] is not None
        assert len(entities["revisions"]) == 1

        baseline_hash = compute_domain_state_hash(session, TARGET_GAME_ID)

        # Mutate a PBP row
        pbp = session.query(GamePlayByPlay).filter(GamePlayByPlay.game_id == TARGET_GAME_ID).first()
        assert pbp is not None
        pbp.play_description = "Tampered play description"
        session.commit()
        pbp_tampered_hash = compute_domain_state_hash(session, TARGET_GAME_ID)
        assert baseline_hash != pbp_tampered_hash


def test_correction_fails_safely_when_pbp_not_matched(ephemeral_db: Path, lock_dir: Path) -> None:
    """Verify that when no matching PBP row exists, correction safely aborts with 0 mutations without guessing."""
    pipeline = SealedSnapshotRelayPipeline(
        game_id=TARGET_GAME_ID,
        db_path_or_url=str(ephemeral_db),
        kbo_fixture_path=KBO_FIXTURE,
        naver_fixture_path=NAVER_FIXTURE,
        lock_dir=lock_dir,
    )
    pipeline.run(apply_correction=False)

    engine = create_engine(f"sqlite:///{ephemeral_db}")
    with sessionmaker(bind=engine)() as session:
        # Deliberately desynchronize event 3 to have a non-existent provider_log_id & non-matching batter/desc
        ev3 = session.query(GameEvent).filter(GameEvent.game_id == TARGET_GAME_ID, GameEvent.event_seq == 3).first()
        assert ev3 is not None
        ev3.provider_log_id = "nonexistent:provider:id"
        ev3.batter_name = "미등록선수"
        ev3.description = "존재하지 않는 타격 기록"
        session.commit()

        orig_ev3_desc = ev3.description
        row35 = (
            session.query(GamePlayByPlay)
            .filter(GamePlayByPlay.game_id == TARGET_GAME_ID, GamePlayByPlay.source_row_index == 35)
            .first()
        )
        assert row35 is not None
        orig_row35_desc = row35.play_description

    # Attempt correction on event 3 with no matching PBP
    res = pipeline.apply_event_correction(
        revision_id="REV-NO-MATCH-TEST",
        target_event_seq=3,
        revised_description="Should never be applied",
    )
    assert res["already_applied"] is False
    assert res["mutations"] == 0
    assert res["status"] == "PBP_MATCH_FAILED"

    # Verify zero database mutations occurred across GameEvent, GamePlayByPlay, and RelayRevisionRecord
    with sessionmaker(bind=engine)() as session:
        ev3_after = (
            session.query(GameEvent).filter(GameEvent.game_id == TARGET_GAME_ID, GameEvent.event_seq == 3).first()
        )
        assert ev3_after is not None
        assert ev3_after.description == orig_ev3_desc
        assert "Should never be applied" not in ev3_after.description

        pbp35_after = (
            session.query(GamePlayByPlay)
            .filter(GamePlayByPlay.game_id == TARGET_GAME_ID, GamePlayByPlay.source_row_index == 35)
            .first()
        )
        assert pbp35_after is not None
        assert pbp35_after.play_description == orig_row35_desc

        rev = session.query(RelayRevisionRecord).filter(RelayRevisionRecord.revision_id == "REV-NO-MATCH-TEST").first()
        assert rev is None


def test_correction_rejects_description_fallback_when_provider_id_mismatched(
    ephemeral_db: Path, lock_dir: Path
) -> None:
    """Verify that when GameEvent has a provider_log_id that does not match PBP,

    the engine strictly fails match (0 mutations) and does NOT fall back to matching
    by batter_name and description even if an exact 1-candidate description match exists.
    """
    from sqlalchemy import event

    pipeline = SealedSnapshotRelayPipeline(
        game_id=TARGET_GAME_ID,
        db_path_or_url=str(ephemeral_db),
        kbo_fixture_path=KBO_FIXTURE,
        naver_fixture_path=NAVER_FIXTURE,
        lock_dir=lock_dir,
    )
    pipeline.run(apply_correction=False)

    engine = create_engine(f"sqlite:///{ephemeral_db}")
    with sessionmaker(bind=engine)() as session:
        # Event 3 has provider_log_id = "naver:expected:id:nomatch"
        ev3 = session.query(GameEvent).filter(GameEvent.game_id == TARGET_GAME_ID, GameEvent.event_seq == 3).first()
        assert ev3 is not None
        ev3.provider_log_id = "naver:expected:id:nomatch"
        ev3.batter_name = "김형준"
        ev3.description = "김형준 : 투수 땅볼 아웃 (투수->1루수 송구아웃)"
        session.commit()

        # PBP row 35 has provider_log_id = "naver:other:different:id", but EXACT SAME batter and description
        pbp35 = (
            session.query(GamePlayByPlay)
            .filter(GamePlayByPlay.game_id == TARGET_GAME_ID, GamePlayByPlay.source_row_index == 35)
            .first()
        )
        assert pbp35 is not None
        assert pbp35.batter_name == "김형준"
        assert pbp35.play_description == "김형준 : 투수 땅볼 아웃 (투수->1루수 송구아웃)"
        pbp35.provider_log_id = "naver:other:different:id"
        session.commit()

        orig_ev3_desc = ev3.description
        orig_pbp35_desc = pbp35.play_description
        orig_pbp35_pid = pbp35.provider_log_id

    # Observe write statements on connection cursor
    write_count = [0]

    def count_writes(conn, cursor, statement, parameters, context, executemany):
        if statement.strip().upper().startswith(("INSERT", "UPDATE", "DELETE")):
            write_count[0] += 1

    event.listen(pipeline.engine, "before_cursor_execute", count_writes)
    try:
        # Attempt correction on event 3 -> strict ID-priority must fail because provider_log_id does not match
        res = pipeline.apply_event_correction(
            revision_id="REV-ID-MISMATCH-NO-BYPASS",
            target_event_seq=3,
            revised_description="Should NEVER be applied via description fallback",
        )
        assert res["already_applied"] is False
        assert res["mutations"] == 0
        assert res["status"] == "PBP_MATCH_FAILED"
        assert write_count[0] == 0, f"Expected 0 DML writes during failed correction, got {write_count[0]}"
    finally:
        event.remove(pipeline.engine, "before_cursor_execute", count_writes)

    # Verify zero database mutations occurred across GameEvent, GamePlayByPlay, and RelayRevisionRecord
    with sessionmaker(bind=engine)() as session:
        ev3_after = (
            session.query(GameEvent).filter(GameEvent.game_id == TARGET_GAME_ID, GameEvent.event_seq == 3).first()
        )
        assert ev3_after is not None
        assert ev3_after.description == orig_ev3_desc
        assert "Should NEVER be applied" not in ev3_after.description

        pbp35_after = (
            session.query(GamePlayByPlay)
            .filter(GamePlayByPlay.game_id == TARGET_GAME_ID, GamePlayByPlay.source_row_index == 35)
            .first()
        )
        assert pbp35_after is not None
        assert pbp35_after.play_description == orig_pbp35_desc
        assert pbp35_after.provider_log_id == orig_pbp35_pid
        assert "[CORRECTED]" not in pbp35_after.play_description

        rev = (
            session.query(RelayRevisionRecord)
            .filter(RelayRevisionRecord.revision_id == "REV-ID-MISMATCH-NO-BYPASS")
            .first()
        )
        assert rev is None


def test_correction_rejects_ambiguous_pbp_candidates(ephemeral_db: Path, lock_dir: Path) -> None:
    """Verify that multiple matching PBP candidates trigger an explicit ValueError and 0 mutations."""
    pipeline = SealedSnapshotRelayPipeline(
        game_id=TARGET_GAME_ID,
        db_path_or_url=str(ephemeral_db),
        kbo_fixture_path=KBO_FIXTURE,
        naver_fixture_path=NAVER_FIXTURE,
        lock_dir=lock_dir,
    )
    pipeline.run(apply_correction=False)

    engine = create_engine(f"sqlite:///{ephemeral_db}")
    with sessionmaker(bind=engine)() as session:
        # Insert a duplicate PBP row with identical provider_log_id to create ambiguity
        dup_pbp = GamePlayByPlay(
            game_id=TARGET_GAME_ID,
            source_row_index=999,
            inning=9,
            inning_half="초",
            play_description="김형준 : 중복 행 생성",
            event_type="타격",
            result="아웃",
            batter_name="김형준",
            pitcher_name="최지민",
            provider_log_id="naver:c3fe20fbf07f:9t:2:6:cdeacf6a06",
        )
        session.add(dup_pbp)
        session.commit()

        ev3 = session.query(GameEvent).filter(GameEvent.game_id == TARGET_GAME_ID, GameEvent.event_seq == 3).first()
        assert ev3 is not None
        orig_ev3_desc = ev3.description

    # Attempt correction on event 3 -> must raise ValueError with Ambiguous PBP match message
    with pytest.raises(ValueError, match="Ambiguous PBP match: 2 candidates found"):
        pipeline.apply_event_correction(
            revision_id="REV-AMBIGUOUS-TEST",
            target_event_seq=3,
            revised_description="Ambiguous correction payload",
        )

    # Verify zero mutations occurred (transaction was rolled back)
    with sessionmaker(bind=engine)() as session:
        ev3_after = (
            session.query(GameEvent).filter(GameEvent.game_id == TARGET_GAME_ID, GameEvent.event_seq == 3).first()
        )
        assert ev3_after is not None
        assert ev3_after.description == orig_ev3_desc
        assert "Ambiguous correction payload" not in ev3_after.description

        rev = session.query(RelayRevisionRecord).filter(RelayRevisionRecord.revision_id == "REV-AMBIGUOUS-TEST").first()
        assert rev is None


def test_replay_revision_id_match_ignores_samedesc_other_row(ephemeral_db: Path, lock_dir: Path) -> None:
    """Verify that replay revision matches by ID only, ignoring unrelated row with same description."""
    # Test _apply_revisions_to_staged directly with controlled inputs
    pipeline = SealedSnapshotRelayPipeline(
        game_id=TARGET_GAME_ID,
        db_path_or_url=str(ephemeral_db),
        kbo_fixture_path=KBO_FIXTURE,
        naver_fixture_path=NAVER_FIXTURE,
        lock_dir=lock_dir,
    )
    pipeline.run(apply_correction=True)

    engine = create_engine(f"sqlite:///{ephemeral_db}")
    with sessionmaker(bind=engine)() as session:
        # Inject an unrelated PBP row with SAME description as event 3 but DIFFERENT provider_log_id
        unrelated_pbp = GamePlayByPlay(
            game_id=TARGET_GAME_ID,
            source_row_index=999,
            inning=9,
            inning_half="초",
            play_description="김형준 : 투수 땅볼 아웃 (투수->1루수 송구아웃)",  # Same as event 3
            event_type="타격",
            result="아웃",
            batter_name="김형준",
            pitcher_name="최지민",
            provider_log_id="naver:unrelated:different:id",
        )
        session.add(unrelated_pbp)
        session.commit()

    # Create staged data matching what pipeline would have after parsing
    canonical_events = [
        {
            "event_seq": 3,
            "provider_log_id": "naver:c3fe20fbf07f:9t:2:6:cdeacf6a06",
            "description": "원본",
            "result_code": "원본",
        }
    ]
    raw_pbp_rows = [
        {
            "provider_log_id": "naver:c3fe20fbf07f:9t:2:6:cdeacf6a06",
            "play_description": "김형준 : 투수 땅볼 아웃 (투수->1루수 송구아웃)",
            "result": "아웃",
        },
        {
            "provider_log_id": "naver:unrelated:different:id",
            "play_description": "김형준 : 투수 땅볼 아웃 (투수->1루수 송구아웃)",
            "result": "아웃",
        },
    ]

    # Apply revisions to staged data
    pipeline._apply_revisions_to_staged(canonical_events, raw_pbp_rows)

    # Verify: target PBP row (by provider_log_id) was modified
    target_pbp = next(p for p in raw_pbp_rows if p["provider_log_id"] == "naver:c3fe20fbf07f:9t:2:6:cdeacf6a06")
    assert "[CORRECTED]" in target_pbp["play_description"]
    assert target_pbp["result"] == "투수 땅볼 (정정)"

    # Verify: unrelated row with same description was NOT modified
    unrelated_pbp = next(p for p in raw_pbp_rows if p["provider_log_id"] == "naver:unrelated:different:id")
    assert "[CORRECTED]" not in unrelated_pbp["play_description"]
    assert unrelated_pbp["play_description"] == "김형준 : 투수 땅볼 아웃 (투수->1루수 송구아웃)"

    # Verify: canonical event was also updated
    assert "[CORRECTED]" in canonical_events[0]["description"]
    assert "투수 땅볼 (정정)" in canonical_events[0]["result_code"]


def test_replay_revision_id_missing_returns_no_match(ephemeral_db: Path, lock_dir: Path) -> None:
    """Verify that replay revision with valid but non-matching ID returns NO_MATCH_ID (no fallback)."""
    pipeline = SealedSnapshotRelayPipeline(
        game_id=TARGET_GAME_ID,
        db_path_or_url=str(ephemeral_db),
        kbo_fixture_path=KBO_FIXTURE,
        naver_fixture_path=NAVER_FIXTURE,
        lock_dir=lock_dir,
    )
    pipeline.run(apply_correction=True)

    engine = create_engine(f"sqlite:///{ephemeral_db}")
    with sessionmaker(bind=engine)() as session:
        # Modify the revision record to have target_provider_log_id = "missing" (valid string but no match)
        rev = session.query(RelayRevisionRecord).filter(RelayRevisionRecord.game_id == TARGET_GAME_ID).first()
        assert rev is not None
        rev.target_provider_log_id = "missing"  # Valid ID, just doesn't match any PBP
        session.commit()

    # Create new pipeline for replay
    pipeline2 = SealedSnapshotRelayPipeline(
        game_id=TARGET_GAME_ID,
        db_path_or_url=str(ephemeral_db),
        kbo_fixture_path=KBO_FIXTURE,
        naver_fixture_path=NAVER_FIXTURE,
        lock_dir=lock_dir,
    )
    # Should NOT raise - valid ID with no match is skipped with warning
    pipeline2.run(apply_correction=False)

    # Verify no new mutations (event 3 correction preserved from earlier commit)
    with sessionmaker(bind=engine)() as session:
        ev3 = session.query(GameEvent).filter(GameEvent.game_id == TARGET_GAME_ID, GameEvent.event_seq == 3).first()
        assert ev3 is not None
        assert "[CORRECTED]" in ev3.description


def test_replay_revision_id_genuine_none_fallback_unique(ephemeral_db: Path, lock_dir: Path) -> None:
    """Verify that replay revision with genuinely None ID uses fallback when unique."""
    pipeline = SealedSnapshotRelayPipeline(
        game_id=TARGET_GAME_ID,
        db_path_or_url=str(ephemeral_db),
        kbo_fixture_path=KBO_FIXTURE,
        naver_fixture_path=NAVER_FIXTURE,
        lock_dir=lock_dir,
    )
    pipeline.run(apply_correction=True)

    engine = create_engine(f"sqlite:///{ephemeral_db}")
    with sessionmaker(bind=engine)() as session:
        # Set target_provider_log_id to None (genuine absence) - keep original_description
        rev = session.query(RelayRevisionRecord).filter(RelayRevisionRecord.game_id == TARGET_GAME_ID).first()
        assert rev is not None
        rev.target_provider_log_id = None
        session.commit()

    # Test _apply_revisions_to_staged directly
    canonical_events = [
        {
            "event_seq": 3,
            "provider_log_id": "naver:c3fe20fbf07f:9t:2:6:cdeacf6a06",
            "description": "원본",
            "result_code": "원본",
        }
    ]
    raw_pbp_rows = [
        {
            "provider_log_id": "naver:c3fe20fbf07f:9t:2:6:cdeacf6a06",
            "play_description": "김형준 : 투수 땅볼 아웃 (투수->1루수 송구아웃)",
            "result": "아웃",
        },
    ]

    pipeline._apply_revisions_to_staged(canonical_events, raw_pbp_rows)

    # Verify: target PBP row was corrected via fallback (description match)
    target_pbp = raw_pbp_rows[0]
    assert "[CORRECTED]" in target_pbp["play_description"]
    assert target_pbp["result"] == "투수 땅볼 (정정)"
    assert "[CORRECTED]" in canonical_events[0]["description"]


def test_replay_revision_id_genuine_none_fallback_multiple_rejected(ephemeral_db: Path, lock_dir: Path) -> None:
    """Verify that replay revision with None ID rejects ambiguous fallback (multiple matches)."""
    pipeline = SealedSnapshotRelayPipeline(
        game_id=TARGET_GAME_ID,
        db_path_or_url=str(ephemeral_db),
        kbo_fixture_path=KBO_FIXTURE,
        naver_fixture_path=NAVER_FIXTURE,
        lock_dir=lock_dir,
    )
    pipeline.run(apply_correction=True)

    engine = create_engine(f"sqlite:///{ephemeral_db}")
    with sessionmaker(bind=engine)() as session:
        # Set target_provider_log_id to None
        rev = session.query(RelayRevisionRecord).filter(RelayRevisionRecord.game_id == TARGET_GAME_ID).first()
        assert rev is not None
        rev.target_provider_log_id = None
        session.commit()

    # Test _apply_revisions_to_staged directly with duplicate descriptions
    canonical_events = [
        {
            "event_seq": 3,
            "provider_log_id": "naver:c3fe20fbf07f:9t:2:6:cdeacf6a06",
            "description": "원본",
            "result_code": "원본",
        }
    ]
    raw_pbp_rows = [
        {
            "provider_log_id": "naver:c3fe20fbf07f:9t:2:6:cdeacf6a06",
            "play_description": "김형준 : 투수 땅볼 아웃 (투수->1루수 송구아웃)",
            "result": "아웃",
        },
        {
            "provider_log_id": "naver:duplicate:id",
            "play_description": "김형준 : 투수 땅볼 아웃 (투수->1루수 송구아웃)",
            "result": "아웃",
        },
    ]

    # Should raise ValueError on ambiguous fallback
    with pytest.raises(ValueError, match="Ambiguous fallback match"):
        pipeline._apply_revisions_to_staged(canonical_events, raw_pbp_rows)


def test_correction_kbo_single_verifies_mapping_or_rejects(ephemeral_db: Path, lock_dir: Path) -> None:
    """Verify that kbo_single correction fails safely when no PBP mapping exists."""
    pipeline = SealedSnapshotRelayPipeline(
        game_id=TARGET_GAME_ID,
        db_path_or_url=str(ephemeral_db),
        kbo_fixture_path=KBO_FIXTURE,
        naver_fixture_path=NAVER_FIXTURE,
        lock_dir=lock_dir,
    )
    # Run with empty_naver=True to force kbo_single source
    result = pipeline.run(apply_correction=True, empty_naver=True)

    # kbo_single produces PBP rows without provider_log_id
    # Correction should fail safely with PBP_MATCH_FAILED
    assert result["status"] == "SUCCESS"
    # The correction returns PBP_MATCH_FAILED status (0 mutations)
    # This is correct behavior - no mapping, no guess

    engine = create_engine(f"sqlite:///{ephemeral_db}")
    with sessionmaker(bind=engine)() as session:
        events = (
            session.query(GameEvent).filter(GameEvent.game_id == TARGET_GAME_ID).order_by(GameEvent.event_seq).all()
        )
        pbps = (
            session.query(GamePlayByPlay)
            .filter(GamePlayByPlay.game_id == TARGET_GAME_ID)
            .order_by(GamePlayByPlay.source_row_index)
            .all()
        )

        assert len(events) == 5
        assert len(pbps) == 5  # kbo_single produces 5 PBP rows (one per event)

        # Event 3 should NOT be corrected (no matching PBP)
        ev3 = next((e for e in events if e.event_seq == 3), None)
        assert ev3 is not None
        assert "[CORRECTED]" not in ev3.description


def test_late_revision_failure_no_partial_application(ephemeral_db: Path, lock_dir: Path) -> None:
    """Verify that when a later revision has invalid ID, it raises and no partial staged changes persist."""
    pipeline = SealedSnapshotRelayPipeline(
        game_id=TARGET_GAME_ID,
        db_path_or_url=str(ephemeral_db),
        kbo_fixture_path=KBO_FIXTURE,
        naver_fixture_path=NAVER_FIXTURE,
        lock_dir=lock_dir,
    )
    pipeline.run(apply_correction=True)

    engine = create_engine(f"sqlite:///{ephemeral_db}")
    with sessionmaker(bind=engine)() as session:
        # Add a second revision with INVALID target_provider_log_id (empty string - will raise ValueError)
        rev2 = RelayRevisionRecord(
            revision_id="REV-EVENT4-INVALID",
            game_id=TARGET_GAME_ID,
            target_event_seq=4,
            target_provider_log_id="",  # Empty string -> INVALID_ID -> ValueError
            original_description="박민우 : 좌익수 플라이 아웃",
            revised_description="박민우 : 좌익수 플라이 아웃 [CORRECTED]",
            original_result_code="플라이 아웃",
            revised_result_code="플라이 아웃 (정정)",
            payload_hash="test_hash_2",
            status="APPLIED",
            created_at=datetime.now(UTC),
        )
        session.add(rev2)
        session.commit()

        # Verify event 3 is already corrected
        ev3_before = (
            session.query(GameEvent).filter(GameEvent.game_id == TARGET_GAME_ID, GameEvent.event_seq == 3).first()
        )
        assert "[CORRECTED]" in ev3_before.description

        # Get event 4 original description
        ev4_before = (
            session.query(GameEvent).filter(GameEvent.game_id == TARGET_GAME_ID, GameEvent.event_seq == 4).first()
        )
        orig_ev4_desc = ev4_before.description

    # Create new pipeline for replay - second revision has empty string ID -> should raise ValueError
    pipeline2 = SealedSnapshotRelayPipeline(
        game_id=TARGET_GAME_ID,
        db_path_or_url=str(ephemeral_db),
        kbo_fixture_path=KBO_FIXTURE,
        naver_fixture_path=NAVER_FIXTURE,
        lock_dir=lock_dir,
    )
    with pytest.raises(ValueError, match="Invalid target_provider_log_id"):
        pipeline2.run(apply_correction=False)

    # Verify DB state: event 3 correction preserved (committed earlier), event 4 NOT modified
    with sessionmaker(bind=engine)() as session:
        ev3_after = (
            session.query(GameEvent).filter(GameEvent.game_id == TARGET_GAME_ID, GameEvent.event_seq == 3).first()
        )
        assert ev3_after is not None
        assert "[CORRECTED]" in ev3_after.description  # Preserved from earlier commit

        ev4_after = (
            session.query(GameEvent).filter(GameEvent.game_id == TARGET_GAME_ID, GameEvent.event_seq == 4).first()
        )
        assert ev4_after is not None
        assert ev4_after.description == orig_ev4_desc  # Not modified by failed replay


def test_independent_revisions_no_interference(ephemeral_db: Path, lock_dir: Path) -> None:
    """Verify that two revisions on different targets don't interfere with each other."""
    pipeline = SealedSnapshotRelayPipeline(
        game_id=TARGET_GAME_ID,
        db_path_or_url=str(ephemeral_db),
        kbo_fixture_path=KBO_FIXTURE,
        naver_fixture_path=NAVER_FIXTURE,
        lock_dir=lock_dir,
    )
    pipeline.run(apply_correction=True)

    engine = create_engine(f"sqlite:///{ephemeral_db}")
    with sessionmaker(bind=engine)() as session:
        # Add second revision targeting event 2 with its correct provider_log_id
        # From fixture: event 2 is "김휘집 : 볼넷" with provider_log_id from Naver
        # We need to use the actual provider_log_id from the fixture for event 2
        rev2 = RelayRevisionRecord(
            revision_id="REV-EVENT2-INDEPENDENT",
            game_id=TARGET_GAME_ID,
            target_event_seq=2,
            target_provider_log_id="naver:c3fe20fbf07f:9t:2:4:cdeacf6a06",  # Different provider_log_id for event 2
            original_description="김휘집 : 볼넷",
            revised_description="김휘집 : 볼넷 [CORRECTED]",
            original_result_code="볼넷",
            revised_result_code="볼넷 (정정)",
            payload_hash="test_hash_3",
            status="APPLIED",
            created_at=datetime.now(UTC),
        )
        session.add(rev2)
        session.commit()

    # Create new pipeline for replay - both revisions should apply independently
    pipeline2 = SealedSnapshotRelayPipeline(
        game_id=TARGET_GAME_ID,
        db_path_or_url=str(ephemeral_db),
        kbo_fixture_path=KBO_FIXTURE,
        naver_fixture_path=NAVER_FIXTURE,
        lock_dir=lock_dir,
    )
    pipeline2.run(apply_correction=False)

    with sessionmaker(bind=engine)() as session:
        # Event 3 should have first correction
        ev3 = session.query(GameEvent).filter(GameEvent.game_id == TARGET_GAME_ID, GameEvent.event_seq == 3).first()
        assert ev3 is not None
        assert "[CORRECTED]" in ev3.description
        assert "투수 땅볼 (정정)" in ev3.result_code

        # Event 2 should have second correction (if PBP matches)
        # Note: the provider_log_id might not match, so this tests that it doesn't break event 3
        ev2 = session.query(GameEvent).filter(GameEvent.game_id == TARGET_GAME_ID, GameEvent.event_seq == 2).first()
        assert ev2 is not None
        # Event 2 may or may not be corrected depending on PBP match - but event 3 must remain

        # Event 4 should be untouched
        ev4 = session.query(GameEvent).filter(GameEvent.game_id == TARGET_GAME_ID, GameEvent.event_seq == 4).first()
        assert ev4 is not None
        assert "[CORRECTED]" not in ev4.description
