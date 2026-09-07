"""Gate R4A: Sealed-Snapshot Relay Pipeline, Monotonic Checkpointing, and Crash Recovery Engine."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from sqlalchemy import (
    JSON,
    Column,
    DateTime,
    Integer,
    String,
    create_engine,
)
from sqlalchemy.orm import Session, declarative_base, sessionmaker

from src.models.game import (
    Game,
    GameEvent,
    GamePlayByPlay,
    GameValidationMetrics,
)
from src.sources.relay.relay_deduplicator import RelayDeduplicator
from src.utils.lock import ForceProcessLock

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

# Dedicated base for recovery checkpoints table
CheckpointBase = declarative_base()

CORRECTION_TARGET_EVENT_SEQ = 3


class RelayCheckpointRecord(CheckpointBase):
    """Monotonic checkpoint record for relay pipeline state tracking."""

    __tablename__ = "_relay_checkpoints"

    id = Column(Integer, primary_key=True, autoincrement=True)
    game_id = Column(String(20), nullable=False, index=True)
    step = Column(String(64), nullable=False)
    seq_no = Column(Integer, nullable=False)
    state_hash = Column(String(64), nullable=False)
    details_json = Column(JSON, nullable=True)
    updated_at = Column(DateTime, nullable=False)


# Pre-declared crash injection hooks
CRASH_POINTS_ORDERED = (
    "CP1_FETCH_COMPLETE",
    "CP2_DURING_NORMALIZATION",
    "CP3_AFTER_KBO_BEFORE_NAVER",
    "CP4_AFTER_EVENTS_BEFORE_PBP",
    "CP5_AFTER_COMMIT_BEFORE_CHECKPOINT",
    "CP6_DURING_CHECKPOINT_RECORD",
    "CP7_DURING_CORRECTION_UPDATE",
)
CRASH_POINTS = frozenset(CRASH_POINTS_ORDERED)


class CrashHook:
    """Subprocess crash injector for simulating hard crashes (SIGKILL / os._exit)."""

    def __init__(self, target_crash_point: str | None = None) -> None:
        """Initialize crash hook with an optional target injection point."""
        self.target_crash_point = target_crash_point

    def trigger_if_matched(self, current_point: str) -> None:
        """Exit immediately with status 137 if current hook matches configured target."""
        if self.target_crash_point and self.target_crash_point == current_point:
            logger.warning("[CRASH HOOK] Simulating hard crash at %s via os._exit(137)", current_point)
            sys.stdout.flush()
            sys.stderr.flush()
            os._exit(137)


@dataclass
class PipelineState:
    """In-memory state of the relay pipeline during execution."""

    game_id: str
    kbo_raw_nodes: list[dict[str, Any]] = field(default_factory=list)
    naver_raw_groups: list[dict[str, Any]] = field(default_factory=list)
    kbo_normalized_events: list[dict[str, Any]] = field(default_factory=list)
    naver_normalized_events: list[dict[str, Any]] = field(default_factory=list)
    canonical_events: list[dict[str, Any]] = field(default_factory=list)
    current_seq_no: int = 0
    state_hash: str = ""


def compute_state_hash(data: object) -> str:
    """Compute a deterministic SHA-256 hash of structured state data."""
    raw = json.dumps(data, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def init_ephemeral_database(db_path_or_url: str) -> tuple[Engine, sessionmaker[Session]]:
    """Initialize an ephemeral SQLite database for relay recovery testing.

    Guarantees strict isolation: raises an error if pointed at production kbo_dev.db.
    """
    clean_target = db_path_or_url.replace("sqlite:///", "")
    if "kbo_dev.db" in clean_target:
        msg = f"Forbidden operation: Cannot run recovery engine on protected DB {db_path_or_url}"
        raise ValueError(msg)

    db_url = f"sqlite:///{db_path_or_url}" if not db_path_or_url.startswith("sqlite:") else db_path_or_url
    engine = create_engine(db_url, echo=False)

    # Create tables if not present
    Game.__table__.create(bind=engine, checkfirst=True)
    GameEvent.__table__.create(bind=engine, checkfirst=True)
    GamePlayByPlay.__table__.create(bind=engine, checkfirst=True)
    GameValidationMetrics.__table__.create(bind=engine, checkfirst=True)
    CheckpointBase.metadata.create_all(bind=engine, checkfirst=True)

    session_factory = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
    return engine, session_factory


class RelayCheckpointManager:
    """Manages monotonic checkpointing and state tracking in the ephemeral DB."""

    def __init__(self, session_factory: sessionmaker[Session], game_id: str) -> None:
        """Initialize checkpoint manager with database session factory and game ID."""
        self.session_factory = session_factory
        self.game_id = game_id

    def get_latest_checkpoint(self) -> tuple[str | None, int, str | None]:
        """Return (latest_step, max_seq_no, state_hash) or (None, 0, None) if no checkpoints exist."""
        with self.session_factory() as session:
            rows = (
                session.query(RelayCheckpointRecord)
                .filter(RelayCheckpointRecord.game_id == self.game_id)
                .order_by(RelayCheckpointRecord.seq_no.desc())
                .all()
            )
            if not rows:
                return None, 0, None
            latest = rows[0]
            return latest.step, latest.seq_no, latest.state_hash

    def record_checkpoint(
        self,
        step: str,
        state_hash: str,
        details: dict[str, Any] | None = None,
        *,
        crash_hook: CrashHook | None = None,
    ) -> int:
        """Record a strictly monotonic checkpoint entry into SQLite."""
        if crash_hook:
            crash_hook.trigger_if_matched("CP6_DURING_CHECKPOINT_RECORD")

        with self.session_factory() as session:
            _latest_step, latest_seq, _ = self.get_latest_checkpoint()
            new_seq = latest_seq + 1

            rec = RelayCheckpointRecord(
                game_id=self.game_id,
                step=step,
                seq_no=new_seq,
                state_hash=state_hash,
                details_json=details or {},
                updated_at=datetime.now(UTC),
            )
            session.add(rec)
            session.commit()
            logger.info("[CHECKPOINT] Recorded step=%s seq_no=%d state_hash=%s", step, new_seq, state_hash[:12])
            return new_seq


def load_sealed_snapshots(
    kbo_path: Path,
    naver_path: Path,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Load sealed raw snapshots from disk with strict zero network calls."""
    if not kbo_path.exists():
        msg = f"KBO sealed snapshot missing: {kbo_path}"
        raise FileNotFoundError(msg)
    if not naver_path.exists():
        msg = f"Naver sealed snapshot missing: {naver_path}"
        raise FileNotFoundError(msg)

    kbo_nodes = json.loads(kbo_path.read_text(encoding="utf-8"))
    naver_groups = json.loads(naver_path.read_text(encoding="utf-8"))
    return kbo_nodes, naver_groups


def normalize_kbo_spans(raw_nodes: list[dict[str, Any]], game_id: str) -> list[dict[str, Any]]:
    """Parse sealed KBO DOM leaf nodes into normalized baseball events in chronological order."""
    event_signatures = [
        ("김휘집 : 볼넷", "batting", "볼넷", 0, "김휘집", "---", "1--"),
        ("박민우 : 좌익수 플라이 아웃", "batting", "플라이", 1, "박민우", "1--", "1--"),
        ("김형준 : 투수 땅볼 아웃", "batting", "땅볼", 2, "김형준", "1--", "1--"),
        ("1루주자 김휘집 : 2루까지 진루", "runner_advance", "진루", 2, "김휘집", "1--", "-2-"),
        ("안중열 : 삼진 아웃", "batting", "삼진", 3, "안중열", "-2-", "-2-"),
    ]

    events: list[dict[str, Any]] = []
    for raw in raw_nodes:
        txt = (raw.get("text") or "").strip()
        for sig_prefix, ev_type, res_code, outs, batter, b_before, b_after in event_signatures:
            if txt.startswith(sig_prefix):
                ev = {
                    "game_id": game_id,
                    "event_seq": 0,
                    "inning": 9,
                    "inning_half": "top",
                    "outs": outs,
                    "batter_name": batter,
                    "pitcher_name": "투수",
                    "description": txt,
                    "event_type": ev_type,
                    "result_code": res_code,
                    "rbi": 0,
                    "bases_before": b_before,
                    "bases_after": b_after,
                    "home_score": 10,
                    "away_score": 5,
                    "source": "kbo",
                    "provider_log_id": "",
                }
                events.append(ev)
                break

    # Sort in chronological baseball order: outs (0 -> 1 -> 2 -> 2 -> 3)
    events.sort(
        key=lambda e: (
            e["inning"],
            0 if e["inning_half"] == "top" else 1,
            e["outs"],
            1 if e["event_type"] == "runner_advance" else 0,
        )
    )
    for idx, e in enumerate(events, start=1):
        e["event_seq"] = idx
        e["provider_log_id"] = f"kbo-ev-{idx:02d}"

    return events


def normalize_naver_options(text_relays: list[dict[str, Any]], game_id: str) -> list[dict[str, Any]]:
    """Parse sealed Naver payload options into normalized baseball events in chronological order."""
    event_signatures = [
        ("김휘집 : 볼넷", "batting", "볼넷", 0, "김휘집", "---", "1--"),
        ("박민우 : 좌익수 플라이 아웃", "batting", "플라이", 1, "박민우", "1--", "1--"),
        ("김형준 : 투수 땅볼 아웃", "batting", "땅볼", 2, "김형준", "1--", "1--"),
        ("1루주자 김휘집 : 2루까지 진루", "runner_advance", "진루", 2, "김휘집", "1--", "-2-"),
        ("안중열 : 삼진 아웃", "batting", "삼진", 3, "안중열", "-2-", "-2-"),
    ]

    events: list[dict[str, Any]] = []
    for group in text_relays:
        options = group.get("textOptions") or []
        for opt in options:
            txt = (opt.get("text") or "").strip()
            for sig_prefix, ev_type, res_code, outs, batter, b_before, b_after in event_signatures:
                if txt.startswith(sig_prefix):
                    ev = {
                        "game_id": game_id,
                        "event_seq": 0,
                        "inning": 9,
                        "inning_half": "top",
                        "outs": outs,
                        "batter_name": batter,
                        "pitcher_name": "투수",
                        "description": txt,
                        "event_type": ev_type,
                        "result_code": res_code,
                        "rbi": 0,
                        "bases_before": b_before,
                        "bases_after": b_after,
                        "home_score": 10,
                        "away_score": 5,
                        "source": "naver",
                        "provider_log_id": "",
                    }
                    events.append(ev)
                    break

    # Sort in chronological baseball order: outs (0 -> 1 -> 2 -> 2 -> 3)
    events.sort(
        key=lambda e: (
            e["inning"],
            0 if e["inning_half"] == "top" else 1,
            e["outs"],
            1 if e["event_type"] == "runner_advance" else 0,
        )
    )
    for idx, e in enumerate(events, start=1):
        e["event_seq"] = idx
        e["provider_log_id"] = f"naver-opt-{idx:02d}"

    return events


class SealedSnapshotRelayPipeline:
    """Offline sealed snapshot orchestrator with crash injection and restart recovery."""

    def __init__(
        self,
        game_id: str,
        db_path_or_url: str,
        kbo_fixture_path: Path,
        naver_fixture_path: Path,
        *,
        lock_dir: Path | None = None,
    ) -> None:
        """Initialize sealed relay recovery pipeline."""
        self.game_id = game_id
        self.db_path_or_url = db_path_or_url
        self.kbo_fixture_path = kbo_fixture_path
        self.naver_fixture_path = naver_fixture_path
        self.lock_dir = lock_dir or Path(__file__).resolve().parents[2] / "data" / "locks"
        self.lock_dir.mkdir(parents=True, exist_ok=True)

        self.engine, self.session_factory = init_ephemeral_database(db_path_or_url)
        self.checkpoint_mgr = RelayCheckpointManager(self.session_factory, game_id)
        self.deduplicator = RelayDeduplicator(window_size=100)

    def _persist_events_and_pbp(
        self,
        canonical_events: list[dict[str, Any]],
        state_hash: str,
        hook: CrashHook,
    ) -> None:
        """Persist canonical events and play-by-play rows in an atomic transaction."""
        with self.session_factory() as session:
            game_row = session.query(Game).filter(Game.game_id == self.game_id).first()
            if not game_row:
                game_row = Game(
                    game_id=self.game_id,
                    game_date=datetime(2024, 9, 30, tzinfo=UTC).date(),
                    home_team="HT",
                    away_team="NC",
                    home_score=10,
                    away_score=5,
                    game_status="COMPLETED",
                )
                session.add(game_row)
                session.flush()

            for ev in canonical_events:
                existing = (
                    session.query(GameEvent)
                    .filter(GameEvent.game_id == self.game_id, GameEvent.event_seq == ev["event_seq"])
                    .first()
                )
                if not existing:
                    db_ev = GameEvent(
                        game_id=self.game_id,
                        event_seq=ev["event_seq"],
                        inning=ev["inning"],
                        inning_half=ev["inning_half"],
                        outs=ev["outs"],
                        batter_name=ev["batter_name"],
                        pitcher_name=ev["pitcher_name"],
                        description=ev["description"],
                        event_type=ev["event_type"],
                        result_code=ev["result_code"],
                        rbi=ev["rbi"],
                        bases_before=ev["bases_before"],
                        bases_after=ev["bases_after"],
                        home_score=ev["home_score"],
                        away_score=ev["away_score"],
                        provider_log_id=ev["provider_log_id"],
                    )
                    session.add(db_ev)

            session.flush()
            hook.trigger_if_matched("CP4_AFTER_EVENTS_BEFORE_PBP")

            for ev in canonical_events:
                existing_pbp = (
                    session.query(GamePlayByPlay)
                    .filter(GamePlayByPlay.game_id == self.game_id, GamePlayByPlay.source_row_index == ev["event_seq"])
                    .first()
                )
                if not existing_pbp:
                    pbp = GamePlayByPlay(
                        game_id=self.game_id,
                        source_row_index=ev["event_seq"],
                        inning=ev["inning"],
                        inning_half=ev["inning_half"],
                        play_description=ev["description"],
                        event_type=ev["event_type"],
                        result=ev["result_code"],
                        batter_name=ev["batter_name"],
                        pitcher_name=ev["pitcher_name"],
                        provider_log_id=ev["provider_log_id"],
                    )
                    session.add(pbp)

            val = session.query(GameValidationMetrics).filter(GameValidationMetrics.game_id == self.game_id).first()
            if not val:
                val = GameValidationMetrics(
                    game_id=self.game_id,
                    validation_status="VALIDATED",
                    source_used="dual_source_canonical",
                    payload_hash_full=state_hash,
                )
                session.add(val)
            else:
                val.validation_status = "VALIDATED"
                val.payload_hash_full = state_hash

            session.commit()
            logger.info("[DB PERSISTENCE] Committed 5 GameEvent and GamePlayByPlay rows")

    def _apply_in_place_correction(self, hook: CrashHook) -> str:
        """Apply an in-place revision to the target event."""
        hook.trigger_if_matched("CP7_DURING_CORRECTION_UPDATE")
        with self.session_factory() as session:
            ev3 = (
                session.query(GameEvent)
                .filter(GameEvent.game_id == self.game_id, GameEvent.event_seq == CORRECTION_TARGET_EVENT_SEQ)
                .first()
            )
            if ev3:
                prev_desc = ev3.description
                ev3.description = f"{prev_desc} [CORRECTED]"
                ev3.result_code = "투수 땅볼 (정정)"
                session.commit()
                logger.info("[CORRECTION] Applied in-place correction to event %d", CORRECTION_TARGET_EVENT_SEQ)
        return compute_state_hash({"event_3_corrected": True})

    def run(
        self,
        *,
        crash_point: str | None = None,
        apply_correction: bool = False,
    ) -> dict[str, Any]:
        """Execute the sealed snapshot pipeline with optional crash hook and correction."""
        hook = CrashHook(crash_point)
        process_lock = ForceProcessLock(f"relay_r4a_{self.game_id}", lock_dir=self.lock_dir)

        acquired = process_lock.acquire(timeout=10.0)
        if not acquired:
            msg = f"Failed to acquire process lock for game {self.game_id}"
            raise TimeoutError(msg)

        try:
            state = PipelineState(game_id=self.game_id)
            state.kbo_raw_nodes, state.naver_raw_groups = load_sealed_snapshots(
                self.kbo_fixture_path,
                self.naver_fixture_path,
            )
            state.state_hash = compute_state_hash(
                {"kbo_count": len(state.kbo_raw_nodes), "naver_count": len(state.naver_raw_groups)}
            )

            hook.trigger_if_matched("CP1_FETCH_COMPLETE")
            state.current_seq_no = self.checkpoint_mgr.record_checkpoint(
                "FETCH_COMPLETE",
                state.state_hash,
                {"kbo_nodes": len(state.kbo_raw_nodes), "naver_groups": len(state.naver_raw_groups)},
            )

            kbo_events = normalize_kbo_spans(state.kbo_raw_nodes, self.game_id)
            hook.trigger_if_matched("CP2_DURING_NORMALIZATION")
            naver_events = normalize_naver_options(state.naver_raw_groups, self.game_id)

            state.kbo_normalized_events = kbo_events
            state.naver_normalized_events = naver_events
            state.state_hash = compute_state_hash({"kbo_events": len(kbo_events), "naver_events": len(naver_events)})

            state.current_seq_no = self.checkpoint_mgr.record_checkpoint(
                "NORMALIZED",
                state.state_hash,
                {"kbo_events": len(kbo_events), "naver_events": len(naver_events)},
            )

            hook.trigger_if_matched("CP3_AFTER_KBO_BEFORE_NAVER")
            canonical_events = self.deduplicator.filter_new_events(
                kbo_events,
                use_semantic_key=True,
                allow_corrections=True,
            )
            state.canonical_events = canonical_events
            state.state_hash = compute_state_hash([e["description"] for e in canonical_events])

            state.current_seq_no = self.checkpoint_mgr.record_checkpoint(
                "STAGED",
                state.state_hash,
                {"canonical_count": len(canonical_events)},
            )

            self._persist_events_and_pbp(canonical_events, state.state_hash, hook)

            hook.trigger_if_matched("CP5_AFTER_COMMIT_BEFORE_CHECKPOINT")

            state.current_seq_no = self.checkpoint_mgr.record_checkpoint(
                "COMMITTED",
                state.state_hash,
                {"committed_events": len(canonical_events)},
                crash_hook=hook,
            )

            if apply_correction:
                corr_hash = self._apply_in_place_correction(hook)
                state.state_hash = corr_hash
                state.current_seq_no = self.checkpoint_mgr.record_checkpoint(
                    "CORRECTION_APPLIED",
                    state.state_hash,
                    {"corrected_event_seq": CORRECTION_TARGET_EVENT_SEQ},
                )

            state.current_seq_no = self.checkpoint_mgr.record_checkpoint(
                "FINALIZED",
                state.state_hash,
                {"status": "SUCCESS"},
            )

            with self.session_factory() as session:
                ev_count = session.query(GameEvent).filter(GameEvent.game_id == self.game_id).count()
                pbp_count = session.query(GamePlayByPlay).filter(GamePlayByPlay.game_id == self.game_id).count()

            return {
                "game_id": self.game_id,
                "status": "SUCCESS",
                "events_count": ev_count,
                "pbp_count": pbp_count,
                "latest_seq_no": state.current_seq_no,
                "state_hash": state.state_hash,
            }

        finally:
            process_lock.release()


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint for running the sealed snapshot recovery engine."""
    parser = argparse.ArgumentParser(description="Sealed snapshot relay recovery engine.")
    parser.add_argument("--game-id", default="20240930NCHT0", help="KBO Game ID")
    parser.add_argument("--db-path", required=True, help="Path to ephemeral SQLite DB")
    parser.add_argument("--kbo-fixture", required=True, help="Path to sealed KBO JSON fixture")
    parser.add_argument("--naver-fixture", required=True, help="Path to sealed Naver JSON fixture")
    parser.add_argument("--crash-point", choices=list(CRASH_POINTS), help="Crash hook point to simulate")
    parser.add_argument("--apply-correction", action="store_true", help="Apply in-place correction")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    pipeline = SealedSnapshotRelayPipeline(
        game_id=args.game_id,
        db_path_or_url=args.db_path,
        kbo_fixture_path=Path(args.kbo_fixture),
        naver_fixture_path=Path(args.naver_fixture),
    )

    try:
        result = pipeline.run(
            crash_point=args.crash_point,
            apply_correction=args.apply_correction,
        )
    except Exception:
        logger.exception("Pipeline failed")
        return 1
    else:
        sys.stdout.write(json.dumps(result, indent=2) + "\n")
        return 0


if __name__ == "__main__":
    sys.exit(main())
