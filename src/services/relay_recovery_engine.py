"""Gate R4A: Sealed-Snapshot Relay Pipeline, Monotonic Checkpointing, and Crash Recovery Engine."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import sys
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

if os.environ.get("KBO_SEALED_REPLAY_OFFLINE") == "1":
    import socket

    def _blocked_connect(*_args: object, **_kwargs: object) -> None:
        msg = "Network forbidden during sealed replay: external socket connection blocked"
        raise RuntimeError(msg)

    socket.socket.connect = _blocked_connect

from sqlalchemy import (
    JSON,
    Column,
    DateTime,
    Integer,
    String,
    create_engine,
)
from sqlalchemy.orm import Session, declarative_base, sessionmaker

from src.crawlers.pbp_crawler import PBPCrawler
from src.crawlers.relay_crawler import RelayCrawler
from src.models.base import Base
from src.models.game import (
    Game,
    GameEvent,
    GamePlayByPlay,
    GameValidationMetrics,
)
from src.repositories.game_relay import RelaySaveOptions, save_relay_data
from src.services.wpa_transitions import apply_wpa_transitions
from src.sources.relay.relay_deduplicator import RelayDeduplicator
from src.utils.lock import ForceProcessLock
from src.utils.relay_text import (
    classify_relay_result,
    detect_relay_event_type,
    is_relay_result_event_text,
)

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

# Dedicated base for recovery checkpoints table
CheckpointBase = declarative_base()

CORRECTION_TARGET_EVENT_SEQ = 3
DEFAULT_REVISION_ID = "REV-20240930-EV03-01"

# Fixture SHA256 checksums
EXPECTED_KBO_FIXTURE_SHA256 = "5d04010809ff8cc85b0f95d51ebc0a6b6b98337387848446287934a199eb7c95"
EXPECTED_NAVER_FIXTURE_SHA256 = "aa3a45fdf6fe380b5ec6df483064e45914667da9074fedd91d45cd11d88233ad"


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
    "CP3_BEFORE_DEDUPLICATION_MERGE",
    "CP4_IN_TRANSACTION_DURING_SAVE",
    "CP5_AFTER_COMMIT_BEFORE_CHECKPOINT",
    "CP6_DURING_CHECKPOINT_RECORD",
    "CP7_DURING_CORRECTION_UPDATE",
)
CRASH_POINT_ALIASES = {
    "CP3_AFTER_KBO_BEFORE_NAVER": "CP3_BEFORE_DEDUPLICATION_MERGE",
    "CP4_AFTER_EVENTS_BEFORE_PBP": "CP4_IN_TRANSACTION_DURING_SAVE",
}
CRASH_POINTS = frozenset(CRASH_POINTS_ORDERED) | frozenset(CRASH_POINT_ALIASES.keys())


class CrashHook:
    """Subprocess crash injector for simulating hard crashes (SIGKILL / os._exit)."""

    def __init__(self, target_crash_point: str | None = None) -> None:
        """Initialize crash hook with an optional target injection point."""
        self.target_crash_point = target_crash_point

    def trigger_if_matched(self, current_point: str) -> None:
        """Exit immediately with status 137 if current hook matches configured target."""
        if not self.target_crash_point:
            return
        target = CRASH_POINT_ALIASES.get(self.target_crash_point, self.target_crash_point)
        current = CRASH_POINT_ALIASES.get(current_point, current_point)
        if target == current:
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
    raw_pbp_rows: list[dict[str, Any]] = field(default_factory=list)
    canonical_events: list[dict[str, Any]] = field(default_factory=list)
    source_used: str = "dual_canonical"
    current_seq_no: int = 0
    state_hash: str = ""


def compute_state_hash(data: object) -> str:
    """Compute a deterministic SHA-256 hash of structured state data."""
    raw = json.dumps(data, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def compute_domain_state_hash(session: Session, game_id: str) -> str:
    """Compute a deep domain state hash over substantive database entities."""
    events = session.query(GameEvent).filter(GameEvent.game_id == game_id).order_by(GameEvent.event_seq.asc()).all()
    pbp_count = session.query(GamePlayByPlay).filter(GamePlayByPlay.game_id == game_id).count()
    val = session.query(GameValidationMetrics).filter(GameValidationMetrics.game_id == game_id).first()

    event_payloads = [
        {
            "event_seq": e.event_seq,
            "inning": e.inning,
            "inning_half": e.inning_half,
            "outs": e.outs,
            "batter_name": e.batter_name,
            "description": e.description,
            "event_type": e.event_type,
            "result_code": e.result_code,
            "home_score": e.home_score,
            "away_score": e.away_score,
            "bases_before": e.bases_before,
            "bases_after": e.bases_after,
        }
        for e in events
    ]

    full_state = {
        "game_id": game_id,
        "events_count": len(events),
        "pbp_count": pbp_count,
        "validation_status": val.validation_status if val else None,
        "source_used": val.source_used if val else None,
        "events": event_payloads,
    }
    raw = json.dumps(full_state, sort_keys=True, ensure_ascii=False).encode("utf-8")
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

    # Create all ORM tables in the ephemeral database
    Base.metadata.create_all(bind=engine, checkfirst=True)
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
            session.flush()

            # CP6: Trigger inside transaction after flush, before commit
            if crash_hook:
                crash_hook.trigger_if_matched("CP6_DURING_CHECKPOINT_RECORD")

            session.commit()
            logger.info("[CHECKPOINT] Recorded step=%s seq_no=%d state_hash=%s", step, new_seq, state_hash[:12])
            return new_seq


def load_sealed_snapshots(
    kbo_path: Path,
    naver_path: Path,
    *,
    empty_naver: bool = False,
    verify_checksums: bool = True,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Load sealed raw snapshots from disk with checksum validation and strict zero network."""
    if not kbo_path.exists():
        msg = f"KBO sealed snapshot missing: {kbo_path}"
        raise FileNotFoundError(msg)
    if not naver_path.exists():
        msg = f"Naver sealed snapshot missing: {naver_path}"
        raise FileNotFoundError(msg)

    kbo_bytes = kbo_path.read_bytes()
    naver_bytes = naver_path.read_bytes()

    if verify_checksums:
        kbo_hash = hashlib.sha256(kbo_bytes).hexdigest()
        naver_hash = hashlib.sha256(naver_bytes).hexdigest()
        if kbo_hash != EXPECTED_KBO_FIXTURE_SHA256:
            msg = f"KBO fixture checksum mismatch: expected {EXPECTED_KBO_FIXTURE_SHA256}, got {kbo_hash}"
            raise ValueError(msg)
        if naver_hash != EXPECTED_NAVER_FIXTURE_SHA256:
            msg = f"Naver fixture checksum mismatch: expected {EXPECTED_NAVER_FIXTURE_SHA256}, got {naver_hash}"
            raise ValueError(msg)

    kbo_nodes = json.loads(kbo_bytes.decode("utf-8"))
    naver_groups = [] if empty_naver else json.loads(naver_bytes.decode("utf-8"))
    return kbo_nodes, naver_groups


def parse_kbo_nodes_to_events(raw_nodes: list[dict[str, Any]], game_id: str) -> list[dict[str, Any]]:
    """Parse sealed KBO DOM leaf nodes into normalized baseball events dynamically."""
    result_nodes = [
        node for node in reversed(raw_nodes) if is_relay_result_event_text((node.get("text") or "").strip())
    ]

    state = {
        "current_inning": 9,
        "current_half": "top",
        "home_score": 10,
        "away_score": 5,
        "current_outs": 0,
        "current_runners": 0,
    }

    events: list[dict[str, Any]] = []
    for idx, node in enumerate(result_nodes, 1):
        text = (node.get("text") or "").strip()
        outs_before, runners_before = PBPCrawler._update_out_base_state(state, text)  # noqa: SLF001
        ev_type = detect_relay_event_type(text)
        res_code = classify_relay_result(text)
        batter = text.split(":", 1)[0].replace("타자", "").strip() if ":" in text else ""
        events.append(
            {
                "game_id": game_id,
                "event_seq": idx,
                "inning": 9,
                "inning_half": "top",
                "outs": outs_before,
                "batter_name": batter,
                "pitcher_name": "최지민",
                "description": text,
                "event_type": ev_type,
                "result_code": res_code,
                "rbi": 0,
                "bases_before": PBPCrawler._format_base_string(runners_before),  # noqa: SLF001
                "bases_after": PBPCrawler._format_base_string(state["current_runners"]),  # noqa: SLF001
                "home_score": 10,
                "away_score": 5,
                "source": "kbo",
                "provider_log_id": f"kbo-ev-{idx:02d}",
            }
        )
    apply_wpa_transitions(events)
    return events


class SealedSnapshotRelayPipeline:
    """Offline sealed snapshot orchestrator with crash injection and restart recovery."""

    def __init__(  # noqa: PLR0913
        self,
        game_id: str,
        db_path_or_url: str,
        *,
        kbo_fixture_path: Path,
        naver_fixture_path: Path,
        lock_dir: Path | None = None,
        verify_checksums: bool = True,
    ) -> None:
        """Initialize sealed relay recovery pipeline."""
        self.game_id = game_id
        self.db_path_or_url = db_path_or_url
        self.kbo_fixture_path = kbo_fixture_path
        self.naver_fixture_path = naver_fixture_path
        self.verify_checksums = verify_checksums
        self.lock_dir = lock_dir or Path(__file__).resolve().parents[2] / "data" / "locks"
        self.lock_dir.mkdir(parents=True, exist_ok=True)

        self.engine, self.session_factory = init_ephemeral_database(db_path_or_url)
        self.checkpoint_mgr = RelayCheckpointManager(self.session_factory, game_id)
        self.deduplicator = RelayDeduplicator(window_size=100)

    def _persist_events_and_pbp(
        self,
        canonical_events: list[dict[str, Any]],
        raw_pbp_rows: list[dict[str, Any]],
        source_used: str,
        hook: CrashHook,
    ) -> None:
        """Persist canonical events and play-by-play rows using production save_relay_data."""
        with self.session_factory() as session:
            game_row = session.query(Game).filter(Game.game_id == self.game_id).first()
            if not game_row:
                game_row = Game(
                    game_id=self.game_id,
                    game_date=date(2024, 9, 30),
                    home_team="HT",
                    away_team="NC",
                    home_score=10,
                    away_score=5,
                    game_status="COMPLETED",
                )
                session.add(game_row)
                session.flush()

            # Execute production repository save logic
            save_relay_data(
                game_id=self.game_id,
                events=canonical_events,
                raw_pbp_rows=raw_pbp_rows,
                options=RelaySaveOptions(source_name=source_used),
                session=session,
            )

            # CP4: Crash hook inside transaction after flush, before commit
            hook.trigger_if_matched("CP4_IN_TRANSACTION_DURING_SAVE")

            session.commit()
            logger.info(
                "[DB PERSISTENCE] Committed %d GameEvent rows and %d GamePlayByPlay rows",
                len(canonical_events),
                len(raw_pbp_rows),
            )

    def apply_event_correction(
        self,
        *,
        revision_id: str = DEFAULT_REVISION_ID,
        target_event_seq: int = CORRECTION_TARGET_EVENT_SEQ,
        hook: CrashHook | None = None,
    ) -> dict[str, Any]:
        """Apply an idempotent in-place revision to the target event."""
        with self.session_factory() as session:
            ev3 = (
                session.query(GameEvent)
                .filter(GameEvent.game_id == self.game_id, GameEvent.event_seq == target_event_seq)
                .first()
            )
            if not ev3:
                logger.warning("[CORRECTION] Target event seq=%d not found", target_event_seq)
                return {"revision_id": revision_id, "already_applied": False, "mutations": 0}

            # Idempotency guard: do not re-apply or duplicate tags if already corrected
            if "[CORRECTED]" in (ev3.description or ""):
                logger.info(
                    "[CORRECTION] Event %d already corrected (revision %s); skipping mutation",
                    target_event_seq,
                    revision_id,
                )
                return {"revision_id": revision_id, "already_applied": True, "mutations": 0}

            # Apply revision
            ev3.description = f"{ev3.description} [CORRECTED]"
            ev3.result_code = "투수 땅볼 (정정)"
            session.flush()

            # CP7: Crash hook inside transaction after flush, before commit
            if hook:
                hook.trigger_if_matched("CP7_DURING_CORRECTION_UPDATE")

            session.commit()
            logger.info(
                "[CORRECTION] Committed revision %s to event %d",
                revision_id,
                target_event_seq,
            )
            return {"revision_id": revision_id, "already_applied": False, "mutations": 1}

    def _normalize_sources(
        self,
        state: PipelineState,
        *,
        empty_naver: bool,
        hook: CrashHook,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], str]:
        """Parse raw KBO and Naver snapshots into normalized events and PBP rows."""
        kbo_events = parse_kbo_nodes_to_events(state.kbo_raw_nodes, self.game_id)
        hook.trigger_if_matched("CP2_DURING_NORMALIZATION")

        if empty_naver or not state.naver_raw_groups:
            naver_events = []
            raw_pbp_rows = []
            source_used = "kbo_single"
        else:
            crawler = RelayCrawler()
            naver_result = crawler._parse_naver_payload(state.naver_raw_groups)  # noqa: SLF001
            naver_events = naver_result["events"]
            raw_pbp_rows = naver_result["raw_pbp_rows"]
            source_used = "dual_canonical"

        return kbo_events, naver_events, raw_pbp_rows, source_used

    def run(
        self,
        *,
        crash_point: str | None = None,
        apply_correction: bool = False,
        empty_naver: bool = False,
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
                empty_naver=empty_naver,
                verify_checksums=self.verify_checksums,
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

            kbo_events, naver_events, raw_pbp_rows, source_used = self._normalize_sources(
                state,
                empty_naver=empty_naver,
                hook=hook,
            )
            state.kbo_normalized_events = kbo_events
            state.naver_normalized_events = naver_events
            state.raw_pbp_rows = raw_pbp_rows
            state.source_used = source_used
            state.state_hash = compute_state_hash(
                {"kbo": len(kbo_events), "naver": len(naver_events), "pbp": len(raw_pbp_rows)}
            )

            state.current_seq_no = self.checkpoint_mgr.record_checkpoint(
                "NORMALIZED",
                state.state_hash,
                {"kbo": len(kbo_events), "naver": len(naver_events), "source": source_used},
            )

            hook.trigger_if_matched("CP3_BEFORE_DEDUPLICATION_MERGE")

            target_events = naver_events or kbo_events
            canonical_events = self.deduplicator.filter_new_events(
                target_events,
                use_semantic_key=True,
                allow_corrections=True,
            )
            state.canonical_events = canonical_events
            state.state_hash = compute_state_hash([e.get("description") for e in canonical_events])

            state.current_seq_no = self.checkpoint_mgr.record_checkpoint(
                "STAGED",
                state.state_hash,
                {"canonical_count": len(canonical_events), "source_used": source_used},
            )

            self._persist_events_and_pbp(canonical_events, raw_pbp_rows, source_used, hook)

            hook.trigger_if_matched("CP5_AFTER_COMMIT_BEFORE_CHECKPOINT")

            state.current_seq_no = self.checkpoint_mgr.record_checkpoint(
                "COMMITTED",
                state.state_hash,
                {"committed_events": len(canonical_events), "pbp_rows": len(raw_pbp_rows)},
                crash_hook=hook,
            )

            if apply_correction:
                corr_res = self.apply_event_correction(hook=hook)
                state.state_hash = compute_state_hash(corr_res)
                state.current_seq_no = self.checkpoint_mgr.record_checkpoint(
                    "CORRECTION_APPLIED",
                    state.state_hash,
                    corr_res,
                )

            with self.session_factory() as session:
                deep_domain_hash = compute_domain_state_hash(session, self.game_id)
                ev_count = session.query(GameEvent).filter(GameEvent.game_id == self.game_id).count()
                pbp_count = session.query(GamePlayByPlay).filter(GamePlayByPlay.game_id == self.game_id).count()

            state.state_hash = deep_domain_hash
            state.current_seq_no = self.checkpoint_mgr.record_checkpoint(
                "FINALIZED",
                state.state_hash,
                {"status": "SUCCESS", "deep_domain_hash": deep_domain_hash},
            )

            return {
                "game_id": self.game_id,
                "status": "SUCCESS",
                "events_count": ev_count,
                "pbp_count": pbp_count,
                "source_used": source_used,
                "latest_seq_no": state.current_seq_no,
                "state_hash": deep_domain_hash,
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
    parser.add_argument("--empty-naver", action="store_true", help="Simulate empty Naver payload")
    parser.add_argument("--no-verify-checksums", action="store_true", help="Disable fixture checksum verification")
    parser.add_argument("--lock-dir", help="Directory for process locks")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    pipeline = SealedSnapshotRelayPipeline(
        game_id=args.game_id,
        db_path_or_url=args.db_path,
        kbo_fixture_path=Path(args.kbo_fixture),
        naver_fixture_path=Path(args.naver_fixture),
        lock_dir=Path(args.lock_dir) if args.lock_dir else None,
        verify_checksums=not args.no_verify_checksums,
    )

    try:
        result = pipeline.run(
            crash_point=args.crash_point,
            apply_correction=args.apply_correction,
            empty_naver=args.empty_naver,
        )
    except Exception:
        logger.exception("Pipeline failed")
        return 1
    else:
        sys.stdout.write(json.dumps(result, indent=2) + "\n")
        return 0


if __name__ == "__main__":
    sys.exit(main())
