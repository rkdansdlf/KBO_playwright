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

# Dedicated base for recovery checkpoints and revision ledger tables
CheckpointBase = declarative_base()

CORRECTION_TARGET_EVENT_SEQ = 3
DEFAULT_REVISION_ID = "REV-20240930-EV03-01"
DEFAULT_REVISED_DESCRIPTION_SUFFIX = " [CORRECTED]"
DEFAULT_REVISED_RESULT_CODE = "투수 땅볼 (정정)"

# Fixture SHA256 checksums
EXPECTED_KBO_FIXTURE_SHA256 = "5d04010809ff8cc85b0f95d51ebc0a6b6b98337387848446287934a199eb7c95"
EXPECTED_NAVER_FIXTURE_SHA256 = "aa3a45fdf6fe380b5ec6df483064e45914667da9074fedd91d45cd11d88233ad"


@dataclass(frozen=True)
class HalfInningContext:
    """Initial game/half-inning context for half-inning relay parsing.

    Explicit Provenance:
    Derived from the official boxscore and fixture records for game 20240930NCHT0,
    top of the 9th inning (NC Dinos at KIA Tigers, Gwangju-Kia Champions Field).
    - Inning: 9, Half: top
    - Score at start of half-inning: Home (KIA) 10, Away (NC) 5
    - Active relief pitcher entering 9th top: 최지민 (KIA Tigers)
    """

    inning: int = 9
    inning_half: str = "top"
    home_score: int = 10
    away_score: int = 5
    initial_outs: int = 0
    initial_runners: int = 0
    active_pitcher: str = "최지민"
    provenance: str = "boxscore_20240930NCHT0_inn9_top_kia_nc"


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


class RelayRevisionRecord(CheckpointBase):
    """Permanent audit ledger of applied relay event corrections/revisions."""

    __tablename__ = "_relay_revisions"

    revision_id = Column(String(64), primary_key=True)
    game_id = Column(String(20), nullable=False, index=True)
    target_event_seq = Column(Integer, nullable=False)
    target_provider_log_id = Column(String(64), nullable=True)
    original_description = Column(String(255), nullable=False)
    revised_description = Column(String(255), nullable=False)
    original_result_code = Column(String(64), nullable=True)
    revised_result_code = Column(String(64), nullable=True)
    payload_hash = Column(String(64), nullable=False)
    status = Column(String(32), nullable=False, default="APPLIED")
    created_at = Column(DateTime, nullable=False)


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


def extract_domain_entities(session: Session, game_id: str) -> dict[str, Any]:
    """Extract all substantive domain entities (Events, PBPs, Validation, Revisions) for verification."""
    events = session.query(GameEvent).filter(GameEvent.game_id == game_id).order_by(GameEvent.event_seq.asc()).all()
    pbps = (
        session.query(GamePlayByPlay)
        .filter(GamePlayByPlay.game_id == game_id)
        .order_by(GamePlayByPlay.source_row_index.asc())
        .all()
    )
    val = session.query(GameValidationMetrics).filter(GameValidationMetrics.game_id == game_id).first()
    revisions = (
        session.query(RelayRevisionRecord)
        .filter(RelayRevisionRecord.game_id == game_id)
        .order_by(RelayRevisionRecord.revision_id.asc())
        .all()
    )

    event_payloads = [
        {
            "event_seq": e.event_seq,
            "inning": e.inning,
            "inning_half": e.inning_half,
            "outs": e.outs,
            "batter_name": e.batter_name,
            "pitcher_name": e.pitcher_name,
            "description": e.description,
            "event_type": e.event_type,
            "result_code": e.result_code,
            "home_score": e.home_score,
            "away_score": e.away_score,
            "bases_before": e.bases_before,
            "bases_after": e.bases_after,
            "provider_log_id": e.provider_log_id,
        }
        for e in events
    ]

    pbp_payloads = [
        {
            "source_row_index": p.source_row_index,
            "inning": p.inning,
            "inning_half": p.inning_half,
            "play_description": p.play_description,
            "event_type": p.event_type,
            "result": p.result,
            "batter_name": p.batter_name,
            "pitcher_name": p.pitcher_name,
            "provider_log_id": p.provider_log_id,
        }
        for p in pbps
    ]

    payload_binding_raw = json.dumps(
        {"events": event_payloads, "pbps": pbp_payloads},
        sort_keys=True,
        ensure_ascii=False,
    ).encode("utf-8")
    # observed_event_pbp_state_sha256 verifies cryptographic state equivalence of normalized
    # in-memory/replayed events & PBPs against the golden baseline; does NOT assert coupling
    # to historical database validation records.
    observed_event_pbp_state_sha256 = hashlib.sha256(payload_binding_raw).hexdigest()

    revision_payloads = [
        {
            "revision_id": r.revision_id,
            "target_event_seq": r.target_event_seq,
            "target_provider_log_id": r.target_provider_log_id,
            "original_description": r.original_description,
            "revised_description": r.revised_description,
            "original_result_code": r.original_result_code,
            "revised_result_code": r.revised_result_code,
            "status": r.status,
            "payload_hash": r.payload_hash,
        }
        for r in revisions
    ]

    return {
        "game_id": game_id,
        "events": event_payloads,
        "pbps": pbp_payloads,
        "validation": {
            "validation_status": val.validation_status if val else None,
            "source_used": val.source_used if val else None,
            "observed_event_pbp_state_sha256": observed_event_pbp_state_sha256,
        },
        "revisions": revision_payloads,
    }


def compute_domain_state_hash(session: Session, game_id: str) -> str:
    """Compute a deep domain state hash over all 4 substantive entities."""
    full_entities = extract_domain_entities(session, game_id)
    raw = json.dumps(full_entities, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def init_ephemeral_database(
    db_path_or_url: str,
    *,
    allowed_root: Path | None = None,
) -> tuple[Engine, sessionmaker[Session]]:
    """Initialize an ephemeral SQLite database with strict path containment and isolation.

    Guarantees strict isolation:
    - Rejects production kbo_dev.db.
    - Rejects any database inside the repository data/ directory.
    - If allowed_root is provided, enforces that the resolved target path is strictly within allowed_root.
    """
    clean_target = Path(db_path_or_url.replace("sqlite:///", "")).resolve()

    if "kbo_dev.db" in clean_target.name:
        msg = f"Forbidden operation: Cannot run recovery engine on protected DB {db_path_or_url}"
        raise ValueError(msg)

    repo_data_dir = (Path(__file__).resolve().parents[2] / "data").resolve()
    if clean_target.is_relative_to(repo_data_dir):
        msg = f"Forbidden operation: Cannot run recovery engine inside repository data directory: {clean_target}"
        raise ValueError(msg)

    if allowed_root:
        resolved_allowed = allowed_root.resolve()
        if not clean_target.is_relative_to(resolved_allowed):
            msg = f"Path confinement violation: DB path {clean_target} is outside allowed root {resolved_allowed}"
            raise ValueError(msg)

    db_url = f"sqlite:///{clean_target}"
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


def parse_kbo_nodes_to_events(
    raw_nodes: list[dict[str, Any]],
    game_id: str,
    *,
    context: HalfInningContext | None = None,
) -> list[dict[str, Any]]:
    """Parse sealed KBO DOM leaf nodes into normalized baseball events dynamically."""
    ctx = context or HalfInningContext()

    result_nodes = [
        node for node in reversed(raw_nodes) if is_relay_result_event_text((node.get("text") or "").strip())
    ]

    state = {
        "current_inning": ctx.inning,
        "current_half": ctx.inning_half,
        "home_score": ctx.home_score,
        "away_score": ctx.away_score,
        "current_outs": ctx.initial_outs,
        "current_runners": ctx.initial_runners,
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
                "inning": ctx.inning,
                "inning_half": ctx.inning_half,
                "outs": outs_before,
                "batter_name": batter,
                "pitcher_name": ctx.active_pitcher,
                "description": text,
                "event_type": ev_type,
                "result_code": res_code,
                "rbi": 0,
                "bases_before": PBPCrawler._format_base_string(runners_before),  # noqa: SLF001
                "bases_after": PBPCrawler._format_base_string(state["current_runners"]),  # noqa: SLF001
                "home_score": ctx.home_score,
                "away_score": ctx.away_score,
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
        allowed_root: Path | None = None,
        lock_dir: Path | None = None,
        context: HalfInningContext | None = None,
        verify_checksums: bool = True,
    ) -> None:
        """Initialize sealed relay recovery pipeline."""
        self.game_id = game_id
        self.db_path_or_url = db_path_or_url
        self.kbo_fixture_path = kbo_fixture_path
        self.naver_fixture_path = naver_fixture_path
        self.allowed_root = allowed_root
        self.context = context or HalfInningContext()
        self.verify_checksums = verify_checksums

        if lock_dir:
            self.lock_dir = lock_dir
        elif allowed_root:
            self.lock_dir = allowed_root / "locks"
        else:
            self.lock_dir = Path(__file__).resolve().parents[2] / "data" / "locks"

        if allowed_root and self.lock_dir:
            resolved_lock = self.lock_dir.resolve()
            resolved_allowed = allowed_root.resolve()
            if not resolved_lock.is_relative_to(resolved_allowed):
                msg = f"Path confinement violation: Lock dir {resolved_lock} is outside allowed root {resolved_allowed}"
                raise ValueError(msg)

        self.lock_dir.mkdir(parents=True, exist_ok=True)

        self.engine, self.session_factory = init_ephemeral_database(
            db_path_or_url,
            allowed_root=allowed_root,
        )
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
                    home_score=self.context.home_score,
                    away_score=self.context.away_score,
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

    def _find_matching_pbp_row(
        self,
        session: Session,
        ev: GameEvent,
        orig_desc: str | None,
    ) -> GamePlayByPlay | None:
        """Locate corresponding GamePlayByPlay row via provider_log_id or batter fallback.

        Strictly prohibits falling back to synthetic/numeric indices.
        If matching fails to find any candidate, returns None.
        If multiple candidates are found, raises ValueError to prevent ambiguous mutation.
        """
        target_pid = ev.provider_log_id
        pbp_query = session.query(GamePlayByPlay).filter(GamePlayByPlay.game_id == self.game_id)
        if target_pid is not None:
            pbp_rows = pbp_query.filter(GamePlayByPlay.provider_log_id == target_pid).all()
            if len(pbp_rows) > 1:
                msg = f"Ambiguous PBP match: {len(pbp_rows)} candidates found for provider_log_id '{target_pid}'"
                raise ValueError(msg)
            if len(pbp_rows) == 1:
                return pbp_rows[0]
            # If we get here, target_pid was provided but no match -> return None, do not fall back
            return None

        if orig_desc:
            pbp_rows = pbp_query.filter(
                GamePlayByPlay.batter_name == ev.batter_name,
                GamePlayByPlay.play_description == orig_desc,
            ).all()
            if len(pbp_rows) > 1:
                msg = (
                    f"Ambiguous PBP match: {len(pbp_rows)} candidates found for "
                    f"batter '{ev.batter_name}' and description '{orig_desc}'"
                )
                raise ValueError(msg)
            if len(pbp_rows) == 1:
                return pbp_rows[0]

        return None

    def apply_event_correction(
        self,
        *,
        revision_id: str = DEFAULT_REVISION_ID,
        target_event_seq: int = CORRECTION_TARGET_EVENT_SEQ,
        revised_description: str | None = None,
        revised_result_code: str | None = None,
        hook: CrashHook | None = None,
    ) -> dict[str, Any]:
        """Apply an idempotent in-place revision with permanent lineage in _relay_revisions."""
        with self.session_factory() as session:
            # 1. Inspect existing revision record
            existing_rev = (
                session.query(RelayRevisionRecord).filter(RelayRevisionRecord.revision_id == revision_id).first()
            )

            target_revised_code = revised_result_code or DEFAULT_REVISED_RESULT_CODE

            ev3 = (
                session.query(GameEvent)
                .filter(GameEvent.game_id == self.game_id, GameEvent.event_seq == target_event_seq)
                .first()
            )
            if not ev3:
                logger.warning("[CORRECTION] Target event seq=%d not found", target_event_seq)
                return {
                    "revision_id": revision_id,
                    "already_applied": False,
                    "mutations": 0,
                    "status": "EVENT_NOT_FOUND",
                }

            base_desc = ev3.description or ""
            if revised_description:
                target_revised_desc = revised_description
            elif DEFAULT_REVISED_DESCRIPTION_SUFFIX in base_desc:
                target_revised_desc = base_desc
            else:
                target_revised_desc = f"{base_desc}{DEFAULT_REVISED_DESCRIPTION_SUFFIX}"

            incoming_payload = {
                "target_event_seq": target_event_seq,
                "revised_description": target_revised_desc,
                "revised_result_code": target_revised_code,
            }
            incoming_hash = compute_state_hash(incoming_payload)

            if existing_rev:
                if existing_rev.payload_hash == incoming_hash:
                    logger.info(
                        "[CORRECTION] Revision %s already applied with identical payload; skipping mutation",
                        revision_id,
                    )
                    return {
                        "revision_id": revision_id,
                        "already_applied": True,
                        "mutations": 0,
                        "status": "ALREADY_APPLIED",
                    }
                msg = (
                    f"Revision conflict: revision {revision_id} already applied with different payload "
                    f"(existing hash={existing_rev.payload_hash[:8]}, incoming hash={incoming_hash[:8]})"
                )
                raise ValueError(msg)

            orig_desc = ev3.description
            orig_code = ev3.result_code

            # Strict ambiguity guard & fallback elimination:
            # Locate corresponding PBP before mutating any entity. If multiple candidates exist,
            # _find_matching_pbp_row raises ValueError (triggering rollback). If no candidates match,
            # abort immediately with 0 mutations without altering GameEvent or recording revision.
            pbp_row = self._find_matching_pbp_row(session, ev3, orig_desc)
            if not pbp_row:
                logger.warning(
                    "[CORRECTION] Matching GamePlayByPlay row not found for event seq=%d; aborting with 0 mutations",
                    target_event_seq,
                )
                return {
                    "revision_id": revision_id,
                    "already_applied": False,
                    "mutations": 0,
                    "status": "PBP_MATCH_FAILED",
                }

            # 2. Update GameEvent
            ev3.description = target_revised_desc
            ev3.result_code = target_revised_code

            # 3. Synchronize GamePlayByPlay via semantic provider_log_id link
            pbp_row.play_description = target_revised_desc
            pbp_row.result = target_revised_code
            matched_provider_log_id = pbp_row.provider_log_id or ev3.provider_log_id

            # 4. Insert permanent RelayRevisionRecord with preimage & provider_log_id binding
            rev_record = RelayRevisionRecord(
                revision_id=revision_id,
                game_id=self.game_id,
                target_event_seq=target_event_seq,
                target_provider_log_id=matched_provider_log_id,
                original_description=orig_desc or "",
                revised_description=target_revised_desc,
                original_result_code=orig_code,
                revised_result_code=target_revised_code,
                payload_hash=incoming_hash,
                status="APPLIED",
                created_at=datetime.now(UTC),
            )
            session.add(rev_record)
            session.flush()

            # CP7: Crash hook inside transaction after flush, before commit
            if hook:
                hook.trigger_if_matched("CP7_DURING_CORRECTION_UPDATE")

            session.commit()
            logger.info(
                "[CORRECTION] Committed revision %s to event %d with permanent ledger entry",
                revision_id,
                target_event_seq,
            )
            return {
                "revision_id": revision_id,
                "already_applied": False,
                "mutations": 1,
                "status": "APPLIED",
            }

    def _normalize_sources(
        self,
        state: PipelineState,
        *,
        empty_naver: bool,
        hook: CrashHook,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], str]:
        """Parse raw KBO and Naver snapshots into normalized events and PBP rows."""
        kbo_events = parse_kbo_nodes_to_events(state.kbo_raw_nodes, self.game_id, context=self.context)
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

    def _match_pbp_for_revision(
        self,
        revision: RelayRevisionRecord,
        raw_pbp_rows: list[dict[str, Any]],
    ) -> tuple[dict[str, Any] | None, str]:
        """Match a PBP row for a revision using strict ID-priority policy.

        Returns:
            (matched_pbp_row, match_status) where match_status is one of:
            - "MATCHED_BY_ID": matched via provider_log_id
            - "MATCHED_BY_FALLBACK": matched via description (ID genuinely None)
            - "NO_MATCH_ID": valid ID provided but no matching PBP row
            - "AMBIGUOUS_ID": valid ID matches multiple PBP rows
            - "INVALID_ID": target_provider_log_id is invalid (empty/whitespace)
            - "NO_MATCH_FALLBACK": ID None but no matching description
            - "AMBIGUOUS_FALLBACK": ID None but multiple matching descriptions

        """
        target_pid = revision.target_provider_log_id

        # 1. Valid ID provided -> ID-only matching
        if target_pid is not None:
            # Reject empty string, whitespace-only, invalid types
            if not isinstance(target_pid, str) or not target_pid.strip():
                return None, "INVALID_ID"
            candidates = [p for p in raw_pbp_rows if p.get("provider_log_id") == target_pid]
            if len(candidates) == 0:
                return None, "NO_MATCH_ID"
            if len(candidates) > 1:
                msg = f"Ambiguous PBP match: {len(candidates)} candidates for provider_log_id '{target_pid}'"
                raise ValueError(msg)
            return candidates[0], "MATCHED_BY_ID"

        # 2. ID genuinely None -> contracted fallback mapping only
        if revision.original_description:
            candidates = [p for p in raw_pbp_rows if p.get("play_description") == revision.original_description]
            if len(candidates) == 0:
                return None, "NO_MATCH_FALLBACK"
            if len(candidates) > 1:
                msg = (
                    f"Ambiguous fallback match: {len(candidates)} candidates for "
                    f"description '{revision.original_description}'"
                )
                raise ValueError(msg)
            return candidates[0], "MATCHED_BY_FALLBACK"

        return None, "NO_IDENTIFIER"

    def _apply_revisions_to_staged(
        self,
        canonical_events: list[dict[str, Any]],
        raw_pbp_rows: list[dict[str, Any]],
    ) -> None:
        """Apply committed permanent revisions to staged in-memory events and PBP rows."""
        with self.session_factory() as session:
            applied_revisions = (
                session.query(RelayRevisionRecord)
                .filter(
                    RelayRevisionRecord.game_id == self.game_id,
                    RelayRevisionRecord.status == "APPLIED",
                )
                .all()
            )

        if not applied_revisions:
            return

        # Apply to canonical events (match by event_seq or provider_log_id)
        for ev in canonical_events:
            seq = ev.get("event_seq")
            pid = ev.get("provider_log_id")
            for r in applied_revisions:
                if (seq is not None and seq == r.target_event_seq) or (
                    pid and r.target_provider_log_id and pid == r.target_provider_log_id
                ):
                    ev["description"] = r.revised_description
                    ev["result_code"] = r.revised_result_code
                    break

        # Apply to PBP rows using strict ID-priority policy
        for r in applied_revisions:
            matched_pbp, status = self._match_pbp_for_revision(r, raw_pbp_rows)
            if status in ("MATCHED_BY_ID", "MATCHED_BY_FALLBACK"):
                matched_pbp["play_description"] = r.revised_description
                matched_pbp["result"] = r.revised_result_code
            elif status == "INVALID_ID":
                msg = f"Invalid target_provider_log_id: {r.target_provider_log_id!r}"
                raise ValueError(msg)
            elif status in ("NO_MATCH_ID", "NO_MATCH_FALLBACK", "NO_IDENTIFIER"):
                # Log and skip - no mutation for this revision
                logger.warning(
                    "[REPLAY REVISION] Revision %s skipped: %s",
                    r.revision_id,
                    status,
                )
            # AMBIGUOUS_ID and AMBIGUOUS_FALLBACK raise ValueError from _match_pbp_for_revision

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

            # Replay preservation: apply committed revisions if present in DB
            self._apply_revisions_to_staged(canonical_events, raw_pbp_rows)

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
    parser.add_argument("--allowed-root", help="Directory for path confinement verification")
    parser.add_argument("--crash-point", choices=list(CRASH_POINTS), help="Crash hook point to simulate")
    parser.add_argument("--apply-correction", action="store_true", help="Apply in-place correction")
    parser.add_argument("--empty-naver", action="store_true", help="Simulate empty Naver payload")
    parser.add_argument("--no-verify-checksums", action="store_true", help="Disable fixture checksum verification")
    parser.add_argument("--lock-dir", help="Directory for process locks")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    allowed_root = Path(args.allowed_root).resolve() if args.allowed_root else None

    pipeline = SealedSnapshotRelayPipeline(
        game_id=args.game_id,
        db_path_or_url=args.db_path,
        kbo_fixture_path=Path(args.kbo_fixture),
        naver_fixture_path=Path(args.naver_fixture),
        allowed_root=allowed_root,
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
