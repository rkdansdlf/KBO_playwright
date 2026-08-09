"""Persistence helpers for immutable crawl evidence."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from src.models.crawl_evidence import CrawlEvidence
from src.utils.data_lineage import canonical_json, diff_values, sha256_bytes, sha256_json

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_EVIDENCE_DIR = PROJECT_ROOT / "data" / "crawl_evidence"
MAX_DIFF_ITEMS = 50


def evidence_root() -> Path:
    """Return the configured content-addressed evidence directory."""
    configured = Path(os.getenv("CRAWL_EVIDENCE_DIR", str(DEFAULT_EVIDENCE_DIR))).expanduser()
    return configured if configured.is_absolute() else PROJECT_ROOT / configured


def _write_content(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_bytes() != content:
            msg = f"Evidence artifact collision: {path}"
            raise RuntimeError(msg)
        return
    temporary = path.with_suffix(f"{path.suffix}.tmp-{os.getpid()}")
    temporary.write_bytes(content)
    temporary.replace(path)


def _write_json_payload(root: Path, payload: object, digest: str) -> str:
    path = root / "payloads" / f"{digest}.json"
    _write_content(path, (canonical_json(payload) + "\n").encode("utf-8"))
    return str(path)


def _write_raw_artifact(root: Path, raw_body: str | bytes, digest: str) -> str:
    content = raw_body if isinstance(raw_body, bytes) else raw_body.encode("utf-8")
    path = root / "raw" / f"{digest}.bin"
    _write_content(path, content)
    return str(path)


def _capture_metadata(source_capture: dict[str, object] | None) -> dict[str, object]:
    if not source_capture:
        return {}
    metadata = {
        key: value
        for key, value in source_capture.items()
        if key not in {"body", "parsed_payload", "normalized_payload"}
    }
    return json.loads(canonical_json(metadata))


def record_crawl_evidence(  # noqa: PLR0913
    session: Session,
    *,
    entity_type: str,
    entity_id: str,
    dataset: str,
    source_name: str,
    parsed_payload: object,
    normalized_payload: object,
    source_capture: dict[str, object] | None = None,
    db_projection: object | None = None,
    parser_version: str | None = None,
    normalization_version: str = "v1",
) -> CrawlEvidence:
    """Persist immutable source, payload hashes, and an optional DB comparison."""
    root = evidence_root()
    parsed_hash = sha256_json(parsed_payload)
    normalized_hash = sha256_json(normalized_payload)
    parsed_path = _write_json_payload(root, parsed_payload, parsed_hash)
    normalized_path = _write_json_payload(root, normalized_payload, normalized_hash)

    raw_body = source_capture.get("body") if source_capture else None
    raw_hash = None
    raw_path = None
    if isinstance(raw_body, (str, bytes)):
        raw_content = raw_body if isinstance(raw_body, bytes) else raw_body.encode("utf-8")
        raw_hash = sha256_bytes(raw_content)
        raw_path = _write_raw_artifact(root, raw_content, raw_hash)

    db_hash = sha256_json(db_projection) if db_projection is not None else None
    differences = diff_values(normalized_payload, db_projection) if db_projection is not None else []
    status = "verified" if db_projection is not None and not differences else "mismatch" if differences else "captured"
    evidence = CrawlEvidence(
        entity_type=entity_type,
        entity_id=entity_id,
        dataset=dataset,
        source_name=source_name,
        source_url=_optional_string(source_capture.get("url") if source_capture else None),
        captured_at=_capture_time(source_capture),
        raw_artifact_path=raw_path,
        parsed_payload_path=parsed_path,
        normalized_payload_path=normalized_path,
        raw_hash=raw_hash,
        parsed_hash=parsed_hash,
        normalized_hash=normalized_hash,
        db_projection_hash=db_hash,
        parser_version=parser_version,
        normalization_version=normalization_version,
        validation_status=status,
        diff_summary={"count": len(differences), "items": differences[:MAX_DIFF_ITEMS]} if differences else None,
        capture_metadata=_capture_metadata(source_capture),
    )
    session.add(evidence)
    session.flush()
    return evidence


def load_json_artifact(path: str | None, expected_hash: str | None = None) -> object | None:
    """Load and optionally verify a JSON payload from an evidence path."""
    if not path:
        return None
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if expected_hash and sha256_json(payload) != expected_hash:
        msg = f"Evidence artifact hash mismatch: {path}"
        raise ValueError(msg)
    return payload


def compare_evidence_to_projection(session: Session, evidence_id: int, projection: object) -> CrawlEvidence:
    """Compare stored normalized evidence against a freshly built DB projection."""
    evidence = session.get(CrawlEvidence, evidence_id)
    if evidence is None:
        msg = f"Crawl evidence not found: {evidence_id}"
        raise ValueError(msg)
    expected = load_json_artifact(evidence.normalized_payload_path, evidence.normalized_hash)
    differences = diff_values(expected, projection)
    evidence.db_projection_hash = sha256_json(projection)
    evidence.validation_status = "verified" if not differences else "mismatch"
    evidence.diff_summary = {"count": len(differences), "items": differences[:MAX_DIFF_ITEMS]} if differences else None
    session.flush()
    return evidence


def build_game_detail_db_projection(
    session: Session,
    game_id: str,
    expected: dict[str, object],
) -> dict[str, object]:
    """Build a DB projection using the fields present in an expected detail payload."""
    from src.models.game import (
        Game,
        GameBattingStat,
        GameEvent,
        GameInningScore,
        GameLineup,
        GameMetadata,
        GamePitchingStat,
        GamePlayByPlay,
        GameSummary,
    )

    game = session.query(Game).filter(Game.game_id == game_id).one_or_none()
    metadata = session.query(GameMetadata).filter(GameMetadata.game_id == game_id).one_or_none()
    projection: dict[str, object] = {}
    if "game" in expected:
        projection["game"] = _model_fields(game, expected.get("game"))
    if "metadata" in expected:
        projection["metadata"] = _model_fields(metadata, expected.get("metadata"))
    model_map = {
        "innings": GameInningScore,
        "lineups": GameLineup,
        "batting": GameBattingStat,
        "pitching": GamePitchingStat,
        "summary": GameSummary,
        "events": GameEvent,
        "raw_pbp_rows": GamePlayByPlay,
    }
    for section, model in model_map.items():
        expected_rows = expected.get(section)
        if not isinstance(expected_rows, list):
            continue
        rows = session.query(model).filter(model.game_id == game_id).all()
        keys = _projection_keys(expected_rows)
        actual_rows = [{key: getattr(row, key, None) for key in keys} for row in rows]
        projection[section] = sorted(actual_rows, key=_projection_sort_key)
    return projection


def build_relay_db_projection(
    session: Session,
    game_id: str,
    expected: dict[str, object],
) -> dict[str, object]:
    """Build a DB projection for normalized relay event and PBP rows."""
    from src.models.game import GameEvent, GamePlayByPlay

    projection: dict[str, object] = {}
    for section, model in {"events": GameEvent, "raw_pbp_rows": GamePlayByPlay}.items():
        expected_rows = expected.get(section)
        if not isinstance(expected_rows, list):
            continue
        rows = session.query(model).filter(model.game_id == game_id).all()
        keys = _projection_keys(expected_rows)
        actual_rows = [{key: getattr(row, key, None) for key in keys} for row in rows]
        projection[section] = sorted(actual_rows, key=_projection_sort_key)
    return projection


def _model_fields(model: object | None, expected: object) -> dict[str, object]:
    if not isinstance(expected, dict):
        return {}
    return {key: getattr(model, key, None) if model is not None else None for key in expected}


def _projection_keys(rows: list[object]) -> list[str]:
    keys: set[str] = set()
    for row in rows:
        if isinstance(row, dict):
            keys.update(row)
    return sorted(key for key in keys if key not in {"id", "created_at", "updated_at"})


def _projection_sort_key(row: dict[str, object]) -> tuple[str, ...]:
    """Use domain ordering instead of JSON key ordering for row comparisons."""
    return (
        str(row.get("team_side") or ""),
        str(row.get("appearance_seq") or row.get("event_seq") or row.get("inning") or ""),
        str(row.get("player_id") or row.get("player_name") or row.get("description") or ""),
        str(row.get("summary_type") or ""),
    )


def _optional_string(value: object) -> str | None:
    return str(value) if value not in (None, "") else None


def _capture_time(source_capture: dict[str, object] | None) -> datetime:
    value = source_capture.get("captured_at") if source_capture else None
    if isinstance(value, datetime):
        return value.astimezone(UTC).replace(tzinfo=None) if value.tzinfo else value
    return datetime.now(UTC).replace(tzinfo=None)
