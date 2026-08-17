from __future__ import annotations

from datetime import UTC, date, datetime

import pytest
import contextlib
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.base import Base
from src.repositories.crawl_evidence_repository import (
    build_relay_db_projection,
    compare_evidence_to_projection,
    load_json_artifact,
    record_crawl_evidence,
)


def test_record_and_compare_crawl_evidence(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("CRAWL_EVIDENCE_DIR", str(tmp_path / "evidence"))
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine)()

    payload = {"game_id": "20260718LGSS0", "rows": [{"value": 1}]}
    evidence = record_crawl_evidence(
        session,
        entity_type="game",
        entity_id="20260718LGSS0",
        dataset="detail",
        source_name="test",
        parsed_payload=payload,
        normalized_payload=payload,
        source_capture={
            "body": "<html>source</html>",
            "url": "https://example.test/game",
            "captured_at": datetime.now(UTC),
        },
    )
    session.commit()

    assert evidence.validation_status == "captured"
    assert len(evidence.raw_hash) == 64
    assert load_json_artifact(evidence.normalized_payload_path) == payload

    compare_evidence_to_projection(session, evidence.id, payload)
    session.commit()
    assert evidence.validation_status == "verified"
    assert evidence.db_projection_hash == evidence.normalized_hash

    compare_evidence_to_projection(session, evidence.id, {"game_id": "different"})
    session.commit()
    assert evidence.validation_status == "mismatch"
    assert evidence.diff_summary["count"] == 2


def test_build_relay_db_projection_uses_expected_fields():
    from src.models.game import Game, GameEvent, GamePlayByPlay

    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine)()
    session.add(
        Game(
            game_id="20260718LGSS0",
            game_date=date(2026, 7, 18),
            away_team="LG",
            home_team="SS",
        ),
    )
    session.add(
        GameEvent(
            game_id="20260718LGSS0",
            event_seq=2,
            inning=1,
            inning_half="top",
            description="single",
        ),
    )
    session.add(
        GamePlayByPlay(
            game_id="20260718LGSS0",
            inning=1,
            inning_half="top",
            play_description="single",
        ),
    )
    session.commit()

    projection = build_relay_db_projection(
        session,
        "20260718LGSS0",
        {
            "events": [{"event_seq": 2, "inning": 1, "description": "single"}],
            "raw_pbp_rows": [{"inning": 1, "play_description": "single"}],
        },
    )

    assert projection["events"] == [{"description": "single", "event_seq": 2, "inning": 1}]
    assert projection["raw_pbp_rows"] == [{"inning": 1, "play_description": "single"}]


def test_load_json_artifact_rejects_tampering(tmp_path):
    from src.repositories.crawl_evidence_repository import load_json_artifact

    path = tmp_path / "payload.json"
    path.write_text('{"value": 1}\n', encoding="utf-8")

    with pytest.raises(ValueError, match="hash mismatch"):
        load_json_artifact(str(path), "0" * 64)
