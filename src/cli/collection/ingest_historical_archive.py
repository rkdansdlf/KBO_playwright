"""CLI for ingesting historical archive."""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import sys
from dataclasses import dataclass
from datetime import date as dt_date
from pathlib import Path
from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from src.db.engine import SessionLocal
from src.models.game import Game, GameBattingStat, GameInningScore, GameMetadata, GamePitchingStat

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


@dataclass
class IngestSummary:
    """Historical archive ingest summary."""

    season: int
    games_ingested: int
    innings_records_ingested: int
    game_batting_records_ingested: int
    game_pitching_records_ingested: int
    batting_records_ingested: int
    pitching_records_ingested: int
    source_name: str
    provenance_verified: bool
    status: str


class HistoricalArchiveIngestor:
    """Ingestor for historical archives."""

    def __init__(self, session: Session) -> None:
        """Initialize with DB session."""
        self.session = session

    def _upsert_games(self, games: list[dict[str, Any]], src_name: str) -> None:
        for g in games:
            raw_date = g.get("game_date")
            parsed_date = dt_date.fromisoformat(raw_date) if isinstance(raw_date, str) else raw_date
            existing_game = self.session.execute(select(Game).where(Game.game_id == g["game_id"])).scalar_one_or_none()
            if existing_game:
                existing_game.game_date = parsed_date
                existing_game.away_team = g.get("away_team")
                existing_game.home_team = g.get("home_team")
                existing_game.away_score = g.get("away_score")
                existing_game.home_score = g.get("home_score")
                existing_game.stadium = g.get("stadium")
                existing_game.game_status = g.get("game_status", "COMPLETED")
            else:
                self.session.add(
                    Game(
                        game_id=g["game_id"],
                        game_date=parsed_date,
                        away_team=g.get("away_team"),
                        home_team=g.get("home_team"),
                        away_score=g.get("away_score"),
                        home_score=g.get("home_score"),
                        stadium=g.get("stadium"),
                        game_status=g.get("game_status", "COMPLETED"),
                    )
                )

            existing_meta = self.session.execute(
                select(GameMetadata).where(GameMetadata.game_id == g["game_id"])
            ).scalar_one_or_none()
            if existing_meta:
                existing_meta.source_payload = {"source_name": src_name or "archive"}
            else:
                self.session.add(
                    GameMetadata(
                        game_id=g["game_id"],
                        source_payload={"source_name": src_name or "archive"},
                    )
                )

    def _upsert_innings(self, innings: list[dict[str, Any]]) -> None:
        for inn in innings:
            existing_inn = self.session.execute(
                select(GameInningScore).where(
                    GameInningScore.game_id == inn["game_id"],
                    GameInningScore.team_side == inn.get("team_side", "away"),
                    GameInningScore.inning == inn.get("inning", 1),
                )
            ).scalar_one_or_none()
            if existing_inn:
                existing_inn.runs = inn.get("runs", 0)
            else:
                self.session.add(
                    GameInningScore(
                        game_id=inn["game_id"],
                        team_side=inn.get("team_side", "away"),
                        inning=inn.get("inning", 1),
                        runs=inn.get("runs", 0),
                    )
                )

    def _upsert_batting(self, batting: list[dict[str, Any]]) -> None:
        for b in batting:
            existing_bat = self.session.execute(
                select(GameBattingStat).where(
                    GameBattingStat.game_id == b["game_id"],
                    GameBattingStat.player_id == b.get("player_id"),
                    GameBattingStat.appearance_seq == b.get("appearance_seq", 1),
                )
            ).scalar_one_or_none()
            if existing_bat:
                existing_bat.plate_appearances = b.get("pa", 0)
                existing_bat.at_bats = b.get("ab", 0)
                existing_bat.hits = b.get("hits", 0)
                existing_bat.home_runs = b.get("hr", 0)
                existing_bat.rbi = b.get("rbi", 0)
                existing_bat.walks = b.get("bb", 0)
            else:
                self.session.add(
                    GameBattingStat(
                        game_id=b["game_id"],
                        team_side=b.get("team_side", "home"),
                        player_id=b.get("player_id"),
                        player_name=b.get("player_name", ""),
                        team_code=b.get("team_code", ""),
                        appearance_seq=b.get("appearance_seq", 1),
                        plate_appearances=b.get("pa", 0),
                        at_bats=b.get("ab", 0),
                        hits=b.get("hits", 0),
                        home_runs=b.get("hr", 0),
                        rbi=b.get("rbi", 0),
                        walks=b.get("bb", 0),
                    )
                )

    def _upsert_pitching(self, pitching: list[dict[str, Any]]) -> None:
        for p in pitching:
            existing_pit = self.session.execute(
                select(GamePitchingStat).where(
                    GamePitchingStat.game_id == p["game_id"],
                    GamePitchingStat.player_id == p.get("player_id"),
                    GamePitchingStat.appearance_seq == p.get("appearance_seq", 1),
                )
            ).scalar_one_or_none()
            if existing_pit:
                existing_pit.strikeouts = p.get("so", 0)
            else:
                self.session.add(
                    GamePitchingStat(
                        game_id=p["game_id"],
                        team_side=p.get("team_side", "home"),
                        player_id=p.get("player_id"),
                        player_name=p.get("player_name", ""),
                        team_code=p.get("team_code", ""),
                        appearance_seq=p.get("appearance_seq", 1),
                        strikeouts=p.get("so", 0),
                    )
                )

    def ingest_season_archive(
        self,
        season: int,
        payload: dict[str, Any],
        provenance: dict[str, Any] | None = None,
        source_name: str | None = None,
        *,
        dry_run: bool = False,
    ) -> IngestSummary:
        """Ingest season archive payload."""
        if not provenance and not source_name:
            error_msg = "provenance is required"
            raise ValueError(error_msg)
        src_name = source_name or (provenance.get("source_name") if provenance else "")
        if src_name == "fixture":
            error_msg = "Synthetic archive payload"
            raise ValueError(error_msg)

        games = payload.get("games", [])
        innings = payload.get("game_inning_scores", [])
        batting = payload.get("game_batting_stats", [])
        pitching = payload.get("game_pitching_stats", [])
        player_batting = payload.get("player_season_batting", [])
        player_pitching = payload.get("player_season_pitching", [])

        if not dry_run:
            self._upsert_games(games, src_name)
            self._upsert_innings(innings)
            self._upsert_batting(batting)
            self._upsert_pitching(pitching)
            self.session.flush()

        return IngestSummary(
            season=season,
            games_ingested=len(games),
            innings_records_ingested=len(innings),
            game_batting_records_ingested=len(batting),
            game_pitching_records_ingested=len(pitching),
            batting_records_ingested=len(player_batting),
            pitching_records_ingested=len(player_pitching),
            source_name=src_name or "archive",
            provenance_verified=bool(provenance or source_name),
            status="DRY_RUN" if dry_run else "SUCCESS",
        )


KBO_FOUNDING_YEAR = 1982


def main(argv: list[str] | None = None) -> int:
    """Run historical archive ingestion CLI."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", type=str, required=True)
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--manifest", type=str)
    parser.add_argument("--source-name", type=str, default="kbo_official_archive")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    file_bytes = Path(args.file).read_bytes()
    payload = json.loads(file_bytes.decode("utf-8"))

    if args.season >= KBO_FOUNDING_YEAR and not args.manifest:
        return 2

    if args.manifest:
        manifest = json.loads(Path(args.manifest).read_text(encoding="utf-8"))
        if manifest.get("sha256") != hashlib.sha256(file_bytes).hexdigest():
            return 2
        provenance = manifest
    else:
        provenance = {"source_name": args.source_name}

    with SessionLocal() as session:
        ingestor = HistoricalArchiveIngestor(session)
        summary = ingestor.ingest_season_archive(
            args.season,
            payload,
            provenance=provenance,
            source_name=args.source_name,
            dry_run=args.dry_run,
        )
        if not args.dry_run:
            session.commit()

    if args.json:
        sys.stdout.write(json.dumps(dataclasses.asdict(summary)) + "\n")

    return 0


if __name__ == "__main__":
    sys.exit(main())
