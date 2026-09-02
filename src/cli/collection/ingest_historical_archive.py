"""CLI for ingesting historical archive."""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import re
import sys
from dataclasses import dataclass
from datetime import date as dt_date
from pathlib import Path
from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from src.models.game import Game, GameBattingStat, GameInningScore, GameMetadata, GamePitchingStat
from src.models.player import PlayerSeasonBatting, PlayerSeasonPitching
from src.models.team_stats import TeamSeasonBatting, TeamSeasonPitching

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


HISTORICAL_ARCHIVE_SOURCE = "OFFICIAL_ARCHIVE"


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
    team_batting_records_ingested: int
    team_pitching_records_ingested: int
    source_name: str
    provenance_verified: bool
    status: str


def _summary_for_payload(
    season: int,
    payload: dict[str, Any],
    *,
    source_name: str,
    provenance: dict[str, Any] | None,
    dry_run: bool,
) -> IngestSummary:
    """Build an ingest summary without opening a database connection."""
    return IngestSummary(
        season=season,
        games_ingested=len(payload.get("games", [])),
        innings_records_ingested=len(payload.get("game_inning_scores", [])),
        game_batting_records_ingested=len(payload.get("game_batting_stats", [])),
        game_pitching_records_ingested=len(payload.get("game_pitching_stats", [])),
        batting_records_ingested=len(payload.get("player_season_batting", [])),
        pitching_records_ingested=len(payload.get("player_season_pitching", [])),
        team_batting_records_ingested=len(payload.get("team_season_batting", [])),
        team_pitching_records_ingested=len(payload.get("team_season_pitching", [])),
        source_name=source_name or "archive",
        provenance_verified=bool(provenance),
        status="DRY_RUN" if dry_run else "SUCCESS",
    )


@dataclass(frozen=True, slots=True)
class _SeasonRowSpec:
    """Describe the natural identity and aliases for one season-stat model."""

    model: type[Any]
    unique_fields: tuple[str, ...]
    season: int
    aliases: dict[str, tuple[str, ...]]
    defaults: dict[str, Any]


class HistoricalArchiveIngestor:
    """Ingestor for historical archives."""

    def __init__(self, session: Session) -> None:
        """Initialize with DB session."""
        self.session = session

    def _upsert_games(
        self,
        games: list[dict[str, Any]],
        src_name: str,
        provenance: dict[str, Any] | None = None,
    ) -> None:
        source_payload = {
            "source_name": src_name or "archive",
            "provenance": dict(provenance or {}),
        }
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
                existing_meta.source_payload = source_payload
            else:
                self.session.add(
                    GameMetadata(
                        game_id=g["game_id"],
                        source_payload=source_payload,
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

    @staticmethod
    def _normalise_record(
        record: dict[str, Any],
        model: type[Any],
        *,
        season: int,
        aliases: dict[str, tuple[str, ...]],
        defaults: dict[str, Any],
    ) -> dict[str, Any]:
        """Map archive abbreviations to a model payload and discard unknown fields."""
        columns = set(model.__table__.columns.keys())
        values = {key: value for key, value in record.items() if key in columns and value is not None}
        for target, source_names in aliases.items():
            if target in values:
                continue
            for source_name in source_names:
                if record.get(source_name) is not None:
                    values[target] = record[source_name]
                    break
        values.setdefault("season", season)
        for key, value in defaults.items():
            values.setdefault(key, value)
        return {key: value for key, value in values.items() if key in columns}

    def _upsert_season_rows(
        self,
        records: list[dict[str, Any]],
        spec: _SeasonRowSpec,
    ) -> None:
        """Upsert season-level rows by their natural archive identity."""
        model = spec.model
        for record in records:
            record_season = record.get("season")
            if record_season is not None and record_season != spec.season:
                error_msg = f"Historical archive row has a mismatched season for {model.__tablename__}"
                raise ValueError(error_msg)
            values = self._normalise_record(
                record,
                model,
                season=spec.season,
                aliases=spec.aliases,
                defaults=spec.defaults,
            )
            if model in (TeamSeasonBatting, TeamSeasonPitching):
                values.setdefault("team_name", values.get("team_id", "archive"))
            if any(values.get(field) is None for field in spec.unique_fields):
                error_msg = f"Historical archive row is missing a natural key for {model.__tablename__}"
                raise ValueError(error_msg)
            key = {field: values[field] for field in spec.unique_fields}
            existing = self.session.execute(select(model).filter_by(**key)).scalar_one_or_none()
            if existing is None:
                self.session.add(model(**values))
            else:
                for field, value in values.items():
                    if field != "id":
                        setattr(existing, field, value)

    def _upsert_player_season_rows(
        self,
        player_batting: list[dict[str, Any]],
        player_pitching: list[dict[str, Any]],
        season: int,
    ) -> None:
        batting_aliases = {
            "plate_appearances": ("pa",),
            "at_bats": ("ab",),
            "runs": ("r",),
            "hits": ("h",),
            "doubles": ("2b",),
            "triples": ("3b",),
            "home_runs": ("hr",),
            "walks": ("bb",),
            "intentional_walks": ("ibb",),
            "strikeouts": ("so",),
            "stolen_bases": ("sb",),
            "caught_stealing": ("cs",),
            "sacrifice_hits": ("sh",),
            "sacrifice_flies": ("sf",),
            "gdp": ("gidp",),
        }
        pitching_aliases = {
            "innings_pitched": ("ip",),
            "innings_outs": ("outs",),
            "hits_allowed": ("h",),
            "runs_allowed": ("r",),
            "earned_runs": ("er",),
            "home_runs_allowed": ("hr",),
            "walks_allowed": ("bb",),
            "intentional_walks": ("ibb",),
            "hit_batters": ("hbp",),
            "strikeouts": ("so",),
            "games_started": ("gs",),
        }
        common_defaults = {"league": "REGULAR", "level": "KBO1"}
        self._upsert_season_rows(
            player_batting,
            _SeasonRowSpec(
                model=PlayerSeasonBatting,
                unique_fields=("player_id", "season", "league", "level", "team_code"),
                season=season,
                aliases=batting_aliases,
                defaults={**common_defaults, "source": HISTORICAL_ARCHIVE_SOURCE},
            ),
        )
        self._upsert_season_rows(
            player_pitching,
            _SeasonRowSpec(
                model=PlayerSeasonPitching,
                unique_fields=("player_id", "season", "league", "level", "team_code"),
                season=season,
                aliases=pitching_aliases,
                defaults={**common_defaults, "source": HISTORICAL_ARCHIVE_SOURCE},
            ),
        )

    def _upsert_team_season_rows(
        self,
        team_batting: list[dict[str, Any]],
        team_pitching: list[dict[str, Any]],
        season: int,
    ) -> None:
        batting_aliases = {
            "team_id": ("team_code",),
            "plate_appearances": ("pa",),
            "at_bats": ("ab",),
            "runs": ("r",),
            "hits": ("h",),
            "doubles": ("2b",),
            "triples": ("3b",),
            "home_runs": ("hr",),
            "walks": ("bb",),
            "intentional_walks": ("ibb",),
            "strikeouts": ("so",),
            "stolen_bases": ("sb",),
            "caught_stealing": ("cs",),
            "sacrifice_hits": ("sh",),
            "sacrifice_flies": ("sf",),
            "gdp": ("gidp",),
        }
        pitching_aliases = {
            "team_id": ("team_code",),
            "innings_pitched": ("ip",),
            "innings_outs": ("outs",),
            "runs_allowed": ("r",),
            "earned_runs": ("er",),
            "hits_allowed": ("h",),
            "home_runs_allowed": ("hr",),
            "walks_allowed": ("bb",),
            "strikeouts": ("so",),
            "ties": ("draws",),
        }
        common_defaults = {"league": "REGULAR"}
        for records, model, aliases in (
            (team_batting, TeamSeasonBatting, batting_aliases),
            (team_pitching, TeamSeasonPitching, pitching_aliases),
        ):
            self._upsert_season_rows(
                records,
                _SeasonRowSpec(
                    model=model,
                    unique_fields=("team_id", "season", "league"),
                    season=season,
                    aliases=aliases,
                    defaults=common_defaults,
                ),
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
        if src_name == "fixture" or (
            provenance and (provenance.get("data_class") == "synthetic_fixture" or provenance.get("verified") is False)
        ):
            error_msg = "Synthetic archive payload"
            raise ValueError(error_msg)

        games = payload.get("games", [])
        innings = payload.get("game_inning_scores", [])
        batting = payload.get("game_batting_stats", [])
        pitching = payload.get("game_pitching_stats", [])
        player_batting = payload.get("player_season_batting", [])
        player_pitching = payload.get("player_season_pitching", [])
        team_batting = payload.get("team_season_batting", [])
        team_pitching = payload.get("team_season_pitching", [])

        if not dry_run:
            self._upsert_games(games, src_name, provenance)
            self._upsert_innings(innings)
            self._upsert_batting(batting)
            self._upsert_pitching(pitching)
            self._upsert_player_season_rows(player_batting, player_pitching, season)
            self._upsert_team_season_rows(team_batting, team_pitching, season)
            self.session.flush()

        return _summary_for_payload(
            season,
            payload,
            source_name=src_name or "archive",
            provenance=provenance or ({"source_name": source_name} if source_name else None),
            dry_run=dry_run,
        )


KBO_FOUNDING_YEAR = 1982
MANIFEST_REQUIRED_FIELDS = ("source_name", "source_url", "authorization_ref", "sha256", "season")
_SHA256_PATTERN = re.compile(r"^[0-9a-fA-F]{64}$")


def _validate_manifest_metadata(
    manifest: object,
    *,
    season: int,
    file_digest: str,
) -> bool:
    """Validate manifest provenance fields and checksum."""
    if not isinstance(manifest, dict):
        return False
    if any(not manifest.get(field) for field in MANIFEST_REQUIRED_FIELDS):
        return False
    source_name = manifest["source_name"]
    source_url = manifest["source_url"]
    authorization_ref = manifest["authorization_ref"]
    digest = manifest["sha256"]
    return (
        isinstance(source_name, str)
        and isinstance(source_url, str)
        and source_url.startswith(("http://", "https://"))
        and isinstance(authorization_ref, str)
        and authorization_ref.strip().lower() not in {"unknown", "tbd", "n/a"}
        and isinstance(digest, str)
        and _SHA256_PATTERN.fullmatch(digest) is not None
        and digest.lower() == file_digest.lower()
        and manifest["season"] == season
    )


def _validate_archive_game_ids(payload: object, season: int) -> bool:
    """Validate the season and uniqueness of game IDs in an archive payload."""
    if not isinstance(payload, dict):
        return False
    payload_season = payload.get("season")
    if payload_season is not None and payload_season != season:
        return False
    games = payload.get("games", [])
    if not isinstance(games, list):
        return False
    game_ids = []
    valid = True
    for game in games:
        if not isinstance(game, dict) or not isinstance(game.get("game_id"), str):
            valid = False
            continue
        game_ids.append(game["game_id"])
    return (
        valid and len(game_ids) == len(set(game_ids)) and all(game_id.startswith(str(season)) for game_id in game_ids)
    )


def _validate_archive_manifest(
    manifest: object,
    *,
    season: int,
    file_digest: str,
    payload: object,
) -> bool:
    """Validate the provenance and identity contract for a historical archive."""
    return _validate_manifest_metadata(manifest, season=season, file_digest=file_digest) and _validate_archive_game_ids(
        payload,
        season,
    )


def _load_archive_payload(path: str) -> tuple[bytes, dict[str, Any]] | None:
    """Load a JSON archive payload, returning ``None`` for invalid input."""
    try:
        file_bytes = Path(path).read_bytes()
        payload = json.loads(file_bytes.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    return file_bytes, payload


def _load_provenance(
    manifest_path: str | None,
    *,
    season: int,
    file_bytes: bytes,
    payload: dict[str, Any],
    source_name: str,
) -> tuple[dict[str, Any], bool] | None:
    """Load and validate a manifest, or reject a historical write without one."""
    if not manifest_path:
        if season >= KBO_FOUNDING_YEAR:
            return None
        return {"source_name": source_name}, False
    try:
        manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not _validate_archive_manifest(
        manifest,
        season=season,
        file_digest=hashlib.sha256(file_bytes).hexdigest(),
        payload=payload,
    ):
        return None
    return manifest, True


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

    archive_input = _load_archive_payload(args.file)
    if archive_input is None:
        return 2
    file_bytes, payload = archive_input

    provenance_input = _load_provenance(
        args.manifest,
        season=args.season,
        file_bytes=file_bytes,
        payload=payload,
        source_name=args.source_name,
    )
    if provenance_input is None:
        return 2
    provenance, manifest_loaded = provenance_input

    source_name = str(provenance.get("source_name") or args.source_name)
    if args.dry_run:
        summary = _summary_for_payload(
            args.season,
            payload,
            source_name=source_name,
            provenance=provenance,
            dry_run=True,
        )
    else:
        from src.db.engine import SessionLocal

        try:
            with SessionLocal() as session:
                ingestor = HistoricalArchiveIngestor(session)
                summary = ingestor.ingest_season_archive(
                    args.season,
                    payload,
                    provenance=provenance,
                    source_name=None if manifest_loaded else args.source_name,
                    dry_run=False,
                )
                session.commit()
        except ValueError:
            return 2

    if args.json:
        sys.stdout.write(json.dumps(dataclasses.asdict(summary)) + "\n")

    return 0


if __name__ == "__main__":
    sys.exit(main())
