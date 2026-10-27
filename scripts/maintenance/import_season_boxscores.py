"""Turn externally obtained box-score CSVs into an archive payload plus manifest.

2001~2007 cannot be filled from inside the repository: the KBO GameCenter page
has the data but blocks every crawler (``Disallow: /``), Naver never served
those seasons, and the sources that do keep the numbers (the printed yearbook,
Sports2i's licensed database, news box scores) live outside it. What the
repository can own is the step after that: take whatever was obtained, check it
hard, and hand it to the existing ingestor in the shape it reads.

The checks are the point of this tool, not the conversion:

* an inning total that disagrees with the final score is refused, because that
  is what a mis-read OCR table looks like and a stored box score becomes the
  evidence for every later aggregate;
* a game whose id does not belong to the target season is refused, because the
  ingestor writes by ``game_id`` and a wrong one would edit another season;
* an inning row whose parent game is not in the file is refused, because the
  ingestor would insert it as an orphan;
* an authorization reference that is empty or a placeholder is refused, because
  the manifest is a provenance record and "unknown" records nothing.

The tool never writes to the database. It writes ``data/archives/kbo_<season>.json``
and ``kbo_<season>.manifest.json``; ``src.cli.collection.ingest_historical_archive``
reads both and performs the write.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

#: Fields the ingestor reads off each table. Anything else in the CSV is carried
#: through untouched so a later ingestor revision can pick it up without a re-import.
GAME_FIELDS = (
    "game_id",
    "game_date",
    "away_team",
    "home_team",
    "away_score",
    "home_score",
    "stadium",
    "game_status",
)
INNING_FIELDS = ("game_id", "team_side", "inning", "runs")
BATTING_FIELDS = (
    "game_id",
    "team_side",
    "player_id",
    "player_name",
    "team_code",
    "appearance_seq",
    "pa",
    "ab",
    "hits",
    "hr",
    "rbi",
    "bb",
)
PITCHING_FIELDS = ("game_id", "team_side", "player_id", "player_name", "team_code", "appearance_seq", "so")

TABLE_KEYS = {
    "games": (GAME_FIELDS, "games"),
    "innings": (INNING_FIELDS, "game_inning_scores"),
    "batting": (BATTING_FIELDS, "game_batting_stats"),
    "pitching": (PITCHING_FIELDS, "game_pitching_stats"),
}

NUMERIC_FIELDS = {
    "games": ("away_score", "home_score"),
    "innings": ("inning", "runs"),
    "batting": ("appearance_seq", "pa", "ab", "hits", "hr", "rbi", "bb"),
    "pitching": ("appearance_seq", "so"),
}

#: References that record nothing. The manifest exists to say who allowed the
#: copy, so an empty or placeholder value is a rejection rather than a default.
PLACEHOLDER_REFS = {"", "unknown", "n/a", "none", "tbd", "-"}

VALID_SIDES = {"away", "home"}


class BoxscoreImportError(Exception):
    """The input cannot become an archive payload."""


def _as_int(value: Any) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(float(text))
    except ValueError as exc:
        msg = f"not a number: {value!r}"
        raise BoxscoreImportError(msg) from exc


def _clean_rows(key: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    fields, _ = TABLE_KEYS[key]
    cleaned: list[dict[str, Any]] = []
    for row in rows:
        item: dict[str, Any] = {field: row.get(field) for field in fields}
        for field in NUMERIC_FIELDS[key]:
            item[field] = _as_int(row.get(field))
        cleaned.append(item)
    return cleaned


def load_csv_tables(paths: dict[str, Path]) -> dict[str, list[dict[str, Any]]]:
    """Read the four CSVs into raw rows, keyed by table name.

    Args:
        paths: Mapping of table name (``games``, ``innings``, ``batting``,
            ``pitching``) to its CSV path.

    Returns:
        One list of dicts per table, in file order.

    Raises:
        BoxscoreImportError: A file is missing or unreadable.

    """
    tables: dict[str, list[dict[str, Any]]] = {}
    for key, path in paths.items():
        if key not in TABLE_KEYS:
            msg = f"unknown table: {key}"
            raise BoxscoreImportError(msg)
        try:
            with Path(path).open(encoding="utf-8", newline="") as handle:
                tables[key] = list(csv.DictReader(handle))
        except OSError as exc:
            msg = f"cannot read {key} from {path}: {exc}"
            raise BoxscoreImportError(msg) from exc
    return tables


def _validate_games(games: list[dict[str, Any]], season: int) -> None:
    seen: set[str] = set()
    prefix = str(season)
    for game in games:
        game_id = str(game.get("game_id") or "").strip()
        if not game_id:
            msg = "game row without a game_id"
            raise BoxscoreImportError(msg)
        if not game_id.startswith(prefix):
            msg = f"game_id {game_id} does not belong to the target season {prefix}"
            raise BoxscoreImportError(msg)
        if game_id in seen:
            msg = f"duplicate game_id in games: {game_id}"
            raise BoxscoreImportError(msg)
        seen.add(game_id)
        if game.get("away_score") is None or game.get("home_score") is None:
            msg = f"{game_id} carries no final score; without one the file cannot be checked"
            raise BoxscoreImportError(msg)
        if not str(game.get("game_date") or "").strip():
            msg = f"{game_id} carries no game_date"
            raise BoxscoreImportError(msg)


def _validate_sides(key: str, rows: list[dict[str, Any]]) -> None:
    for row in rows:
        side = str(row.get("team_side") or "").strip()
        if side not in VALID_SIDES:
            msg = f"{key} row for {row.get('game_id')} has team_side={side!r}; expected away or home"
            raise BoxscoreImportError(msg)


def _validate_children(key: str, rows: list[dict[str, Any]], game_ids: set[str]) -> None:
    for row in rows:
        game_id = str(row.get("game_id") or "").strip()
        if game_id not in game_ids:
            msg = f"{key} row references unknown game {game_id}"
            raise BoxscoreImportError(msg)


def _validate_inning_totals(
    games: list[dict[str, Any]],
    innings: list[dict[str, Any]],
) -> None:
    totals: dict[str, dict[str, int]] = {}
    for row in innings:
        game_id = str(row["game_id"])
        side = str(row["team_side"])
        runs = row.get("runs")
        if runs is None:
            msg = f"inning row for {game_id} {side} has no runs"
            raise BoxscoreImportError(msg)
        totals.setdefault(game_id, {"away": 0, "home": 0})[side] += runs

    for game in games:
        game_id = str(game["game_id"])
        if game_id not in totals:
            # No innings at all is "not supplied", which is not the same claim as
            # "supplied and wrong". A partial table elsewhere in the file still fails.
            continue
        away = totals[game_id]["away"]
        home = totals[game_id]["home"]
        if away != game["away_score"] or home != game["home_score"]:
            msg = (
                f"inning total for {game_id} is {away}-{home} but the final score is "
                f"{game['away_score']}-{game['home_score']}"
            )
            raise BoxscoreImportError(msg)


def build_archive_payload(tables: dict[str, list[dict[str, Any]]], *, season: int) -> dict[str, Any]:
    """Validate the tables and arrange them under the ingestor's payload keys.

    Args:
        tables: Raw rows from :func:`load_csv_tables`.
        season: Season the file is expected to cover.

    Returns:
        The payload ``ingest_historical_archive`` reads.

    Raises:
        BoxscoreImportError: Any check in the module docstring fails.

    """
    games = _clean_rows("games", tables.get("games") or [])
    innings = _clean_rows("innings", tables.get("innings") or [])
    batting = _clean_rows("batting", tables.get("batting") or [])
    pitching = _clean_rows("pitching", tables.get("pitching") or [])

    if not games:
        msg = "games table is empty"
        raise BoxscoreImportError(msg)

    _validate_games(games, season)
    for key, rows in (("innings", innings), ("batting", batting), ("pitching", pitching)):
        _validate_sides(key, rows)

    game_ids = {str(game["game_id"]) for game in games}
    for key, rows in (("innings", innings), ("batting", batting), ("pitching", pitching)):
        _validate_children(key, rows, game_ids)

    _validate_inning_totals(games, innings)

    return {
        "games": games,
        "game_inning_scores": innings,
        "game_batting_stats": batting,
        "game_pitching_stats": pitching,
    }


def build_manifest(
    *,
    season: int,
    digest: str,
    source_name: str,
    source_url: str,
    authorization_ref: str,
) -> dict[str, Any]:
    """Build the manifest ``ingest_historical_archive`` requires.

    Args:
        season: Season covered by the archive.
        digest: SHA-256 of the archive file.
        source_name: Human-readable name of where the data came from.
        source_url: Address of the source, or a description of the medium.
        authorization_ref: Recorded permission (library visit, licence, ticket).

    Returns:
        The manifest mapping.

    Raises:
        BoxscoreImportError: A field is missing or the reference is a placeholder.

    """
    ref = str(authorization_ref or "").strip()
    if ref.lower() in PLACEHOLDER_REFS:
        msg = "authorization_ref must record a real permission; placeholders are refused"
        raise BoxscoreImportError(msg)
    for label, value in (("source_name", source_name), ("source_url", source_url)):
        if not str(value or "").strip():
            msg = f"{label} is required"
            raise BoxscoreImportError(msg)
    return {
        "source_name": str(source_name).strip(),
        "source_url": str(source_url).strip(),
        "authorization_ref": ref,
        "sha256": digest,
        "season": season,
    }


def write_archive(
    payload: dict[str, Any],
    *,
    season: int,
    out_dir: Path,
    source_name: str,
    source_url: str,
    authorization_ref: str,
) -> tuple[Path, Path]:
    """Write the archive and its manifest, digesting the bytes that were written.

    The digest is taken from the file on disk rather than from the payload, so a
    manifest can never describe bytes that are not there.

    Args:
        payload: The mapping from :func:`build_archive_payload`.
        season: Season covered by the archive.
        out_dir: Directory to write both files into.
        source_name: Provenance source name.
        source_url: Provenance source address.
        authorization_ref: Recorded permission.

    Returns:
        ``(archive_path, manifest_path)``.

    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    archive_path = out_dir / f"kbo_{season}.json"
    manifest_path = out_dir / f"kbo_{season}.manifest.json"

    archive_path.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
    digest = hashlib.sha256(archive_path.read_bytes()).hexdigest()
    manifest = build_manifest(
        season=season,
        digest=digest,
        source_name=source_name,
        source_url=source_url,
        authorization_ref=authorization_ref,
    )
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=1), encoding="utf-8")
    return archive_path, manifest_path


def main(argv: list[str] | None = None) -> int:
    """Run the converter."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--games", type=Path, required=True)
    parser.add_argument("--innings", type=Path)
    parser.add_argument("--batting", type=Path)
    parser.add_argument("--pitching", type=Path)
    parser.add_argument("--out-dir", type=Path, default=Path("data/archives"))
    parser.add_argument("--source-name", required=True)
    parser.add_argument("--source-url", required=True)
    parser.add_argument("--authorization-ref", required=True)
    args = parser.parse_args(argv)

    paths = {"games": args.games}
    for key in ("innings", "batting", "pitching"):
        value = getattr(args, key)
        if value:
            paths[key] = value

    try:
        tables = load_csv_tables(paths)
        payload = build_archive_payload(tables, season=args.season)
        archive_path, manifest_path = write_archive(
            payload,
            season=args.season,
            out_dir=args.out_dir,
            source_name=args.source_name,
            source_url=args.source_url,
            authorization_ref=args.authorization_ref,
        )
    except BoxscoreImportError as exc:
        print(f"[refused] {exc}")
        return 1

    counts = {key: len(rows) for key, rows in payload.items()}
    print(f"archive:  {archive_path}")
    print(f"manifest: {manifest_path}")
    print(f"rows:     {counts}")
    print()
    print("next:")
    print(
        "  python3 -m src.cli.collection.ingest_historical_archive "
        f"--file {archive_path} --manifest {manifest_path} --season {args.season} --dry-run",
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
