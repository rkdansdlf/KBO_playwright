"""
Repository for saving game details, box scores, and normalized relay data.
"""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Dict, Any, List, Iterable

from src.db.engine import SessionLocal
from src.models.game import (
    Game,
    BoxScore,
    GameSummary,
    GamePlayByPlay,
    GameMetadata,
    GameInningScore,
    GameLineup,
    GameBattingStat,
    GamePitchingStat,
    GameEvent,
)
from src.utils.safe_print import safe_print as print


def save_schedule_game(game_data: Dict[str, Any]) -> bool:
    """Persist basic game info from schedule crawler."""
    game_id = game_data.get("game_id")
    if not game_id:
        return False

    game_date_str = str(game_data.get("game_date", "")).replace("-", "")
    try:
        game_date = datetime.strptime(game_date_str, "%Y%m%d").date()
    except Exception:
        return False

    with SessionLocal() as session:
        try:
            game = session.query(Game).filter(Game.game_id == game_id).one_or_none()
            if not game:
                game = Game(game_id=game_id)
                session.add(game)
            
            game.game_date = game_date
            game.home_team = game_data.get("home_team_code")
            game.away_team = game_data.get("away_team_code")
            game.season_id = game_data.get("season_year")
            
            # Note: Scores and other details are not available in basic schedule crawl
            session.commit()
            return True
        except Exception as exc:
            session.rollback()
            print(f"[ERROR] DB Error (Schedule): {exc}")
            return False


def save_game_detail(game_data: Dict[str, Any]) -> bool:
    """Persist full game snapshot including box score + player stats."""
    if not game_data:
        return False

    game_id = game_data["game_id"]
    game_date_str = str(game_data.get("game_date", "")).replace("-", "") or game_id[:8]
    try:
        game_date = datetime.strptime(game_date_str, "%Y%m%d").date()
    except Exception:
        game_date = datetime.now().date()

    metadata = game_data.get("metadata", {}) or {}
    teams = game_data.get("teams", {}) or {}
    hitters = game_data.get("hitters", {}) or {}
    pitchers = game_data.get("pitchers", {}) or {}

    with SessionLocal() as session:
        try:
            game = session.query(Game).filter(Game.game_id == game_id).one_or_none()
            if not game:
                game = Game(game_id=game_id)
                session.add(game)

            away_info = teams.get("away", {})
            home_info = teams.get("home", {})

            game.game_date = game_date
            game.stadium = metadata.get("stadium")
            game.home_team = home_info.get("code")
            game.away_team = away_info.get("code")
            game.home_score = home_info.get("score")
            game.away_score = away_info.get("score")
            game.winning_team, game.winning_score = _resolve_winner(home_info, away_info)
            season_id = game_data.get("season_id")
            if season_id:
                game.season_id = season_id

            box = session.query(BoxScore).filter(BoxScore.game_id == game_id).one_or_none()
            if not box:
                box = BoxScore(game_id=game_id)
                session.add(box)

            away_line = away_info.get("line_score") or []
            home_line = home_info.get("line_score") or []
            for i in range(1, 16):
                setattr(box, f"away_{i}", away_line[i - 1] if len(away_line) >= i else None)
                setattr(box, f"home_{i}", home_line[i - 1] if len(home_line) >= i else None)

            box.away_r = away_info.get("score")
            box.away_h = away_info.get("hits")
            box.away_e = away_info.get("errors")
            box.home_r = home_info.get("score")
            box.home_h = home_info.get("hits")
            box.home_e = home_info.get("errors")

            _upsert_metadata(session, game_id, metadata)
            _replace_records(session, GameInningScore, game_id, _build_inning_scores(game_id, teams))
            _replace_records(session, GameLineup, game_id, _build_lineups(game_id, hitters))
            _replace_records(session, GameBattingStat, game_id, _build_batting_stats(game_id, hitters))
            _replace_records(session, GamePitchingStat, game_id, _build_pitching_stats(game_id, pitchers))

            session.query(GameSummary).filter(GameSummary.game_id == game_id).delete()
            for category, items in (game_data.get("summary") or {}).items():
                for item in items:
                    session.add(
                        GameSummary(
                            game_id=game_id,
                            summary_type=category,
                            detail_text=item.get("content"),
                            player_name=item.get("player"),
                        )
                    )

            session.commit()
            return True
        except Exception as exc:
            session.rollback()
            print(f"[ERROR] DB Error (Detail): {exc}")
            return False


def save_relay_data(game_id: str, events: List[Dict[str, Any]]) -> int:
    """Persist both legacy play_by_play and normalized game_events."""
    if not events:
        return 0

    with SessionLocal() as session:
        try:
            session.query(GamePlayByPlay).filter(GamePlayByPlay.game_id == game_id).delete()
            session.query(GameEvent).filter(GameEvent.game_id == game_id).delete()

            pbp_rows = []
            event_rows = []
            for idx, event in enumerate(events, start=1):
                inning = event.get("inning")
                half = event.get("inning_half")
                pbp_rows.append(
                    GamePlayByPlay(
                        game_id=game_id,
                        inning=inning,
                        inning_half=half,
                        batter_name=event.get("batter"),
                        pitcher_name=event.get("pitcher"),
                        play_description=event.get("description"),
                        event_type=event.get("event_type"),
                        result=event.get("result"),
                    )
                )
                event_rows.append(
                    GameEvent(
                        game_id=game_id,
                        event_seq=event.get("event_seq") or idx,
                        inning=inning,
                        inning_half=half,
                        outs=event.get("outs"),
                        batter_name=event.get("batter"),
                        pitcher_name=event.get("pitcher"),
                        description=event.get("description"),
                        event_type=event.get("event_type"),
                        result_code=event.get("result_code") or event.get("result"),
                        rbi=event.get("rbi"),
                        bases_before=event.get("bases_before"),
                        bases_after=event.get("bases_after"),
                        extra_json=event.get("extra_json"),
                    )
                )

            session.add_all(pbp_rows + event_rows)
            session.commit()
            return len(events)
        except Exception as exc:
            session.rollback()
            print(f"[ERROR] DB Error (Relay): {exc}")
            return 0


def _upsert_metadata(session, game_id: str, metadata: Dict[str, Any]) -> None:
    meta = session.query(GameMetadata).filter(GameMetadata.game_id == game_id).one_or_none()
    if not meta:
        meta = GameMetadata(game_id=game_id)
        session.add(meta)

    meta.stadium_code = metadata.get("stadium_code")
    meta.stadium_name = metadata.get("stadium")
    meta.attendance = metadata.get("attendance")
    meta.start_time = _safe_time(metadata.get("start_time"))
    meta.end_time = _safe_time(metadata.get("end_time"))
    meta.game_time_minutes = metadata.get("duration_minutes")
    meta.weather = metadata.get("weather")
    meta.source_payload = metadata or None


def _replace_records(session, model, game_id: str, rows: Iterable[Any]) -> None:
    session.query(model).filter(model.game_id == game_id).delete()
    objects = list(rows)
    if objects:
        session.add_all(objects)


def _build_inning_scores(game_id: str, teams: Dict[str, Any]) -> List[GameInningScore]:
    records: List[GameInningScore] = []
    for side in ("away", "home"):
        team_info = teams.get(side, {}) or {}
        line_score = team_info.get("line_score") or []
        team_code = team_info.get("code")
        for idx, runs in enumerate(line_score, start=1):
            records.append(
                GameInningScore(
                    game_id=game_id,
                    team_side=side,
                    team_code=team_code,
                    inning=idx,
                    runs=runs if runs is not None else 0,
                    is_extra=idx > 9,
                )
            )
    return records


def _build_lineups(game_id: str, hitters: Dict[str, List[Dict[str, Any]]]) -> List[GameLineup]:
    records: List[GameLineup] = []
    for side, entries in hitters.items():
        for entry in entries:
            player_name = entry.get("player_name")
            if not player_name:
                continue
            records.append(
                GameLineup(
                    game_id=game_id,
                    team_side=side,
                    team_code=entry.get("team_code"),
                    player_id=_normalize_player_id(entry.get("player_id")),
                    player_name=player_name,
                    batting_order=entry.get("batting_order"),
                    position=entry.get("position"),
                    is_starter=bool(entry.get("is_starter")),
                    appearance_seq=entry.get("appearance_seq") or len(records) + 1,
                )
            )
    return records


def _build_batting_stats(game_id: str, hitters: Dict[str, List[Dict[str, Any]]]) -> List[GameBattingStat]:
    records: List[GameBattingStat] = []
    for side, entries in hitters.items():
        for entry in entries:
            player_name = entry.get("player_name")
            stats = entry.get("stats") or {}
            if not player_name:
                continue
            records.append(
                GameBattingStat(
                    game_id=game_id,
                    team_side=side,
                    team_code=entry.get("team_code"),
                    player_id=_normalize_player_id(entry.get("player_id")),
                    player_name=player_name,
                    batting_order=entry.get("batting_order"),
                    is_starter=bool(entry.get("is_starter")),
                    appearance_seq=entry.get("appearance_seq") or len(records) + 1,
                    position=entry.get("position"),
                    plate_appearances=_stat_int(stats, "plate_appearances"),
                    at_bats=_stat_int(stats, "at_bats"),
                    runs=_stat_int(stats, "runs"),
                    hits=_stat_int(stats, "hits"),
                    doubles=_stat_int(stats, "doubles"),
                    triples=_stat_int(stats, "triples"),
                    home_runs=_stat_int(stats, "home_runs"),
                    rbi=_stat_int(stats, "rbi"),
                    walks=_stat_int(stats, "walks"),
                    intentional_walks=_stat_int(stats, "intentional_walks"),
                    hbp=_stat_int(stats, "hbp"),
                    strikeouts=_stat_int(stats, "strikeouts"),
                    stolen_bases=_stat_int(stats, "stolen_bases"),
                    caught_stealing=_stat_int(stats, "caught_stealing"),
                    sacrifice_hits=_stat_int(stats, "sacrifice_hits"),
                    sacrifice_flies=_stat_int(stats, "sacrifice_flies"),
                    gdp=_stat_int(stats, "gdp"),
                    avg=_stat_float(stats, "avg"),
                    obp=_stat_float(stats, "obp"),
                    slg=_stat_float(stats, "slg"),
                    ops=_stat_float(stats, "ops"),
                    iso=_stat_float(stats, "iso"),
                    babip=_stat_float(stats, "babip"),
                    extra_stats=entry.get("extras"),
                )
            )
    return records


def _build_pitching_stats(game_id: str, pitchers: Dict[str, List[Dict[str, Any]]]) -> List[GamePitchingStat]:
    records: List[GamePitchingStat] = []
    for side, entries in pitchers.items():
        for entry in entries:
            player_name = entry.get("player_name")
            stats = entry.get("stats") or {}
            if not player_name:
                continue
            innings_outs = stats.get("innings_outs")
            records.append(
                GamePitchingStat(
                    game_id=game_id,
                    team_side=side,
                    team_code=entry.get("team_code"),
                    player_id=_normalize_player_id(entry.get("player_id")),
                    player_name=player_name,
                    is_starting=bool(entry.get("is_starting")),
                    appearance_seq=entry.get("appearance_seq") or len(records) + 1,
                    innings_outs=innings_outs,
                    innings_pitched=_outs_to_decimal(innings_outs),
                    batters_faced=_stat_int(stats, "batters_faced"),
                    pitches=_stat_int(stats, "pitches"),
                    hits_allowed=_stat_int(stats, "hits_allowed"),
                    runs_allowed=_stat_int(stats, "runs_allowed"),
                    earned_runs=_stat_int(stats, "earned_runs"),
                    home_runs_allowed=_stat_int(stats, "home_runs_allowed"),
                    walks_allowed=_stat_int(stats, "walks_allowed"),
                    strikeouts=_stat_int(stats, "strikeouts"),
                    hit_batters=_stat_int(stats, "hit_batters"),
                    wild_pitches=_stat_int(stats, "wild_pitches"),
                    balks=_stat_int(stats, "balks"),
                    wins=_stat_int(stats, "wins"),
                    losses=_stat_int(stats, "losses"),
                    saves=_stat_int(stats, "saves"),
                    holds=_stat_int(stats, "holds"),
                    decision=stats.get("decision"),
                    era=_stat_float(stats, "era"),
                    whip=_stat_float(stats, "whip"),
                    k_per_nine=_stat_float(stats, "k_per_nine"),
                    bb_per_nine=_stat_float(stats, "bb_per_nine"),
                    kbb=_stat_float(stats, "kbb"),
                    extra_stats=entry.get("extras"),
                )
            )
    return records


def _stat_int(stats: Dict[str, Any], key: str) -> int:
    value = stats.get(key)
    if value in (None, ""):
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _stat_float(stats: Dict[str, Any], key: str) -> Any:
    value = stats.get(key)
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_player_id(value: Any) -> Any:
    if value in (None, "", "null"):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _outs_to_decimal(outs: Any) -> Any:
    if outs in (None, "", 0):
        return Decimal("0") if outs in (0,) else None
    try:
        whole, remainder = divmod(int(outs), 3)
        return Decimal(whole) + (Decimal(remainder) / Decimal(3))
    except (TypeError, ValueError, ZeroDivisionError):
        return None


def _safe_time(value: Any):
    if not value:
        return None
    if isinstance(value, datetime):
        return value.time()
    try:
        parts = str(value).split(":")
        if len(parts) >= 2:
            return datetime.strptime(":".join(parts[:2]), "%H:%M").time()
    except Exception:
        return None
    return None


def _resolve_winner(home: Dict[str, Any], away: Dict[str, Any]) -> tuple[str | None, int | None]:
    """Determine winning team code and score based on box score."""
    home_score = home.get("score")
    away_score = away.get("score")
    if home_score is None or away_score is None:
        return None, None
    if home_score > away_score:
        return home.get("code"), home_score
    if away_score > home_score:
        return away.get("code"), away_score
    return None, home_score
