from __future__ import annotations

from collections import defaultdict
from typing import Any

from sqlalchemy import func
from sqlalchemy.orm import Session

from src.models.game import Game, GameBattingStat, GamePitchingStat, PlayerGameBatting, PlayerGamePitching
from src.utils.game_status import COMPLETED_LIKE_GAME_STATUSES


def _compute_batting_rates(hits: int, at_bats: int, walks: int, hbp: int, sf: int,
                           strikeouts: int, doubles: int, triples: int, home_runs: int) -> dict[str, float]:
    ab = at_bats or 0
    pa_base = ab + walks + hbp + sf
    avg = round(hits / ab, 3) if ab > 0 else 0.0
    obp = round((hits + walks + hbp) / pa_base, 3) if pa_base > 0 else 0.0
    total_bases = hits + doubles + 2 * triples + 3 * home_runs
    slg = round(total_bases / ab, 3) if ab > 0 else 0.0
    ops = round(obp + slg, 3)
    iso = round(slg - avg, 3)
    babip = round((hits - home_runs) / (ab - strikeouts - home_runs + sf), 3) if (ab - strikeouts - home_runs + sf) > 0 else 0.0
    return {"avg": avg, "obp": obp, "slg": slg, "ops": ops, "iso": iso, "babip": babip}


def _compute_pitching_rates(total_outs: int, hits: int, bb: int, er: int, k: int, hr: int) -> dict[str, float]:
    ip = total_outs / 3.0
    era = round(er * 9 / ip, 2) if ip > 0 else 0.0
    whip = round((bb + hits) / ip, 2) if ip > 0 else 0.0
    fip = round((13 * hr + 3 * bb - 2 * k) / ip + 3.10, 2) if ip > 0 else 0.0
    k9 = round(k * 9 / ip, 2) if ip > 0 else 0.0
    bb9 = round(bb * 9 / ip, 2) if ip > 0 else 0.0
    kbb = round(k / bb, 2) if bb > 0 else 0.0
    return {"era": era, "whip": whip, "fip": fip, "k_per_nine": k9, "bb_per_nine": bb9, "kbb": kbb}


def aggregate_game_batting(session: Session, game_id: str) -> list[dict[str, Any]]:
    """Aggregate GameBattingStat rows into a single PlayerGameBatting row per player for a game."""
    game = session.query(Game).filter(Game.game_id == game_id).first()
    if not game or game.game_status not in COMPLETED_LIKE_GAME_STATUSES:
        return []

    rows = session.query(GameBattingStat).filter(GameBattingStat.game_id == game_id).all()
    if not rows:
        return []

    groups: dict[int, list[GameBattingStat]] = defaultdict(list)
    for r in rows:
        if r.player_id is not None:
            groups[r.player_id].append(r)

    results: list[dict[str, Any]] = []
    for player_id, appearances in groups.items():
        first = appearances[0]
        totals: dict[str, int] = defaultdict(int)
        any_starter = False
        for a in appearances:
            totals["plate_appearances"] += a.plate_appearances or 0
            totals["at_bats"] += a.at_bats or 0
            totals["runs"] += a.runs or 0
            totals["hits"] += a.hits or 0
            totals["doubles"] += a.doubles or 0
            totals["triples"] += a.triples or 0
            totals["home_runs"] += a.home_runs or 0
            totals["rbi"] += a.rbi or 0
            totals["walks"] += a.walks or 0
            totals["intentional_walks"] += a.intentional_walks or 0
            totals["hbp"] += a.hbp or 0
            totals["strikeouts"] += a.strikeouts or 0
            totals["stolen_bases"] += a.stolen_bases or 0
            totals["caught_stealing"] += a.caught_stealing or 0
            totals["sacrifice_hits"] += a.sacrifice_hits or 0
            totals["sacrifice_flies"] += a.sacrifice_flies or 0
            totals["gdp"] += a.gdp or 0
            if a.is_starter:
                any_starter = True

        rates = _compute_batting_rates(
            hits=totals["hits"],
            at_bats=totals["at_bats"],
            walks=totals["walks"],
            hbp=totals["hbp"],
            sf=totals["sacrifice_flies"],
            strikeouts=totals["strikeouts"],
            doubles=totals["doubles"],
            triples=totals["triples"],
            home_runs=totals["home_runs"],
        )

        results.append({
            "game_id": game_id,
            "player_id": player_id,
            "player_name": first.player_name,
            "team_side": first.team_side,
            "team_code": first.team_code,
            "batting_order": first.batting_order,
            "appearance_seq": first.appearance_seq,
            "position": first.position,
            "is_starter": any_starter,
            **totals,
            **rates,
        })
    return results


def aggregate_game_pitching(session: Session, game_id: str) -> list[dict[str, Any]]:
    """Aggregate GamePitchingStat rows into a single PlayerGamePitching row per player for a game."""
    game = session.query(Game).filter(Game.game_id == game_id).first()
    if not game or game.game_status not in COMPLETED_LIKE_GAME_STATUSES:
        return []

    rows = session.query(GamePitchingStat).filter(GamePitchingStat.game_id == game_id).all()
    if not rows:
        return []

    groups: dict[int, list[GamePitchingStat]] = defaultdict(list)
    for r in rows:
        if r.player_id is not None:
            groups[r.player_id].append(r)

    results: list[dict[str, Any]] = []
    for player_id, appearances in groups.items():
        first = appearances[0]
        totals: dict[str, int] = defaultdict(int)
        any_starting = False
        for a in appearances:
            totals["innings_outs"] += a.innings_outs or 0
            totals["hits_allowed"] += a.hits_allowed or 0
            totals["runs_allowed"] += a.runs_allowed or 0
            totals["earned_runs"] += a.earned_runs or 0
            totals["home_runs_allowed"] += a.home_runs_allowed or 0
            totals["walks_allowed"] += a.walks_allowed or 0
            totals["strikeouts"] += a.strikeouts or 0
            totals["hit_batters"] += a.hit_batters or 0
            totals["wild_pitches"] += a.wild_pitches or 0
            totals["balks"] += a.balks or 0
            totals["wins"] += a.wins or 0
            totals["losses"] += a.losses or 0
            totals["saves"] += a.saves or 0
            totals["holds"] += a.holds or 0
            totals["batters_faced"] += a.batters_faced or 0
            if a.is_starting:
                any_starting = True

        decision = next((a.decision for a in appearances if a.decision), None)
        rates = _compute_pitching_rates(
            total_outs=totals["innings_outs"],
            hits=totals["hits_allowed"],
            bb=totals["walks_allowed"],
            er=totals["earned_runs"],
            k=totals["strikeouts"],
            hr=totals["home_runs_allowed"],
        )

        results.append({
            "game_id": game_id,
            "player_id": player_id,
            "player_name": first.player_name,
            "team_side": first.team_side,
            "team_code": first.team_code,
            "is_starting": any_starting,
            "appearance_seq": first.appearance_seq,
            "decision": decision,
            **totals,
            **rates,
        })
    return results


def upsert_player_game_batting(session: Session, records: list[dict[str, Any]]) -> int:
    if not records:
        return 0
    from sqlalchemy.dialects.sqlite import insert as sqlite_insert

    conflict_keys = ["game_id", "player_id"]
    count = 0
    for data in records:
        stmt = sqlite_insert(PlayerGameBatting).values(**data)
        stmt = stmt.on_conflict_do_update(
            index_elements=conflict_keys,
            set_={k: stmt.excluded[k] for k in data if k not in conflict_keys},
        )
        session.execute(stmt)
        count += 1
    session.commit()
    return count


def upsert_player_game_pitching(session: Session, records: list[dict[str, Any]]) -> int:
    if not records:
        return 0
    from sqlalchemy.dialects.sqlite import insert as sqlite_insert

    conflict_keys = ["game_id", "player_id"]
    count = 0
    for data in records:
        stmt = sqlite_insert(PlayerGamePitching).values(**data)
        stmt = stmt.on_conflict_do_update(
            index_elements=conflict_keys,
            set_={k: stmt.excluded[k] for k in data if k not in conflict_keys},
        )
        session.execute(stmt)
        count += 1
    session.commit()
    return count
