
import sys
import os
from sqlalchemy import text
from src.db.engine import SessionLocal
from src.models.game import Game, GameInningScore, GameLineup, GameBattingStat, GamePitchingStat, GameIdAlias
from datetime import datetime

# Import internal helpers if possible or recreate them for debugging
from src.repositories.game_repository import (
    _canonicalize_game_id, _record_game_id_alias, _has_game_child_rows,
    _resolve_game_season_id, _infer_team_code_from_children, _infer_score_from_children,
    _infer_pitcher_from_children, _resolve_winner, _resolve_terminal_status,
    _apply_game_team_identity, _enrich_existing_child_team_identity,
    GAME_STATUS_UNRESOLVED
)

def debug_repair(game_id):
    print(f"DEBUG: Starting repair for {game_id}")
    game_id, original_game_id = _canonicalize_game_id(game_id)
    if not game_id:
        print("DEBUG: Canonicalization failed")
        return False

    with SessionLocal() as session:
        try:
            _record_game_id_alias(
                session,
                original_game_id,
                game_id,
                source="parent_repair",
                reason="normalized_to_kbo_legacy_game_id",
            )
            print(f"DEBUG: Alias recorded for {game_id}")
            
            child_models = (GameInningScore, GameLineup, GameBattingStat, GamePitchingStat)
            has_children = False
            for model in child_models:
                exists = _has_game_child_rows(session, model, game_id)
                print(f"DEBUG: Model {model.__tablename__} has children: {exists}")
                if exists:
                    has_children = True
            
            if not has_children:
                print("DEBUG: No children found, aborting")
                return False

            try:
                game_date = datetime.strptime(game_id[:8], "%Y%m%d").date()
            except Exception as e:
                print(f"DEBUG: Date parse error: {e}")
                game_date = datetime.now().date()
            season_year = game_date.year
            print(f"DEBUG: Game Date: {game_date}, Year: {season_year}")

            game = session.query(Game).filter(Game.game_id == game_id).one_or_none()
            if not game:
                print("DEBUG: Game not found, creating stub")
                game = Game(game_id=game_id, game_date=game_date)
                session.add(game)
                session.flush()
            else:
                print("DEBUG: Existing game found")

            game.game_date = game_date
            season_id = _resolve_game_season_id(
                session,
                {"season_year": season_year, "season_type": "regular"},
                game_date,
                game.season_id,
            )
            print(f"DEBUG: Resolved season_id: {season_id}")
            if season_id:
                game.season_id = season_id

            away_team = _infer_team_code_from_children(session, game_id, "away", season_year)
            home_team = _infer_team_code_from_children(session, game_id, "home", season_year)
            print(f"DEBUG: Inferred teams: Away={away_team}, Home={home_team}")
            if away_team:
                game.away_team = away_team
            if home_team:
                game.home_team = home_team

            away_score = _infer_score_from_children(session, game_id, "away")
            home_score = _infer_score_from_children(session, game_id, "home")
            print(f"DEBUG: Inferred scores: Away={away_score}, Home={home_score}")
            if away_score is not None:
                game.away_score = away_score
            if home_score is not None:
                game.home_score = home_score

            # Infer starting pitchers
            away_pitcher = _infer_pitcher_from_children(session, game_id, "away")
            home_pitcher = _infer_pitcher_from_children(session, game_id, "home")
            print(f"DEBUG: Inferred pitchers: Away={away_pitcher}, Home={home_pitcher}")
            if away_pitcher:
                game.away_pitcher = away_pitcher
            if home_pitcher:
                game.home_pitcher = home_pitcher

            if game.home_score is not None and game.away_score is not None:
                print("DEBUG: Both scores present, resolving winner/status")
                game.winning_team, game.winning_score = _resolve_winner(
                    {"code": game.home_team, "score": game.home_score},
                    {"code": game.away_team, "score": game.away_score},
                )
                game.game_status = _resolve_terminal_status(game.home_score, game.away_score)
                print(f"DEBUG: Resolved status: {game.game_status}")
            elif not game.game_status:
                print("DEBUG: No scores, setting unresolved")
                game.game_status = GAME_STATUS_UNRESOLVED

            print("DEBUG: Applying team identity")
            _apply_game_team_identity(game, season_year)
            print("DEBUG: Enriching child identity")
            _enrich_existing_child_team_identity(session, game_id, season_year)
            print("DEBUG: Committing")
            session.commit()
            print("DEBUG: Success")
            return True
        except Exception as exc:
            session.rollback()
            print(f"DEBUG: DB Error: {exc}")
            import traceback
            traceback.print_exc()
            return False

if __name__ == "__main__":
    debug_repair("20200827KHLT0")
