"""
CLI tool to generate embeddings for KBO entities.
"""
import argparse
import logging
import sys
from typing import List

from sqlalchemy import select

from src.db.engine import get_db_session
from src.services.embedding_service import EmbeddingService
from src.models.team import Team, TeamDailyRoster
from src.models.franchise import Franchise
from src.models.team_history import TeamHistory
from src.models.player import Player, PlayerSeasonBatting, PlayerSeasonPitching, PlayerMovement
from src.models.award import Award
from src.models.game import Game, GameBattingStat, GamePitchingStat, GameMetadata
from src.models.kbo_embedding import KBOEmbedding

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ... existing generate functions ...

def generate_generic_embeddings(session, service: EmbeddingService, model_class, table_label: str, limit: int = 0, force: bool = False, serializer_method=None, id_attr='id'):
    """Generic generator for simple models."""
    logger.info(f"Fetching {table_label}...")
    stmt = select(model_class)
    records = session.scalars(stmt).all()
    
    count = 0
    for r in records:
        if limit > 0 and count >= limit:
            break
            
        record_id = str(getattr(r, id_attr))
        
        if not force:
            exists = session.execute(
                select(KBOEmbedding).where(
                    KBOEmbedding.table_name == table_label,
                    KBOEmbedding.record_id == record_id
                )
            ).scalar_one_or_none()
            if exists:
                continue

        if not serializer_method:
            continue
            
        text = serializer_method(r)
        
        try:
            vector = service.get_embedding(text)
            service.save_embedding(
                table_name=table_label,
                record_id=record_id,
                content=text,
                vector=vector,
                metadata={}
            )
            count += 1
            if count % 10 == 0:
                logger.info(f"Processed {count} {table_label}")
        except Exception as e:
            logger.error(f"Failed to process {table_label} {record_id}: {e}")

    logger.info(f"Finished. Processed {count} {table_label}.")

def main():
    parser = argparse.ArgumentParser(description="Generate embeddings for KBO data")
    parser.add_argument("--table", type=str, choices=['all', 'players', 'game', 'team', 'stats', 'meta', 'roster'], default='all', help="Target table group")
    parser.add_argument("--limit", type=int, default=0, help="Max records to process (0 for all)")
    parser.add_argument("--force", action="store_true", help="Regenerate existing embeddings")
    
    args = parser.parse_args()
    
    with get_db_session() as session:
        service = EmbeddingService(session)
        
        if args.table in ['players', 'all']:
            generate_player_embeddings(session, service, args.limit, args.force)
            
        if args.table in ['game', 'all']:
            generate_game_embeddings(session, service, args.limit, args.force)
            
        if args.table in ['team', 'all']:
            generate_generic_embeddings(session, service, Team, 'teams', args.limit, args.force, service.serialize_team, 'team_id')
            generate_generic_embeddings(session, service, Franchise, 'franchises', args.limit, args.force, service.serialize_franchise)
            generate_generic_embeddings(session, service, TeamHistory, 'team_history', args.limit, args.force, service.serialize_team_history)
            
        if args.table in ['stats', 'all']:
            generate_generic_embeddings(session, service, PlayerSeasonBatting, 'player_season_batting', args.limit, args.force, service.serialize_player_season_batting)
            generate_generic_embeddings(session, service, PlayerSeasonPitching, 'player_season_pitching', args.limit, args.force, service.serialize_player_season_pitching)
            
            # Game Stats might be too huge for 'all', maybe distinct? for now include.
            # Warning: Table size could be large.
            # Also GameStats tables have composite PKs usually, 'id' attribute works if integer PK exists.
            # Models have 'id' PK.
            generate_generic_embeddings(session, service, GameBattingStat, 'game_batting_stats', args.limit, args.force, service.serialize_game_batting_stat)
            generate_generic_embeddings(session, service, GamePitchingStat, 'game_pitching_stats', args.limit, args.force, service.serialize_game_pitching_stat)

        if args.table in ['roster', 'all']:
            generate_generic_embeddings(session, service, TeamDailyRoster, 'team_daily_roster', args.limit, args.force, service.serialize_roster)

        if args.table in ['meta', 'all']:
            generate_generic_embeddings(session, service, Award, 'awards', args.limit, args.force, service.serialize_award)
            generate_generic_embeddings(session, service, PlayerMovement, 'player_movements', args.limit, args.force, service.serialize_movement)
            # GameMetadata usually 1:1 with Game, PK is game_id
            generate_generic_embeddings(session, service, GameMetadata, 'game_metadata', args.limit, args.force, service.serialize_game_metadata, 'game_id')

if __name__ == "__main__":
    main()
