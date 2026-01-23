"""
Service for generating and managing KBO data embeddings.
"""
import os
import json
import logging
from typing import List, Dict, Any, Optional

import os
import json
import logging
from typing import List, Dict, Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

try:
    import openai
except ImportError:
    openai = None

try:
    import google.generativeai as genai
except ImportError:
    genai = None

from src.models.kbo_embedding import KBOEmbedding
from src.models.team import Team, TeamDailyRoster
from src.models.franchise import Franchise
from src.models.team_history import TeamHistory
from src.models.award import Award
from src.models.game import Game, GameBattingStat, GamePitchingStat, GameMetadata, GameInningScore, GameLineup
from src.models.player import Player, PlayerBasic, PlayerSeasonBatting, PlayerSeasonPitching, PlayerMovement

logger = logging.getLogger(__name__)

class EmbeddingService:
    def __init__(self, db_session: Session):
        self.db = db_session
        self.openai_api_key = os.getenv("OPENAI_API_KEY")
        self.gemini_api_key = os.getenv("GEMINI_API_KEY")
        
        self.provider = None
        self.client = None

        if self.openai_api_key and openai:
            self.client = openai.OpenAI(api_key=self.openai_api_key)
            self.provider = "openai"
        elif self.gemini_api_key:
            if self.gemini_api_key.startswith("sk-or-") and openai:
                # OpenRouter configuration
                self.client = openai.OpenAI(
                    api_key=self.gemini_api_key,
                    base_url="https://openrouter.ai/api/v1"
                )
                self.provider = "openai" # Use openai interface logic
            elif genai:
                genai.configure(api_key=self.gemini_api_key)
                self.provider = "gemini"
            else:
                 logger.warning("google-generativeai not installed but GEMINI_API_KEY present.")
        else:
            logger.warning("No valid API key found. Embedding generation will fail.")

    def get_embedding(self, text: str, model: str = None) -> List[float]:
        """Generate vector embedding for a single string."""
        if self.provider == "openai":
            # For OpenRouter, we might need a specific model they support.
            # trying default text-embedding-3-small, but if it fails, user might need to specify.
            if not model: model = "text-embedding-3-small"
            text = text.replace("\n", " ")
            return self.client.embeddings.create(input=[text], model=model).data[0].embedding
        elif self.provider == "gemini":
            if not model: model = "models/text-embedding-004"
            try:
                result = genai.embed_content(model=model, content=text)
                return result['embedding']
            except Exception as e:
                # Fallback or retry logic if needed
                logger.error(f"Gemini embedding failed: {e}")
                raise e
        else:
            raise ValueError("Embedding provider not initialized")

    def save_embedding(self, table_name: str, record_id: str, content: str, vector: List[float], metadata: Dict[str, Any] = None):
        """Save or update embedding in the database."""
        stmt = select(KBOEmbedding).where(
            KBOEmbedding.table_name == table_name,
            KBOEmbedding.record_id == record_id
        )
        existing = self.db.execute(stmt).scalar_one_or_none()
        
        if existing:
            existing.content = content
            existing.vector_data = vector
            existing.metadata_json = metadata
        else:
            embedding = KBOEmbedding(
                table_name=table_name,
                record_id=record_id,
                content=content,
                vector_data=vector,
                metadata_json=metadata
            )
            self.db.add(embedding)
        
        self.db.commit()

    # --- Serializers ---

    def serialize_player(self, player: Player) -> str:
        """Serialize a Player object to natural language."""
        lines = [f"Player: {player.kbo_person_id}"] # Fallback name/ID
        # Note: Player model doesn't have a direct 'name' field in the snippet I saw, 
        # it might be in PlayerBasic or PlayerIdentity. 
        # Checking Player class: it has birth_date, height, etc.
        # I'll add what I can.
        
        lines.append(f"Status: {player.status}")
        if player.birth_date:
            lines.append(f"Born: {player.birth_date}")
        if player.height_cm and player.weight_kg:
            lines.append(f"Physique: {player.height_cm}cm, {player.weight_kg}kg")
        if player.bats and player.throws:
            lines.append(f"Throws/Bats: {player.throws}/{player.bats}")
        
        return ". ".join(lines)

    def serialize_game(self, game: Game) -> str:
        """Serialize a Game object."""
        date_str = game.game_date.strftime("%Y-%m-%d")
        lines = [f"Game on {date_str} at {game.stadium}"]
        lines.append(f"{game.away_team} (Away) vs {game.home_team} (Home)")
        lines.append(f"Score: {game.away_score} - {game.home_score}")
        if game.winning_team:
            lines.append(f"Winner: {game.winning_team}")
        
        # Add summary if available via relationship? 
        # Assuming eager load or separate query, but for now base fields.
        
        return ". ".join(lines)

    # --- Team Related ---

    def serialize_team(self, team: Team) -> str:
        """Serialize a Team object."""
        lines = [f"Team: {team.team_name} ({team.team_short_name})"]
        lines.append(f"City: {team.city}")
        if team.founded_year:
            lines.append(f"Founded: {team.founded_year}")
        if team.stadium_name:
            lines.append(f"Home Stadium: {team.stadium_name}")
        if team.aliases:
            lines.append(f"Aliases: {', '.join(team.aliases)}")
        return ". ".join(lines)

    def serialize_franchise(self, franchise: Franchise) -> str:
        return f"Franchise: {franchise.name}, Current Code: {franchise.current_code}. {franchise.web_url}"

    def serialize_team_history(self, history: TeamHistory) -> str:
        return f"Team History ({history.season}): {history.team_name}. Rank: {history.ranking}. Stadium: {history.stadium}."

    def serialize_roster(self, roster: TeamDailyRoster) -> str:
        return f"Roster {roster.roster_date}: {roster.player_name} ({roster.position}) on Team {roster.team_code}."

    # --- Stats Related ---

    def serialize_player_season_batting(self, stat: PlayerSeasonBatting) -> str:
        lines = [f"Season Batting: ID {stat.player_id}, Year {stat.season}"]
        lines.append(f"Stats: AVG {stat.avg}, HR {stat.home_runs}, RBI {stat.rbi}, OPS {stat.ops}")
        lines.append(f"Games: {stat.games}, PA: {stat.plate_appearances}")
        return ". ".join(lines)

    def serialize_player_season_pitching(self, stat: PlayerSeasonPitching) -> str:
        lines = [f"Season Pitching: ID {stat.player_id}, Year {stat.season}"]
        lines.append(f"Stats: ERA {stat.era}, Wins {stat.wins}, Losses {stat.losses}, Saves {stat.saves}")
        lines.append(f"IP: {stat.innings_pitched}, K: {stat.strikeouts}")
        return ". ".join(lines)

    def serialize_game_batting_stat(self, stat: GameBattingStat) -> str:
        results = []
        if stat.hits > 0: results.append(f"{stat.hits} Hits")
        if stat.home_runs > 0: results.append(f"{stat.home_runs} HR")
        if stat.rbi > 0: results.append(f"{stat.rbi} RBI")
        result_str = ", ".join(results) if results else "No hits"
        return f"Game Batting ({stat.game_id}): {stat.player_name}. {result_str} in {stat.at_bats} AB."

    def serialize_game_pitching_stat(self, stat: GamePitchingStat) -> str:
        return f"Game Pitching ({stat.game_id}): {stat.player_name}. ERA {stat.era}, IP {stat.innings_pitched}, K {stat.strikeouts}, R {stat.runs_allowed}."

    # --- Metadata & Awards ---

    def serialize_award(self, award: Award) -> str:
        return f"Award: {award.award_name} ({award.season}). Winner: {award.player_name} ({award.team_name}). {award.category}."

    def serialize_movement(self, move: PlayerMovement) -> str:
        return f"Player Movement ({move.date}): {move.player_name} - {move.section}. Team: {move.team_code}. {move.remarks or ''}"

    def serialize_game_metadata(self, meta: GameMetadata) -> str:
        return f"Game Metadata ({meta.game_id}): Stadium {meta.stadium_name}, Attendance {meta.attendance}, Weather {meta.weather}."

