"""
Repository for saving RELAY (play-by-play) data.
"""
from typing import Dict, List, Any
from src.db.engine import SessionLocal
from src.models.game import GamePlayByPlay
from src.utils.safe_print import safe_print as print


def save_relay_data(game_id: str, innings_data: List[Dict[str, Any]]) -> int:
    """
    Save play-by-play data from RELAY section.

    Args:
        game_id: Game ID
        innings_data: List of inning dicts from relay_crawler

    Returns:
        Number of plays saved
    """
    if not innings_data:
        return 0

    saved_count = 0

    with SessionLocal() as session:
        # Delete existing plays for this game (clean slate)
        session.query(GamePlayByPlay).filter(
            GamePlayByPlay.game_id == game_id
        ).delete()

        for inning_data in innings_data:
            inning = inning_data.get('inning')
            half = inning_data.get('half')
            plays = inning_data.get('plays', [])

            for seq, play in enumerate(plays, start=1):
                pbp_record = GamePlayByPlay(
                    game_id=game_id,
                    inning=inning,
                    half=half,
                    play_seq=seq,
                    event_type=play.get('event_type', 'unknown'),
                    description=play.get('description', ''),
                    batter_name=play.get('batter'),
                    pitcher_name=play.get('pitcher'),
                    result=play.get('result'),
                    outs_after=play.get('outs'),
                    raw_data=play
                )

                session.add(pbp_record)
                saved_count += 1

        session.commit()

    return saved_count


def get_game_relay_summary(game_id: str) -> Dict[str, Any]:
    """
    Get summary statistics for a game's play-by-play data.

    Args:
        game_id: Game ID

    Returns:
        Dict with summary stats
    """
    with SessionLocal() as session:
        plays = session.query(GamePlayByPlay).filter(
            GamePlayByPlay.game_id == game_id
        ).all()

        if not plays:
            return {
                'game_id': game_id,
                'total_plays': 0,
                'innings': 0
            }

        innings_set = set((p.inning, p.half) for p in plays)

        return {
            'game_id': game_id,
            'total_plays': len(plays),
            'innings': len(innings_set),
            'event_types': {
                'batting': sum(1 for p in plays if p.event_type == 'batting'),
                'strikeout': sum(1 for p in plays if p.event_type == 'strikeout'),
                'walk': sum(1 for p in plays if p.event_type == 'walk'),
                'hit': sum(1 for p in plays if p.event_type == 'hit'),
                'home_run': sum(1 for p in plays if p.event_type == 'home_run'),
            }
        }
