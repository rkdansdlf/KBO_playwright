import pytest

pytest.importorskip("sqlalchemy")

from src.aggregators.ranking_aggregator import RankingAggregator

pytestmark = pytest.mark.integration


class DummyRankingRepository:
    def __init__(self):
        self.saved = []

    def save_rankings(self, rankings):
        self.saved.extend(rankings)
        return len(rankings)


def test_ranking_aggregator_batting_pitching_filters():
    repo = DummyRankingRepository()
    aggregator = RankingAggregator(repository=repo)

    batting_stats = [
        {"player_id": 1, "player_name": "Slugger A", "avg": 0.350, "plate_appearances": 500},
        {"player_id": 2, "player_name": "Slugger B", "avg": 0.380, "plate_appearances": 100}, # Should be filtered out if min_pa=400
        {"player_id": 3, "player_name": "Slugger C", "avg": 0.310, "plate_appearances": 450},
    ]
    pitching_stats = [
        {"player_id": 101, "player_name": "Ace A", "era": 2.50, "innings_outs": 450}, # 150 IP
        {"player_id": 102, "player_name": "Relief B", "era": 1.20, "innings_outs": 60}, # 20 IP - should be filtered if min_ip_outs=432
    ]

    # Test with PA filter (Qualified limit)
    rankings = aggregator.generate_rankings(
        season=2024,
        batting_stats=batting_stats,
        pitching_stats=pitching_stats,
        min_pa=446,
        min_ip_outs=432, 
        persist=False
    )

    # Batting AVG check
    avg_ranks = [r for r in rankings if r["metric"] == "avg"]
    assert len(avg_ranks) == 2  # Slugger B excluded
    assert avg_ranks[0]["entity_id"] == "1" # Slugger A (0.350)
    assert avg_ranks[1]["entity_id"] == "3" # Slugger C (0.310)

    # Pitching ERA check
    era_ranks = [r for r in rankings if r["metric"] == "era"]
    assert len(era_ranks) == 1  # Relief B excluded
    assert era_ranks[0]["entity_id"] == "101" # Ace A

def test_ranking_aggregator_handles_ties_and_sources():
    repo = DummyRankingRepository()
    aggregator = RankingAggregator(repository=repo)

    fielding_stats = [
        {"player_id": 10, "player_name": "Player A", "team_id": "LG", "fielding_pct": 0.992, "putouts": 200, "assists": 50, "errors": 3},
        {"player_id": 11, "player_name": "Player B", "team_id": "SS", "fielding_pct": 0.992, "putouts": 190, "assists": 65, "errors": 3},
        {"player_id": 12, "player_name": "Player C", "team_id": "NC", "fielding_pct": 0.980, "putouts": 210, "assists": 40, "errors": 5},
    ]
    
    rankings = aggregator.generate_rankings(
        season=2023,
        fielding_stats=fielding_stats,
        persist=True,
    )

    assert repo.saved
    fielding_pct_ranks = [r for r in rankings if r["metric"] == "fielding_pct"]
    assert fielding_pct_ranks[0]["rank"] == 1
    assert fielding_pct_ranks[1]["rank"] == 1
    assert fielding_pct_ranks[2]["rank"] == 3
