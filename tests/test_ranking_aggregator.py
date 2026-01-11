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


def test_ranking_aggregator_handles_ties_and_sources():
    repo = DummyRankingRepository()
    aggregator = RankingAggregator(repository=repo)

    fielding_stats = [
        {"player_id": 10, "player_name": "Player A", "team_id": "LG", "fielding_pct": 0.992, "putouts": 200, "assists": 50, "errors": 3},
        {"player_id": 11, "player_name": "Player B", "team_id": "SS", "fielding_pct": 0.992, "putouts": 190, "assists": 65, "errors": 3},
        {"player_id": 12, "player_name": "Player C", "team_id": "NC", "fielding_pct": 0.980, "putouts": 210, "assists": 40, "errors": 5},
    ]
    baserunning_stats = [
        {"player_id": 20, "player_name": "Runner A", "team_id": "KT", "stolen_bases": 30, "stolen_base_percentage": 88.2, "caught_stealing": 4},
        {"player_id": 21, "player_name": "Runner B", "team_id": "HH", "stolen_bases": 28, "stolen_base_percentage": 88.2, "caught_stealing": 3},
        {"player_id": 22, "player_name": "Runner C", "team_id": "OB", "stolen_bases": 10, "stolen_base_percentage": 66.6, "caught_stealing": 8},
    ]

    rankings = aggregator.generate_rankings(
        season=2023,
        fielding_stats=fielding_stats,
        baserunning_stats=baserunning_stats,
        persist=True,
    )

    assert repo.saved  # persisted
    fielding_pct_ranks = [r for r in rankings if r["metric"] == "fielding_pct"]
    assert fielding_pct_ranks[0]["rank"] == 1
    assert fielding_pct_ranks[1]["rank"] == 1  # tie
    assert fielding_pct_ranks[2]["rank"] == 3  # skip rank 2
    assert fielding_pct_ranks[1]["is_tie"] is True

    caught = [r for r in rankings if r["metric"] == "caught_stealing"]
    assert caught[0]["value"] == 3  # ascending ranking
    assert caught[-1]["value"] == 8

    assert all(entry["season"] == 2023 for entry in rankings)
    assert all(entry["entity_type"] == "PLAYER" for entry in rankings)
