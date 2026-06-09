"""Tests for RankingAggregator — fielding, baserunning, batting, pitching rankings."""


from src.aggregators.ranking_aggregator import RankingAggregator


def _make_fielder(player_id=10001, name="홍길동", team="LG",
                  fielding_pct=0.980, putouts=50, assists=10, errors=2):
    return {
        "player_id": player_id,
        "player_name": name,
        "team_id": team,
        "team_code": team,
        "fielding_pct": fielding_pct,
        "putouts": putouts,
        "assists": assists,
        "errors": errors,
    }


def _make_batter(player_id=10001, name="홍길동", team="LG",
                 pa=300, ab=260, hits=80, doubles=15, triples=2, home_runs=10,
                 walks=30, hbp=5, sacrifice_flies=3, rbi=45, runs=50,
                 stolen_bases=5, caught_stealing=2, avg=0.308, obp=0.380,
                 slg=0.496, ops=0.876, iso=0.188, babip=0.320,
                 xr=45.0, woba=0.360, wrc_plus=120, war=3.5, ops_plus=115,
                 clutch=0.5, wpa_sum=1.2):
    return {
        "player_id": player_id,
        "player_name": name,
        "team_id": team,
        "team_code": team,
        "plate_appearances": pa,
        "at_bats": ab,
        "hits": hits,
        "doubles": doubles,
        "triples": triples,
        "home_runs": home_runs,
        "walks": walks,
        "hbp": hbp,
        "sacrifice_flies": sacrifice_flies,
        "rbi": rbi,
        "runs": runs,
        "stolen_bases": stolen_bases,
        "caught_stealing": caught_stealing,
        "avg": avg,
        "obp": obp,
        "slg": slg,
        "ops": ops,
        "iso": iso,
        "babip": babip,
        "xr": xr,
        "woba": woba,
        "wrc_plus": wrc_plus,
        "war": war,
        "ops_plus": ops_plus,
        "clutch": clutch,
        "wpa_sum": wpa_sum,
    }


def _make_pitcher(player_id=20001, name="김투수", team="SS",
                  ip=180, ip_outs=540, era=3.50, whip=1.20,
                  fip=3.40, k_per_nine=8.0, bb_per_nine=2.5, kbb=3.2,
                  war_pitch=4.0, wins=12, saves=0, holds=0,
                  innings_outs=540):
    return {
        "player_id": player_id,
        "player_name": name,
        "team_id": team,
        "team_code": team,
        "innings_pitched": ip,
        "innings_outs": ip_outs,
        "era": era,
        "whip": whip,
        "fip": fip,
        "k_per_nine": k_per_nine,
        "bb_per_nine": bb_per_nine,
        "kbb": kbb,
        "war_pitch": war_pitch,
        "wins": wins,
        "saves": saves,
        "holds": holds,
    }


class TestRankingAggregatorFielding:
    def test_fielding_rankings_basic(self):
        agg = RankingAggregator()
        rows = [_make_fielder(player_id=10001, fielding_pct=0.990),
                _make_fielder(player_id=10002, fielding_pct=0.970)]
        results = agg.generate_rankings(2025, fielding_stats=rows, persist=False)
        fld = [r for r in results if r["metric"] == "fielding_pct"]
        assert len(fld) == 2
        assert fld[0]["entity_id"] == "10001"
        assert fld[0]["rank"] == 1
        assert fld[1]["entity_id"] == "10002"
        assert fld[1]["rank"] == 2

    def test_fielding_errors_ascending(self):
        agg = RankingAggregator()
        rows = [_make_fielder(player_id=10001, errors=1),
                _make_fielder(player_id=10002, errors=3)]
        results = agg.generate_rankings(2025, fielding_stats=rows, persist=False)
        err = [r for r in results if r["metric"] == "errors"]
        assert len(err) == 2
        assert err[0]["value"] == 1
        assert err[0]["rank"] == 1
        assert err[1]["value"] == 3
        assert err[1]["rank"] == 2

    def test_fielding_empty(self):
        agg = RankingAggregator()
        assert agg.generate_rankings(2025, fielding_stats=[], persist=False) == []


class TestRankingAggregatorBaserunning:
    def test_baserunning_rankings(self):
        agg = RankingAggregator()
        rows = [
            {"player_id": 10001, "player_name": "A", "stolen_bases": 30,
             "stolen_base_percentage": 0.85, "caught_stealing": 5},
            {"player_id": 10002, "player_name": "B", "stolen_bases": 20,
             "stolen_base_percentage": 0.75, "caught_stealing": 8},
        ]
        results = agg.generate_rankings(2025, baserunning_stats=rows, persist=False)
        sb = [r for r in results if r["metric"] == "stolen_bases"]
        assert sb[0]["entity_id"] == "10001"
        assert sb[1]["entity_id"] == "10002"


class TestRankingAggregatorBatting:
    def test_batting_rankings_basic(self):
        agg = RankingAggregator()
        rows = [_make_batter(player_id=10001, avg=0.320),
                _make_batter(player_id=10002, avg=0.280)]
        results = agg.generate_rankings(2025, batting_stats=rows, persist=False, min_pa=1)
        avg = [r for r in results if r["metric"] == "avg"]
        assert len(avg) == 2
        assert avg[0]["entity_id"] == "10001"
        assert avg[0]["value"] == 0.320

    def test_batting_ascending_metrics(self):
        agg = RankingAggregator()
        rows = [_make_batter(player_id=10001, avg=0.280),
                _make_batter(player_id=10002, avg=0.320)]
        results = agg.generate_rankings(2025, batting_stats=rows, persist=False, min_pa=1)
        avg = [r for r in results if r["metric"] == "avg"]
        assert avg[0]["entity_id"] == "10002"
        assert avg[0]["rank"] == 1
        assert avg[1]["entity_id"] == "10001"

    def test_batting_min_pa_filter(self):
        agg = RankingAggregator()
        rows = [_make_batter(player_id=10001, pa=500, avg=0.300),
                _make_batter(player_id=10002, pa=10, avg=0.350)]
        results = agg.generate_rankings(2025, batting_stats=rows, persist=False, min_pa=100)
        avg = [r for r in results if r["metric"] == "avg"]
        assert len(avg) == 1
        assert avg[0]["entity_id"] == "10001"

    def test_batting_all_config_generated(self):
        agg = RankingAggregator()
        rows = [_make_batter(player_id=10001, pa=500, avg=0.300)]
        results = agg.generate_rankings(2025, batting_stats=rows, persist=False, min_pa=100)
        metrics = {r["metric"] for r in results}
        assert "avg" in metrics
        assert "avg_all" in metrics

    def test_batting_all_config_no_pa_filter(self):
        agg = RankingAggregator()
        rows = [_make_batter(player_id=10001, pa=500, avg=0.300)]
        results = agg.generate_rankings(2025, batting_stats=rows, persist=False)
        metrics = {r["metric"] for r in results}
        assert "avg" in metrics
        assert "avg_all" not in metrics

    def test_batting_non_ratio_metric_no_all(self):
        agg = RankingAggregator()
        rows = [_make_batter(player_id=10001, home_runs=25)]
        results = agg.generate_rankings(2025, batting_stats=rows, persist=False, min_pa=100)
        metrics = {r["metric"] for r in results}
        assert "home_runs" in metrics
        assert "home_runs_all" not in metrics

    def test_batting_sorts_by_hr(self):
        agg = RankingAggregator()
        rows = [_make_batter(player_id=10001, home_runs=30, pa=500),
                _make_batter(player_id=10002, home_runs=20, pa=500)]
        results = agg.generate_rankings(2025, batting_stats=rows, persist=False, min_pa=100)
        hr = [r for r in results if r["metric"] == "home_runs"]
        assert hr[0]["entity_id"] == "10001"

    def test_batting_saber_metric_from_extra_stats(self):
        agg = RankingAggregator()
        rows = [
            {"player_id": 10001, "player_name": "A", "team_id": "LG",
             "plate_appearances": 500, "extra_stats": {"wrc_plus": 130}},
            {"player_id": 10002, "player_name": "B", "team_id": "SS",
             "plate_appearances": 500, "extra_stats": {"wrc_plus": 110}},
        ]
        results = agg.generate_rankings(2025, batting_stats=rows, persist=False, min_pa=100)
        wrc = [r for r in results if r["metric"] == "wrc_plus"]
        assert len(wrc) == 2
        assert wrc[0]["entity_id"] == "10001"
        assert wrc[0]["value"] == 130


class TestRankingAggregatorPitching:
    def test_pitching_rankings_basic(self):
        agg = RankingAggregator()
        rows = [_make_pitcher(player_id=20001, era=2.80),
                _make_pitcher(player_id=20002, era=4.50)]
        results = agg.generate_rankings(2025, pitching_stats=rows, persist=False, min_ip_outs=1)
        era = [r for r in results if r["metric"] == "era"]
        assert len(era) == 2
        assert era[0]["entity_id"] == "20001"
        assert era[0]["rank"] == 1

    def test_pitching_ascending_era(self):
        agg = RankingAggregator()
        rows = [_make_pitcher(player_id=20001, era=3.00),
                _make_pitcher(player_id=20002, era=2.50)]
        results = agg.generate_rankings(2025, pitching_stats=rows, persist=False, min_ip_outs=1)
        era = [r for r in results if r["metric"] == "era"]
        assert era[0]["entity_id"] == "20002"
        assert era[1]["entity_id"] == "20001"
        assert era[0]["rank"] == 1
        assert era[1]["rank"] == 2

    def test_pitching_wins_descending(self):
        agg = RankingAggregator()
        rows = [_make_pitcher(player_id=20001, wins=15),
                _make_pitcher(player_id=20002, wins=10)]
        results = agg.generate_rankings(2025, pitching_stats=rows, persist=False, min_ip_outs=1)
        wins = [r for r in results if r["metric"] == "wins"]
        assert wins[0]["entity_id"] == "20001"

    def test_pitching_all_config_generated(self):
        agg = RankingAggregator()
        rows = [_make_pitcher(player_id=20001, ip_outs=200)]
        results = agg.generate_rankings(2025, pitching_stats=rows, persist=False, min_ip_outs=100)
        metrics = {r["metric"] for r in results}
        assert "era" in metrics
        assert "era_all" in metrics

    def test_pitching_min_ip_outs_filter(self):
        agg = RankingAggregator()
        rows = [_make_pitcher(player_id=20001, ip_outs=500, era=3.00),
                _make_pitcher(player_id=20002, ip_outs=10, era=2.00)]
        results = agg.generate_rankings(2025, pitching_stats=rows, persist=False, min_ip_outs=100)
        era = [r for r in results if r["metric"] == "era"]
        assert len(era) == 1
        assert era[0]["entity_id"] == "20001"

    def test_pitching_non_ratio_no_all(self):
        agg = RankingAggregator()
        rows = [_make_pitcher(player_id=20001, wins=10)]
        results = agg.generate_rankings(2025, pitching_stats=rows, persist=False, min_ip_outs=100)
        metrics = {r["metric"] for r in results}
        assert "wins" in metrics
        assert "wins_all" not in metrics


class TestRankingAggregatorTies:
    def test_ties_same_rank(self):
        agg = RankingAggregator()
        rows = [_make_fielder(player_id=10001, fielding_pct=0.990),
                _make_fielder(player_id=10002, fielding_pct=0.990)]
        results = agg.generate_rankings(2025, fielding_stats=rows, persist=False)
        fld = [r for r in results if r["metric"] == "fielding_pct"]
        assert fld[0]["rank"] == 1
        assert fld[1]["rank"] == 1
        assert fld[0]["is_tie"] is False
        assert fld[1]["is_tie"] is True

    def test_ties_after_tie(self):
        agg = RankingAggregator()
        rows = [_make_fielder(player_id=10001, fielding_pct=0.990),
                _make_fielder(player_id=10002, fielding_pct=0.990),
                _make_fielder(player_id=10003, fielding_pct=0.980)]
        results = agg.generate_rankings(2025, fielding_stats=rows, persist=False)
        fld = [r for r in results if r["metric"] == "fielding_pct"]
        assert fld[0]["rank"] == 1
        assert fld[1]["rank"] == 1
        assert fld[2]["rank"] == 3


class TestRankingAggregatorEntityLabel:
    def test_fallback_to_player_id(self):
        agg = RankingAggregator()
        rows = [{"player_name": "", "fielding_pct": 0.980}]
        results = agg.generate_rankings(2025, fielding_stats=rows, persist=False)
        assert len(results) == 0  # No player_id or player_name

    def test_uses_player_id_when_no_name(self):
        agg = RankingAggregator()
        rows = [{"player_id": 99999, "fielding_pct": 0.990}]
        results = agg.generate_rankings(2025, fielding_stats=rows, persist=False)
        fld = [r for r in results if r["metric"] == "fielding_pct"]
        assert len(fld) == 1
        assert fld[0]["entity_id"] == "99999"


class TestRankingAggregatorNullHandling:
    def test_none_value_skipped(self):
        agg = RankingAggregator()
        rows = [_make_fielder(player_id=10001, fielding_pct=None)]
        results = agg.generate_rankings(2025, fielding_stats=rows, persist=False)
        fld = [r for r in results if r["metric"] == "fielding_pct"]
        assert len(fld) == 0

    def test_null_player_id_and_name_skipped(self):
        agg = RankingAggregator()
        rows = [{"fielding_pct": 0.990}]
        results = agg.generate_rankings(2025, fielding_stats=rows, persist=False)
        assert len(results) == 0


class TestRankingAggregatorSourceField:
    def test_source_field_set_correctly(self):
        agg = RankingAggregator()
        rows = [_make_fielder()]
        results = agg.generate_rankings(2025, fielding_stats=rows, persist=False)
        for r in results:
            assert r["source"] == "FIELDING"

        batters = [_make_batter()]
        bat_results = agg.generate_rankings(2025, batting_stats=batters, persist=False)
        for r in bat_results:
            assert r["source"] == "BATTING"

        pitchers = [_make_pitcher()]
        pit_results = agg.generate_rankings(2025, pitching_stats=pitchers, persist=False)
        for r in pit_results:
            assert r["source"] == "PITCHING"


class TestRankingAggregatorAllConfigs:
    def test_batting_all_has_no_qualified_flag(self):
        agg = RankingAggregator()
        rows = [_make_batter(player_id=10001, pa=500, avg=0.300)]
        results = agg.generate_rankings(2025, batting_stats=rows, persist=False, min_pa=100)
        avg_all = next(r for r in results if r["metric"] == "avg_all")
        assert avg_all["extra"]["rank_mode"] == "all"

    def test_pitching_all_has_no_qualified_flag(self):
        agg = RankingAggregator()
        rows = [_make_pitcher(player_id=20001, ip_outs=500)]
        results = agg.generate_rankings(2025, pitching_stats=rows, persist=False, min_ip_outs=100)
        era_all = next(r for r in results if r["metric"] == "era_all")
        assert era_all["extra"]["rank_mode"] == "all"
