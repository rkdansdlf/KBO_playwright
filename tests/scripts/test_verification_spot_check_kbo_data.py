from scripts.verification.spot_check_kbo_data import SpotChecker


class TestSpotChecker:
    def test_log_mismatch(self):
        checker = SpotChecker(None)  # type: ignore[arg-type]
        checker.log_mismatch("Player", "123", "name", "Foo", "Bar")
        assert len(checker.mismatches) == 1
        m = checker.mismatches[0]
        assert m["category"] == "Player"
        assert m["id"] == "123"
        assert m["field"] == "name"
        assert m["db_value"] == "Foo"
        assert m["live_value"] == "Bar"
