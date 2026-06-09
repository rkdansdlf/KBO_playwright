# This script runs async Playwright; just verify imports and function signatures.
from scripts.investigations.check_2000s_data import check_historical_data, main


class TestCheckHistoricalData:
    def test_is_coroutine(self):
        import asyncio
        assert asyncio.iscoroutinefunction(check_historical_data)


def test_main_is_coroutine():
    import asyncio
    assert asyncio.iscoroutinefunction(main)
