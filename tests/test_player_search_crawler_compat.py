import asyncio

from src.crawlers import player_search_crawler as module
from src.crawlers.player_search_crawler import (
    PlayerRow,
    PlayerSearchCrawler,
    parse_birth_date,
    player_row_to_dict,
)


def test_parse_birth_date_accepts_legacy_search_formats():
    assert parse_birth_date("1990.7.3").isoformat() == "1990-07-03"
    assert parse_birth_date("19900703").isoformat() == "1990-07-03"
    assert parse_birth_date("-") is None


def test_player_row_to_dict_preserves_legacy_payload_fields():
    row = PlayerRow(
        player_id=12345,
        uniform_no="7",
        name="홍길동",
        team="LG",
        position="감독",
        birth_date="1970.01.02",
        height_cm=180,
        weight_kg=80,
        career="서울고",
    )

    payload = player_row_to_dict(row)

    assert payload["player_id"] == 12345
    assert payload["birth_date_date"].isoformat() == "1970-01-02"
    assert payload["status"] == "staff"
    assert payload["staff_role"] == "manager"
    assert payload["status_source"] == "heuristic"
    assert PlayerSearchCrawler.row_to_dict(row) == payload


def test_module_crawl_all_players_delegates_to_class(monkeypatch):
    calls = {}
    expected_rows = [
        PlayerRow(
            player_id=42,
            uniform_no=None,
            name="테스트",
            team="KT",
            position="투수",
            birth_date=None,
            height_cm=None,
            weight_kg=None,
            career=None,
        )
    ]

    class FakeCrawler:
        def __init__(self, pool=None, request_delay=None, headless=None):
            calls["init"] = {
                "pool": pool,
                "request_delay": request_delay,
                "headless": headless,
            }

        async def crawl_all_players(self, max_pages=None):
            calls["max_pages"] = max_pages
            return expected_rows

    monkeypatch.setattr(module, "PlayerSearchCrawler", FakeCrawler)

    rows = asyncio.run(
        module.crawl_all_players(
            max_pages=3,
            headless=True,
            slow_mo=50,
            request_delay=1.5,
            pool="pool",
        )
    )

    assert rows == expected_rows
    assert calls == {
        "init": {
            "pool": "pool",
            "request_delay": 1.5,
            "headless": True,
        },
        "max_pages": 3,
    }


def test_trigger_postback_waits_for_load_state_after_click():
    class FakeAnchor:
        def __init__(self):
            self.click_timeout = None

        async def click(self, timeout):
            self.click_timeout = timeout

        async def get_attribute(self, *_args, **_kwargs):
            raise AssertionError("fallback should not run after click success")

    class FakePage:
        def __init__(self):
            self.wait_calls = []

        async def wait_for_load_state(self, state, timeout):
            self.wait_calls.append((state, timeout))

    anchor = FakeAnchor()
    page = FakePage()
    crawler = PlayerSearchCrawler()

    assert asyncio.run(crawler._trigger_postback(page, anchor)) is True
    assert anchor.click_timeout == 10000
    assert page.wait_calls == [("load", 10000)]
