from __future__ import annotations

from src.crawlers.relay_crawler import RelayCrawler
from src.sources.relay.base import default_source_order_for_bucket
import asyncio


def test_match_schedule_game_for_postseason_prefix_id():
    crawler = RelayCrawler()
    games = [
        {
            "gameId": "44441002KTOB02024",
            "awayTeamCode": "KT",
            "homeTeamCode": "OB",
        }
    ]

    matched = crawler._match_schedule_game("20241002KTOB0", games)

    assert matched is not None
    assert matched["gameId"] == "44441002KTOB02024"


def test_match_schedule_game_prefers_original_date_suffix_when_same_teams_repeat():
    crawler = RelayCrawler()
    games = [
        {
            "gameId": "77771023SSHT02024",
            "awayTeamCode": "SS",
            "homeTeamCode": "HT",
        },
        {
            "gameId": "77771021SSHT02024",
            "awayTeamCode": "SS",
            "homeTeamCode": "HT",
        },
    ]

    matched = crawler._match_schedule_game("20241021SSHT0", games)

    assert matched is not None
    assert matched["gameId"] == "77771021SSHT02024"


def test_match_schedule_game_for_all_star():
    crawler = RelayCrawler()
    games = [
        {
            "gameId": "99990712EAWE02025",
            "awayTeamCode": "EA",
            "homeTeamCode": "WE",
        }
    ]

    matched = crawler._match_schedule_game("20250712EAWE0", games)

    assert matched is not None
    assert matched["gameId"] == "99990712EAWE02025"


def test_match_schedule_game_maps_international_team_codes():
    crawler = RelayCrawler()
    games = [
        {
            "gameId": "88881113DOCU02024",
            "awayTeamCode": "DO",
            "homeTeamCode": "CU",
        },
        {
            "gameId": "88881113PNUS02024",
            "awayTeamCode": "PN",
            "homeTeamCode": "US",
        },
    ]

    matched_do = crawler._match_schedule_game("20241113DOCU0", games)
    matched_pa = crawler._match_schedule_game("20241113PAUS0", games)

    assert matched_do is not None
    assert matched_do["gameId"] == "88881113DOCU02024"
    assert matched_pa is not None
    assert matched_pa["gameId"] == "88881113PNUS02024"


def test_schedule_query_context_switches_to_premier12_bucket():
    crawler = RelayCrawler()

    context = crawler._schedule_query_context("20241113KRTW0")

    assert context == {
        "sectionId": "worldbaseball",
        "categoryId": "premier12",
        "seasonYear": "2024",
        "date": "2024-11-13",
    }


def test_resolve_naver_game_id_scans_nearby_dates_for_rescheduled_postseason_game():
    crawler = RelayCrawler()

    class _Response:
        def __init__(self, payload):
            self.status_code = 200
            self._payload = payload

        def json(self):
            return self._payload

    class _Client:
        def __init__(self):
            self.requested_dates = []

        async def get(self, url, params=None, headers=None, timeout=None):
            date = params["date"]
            self.requested_dates.append(date)
            if date == "2024-10-22":
                return _Response(
                    {
                        "result": {
                            "games": [
                                {"gameId": "77771022SSHT02024", "awayTeamCode": "SS", "homeTeamCode": "HT"},
                            ]
                        }
                    }
                )
            if date == "2024-10-23":
                return _Response(
                    {
                        "result": {
                            "games": [
                                {"gameId": "77771023SSHT02024", "awayTeamCode": "SS", "homeTeamCode": "HT"},
                                {"gameId": "77771021SSHT02024", "awayTeamCode": "SS", "homeTeamCode": "HT"},
                            ]
                        }
                    }
                )
            return _Response({"result": {"games": []}})

    client = _Client()

    resolved = asyncio.run(crawler._resolve_naver_game_id(client, "20241021SSHT0"))

    assert resolved == "77771021SSHT02024"
    assert client.requested_dates[:4] == ["2024-10-21", "2024-10-22", "2024-10-20", "2024-10-23"]


def test_special_bucket_source_order_includes_naver_after_kbo():
    assert default_source_order_for_bucket("2024_postseason") == ["kbo", "naver", "import", "manual"]


def test_parse_naver_data_handles_null_nested_payloads():
    crawler = RelayCrawler()

    events = crawler._parse_naver_data(
        [
            {
                "title": "1회 초",
                "inn": 1,
                "homeOrAway": "AWAY",
                "textOptions": [
                    {
                        "currentGameState": None,
                        "batterRecord": None,
                        "text": "대한민국 : 좌익수 뜬공",
                        "pitcherName": None,
                    }
                ],
            }
        ]
    )

    assert len(events) == 1
    assert events[0]["description"] == "대한민국 : 좌익수 뜬공"


def test_fetch_text_relays_handles_null_result_payload():
    crawler = RelayCrawler()

    class _Response:
        def __init__(self, payload):
            self.status_code = 200
            self._payload = payload

        def json(self):
            return self._payload

    class _Client:
        def __init__(self):
            self.calls = 0

        async def get(self, *args, **kwargs):
            self.calls += 1
            if self.calls == 1:
                return _Response({"result": None})
            return _Response({"result": {"textRelayData": {"textRelays": []}}})

    relays = asyncio.run(crawler._fetch_text_relays(_Client(), "dummy"))

    assert relays == []
