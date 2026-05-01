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


def test_parse_naver_payload_splits_raw_pbp_from_result_events():
    crawler = RelayCrawler()

    payload = crawler._parse_naver_payload(
        [
            {
                "title": "9회 말",
                "inn": 9,
                "homeOrAway": "HOME",
                "textOptions": [
                    {
                        "currentGameState": {"homeScore": "2", "awayScore": "3", "out": "0"},
                        "batterRecord": {"name": "박성한"},
                        "text": "9회말 삼성 공격",
                    },
                    {
                        "currentGameState": {"homeScore": "2", "awayScore": "3", "out": "0"},
                        "batterRecord": {"name": "박성한"},
                        "text": "1번타자 박성한",
                    },
                    {
                        "currentGameState": {"homeScore": "2", "awayScore": "3", "out": "0"},
                        "text": "1구 볼",
                    },
                    {
                        "currentGameState": {"homeScore": "2", "awayScore": "3", "out": "1"},
                        "batterRecord": {"name": "박성한"},
                        "text": "박성한 : 중견수 플라이 아웃",
                    },
                    {
                        "currentGameState": {"homeScore": "2", "awayScore": "3", "out": "1"},
                        "text": "=====================================",
                    },
                    {
                        "currentGameState": {"homeScore": "2", "awayScore": "3", "out": "1"},
                        "text": "피치클락 위반 타자 경고 : 삼성 류지혁",
                    },
                    {
                        "currentGameState": {"homeScore": "2", "awayScore": "3", "out": "1"},
                        "text": "승리투수: 이로운",
                    },
                ],
            }
        ]
    )

    assert [event["description"] for event in payload["events"]] == ["박성한 : 중견수 플라이 아웃"]
    assert payload["events"][0]["event_type"] == "batting"
    assert len(payload["raw_pbp_rows"]) == 7
    assert "=====================================" in [row["play_description"] for row in payload["raw_pbp_rows"]]


def test_parse_naver_payload_promotes_scoring_runner_homein_rows():
    crawler = RelayCrawler()

    payload = crawler._parse_naver_payload(
        [
            {
                "title": "9회 말",
                "inn": 9,
                "homeOrAway": "HOME",
                "textOptions": [
                    {
                        "currentGameState": {"homeScore": "3", "awayScore": "3", "out": "2"},
                        "batterRecord": {"name": "데이비슨"},
                        "text": "데이비슨 : 좌익수 앞 1루타",
                    },
                    {
                        "currentGameState": {
                            "homeScore": "4",
                            "awayScore": "3",
                            "out": "2",
                            "base1": "1",
                            "base2": "1",
                        },
                        "text": "3루주자 김주원 : 홈인",
                    },
                ],
            }
        ]
    )

    assert [event["description"] for event in payload["events"]] == [
        "데이비슨 : 좌익수 앞 1루타",
        "3루주자 김주원 : 홈인",
    ]
    assert payload["events"][-1]["event_type"] == "runner_advance"
    assert (payload["events"][-1]["away_score"], payload["events"][-1]["home_score"]) == (3, 4)


def test_parse_naver_payload_keeps_home_run_when_result_contains_distance_colon():
    crawler = RelayCrawler()

    payload = crawler._parse_naver_payload(
        [
            {
                "title": "9회 말",
                "inn": 9,
                "homeOrAway": "HOME",
                "textOptions": [
                    {
                        "currentGameState": {"homeScore": "7", "awayScore": "6", "out": "0"},
                        "batterRecord": {"name": "에레디아"},
                        "text": "에레디아 : 좌익수 뒤 홈런 (홈런거리:105M)",
                    },
                ],
            }
        ]
    )

    assert [event["description"] for event in payload["events"]] == [
        "에레디아 : 좌익수 뒤 홈런 (홈런거리:105M)",
    ]
    assert payload["events"][0]["event_type"] == "batting"
    assert payload["events"][0]["result"] == "좌익수 뒤 홈런 (홈런거리:105M)"
    assert (payload["events"][0]["away_score"], payload["events"][0]["home_score"]) == (6, 7)


def test_parse_naver_payload_keeps_all_batter_segments_in_chronological_order():
    crawler = RelayCrawler()

    payload = crawler._parse_naver_payload(
        [
            {
                "title": "2번타자 홈타자",
                "inn": 1,
                "homeOrAway": 1,
                "textOptions": [
                    {
                        "currentGameState": {"homeScore": "0", "awayScore": "1", "out": "2"},
                        "batterRecord": {"name": "홈타자"},
                        "text": "홈타자 : 유격수 땅볼 아웃",
                    },
                ],
            },
            {
                "title": "1번타자 홈선두",
                "inn": 1,
                "homeOrAway": 1,
                "textOptions": [
                    {
                        "currentGameState": {"homeScore": "0", "awayScore": "1", "out": "1"},
                        "batterRecord": {"name": "홈선두"},
                        "text": "홈선두 : 삼진 아웃",
                    },
                ],
            },
            {
                "title": "1회말 홈 공격",
                "inn": 1,
                "homeOrAway": 1,
                "textOptions": [
                    {
                        "currentGameState": {"homeScore": "0", "awayScore": "1", "out": "0"},
                        "text": "1회말 홈 공격",
                    },
                ],
            },
            {
                "title": "2번타자 원정타자",
                "inn": 1,
                "homeOrAway": 0,
                "textOptions": [
                    {
                        "currentGameState": {"homeScore": "0", "awayScore": "1", "out": "1"},
                        "batterRecord": {"name": "원정타자"},
                        "text": "원정타자 : 좌익수 뒤 2루타",
                    },
                ],
            },
            {
                "title": "1번타자 원정선두",
                "inn": 1,
                "homeOrAway": 0,
                "textOptions": [
                    {
                        "currentGameState": {"homeScore": "0", "awayScore": "0", "out": "1"},
                        "batterRecord": {"name": "원정선두"},
                        "text": "원정선두 : 삼진 아웃",
                    },
                ],
            },
            {
                "title": "1회초 원정 공격",
                "inn": 1,
                "homeOrAway": 0,
                "textOptions": [
                    {
                        "currentGameState": {"homeScore": "0", "awayScore": "0", "out": "0"},
                        "text": "1회초 원정 공격",
                    },
                ],
            },
        ]
    )

    assert [event["description"] for event in payload["events"]] == [
        "원정선두 : 삼진 아웃",
        "원정타자 : 좌익수 뒤 2루타",
        "홈선두 : 삼진 아웃",
        "홈타자 : 유격수 땅볼 아웃",
    ]
    assert [event["inning_half"] for event in payload["events"]] == ["top", "top", "bottom", "bottom"]
    assert len(payload["raw_pbp_rows"]) == 6


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
