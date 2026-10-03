from __future__ import annotations

import asyncio

import src.crawlers.relay_crawler as relay_module
from src.crawlers.relay_crawler import RelayCrawler
from src.crawlers.relay_outcome import InningStop
from src.crawlers.result import CrawlOutcome
from src.sources.relay.base import default_source_order_for_bucket


class _FakePolicy:
    def __init__(self):
        self.delay_hosts = []
        self.retry_calls = 0

    async def delay_async(self, *, host="koreabaseball.com"):
        self.delay_hosts.append(host)

    async def run_with_retry_async(self, func, *args, **kwargs):
        self.retry_calls += 1
        return await func(*args, **kwargs)


class _FakeCompliance:
    def __init__(self, allowed=True):
        self.allowed = allowed
        self.urls = []

    async def is_allowed(self, url: str):
        self.urls.append(url)
        return self.allowed


def test_match_schedule_game_for_postseason_prefix_id():
    crawler = RelayCrawler()
    games = [
        {
            "gameId": "44441002KTOB02024",
            "awayTeamCode": "KT",
            "homeTeamCode": "OB",
        },
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
        },
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


def test_resolve_naver_game_id_scans_nearby_dates_for_rescheduled_postseason_game(monkeypatch):
    crawler = RelayCrawler(policy=_FakePolicy())

    # The seam is now `_request_json`, which delegates to the shared transport.
    # Patching it keeps this test about the date scan, which is what it is for.
    requested_dates: list[str] = []

    def _games(game_ids: list[str]) -> dict:
        return {
            "result": {
                "games": [{"gameId": game_id, "awayTeamCode": "SS", "homeTeamCode": "HT"} for game_id in game_ids],
            },
        }

    async def _request(url, *, params=None, headers=None):
        date = params["date"]
        requested_dates.append(date)
        if date == "2024-10-22":
            return _games(["77771022SSHT02024"]), None
        if date == "2024-10-23":
            return _games(["77771023SSHT02024", "77771021SSHT02024"]), None
        return _games([]), None

    monkeypatch.setattr(crawler, "_request_json", _request)

    resolved = asyncio.run(crawler._resolve_naver_game_id("20241021SSHT0"))

    assert resolved == "77771021SSHT02024"
    assert requested_dates[:4] == ["2024-10-21", "2024-10-22", "2024-10-20", "2024-10-23"]


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
                    },
                ],
            },
        ],
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
            },
        ],
    )

    assert [event["description"] for event in payload["events"]] == ["박성한 : 중견수 플라이 아웃"]
    assert payload["events"][0]["event_type"] == "batting"
    assert len(payload["raw_pbp_rows"]) == 8
    assert "=====================================" in [row["play_description"] for row in payload["raw_pbp_rows"]]
    assert "9회 말" in [
        row["play_description"] for row in payload["raw_pbp_rows"] if row["event_type"] == "inning_header"
    ]


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
            },
        ],
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
            },
        ],
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
        ],
    )

    assert [event["description"] for event in payload["events"]] == [
        "원정선두 : 삼진 아웃",
        "원정타자 : 좌익수 뒤 2루타",
        "홈선두 : 삼진 아웃",
        "홈타자 : 유격수 땅볼 아웃",
    ]
    assert [event["inning_half"] for event in payload["events"]] == ["top", "top", "bottom", "bottom"]
    assert len(payload["raw_pbp_rows"]) == 12
    header_titles = [row["play_description"] for row in payload["raw_pbp_rows"] if row["event_type"] == "inning_header"]
    assert len(header_titles) == 6
    assert all(t for t in header_titles)


def test_fetch_text_relays_handles_null_result_payload():
    crawler = RelayCrawler(policy=_FakePolicy())

    async def _request(url, *, params=None, headers=None):
        return {"result": None}, None

    crawler._request_json = _request

    fetched = asyncio.run(crawler._fetch_text_relays("dummy"))

    # The fetch also says why it stopped. A `result: None` envelope is the source
    # answering with nothing, which is an empty first inning rather than a failed
    # request -- the two used to arrive as the same empty list.
    assert fetched.relays == []
    assert fetched.stop == InningStop.EMPTY_INNING
    assert fetched.innings_fetched == 0
    assert fetched.failure_reason is None


class _FakeHttp:
    """Stands in for the shared transport, recording how it was called."""

    def __init__(self, result):
        self.result = result
        self.calls = []

    async def fetch_json(self, url, *, params=None, headers=None):
        self.calls.append((url, params, headers))
        return self.result


def _crawl_result(outcome, *, status=None, code=None, data=None):
    """Build the transport result the shared client would produce."""
    from src.crawlers.result import CrawlResult

    if outcome is CrawlOutcome.SUCCESS:
        return CrawlResult.success(data, http_status=status or 200)
    if outcome is CrawlOutcome.EMPTY:
        return CrawlResult.empty(http_status=status or 200)
    return CrawlResult.failure(outcome, error="x", http_status=status, error_code=code)


def test_request_json_delegates_to_the_shared_transport():
    """Compliance, throttling and retries are the shared client's job now.

    Kept here as a delegation contract: the crawler must hand the shared client
    the URL, the query and the per-request headers, because the shared client
    merges its own defaults with what it is given.
    """
    fake = _FakeHttp(_crawl_result(CrawlOutcome.PERMANENT_ERROR, status=500, code="FETCH_HTTP_ERROR"))
    crawler = RelayCrawler(http=fake)

    payload, reason = asyncio.run(
        crawler._request_json(
            "https://api-gw.sports.naver.com/schedule/today-games",
            params={"date": "2025-04-01"},
            headers={"Referer": "https://m.sports.naver.com/"},
        )
    )

    # A permanent status keeps its number, so the ledger can tell a 500 from
    # a 404 without the reason string having to encode every case.
    assert (payload, reason) == (None, "http_500")
    url, params, headers = fake.calls[0]
    assert url.endswith("/schedule/today-games")
    assert params == {"date": "2025-04-01"}
    assert headers == {"Referer": "https://m.sports.naver.com/"}


def test_request_json_does_not_throttle_itself():
    """Two throttles on one request means every wait is paid twice.

    The shared client already waits on the same per-host limiter, so a second
    delay here would double every request's pause and quietly slow the crawl.
    """
    policy = _FakePolicy()
    crawler = RelayCrawler(policy=policy, http=_FakeHttp(_crawl_result(CrawlOutcome.PERMANENT_ERROR, status=500)))

    asyncio.run(crawler._request_json("https://api-gw.sports.naver.com/schedule/today-games"))

    assert policy.delay_hosts == []


def test_a_relay_404_stays_an_absence_and_not_a_transport_fault():
    """The translation the whole vocabulary rests on.

    The shared client reports a 404 as a permanent HTTP error, which is right in
    general and wrong here: for a relay endpoint it means the source does not
    have the game. Collapsing the two would queue every absent game for retries
    that cannot succeed.
    """
    crawler = RelayCrawler(
        http=_FakeHttp(_crawl_result(CrawlOutcome.PERMANENT_ERROR, status=404, code="FETCH_HTTP_ERROR"))
    )

    payload, reason = asyncio.run(crawler._request_json("https://api-gw.sports.naver.com/schedule/games/x/relay"))

    assert payload is None
    assert reason == "http_404"


def test_an_undecodable_body_is_named_as_drift_not_as_an_api_error():
    """A shape we no longer read is our problem, not the site's."""
    crawler = RelayCrawler(
        http=_FakeHttp(_crawl_result(CrawlOutcome.SCHEMA_CHANGED, status=200, code="PARSE_INVALID_FORMAT"))
    )

    payload, reason = asyncio.run(crawler._request_json("https://api-gw.sports.naver.com/schedule/games/x/relay"))

    assert payload is None
    assert reason == "relay_schema_drift"


def test_an_empty_body_is_a_successful_request_with_nothing_in_it():
    """It stays a payload, because the inning loop reads an envelope.

    Turning it into a failure would report every finished game as a broken one.
    """
    crawler = RelayCrawler(http=_FakeHttp(_crawl_result(CrawlOutcome.EMPTY, status=200)))

    payload, reason = asyncio.run(crawler._request_json("https://api-gw.sports.naver.com/schedule/games/x/relay"))

    assert payload == {}
    assert reason is None


def test_match_schedule_game_rejects_team_mismatch_even_when_id_suffix_matches():
    crawler = RelayCrawler(policy=_FakePolicy())

    matched = crawler._match_schedule_game(
        "20250401LGSS0",
        [
            {
                "gameId": "77770401LGSS02025",
                "awayTeamCode": "KT",
                "homeTeamCode": "SS",
            },
        ],
    )

    assert matched is None


def test_match_schedule_game_uses_doubleheader_number():
    crawler = RelayCrawler(policy=_FakePolicy())
    games = [
        {"gameId": "77770401LGSS02025", "awayTeamCode": "LG", "homeTeamCode": "SS"},
        {"gameId": "77770401LGSS12025", "awayTeamCode": "LG", "homeTeamCode": "SS"},
    ]

    matched = crawler._match_schedule_game("20250401LGSS1", games)

    assert matched is not None
    assert matched["gameId"] == "77770401LGSS12025"


def test_match_schedule_game_doubleheader_mismatch_penalty():
    crawler = RelayCrawler(policy=_FakePolicy())
    games = [
        {"gameId": "77770401LGSS12025", "awayTeamCode": "LG", "homeTeamCode": "SS", "doubleHeader": "1"},
    ]
    # We want game with DH number 0, but games only has doubleheader 1
    matched = crawler._match_schedule_game("20250401LGSS0", games)
    assert matched is None  # Should be rejected due to heavy doubleheader penalty


def test_match_schedule_game_stadium_and_start_time_matching():
    crawler = RelayCrawler(policy=_FakePolicy())
    games = [
        {
            "gameId": "77770401LGSS02025",
            "awayTeamCode": "LG",
            "homeTeamCode": "SS",
            "stadiumName": "인천 SSG 랜더스필드",
            "gameStartTime": "18:30",
        },
        {
            "gameId": "99990401LGSS02025",
            "awayTeamCode": "LG",
            "homeTeamCode": "SS",
            "stadiumName": "잠실",
            "gameStartTime": "14:00",
        },
    ]

    # Test stadium synonym match and exact time match
    matched_landers = crawler._match_schedule_game("20250401LGSS0", games, stadium="문학", game_time="18:30")
    assert matched_landers is not None
    assert matched_landers["gameId"] == "77770401LGSS02025"

    # Test other stadium match
    matched_jamsil = crawler._match_schedule_game("20250401LGSS0", games, stadium="잠실", game_time="14:00")
    assert matched_jamsil is not None
    assert matched_jamsil["gameId"] == "99990401LGSS02025"


def test_match_schedule_game_doubleheader_boolean_normalization():
    crawler = RelayCrawler(policy=_FakePolicy())
    games = [
        {
            "gameId": "77770401LGSS12025",
            "awayTeamCode": "LG",
            "homeTeamCode": "SS",
            "doubleHeader": True,
            "gameStartTime": "14:00",
        },
        {
            "gameId": "77770401LGSS22025",
            "awayTeamCode": "LG",
            "homeTeamCode": "SS",
            "doubleHeader": True,
            "gameStartTime": "18:30",
        },
    ]

    matched_1 = crawler._match_schedule_game("20250401LGSS1", games)
    assert matched_1 is not None
    assert matched_1["gameId"] == "77770401LGSS12025"

    matched_2 = crawler._match_schedule_game("20250401LGSS2", games)
    assert matched_2 is not None
    assert matched_2["gameId"] == "77770401LGSS22025"
