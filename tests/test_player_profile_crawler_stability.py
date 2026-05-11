import asyncio

import src.crawlers.player_profile_crawler as module
from src.crawlers.player_profile_crawler import PlayerProfileCrawler


class _FakeCompliance:
    async def is_allowed(self, _url):
        return True


class _FakePolicy:
    def __init__(self):
        self.retry_calls = 0
        self.delay_hosts = []

    async def delay_async(self, host="koreabaseball.com"):
        self.delay_hosts.append(host)

    async def run_with_retry_async(self, func, *args, **kwargs):
        self.retry_calls += 1
        return await func(*args, **kwargs)


class _FakePage:
    def __init__(self, raws):
        self.raws = list(raws)
        self.goto_calls = []
        self.selector_calls = []

    async def goto(self, url, wait_until=None, timeout=None):
        self.goto_calls.append((url, wait_until, timeout))

    async def wait_for_timeout(self, _timeout):
        return None

    async def wait_for_selector(self, selector, state=None, timeout=None):
        self.selector_calls.append((selector, state, timeout))

    async def wait_for_function(self, *_args, **_kwargs):
        return None

    async def evaluate(self, *_args):
        if len(self.raws) == 1:
            return self.raws[0]
        return self.raws.pop(0)


def _valid_raw(name="홍길동"):
    return {
        "name": name,
        "photo_url": "https://example.test/player.jpg",
        "photo_attr": None,
        "salary": "10000만원",
        "signing": "0만원",
        "draft": "25 LG 1차",
        "debut": "2025 LG",
        "height_weight": "180cm/80kg",
        "raw_text": "포지션: 투수(우투좌타)",
    }


def test_profile_crawler_falls_back_to_next_url_candidate(monkeypatch):
    monkeypatch.setattr(module, "compliance", _FakeCompliance())
    crawler = PlayerProfileCrawler(request_delay=0)
    policy = _FakePolicy()
    crawler.policy = policy
    page = _FakePage([{"error": "NO_PROFILE_ELEMENT"}, _valid_raw()])

    payload = asyncio.run(crawler._fetch_profile(page, "1001", position=None))

    assert payload["player_id"] == "1001"
    assert payload["name"] == "홍길동"
    assert payload["throws"] == "R"
    assert payload["bats"] == "L"
    assert payload["height_cm"] == 180
    assert crawler.get_last_failure_reason("1001") is None
    assert policy.retry_calls == 2
    assert policy.delay_hosts == ["www.koreabaseball.com", "www.koreabaseball.com"]
    assert len(page.goto_calls) == 2


def test_profile_crawler_records_profile_stub_when_all_candidates_have_invalid_name(monkeypatch):
    monkeypatch.setattr(module, "compliance", _FakeCompliance())
    crawler = PlayerProfileCrawler(request_delay=0)
    crawler.policy = _FakePolicy()
    page = _FakePage([_valid_raw(name="Unknown Player")])

    payload = asyncio.run(crawler._fetch_profile(page, "1002", position="투수"))

    assert payload is None
    assert crawler.get_last_failure_reason("1002") == "profile_stub"
