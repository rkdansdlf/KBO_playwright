import asyncio
from types import SimpleNamespace

import scripts.maintenance.enrich_player_stubs as enrich_player_stubs


class _FakeResult:
    def __init__(self, rows):
        self.rows = rows

    def fetchall(self):
        return self.rows


class _FakeSession:
    def __init__(self, rows):
        self.rows = rows
        self.query = None
        self.params = None
        self.closed = False

    def execute(self, query, params):
        self.query = str(query)
        self.params = dict(params)
        return _FakeResult(self.rows)

    def close(self):
        self.closed = True


class _FakePool:
    created = []

    def __init__(self, max_pages):
        self.max_pages = max_pages
        self.started = False
        self.closed = False
        _FakePool.created.append(self)

    async def start(self):
        self.started = True

    async def close(self):
        self.closed = True


class _FakeCrawler:
    calls = []
    profiles = {}

    def __init__(self, pool):
        self.pool = pool

    async def crawl_player_profile(self, player_id, position=None):
        _FakeCrawler.calls.append((player_id, position))
        profile = {
            "photo_url": f"https://example.test/{player_id}.png",
            "bats": "R",
            "throws": "R",
            "debut_year": 2020,
            "salary_original": "10000만원",
            "signing_bonus_original": "0만원",
            "draft_info": "unit-test",
        }
        profile.update(_FakeCrawler.profiles.get(player_id, {}))
        return profile


class _FakeRepository:
    payloads = []

    def upsert_players(self, players):
        self.payloads.extend(players)
        return len(players)


def _patch_runtime(monkeypatch, session):
    _FakePool.created = []
    _FakeCrawler.calls = []
    _FakeCrawler.profiles = {}
    _FakeRepository.payloads = []
    monkeypatch.setattr(enrich_player_stubs, "load_dotenv", lambda: None)
    monkeypatch.setattr(enrich_player_stubs, "SessionLocal", lambda: session)
    monkeypatch.setattr(enrich_player_stubs, "AsyncPlaywrightPool", _FakePool)
    monkeypatch.setattr(enrich_player_stubs, "PlayerProfileCrawler", _FakeCrawler)
    monkeypatch.setattr(enrich_player_stubs, "PlayerBasicRepository", _FakeRepository)


def test_parse_player_ids_ignores_blank_tokens():
    assert enrich_player_stubs.parse_player_ids("42, 84,, 101 ") == [42, 84, 101]


def test_enrich_stubs_filters_query_by_explicit_player_ids(monkeypatch):
    session = _FakeSession(
        [
            SimpleNamespace(
                player_id=42,
                name="테스트투수",
                uniform_no="10",
                team="LG",
                position="투수",
                birth_date="2000.01.01",
                birth_date_date=None,
                height_cm=180,
                weight_kg=80,
                career="테스트고",
                status="active",
                staff_role=None,
                status_source="fixture",
                photo_url=None,
                bats=None,
                throws=None,
                debut_year=None,
                salary_original=None,
                signing_bonus_original=None,
                draft_info=None,
            ),
            SimpleNamespace(player_id=84, name="테스트타자", position="타자"),
        ]
    )
    _patch_runtime(monkeypatch, session)

    enriched = asyncio.run(enrich_player_stubs.enrich_stubs(limit=2, player_ids=[42, 84]))

    assert enriched == 2
    assert "player_id IN" in session.query
    assert "LIMIT" not in session.query
    assert session.params == {"player_ids": [42, 84]}
    assert _FakeCrawler.calls == [("42", "투수"), ("84", "타자")]
    assert [payload["player_id"] for payload in _FakeRepository.payloads] == [42, 84]
    assert _FakeRepository.payloads[0]["photo_url"] == "https://example.test/42.png"
    assert _FakeRepository.payloads[0]["team"] == "LG"
    assert _FakeRepository.payloads[0]["height_cm"] == 180
    assert _FakeRepository.payloads[0]["status_source"] == "fixture"
    assert _FakePool.created[0].started is True
    assert _FakePool.created[0].closed is True
    assert session.closed is True


def test_enrich_stubs_applies_profile_height_weight(monkeypatch):
    session = _FakeSession(
        [
            SimpleNamespace(
                player_id=99,
                name="신규선수",
                position="투수",
                birth_date_date="2001-02-03",
                height_cm=None,
                weight_kg=None,
            ),
        ]
    )
    _patch_runtime(monkeypatch, session)
    _FakeCrawler.profiles = {"99": {"height_cm": 191, "weight_kg": 96}}

    enriched = asyncio.run(enrich_player_stubs.enrich_stubs(limit=1, player_ids=[99]))

    assert enriched == 1
    payload = _FakeRepository.payloads[0]
    assert payload["height_cm"] == 191
    assert payload["weight_kg"] == 96
    assert payload["birth_date_date"].isoformat() == "2001-02-03"


def test_enrich_stubs_uses_limit_when_ids_are_not_provided(monkeypatch):
    session = _FakeSession([])
    _patch_runtime(monkeypatch, session)

    enriched = asyncio.run(enrich_player_stubs.enrich_stubs(limit=3))

    assert enriched == 0
    assert "player_id IN" not in session.query
    assert "LIMIT" in session.query
    assert session.params == {"limit": 3}
    assert _FakePool.created == []
    assert session.closed is True
