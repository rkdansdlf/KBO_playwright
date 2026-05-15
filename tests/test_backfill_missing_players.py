from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

import scripts.crawling.backfill_missing_players as module


class _FakeCrawler:
    profiles = {}
    calls = []

    async def crawl_player_profile(self, player_id, position=None):
        self.calls.append((player_id, position))
        return self.profiles.get(str(player_id))


class _FakeRepository:
    payloads = []

    def upsert_players(self, players):
        self.payloads.extend(players)
        return len(players)


def _build_db(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'players.db'}")
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE player_basic (
                    player_id INTEGER PRIMARY KEY,
                    name TEXT,
                    team TEXT,
                    position TEXT,
                    height_cm INTEGER,
                    weight_kg INTEGER,
                    bats TEXT,
                    throws TEXT,
                    debut_year INTEGER,
                    photo_url TEXT,
                    salary_original TEXT,
                    signing_bonus_original TEXT,
                    draft_info TEXT
                )
                """
            )
        )
        conn.execute(text("CREATE TABLE player_season_batting (player_id INTEGER, team_code TEXT)"))
        conn.execute(text("CREATE TABLE player_season_pitching (player_id INTEGER, team_code TEXT)"))
        conn.execute(
            text(
                """
                CREATE TABLE players (
                    id INTEGER PRIMARY KEY,
                    kbo_person_id TEXT,
                    height_cm INTEGER,
                    weight_kg INTEGER,
                    bats TEXT,
                    throws TEXT,
                    debut_year INTEGER,
                    photo_url TEXT,
                    salary_original TEXT,
                    signing_bonus_original TEXT,
                    draft_info TEXT,
                    status TEXT
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE player_identities (
                    id INTEGER PRIMARY KEY,
                    player_id INTEGER,
                    name_kor TEXT,
                    is_primary INTEGER
                )
                """
            )
        )
        conn.execute(text("INSERT INTO player_basic (player_id, name, team, position) VALUES (1002, 'Unknown 1002', 'LG', '타자')"))
        conn.execute(text("INSERT INTO player_basic (player_id, name, team, position) VALUES (2002, 'Unknown 2002', 'SS', '투수')"))
        conn.execute(text("INSERT INTO player_season_batting (player_id, team_code) VALUES (1001, 'LG'), (1002, 'LG')"))
        conn.execute(text("INSERT INTO player_season_pitching (player_id, team_code) VALUES (2001, 'SS'), (2002, 'SS')"))
    return sessionmaker(bind=engine)


def _patch_runtime(monkeypatch, tmp_path):
    session_factory = _build_db(tmp_path)
    _FakeCrawler.profiles = {}
    _FakeCrawler.calls = []
    _FakeRepository.payloads = []
    monkeypatch.setattr(module, "SessionLocal", session_factory)
    monkeypatch.setattr(module, "PlayerProfileCrawler", _FakeCrawler)
    monkeypatch.setattr(module, "PlayerBasicRepository", _FakeRepository)
    return session_factory


def test_find_candidates_can_include_pitching_and_unknown_stubs(monkeypatch, tmp_path):
    _patch_runtime(monkeypatch, tmp_path)

    batting_only = module.find_candidates()
    all_candidates = module.find_candidates(include_pitching=True, include_unknown_stubs=True)

    assert [(row.player_id, row.source) for row in batting_only] == [(1001, "batting_missing")]
    assert [(row.player_id, row.source) for row in all_candidates] == [
        (1001, "batting_missing"),
        (1002, "batting_unknown_stub"),
        (2001, "pitching_missing"),
        (2002, "pitching_unknown_stub"),
    ]


def test_backfill_players_saves_only_verified_non_unknown_profiles(monkeypatch, tmp_path):
    _patch_runtime(monkeypatch, tmp_path)
    _FakeCrawler.profiles = {
        "2001": {"name": "검증투수", "throws": "R"},
        "2002": {"name": "Unknown 2002", "throws": "R"},
    }

    result = module.asyncio.run(
        module.backfill_players(
            include_pitching=True,
            include_unknown_stubs=True,
            apply=True,
            player_ids=[2001, 2002],
            report_dir=tmp_path / "reports",
            delay=0,
        )
    )

    assert result["candidates"] == 2
    assert result["prepared"] == 1
    assert result["saved"] == 1
    assert result["skipped"] == 1
    assert [payload["player_id"] for payload in _FakeRepository.payloads] == [2001]
    assert _FakeRepository.payloads[0]["name"] == "검증투수"
    assert _FakeRepository.payloads[0]["team"] == "SS"


def test_backfill_uses_local_canonical_profiles_before_crawler(monkeypatch, tmp_path):
    session_factory = _patch_runtime(monkeypatch, tmp_path)
    with session_factory() as session:
        session.execute(
            text(
                """
                INSERT INTO players (
                    id, height_cm, weight_kg, bats, throws, debut_year, photo_url,
                    salary_original, signing_bonus_original, draft_info, status
                )
                VALUES (
                    2001, 188, 92, 'R', 'L', 2020, 'https://example.test/player.jpg',
                    '5000만원', '1000만원', '20 삼성 1차', 'ACTIVE'
                )
                """
            )
        )
        session.execute(
            text(
                "INSERT INTO player_identities (player_id, name_kor, is_primary) VALUES (2001, '캐논투수', 1)"
            )
        )
        session.commit()

    result = module.asyncio.run(
        module.backfill_players(
            include_pitching=True,
            apply=True,
            player_ids=[2001],
            report_dir=tmp_path / "reports",
            delay=0,
        )
    )

    assert result["prepared"] == 1
    assert result["saved"] == 1
    assert _FakeCrawler.calls == []
    assert _FakeRepository.payloads[0]["name"] == "캐논투수"
    assert _FakeRepository.payloads[0]["height_cm"] == 188
    assert _FakeRepository.payloads[0]["status"] == "active"
    assert _FakeRepository.payloads[0]["status_source"] == "canonical_fb"


def test_backfill_uses_kbo_person_profile_when_identity_is_missing(monkeypatch, tmp_path):
    session_factory = _patch_runtime(monkeypatch, tmp_path)
    with session_factory() as session:
        session.execute(
            text("INSERT INTO players (id, kbo_person_id, status) VALUES (2001, '92001', 'RETIRED')")
        )
        session.execute(
            text(
                """
                INSERT INTO player_basic (player_id, name, team, position, height_cm)
                VALUES (92001, '공식투수', 'LT', '투수', 181)
                """
            )
        )
        session.commit()

    result = module.asyncio.run(
        module.backfill_players(
            include_pitching=True,
            apply=True,
            player_ids=[2001],
            report_dir=tmp_path / "reports",
            delay=0,
        )
    )

    assert result["prepared"] == 1
    assert result["saved"] == 1
    assert _FakeCrawler.calls == []
    assert _FakeRepository.payloads[0]["name"] == "공식투수"
    assert _FakeRepository.payloads[0]["height_cm"] == 181
    assert _FakeRepository.payloads[0]["status_source"] == "kbo_person_fb"
