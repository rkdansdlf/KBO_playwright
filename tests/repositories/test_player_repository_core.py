from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from src.models.player import Player, PlayerBasic, PlayerIdentity, PlayerSeasonBatting
from src.parsers.player_profile_parser import PlayerProfileParsed
from src.repositories.player_repository import PlayerRepository


@pytest.fixture
def repo():
    return PlayerRepository()


@pytest.fixture
def inmemory_session():
    engine = create_engine("sqlite:///:memory:")
    Player.__table__.create(engine)
    PlayerBasic.__table__.create(engine)
    PlayerIdentity.__table__.create(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        yield session


class TestCanonicalPlayerBasicId:
    def test_valid_id_found(self, inmemory_session, repo):
        inmemory_session.add(PlayerBasic(player_id=12345, name="Test"))
        inmemory_session.commit()
        result = repo._canonical_player_basic_id(inmemory_session, "12345")
        assert result == 12345

    def test_valid_id_not_found(self, inmemory_session, repo):
        result = repo._canonical_player_basic_id(inmemory_session, "99999")
        assert result is None

    def test_invalid_id_returns_none(self, inmemory_session, repo):
        result = repo._canonical_player_basic_id(inmemory_session, "not-a-number")
        assert result is None


class TestGetOrCreatePlayer:
    def test_creates_new_player(self, inmemory_session, repo):
        inmemory_session.add(PlayerBasic(player_id=100, name="Test"))
        inmemory_session.commit()
        player = repo._get_or_create_player(inmemory_session, "100")
        assert player is not None
        assert player.kbo_person_id == "100"
        assert player.player_basic_id == 100

    def test_returns_existing_player(self, inmemory_session, repo):
        inmemory_session.add_all(
            [
                PlayerBasic(player_id=200, name="Existing"),
                Player(kbo_person_id="200", player_basic_id=200),
            ]
        )
        inmemory_session.commit()
        player = repo._get_or_create_player(inmemory_session, "200")
        assert player.player_basic_id == 200

    def test_updates_basic_id_when_mismatched(self, inmemory_session, repo):
        inmemory_session.add_all(
            [
                PlayerBasic(player_id=300, name="Updated"),
                Player(kbo_person_id="300", player_basic_id=999),
            ]
        )
        inmemory_session.commit()
        player = repo._get_or_create_player(inmemory_session, "300")
        assert player.player_basic_id == 300


class TestApplyProfileFields:
    def test_sets_birth_date(self, repo):
        player = Player()
        profile = PlayerProfileParsed(birth_date="1990-05-15")
        repo._apply_profile_fields(player, profile)
        assert player.birth_date is not None
        assert player.birth_date.year == 1990

    def test_sets_foreign_flag(self, repo):
        player = Player()
        profile = PlayerProfileParsed(is_foreign=True)
        repo._apply_profile_fields(player, profile)
        assert player.is_foreign_player is True

    def test_sets_active_status(self, repo):
        player = Player()
        profile = PlayerProfileParsed(is_active=True)
        repo._apply_profile_fields(player, profile)
        assert player.status == "ACTIVE"

    def test_sets_retired_status(self, repo):
        player = Player()
        profile = PlayerProfileParsed(is_active=False)
        repo._apply_profile_fields(player, profile)
        assert player.status == "RETIRED"

    def test_sets_draft_info(self, repo):
        player = Player()
        profile = PlayerProfileParsed(draft_year=2010, draft_team_code="LG", draft_type="2차")
        repo._apply_profile_fields(player, profile)
        assert "10" in player.draft_info
        assert "LG" in player.draft_info
        assert "2차" in player.draft_info

    def test_sets_career_path(self, repo):
        player = Player()
        profile = PlayerProfileParsed(education_or_career_path=["A고", "B대", "C구단"])
        repo._apply_profile_fields(player, profile)
        assert "A고" in f"{player.notes}"
        assert "->" in player.notes

    def test_does_not_override_with_none(self, repo):
        player = Player(height_cm=180, bats="R")
        profile = PlayerProfileParsed(height_cm=None, batting_hand=None)
        repo._apply_profile_fields(player, profile)
        assert player.height_cm == 180
        assert player.bats == "R"

    def test_structured_fields_set(self, repo):
        player = Player()
        profile = PlayerProfileParsed(draft_year=2015, draft_round=3, draft_pick_overall=10, draft_type="1차")
        repo._apply_profile_fields(player, profile)
        assert player.draft_year == 2015
        assert player.draft_round == 3
        assert player.draft_pick_overall == 10
        assert player.draft_type == "1차"

    def test_draft_info_with_pick_only(self, repo):
        player = Player()
        profile = PlayerProfileParsed(draft_year=2018, draft_team_code="SS", draft_pick_overall=7)
        repo._apply_profile_fields(player, profile)
        assert "18" in player.draft_info
        assert "7순위" in player.draft_info


class TestSyncToPlayerBasic:
    def test_invalid_kbo_id_returns_early(self, repo):
        basic = PlayerBasic()
        profile = PlayerProfileParsed()
        repo._sync_to_player_basic(None, "not-a-number", profile)
        assert basic.status is None

    def test_skip_when_no_basic_found(self, inmemory_session, repo):
        profile = PlayerProfileParsed(is_active=True)
        repo._sync_to_player_basic(inmemory_session, "99999", profile)
        result = inmemory_session.query(PlayerBasic).count()
        assert result == 0

    def test_updates_basic_fields(self, inmemory_session, repo):
        inmemory_session.add(PlayerBasic(player_id=500, name="Test"))
        inmemory_session.commit()
        profile = PlayerProfileParsed(
            is_active=True,
            photo_url="https://example.com/pic.jpg",
            height_cm=180,
            weight_kg=80,
            batting_hand="R",
            throwing_hand="R",
            entry_year=2018,
            salary_original="1억",
            signing_bonus_original="5천",
        )
        repo._sync_to_player_basic(inmemory_session, "500", profile)
        basic = inmemory_session.query(PlayerBasic).first()
        assert basic.status == "active"
        assert basic.photo_url == "https://example.com/pic.jpg"


class TestSyncBasicDraftAndCareer:
    def test_sets_draft_info(self, repo):
        basic = PlayerBasic()
        profile = PlayerProfileParsed(
            draft_year=2012, draft_team_code="LG", draft_type="2차", draft_round=3, draft_pick_overall=50
        )
        repo._sync_basic_draft_and_career(basic, profile)
        assert "12" in basic.draft_info
        assert "LG" in basic.draft_info
        assert "2차" in basic.draft_info
        assert "3라운드" in basic.draft_info
        assert "50순위" in basic.draft_info

    def test_draft_info_minimal(self, repo):
        basic = PlayerBasic()
        profile = PlayerProfileParsed(draft_year=2012)
        repo._sync_basic_draft_and_career(basic, profile)
        assert basic.draft_info == "12"

    def test_sets_career_path(self, repo):
        basic = PlayerBasic()
        profile = PlayerProfileParsed(education_or_career_path=["A고", "B대", "C구단"])
        repo._sync_basic_draft_and_career(basic, profile)
        assert basic.career == "A고-B대-C구단"

    def test_skip_when_no_draft_year(self, repo):
        basic = PlayerBasic()
        profile = PlayerProfileParsed()
        repo._sync_basic_draft_and_career(basic, profile)
        assert basic.draft_info is None


class TestSyncBasicStructuredFields:
    def test_sets_all_fields(self, repo):
        basic = PlayerBasic()
        profile = PlayerProfileParsed(
            salary_amount=50000,
            salary_currency="KRW",
            signing_bonus_amount=10000,
            signing_bonus_currency="KRW",
            draft_year=2015,
            draft_round=2,
            draft_pick_overall=30,
        )
        repo._sync_basic_structured_fields(basic, profile)
        assert basic.salary_amount == 50000
        assert basic.draft_year == 2015
        assert basic.draft_pick_overall == 30


class TestApplyBasicProfileFields:
    def test_sets_active_status(self, repo):
        basic = PlayerBasic()
        profile = PlayerProfileParsed(is_active=True)
        repo._apply_basic_profile_fields(basic, profile)
        assert basic.status == "active"
        assert basic.status_source == "profile"

    def test_sets_retired_status(self, repo):
        basic = PlayerBasic()
        profile = PlayerProfileParsed(is_active=False)
        repo._apply_basic_profile_fields(basic, profile)
        assert basic.status == "retired"

    def test_sets_photo_when_provided(self, repo):
        basic = PlayerBasic()
        profile = PlayerProfileParsed(photo_url="https://example.com/photo.jpg")
        repo._apply_basic_profile_fields(basic, profile)
        assert basic.photo_url == "https://example.com/photo.jpg"

    def test_sets_height_weight(self, repo):
        basic = PlayerBasic()
        profile = PlayerProfileParsed(height_cm=185, weight_kg=90)
        repo._apply_basic_profile_fields(basic, profile)
        assert basic.height_cm == 185
        assert basic.weight_kg == 90

    def test_skips_empty_fields(self, repo):
        basic = PlayerBasic(name="test")
        profile = PlayerProfileParsed(photo_url=None, height_cm=None)
        repo._apply_basic_profile_fields(basic, profile)
        assert basic.name == "test"


class TestUpsertIdentity:
    def test_creates_new_identity(self, inmemory_session, repo):
        inmemory_session.add(Player(kbo_person_id="1"))
        inmemory_session.flush()
        player = inmemory_session.query(Player).first()
        profile = PlayerProfileParsed(player_name="홍길동")
        repo._upsert_identity(inmemory_session, player, profile)
        identity = inmemory_session.query(PlayerIdentity).first()
        assert identity is not None
        assert identity.name_kor == "홍길동"
        assert identity.is_primary is True

    def test_skips_when_no_name(self, inmemory_session, repo):
        inmemory_session.add(Player(kbo_person_id="1"))
        inmemory_session.flush()
        player = inmemory_session.query(Player).first()
        profile = PlayerProfileParsed(player_name=None)
        repo._upsert_identity(inmemory_session, player, profile)
        count = inmemory_session.query(PlayerIdentity).count()
        assert count == 0

    def test_ensures_existing_is_primary(self, inmemory_session, repo):
        inmemory_session.add(Player(kbo_person_id="1"))
        inmemory_session.flush()
        player = inmemory_session.query(Player).first()
        inmemory_session.add(PlayerIdentity(player_id=player.id, name_kor="홍길동", is_primary=False))
        inmemory_session.commit()
        profile = PlayerProfileParsed(player_name="홍길동")
        repo._upsert_identity(inmemory_session, player, profile)
        identity = inmemory_session.query(PlayerIdentity).first()
        assert identity.is_primary is True

    def test_skips_when_already_primary(self, inmemory_session, repo):
        inmemory_session.add(Player(kbo_person_id="1"))
        inmemory_session.flush()
        player = inmemory_session.query(Player).first()
        inmemory_session.add(PlayerIdentity(player_id=player.id, name_kor="홍길동", is_primary=True))
        inmemory_session.commit()
        profile = PlayerProfileParsed(player_name="홍길동")
        repo._upsert_identity(inmemory_session, player, profile)
        identity = inmemory_session.query(PlayerIdentity).first()
        assert identity.is_primary is True

    def test_demotes_old_primaries_on_new_name(self, inmemory_session, repo):
        inmemory_session.add(Player(kbo_person_id="1"))
        inmemory_session.flush()
        player = inmemory_session.query(Player).first()
        inmemory_session.add(PlayerIdentity(player_id=player.id, name_kor="이전이름", is_primary=True))
        inmemory_session.commit()
        profile = PlayerProfileParsed(player_name="새이름")
        repo._upsert_identity(inmemory_session, player, profile)
        identities = inmemory_session.query(PlayerIdentity).all()
        assert len(identities) == 2
        old_identity = [i for i in identities if i.name_kor == "이전이름"][0]
        assert old_identity.is_primary is False


class TestUpsertSeasonPitching:
    def test_insert_new_pitching_record(self, repo):
        with patch("src.repositories.player_repository.SessionLocal") as mock_session_local:
            mock_session = MagicMock(spec=Session)
            mock_session_local.return_value.__enter__.return_value = mock_session
            mock_session.execute.return_value.scalar_one_or_none.return_value = None
            repo.upsert_season_pitching(1, {"season": 2025, "league": "REGULAR", "level": "KBO1", "era": 3.50})
            mock_session.add.assert_called_once()
            mock_session.commit.assert_called_once()


class TestUpsertSeasonStats:
    def test_insert_new_batting_record(self, repo):
        with patch("src.repositories.player_repository.SessionLocal") as mock_session_local:
            mock_session = MagicMock(spec=Session)
            mock_session_local.return_value.__enter__.return_value = mock_session
            mock_session.execute.return_value.scalar_one_or_none.return_value = None
            repo.upsert_season_batting(1, {"season": 2025, "league": "REGULAR", "level": "KBO1", "at_bats": 100})
            mock_session.add.assert_called_once()
            mock_session.commit.assert_called_once()

    def test_update_existing_batting_record(self, repo):
        existing = MagicMock(spec=PlayerSeasonBatting)
        with patch("src.repositories.player_repository.SessionLocal") as mock_session_local:
            mock_session = MagicMock(spec=Session)
            mock_session_local.return_value.__enter__.return_value = mock_session
            mock_session.execute.return_value.scalar_one_or_none.return_value = existing
            repo.upsert_season_batting(1, {"season": 2025, "league": "REGULAR", "level": "KBO1", "at_bats": 200})
            assert existing.at_bats == 200
            mock_session.commit.assert_called_once()

    def test_skips_empty_payload(self, repo):
        repo.upsert_season_batting(1, {})

    def test_raises_on_missing_season(self, repo):
        with pytest.raises(ValueError, match="season"):
            repo.upsert_season_batting(1, {"league": "REGULAR"})


class TestApplyProfileFieldsEntryYear:
    def test_sets_debut_year_when_entry_year_provided(self, repo):
        player = Player()
        profile = PlayerProfileParsed(entry_year=2020)
        repo._apply_profile_fields(player, profile)
        assert player.debut_year == 2020


class TestUpsertPlayerProfile:
    def test_requires_kbo_player_id(self, repo):
        with pytest.raises(ValueError, match="kbo_player_id"):
            repo.upsert_player_profile("", PlayerProfileParsed())

    def test_upsert_creates_player(self, repo):
        profile = PlayerProfileParsed(player_name="테스트", birth_date="1995-03-15", is_active=True)
        with patch("src.repositories.player_repository.SessionLocal") as mock_session_local:
            mock_session = MagicMock(spec=Session)
            mock_session_local.return_value.__enter__.return_value = mock_session
            mock_session.execute.return_value.scalar_one_or_none.return_value = None
            player = repo.upsert_player_profile("12345", profile)
            assert player is not None
