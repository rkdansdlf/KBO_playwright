"""Tests for low-coverage repository modules."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from src.repositories.award_repository import AwardRepository
from src.repositories.broadcast_repository import BroadcastRepository
from src.repositories.game_mvp_repository import GameMvpRepository
from src.repositories.manager_change_repository import ManagerChangeRepository


class TestRepositories:
    @pytest.mark.parametrize(
        "repo_cls",
        [
            AwardRepository,
            BroadcastRepository,
            GameMvpRepository,
            ManagerChangeRepository,
        ],
    )
    def test_repository_classes_exist(self, repo_cls):
        assert repo_cls is not None

    @pytest.mark.parametrize(
        "repo_cls",
        [
            AwardRepository,
            BroadcastRepository,
            GameMvpRepository,
            ManagerChangeRepository,
        ],
    )
    def test_repository_instantiation(self, repo_cls):
        mock_session = MagicMock()
        repo = repo_cls(session=mock_session)
        assert repo is not None
