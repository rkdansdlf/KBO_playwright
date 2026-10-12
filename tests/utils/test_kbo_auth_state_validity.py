"""An auth-state file is not proof that the session still works.

``is_authenticated`` used to answer "does the file exist". Playwright's pool then
treated a ``True`` as "skip the login", so a session the server had long since
ended was carried indefinitely: nothing expires on disk, nothing re-checks, and
the only symptom is a crawler that returns less than it used to.

The file in this repository is the evidence. It was last written on 11 May and
its only session cookie, ``ASP.NET_SessionId``, carries no expiry -- so an
existence check reads it as valid indefinitely while the cookies that do expire
around it lapsed months ago.

These fix the decision to the evidence that exists rather than to a plan to
collect it.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.utils.kbo_auth import KboAuthenticator

STATE = {
    "cookies": [
        {
            "name": "ASP.NET_SessionId",
            "domain": "www.koreabaseball.com",
            "expires": -1,
            "httpOnly": True,
        },
    ],
    "origins": [],
}


@pytest.fixture
def state_file(tmp_path, monkeypatch):
    """A state file the tests own, so nothing reads the real one."""
    path = tmp_path / "kbo_auth_state.json"
    monkeypatch.setattr(KboAuthenticator, "AUTH_STATE_PATH", str(path))
    return path


def _write(path: Path, body: dict) -> None:
    path.write_text(json.dumps(body), encoding="utf-8")


class TestTheFileIsNotTheEvidence:
    def test_a_file_full_of_analytics_cookies_is_not_a_session(self, state_file) -> None:
        """Only analytics cookies means the login produced nothing usable.

        Google Analytics cookies outlive a session by months, so a file holding
        nothing else is what a login that failed to persist a session leaves
        behind. Reading it as authenticated skips the login that would have fixed
        it -- and because the file is never rewritten, it is skipped forever.
        """
        _write(
            state_file,
            {
                "cookies": [
                    {"name": "_ga", "domain": ".koreabaseball.com", "expires": 1893456000},
                    {"name": "_gid", "domain": ".koreabaseball.com", "expires": 1778500000},
                ],
                "origins": [],
            },
        )

        assert KboAuthenticator.is_authenticated() is False

    def test_an_empty_file_is_not_a_session(self, state_file) -> None:
        """A zero-byte state is a crash mid-write, not a signed-out state."""
        _write(state_file, {"cookies": [], "origins": []})

        assert KboAuthenticator.is_authenticated() is False

    def test_an_unreadable_file_is_not_a_session(self, state_file) -> None:
        """Corrupt JSON has to read as absent rather than raise.

        The caller is a pool deciding whether to log in before a crawl; an
        exception there would take down the crawl instead of triggering the
        login that recovers it.
        """
        state_file.write_text("{not json", encoding="utf-8")

        assert KboAuthenticator.is_authenticated() is False

    def test_a_real_session_cookie_is_a_session(self, state_file) -> None:
        _write(state_file, STATE)

        assert KboAuthenticator.is_authenticated() is True


class TestExpiryIsHonoured:
    def test_a_session_cookie_past_its_expiry_is_not_a_session(self, state_file) -> None:
        """An expired cookie is the one unambiguous signal available offline.

        A file may carry no expiry at all, in which case only the server can say.
        Where the cookie does name an expiry, ignoring it means ignoring the only
        deadline the file records.
        """
        _write(
            state_file,
            {
                "cookies": [
                    {
                        "name": "ASP.NET_SessionId",
                        "domain": "www.koreabaseball.com",
                        "expires": 1000000000,  # 2001-09-09
                    },
                ],
                "origins": [],
            },
        )

        assert KboAuthenticator.is_authenticated() is False

    def test_a_cookie_with_no_expiry_is_still_accepted(self, state_file) -> None:
        """A session cookie's lifetime is the server's business, not the file's.

        ``expires: -1`` is how Playwright writes a session cookie. Rejecting it
        would log in on every crawl, which is the expensive failure this check
        exists to avoid.
        """
        _write(state_file, STATE)

        assert KboAuthenticator.is_authenticated() is True


class TestThePoolTrustsTheAnswer:
    def test_a_stale_file_sends_the_pool_back_through_login(self, state_file, monkeypatch) -> None:
        """The decision has to change the behaviour that was skipping the login.

        Without this, the predicate above could be made correct while the pool
        kept trusting the old meaning of it. Only the login is replaced -- the
        predicate is the thing under test, so patching it would test nothing.
        """
        import asyncio

        from src.utils.playwright_pool import AsyncPlaywrightPool

        _write(state_file, {"cookies": [{"name": "_ga", "domain": ".koreabaseball.com", "expires": 1893456000}]})

        pool = AsyncPlaywrightPool(max_pages=1, requires_auth=True)
        logged_in: list[bool] = []

        async def _fake_login(self=None, *, headless: bool = True) -> bool:
            logged_in.append(True)
            _write(state_file, STATE)
            return True

        monkeypatch.setattr(KboAuthenticator, "login", _fake_login)
        asyncio.run(pool._prepare_auth_state())

        assert logged_in == [True], "a stale state file skipped the login that would refresh it"
        assert pool.context_kwargs.get("storage_state"), "the refreshed session was not adopted"

    def test_a_live_file_still_skips_the_login(self, state_file, monkeypatch) -> None:
        """The opposite side of the same decision.

        Logging in on every crawl is the failure this check is meant to avoid,
        so it is worth pinning that a good file is taken at face value.
        """
        import asyncio

        from src.utils.playwright_pool import AsyncPlaywrightPool

        _write(state_file, STATE)
        pool = AsyncPlaywrightPool(max_pages=1, requires_auth=True)
        logged_in: list[bool] = []

        async def _fake_login(self=None, *, headless: bool = True) -> bool:
            logged_in.append(True)
            return True

        monkeypatch.setattr(KboAuthenticator, "login", _fake_login)
        asyncio.run(pool._prepare_auth_state())

        assert logged_in == [], "a usable session was thrown away and re-fetched"
        assert pool.context_kwargs.get("storage_state"), "the saved session was not adopted"
