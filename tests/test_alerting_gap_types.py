"""Tests for gap-type-aware alerting (GAP_EMOJI_MAP, send_gap_alert)."""

from __future__ import annotations

from src.utils.alerting import (
    GAP_CATEGORY_ENV_MAP,
    GAP_EMOJI_MAP,
    SlackWebhookClient,
    TelegramBotClient,
)

EXPECTED_GAP_CATEGORIES = {"FRESHNESS", "P0", "STALENESS", "RELAY", "PROFILE", "ID_RESOLUTION", "PA_FORMULA"}


def test_gap_emoji_map_has_all_categories():
    assert set(GAP_EMOJI_MAP.keys()) == EXPECTED_GAP_CATEGORIES
    for cat in EXPECTED_GAP_CATEGORIES:
        assert GAP_EMOJI_MAP[cat], f"Empty emoji for {cat}"


def test_gap_category_env_map_has_all_categories():
    assert set(GAP_CATEGORY_ENV_MAP.keys()) == EXPECTED_GAP_CATEGORIES
    for cat in EXPECTED_GAP_CATEGORIES:
        env_var = GAP_CATEGORY_ENV_MAP[cat]
        assert env_var.startswith("TELEGRAM_CHAT_ID_"), f"Unexpected env var name: {env_var}"


def test_send_gap_alert_sends_telegram_with_correct_prefix(monkeypatch):
    sent: list[dict] = []

    def fake_send_message(message: str, chat_id: str | None = None) -> bool:
        sent.append({"message": message, "chat_id": chat_id})
        return True

    monkeypatch.setattr(TelegramBotClient, "send_message", fake_send_message)

    result = SlackWebhookClient.send_gap_alert("RELAY", "3 games missing PBP", ["game1", "game2"])
    assert result is True
    assert len(sent) == 1
    assert "💾" in sent[0]["message"]  # RELAY emoji
    assert "RELAY" in sent[0]["message"]
    assert "3 games missing PBP" in sent[0]["message"]
    assert sent[0]["chat_id"] is None  # default chat


def test_send_gap_alert_uses_per_category_chat(monkeypatch):
    monkeypatch.setenv("TELEGRAM_CHAT_ID_RELAY", "-1001234567890")
    sent: list[dict] = []

    def fake_send_message(message: str, chat_id: str | None = None) -> bool:
        sent.append({"message": message, "chat_id": chat_id})
        return True

    monkeypatch.setattr(TelegramBotClient, "send_message", fake_send_message)

    SlackWebhookClient.send_gap_alert("RELAY", "test")
    assert len(sent) == 1
    assert sent[0]["chat_id"] == "-1001234567890"


def test_send_gap_alert_truncates_long_details(monkeypatch):
    sent_messages: list[str] = []

    def fake_send_message(message: str, chat_id: str | None = None) -> bool:
        sent_messages.append(message)
        return True

    monkeypatch.setattr(TelegramBotClient, "send_message", fake_send_message)

    many_details = [f"item_{i}" for i in range(20)]
    SlackWebhookClient.send_gap_alert("PROFILE", "20 missing", many_details)
    assert len(sent_messages) == 1
    assert "item_0" in sent_messages[0]
    assert "... and 5 more" in sent_messages[0]


def test_send_gap_alert_no_details(monkeypatch):
    sent_messages: list[str] = []

    def fake_send_message(message: str, chat_id: str | None = None) -> bool:
        sent_messages.append(message)
        return True

    monkeypatch.setattr(TelegramBotClient, "send_message", fake_send_message)

    SlackWebhookClient.send_gap_alert("P0", "Pipeline ready")
    assert len(sent_messages) == 1
    assert "P0" in sent_messages[0]


def test_send_gap_alert_fallback_to_slack(monkeypatch):
    """When Telegram fails, should fall back to Slack."""
    monkeypatch.setattr(TelegramBotClient, "send_message", lambda message=None, chat_id=None: False)
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.com/fake")

    sent_slack: list[str] = []

    from contextlib import contextmanager

    @contextmanager
    def fake_urlopen(req, timeout=5):
        sent_slack.append(req.data.decode())

        class FakeResp:
            status = 200

        yield FakeResp()

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)

    result = SlackWebhookClient.send_gap_alert("FRESHNESS", "6 issues")
    assert result is True
    assert len(sent_slack) == 1
    assert "FRESHNESS" in sent_slack[0]


def test_send_gap_alert_unknown_category_defaults_to_warning(monkeypatch):
    sent_messages: list[str] = []

    def fake_send_message(message: str, chat_id: str | None = None) -> bool:
        sent_messages.append(message)
        return True

    monkeypatch.setattr(TelegramBotClient, "send_message", fake_send_message)

    SlackWebhookClient.send_gap_alert("UNKNOWN_CAT", "test")
    assert len(sent_messages) == 1
    assert "⚠️" in sent_messages[0]  # fallback emoji
