from __future__ import annotations

from scripts.maintenance import discover_wayback_relay_captures as wayback


def test_candidate_targets_include_gamecenter_and_livetext():
    targets = wayback._candidate_targets("20241002KTOB0")

    assert [target["url_kind"] for target in targets] == [
        "gamecenter_relay",
        "gamecenter_main_prefix",
        "livetext",
    ]
    assert "section=RELAY" in targets[0]["search_url"]
    assert "LiveText.aspx" in targets[2]["search_url"]


def test_pick_capture_prefers_relay_section_for_gamecenter_prefix():
    selected = wayback._pick_capture(
        "gamecenter_main_prefix",
        [
            {"timestamp": "1", "original": "https://www.koreabaseball.com/...&section=REVIEW", "mimetype": "text/html"},
            {"timestamp": "2", "original": "https://www.koreabaseball.com/...&section=RELAY", "mimetype": "text/html"},
        ],
    )

    assert selected is not None
    assert selected["timestamp"] == "2"
