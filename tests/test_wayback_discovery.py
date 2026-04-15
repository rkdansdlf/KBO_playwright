from __future__ import annotations

import importlib.util
from pathlib import Path


def _load_module():
    module_path = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "maintenance"
        / "discover_wayback_relay_captures.py"
    )
    spec = importlib.util.spec_from_file_location("discover_wayback_relay_captures", module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_candidate_targets_include_gamecenter_and_livetext():
    module = _load_module()

    targets = module._candidate_targets("20241002KTOB0")

    assert [target["url_kind"] for target in targets] == [
        "gamecenter_relay",
        "gamecenter_main_prefix",
        "livetext",
    ]
    assert "section=RELAY" in targets[0]["search_url"]
    assert "LiveText.aspx" in targets[2]["search_url"]


def test_pick_capture_prefers_relay_section_for_gamecenter_prefix():
    module = _load_module()

    selected = module._pick_capture(
        "gamecenter_main_prefix",
        [
            {"timestamp": "1", "original": "https://www.koreabaseball.com/...&section=REVIEW", "mimetype": "text/html"},
            {"timestamp": "2", "original": "https://www.koreabaseball.com/...&section=RELAY", "mimetype": "text/html"},
        ],
    )

    assert selected is not None
    assert selected["timestamp"] == "2"
