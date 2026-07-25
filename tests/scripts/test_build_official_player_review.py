from __future__ import annotations

import scripts.maintenance.build_official_player_review as review


def test_render_review_includes_official_and_db_evidence(monkeypatch) -> None:
    report = {
        "generated_at": "2026-07-23T00:00:00+09:00",
        "source": "test",
        "results": [
            {
                "target": {"name": "김민", "team_code": "KT", "season": 2020},
                "search_url": "https://example.test/player",
                "candidate_count": 1,
                "candidates": [
                    {
                        "player_id": 68043,
                        "name": "김민",
                        "uniform_no": "1",
                        "team": "SSG",
                        "position": "투수",
                        "career": "KT-상무-KT",
                        "current_team_match": False,
                        "career_team_match": True,
                    },
                ],
            },
        ],
    }
    evidence = review.DbEvidence(14, 0, (68043,), ("1",), ("투수",), (("game_pitching_stats", 14),))
    monkeypatch.setattr(review, "load_db_evidence", lambda _target: evidence)

    output = review.render_review_markdown(
        report,
        resolver_rows={("김민", "KT", 2020): {"resolved_player_id": "68043", "resolution_reason": "group"}},
    )

    assert "Official Player ID Review" in output
    assert "DB_ID_MATCH" in output
    assert "career=true" not in output
    assert "KT-상무-KT" in output
