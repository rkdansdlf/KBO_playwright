r"""A snapshot artifact's path has to mean the same thing to every process.

The evidence directory is content-addressed, so the part of an artifact path that
matters -- ``raw/<hash>.bin`` -- is identical on every host. The stored path used
to be absolute, which made each row readable by exactly one process: rows captured
on the Windows host (``D:\\server-data\\...\\crawl_evidence\\raw\\<hash>``) and on
the Mac (``/Users/.../crawl_evidence/raw/<hash>``) are both unresolvable inside
the container, which mounts its own root at ``/app/data``.

The snapshot drift check reads those rows and counted the unreadable ones as parse
failures: 21 of the 100 it sampled, which is how every finding of the check came
to be about where a path was written from rather than about a parser.

Two halves are contracted here: writes record the path relative to the root, and
reads re-anchor a path recorded against another root -- without ever letting a
stored value reach outside the root it is resolved against.
"""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.source_registry import DataSource, RawSourceSnapshot
from src.repositories.source_registry_repository import save_raw_snapshots
from src.services.snapshot_replay import SnapshotReplayError, load_snapshot_text

HASH = "e29e6b55da6fd4f32a1ebe58c5f0a1b2"


@pytest.fixture
def evidence_dir(tmp_path):
    """A root named like the real one, so a foreign path can name it."""
    root = tmp_path / "crawl_evidence"
    (root / "raw").mkdir(parents=True)
    (root / "raw" / f"{HASH}.bin").write_text("<html>박스스코어</html>", encoding="utf-8")
    return root


class TestWritingThePath:
    @pytest.fixture
    def session(self):
        engine = create_engine("sqlite:///:memory:")
        for table in (DataSource.__table__, RawSourceSnapshot.__table__):
            table.create(engine)
        db = sessionmaker(bind=engine)()
        db.add(
            DataSource(
                source_key="lg_twins_events",
                source_type="html",
                target_domain="kbo",
                reliability="official",
            ),
        )
        db.commit()
        return db

    def test_the_recorded_path_is_relative_to_the_root(self, session, evidence_dir, monkeypatch) -> None:
        """So the same row resolves against whichever root reads it."""
        monkeypatch.setenv("CRAWL_EVIDENCE_DIR", str(evidence_dir))

        saved = save_raw_snapshots(
            session,
            [
                {
                    "source_key": "lg_twins_events",
                    "url": "https://example/",
                    "html": "<html>박스스코어</html>",
                    "status_code": 200,
                }
            ],
        )

        assert saved == 1
        row = session.query(RawSourceSnapshot).one()
        assert row.raw_html_or_json_path is not None
        assert not row.raw_html_or_json_path.startswith("/")
        assert row.raw_html_or_json_path.startswith("raw/")
        assert (evidence_dir / row.raw_html_or_json_path).is_file()

    def test_the_recorded_row_can_be_read_back(self, session, evidence_dir, monkeypatch) -> None:
        monkeypatch.setenv("CRAWL_EVIDENCE_DIR", str(evidence_dir))
        save_raw_snapshots(
            session,
            [
                {
                    "source_key": "lg_twins_events",
                    "url": "https://example/",
                    "html": "<html>박스스코어</html>",
                    "status_code": 200,
                }
            ],
        )
        stored = session.query(RawSourceSnapshot).one().raw_html_or_json_path

        text = load_snapshot_text(stored, allowed_root=evidence_dir)

        assert "박스스코어" in text


class TestReadingAPathRecordedAgainstAnotherRoot:
    def test_a_windows_host_path_is_re_anchored(self, evidence_dir) -> None:
        stored = f"D:\\server-data\\KBO_playwright\\data\\crawl_evidence\\raw\\{HASH}.bin"

        assert "박스스코어" in load_snapshot_text(stored, allowed_root=evidence_dir)

    def test_a_mac_path_is_re_anchored(self, evidence_dir) -> None:
        stored = f"/Users/mac/project/KBO_playwright/data/crawl_evidence/raw/{HASH}.bin"

        assert "박스스코어" in load_snapshot_text(stored, allowed_root=evidence_dir)

    def test_a_relative_path_resolves_against_the_root(self, evidence_dir) -> None:
        assert "박스스코어" in load_snapshot_text(f"raw/{HASH}.bin", allowed_root=evidence_dir)

    def test_a_path_inside_the_root_is_unchanged(self, evidence_dir) -> None:
        stored = str(evidence_dir / "raw" / f"{HASH}.bin")

        assert "박스스코어" in load_snapshot_text(stored, allowed_root=evidence_dir)


class TestWhatIsStillRefused:
    def test_a_url_is_not_a_file(self, evidence_dir) -> None:
        with pytest.raises(SnapshotReplayError, match="is a URL"):
            load_snapshot_text("https://example/artifact.bin", allowed_root=evidence_dir)

    def test_a_foreign_path_with_no_evidence_segment_is_not_guessed(self, evidence_dir) -> None:
        """Without the root's name there is nothing to re-anchor on."""
        with pytest.raises(SnapshotReplayError, match="not found"):
            load_snapshot_text(f"D:\\elsewhere\\{HASH}.bin", allowed_root=evidence_dir)

    def test_a_traversal_is_refused_even_when_the_name_matches(self, evidence_dir, tmp_path) -> None:
        """The tail is joined and then held to the same containment rule."""
        outside = tmp_path / "secret.bin"
        outside.write_text("밖의 파일", encoding="utf-8")

        with pytest.raises(SnapshotReplayError, match="outside the evidence root"):
            load_snapshot_text("crawl_evidence/../secret.bin", allowed_root=evidence_dir)

    def test_an_empty_path_is_refused(self, evidence_dir) -> None:
        with pytest.raises(SnapshotReplayError, match="no stored artifact path"):
            load_snapshot_text(None, allowed_root=evidence_dir)
