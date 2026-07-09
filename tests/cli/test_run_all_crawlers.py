from argparse import Namespace
import asyncio
from unittest.mock import MagicMock, patch

from src.cli.run_all_crawlers import (
    _chunk_static_docs,
    _crawl_static_docs,
    _embed_and_save_static_chunks,
    _load_local_markdown_docs,
    _markdown_category,
    _markdown_title,
    enrich_and_prepare_contents,
    main,
    run_consistency_check,
    run_pipeline_sync,
    start_scheduler,
)


class TestRunAllCrawlers:
    def test_no_args_prints_help(self):
        try:
            main()
            raise AssertionError("Should have raised SystemExit")
        except SystemExit:
            pass

    def test_static_pipeline(self):
        with (
            patch("argparse.ArgumentParser.parse_args") as mock_parse,
            patch("src.cli.run_all_crawlers.run_pipeline_sync") as mock,
        ):
            mock_parse.return_value = Namespace(type="static", pdf=None, daemon=False)
            mock.return_value = None
            result = main()
            assert result == 0

    def test_dynamic_pipeline(self):
        with (
            patch("argparse.ArgumentParser.parse_args") as mock_parse,
            patch("src.cli.run_all_crawlers.run_pipeline_sync") as mock,
        ):
            mock_parse.return_value = Namespace(type="dynamic", pdf=None, daemon=False)
            mock.return_value = None
            result = main()
            assert result == 0

    def test_realtime_pipeline(self):
        with (
            patch("argparse.ArgumentParser.parse_args") as mock_parse,
            patch("src.cli.run_all_crawlers.run_pipeline_sync") as mock,
        ):
            mock_parse.return_value = Namespace(type="realtime", pdf=None, daemon=False)
            mock.return_value = None
            result = main()
            assert result == 0


class TestRunAllCrawlerHelpers:
    def test_markdown_category_maps_nested_parts(self):
        assert _markdown_category(["baseball_rules", "foo.md"]) == ("game_rules", None)
        assert _markdown_category(["baseball_rules", "glossary", "foo.md"]) == ("game_rules", "glossary")
        assert _markdown_category(["unknown", "foo.md"]) == ("rulebook", None)

    def test_markdown_title_prefers_heading(self):
        assert _markdown_title("# Official Rule\nBody", "fallback_name.md") == "Official Rule"
        assert _markdown_title("Body", "fallback_name.md") == "Fallback Name"

    def test_load_local_markdown_docs_missing_dir(self, tmp_path):
        assert _load_local_markdown_docs(str(tmp_path / "missing")) == []

    def test_load_local_markdown_docs_reads_markdown(self, tmp_path):
        root = tmp_path / "Docs" / "baseball" / "glossary"
        root.mkdir(parents=True)
        (root / "term.md").write_text("# Term\nDefinition", encoding="utf-8")
        (root / "ignore.txt").write_text("nope", encoding="utf-8")

        docs = _load_local_markdown_docs(str(tmp_path / "Docs" / "baseball"))

        assert len(docs) == 1
        assert docs[0]["title"] == "Term"
        assert docs[0]["meta"]["category"] == "glossary"

    def test_enrich_and_prepare_contents_without_enrichment(self):
        chunks = [
            {"content": "plain", "meta": {}},
            {"content": "rich", "meta": {"keywords": ["k"], "questions": ["q?"]}},
        ]
        with patch("src.services.metadata_enrichment_service.MetadataEnrichmentService") as svc_cls:
            svc_cls.return_value.enabled = False
            result = enrich_and_prepare_contents(chunks)

        assert result[0] == "plain"
        assert "Keywords: k" in result[1]

    def test_enrich_and_prepare_contents_with_enrichment(self):
        chunks = [{"content": "content", "meta": {}}]
        with patch("src.services.metadata_enrichment_service.MetadataEnrichmentService") as svc_cls:
            svc_cls.return_value.enabled = True
            svc_cls.return_value.enrich_chunk.return_value = {"summary": "s", "keywords": ["k"], "questions": ["q"]}
            result = enrich_and_prepare_contents(chunks)

        assert "Summary: s" in result[0]
        assert chunks[0]["meta"]["summary"] == "s"

    def test_chunk_static_docs_delegates_transformer(self):
        transformer = MagicMock()
        transformer.chunk_document.side_effect = [[{"id": 1}], [{"id": 2}, {"id": 3}]]
        result = _chunk_static_docs(transformer, [{"doc": 1}, {"doc": 2}])
        assert result == [{"id": 1}, {"id": 2}, {"id": 3}]

    def test_embed_and_save_static_chunks_skips_empty(self):
        _embed_and_save_static_chunks(MagicMock(), MagicMock(), [])


class TestRunAllCrawlerAsyncAndRouting:
    def test_crawl_static_docs_pdf_missing_and_namuwiki_failure(self, tmp_path):
        crawler = MagicMock()
        crawler.crawl_namuwiki.side_effect = RuntimeError("boom")

        with patch("src.cli.run_all_crawlers._load_local_markdown_docs", return_value=[{"title": "local"}]):
            docs = asyncio.run(_crawl_static_docs(crawler, str(tmp_path / "missing.pdf")))

        assert docs == [{"title": "local"}]

    def test_run_consistency_check_skips_without_oci_url(self):
        with patch("src.cli.run_all_crawlers.get_oci_url", return_value=None):
            run_consistency_check()

    def test_run_consistency_check_success_and_failure(self):
        with (
            patch("src.cli.run_all_crawlers.get_oci_url", return_value="oci"),
            patch("src.cli.run_all_crawlers.run_consistency_audit", return_value=True) as audit,
        ):
            run_consistency_check(deep=True)
        audit.assert_called_once_with(deep=True, trigger_alert=True)

        with (
            patch("src.cli.run_all_crawlers.get_oci_url", return_value="oci"),
            patch("src.cli.run_all_crawlers.run_consistency_audit", side_effect=RuntimeError("boom")),
            patch("src.cli.run_all_crawlers.SlackWebhookClient.send_error_alert") as send_alert,
        ):
            run_consistency_check()
        send_alert.assert_called_once()

    def test_run_pipeline_sync_routes_and_handles_unknown(self):
        with (
            patch("src.cli.run_all_crawlers.asyncio.run") as run_async,
            patch("src.cli.run_all_crawlers.run_static_pipeline", new=MagicMock(return_value="static")),
            patch("src.cli.run_all_crawlers.run_dynamic_pipeline", new=MagicMock(return_value="dynamic")),
            patch("src.cli.run_all_crawlers.run_realtime_pipeline", new=MagicMock(return_value="realtime")),
        ):
            run_pipeline_sync("static", "rules.pdf")
            run_pipeline_sync("dynamic")
            run_pipeline_sync("realtime")
        assert run_async.call_count == 3

        run_pipeline_sync("unknown")

    def test_run_pipeline_sync_alerts_on_failure_and_runs_consistency_when_sync_enabled(self, monkeypatch):
        monkeypatch.setenv("RUN_SYNC_OCI", "1")
        with (
            patch("src.cli.run_all_crawlers.asyncio.run", side_effect=[None, RuntimeError("boom")]),
            patch("src.cli.run_all_crawlers.run_dynamic_pipeline", new=MagicMock(return_value="dynamic")),
            patch("src.cli.run_all_crawlers.run_consistency_check") as consistency,
            patch("src.cli.run_all_crawlers.SlackWebhookClient.send_error_alert") as send_alert,
        ):
            run_pipeline_sync("dynamic")
            run_pipeline_sync("dynamic")

        consistency.assert_called_once_with(deep=False)
        send_alert.assert_called_once()

    def test_start_scheduler_registers_jobs_and_handles_stop(self):
        scheduler = MagicMock()
        scheduler.get_jobs.return_value = [MagicMock(id="job", name="Job", next_run_time="later")]
        scheduler.start.side_effect = KeyboardInterrupt

        with patch("src.cli.run_all_crawlers.BlockingScheduler", return_value=scheduler):
            start_scheduler()

        assert scheduler.add_job.call_count == 4
        scheduler.start.assert_called_once()
