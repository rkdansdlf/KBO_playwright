from argparse import Namespace
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from src.cli.run_all_crawlers import (
    _chunk_static_docs,
    _crawl_static_docs,
    _embed_and_save_static_chunks,
    _load_local_markdown_docs,
    _markdown_category,
    _markdown_title,
    _sync_static_chunks_to_oci,
    enrich_and_prepare_contents,
    main,
    run_consistency_check,
    run_dynamic_pipeline,
    run_pipeline_sync,
    run_realtime_pipeline,
    run_static_pipeline,
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

    def test_embed_and_save_static_chunks_embeds_saves_and_syncs(self, monkeypatch):
        monkeypatch.setenv("RUN_SYNC_OCI", "1")
        embedding_svc = MagicMock()
        embedding_svc.get_embeddings_batch.return_value = [[0.1], [0.2]]
        repo = MagicMock()
        repo.upsert_chunks.return_value = 2
        chunks = [{"content": "a", "meta": {}}, {"content": "b", "meta": {}}]
        session = MagicMock()

        with (
            patch("src.cli.run_all_crawlers.get_db_session", return_value=_session_cm(session)),
            patch("src.cli.run_all_crawlers._sync_static_chunks_to_oci") as sync,
            patch("src.cli.run_all_crawlers.enrich_and_prepare_contents", return_value=["a", "b"]),
        ):
            _embed_and_save_static_chunks(embedding_svc, repo, chunks)

        embedding_svc.get_embeddings_batch.assert_called_once_with(["a", "b"])
        repo.upsert_chunks.assert_called_once_with(session, chunks)
        sync.assert_called_once_with(session)
        assert chunks[0]["embedding"] == [0.1]

    def test_sync_static_chunks_to_oci_skips_and_closes_syncer(self):
        session = MagicMock()
        with patch("src.cli.run_all_crawlers.get_oci_url", return_value=None):
            _sync_static_chunks_to_oci(session)

        syncer = MagicMock()
        with (
            patch("src.cli.run_all_crawlers.get_oci_url", return_value="oci"),
            patch.dict("sys.modules", {"src.sync.oci_sync": MagicMock(OCISync=MagicMock(return_value=syncer))}),
        ):
            _sync_static_chunks_to_oci(session)

        syncer.sync_rag_chunks.assert_called_once_with()
        syncer.close.assert_called_once_with()


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

    def test_run_pipeline_sync_routes_and_handles_unknown(self, monkeypatch):
        monkeypatch.delenv("RUN_SYNC_OCI", raising=False)
        monkeypatch.delenv("RUN_SYNC_SUPABASE", raising=False)
        with (
            patch("src.cli.run_all_crawlers.asyncio.run") as run_async,
            patch("src.cli.run_all_crawlers.run_static_pipeline", new=MagicMock(return_value="static")),
            patch("src.cli.run_all_crawlers.run_dynamic_pipeline", new=MagicMock(return_value="dynamic")),
            patch("src.cli.run_all_crawlers.run_realtime_pipeline", new=MagicMock(return_value="realtime")),
            patch("src.cli.run_all_crawlers.run_consistency_check") as consistency,
        ):
            run_pipeline_sync("static", "rules.pdf")
            run_pipeline_sync("dynamic")
            run_pipeline_sync("realtime")
        assert run_async.call_count == 3
        consistency.assert_not_called()

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

    def test_run_static_pipeline_handles_empty_and_processes_docs(self):
        with patch("src.cli.run_all_crawlers._crawl_static_docs", new=AsyncMock(return_value=[])) as crawl:
            asyncio.run(run_static_pipeline("rules.pdf"))
        crawl.assert_awaited_once()

        with (
            patch("src.cli.run_all_crawlers._crawl_static_docs", new=AsyncMock(return_value=[{"doc": 1}])) as crawl,
            patch(
                "src.cli.run_all_crawlers._chunk_static_docs", return_value=[{"content": "chunk", "meta": {}}]
            ) as chunk,
            patch("src.cli.run_all_crawlers._embed_and_save_static_chunks") as embed_save,
        ):
            asyncio.run(run_static_pipeline())

        crawl.assert_awaited_once()
        chunk.assert_called_once()
        embed_save.assert_called_once()

    def test_run_dynamic_pipeline_saves_rosters_and_syncs(self, monkeypatch):
        monkeypatch.setenv("RUN_SYNC_OCI", "1")
        session = MagicMock()
        crawler = MagicMock()
        crawler.crawl_roster_changes = AsyncMock(return_value=[{"player": "p"}])
        team_repo = MagicMock()
        team_repo.save_daily_rosters.return_value = 1
        syncer = MagicMock()
        syncer.sync_ticket_schedules.return_value = 2
        syncer.sync_daily_rosters.return_value = 3

        with (
            patch("src.cli.run_all_crawlers.get_db_session", return_value=_session_cm(session)),
            patch("src.cli.run_all_crawlers.DynamicDataCrawler", return_value=crawler),
            patch("src.repositories.team_repository.TeamRepository", return_value=team_repo),
            patch("src.cli.run_all_crawlers.get_oci_url", return_value="oci"),
            patch.dict("sys.modules", {"src.sync.oci_sync": MagicMock(OCISync=MagicMock(return_value=syncer))}),
        ):
            asyncio.run(run_dynamic_pipeline())

        crawler.crawl_and_update_ticket_times.assert_called_once_with(lookahead_days=14)
        crawler.crawl_roster_changes.assert_awaited_once()
        team_repo.save_daily_rosters.assert_called_once_with([{"player": "p"}])
        syncer.sync_ticket_schedules.assert_called_once_with()
        syncer.sync_daily_rosters.assert_called_once_with()
        syncer.close.assert_called_once_with()

    def test_run_realtime_pipeline_handles_empty_and_processes_chunks(self, monkeypatch):
        monkeypatch.delenv("RUN_SYNC_OCI", raising=False)
        monkeypatch.delenv("RUN_SYNC_SUPABASE", raising=False)
        empty_crawler = MagicMock()
        empty_crawler.fetch_naver_news_headlines.return_value = []
        empty_crawler.fetch_mlbpark_bullpen_posts.return_value = []
        with patch("src.cli.run_all_crawlers.RealtimeIssueCrawler", return_value=empty_crawler):
            asyncio.run(run_realtime_pipeline())

        crawler = MagicMock()
        crawler.fetch_naver_news_headlines.return_value = [{"title": "news"}]
        crawler.fetch_mlbpark_bullpen_posts.return_value = [{"title": "forum"}]
        transformer = MagicMock()
        transformer.chunk_document.side_effect = [[{"content": "n", "meta": {}}], [{"content": "f", "meta": {}}]]
        embedding_svc = MagicMock()
        embedding_svc.get_embeddings_batch.return_value = [[0.1], [0.2]]
        repo = MagicMock()
        repo.upsert_chunks.return_value = 2
        session = MagicMock()

        with (
            patch("src.cli.run_all_crawlers.RealtimeIssueCrawler", return_value=crawler),
            patch("src.cli.run_all_crawlers.TextTransformer", return_value=transformer),
            patch("src.cli.run_all_crawlers.EmbeddingService", return_value=embedding_svc),
            patch("src.cli.run_all_crawlers.RagChunkRepository", return_value=repo),
            patch("src.cli.run_all_crawlers.enrich_and_prepare_contents", return_value=["n", "f"]),
            patch("src.cli.run_all_crawlers.get_db_session", return_value=_session_cm(session)),
        ):
            asyncio.run(run_realtime_pipeline())

        assert transformer.chunk_document.call_count == 2
        embedding_svc.get_embeddings_batch.assert_called_once_with(["n", "f"])
        repo.upsert_chunks.assert_called_once()

    def test_start_scheduler_registers_jobs_and_handles_stop(self):
        scheduler = MagicMock()
        scheduler.get_jobs.return_value = [MagicMock(id="job", name="Job", next_run_time="later")]
        scheduler.start.side_effect = KeyboardInterrupt

        with patch("src.cli.run_all_crawlers.BlockingScheduler", return_value=scheduler):
            start_scheduler()

        assert scheduler.add_job.call_count == 4
        scheduler.start.assert_called_once()


def _session_cm(session):
    session_cm = MagicMock()
    session_cm.__enter__.return_value = session
    return session_cm
