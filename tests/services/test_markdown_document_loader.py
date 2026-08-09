from __future__ import annotations

from src.services.markdown_document_loader import load_local_markdown_docs, markdown_source_table


def test_load_local_markdown_docs_returns_json_safe_source_metadata(tmp_path):
    document = tmp_path / "kbo_rulebook" / "league_regulations" / "01_regular_season.md"
    document.parent.mkdir(parents=True)
    document.write_text("# Regular season\n\nGames are played under KBO rules.", encoding="utf-8")

    docs = load_local_markdown_docs(tmp_path)

    assert len(docs) == 1
    assert docs[0]["meta"]["source"] == str(document)
    assert docs[0]["meta"]["source_path"] == "kbo_rulebook/league_regulations/01_regular_season.md"
    assert markdown_source_table(docs[0]) == "kbo_regulations"


def test_markdown_source_table_classifies_definitions_and_general_docs(tmp_path):
    glossary = tmp_path / "glossary.md"
    general = tmp_path / "kbo_culture.md"
    glossary.write_text("# Glossary\n\nDefinition", encoding="utf-8")
    general.write_text("# Culture\n\nGeneral document", encoding="utf-8")

    docs = load_local_markdown_docs(tmp_path)
    by_name = {doc["meta"]["source_file"]: doc for doc in docs}

    assert markdown_source_table(by_name["glossary.md"]) == "kbo_definitions"
    assert markdown_source_table(by_name["kbo_culture.md"]) == "markdown_docs"
