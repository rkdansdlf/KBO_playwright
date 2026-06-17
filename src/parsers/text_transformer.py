"""
Data transformer for cleansing and chunking extracted raw text before embedding.
"""

from __future__ import annotations

import hashlib
import re
from typing import Any


class TextTransformer:
    """
    Cleans raw text and splits it into semantic chunks based on document type (rules vs news).
    """

    def clean_text(self, text: str) -> str:
        """
        Removes noisy characters, consecutive whitespace, tabs, and markdown clutter.
        """
        if not text:
            return ""

        # 1. Normalize carriage returns
        text = text.replace("\r\n", "\n").replace("\r", "\n")

        # 2. Remove whitespace/tabs on the boundaries of newlines
        text = re.sub(r"[ \t]*\n[ \t]*", "\n", text)

        # 3. Replace multiple consecutive tabs/spaces on single lines with a single space
        text = re.sub(r"[ \t]+", " ", text)

        # 4. Remove multiple consecutive empty lines
        text = re.sub(r"\n{3,}", "\n\n", text)

        # 5. Strip leading/trailing whitespaces
        return text.strip()

    def chunk_document(self, doc: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Main entry point for chunking. Dispatches to the appropriate chunking strategy
        based on the document's category metadata and environment configuration.
        """
        import os

        content = self.clean_text(doc.get("content", ""))
        meta = doc.get("meta", {}).copy()
        category = meta.get("category", "unknown")
        title = doc.get("title", "")

        if not content:
            return []

        if category in ("rulebook", "rules", "namuwiki"):
            # Rules/Glossary structure chunking by headers
            return self.chunk_by_headings(title, content, meta)
        # Route by strategy environment variable
        strategy = os.getenv("CHUNK_STRATEGY", "overlap").lower()
        if strategy == "semantic":
            return self.chunk_semantically(title, content, meta)
        if strategy == "parent_child":
            return self.chunk_parent_child(title, content, meta)
        return self.chunk_with_overlap(title, content, meta, chunk_char_limit=800, overlap_char_limit=150)

    def chunk_semantically(
        self,
        doc_title: str,
        text: str,
        meta: dict[str, Any],
        similarity_threshold: float = 0.6,
    ) -> list[dict[str, Any]]:
        """
        Splits text into sentences, generates embeddings, calculates cosine similarity between adjacent sentences,
        and splits the document at boundaries where similarity falls below the threshold.
        """
        # 1. Split text into sentences using simple regex
        sentences = [s.strip() for s in re.split(r"(?<=[.!?])\s+", text) if s.strip()]
        if len(sentences) <= 1:
            return [self._create_news_chunk(doc_title, text, meta, 1)]

        # 2. Get embeddings for each sentence (dynamically load EmbeddingService)
        from src.services.embedding_service import EmbeddingService

        embedding_svc = EmbeddingService()
        embeddings = embedding_svc.get_embeddings_batch(sentences)

        # 3. Calculate cosine similarity between adjacent sentences
        def cosine_similarity(v1, v2) -> float:
            # Since our embeddings are already L2 normalized: similarity = dot product
            return sum(x * y for x, y in zip(v1, v2, strict=False))

        similarities = []
        for i in range(len(sentences) - 1):
            sim = cosine_similarity(embeddings[i], embeddings[i + 1])
            similarities.append(sim)

        # 4. Group sentences into chunks based on similarities
        chunks_text = []
        current_chunk = [sentences[0]]

        for i, sim in enumerate(similarities):
            # If similarity is below threshold, split here
            if sim < similarity_threshold:
                chunks_text.append(" ".join(current_chunk))
                current_chunk = [sentences[i + 1]]
            else:
                current_chunk.append(sentences[i + 1])

        if current_chunk:
            chunks_text.append(" ".join(current_chunk))

        # 5. Create final chunks
        chunks = []
        for idx, content in enumerate(chunks_text):
            chunks.append(self._create_news_chunk(doc_title, content, meta, idx + 1))

        return chunks

    def chunk_parent_child(
        self,
        doc_title: str,
        text: str,
        meta: dict[str, Any],
        parent_size: int = 1000,
        child_size: int = 250,
        child_overlap: int = 50,
    ) -> list[dict[str, Any]]:
        """
        Divides the document into large parent chunks, then splits each parent into smaller child chunks.
        Stores parent content inside the child's metadata so that the child can be retrieved via embedding,
        while the parent context is passed to the LLM.
        """
        # 1. Split into parent chunks using paragraph overlap
        parent_chunks = self.chunk_with_overlap(
            doc_title,
            text,
            meta,
            chunk_char_limit=parent_size,
            overlap_char_limit=100,
        )

        all_child_chunks = []
        child_idx = 1

        for p_idx, parent in enumerate(parent_chunks):
            parent_content = parent["content"]

            # 2. Split this parent's content into smaller child chunks
            child_texts = []
            paragraphs = [p.strip() for p in parent_content.split("\n\n") if p.strip()]
            if not paragraphs:
                paragraphs = [p.strip() for p in parent_content.split("\n") if p.strip()]

            current_child = ""
            for para in paragraphs:
                if len(current_child) + len(para) > child_size:
                    if current_child:
                        child_texts.append(current_child)
                        current_child = current_child[-child_overlap:] + "\n\n" + para
                    else:
                        current_child = para
                else:
                    if current_child:
                        current_child += "\n\n" + para
                    else:
                        current_child = para
            if current_child:
                child_texts.append(current_child)

            # 3. Create child chunks with parent content referenced in meta
            for c_txt in child_texts:
                child_meta = meta.copy()
                child_meta["chunk_index"] = child_idx
                child_meta["parent_chunk_index"] = p_idx + 1
                child_meta["parent_content"] = parent_content

                unique_str = f"{child_meta.get('source', '')}_child_{child_idx}_{c_txt}"
                row_id_hash = hashlib.sha256(unique_str.encode("utf-8")).hexdigest()
                child_meta["source_row_id"] = row_id_hash

                all_child_chunks.append(
                    {"title": f"{doc_title} (Child {child_idx})", "content": c_txt.strip(), "meta": child_meta},
                )
                child_idx += 1

        return all_child_chunks

    def chunk_by_headings(self, doc_title: str, text: str, meta: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Splits rulebooks or glossaries by clause headings.

        Recognized heading patterns:
          - Markdown: ## 개요, ### 조항 N
          - Korean clause: 제 N조, 제N조:
          - Numbered section: 1. 가나다 (bullet with Korean/alpha text)
          - English article: ARTICLE N

        Also extracts a '## 키워드' section as meta["keywords"] list and
        merges stub chunks (< 50 chars) into the preceding chunk.
        """
        import logging as _log

        _logger = _log.getLogger(__name__)

        chunks = []

        # 1. Extract keywords block before chunking (removes it from main text)
        keywords: list[str] = []
        keyword_pattern = re.compile(r"^#{1,3}\s*키워드\s*\n(.*?)(?=^#{1,3}\s|\Z)", re.MULTILINE | re.DOTALL)
        kw_match = keyword_pattern.search(text)
        if kw_match:
            kw_block = kw_match.group(1)
            # Each line may have comma-separated items or dash-prefixed items
            for line in kw_block.splitlines():
                line = line.strip().lstrip("-").strip()
                if line:
                    for kw in re.split(r"[,，]", line):
                        kw = kw.strip()
                        if kw:
                            keywords.append(kw)
            # Remove the keyword block from text to avoid it becoming its own chunk
            text = keyword_pattern.sub("", text).strip()

        # 2. Split by heading patterns (markdown + PDF-style)
        #    Extended to cover: ## H, ### H, 제 N조, ARTICLE N, 1. 한글/Alpha
        pattern = re.compile(
            r"(?=(?:"
            r"^\s*#{1,3}\s+조항\s+\d+"  # ### 조항 N
            r"|^\s*제\s*\d+\s*조(?:\s|\.|\:)"  # 제 N조. / 제N조: / 제 N조 (보다 정확한 구분자 매치)
            r"|^\s*#{1,3}\s+[\uac00-\ud7a3A-Za-z]"  # ## 개요, ### Glossary
            r"|^\s*ARTICLE\s+\d+"  # ARTICLE 1 (English PDF)
            r"|^\s*\d+\.\s+[\uac00-\ud7a3A-Z]"  # 1. 가나다 (numbered sections)
            r"))",
            re.MULTILINE,
        )
        sections = pattern.split(text)

        section_idx = 1
        for sec in sections:
            sec_clean = sec.strip()
            if not sec_clean:
                continue

            # Attempt to extract heading title from first line
            lines = sec_clean.split("\n")
            heading = lines[0].strip().replace("#", "").strip()
            if heading.endswith(":"):
                heading = heading[:-1].strip()

            # Content is the rest of the section
            sec_content = "\n".join(lines[1:]).strip() if len(lines) > 1 else sec_clean

            if not sec_content:
                sec_content = sec_clean
                heading = f"{doc_title} - Section {section_idx}"

            chunk_title = f"{doc_title} - {heading}" if heading not in doc_title else doc_title

            chunk_meta = meta.copy()
            chunk_meta["heading"] = heading
            chunk_meta["chunk_index"] = section_idx
            if keywords:
                chunk_meta["keywords"] = keywords

            # Generate a stable unique ID hash for this chunk
            unique_str = f"{chunk_meta.get('source', '')}_{chunk_title}_{sec_content}"
            row_id_hash = hashlib.sha256(unique_str.encode("utf-8")).hexdigest()
            chunk_meta["source_row_id"] = row_id_hash

            chunks.append(
                {
                    "title": chunk_title,
                    "content": sec_clean,  # Maintain heading in contents for RAG context
                    "meta": chunk_meta,
                },
            )
            section_idx += 1

        # 3. Merge stub chunks (< 30 chars) into previous chunk
        #    30-char threshold keeps real short clauses intact while eliminating true stubs
        merged: list[dict[str, Any]] = []
        for chunk in chunks:
            if merged and len(chunk["content"]) < 30:
                # Append this stub to the previous chunk's content
                merged[-1]["content"] += "\n" + chunk["content"]
            else:
                merged.append(chunk)

        _logger.debug(
            "chunk_by_headings: %s raw sections → %s chunks → %s after merge (keywords extracted: %s)",
            len(sections),
            len(chunks),
            len(merged),
            len(keywords),
        )

        return merged

    def chunk_with_overlap(
        self,
        doc_title: str,
        text: str,
        meta: dict[str, Any],
        chunk_char_limit: int = 800,
        overlap_char_limit: int = 150,
    ) -> list[dict[str, Any]]:
        """
        Splits articles or columns by paragraph blocks, merging them until the character limit is reached,
        then overlaps 10%-20% of the text.
        """
        chunks = []

        # Split text into paragraphs
        paragraphs = [p.strip() for p in text.split("\n\n") if p.strip()]
        if not paragraphs:
            # Fallback to single line split
            paragraphs = [p.strip() for p in text.split("\n") if p.strip()]

        current_chunk_text = ""
        chunk_idx = 1

        for para in paragraphs:
            # If paragraph is huge, split it by sentences
            if len(para) > chunk_char_limit:
                sentences = re.split(r"(?<=[.!?])\s+", para)
                for sent in sentences:
                    if len(current_chunk_text) + len(sent) > chunk_char_limit:
                        if current_chunk_text:
                            chunks.append(self._create_news_chunk(doc_title, current_chunk_text, meta, chunk_idx))
                            chunk_idx += 1
                            # Retain overlap from end of previous text
                            current_chunk_text = current_chunk_text[-overlap_char_limit:] + " " + sent
                        else:
                            # Sentence is larger than limit, force append
                            current_chunk_text = sent
                    else:
                        current_chunk_text = (current_chunk_text + " " + sent).strip()
            else:
                if len(current_chunk_text) + len(para) > chunk_char_limit:
                    if current_chunk_text:
                        chunks.append(self._create_news_chunk(doc_title, current_chunk_text, meta, chunk_idx))
                        chunk_idx += 1
                        # Overlap: take characters from current chunk end
                        current_chunk_text = current_chunk_text[-overlap_char_limit:] + "\n\n" + para
                    else:
                        current_chunk_text = para
                else:
                    if current_chunk_text:
                        current_chunk_text += "\n\n" + para
                    else:
                        current_chunk_text = para

        if current_chunk_text:
            chunks.append(self._create_news_chunk(doc_title, current_chunk_text, meta, chunk_idx))

        return chunks

    def _create_news_chunk(self, doc_title: str, content: str, meta: dict[str, Any], index: int) -> dict[str, Any]:
        chunk_meta = meta.copy()
        chunk_meta["chunk_index"] = index

        # Generate stable unique ID hash based on content and source
        unique_str = f"{chunk_meta.get('source', '')}_chunk_{index}_{content}"
        row_id_hash = hashlib.sha256(unique_str.encode("utf-8")).hexdigest()
        chunk_meta["source_row_id"] = row_id_hash

        return {"title": f"{doc_title} (Part {index})", "content": content.strip(), "meta": chunk_meta}
