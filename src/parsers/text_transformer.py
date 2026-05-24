"""
Data transformer for cleansing and chunking extracted raw text before embedding.
"""
from __future__ import annotations

import re
import hashlib
from datetime import datetime
from typing import Dict, List, Any

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
        
        # 1. Replace multiple consecutive tabs/spaces with a single space
        text = re.sub(r'[ \t]+', ' ', text)
        
        # 2. Normalize carriage returns and remove multiple empty lines
        text = text.replace('\r\n', '\n').replace('\r', '\n')
        text = re.sub(r'\n{3,}', '\n\n', text)
        
        # 3. Strip leading/trailing whitespaces
        return text.strip()

    def chunk_document(self, doc: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Main entry point for chunking. Dispatches to the appropriate chunking strategy
        based on the document's category metadata.
        """
        content = self.clean_text(doc.get("content", ""))
        meta = doc.get("meta", {}).copy()
        category = meta.get("category", "unknown")
        title = doc.get("title", "")

        if not content:
            return []

        if category in ("rulebook", "rules", "namuwiki"):
            # Rules/Glossary structure chunking by headers
            return self.chunk_by_headings(title, content, meta)
        else:
            # News/Paragraph chunking with overlap
            return self.chunk_with_overlap(title, content, meta, chunk_char_limit=800, overlap_char_limit=150)

    def chunk_by_headings(self, doc_title: str, text: str, meta: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Splits rulebooks or glossaries by clause headings like "제 N조", "### 조항", or heading blocks.
        """
        chunks = []
        
        # Split by typical markdown headers or Korean rule patterns:
        # e.g., "### 조항 1:", "제 1조:", "## 개요", etc.
        pattern = r'(?=(?:^\s*###\s+조항\s+\d+|^\s*제\s*\d+\s*조|^\s*##\s+|^\s*###\s+))'
        sections = re.split(pattern, text, flags=re.MULTILINE)
        
        section_idx = 1
        for sec in sections:
            sec_clean = sec.strip()
            if not sec_clean:
                continue
                
            # Attempt to extract heading title from first line
            lines = sec_clean.split('\n')
            heading = lines[0].strip().replace('#', '').strip()
            if heading.endswith(':'):
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
            
            # Generate a stable unique ID hash for this chunk
            unique_str = f"{chunk_meta.get('source', '')}_{chunk_title}_{sec_content}"
            row_id_hash = hashlib.sha256(unique_str.encode('utf-8')).hexdigest()
            chunk_meta["source_row_id"] = row_id_hash

            chunks.append({
                "title": chunk_title,
                "content": sec_clean, # Maintain heading in contents for RAG context
                "meta": chunk_meta
            })
            section_idx += 1

        return chunks

    def chunk_with_overlap(
        self, 
        doc_title: str, 
        text: str, 
        meta: Dict[str, Any], 
        chunk_char_limit: int = 800, 
        overlap_char_limit: int = 150
    ) -> List[Dict[str, Any]]:
        """
        Splits articles or columns by paragraph blocks, merging them until the character limit is reached,
        then overlaps 10%-20% of the text.
        """
        chunks = []
        
        # Split text into paragraphs
        paragraphs = [p.strip() for p in text.split('\n\n') if p.strip()]
        if not paragraphs:
            # Fallback to single line split
            paragraphs = [p.strip() for p in text.split('\n') if p.strip()]

        current_chunk_text = ""
        chunk_idx = 1

        for para in paragraphs:
            # If paragraph is huge, split it by sentences
            if len(para) > chunk_char_limit:
                sentences = re.split(r'(?<=[.!?])\s+', para)
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

    def _create_news_chunk(self, doc_title: str, content: str, meta: Dict[str, Any], index: int) -> Dict[str, Any]:
        chunk_meta = meta.copy()
        chunk_meta["chunk_index"] = index
        
        # Generate stable unique ID hash based on content and source
        unique_str = f"{chunk_meta.get('source', '')}_chunk_{index}_{content}"
        row_id_hash = hashlib.sha256(unique_str.encode('utf-8')).hexdigest()
        chunk_meta["source_row_id"] = row_id_hash

        return {
            "title": f"{doc_title} (Part {index})",
            "content": content.strip(),
            "meta": chunk_meta
        }
