#!/usr/bin/env python3
"""
ingest_pdf.py
─────────────
Ingests a PDF into a ChromaDB vector database.
Pipeline: PDF → text extraction → overlapping chunks → embeddings → ChromaDB

Usage:
    python ingest_pdf.py path/to/document.pdf [--db-dir ./vector_db] [--collection docs]
                        [--chunk-size 512] [--overlap 128]

Dependencies (auto-installed if missing):
    pip install pypdf chromadb sentence-transformers
"""

import argparse
import json
import re
import sys
import subprocess
from pathlib import Path


# ── Dependency bootstrap ──────────────────────────────────────────────────────
def _ensure(*pkgs):
    for pkg in pkgs:
        try:
            __import__(pkg.split("[")[0].replace("-", "_"))
        except ImportError:
            print(f"[ingest] Installing {pkg}…")
            subprocess.check_call([sys.executable, "-m", "pip", "install", pkg, "--quiet"])

_ensure("pypdf", "chromadb", "sentence_transformers")

# ── Imports (after install) ───────────────────────────────────────────────────
from pypdf import PdfReader                           # noqa: E402
import chromadb                                       # noqa: E402
from chromadb.utils.embedding_functions import (      # noqa: E402
    SentenceTransformerEmbeddingFunction,
)


# ══════════════════════════════════════════════════════════════════════════════
#  STEP 1 – Extract text from PDF
# ══════════════════════════════════════════════════════════════════════════════

def extract_text(pdf_path: Path) -> list[dict]:
    """Return list of {page, text} dicts."""
    reader = PdfReader(str(pdf_path))
    pages = []
    for i, page in enumerate(reader.pages):
        text = page.extract_text() or ""
        text = re.sub(r"\s+", " ", text).strip()
        if text:
            pages.append({"page": i + 1, "text": text})
    return pages


# ══════════════════════════════════════════════════════════════════════════════
#  STEP 2 – Sliding-window chunker
# ══════════════════════════════════════════════════════════════════════════════

def chunk_text(
    pages: list[dict],
    chunk_size: int = 512,
    overlap: int = 128,
) -> list[dict]:
    """
    Splits page text into overlapping word-level windows.
    Returns list of:
      { chunk_id, text, page, chunk_index, word_start, word_end }
    """
    chunks = []
    chunk_idx = 0

    for page_info in pages:
        words = page_info["text"].split()
        page_num = page_info["page"]
        start = 0

        while start < len(words):
            end = min(start + chunk_size, len(words))
            chunk_text_str = " ".join(words[start:end])

            chunks.append({
                "chunk_id": f"p{page_num}_c{chunk_idx}",
                "text": chunk_text_str,
                "page": page_num,
                "chunk_index": chunk_idx,
                "word_start": start,
                "word_end": end,
            })
            chunk_idx += 1

            if end == len(words):
                break
            start += chunk_size - overlap   # slide with overlap

    return chunks


# ══════════════════════════════════════════════════════════════════════════════
#  STEP 3 – Embed & store in ChromaDB
# ══════════════════════════════════════════════════════════════════════════════

DEFAULT_MODEL = "all-MiniLM-L6-v2"   # fast, 384-dim, MIT license

def embed_and_store(
    chunks: list[dict],
    db_dir: str = "./vector_db",
    collection_name: str = "docs",
    model_name: str = DEFAULT_MODEL,
    source_name: str = "unknown",
) -> dict:
    """Upserts chunks into ChromaDB. Returns ingestion stats."""

    print(f"[ingest] Loading embedding model: {model_name}")
    ef = SentenceTransformerEmbeddingFunction(model_name=model_name)

    client = chromadb.PersistentClient(path=db_dir)
    collection = client.get_or_create_collection(
        name=collection_name,
        embedding_function=ef,
        metadata={"hnsw:space": "cosine"},
    )

    ids       = [c["chunk_id"]   for c in chunks]
    documents = [c["text"]       for c in chunks]
    metadatas = [
        {
            "page":        c["page"],
            "chunk_index": c["chunk_index"],
            "word_start":  c["word_start"],
            "word_end":    c["word_end"],
            "source":      source_name,
        }
        for c in chunks
    ]

    # Upsert in batches of 128 to stay memory-friendly
    batch = 128
    for i in range(0, len(chunks), batch):
        collection.upsert(
            ids=ids[i : i + batch],
            documents=documents[i : i + batch],
            metadatas=metadatas[i : i + batch],
        )
        print(f"[ingest] Upserted chunks {i+1}–{min(i+batch, len(chunks))} / {len(chunks)}")

    stats = {
        "source":       source_name,
        "collection":   collection_name,
        "db_dir":       db_dir,
        "total_chunks": len(chunks),
        "model":        model_name,
    }
    print(f"\n[ingest] ✅ Done! Stats:\n{json.dumps(stats, indent=2)}")
    return stats


# ══════════════════════════════════════════════════════════════════════════════
#  CLI entry-point
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Ingest a PDF into ChromaDB")
    parser.add_argument("pdf", help="Path to the PDF file")
    parser.add_argument("--db-dir",     default="./data/vector_db",  help="ChromaDB persistence directory")
    parser.add_argument("--collection", default="docs",         help="Collection name")
    parser.add_argument("--chunk-size", type=int, default=512,  help="Words per chunk (default 512)")
    parser.add_argument("--overlap",    type=int, default=128,  help="Overlap in words (default 128)")
    parser.add_argument("--model",      default=DEFAULT_MODEL,  help="SentenceTransformer model name")
    args = parser.parse_args()

    pdf_path = Path(args.pdf)
    if not pdf_path.exists():
        sys.exit(f"[ingest] ❌ File not found: {pdf_path}")

    print(f"[ingest] Reading PDF: {pdf_path}")
    pages = extract_text(pdf_path)
    print(f"[ingest] Extracted {len(pages)} pages with text")

    print(f"[ingest] Chunking (size={args.chunk_size}, overlap={args.overlap})")
    chunks = chunk_text(pages, chunk_size=args.chunk_size, overlap=args.overlap)
    print(f"[ingest] Created {len(chunks)} chunks")

    embed_and_store(
        chunks,
        db_dir=args.db_dir,
        collection_name=args.collection,
        model_name=args.model,
        source_name=pdf_path.name,
    )


if __name__ == "__main__":
    main()
