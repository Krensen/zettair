#!/usr/bin/env python3
"""
build_docstore.py — build a random-access document store from enwiki.trec

Produces:
  enwiki.docstore   — raw concatenated plain text (UTF-8)
  enwiki.docmap     — JSON: {docno: [offset, length], ...}

The docstore contains one entry per article: just the plain text from <TEXT>...</TEXT>,
with a single trailing newline. The docmap gives the byte offset and length so
server.py can fseek() to any article in O(1).

Usage:
  python3 build_docstore.py
"""

import os
import json
import sys
import re
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_default   = os.path.join(SCRIPT_DIR, 'enwiki.trec')
TREC_FILE  = sys.argv[1] if len(sys.argv) > 1 else _default
STORE_FILE = TREC_FILE.replace('.trec', '.docstore')
MAP_FILE   = TREC_FILE.replace('.trec', '.docmap')

# Simple, fast patterns — no backtracking risk.
_RE_ISBN    = re.compile(r'ISBN(?:-1[03])?[\s:]*[\d][\d\-X]{8,16}')
_RE_BULLET  = re.compile(r'\*\s+')   # bullet marker anywhere


def _is_citation_sentence(sent: str) -> bool:
    """
    Return True if this sentence looks like a bibliographic citation rather
    than prose.  We use cheap heuristics in priority order:

    1. Contains an ISBN — dead giveaway.
    2. Starts with a capitalised surname + comma pattern and contains a year:
       "Perring, Dominic 1991."  "Dunwoodie, Lesley. 2015."
       We only check the START of the sentence to avoid false positives on
       prose like "In London, Ontario, the 2003 census..."
    3. Ends with a publisher name pattern (University Press, Press, Ltd. etc.)
       and is short (< 120 chars) — these are trailing reference fragments.
    4. Predominantly non-alpha (< 40%) — coordinate strings, ISBNs without
       the keyword, etc.
    """
    # 1. ISBN
    if 'ISBN' in sent:
        return True

    # 2. Citation opener: Surname, Initial(s) YYYY  or  Surname, Name YYYY
    #    Must be at the very start (stripped), and contain a 4-digit year.
    if re.match(r'^[A-Z][a-zA-Z\-]+,\s+[A-Z]', sent):
        if re.search(r'\b(19|20)\d{2}\b', sent):
            return True

    # 3. Short trailing publisher fragment
    if len(sent) < 120 and re.search(
            r'(?:University Press|Cambridge|Oxford|Routledge|Springer|'
            r'Penguin|Bloomsbury|Harvard|Princeton|Yale|MIT Press)\s*\.',
            sent):
        return True

    # 4. Alpha ratio
    alpha = sum(1 for c in sent if c.isalpha())
    if len(sent) > 15 and alpha / len(sent) < 0.40:
        return True

    return False


def _filter_one_paragraph(para: str) -> str:
    """Sentence-tokenise one paragraph, drop citation sentences, rejoin.
    Preserved as its own function so the outer call site can iterate
    over paragraphs without losing paragraph structure."""
    # Strip bullet markers
    para = _RE_BULLET.sub('', para)
    # Remove raw ISBN blocks
    para = _RE_ISBN.sub('', para)
    # Sentence boundaries: . / ! / ? followed by whitespace + uppercase.
    parts = re.split(r'([.!?])\s+(?=[A-Z(])', para)
    sentences = []
    i = 0
    while i < len(parts):
        seg = parts[i]
        if i + 1 < len(parts) and parts[i+1] in '.!?':
            seg = seg + parts[i+1]
            i += 2
        else:
            i += 1
        seg = seg.strip()
        if seg:
            sentences.append(seg)
    prose = [s for s in sentences if not _is_citation_sentence(s)]
    out = ' '.join(prose)
    out = re.sub(r' {2,}', ' ', out)
    return out.strip()


def strip_wiki_markup(text: str) -> str:
    """
    Clean text that has already been through wiki2trec.py's clean() pass.
    Strategy: split on paragraph boundaries first (preserved by the
    PRD-030 step E clean() update), then run sentence-level citation
    filtering inside each paragraph. Rejoin with a paragraph break so
    the downstream summariser can identify the lede and section
    boundaries.
    """
    paragraphs = text.split('\n\n')
    out_paragraphs: list[str] = []
    for para in paragraphs:
        para = para.strip()
        if not para:
            continue
        cleaned = _filter_one_paragraph(para)
        if cleaned:
            out_paragraphs.append(cleaned)
    return '\n\n'.join(out_paragraphs)

def main():
    t0 = time.time()
    print(f"Reading {TREC_FILE}...")

    docmap = {}
    offset = 0
    doc_count = 0
    skipped = 0

    with open(TREC_FILE, 'r', encoding='utf-8', errors='replace') as fin, \
         open(STORE_FILE, 'wb') as fout:

        docno = None
        in_text = False
        text_lines = []

        for line in fin:
            if line.startswith('<DOCNO>'):
                # Extract docno — format: <DOCNO>Article_Name</DOCNO>
                m = re.match(r'<DOCNO>(.*?)</DOCNO>', line.strip())
                if m:
                    docno = m.group(1).strip()
                in_text = False
                text_lines = []

            elif line.strip() == '<TEXT>':
                in_text = True
                text_lines = []

            elif line.strip() == '</TEXT>':
                in_text = False

            elif line.strip() == '</DOC>':
                if docno and text_lines:
                    raw = ''.join(text_lines)
                    text = strip_wiki_markup(raw)
                    if text:
                        encoded = (text + '\n').encode('utf-8')
                        length = len(encoded)
                        docmap[docno] = [offset, length]
                        fout.write(encoded)
                        offset += length
                        doc_count += 1
                    else:
                        skipped += 1
                docno = None
                text_lines = []

                if doc_count % 10000 == 0 and doc_count > 0:
                    elapsed = time.time() - t0
                    print(f"  {doc_count:,} docs written ({elapsed:.1f}s)...", flush=True)

            elif in_text:
                text_lines.append(line)

    print(f"Writing docmap ({len(docmap):,} entries)...")
    with open(MAP_FILE, 'w', encoding='utf-8') as f:
        json.dump(docmap, f, separators=(',', ':'))

    elapsed = time.time() - t0
    store_mb = offset / 1024 / 1024
    map_mb = os.path.getsize(MAP_FILE) / 1024 / 1024
    print(f"\nDone in {elapsed:.1f}s")
    print(f"  {doc_count:,} docs written, {skipped} skipped")
    print(f"  docstore: {store_mb:.1f} MB → {STORE_FILE}")
    print(f"  docmap:   {map_mb:.1f} MB → {MAP_FILE}")

if __name__ == '__main__':
    main()
