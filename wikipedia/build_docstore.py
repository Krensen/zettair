#!/usr/bin/env python3
"""
build_docstore.py — build a random-access document store from simplewiki.trec

Produces:
  simplewiki.docstore   — raw concatenated plain text (UTF-8)
  simplewiki.docmap     — JSON: {docno: [offset, length], ...}

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
TREC_FILE  = os.path.join(SCRIPT_DIR, 'simplewiki.trec')
STORE_FILE = os.path.join(SCRIPT_DIR, 'simplewiki.docstore')
MAP_FILE   = os.path.join(SCRIPT_DIR, 'simplewiki.docmap')

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


def strip_wiki_markup(text: str) -> str:
    """
    Clean text that has already been through wiki2trec.py's clean() pass.
    Strategy: split into sentences, drop citation sentences, rejoin.
    This works on the flat single-line-per-article format wiki2trec produces.
    """
    # Strip bullet markers
    text = _RE_BULLET.sub('', text)

    # Remove raw ISBN blocks (the keyword + number, no surrounding sentence needed)
    text = _RE_ISBN.sub('', text)

    # Split on sentence boundaries: period/!/? followed by space + capital letter.
    # Use re.split with a capturing group so we keep the delimiter characters.
    parts = re.split(r'([.!?])\s+(?=[A-Z(])', text)

    # re.split with a group interleaves [text, delim, text, delim, ...]
    # Reassemble into sentences first.
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

    # Filter out citation sentences
    prose = [s for s in sentences if not _is_citation_sentence(s)]

    text = ' '.join(prose)
    text = re.sub(r' {2,}', ' ', text)
    return text.strip()

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
