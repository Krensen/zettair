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

def strip_wiki_markup(text):
    """Light cleanup — the TREC file is already mostly plain text from wiki2trec.py."""
    # Remove any residual wiki templates {{...}}
    text = re.sub(r'\{\{[^}]*\}\}', ' ', text)
    # Remove image/file links [[File:...]] [[Image:...]]
    text = re.sub(r'\[\[(File|Image):[^\]]*\]\]', ' ', text, flags=re.IGNORECASE)
    # Remove remaining [[link|text]] → text, [[link]] → link
    text = re.sub(r'\[\[(?:[^|\]]*\|)?([^\]]*)\]\]', r'\1', text)
    # Remove bare URLs
    text = re.sub(r'https?://\S+', ' ', text)
    # Collapse multiple spaces/newlines
    text = re.sub(r' {2,}', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
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
