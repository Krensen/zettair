#!/usr/bin/env python3
"""
build_docno_map.py — DEPRECATED. zet now emits <index>.docno_map.tsv at
index-build time, with the docids assigned by zet's own parser. Use
that file directly (e.g. via build_click_prior.py --index <path>) instead
of running this script — it just re-parses the TREC and risks drifting
from zet's own assignment.

Kept temporarily for compatibility with old setup paths. Will be removed
once nothing references it.

Original docs:

Extract title → internal docno mapping from a TREC file.

Zettair assigns docnos sequentially in the order documents appear in the TREC
file. So docno 0 = first <DOCNO> tag, docno 1 = second, etc.

Usage:
  python3 build_docno_map.py [input.trec]   (default: enwiki.trec)

Output: docno_map.tsv  (two columns: internal_docno\ttitle)
"""
import os, re, sys

HERE  = os.path.dirname(os.path.abspath(__file__))
TREC  = sys.argv[1] if len(sys.argv) > 1 else os.path.join(HERE, 'enwiki.trec')
OUT   = os.path.join(HERE, 'docno_map.tsv')

DOCNO_RE = re.compile(r'<DOCNO>(.*?)</DOCNO>')

def main():
    print(f"Reading {TREC}...", flush=True)
    docno = 0
    with open(TREC, encoding='utf-8', errors='replace') as fin, \
         open(OUT, 'w', encoding='utf-8') as fout:
        for line in fin:
            m = DOCNO_RE.search(line)
            if m:
                title = m.group(1).strip()
                fout.write(f'{docno}\t{title}\n')
                docno += 1
                if docno % 50000 == 0:
                    print(f'  {docno:,} docs...', flush=True)

    print(f'Done — {docno:,} docs written to {OUT}')

if __name__ == '__main__':
    main()
