#!/usr/bin/env python3
"""
build_titles_sidecar.py — Build a FlatStore of canonical Wikipedia
display titles from an existing TREC file.

Produces:
  enwiki_top1m_titles.store — concatenated UTF-8 titles
  enwiki_top1m_titles.map   — JSON: {docno: [offset, length]}

This is a one-shot bootstrap for an existing index built before
PRD-031. Future rebuilds via wiki2trec.py write the titles sidecar
inline (see the convert() function there).

The TREC layout we read:

  <DOC>
  <DOCNO>safe_id</DOCNO>
  <TITLE>Canonical Display Title</TITLE>
  <TEXT>
  ...
  </TEXT>
  </DOC>

PRD-017 added the <TITLE> tag for per-field BM25; PRD-031 now uses
the same tag content as the display source.

Usage:
  python3 build_titles_sidecar.py enwiki_top1m.trec \\
      enwiki_top1m_titles.store enwiki_top1m_titles.map
"""

import argparse
import json
import re
import sys


_RE_DOCNO = re.compile(r'<DOCNO>([^<]+)</DOCNO>')
_RE_TITLE = re.compile(r'<TITLE>([^<]*)</TITLE>')


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('trec_in',   help='Input TREC file')
    parser.add_argument('store_out', help='Output titles.store path')
    parser.add_argument('map_out',   help='Output titles.map path')
    args = parser.parse_args()

    print(f'Reading {args.trec_in}...', flush=True)
    title_map: dict[str, list[int]] = {}
    offset = 0
    n_docs = 0
    n_missing_title = 0
    cur_docno: str | None = None

    with open(args.trec_in, encoding='utf-8') as f, \
         open(args.store_out, 'wb') as store:
        for line in f:
            line = line.rstrip('\n')
            if not line:
                continue
            # The TREC is line-oriented in our pipeline: each DOC
            # starts at a line with <DOC>, then a <DOCNO> line, then
            # a <TITLE> line, then <TEXT>. We don't need a full
            # parser; just remember the docno when we see one and
            # write the next title we see.
            m = _RE_DOCNO.search(line)
            if m:
                cur_docno = m.group(1)
                continue
            m = _RE_TITLE.search(line)
            if m and cur_docno:
                title = m.group(1)
                if not title:
                    n_missing_title += 1
                    cur_docno = None
                    continue
                encoded = title.encode('utf-8')
                title_map[cur_docno] = [offset, len(encoded)]
                store.write(encoded)
                offset += len(encoded)
                n_docs += 1
                cur_docno = None
                if n_docs % 100_000 == 0:
                    print(f'  {n_docs:,} titles written...', flush=True)

    print(f'Writing {args.map_out} ({len(title_map):,} entries)...', flush=True)
    with open(args.map_out, 'w', encoding='utf-8') as f:
        json.dump(title_map, f, separators=(',', ':'))

    size_mb = offset / 1024 / 1024
    print(f'Done — {n_docs:,} titles, {size_mb:.1f} MB store '
          f'({n_missing_title:,} docs without <TITLE>)', flush=True)
    return 0


if __name__ == '__main__':
    sys.exit(main())
