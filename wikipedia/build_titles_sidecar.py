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


_RE_DOCNO         = re.compile(r'<DOCNO>([^<]+)</DOCNO>')
_RE_TITLE         = re.compile(r'<TITLE>([^<]*)</TITLE>')
# PRD-031 followup: TRECs emitted by wiki2trec post-disambig-strip
# carry the full canonical title in a sibling <DISPLAY_TITLE> tag.
# When present, prefer that; the <TITLE> tag has been stripped for
# indexing. Older TRECs lack the tag; fall back to <TITLE> which is
# still the unstripped form for them.
_RE_DISPLAY_TITLE = re.compile(r'<DISPLAY_TITLE>([^<]*)</DISPLAY_TITLE>')


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
    n_via_display = 0
    n_via_title = 0
    cur_docno: str | None = None
    pending_index_title: str | None = None
    pending_display_title: str | None = None

    def _commit() -> tuple[int, int, str | None]:
        """Write the best-available title for the pending docno.
        Returns (new_offset_delta, via_display_increment, error_or_None)."""
        nonlocal offset
        chosen = pending_display_title or pending_index_title
        if not chosen:
            return 0, 0, 'missing'
        encoded = chosen.encode('utf-8')
        title_map[cur_docno] = [offset, len(encoded)]
        store.write(encoded)
        offset += len(encoded)
        via_display = 1 if pending_display_title else 0
        return 1, via_display, None

    with open(args.trec_in, encoding='utf-8') as f, \
         open(args.store_out, 'wb') as store:
        for line in f:
            line = line.rstrip('\n')
            if not line:
                continue
            # The TREC is line-oriented: <DOC>, then <DOCNO>, then
            # <TITLE>, optionally <DISPLAY_TITLE>, then <TEXT>. We
            # commit on the next <DOCNO> or on </DOC>; prefer
            # <DISPLAY_TITLE> for the body content when both seen.
            m = _RE_DOCNO.search(line)
            if m:
                # Flush the previous doc, if any.
                if cur_docno is not None:
                    n, via_disp, err = _commit()
                    if err == 'missing':
                        n_missing_title += 1
                    else:
                        n_docs += n
                        n_via_display += via_disp
                        n_via_title   += (n - via_disp)
                        if n_docs % 100_000 == 0:
                            print(f'  {n_docs:,} titles written '
                                  f'({n_via_display:,} via DISPLAY_TITLE)...',
                                  flush=True)
                cur_docno = m.group(1)
                pending_index_title = None
                pending_display_title = None
                continue
            m = _RE_DISPLAY_TITLE.search(line)
            if m and cur_docno:
                pending_display_title = m.group(1) or None
                continue
            m = _RE_TITLE.search(line)
            if m and cur_docno:
                pending_index_title = m.group(1) or None
                continue
            # Otherwise skip — TEXT lines etc.
        # End-of-file: flush the last doc.
        if cur_docno is not None:
            n, via_disp, err = _commit()
            if err == 'missing':
                n_missing_title += 1
            else:
                n_docs += n
                n_via_display += via_disp
                n_via_title   += (n - via_disp)

    print(f'Writing {args.map_out} ({len(title_map):,} entries)...', flush=True)
    with open(args.map_out, 'w', encoding='utf-8') as f:
        json.dump(title_map, f, separators=(',', ':'))

    size_mb = offset / 1024 / 1024
    print(f'Done — {n_docs:,} titles ({n_via_display:,} via DISPLAY_TITLE, '
          f'{n_via_title:,} via TITLE), '
          f'{size_mb:.1f} MB store '
          f'({n_missing_title:,} docs without any title tag)', flush=True)
    return 0


if __name__ == '__main__':
    sys.exit(main())
