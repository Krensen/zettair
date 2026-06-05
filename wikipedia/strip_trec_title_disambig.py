#!/usr/bin/env python3
"""
strip_trec_title_disambig.py — Rewrite an existing TREC file in place
so each <TITLE> tag has its Wikipedia "(disambiguator)" suffix
removed AND a sibling <DISPLAY_TITLE> tag is inserted carrying the
full unstripped title.

Use when the TREC was produced by a wiki2trec.py predating the
PRD-031 followup that strips disambiguators from indexed titles
(and the followup that introduced <DISPLAY_TITLE>). Re-running
wiki2trec from the bz2 (~6h) is too expensive; this script rewrites
just the title tags. <DOCNO>, <TEXT>, and all other content are
untouched.

After rewriting:
  1. Rebuild the display titles sidecar via build_titles_sidecar.py
     (which now prefers <DISPLAY_TITLE> when present so the
     disambiguator survives in the sidecar).
  2. Reindex with `zet -i` (~10 min on 1.5M corpus) so the new
     stripped <TITLE> tokens land in the postings.

Usage:
  python3 strip_trec_title_disambig.py enwiki_top1m.trec

The file is rewritten atomically via a temp file + os.replace. Safe
to re-run: a line that has already been rewritten to its stripped
form does not match the disambiguator regex and is left alone, and
existing <DISPLAY_TITLE> tags are not duplicated.
"""

import argparse
import os
import re
import sys

# Match the full <TITLE>...</TITLE> tag and rewrite the inner content.
_TITLE_TAG_RE        = re.compile(r'<TITLE>([^<]*)</TITLE>')
_DISPLAY_TITLE_TAG_RE = re.compile(r'<DISPLAY_TITLE>([^<]*)</DISPLAY_TITLE>')
_DISAMB_PAREN_RE     = re.compile(r'\s*\([^()]+\)\s*$')


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('trec_in', help='TREC file to rewrite in place')
    args = p.parse_args()

    if not os.path.exists(args.trec_in):
        print(f"ERROR: {args.trec_in} not found", file=sys.stderr)
        return 1

    tmp = args.trec_in + '.tmp'
    n_lines = 0
    n_stripped = 0
    n_already_have_display = 0
    n_display_added = 0
    pending_display_title: str | None = None
    with open(args.trec_in, encoding='utf-8') as src, \
         open(tmp, 'w', encoding='utf-8') as dst:
        for line in src:
            n_lines += 1
            # If the previous line was a <TITLE> we deferred emitting,
            # decide here whether the current line is an existing
            # <DISPLAY_TITLE> (skip the add) or anything else (emit
            # the deferred DISPLAY_TITLE first).
            if pending_display_title is not None:
                if _DISPLAY_TITLE_TAG_RE.search(line):
                    # Existing display title — leave as-is.
                    n_already_have_display += 1
                else:
                    dst.write(f'<DISPLAY_TITLE>{pending_display_title}</DISPLAY_TITLE>\n')
                    n_display_added += 1
                pending_display_title = None
            m = _TITLE_TAG_RE.search(line)
            if not m:
                dst.write(line)
                continue
            full = m.group(1)
            stripped = _DISAMB_PAREN_RE.sub('', full).strip()
            if stripped != full:
                new_tag = f'<TITLE>{stripped}</TITLE>'
                line = line[:m.start()] + new_tag + line[m.end():]
                n_stripped += 1
            dst.write(line)
            # Defer the DISPLAY_TITLE emission to the next iteration so
            # we can peek and avoid duplicating a pre-existing one.
            pending_display_title = full
            if n_lines % 1_000_000 == 0:
                print(f'  {n_lines:,} lines processed, '
                      f'{n_stripped:,} titles stripped, '
                      f'{n_display_added:,} DISPLAY_TITLE added', flush=True)
        # End of file: flush any pending DISPLAY_TITLE.
        if pending_display_title is not None:
            dst.write(f'<DISPLAY_TITLE>{pending_display_title}</DISPLAY_TITLE>\n')
            n_display_added += 1
    os.replace(tmp, args.trec_in)
    print(f'Done — {n_lines:,} lines processed.', flush=True)
    print(f'  titles stripped:        {n_stripped:,}', flush=True)
    print(f'  DISPLAY_TITLE added:    {n_display_added:,}', flush=True)
    print(f'  DISPLAY_TITLE pre-existing (left alone): {n_already_have_display:,}',
          flush=True)
    print(f'Next: rebuild titles sidecar with build_titles_sidecar.py, '
          f'then re-run zet -i.', flush=True)
    return 0


if __name__ == '__main__':
    sys.exit(main())
