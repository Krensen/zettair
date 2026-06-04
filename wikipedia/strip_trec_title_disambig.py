#!/usr/bin/env python3
"""
strip_trec_title_disambig.py — Rewrite an existing TREC file in place
so each <TITLE> tag has its Wikipedia "(disambiguator)" suffix removed.

Use when the TREC was produced by a wiki2trec.py predating the
PRD-031 followup that strips disambiguators from indexed titles, and
re-running wiki2trec from the bz2 (~6h) is too expensive. This
script rewrites just the <TITLE> tag content; <DOCNO>, <TEXT>, and
all other content are untouched.

After rewriting, the zettair index must be rebuilt (`zet -i`) to pick
up the new title-field tokens. ~10 minutes on a 1.5M corpus.

Usage:
  python3 strip_trec_title_disambig.py enwiki_top1m.trec

The file is rewritten atomically via a temp file + os.replace.
"""

import argparse
import os
import re
import sys

# Match a full <TITLE>...</TITLE> tag and rewrite the inner content.
# Same shape as the regex in wiki2trec.py.
_TITLE_TAG_RE   = re.compile(r'<TITLE>([^<]*)</TITLE>')
_DISAMB_PAREN_RE = re.compile(r'\s*\([^()]+\)\s*$')


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
    n_changed = 0
    with open(args.trec_in, encoding='utf-8') as src, \
         open(tmp, 'w', encoding='utf-8') as dst:
        for line in src:
            n_lines += 1
            m = _TITLE_TAG_RE.search(line)
            if m:
                full = m.group(1)
                stripped = _DISAMB_PAREN_RE.sub('', full).strip()
                if stripped != full:
                    new_tag = f'<TITLE>{stripped}</TITLE>'
                    line = line[:m.start()] + new_tag + line[m.end():]
                    n_changed += 1
            dst.write(line)
            if n_lines % 1_000_000 == 0:
                print(f'  {n_lines:,} lines processed, '
                      f'{n_changed:,} titles changed', flush=True)
    os.replace(tmp, args.trec_in)
    print(f'Done — {n_lines:,} lines, {n_changed:,} titles changed.', flush=True)
    print(f'Now reindex with: zet -i ...', flush=True)
    return 0


if __name__ == '__main__':
    sys.exit(main())
