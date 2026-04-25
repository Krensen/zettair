#!/usr/bin/env python3
"""
select_top_articles.py — Select top N Wikipedia articles by decayed click score.

Reads all clickstream-enwiki-*.tsv.gz files, applies monthly decay
(DECAY_RATE=0.85, same as build_click_prior.py), aggregates scores per
article, and writes the top N titles to a file in dbkey form.

Output: top_titles.txt  (one title per line, underscores, descending score order)

Usage:
  python3 select_top_articles.py
  python3 select_top_articles.py --top 500000 --out my_titles.txt
"""

import argparse
import gzip
import os
import re
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))

DECAY_RATE = 0.85


def all_clickstream_months() -> list[str]:
    months = []
    for fname in os.listdir(HERE):
        m = re.match(r'clickstream-enwiki-(\d{4}-\d{2})\.tsv\.gz', fname)
        if m:
            months.append(m.group(1))
    return sorted(months)


def months_ago(ym: str, reference: str) -> int:
    y1, m1 = int(ym[:4]), int(ym[5:])
    y2, m2 = int(reference[:4]), int(reference[5:])
    return (y2 - y1) * 12 + (m2 - m1)


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--top', type=int, default=1_000_000,
                        help='Number of top articles to select [default: 1000000]')
    parser.add_argument('--out', default=None,
                        help='Output path [default: top_titles.txt next to this script]')
    args = parser.parse_args()

    out_path = args.out or os.path.join(HERE, 'top_titles.txt')

    months = all_clickstream_months()
    if not months:
        print('ERROR: no clickstream-enwiki-*.tsv.gz files found', file=sys.stderr)
        sys.exit(1)

    reference = months[-1]
    print(f'Found {len(months)} clickstream months, reference={reference}', flush=True)

    scores: dict[str, float] = {}
    t0 = time.time()

    for month in months:
        fpath = os.path.join(HERE, f'clickstream-enwiki-{month}.tsv.gz')
        age = months_ago(month, reference)
        weight = DECAY_RATE ** age
        rows = matched = 0

        print(f'  {month}  age={age}mo  weight={weight:.4f}', flush=True)

        with gzip.open(fpath, 'rt', encoding='utf-8', errors='replace') as f:
            for line in f:
                parts = line.rstrip('\n').split('\t')
                if len(parts) != 4:
                    continue
                referrer, article, _, count_str = parts
                if referrer != 'other-search':
                    continue
                rows += 1
                try:
                    count = int(count_str)
                except ValueError:
                    continue
                if count < 1:
                    continue
                scores[article] = scores.get(article, 0.0) + count * weight
                matched += 1

        print(f'    {rows:,} search rows, {matched:,} matched', flush=True)

    print(f'\nTotal unique articles scored: {len(scores):,}', flush=True)

    # Sort by descending score, take top N
    ranked = sorted(scores.items(), key=lambda x: -x[1])
    top = ranked[:args.top]

    print(f'Writing top {len(top):,} titles to {out_path}...', flush=True)
    with open(out_path, 'w', encoding='utf-8') as f:
        for title, _ in top:
            f.write(title + '\n')

    elapsed = time.time() - t0
    print(f'Done in {elapsed:.1f}s', flush=True)

    # Spot-check: show top 20
    print('\nTop 20 articles by decayed click score:')
    for title, score in top[:20]:
        print(f'  {round(score):>12,}  {title}')

    # Show the article at the cutoff
    if len(top) >= args.top:
        cutoff_title, cutoff_score = top[args.top - 1]
        print(f'\nCutoff (rank {args.top:,}): {cutoff_title} ({round(cutoff_score):,} decayed clicks)')


if __name__ == '__main__':
    main()
