#!/usr/bin/env python3
"""
build_click_prior.py — Aggregate clickstream → click_prior.bin

Reads all clickstream files, applies monthly decay, outputs a binary
float32 array indexed by Zettair internal docid.

Source of truth for docno→docid: <index>.docno_map.tsv emitted directly
by zet at index time (no separate Python TREC parse). Pass --index
<path> to point at the index. Falls back to ./docno_map.tsv (the legacy
build_docno_map.py output) if --index is not given AND the legacy file
exists; the legacy path is deprecated and will be removed.

Output: <index>.click_prior.bin (default) or --out PATH.
"""
import argparse, gzip, json, os, re, struct, sys, time

HERE       = os.path.dirname(os.path.abspath(__file__))
LOG_DIR    = os.environ.get('CLICKSTREAM_LOG_DIR', os.path.join(HERE, 'logs'))
LOG_FILE   = os.path.join(LOG_DIR, 'clickstream_refresh.jsonl')

DECAY_RATE = 0.85   # per month

def ts():
    import datetime
    return datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

def log(action, **kwargs):
    os.makedirs(LOG_DIR, exist_ok=True)
    record = {'ts': ts(), 'action': action, **kwargs}
    with open(LOG_FILE, 'a') as f:
        f.write(json.dumps(record) + '\n')
    print(f"[{record['ts']}] {action}", ' '.join(f'{k}={v}' for k, v in kwargs.items()), flush=True)

def months_ago(ym, reference):
    y1, m1 = int(ym[:4]), int(ym[5:])
    y2, m2 = int(reference[:4]), int(reference[5:])
    return (y2 - y1) * 12 + (m2 - m1)

def load_docno_map(path):
    """Returns title -> internal_docno dict, and total doc count."""
    title_to_id = {}
    max_id = 0
    with open(path, encoding='utf-8') as f:
        for line in f:
            parts = line.rstrip('\n').split('\t')
            if len(parts) != 2:
                continue
            internal_id, title = int(parts[0]), parts[1]
            title_to_id[title] = internal_id
            if internal_id > max_id:
                max_id = internal_id
    return title_to_id, max_id + 1

def all_clickstream_months():
    """Return sorted list of YYYY-MM for all clickstream files on disk."""
    months = []
    for fname in os.listdir(HERE):
        m = re.match(r'clickstream-enwiki-(\d{4}-\d{2})\.tsv\.gz', fname)
        if m:
            months.append(m.group(1))
    return sorted(months)

def main():
    t_start = time.time()

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--index', help='index path (e.g. /path/wikiindex/index). '
                        'Reads <index>.docno_map.tsv emitted by zet at build time. '
                        'Writes <index>.click_prior.bin unless --out is given.')
    parser.add_argument('--docno-map', help='explicit docno_map.tsv path (overrides --index)')
    parser.add_argument('--out', help='output click_prior.bin path')
    args = parser.parse_args()

    if args.docno_map:
        docno_map_path = args.docno_map
    elif args.index:
        docno_map_path = args.index + '.docno_map.tsv'
    else:
        # Legacy fallback: ./docno_map.tsv from build_docno_map.py.
        legacy = os.path.join(HERE, 'docno_map.tsv')
        if os.path.exists(legacy):
            print(f"WARNING: using legacy {legacy} — pass --index <path> to use the "
                  "indexer-emitted docno_map and avoid drift.", file=sys.stderr)
            docno_map_path = legacy
        else:
            print("ERROR: must pass --index <path> or --docno-map <file>", file=sys.stderr)
            sys.exit(1)

    if args.out:
        output_path = args.out
    elif args.index:
        output_path = args.index + '.click_prior.bin'
    else:
        output_path = os.path.join(HERE, 'click_prior.bin')

    # Load docno map
    print(f"Loading docno map from {docno_map_path}...", flush=True)
    title_to_id, num_docs = load_docno_map(docno_map_path)
    log('docno_map_loaded', docs=num_docs, source=docno_map_path)

    # Accumulate scores per internal docno
    scores = [0.0] * num_docs

    months = all_clickstream_months()
    if not months:
        print("ERROR: No clickstream files found", file=sys.stderr)
        sys.exit(1)

    reference = months[-1]
    log('aggregating', months=len(months), reference=reference, decay=DECAY_RATE)

    for month in months:
        fpath = os.path.join(HERE, f'clickstream-enwiki-{month}.tsv.gz')
        age   = months_ago(month, reference)
        weight = DECAY_RATE ** age
        rows = matched = 0

        print(f"  {month}  age={age}mo  weight={weight:.4f}", flush=True)

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
                if count < 5:
                    continue
                internal_id = title_to_id.get(article)
                if internal_id is None:
                    continue
                scores[internal_id] += count * weight
                matched += 1

        log('month_done', month=month, rows=rows, matched=matched, weight=round(weight, 4))

    # Write binary float32 array
    nonzero = sum(1 for s in scores if s > 0)
    print(f"\nWriting {output_path}...", flush=True)
    with open(output_path, 'wb') as f:
        f.write(struct.pack(f'{num_docs}f', *scores))

    size = os.path.getsize(output_path)
    took = round(time.time() - t_start, 1)
    log('click_prior_built',
        docs=num_docs,
        nonzero=nonzero,
        coverage_pct=round(100 * nonzero / num_docs, 1),
        size_bytes=size,
        took_s=took)

    # Spot check
    print("\nTop 20 articles by decayed click score:")
    top = sorted(enumerate(scores), key=lambda x: -x[1])[:20]
    id_to_title = {v: k for k, v in title_to_id.items()}
    for internal_id, score in top:
        print(f"  {round(score):>12,}  {id_to_title.get(internal_id, '?')}")

if __name__ == '__main__':
    main()
