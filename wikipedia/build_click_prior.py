#!/usr/bin/env python3
"""
build_click_prior.py — Aggregate clickstream → click_prior.bin

Reads all clickstream files, applies monthly decay, outputs a binary
float32 array indexed by Zettair internal docno.

Output: click_prior.bin  (256,523 × 4 bytes = ~1MB)
"""
import gzip, json, os, re, struct, sys, time

HERE       = os.path.dirname(os.path.abspath(__file__))
DOCNO_MAP  = os.path.join(HERE, 'docno_map.tsv')
OUTPUT     = os.path.join(HERE, 'click_prior.bin')
LOG_DIR    = os.path.join(HERE, '../../zettair-service/logs')
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

def load_docno_map():
    """Returns title -> internal_docno dict, and total doc count."""
    title_to_id = {}
    max_id = 0
    with open(DOCNO_MAP, encoding='utf-8') as f:
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

    # Load docno map
    print("Loading docno map...", flush=True)
    title_to_id, num_docs = load_docno_map()
    log('docno_map_loaded', docs=num_docs)

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
    print(f"\nWriting {OUTPUT}...", flush=True)
    with open(OUTPUT, 'wb') as f:
        f.write(struct.pack(f'{num_docs}f', *scores))

    size = os.path.getsize(OUTPUT)
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
