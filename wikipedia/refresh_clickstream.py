#!/usr/bin/env python3
"""
refresh_clickstream.py — Auto-refresh clickstream data and rebuild autosuggest.json

Schedule: run daily from the 10th of each month.
- Checks if the latest expected clickstream file exists on Wikimedia
- If not found: logs and exits (cron will retry tomorrow)
- If found: downloads, aggregates all months with decay, rebuilds autosuggest.json
- Updates state file so we know what we have and when to next check

Decay formula: score = Σ clicks(month) × DECAY_RATE^(months_ago)
  DECAY_RATE=0.85 → last month=100%, 6mo=38%, 12mo=14%

Log: logs/clickstream_refresh.jsonl — one JSON line per event
"""

import datetime
import gzip
import json
import os
import re
import sys
import time
import urllib.request

# ── Paths ──────────────────────────────────────────────────────────────────
HERE         = os.path.dirname(os.path.abspath(__file__))
SERVICE_DIR  = os.path.join(HERE, '../../zettair-service')
LOG_DIR      = os.path.join(SERVICE_DIR, 'logs')
LOG_FILE     = os.path.join(LOG_DIR, 'clickstream_refresh.jsonl')
STATE_FILE   = os.path.join(HERE, 'clickstream_state.json')
TITLES_FILE  = os.path.join(HERE, 'simplewiki_titles.txt')
OUTPUT       = os.path.join(HERE, 'autosuggest.json')

WIKIMEDIA_BASE = 'https://dumps.wikimedia.org/other/clickstream'

# ── Config ─────────────────────────────────────────────────────────────────
DECAY_RATE = 0.85   # per month

SKIP_PREFIXES = (
    'List_of','Lists_of','Index_of','Outline_of','History_of',
    'Geography_of','Demographics_of','Wikipedia:','File:',
    'Template:','Category:','Help:','Portal:','Talk:','User:','Main_Page',
)
SKIP_RE = re.compile(
    r'^(\d{4}_|\d+_|\$|.*_discography$|.*_filmography$|.*_bibliography$)',
    re.IGNORECASE
)
BLOCKLIST = {
    'xxx','xnxx','onlyfans','rule 34','pornhub','xvideos',
    'redtube','youporn','1337x','piratebay','the pirate bay','rarbg','torrentz',
}
BLOCKLIST_WORDS = {'porn','hentai','cum','pussy','cock','nude','naked'}

# ── Logging ────────────────────────────────────────────────────────────────
def ts():
    return datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

def log(action: str, **kwargs):
    os.makedirs(LOG_DIR, exist_ok=True)
    record = {'ts': ts(), 'action': action, **kwargs}
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(json.dumps(record, ensure_ascii=False) + '\n')
    print(f"[{record['ts']}] {action}", ' '.join(f'{k}={v}' for k, v in kwargs.items()), flush=True)

# ── State ──────────────────────────────────────────────────────────────────
def load_state() -> dict:
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {'months_downloaded': []}

def save_state(state: dict):
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)

# ── Month arithmetic ────────────────────────────────────────────────────────
def next_month_str(ym: str) -> str:
    """'2025-01' -> '2025-02', '2025-12' -> '2026-01'"""
    y, m = int(ym[:4]), int(ym[5:])
    m += 1
    if m > 12:
        m, y = 1, y + 1
    return f'{y}-{m:02d}'

def months_ago(ym: str, reference: str) -> int:
    """How many months before reference is ym?"""
    y1, m1 = int(ym[:4]), int(ym[5:])
    y2, m2 = int(reference[:4]), int(reference[5:])
    return (y2 - y1) * 12 + (m2 - m1)

def all_available_months() -> list:
    """Return sorted list of YYYY-MM for all clickstream files on disk."""
    months = []
    for fname in os.listdir(HERE):
        m = re.match(r'clickstream-enwiki-(\d{4}-\d{2})\.tsv\.gz', fname)
        if m:
            months.append(m.group(1))
    return sorted(months)

# ── Check + download ────────────────────────────────────────────────────────
def file_exists_on_wikimedia(month: str) -> bool:
    url = f'{WIKIMEDIA_BASE}/{month}/clickstream-enwiki-{month}.tsv.gz'
    try:
        req = urllib.request.Request(url, method='HEAD')
        with urllib.request.urlopen(req, timeout=15) as r:
            return r.status == 200
    except Exception as e:
        log('check_error', month=month, error=str(e))
        return False

def download_month(month: str) -> bool:
    url = f'{WIKIMEDIA_BASE}/{month}/clickstream-enwiki-{month}.tsv.gz'
    dest = os.path.join(HERE, f'clickstream-enwiki-{month}.tsv.gz')
    log('download_start', month=month, url=url)
    t0 = time.time()
    try:
        urllib.request.urlretrieve(url, dest)
        size = os.path.getsize(dest)
        took = round(time.time() - t0, 1)
        log('download_done', month=month, bytes=size, took_s=took)
        return True
    except Exception as e:
        log('download_error', month=month, error=str(e))
        if os.path.exists(dest):
            os.remove(dest)
        return False

# ── Build autosuggest ───────────────────────────────────────────────────────
def title_to_query(article: str) -> str:
    q = re.sub(r'_\(.*?\)', '', article).replace('_', ' ').strip().lower()
    return q

def is_blocked(query: str) -> bool:
    if query in BLOCKLIST:
        return True
    return bool(set(query.split()) & BLOCKLIST_WORDS)

def build_autosuggest(months: list):
    """Aggregate all months with decay, write autosuggest.json."""
    log('rebuild_start', months=len(months))
    t0 = time.time()

    # Load simplewiki titles
    with open(TITLES_FILE) as f:
        simplewiki = set(l.strip() for l in f if l.strip())
    log('titles_loaded', count=len(simplewiki))

    # Reference = most recent month
    reference = sorted(months)[-1]
    scores: dict = {}  # query -> float score

    for month in sorted(months):
        fpath = os.path.join(HERE, f'clickstream-enwiki-{month}.tsv.gz')
        if not os.path.exists(fpath):
            log('skip_missing', month=month)
            continue

        age = months_ago(month, reference)
        weight = DECAY_RATE ** age
        log('processing', month=month, age_months=age, weight=round(weight, 4))

        rows = 0
        with gzip.open(fpath, 'rt', encoding='utf-8', errors='replace') as f:
            for line in f:
                parts = line.rstrip('\n').split('\t')
                if len(parts) != 4:
                    continue
                referrer, article, _, count_str = parts
                if referrer != 'other-search':
                    continue
                try:
                    count = int(count_str)
                except ValueError:
                    continue
                if count < 10:
                    continue
                if article not in simplewiki:
                    continue
                if any(article.startswith(p) for p in SKIP_PREFIXES):
                    continue
                if SKIP_RE.match(article):
                    continue
                if not re.search(r'[a-zA-Z]', article):
                    continue

                query = title_to_query(article)
                words = query.split()
                if not (1 <= len(words) <= 6):
                    continue
                if is_blocked(query):
                    continue

                scores[query] = scores.get(query, 0.0) + count * weight
                rows += 1

        log('month_done', month=month, rows=rows)

    # Sort alphabetically for binary search
    sorted_pairs = sorted(
        [(q, round(s)) for q, s in scores.items()],
        key=lambda x: x[0]
    )

    with open(OUTPUT, 'w', encoding='utf-8') as f:
        json.dump(sorted_pairs, f, ensure_ascii=False)

    size_mb = os.path.getsize(OUTPUT) / 1024 / 1024
    took = round(time.time() - t0, 1)
    log('rebuild_done',
        entries=len(sorted_pairs),
        size_mb=round(size_mb, 1),
        took_s=took)

    # Show top 10
    top10 = sorted(scores.items(), key=lambda x: -x[1])[:10]
    for q, s in top10:
        print(f'  {round(s):>12,}  {q}')

# ── Main ────────────────────────────────────────────────────────────────────
def main():
    state = load_state()
    months_on_disk = all_available_months()

    if not months_on_disk:
        log('error', msg='No clickstream files found on disk')
        sys.exit(1)

    latest_on_disk = sorted(months_on_disk)[-1]
    next_month = next_month_str(latest_on_disk)

    log('check', latest_on_disk=latest_on_disk, checking_for=next_month)

    # Check if next month's file exists locally already
    next_file = os.path.join(HERE, f'clickstream-enwiki-{next_month}.tsv.gz')
    if os.path.exists(next_file):
        log('already_have', month=next_month)
        # Still rebuild if autosuggest.json is stale
        if state.get('last_built') != latest_on_disk:
            build_autosuggest(all_available_months())
            state['last_built'] = latest_on_disk
            save_state(state)
        return

    # Check Wikimedia
    if not file_exists_on_wikimedia(next_month):
        log('not_available_yet', month=next_month)
        print(f'  {next_month} not yet published — will retry tomorrow')
        return

    # Download it
    log('found', month=next_month)
    ok = download_month(next_month)
    if not ok:
        sys.exit(1)

    # Rebuild autosuggest with all months
    build_autosuggest(all_available_months())

    state['last_built'] = next_month
    save_state(state)
    log('complete', month=next_month)

if __name__ == '__main__':
    main()
