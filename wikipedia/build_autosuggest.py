#!/usr/bin/env python3
"""
Build autosuggest.json from Wikipedia clickstream data.
Filters to articles in our Simple English index, applies blocklist,
outputs a sorted array of [query, count] pairs.
"""
import json, os, re, sys, gzip

HERE = os.path.dirname(os.path.abspath(__file__))

CLICKSTREAM = os.path.join(HERE, 'clickstream-enwiki-2025-01.tsv.gz')
TITLES_FILE = os.path.join(HERE, 'simplewiki_titles.txt')
OUTPUT      = os.path.join(HERE, 'autosuggest.json')

BLOCKLIST = {
    'xxx', 'xnxx', 'onlyfans', 'rule 34', 'pornhub', 'xvideos',
    'redtube', 'youporn', '1337x', 'piratebay', 'the pirate bay',
    'rarbg', 'torrentz',
}
BLOCKLIST_WORDS = {'porn', 'hentai', 'cum', 'pussy', 'cock', 'penis', 'vagina', 'nude', 'naked'}

SKIP_PREFIXES = (
    'List_of','Lists_of','Index_of','Outline_of','History_of',
    'Geography_of','Demographics_of','Wikipedia:','File:',
    'Template:','Category:','Help:','Portal:','Talk:','User:','Main_Page',
)
SKIP_RE = re.compile(
    r'^(\d{4}_|\d+_|\$|.*_discography$|.*_filmography$|.*_bibliography$)',
    re.IGNORECASE
)

def is_blocked(query):
    if query in BLOCKLIST:
        return True
    words = set(query.split())
    if words & BLOCKLIST_WORDS:
        return True
    return False

def title_to_query(article):
    """Convert Wikipedia article title to a search query string."""
    # Strip disambiguation: Foo_(bar) -> Foo
    q = re.sub(r'_\(.*?\)', '', article)
    # Underscores to spaces, strip, lowercase
    q = q.replace('_', ' ').strip().lower()
    return q

def main():
    # Load simplewiki title set
    print("Loading simplewiki titles...", flush=True)
    with open(TITLES_FILE) as f:
        simplewiki = set(l.strip() for l in f if l.strip())
    print(f"  {len(simplewiki):,} titles", flush=True)

    # Stream clickstream
    print("Processing clickstream...", flush=True)
    results = {}
    rows = 0
    matched = 0
    blocked = 0
    skipped = 0

    with gzip.open(CLICKSTREAM, 'rt', encoding='utf-8', errors='replace') as f:
        for line in f:
            parts = line.rstrip('\n').split('\t')
            if len(parts) != 4:
                continue
            referrer, article, typ, count_str = parts
            if referrer != 'other-search':
                continue
            rows += 1

            try:
                count = int(count_str)
            except ValueError:
                continue

            if count < 10:
                continue

            # Must be in our index
            if article not in simplewiki:
                continue
            matched += 1

            # Skip list/index/meta articles
            if any(article.startswith(p) for p in SKIP_PREFIXES):
                skipped += 1
                continue
            if SKIP_RE.match(article):
                skipped += 1
                continue
            if not re.search(r'[a-zA-Z]', article):
                skipped += 1
                continue

            query = title_to_query(article)
            words = query.split()
            if not (1 <= len(words) <= 6):
                skipped += 1
                continue

            # Blocklist
            if is_blocked(query):
                blocked += 1
                continue

            results[query] = results.get(query, 0) + count

            if rows % 500000 == 0:
                print(f"  {rows:,} rows processed, {matched:,} matched...", flush=True)

    print(f"\nDone: {rows:,} rows | {matched:,} in simplewiki | "
          f"{skipped:,} skipped | {blocked:,} blocked | "
          f"{len(results):,} unique queries", flush=True)

    # Sort alphabetically (for binary search at query time)
    sorted_pairs = sorted(results.items(), key=lambda x: x[0])

    print(f"Writing {OUTPUT}...", flush=True)
    with open(OUTPUT, 'w', encoding='utf-8') as f:
        json.dump(sorted_pairs, f, ensure_ascii=False)

    size_mb = os.path.getsize(OUTPUT) / 1024 / 1024
    print(f"Done — {len(sorted_pairs):,} entries, {size_mb:.1f} MB", flush=True)

    # Show top 20 by count
    print("\nTop 20 by click count:")
    for q, c in sorted(results.items(), key=lambda x: -x[1])[:20]:
        print(f"  {c:>10,}  {q}")

if __name__ == '__main__':
    main()
