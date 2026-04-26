#!/usr/bin/env python3
"""
build_dbkey_map.py — Generate a safe_id → dbkey TSV from an existing index.

Walks the docno_map.tsv (column 2 = safe_id docnos used by Zettair) and the
allowlist top_titles.txt (dbkey form), and emits the inverse mapping for
every entry where safe_id and dbkey differ.

Use this once after the index is built. Subsequent rebuilds via wiki2trec.py
should write the dbkeys file directly.

Output format (TSV, sorted by safe_id):
    {safe_id}\t{dbkey}

Usage:
  python3 build_dbkey_map.py top_titles.txt docno_map.tsv enwiki_top1m.dbkeys.tsv
"""
import argparse
import re
import sys


def safe_id(title: str) -> str:
    """Mirror of wiki2trec.py:safe_id() — same regex, same length cap."""
    return re.sub(r'[^\w\-]', '_', title)[:80]


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('titles_file',
                        help='Allowlist with one dbkey per line (e.g. top_titles.txt)')
    parser.add_argument('docno_map',
                        help='Docno map TSV (internal_id\\ttitle, where title is the safe_id)')
    parser.add_argument('output',
                        help='Output TSV path (safe_id\\tdbkey, only differing entries)')
    args = parser.parse_args()

    print(f'Reading allowlist {args.titles_file}...', flush=True)
    safe_to_dbkey: dict[str, str] = {}
    collisions = 0
    with open(args.titles_file, encoding='utf-8') as f:
        for line in f:
            dbkey = line.rstrip('\n')
            if not dbkey:
                continue
            sid = safe_id(dbkey)
            if sid in safe_to_dbkey and safe_to_dbkey[sid] != dbkey:
                collisions += 1
            safe_to_dbkey[sid] = dbkey  # last write wins on collision
    print(f'  {len(safe_to_dbkey):,} unique safe_ids ({collisions:,} collisions)', flush=True)

    print(f'Reading {args.docno_map}...', flush=True)
    indexed_docnos: list[str] = []
    with open(args.docno_map, encoding='utf-8') as f:
        for line in f:
            parts = line.rstrip('\n').split('\t')
            if len(parts) != 2:
                continue
            indexed_docnos.append(parts[1])
    print(f'  {len(indexed_docnos):,} indexed docnos', flush=True)

    print('Computing dbkey map...', flush=True)
    rows: list[tuple[str, str]] = []
    no_dbkey = 0
    same = 0
    for sid in indexed_docnos:
        dbkey = safe_to_dbkey.get(sid)
        if dbkey is None:
            no_dbkey += 1
            continue
        if dbkey == sid:
            same += 1
            continue
        rows.append((sid, dbkey))

    rows.sort()
    print(f'  {len(rows):,} entries with safe_id != dbkey', flush=True)
    print(f'  {same:,} entries already match (no remap needed)', flush=True)
    if no_dbkey:
        print(f'  WARNING: {no_dbkey:,} indexed docnos not found in allowlist', flush=True)

    print(f'Writing {args.output}...', flush=True)
    with open(args.output, 'w', encoding='utf-8') as f:
        for sid, dbkey in rows:
            f.write(f'{sid}\t{dbkey}\n')

    print(f'Done — {len(rows):,} entries written', flush=True)


if __name__ == '__main__':
    main()
