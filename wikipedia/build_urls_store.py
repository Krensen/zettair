#!/usr/bin/env python3
"""
build_urls_store.py — Build a FlatStore of Wikipedia URLs from the dbkeys file.

Produces:
  enwiki_top1m_urls.store — concatenated UTF-8 URLs
  enwiki_top1m_urls.map   — JSON: {safe_id: [offset, length]}

Reads enwiki_top1m.dbkeys.tsv (safe_id -> dbkey) and writes a URL of the form
https://en.wikipedia.org/wiki/{dbkey} for every entry. Skips any docno where
safe_id == dbkey (no remapping needed; server.py falls back to constructing
the URL on-the-fly for those).

This is a one-shot bootstrap for an existing index. Future rebuilds via
wiki2trec.py write the URL store directly.

Usage:
  python3 build_urls_store.py enwiki_top1m.dbkeys.tsv enwiki_top1m_urls.store enwiki_top1m_urls.map
"""
import argparse
import json


URL_PREFIX = "https://en.wikipedia.org/wiki/"


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('dbkeys_file', help='safe_id\\tdbkey TSV')
    parser.add_argument('store_out',   help='Output store path')
    parser.add_argument('map_out',     help='Output map path')
    args = parser.parse_args()

    print(f'Reading {args.dbkeys_file}...', flush=True)
    url_map: dict[str, list[int]] = {}
    offset = 0

    with open(args.dbkeys_file, encoding='utf-8') as f, \
         open(args.store_out, 'wb') as store:
        for line in f:
            parts = line.rstrip('\n').split('\t', 1)
            if len(parts) != 2:
                continue
            safe_id, dbkey = parts
            url = (URL_PREFIX + dbkey).encode('utf-8')
            url_map[safe_id] = [offset, len(url)]
            store.write(url)
            offset += len(url)

    print(f'Writing {args.map_out} ({len(url_map):,} entries)...', flush=True)
    with open(args.map_out, 'w', encoding='utf-8') as f:
        json.dump(url_map, f, separators=(',', ':'))

    print(f'Done — {len(url_map):,} URLs, {offset / 1024 / 1024:.1f} MB store', flush=True)


if __name__ == '__main__':
    main()
