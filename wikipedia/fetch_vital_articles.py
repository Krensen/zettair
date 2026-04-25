#!/usr/bin/env python3
"""
fetch_vital_articles.py — Download Wikipedia Vital Articles Level 4 as a TREC-ready XML dump.

Steps:
  1. Fetch all Level 4 vital article subpages from the Wikipedia API
  2. Extract article titles from wikitext (pattern: [[Article Title]])
  3. Export articles in batches via Special:Export
  4. Write a single combined XML file compatible with wiki2trec.py

Output: vital_articles.xml  (~500MB, ~10K articles)

Usage:
  python3 fetch_vital_articles.py
  python3 fetch_vital_articles.py --level 5   # ~50K articles
  python3 fetch_vital_articles.py --out my.xml
"""

import argparse
import re
import sys
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path

HERE = Path(__file__).parent

API_URL    = "https://en.wikipedia.org/w/api.php"
EXPORT_URL = "https://en.wikipedia.org/wiki/Special:Export"
HEADERS    = {"User-Agent": "ZettairVitalFetcher/1.0 (https://github.com/Krensen/zettair; bot)"}
BATCH_SIZE = 20    # Special:Export chokes on large batches
SUBPAGE_BATCH = 10 # API titles= param limit for wikitext fetches
SLEEP      = 0.5   # seconds between API calls — be polite


def api_get(params: dict) -> dict:
    params["format"] = "json"
    params["formatversion"] = "2"
    qs = urllib.parse.urlencode(params)
    req = urllib.request.Request(f"{API_URL}?{qs}", headers=HEADERS)
    with urllib.request.urlopen(req, timeout=30) as r:
        import json
        return json.loads(r.read().decode("utf-8"))


def get_vital_subpages(level: int) -> list[str]:
    """Return all Wikipedia: subpage titles for the given vital articles level."""
    prefix = f"Vital_articles/Level_{level}/"
    pages = []
    params = {
        "action": "query",
        "list": "allpages",
        "apprefix": prefix,
        "apnamespace": "4",
        "aplimit": "500",
    }
    while True:
        data = api_get(params)
        for p in data["query"]["allpages"]:
            title = p["title"]
            # Skip meta/admin subpages
            if any(skip in title for skip in ("Article alerts", "Candidates", "Draft", "Removed", "Archive")):
                continue
            pages.append(title)
        if "continue" not in data:
            break
        params["apcontinue"] = data["continue"]["apcontinue"]
        time.sleep(SLEEP)

    print(f"Found {len(pages)} subpages for Level {level}", flush=True)
    return pages


def extract_titles_from_wikitext(wikitext: str) -> list[str]:
    """Extract article titles from vital articles wikitext."""
    titles = []
    for m in re.finditer(r"\[\[([^\]|#]+?)(?:\|[^\]]*)?\]\]", wikitext):
        title = m.group(1).strip()
        # Skip Wikipedia meta-namespace links
        if ":" in title:
            continue
        if not title:
            continue
        titles.append(title)
    return titles


def fetch_wikitext_for_subpages(subpages: list[str]) -> dict[str, str]:
    """Fetch wikitext for a list of Wikipedia: namespace subpages, in batches."""
    result = {}
    for i in range(0, len(subpages), SUBPAGE_BATCH):
        batch = subpages[i:i + SUBPAGE_BATCH]
        params = {
            "action": "query",
            "titles": "|".join(batch),
            "prop": "revisions",
            "rvprop": "content",
            "rvslots": "main",
        }
        data = api_get(params)
        for page in data["query"]["pages"]:
            title = page.get("title", "")
            revs = page.get("revisions", [])
            if revs:
                wikitext = revs[0].get("slots", {}).get("main", {}).get("content", "")
                result[title] = wikitext
        print(f"  Fetched wikitext: {i + len(batch)}/{len(subpages)} subpages", flush=True)
        time.sleep(SLEEP)
    return result


def collect_all_titles(level: int) -> list[str]:
    """Fetch all vital article titles for the given level."""
    subpages = get_vital_subpages(level)
    print(f"Fetching wikitext for {len(subpages)} subpages...", flush=True)
    wikitext_map = fetch_wikitext_for_subpages(subpages)

    seen = set()
    titles = []
    for wikitext in wikitext_map.values():
        for t in extract_titles_from_wikitext(wikitext):
            if t not in seen:
                seen.add(t)
                titles.append(t)

    print(f"Collected {len(titles):,} unique article titles", flush=True)
    return titles


def export_articles(titles: list[str], out_path: Path):
    """Fetch articles via Special:Export in batches and write a combined XML file."""
    NS = "http://www.mediawiki.org/xml/export-0.11/"
    total = len(titles)
    written = 0
    header_written = False

    with open(out_path, "w", encoding="utf-8") as out:
        for batch_start in range(0, total, BATCH_SIZE):
            batch = titles[batch_start:batch_start + BATCH_SIZE]
            payload = urllib.parse.urlencode({
                "pages": "\n".join(batch),
                "action": "submit",
                "curonly": "1",
            }).encode("utf-8")
            req = urllib.request.Request(EXPORT_URL, data=payload, headers={
                **HEADERS,
                "Content-Type": "application/x-www-form-urlencoded",
            })
            try:
                with urllib.request.urlopen(req, timeout=60) as r:
                    xml_bytes = r.read()
            except Exception as e:
                print(f"  WARNING: batch {batch_start}–{batch_start+len(batch)} failed: {e}", flush=True)
                time.sleep(2)
                continue

            try:
                root = ET.fromstring(xml_bytes)
            except ET.ParseError as e:
                print(f"  WARNING: XML parse error on batch {batch_start}: {e}", flush=True)
                continue

            if not header_written:
                # Write XML declaration + opening mediawiki tag with namespace
                out.write('<?xml version="1.0" encoding="UTF-8"?>\n')
                out.write(f'<mediawiki xmlns="{NS}">\n')
                header_written = True

            # Write just the <page> elements from this batch
            for page_el in root.findall(f"{{{NS}}}page"):
                out.write("  " + ET.tostring(page_el, encoding="unicode") + "\n")
                written += 1

            print(f"  Exported {batch_start + len(batch)}/{total} titles, {written} pages written", flush=True)
            time.sleep(SLEEP)

        if header_written:
            out.write("</mediawiki>\n")

    print(f"\nDone — {written:,} articles written to {out_path}", flush=True)
    return written


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--level", type=int, default=4, choices=[3, 4, 5],
                        help="Vital articles level (3=1K, 4=10K, 5=50K) [default: 4]")
    parser.add_argument("--out", type=Path, default=None,
                        help="Output XML path [default: vital_articles_levelN.xml]")
    parser.add_argument("--titles-only", action="store_true",
                        help="Just print collected titles, don't download articles")
    args = parser.parse_args()

    out_path = args.out or HERE / f"vital_articles_level{args.level}.xml"

    print(f"Fetching Level {args.level} vital article titles...", flush=True)
    titles = collect_all_titles(args.level)

    if args.titles_only:
        for t in titles:
            print(t)
        return

    # Write titles list as a side effect (useful for autosuggest + docno map)
    titles_path = HERE / f"vital_articles_level{args.level}_titles.txt"
    with open(titles_path, "w", encoding="utf-8") as f:
        f.write("\n".join(titles))
    print(f"Titles written to {titles_path}", flush=True)

    print(f"\nExporting {len(titles):,} articles to {out_path}...", flush=True)
    written = export_articles(titles, out_path)

    size_mb = out_path.stat().st_size / 1024 / 1024
    print(f"\nAll done: {written:,} articles, {size_mb:.0f}MB → {out_path}")
    print(f"Next step: python3 wiki2trec.py {out_path} vital_articles_level{args.level}.trec")


if __name__ == "__main__":
    main()
