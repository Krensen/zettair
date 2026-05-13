#!/usr/bin/env python3
"""
build_entity_set.py — PRD-025 M1.

Classifies each docno in our corpus into one of five entity classes,
using Wikipedia *categories* as the signal (not Wikidata). Categories
are already in the enwiki dump we process, so this adds zero new
downloads and zero new external dependencies.

Classes: human / place / organisation / work / event. A docno without
a recognised class is NOT an entity and is excluded from the
related-entities graph (PRD-025).

Output: entity_classes.json
    {
      "Mark_Carney":         "human",
      "Liverpool_F.C.":      "organisation",
      "London":              "place",
      "Inception_(film)":    "work",
      "Cuban_Missile_Crisis":"event"
    }

Algorithm:

  Stream the enwiki bz2 dump. For each <page>:
    - Skip non-mainspace (we only care about article-namespace pages).
    - Skip redirects (their categories point at the redirect target).
    - Extract all `[[Category:Foo]]` patterns from the wikitext.
    - Match category names against a curated regex set per class.
    - First-match-wins assigns the class.
    - Emit (docno, class) only if docno is in our corpus.

Category classification rules are hand-curated below in CLASS_RULES.
They're generous on purpose: 'Living people', '\d{4} births', etc
catch almost every biography; 'Cities in XXX', 'Towns in XXX', 'Villages
in XXX' catch settlements; etc. Inclusions are scored against the
order of CLASS_RULES — first match wins, so put narrower categories
before broader ones.

CPU-bound (regex scan of ~80 GB of wikitext). Single-pass. Expected
runtime on the Hetzner box: 1-2 hours (matches wiki2trec speed
since we do the same bz2 streaming).

Usage:
  python3 build_entity_set.py \\
      --enwiki-dump  /mnt/wikipedia-source/enwiki-latest-pages-articles.xml.bz2 \\
      --titles       /mnt/wikipedia-source/top_titles.txt \\
      --out          /mnt/wikipedia-source/entity_classes.json
"""

from __future__ import annotations

import argparse
import bz2
import json
import os
import re
import sys
import time
from pathlib import Path


# Per-class regex rules. First match wins; order matters.
# Each rule is a regex evaluated against the category name (without
# the leading 'Category:' prefix). Case-insensitive.
CLASS_RULES: list[tuple[str, list[re.Pattern]]] = [
    ("human", [
        re.compile(r"^Living people$", re.I),
        re.compile(r"^\d{4} births$"),
        re.compile(r"^\d{4} deaths$"),
        re.compile(r"^People from "),
        re.compile(r"^Year of (birth|death) (missing|unknown)", re.I),
    ]),
    ("work", [
        re.compile(r"^\d{4} films$"),
        re.compile(r"^\d{4} novels$"),
        re.compile(r"^\d{4} (albums|songs|EPs|singles)$"),
        re.compile(r"^\d{4} video games$"),
        re.compile(r"^\d{4} books$"),
        re.compile(r"^\d{4} plays$"),
        re.compile(r"^\d{4} television (series|films|specials)( debuts)?$"),
        re.compile(r" television series$", re.I),
        re.compile(r" novels$", re.I),
        re.compile(r" albums$", re.I),
        re.compile(r" video games$", re.I),
        re.compile(r"^Films directed by ", re.I),
        re.compile(r"^Films produced by ", re.I),
        re.compile(r"^Novels by ", re.I),
        re.compile(r"^Albums produced by ", re.I),
        re.compile(r"^Songs written by ", re.I),
    ]),
    ("organisation", [
        re.compile(r"^Companies (based in|established in) ", re.I),
        re.compile(r"^Companies of ", re.I),
        re.compile(r"^Manufacturing companies ", re.I),
        re.compile(r"^Banks (of|established in|based in) ", re.I),
        re.compile(r"^Football clubs in ", re.I),
        re.compile(r"^Association football clubs ", re.I),
        re.compile(r"^Sports clubs (in|established in) ", re.I),
        re.compile(r"^Political parties (in|established in) ", re.I),
        re.compile(r"^Non-profit organi[sz]ations ", re.I),
        re.compile(r"^Universities and colleges (in|of) ", re.I),
        re.compile(r"^Government agencies of ", re.I),
        re.compile(r"^Record labels established in ", re.I),
    ]),
    ("place", [
        re.compile(r"^Cities (in|of) ", re.I),
        re.compile(r"^Towns (in|of) ", re.I),
        re.compile(r"^Villages (in|of) ", re.I),
        re.compile(r"^Municipalities (in|of) ", re.I),
        re.compile(r"^Counties of ", re.I),
        re.compile(r"^States of ", re.I),
        re.compile(r"^Districts of ", re.I),
        re.compile(r"^Populated places (in|of) ", re.I),
        re.compile(r"^Capitals (in|of) ", re.I),
        re.compile(r"^Countries in ", re.I),
        re.compile(r"^Islands of ", re.I),
        re.compile(r"^Mountains of ", re.I),
        re.compile(r"^Rivers of ", re.I),
        re.compile(r"^Lakes of ", re.I),
    ]),
    ("event", [
        re.compile(r"^\d{4} (in|elections|events|natural disasters)", re.I),
        re.compile(r"^Battles of ", re.I),
        re.compile(r"^Wars involving ", re.I),
        re.compile(r"^Conflicts in \d{4}$", re.I),
        re.compile(r"^Riots in \d{4}", re.I),
        re.compile(r"^Earthquakes in ", re.I),
        re.compile(r"^Tropical storms ", re.I),
        re.compile(r"^Elections in ", re.I),
        re.compile(r" Olympics$", re.I),
        re.compile(r" Cup$", re.I),    # FIFA World Cup, AFC Asian Cup etc
        re.compile(r" Cup Final$", re.I),
        re.compile(r"^\d{4} .+ election", re.I),
    ]),
]


# `[[Category:Foo|optional sort key]]` — we want the Foo.
CATEGORY_RE = re.compile(r"\[\[Category:([^|\]\n]+)(?:\|[^\]]*)?\]\]", re.I)
# `<page>` boundary detection in the XML. We pull title + redirect + text.
TITLE_RE    = re.compile(r"<title>([^<]+)</title>")
REDIRECT_RE = re.compile(r"<redirect\b[^/]*/>")
NS_RE       = re.compile(r"<ns>(\d+)</ns>")
TEXT_OPEN   = re.compile(r"<text\b[^>]*>")
TEXT_CLOSE  = re.compile(r"</text>")


def classify_categories(cats: list[str]) -> str | None:
    """First-match-wins across CLASS_RULES.

    Iterates classes in declared order; within each class, returns
    the first rule that any of the categories match. Returns None
    if no rule matches."""
    for cls, rules in CLASS_RULES:
        for r in rules:
            for c in cats:
                if r.search(c):
                    return cls
    return None


def load_titles(path: Path) -> set[str]:
    out = set()
    with open(path, encoding="utf-8") as f:
        for line in f:
            t = line.rstrip("\n")
            if t:
                out.add(t)
    return out


def iter_pages(dump_path: Path):
    """Yield (title, is_redirect, ns, body) tuples by chunked scan of
    the enwiki XML dump. Reads in 1 MB blocks; accumulates until we
    see a complete `<page>...</page>` element, then parses out the
    fields we need with regex. Avoids loading the whole XML."""
    with bz2.open(dump_path, "rt", encoding="utf-8", errors="replace") as f:
        buf = ""
        while True:
            chunk = f.read(1 << 20)   # 1 MB
            if not chunk:
                break
            buf += chunk
            while True:
                start = buf.find("<page>")
                if start < 0:
                    break
                end = buf.find("</page>", start)
                if end < 0:
                    # need more data
                    buf = buf[start:]
                    break
                page = buf[start: end + len("</page>")]
                buf = buf[end + len("</page>"):]

                title_m = TITLE_RE.search(page)
                if not title_m:
                    continue
                title = title_m.group(1)
                is_redirect = bool(REDIRECT_RE.search(page))
                ns_m = NS_RE.search(page)
                ns = int(ns_m.group(1)) if ns_m else 0
                # text body (only present for non-redirects we care about)
                body = ""
                topen = TEXT_OPEN.search(page)
                tclose = TEXT_CLOSE.search(page)
                if topen and tclose and tclose.start() > topen.end():
                    body = page[topen.end(): tclose.start()]
                yield title, is_redirect, ns, body


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--enwiki-dump", type=Path, required=True,
                   help="path to enwiki-latest-pages-articles.xml.bz2")
    p.add_argument("--titles", type=Path, required=True,
                   help="path to top_titles.txt (corpus restriction)")
    p.add_argument("--out", type=Path, required=True,
                   help="output JSON path (entity_classes.json)")
    p.add_argument("--titles-out", type=Path, default=None,
                   help="optional: also emit a (docno\\tdisplay_title) TSV at this path "
                        "(PRD-026 uses this to match Google News headlines onto docnos)")
    args = p.parse_args()

    if not args.enwiki_dump.exists():
        sys.exit(f"ERROR: enwiki dump not found at {args.enwiki_dump}")
    if not args.titles.exists():
        sys.exit(f"ERROR: titles file not found at {args.titles}")

    print(f"loading corpus titles from {args.titles}", flush=True)
    corpus = load_titles(args.titles)
    print(f"  {len(corpus):,} titles in corpus", flush=True)

    t0 = time.time()
    docno_to_class: dict[str, str] = {}
    n_pages = n_skipped_redirect = n_skipped_ns = n_not_in_corpus = n_no_cats = n_unrecognised = 0
    for title, is_redirect, ns, body in iter_pages(args.enwiki_dump):
        n_pages += 1
        if n_pages % 200_000 == 0:
            print(f"[{time.time()-t0:6.1f}s] {n_pages:,} pages, "
                  f"{len(docno_to_class):,} classified", flush=True)
        if ns != 0:
            n_skipped_ns += 1
            continue
        if is_redirect:
            n_skipped_redirect += 1
            continue
        docno = title.replace(" ", "_")
        if docno not in corpus:
            n_not_in_corpus += 1
            continue
        cats = [m.group(1).strip() for m in CATEGORY_RE.finditer(body)]
        if not cats:
            n_no_cats += 1
            continue
        cls = classify_categories(cats)
        if cls is None:
            n_unrecognised += 1
            continue
        docno_to_class[docno] = cls

    elapsed = time.time() - t0
    print(f"[{elapsed:.1f}s] done: {n_pages:,} pages, {len(docno_to_class):,} classified", flush=True)
    print(f"  skipped: ns!=0={n_skipped_ns:,} redirects={n_skipped_redirect:,} "
          f"not-in-corpus={n_not_in_corpus:,}", flush=True)
    print(f"  in-corpus rejected: no-categories={n_no_cats:,} unrecognised={n_unrecognised:,}", flush=True)

    from collections import Counter
    breakdown = Counter(docno_to_class.values())
    print("class breakdown:", flush=True)
    for cls, n in sorted(breakdown.items(), key=lambda kv: -kv[1]):
        print(f"  {cls:15} {n:>8,}", flush=True)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    tmp = args.out.with_suffix(args.out.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(docno_to_class, f, separators=(",", ":"), sort_keys=True)
    os.replace(tmp, args.out)
    print(f"wrote {args.out} ({args.out.stat().st_size/1024/1024:.1f} MB)", flush=True)

    # PRD-026: also emit a TSV of (docno, display_title) for entity
    # articles. The trending fetcher loads this as a reverse index to
    # match Google News top-stories headlines onto Wikipedia docnos.
    if args.titles_out:
        args.titles_out.parent.mkdir(parents=True, exist_ok=True)
        ttmp = args.titles_out.with_suffix(args.titles_out.suffix + ".tmp")
        with open(ttmp, "w", encoding="utf-8") as f:
            for docno in sorted(docno_to_class.keys()):
                display = docno.replace("_", " ")
                f.write(f"{docno}\t{display}\n")
        os.replace(ttmp, args.titles_out)
        print(f"wrote {args.titles_out} ({args.titles_out.stat().st_size/1024/1024:.1f} MB)", flush=True)


if __name__ == "__main__":
    main()
