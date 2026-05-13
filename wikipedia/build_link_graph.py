#!/usr/bin/env python3
r"""
build_link_graph.py — PRD-025 M2.

Streams the enwiki dump and extracts the entity→entity link graph.
Output is a compact CSR (compressed sparse row) representation that
the random-walk stage reads.

  - sources: int32 array of entity-docno ids (sorted by id)
  - offsets: int64 array, length len(sources)+1, where offsets[i] gives
    the start index into the neighbours array for sources[i] and
    offsets[i+1] gives the end.
  - neighbours: int32 array, the concatenated neighbour-id lists.

Plus a small metadata file mapping docno ↔ int32 id, plus the entity
class per docno.

Streaming details:

  - Process the same XML dump build_entity_set.py reads. Single pass.
  - For each page in mainspace, non-redirect, where docno ∈ entities:
    - Extract `[[Target]]` wikitext links (pipe form `[[Target|alt]]`
      taken as Target).
    - Filter to entity targets, drop self-loops, dedupe.
    - Emit (source, target) edges.
  - Aggregate into CSR format.

Output files (written under --out-dir):
  - id_to_docno.json    int-id → docno mapping (for reverse lookup at
                        random-walk score time)
  - docno_to_id.json    inverse mapping (used by build_related.py to
                        find a source's id)
  - entity_class.json   docno → class (subset of the input file, only
                        for entities that actually have outlinks)
  - graph.offsets.bin   int64 packed offsets array
  - graph.neighbours.bin int32 packed neighbours array

Memory: roughly 4 bytes × edges. With ~50-150M edges, that's
~600 MB-1 GB resident at the aggregation stage. Fits on the Hetzner
box; if it doesn't we'd spill edges to disk and sort, but for now
in-memory is simpler.

Usage:
  python3 build_link_graph.py \
      --enwiki-dump /mnt/wikipedia-source/enwiki-latest-pages-articles.xml.bz2 \
      --entity-classes /mnt/wikipedia-source/entity_classes.json \
      --out-dir /mnt/wikipedia-source/related
"""

from __future__ import annotations

import argparse
import array
import bz2
import json
import os
import re
import sys
import time
from pathlib import Path


# Patterns from build_entity_set.py's iterator (kept local to avoid
# importing across the wikipedia tools dir; copies are tiny and
# avoid coupling).
TITLE_RE    = re.compile(r"<title>([^<]+)</title>")
REDIRECT_RE = re.compile(r"<redirect\b[^/]*/>")
NS_RE       = re.compile(r"<ns>(\d+)</ns>")
TEXT_OPEN   = re.compile(r"<text\b[^>]*>")
TEXT_CLOSE  = re.compile(r"</text>")

# Match `[[Target]]` or `[[Target|display text]]` BUT NOT:
#   `[[Category:Foo]]`  (namespace links)
#   `[[File:Foo.jpg]]`  (file embeds)
#   `[[Image:...]]`     (legacy file)
#   `[[en:...]]`        (inter-wiki)
#   `[[:Category:...]]` (link-to-category form)
# We accept any link without a colon prefix in the target.
LINK_RE = re.compile(r"\[\[([^|\]\n#]+?)(?:#[^|\]\n]*)?(?:\|[^\]]*)?\]\]")


def iter_pages(dump_path: Path):
    with bz2.open(dump_path, "rt", encoding="utf-8", errors="replace") as f:
        buf = ""
        while True:
            chunk = f.read(1 << 20)
            if not chunk:
                break
            buf += chunk
            while True:
                start = buf.find("<page>")
                if start < 0:
                    break
                end = buf.find("</page>", start)
                if end < 0:
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
                body = ""
                topen = TEXT_OPEN.search(page)
                tclose = TEXT_CLOSE.search(page)
                if topen and tclose and tclose.start() > topen.end():
                    body = page[topen.end(): tclose.start()]
                yield title, is_redirect, ns, body


def extract_links(body: str) -> set[str]:
    """Return the unique link targets found in `body`, as docno-form
    titles (underscored, leading char un-capitalised → not changed;
    we preserve the source case but normalise spaces to underscores).

    Strips anchor fragments (#section) and pipe aliases ([[Foo|bar]]).
    Skips namespace links (`Category:`, `File:`, `Image:`, anything with
    a colon prefix)."""
    out: set[str] = set()
    for m in LINK_RE.finditer(body):
        target = m.group(1).strip()
        if not target or ":" in target.split("/", 1)[0]:
            # Namespace prefix detected — skip
            continue
        # Wikipedia capitalises the first letter of the article name.
        # Article links in body text often use lowercase initial; we
        # normalise to upper-first so matches against our docnos work.
        if target[0].islower():
            target = target[0].upper() + target[1:]
        out.add(target.replace(" ", "_"))
    return out


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--enwiki-dump", type=Path, required=True)
    p.add_argument("--entity-classes", type=Path, required=True,
                   help="entity_classes.json from build_entity_set.py")
    p.add_argument("--out-dir", type=Path, required=True,
                   help="directory to write graph files to")
    args = p.parse_args()

    if not args.enwiki_dump.exists():
        sys.exit(f"ERROR: enwiki dump not found at {args.enwiki_dump}")
    if not args.entity_classes.exists():
        sys.exit(f"ERROR: entity_classes.json not found at {args.entity_classes}")

    print(f"loading entity classification", flush=True)
    with open(args.entity_classes, encoding="utf-8") as f:
        entity_classes = json.load(f)
    print(f"  {len(entity_classes):,} entities", flush=True)
    entity_set = set(entity_classes.keys())

    args.out_dir.mkdir(parents=True, exist_ok=True)

    # Pass 1: assign int32 ids to entities in (eventual) sorted-docno
    # order. We collect outlinks into an adjacency map keyed by source
    # docno first, then renumber + write CSR at the end.
    t0 = time.time()
    print(f"[{time.time()-t0:6.1f}s] pass: streaming dump + extracting edges", flush=True)

    # adjacency: source_docno -> set of target_docnos (only entities)
    adjacency: dict[str, set[str]] = {}
    n_pages = n_skipped = n_no_outlinks = 0
    n_total_edges = 0
    for title, is_redirect, ns, body in iter_pages(args.enwiki_dump):
        n_pages += 1
        if n_pages % 200_000 == 0:
            print(f"[{time.time()-t0:6.1f}s] {n_pages:,} pages, "
                  f"{len(adjacency):,} sources, {n_total_edges:,} edges", flush=True)
        if ns != 0 or is_redirect:
            n_skipped += 1
            continue
        docno = title.replace(" ", "_")
        if docno not in entity_set:
            continue
        links = extract_links(body)
        # filter to entities, drop self
        entity_links = {t for t in links if t != docno and t in entity_set}
        if not entity_links:
            n_no_outlinks += 1
            continue
        adjacency[docno] = entity_links
        n_total_edges += len(entity_links)

    elapsed = time.time() - t0
    print(f"[{elapsed:.1f}s] pass done: {n_pages:,} pages, "
          f"{len(adjacency):,} sources, {n_total_edges:,} edges", flush=True)
    print(f"  pages skipped (ns/redirect): {n_skipped:,}; "
          f"entities with no entity-outlinks: {n_no_outlinks:,}", flush=True)

    # Build the int32 id mapping. Sort docnos so the int ids are stable
    # across rebuilds with the same corpus (important for caching /
    # debugging).
    print(f"[{time.time()-t0:6.1f}s] building id maps + CSR", flush=True)
    sources_sorted = sorted(adjacency.keys())
    docno_to_id: dict[str, int] = {d: i for i, d in enumerate(sources_sorted)}
    # Target docnos may include entities that have no outlinks (sinks).
    # Give them ids too so the neighbours array can refer to them.
    next_id = len(docno_to_id)
    for src, targets in adjacency.items():
        for t in targets:
            if t not in docno_to_id:
                docno_to_id[t] = next_id
                next_id += 1
    id_to_docno = [None] * next_id
    for docno, i in docno_to_id.items():
        id_to_docno[i] = docno

    print(f"  total entity nodes (sources + sinks): {next_id:,}", flush=True)

    # Build CSR. offsets[i] / offsets[i+1] bracket the neighbour list
    # for source-id i. Sinks have offsets[i] == offsets[i+1] (empty
    # slice). Source-id space is the same as overall id space — sinks
    # appear after the sources in the id range, but the offsets array
    # covers all ids so they're handled uniformly.
    offsets = array.array("q", [0] * (next_id + 1))   # int64
    neighbours = array.array("i", [])                 # int32

    for src in sources_sorted:
        src_id = docno_to_id[src]
        offsets[src_id] = len(neighbours)
        # Sort neighbours by id for deterministic / cache-friendly output.
        nbrs = sorted(docno_to_id[t] for t in adjacency[src])
        neighbours.extend(nbrs)
        offsets[src_id + 1] = len(neighbours)
    # Fill in offsets for nodes that have no outlinks (sinks). They
    # all point at the end of neighbours (zero-length range).
    last = offsets[len(sources_sorted)]
    for i in range(len(sources_sorted), next_id + 1):
        offsets[i] = last

    print(f"  wrote {len(neighbours):,} edges to CSR (int32 each = "
          f"{len(neighbours)*4/1024/1024:.1f} MB)", flush=True)

    # Persist everything.
    print(f"[{time.time()-t0:6.1f}s] writing files to {args.out_dir}", flush=True)

    def _write_atomic(path: Path, body: bytes) -> None:
        tmp = path.with_suffix(path.suffix + ".tmp")
        with open(tmp, "wb") as f:
            f.write(body)
        os.replace(tmp, path)

    _write_atomic(args.out_dir / "graph.offsets.bin", offsets.tobytes())
    _write_atomic(args.out_dir / "graph.neighbours.bin", neighbours.tobytes())

    tmp = args.out_dir / "docno_to_id.json.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(docno_to_id, f, separators=(",", ":"))
    os.replace(tmp, args.out_dir / "docno_to_id.json")

    tmp = args.out_dir / "id_to_docno.json.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(id_to_docno, f, separators=(",", ":"))
    os.replace(tmp, args.out_dir / "id_to_docno.json")

    # Only persist class info for entities that actually have an id
    # (skip dropped no-outlinks entities to save bytes). Sinks keep
    # their class info too since they're still valid targets.
    filtered_classes = {d: entity_classes[d] for d in docno_to_id if d in entity_classes}
    tmp = args.out_dir / "entity_class.json.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(filtered_classes, f, separators=(",", ":"))
    os.replace(tmp, args.out_dir / "entity_class.json")

    elapsed = time.time() - t0
    print(f"[{elapsed:.1f}s] DONE", flush=True)


if __name__ == "__main__":
    main()
