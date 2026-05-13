#!/usr/bin/env python3
"""
build_related.py — PRD-025 M3.

Reads the CSR link graph from build_link_graph.py and computes, per
entity-node, a ranked list of related entities via sampled random
walks. Writes the result as a FlatStore (related.store +
related.map) compatible with the existing FlatStore pattern used by
summaries.store, snippets.store, etc.

Algorithm: for each source entity s,
  - run K random walks of length L hops, starting from s
  - at each hop, restart with probability ALPHA (back to s),
    else jump uniformly to a random outlink of the current node
  - if the current node has no outlinks (sink), restart
  - count visits across all walks (excluding the source itself)
  - keep top-N by log-frequency score, filter same-class only,
    drop scores below noise threshold

PRD-025 says: same-class restriction at OUTPUT time, walks run
unrestricted (so cross-class entities act as bridges for signal
propagation). Class info comes from entity_class.json.

Storage: FlatStore. Per source, store a JSON array of
  [[target_docno, score], ...]
Length up to TOP_N. Map keys are source docnos.

Compute: embarrassingly parallel per-source. We use
multiprocessing.Pool, each worker keeping the graph in shared memory
via mmap'd numpy arrays (reads only, no contention).

Estimated time on the Hetzner box: ~1-2 hours for 500-750k entities
at K=1000, L=8 walks each, with 8 workers.

Usage:
  python3 build_related.py \\
      --graph-dir /mnt/wikipedia-source/related \\
      --out-store /mnt/wikipedia-source/related.store \\
      --out-map   /mnt/wikipedia-source/related.map \\
      --walks 1000 --length 8 --top-n 20 --workers 8
"""

from __future__ import annotations

import argparse
import json
import math
import multiprocessing as mp
import os
import random
import struct
import sys
import time
from collections import Counter
from pathlib import Path

import numpy as np


# Defaults from PRD-025; overridable via CLI.
DEFAULT_WALKS  = 1000
DEFAULT_LENGTH = 8
DEFAULT_ALPHA  = 0.15      # restart probability
DEFAULT_TOP_N  = 20
DEFAULT_WORKERS = max(1, mp.cpu_count() - 1)
SCORE_NOISE_THRESH = 0.05  # drop targets whose score < this fraction of the top score


# --- worker state (set by init) -----------------------------------------

_OFFSETS: np.ndarray | None = None
_NEIGHBOURS: np.ndarray | None = None
_CLASSES: list[str | None] | None = None   # by int id; None = no class info


def _worker_init(offsets_path: str, neighbours_path: str, classes_by_id: list[str | None]):
    """Initialiser per worker: open the graph files as memory-mapped
    arrays so all workers share OS-level pages. Random-walk loop is
    pure NumPy + Python with no contention."""
    global _OFFSETS, _NEIGHBOURS, _CLASSES
    _OFFSETS = np.memmap(offsets_path, dtype=np.int64, mode="r")
    _NEIGHBOURS = np.memmap(neighbours_path, dtype=np.int32, mode="r")
    _CLASSES = classes_by_id
    # Seed RNG with the PID so worker walks are reproducible per process
    # but distinct across workers within a run.
    random.seed(os.getpid() ^ int(time.time()))


def _walks_from(src_id: int, k_walks: int, length: int, alpha: float) -> Counter:
    """Run k_walks random walks of `length` hops from src_id; return
    a Counter of visited node ids (excluding src_id itself)."""
    offsets = _OFFSETS
    neighbours = _NEIGHBOURS
    visits: Counter = Counter()
    n_nodes = len(offsets) - 1
    for _ in range(k_walks):
        cur = src_id
        for _ in range(length):
            if random.random() < alpha:
                cur = src_id
                continue
            start = offsets[cur]
            end = offsets[cur + 1]
            if end == start:
                # sink — restart at source
                cur = src_id
                continue
            cur = int(neighbours[random.randrange(start, end)])
            if cur != src_id:
                visits[cur] += 1
    return visits


def _rank_and_filter(visits: Counter, src_id: int, src_class: str | None,
                     top_n: int) -> list[tuple[int, float]]:
    """Convert visit counts to log-frequency scores, normalise so top=1.0,
    filter to same-class (only if src has a class), drop below noise
    threshold, return top-N as a list of (node_id, score)."""
    if not visits:
        return []
    items: list[tuple[int, float]] = []
    for nid, count in visits.items():
        if nid == src_id:
            continue
        if src_class is not None and _CLASSES is not None:
            target_class = _CLASSES[nid]
            if target_class != src_class:
                continue
        items.append((nid, math.log(count + 1)))
    if not items:
        return []
    items.sort(key=lambda kv: -kv[1])
    top_score = items[0][1]
    if top_score <= 0:
        return []
    normalised = [
        (nid, round(score / top_score, 4))
        for nid, score in items if score / top_score >= SCORE_NOISE_THRESH
    ]
    return normalised[:top_n]


def _process_chunk(args_tuple):
    """Worker: process a batch of source-ids and return their
    (src_id, ranked_targets) tuples. Used with imap_unordered to
    keep the pool fed."""
    chunk, k_walks, length, alpha, top_n = args_tuple
    out = []
    for src_id, src_class in chunk:
        visits = _walks_from(src_id, k_walks, length, alpha)
        ranked = _rank_and_filter(visits, src_id, src_class, top_n)
        out.append((src_id, ranked))
    return out


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--graph-dir", type=Path, required=True,
                   help="dir containing graph.offsets.bin / graph.neighbours.bin / *.json")
    p.add_argument("--out-store", type=Path, required=True,
                   help="output FlatStore path (related.store)")
    p.add_argument("--out-map", type=Path, required=True,
                   help="output FlatStore offset map (related.map)")
    p.add_argument("--walks", type=int, default=DEFAULT_WALKS,
                   help=f"random walks per source [{DEFAULT_WALKS}]")
    p.add_argument("--length", type=int, default=DEFAULT_LENGTH,
                   help=f"hops per walk [{DEFAULT_LENGTH}]")
    p.add_argument("--alpha", type=float, default=DEFAULT_ALPHA,
                   help=f"restart probability [{DEFAULT_ALPHA}]")
    p.add_argument("--top-n", type=int, default=DEFAULT_TOP_N,
                   help=f"top-N related entities per source [{DEFAULT_TOP_N}]")
    p.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                   help=f"parallel workers [{DEFAULT_WORKERS}]")
    p.add_argument("--max-sources", type=int, default=0,
                   help="if >0, cap number of sources processed (for testing)")
    args = p.parse_args()

    t0 = time.time()
    print(f"[{time.time()-t0:6.1f}s] loading id maps from {args.graph_dir}", flush=True)
    with open(args.graph_dir / "id_to_docno.json", encoding="utf-8") as f:
        id_to_docno: list[str] = json.load(f)
    with open(args.graph_dir / "entity_class.json", encoding="utf-8") as f:
        entity_class: dict[str, str] = json.load(f)
    classes_by_id: list[str | None] = [entity_class.get(d) for d in id_to_docno]
    n_nodes = len(id_to_docno)
    print(f"  {n_nodes:,} entity nodes total", flush=True)

    # Process only nodes that have outlinks. We can detect that cheaply:
    # in the CSR they are the ones with offsets[i] < offsets[i+1].
    print(f"[{time.time()-t0:6.1f}s] loading offsets to find non-sink sources", flush=True)
    offsets_path = args.graph_dir / "graph.offsets.bin"
    neighbours_path = args.graph_dir / "graph.neighbours.bin"
    offsets_full = np.memmap(offsets_path, dtype=np.int64, mode="r")
    out_degree = np.diff(offsets_full)
    source_ids = np.where(out_degree > 0)[0]
    if args.max_sources > 0:
        source_ids = source_ids[: args.max_sources]
    print(f"  {len(source_ids):,} non-sink sources to walk", flush=True)

    # Pack (src_id, class) tuples into chunks for the worker pool. A
    # chunk size of ~256 sources is a good trade-off between IPC
    # overhead and load balance.
    CHUNK = 256
    chunks = []
    cur: list = []
    for sid in source_ids.tolist():
        cur.append((sid, classes_by_id[sid]))
        if len(cur) >= CHUNK:
            chunks.append((cur, args.walks, args.length, args.alpha, args.top_n))
            cur = []
    if cur:
        chunks.append((cur, args.walks, args.length, args.alpha, args.top_n))
    print(f"  {len(chunks):,} chunks of up to {CHUNK} sources each", flush=True)

    # Write the FlatStore as we go. We append records to .store and
    # accumulate the map in RAM; at the end we serialise the map.
    args.out_store.parent.mkdir(parents=True, exist_ok=True)
    if args.out_store.exists():
        args.out_store.unlink()
    args.out_map.parent.mkdir(parents=True, exist_ok=True)

    fmap: dict[str, list[int]] = {}     # docno -> [offset, length]
    n_emitted = n_empty = 0
    bytes_written = 0
    last_log = t0

    with open(args.out_store, "wb") as store, \
         mp.Pool(
             args.workers,
             initializer=_worker_init,
             initargs=(str(offsets_path), str(neighbours_path), classes_by_id),
         ) as pool:
        print(f"[{time.time()-t0:6.1f}s] starting {args.workers} workers", flush=True)
        n_done_sources = 0
        for results in pool.imap_unordered(_process_chunk, chunks):
            for src_id, ranked in results:
                n_done_sources += 1
                if not ranked:
                    n_empty += 1
                    continue
                src_docno = id_to_docno[src_id]
                payload = [(id_to_docno[nid], score) for nid, score in ranked]
                blob = json.dumps(payload, separators=(",", ":")).encode("utf-8")
                offset = bytes_written
                store.write(blob)
                bytes_written += len(blob)
                fmap[src_docno] = [offset, len(blob)]
                n_emitted += 1
            now = time.time()
            if now - last_log >= 30:
                last_log = now
                rate = n_done_sources / max(1, now - t0)
                eta_s = (len(source_ids) - n_done_sources) / max(1, rate)
                print(
                    f"[{now-t0:6.1f}s] progress: {n_done_sources:,}/{len(source_ids):,} sources "
                    f"({100*n_done_sources/len(source_ids):.1f}%), "
                    f"emit={n_emitted:,} empty={n_empty:,}, "
                    f"rate={rate:.0f} src/s, ETA {eta_s/60:.1f} min",
                    flush=True,
                )

    print(f"[{time.time()-t0:6.1f}s] writing map ({len(fmap):,} entries)", flush=True)
    tmp_map = args.out_map.with_suffix(args.out_map.suffix + ".tmp")
    with open(tmp_map, "w", encoding="utf-8") as f:
        json.dump(fmap, f, separators=(",", ":"), sort_keys=True)
    os.replace(tmp_map, args.out_map)
    print(f"[{time.time()-t0:6.1f}s] DONE: emitted={n_emitted:,} empty={n_empty:,} "
          f"store={bytes_written/1024/1024:.1f}MB", flush=True)


if __name__ == "__main__":
    main()
