#!/usr/bin/env python3
"""
click_coverage.py — Analyse Wikipedia clickstream to find coverage thresholds.

Downloads one month of enwiki clickstream (if not already present), aggregates
search-driven clicks per article, then:
  - Prints how many articles cover 90%, 95%, 99% of total clicks
  - Plots the click distribution curve (rank vs cumulative % of clicks)

Usage:
  python3 click_coverage.py
  python3 click_coverage.py --month 2025-03 --no-plot
"""

import argparse
import gzip
import os
import sys
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))

THRESHOLDS = [0.50, 0.80, 0.90, 0.95, 0.99, 0.999]


def download_clickstream(month: str) -> str:
    fname = f"clickstream-enwiki-{month}.tsv.gz"
    path = os.path.join(HERE, fname)
    if os.path.exists(path):
        print(f"Using existing {fname}", flush=True)
        return path
    url = f"https://dumps.wikimedia.org/other/clickstream/{month}/{fname}"
    print(f"Downloading {url}...", flush=True)
    urllib.request.urlretrieve(url, path)
    print(f"  Saved to {path}", flush=True)
    return path


def load_clicks(path: str) -> dict[str, int]:
    """Aggregate search-driven clicks per article from a clickstream file."""
    counts: dict[str, int] = {}
    rows = 0
    with gzip.open(path, "rt", encoding="utf-8", errors="replace") as f:
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) != 4:
                continue
            referrer, article, _, count_str = parts
            if referrer != "other-search":
                continue
            try:
                counts[article] = counts.get(article, 0) + int(count_str)
                rows += 1
            except ValueError:
                continue
    print(f"  {rows:,} search-referral rows, {len(counts):,} unique articles", flush=True)
    return counts


def analyse(counts: dict[str, int], thresholds: list[float]) -> list[tuple[float, int, str]]:
    """
    Sort articles by descending clicks, compute cumulative coverage.
    Returns list of (threshold, article_count, top_article_at_threshold).
    """
    sorted_articles = sorted(counts.items(), key=lambda x: -x[1])
    total = sum(counts.values())
    print(f"  Total search-driven clicks: {total:,}", flush=True)
    print(f"  Total articles with clicks: {len(sorted_articles):,}", flush=True)

    results = []
    cumulative = 0
    threshold_idx = 0
    thresholds_sorted = sorted(thresholds)

    for rank, (article, clicks) in enumerate(sorted_articles, 1):
        cumulative += clicks
        coverage = cumulative / total
        while threshold_idx < len(thresholds_sorted) and coverage >= thresholds_sorted[threshold_idx]:
            results.append((thresholds_sorted[threshold_idx], rank, article, clicks))
            threshold_idx += 1
        if threshold_idx >= len(thresholds_sorted):
            break

    return results, sorted_articles, total


def print_report(results, total_articles):
    print()
    print("─" * 65)
    print(f"  {'Coverage':>10}  {'Articles needed':>16}  {'% of total':>12}  {'Top article at threshold'}")
    print("─" * 65)
    for threshold, rank, article, clicks in results:
        pct_of_total = 100 * rank / total_articles
        print(f"  {threshold*100:>9.1f}%  {rank:>16,}  {pct_of_total:>11.1f}%  {article} ({clicks:,} clicks)")
    print("─" * 65)
    print()


def plot_curve(sorted_articles, total, month, no_plot):
    try:
        import matplotlib.pyplot as plt
        import matplotlib.ticker as mticker
    except ImportError:
        print("matplotlib not installed — skipping plot (pip install matplotlib)")
        return

    n = len(sorted_articles)
    # Sample ~2000 points on a log scale for smooth curve without huge arrays
    import math
    sample_ranks = sorted(set(
        int(10 ** (i * math.log10(n) / 2000)) for i in range(2001)
    ))
    sample_ranks = [r for r in sample_ranks if 1 <= r <= n]

    cumulative = 0
    ranks = []
    coverages = []
    rank_iter = 0
    sample_set = set(sample_ranks)

    for rank, (_, clicks) in enumerate(sorted_articles, 1):
        cumulative += clicks
        if rank in sample_set:
            ranks.append(rank)
            coverages.append(100 * cumulative / total)

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(ranks, coverages, linewidth=2, color="#2563eb")

    # Mark thresholds
    colors = ["#dc2626", "#d97706", "#16a34a", "#7c3aed", "#0891b2", "#be185d"]
    cumulative = 0
    prev_threshold_rank = None
    threshold_targets = sorted(THRESHOLDS)
    t_idx = 0
    for rank, (article, clicks) in enumerate(sorted_articles, 1):
        cumulative += clicks
        coverage = cumulative / total
        while t_idx < len(threshold_targets) and coverage >= threshold_targets[t_idx]:
            t = threshold_targets[t_idx]
            ax.axvline(x=rank, color=colors[t_idx % len(colors)], linestyle="--", alpha=0.6, linewidth=1)
            ax.axhline(y=t * 100, color=colors[t_idx % len(colors)], linestyle="--", alpha=0.6, linewidth=1)
            ax.annotate(f"{t*100:.1f}%\n({rank:,} articles)",
                        xy=(rank, t * 100),
                        xytext=(rank * 1.05, t * 100 - 4),
                        fontsize=8, color=colors[t_idx % len(colors)],
                        arrowprops=dict(arrowstyle="-", color=colors[t_idx % len(colors)], alpha=0.5))
            t_idx += 1
        if t_idx >= len(threshold_targets):
            break

    ax.set_xscale("log")
    ax.set_xlabel("Number of articles (ranked by clicks, log scale)", fontsize=12)
    ax.set_ylabel("Cumulative % of search-driven clicks covered", fontsize=12)
    ax.set_title(f"Wikipedia click coverage curve — enwiki clickstream {month}", fontsize=13)
    ax.set_ylim(0, 101)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
    ax.grid(True, alpha=0.3)
    plt.tight_layout()

    out_path = os.path.join(HERE, f"click_coverage_{month}.png")
    plt.savefig(out_path, dpi=150)
    print(f"Plot saved to {out_path}")
    if not no_plot:
        plt.show()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--month", default="2025-03", help="Clickstream month YYYY-MM [default: 2025-03]")
    parser.add_argument("--no-plot", action="store_true", help="Skip interactive plot display (still saves PNG)")
    args = parser.parse_args()

    print(f"Loading clickstream for {args.month}...", flush=True)
    path = download_clickstream(args.month)

    print("Aggregating clicks...", flush=True)
    counts = load_clicks(path)

    print("Analysing coverage...", flush=True)
    results, sorted_articles, total = analyse(counts, THRESHOLDS)

    print_report(results, len(sorted_articles))
    plot_curve(sorted_articles, total, args.month, args.no_plot)


if __name__ == "__main__":
    main()
