"""
experiments.py — Benchmarking Suite for Mini Data Indexer and Query Tool

Runs timed experiments comparing naive vs optimised query strategies.
Results are saved to results/timings.csv and printed to the console.

Usage:
    python experiments.py --dataset dataset_small.csv
    python experiments.py --dataset dataset_main.csv
"""

import argparse
import csv
import os
import random
import time
from typing import Callable, Any

from loader import load_csv, Record
from queries import IDIndex, FrequencyIndex, DuplicateFinder, TopKQuery, RangeQuery


# ─────────────────────────────────────────────
# Timing helper
# ─────────────────────────────────────────────

def measure(label: str, fn: Callable[[], Any]) -> tuple[str, float, Any]:
    """
    Run fn(), measure elapsed time in milliseconds, return (label, ms, result).

    Parameters
    ----------
    label : human-readable name for this experiment
    fn    : zero-argument callable to time

    Returns
    -------
    (label, elapsed_ms, result)
    """
    start = time.perf_counter()
    result = fn()
    elapsed_ms = (time.perf_counter() - start) * 1_000
    print(f"  [{label}]  {elapsed_ms:>10.3f} ms")
    return label, elapsed_ms, result


# ─────────────────────────────────────────────
# Individual experiments
# ─────────────────────────────────────────────

def experiment_id_lookup(records: list[Record], num_queries: int = 1000) -> list[tuple]:
    """
    Step 8a — Compare linear search vs hash-map lookup over 1000 random IDs.

    Why 1000 queries?
    -----------------
    A single lookup is too fast to measure reliably (< 1 µs).
    Running 1000 queries gives a stable, meaningful time difference.

    Expected outcome: dict lookup should be ~100–500× faster on large datasets.
    """
    print(f"\n[Experiment] ID Lookup — {num_queries} random queries")

    idx = IDIndex(records)
    idx.build()

    # Pick random IDs that actually exist (mix of hits)
    all_ids = [r.record_id for r in records]
    rng = random.Random(42)
    query_ids = [rng.choice(all_ids) for _ in range(num_queries)]

    rows = []

    # Linear search — run all queries sequentially
    _, ms_linear, _ = measure(
        "linear_search × 1000",
        lambda: [idx.linear_search(qid) for qid in query_ids],
    )
    rows.append(("id_lookup", "linear_search", num_queries, ms_linear))

    # Hash-map lookup — run all queries sequentially
    _, ms_hash, _ = measure(
        "hash_lookup  × 1000",
        lambda: [idx.lookup(qid) for qid in query_ids],
    )
    rows.append(("id_lookup", "hash_lookup", num_queries, ms_hash))

    speedup = ms_linear / ms_hash if ms_hash > 0 else float("inf")
    print(f"  → Speedup: {speedup:.1f}× (hash vs linear)")

    return rows


def experiment_top_k(records: list[Record]) -> list[tuple]:
    """
    Step 8b — Compare sort-based vs heap-based top-k for k = 10 and k = 100.

    Expected outcome: heap is faster, especially at small k relative to n.
    """
    print("\n[Experiment] Top-K Query")

    tq = TopKQuery(records)
    rows = []

    for k in (10, 100):
        print(f"  k = {k}")
        _, ms_sort, _ = measure(f"sort_based  k={k}", lambda k=k: tq.sort_based(k))
        rows.append(("top_k", f"sort_based_k{k}", k, ms_sort))

        _, ms_heap, _ = measure(f"heap_based  k={k}", lambda k=k: tq.heap_based(k))
        rows.append(("top_k", f"heap_based_k{k}", k, ms_heap))

        speedup = ms_sort / ms_heap if ms_heap > 0 else float("inf")
        print(f"    → Speedup: {speedup:.2f}× (heap vs sort)")

    return rows


def experiment_range_query(records: list[Record], num_queries: int = 100) -> list[tuple]:
    """
    Step 8c — Compare linear scan vs binary search for range queries.

    We run num_queries random ranges of width ~20% of value range (100–9999).
    Expected outcome: binary search is faster, especially with many queries.
    """
    print(f"\n[Experiment] Range Query — {num_queries} random ranges")

    rq = RangeQuery(records)
    rq.build()

    rng = random.Random(99)
    ranges = []
    for _ in range(num_queries):
        lo = round(rng.uniform(100, 7000), 2)
        hi = round(lo + rng.uniform(500, 2000), 2)
        ranges.append((lo, hi))

    rows = []

    _, ms_linear, _ = measure(
        f"linear_scan × {num_queries}",
        lambda: [rq.linear_scan(lo, hi) for lo, hi in ranges],
    )
    rows.append(("range_query", "linear_scan", num_queries, ms_linear))

    _, ms_binary, _ = measure(
        f"binary_search × {num_queries}",
        lambda: [rq.binary_search(lo, hi) for lo, hi in ranges],
    )
    rows.append(("range_query", "binary_search", num_queries, ms_binary))

    speedup = ms_linear / ms_binary if ms_binary > 0 else float("inf")
    print(f"  → Speedup: {speedup:.2f}× (binary vs linear)")

    return rows


def experiment_frequency(records: list[Record]) -> list[tuple]:
    """
    Step 8d — Time the frequency count for category and region.

    This is O(n) with no comparison method — just records the raw time.
    """
    print("\n[Experiment] Frequency Count")

    fi = FrequencyIndex(records)
    rows = []

    for field in ("category", "region"):
        _, ms, _ = measure(f"freq_count field={field}", lambda f=field: fi.count(f))
        rows.append(("frequency", f"count_{field}", len(records), ms))

    return rows


def experiment_duplicates(records: list[Record]) -> list[tuple]:
    """
    Step 8e — Time the O(n) duplicate detection pass.
    """
    print("\n[Experiment] Duplicate Detection")

    df = DuplicateFinder(records)
    _, ms, dupes = measure("find_duplicates", df.find)
    print(f"  → Found {len(dupes):,} duplicate pairs")

    return [("duplicates", "hash_find", len(records), ms)]


# ─────────────────────────────────────────────
# Results writer
# ─────────────────────────────────────────────

def save_results(rows: list[tuple], output_path: str) -> None:
    """
    Save all timing rows to a CSV file.

    Columns: experiment, method, n_or_k, elapsed_ms

    Parameters
    ----------
    rows        : list of (experiment, method, n_or_k, elapsed_ms) tuples
    output_path : path for the output CSV
    """
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    with open(output_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["experiment", "method", "n_or_k", "elapsed_ms"])
        writer.writerows(rows)

    print(f"\n[experiments] Results saved → {output_path}")


# ─────────────────────────────────────────────
# CLI entry point
# ─────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run benchmarks for the Mini Data Indexer."
    )
    parser.add_argument(
        "--dataset", type=str, default="dataset_small.csv",
        help="Path to the CSV dataset (default: dataset_small.csv)",
    )
    parser.add_argument(
        "--output", type=str, default="results/timings.csv",
        help="Output path for timing results (default: results/timings.csv)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    print(f"{'='*60}")
    print(f"  Mini Data Indexer — Benchmarks")
    print(f"  Dataset : {args.dataset}")
    print(f"{'='*60}")

    records = load_csv(args.dataset)
    n = len(records)
    print(f"\n  Loaded {n:,} records.\n")

    all_rows = []
    all_rows.extend(experiment_id_lookup(records))
    all_rows.extend(experiment_top_k(records))
    all_rows.extend(experiment_range_query(records))
    all_rows.extend(experiment_frequency(records))
    all_rows.extend(experiment_duplicates(records))

    save_results(all_rows, args.output)

    print(f"\n{'='*60}")
    print("  All experiments complete.")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
