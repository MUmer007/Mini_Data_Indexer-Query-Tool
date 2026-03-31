"""
main.py — Command-Line Interface for Mini Data Indexer and Query Tool

Provides a single entry point for all project features.

Usage examples:
    python main.py --build dataset_small.csv
    python main.py --build dataset_small.csv --lookup 1000123
    python main.py --build dataset_small.csv --freq category
    python main.py --build dataset_small.csv --duplicates
    python main.py --build dataset_small.csv --topk 10
    python main.py --build dataset_small.csv --range 1000 5000
"""

import argparse
import sys

from loader import load_csv, Record
from queries import IDIndex, FrequencyIndex, DuplicateFinder, TopKQuery, RangeQuery


# ─────────────────────────────────────────────
# Pretty-print helpers
# ─────────────────────────────────────────────

def _header(title: str) -> None:
    """Print a formatted section header."""
    print(f"\n{'─' * 55}")
    print(f"  {title}")
    print(f"{'─' * 55}")


def _print_record(r: Record) -> None:
    """Print one record in a readable single-line format."""
    print(
        f"  id={r.record_id}  name={r.name!r:<22}  "
        f"cat={r.category:<12}  region={r.region:<14}  "
        f"year={r.year}  score={r.score:>3}  value={r.value:>8.2f}"
    )


# ─────────────────────────────────────────────
# Command handlers
# ─────────────────────────────────────────────

def cmd_lookup(records: list[Record], record_id: int) -> None:
    """
    Handle --lookup: find a single record by ID.
    Shows both strategies and their result.
    """
    _header(f"ID Lookup — record_id = {record_id}")

    idx = IDIndex(records)

    # Linear search
    result_linear = idx.linear_search(record_id)
    print(f"\n  [linear search]")
    if result_linear:
        _print_record(result_linear)
    else:
        print(f"  Not found.")

    # Hash-map lookup
    idx.build()
    result_hash = idx.lookup(record_id)
    print(f"\n  [hash lookup]")
    if result_hash:
        _print_record(result_hash)
    else:
        print(f"  Not found.")


def cmd_freq(records: list[Record], field: str) -> None:
    """
    Handle --freq: print frequency table for a categorical field.
    """
    _header(f"Frequency Count — field = '{field}'")
    fi = FrequencyIndex(records)
    fi.display(field)


def cmd_duplicates(records: list[Record]) -> None:
    """
    Handle --duplicates: detect and display (name, year) duplicates.
    """
    _header("Duplicate Detection — rule: (name, year)")
    df = DuplicateFinder(records)
    df.summary()


def cmd_topk(records: list[Record], k: int) -> None:
    """
    Handle --topk: show top-k records by score using both methods.
    """
    _header(f"Top-{k} Records by Score")

    tq = TopKQuery(records)

    print(f"\n  [sort-based] O(n log n)")
    sort_results = tq.sort_based(k)
    for rank, r in enumerate(sort_results, 1):
        print(f"  #{rank:<3} score={r.score:>3}  ", end="")
        print(f"id={r.record_id}  name={r.name!r}")

    print(f"\n  [heap-based] O(n log k)  — same results, faster for large n")
    heap_results = tq.heap_based(k)
    for rank, r in enumerate(heap_results, 1):
        print(f"  #{rank:<3} score={r.score:>3}  ", end="")
        print(f"id={r.record_id}  name={r.name!r}")


def cmd_range(records: list[Record], low: float, high: float) -> None:
    """
    Handle --range: return records where value is in [low, high].
    """
    _header(f"Range Query — value in [{low}, {high}]")

    rq = RangeQuery(records)

    # Linear scan
    linear_results = rq.linear_scan(low, high)
    print(f"\n  [linear scan]  found {len(linear_results):,} records")
    for r in linear_results[:5]:
        _print_record(r)
    if len(linear_results) > 5:
        print(f"  ... and {len(linear_results) - 5:,} more.")

    # Binary search
    rq.build()
    binary_results = rq.binary_search(low, high)
    print(f"\n  [binary search]  found {len(binary_results):,} records")
    for r in binary_results[:5]:
        _print_record(r)
    if len(binary_results) > 5:
        print(f"  ... and {len(binary_results) - 5:,} more.")


# ─────────────────────────────────────────────
# Argument parser
# ─────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="main.py",
        description="Mini Data Indexer and Query Tool",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog="""
Examples:
  python main.py --build dataset_small.csv --lookup 1000123
  python main.py --build dataset_small.csv --freq category
  python main.py --build dataset_small.csv --freq region
  python main.py --build dataset_small.csv --duplicates
  python main.py --build dataset_small.csv --topk 10
  python main.py --build dataset_small.csv --range 1000 5000
        """,
    )

    parser.add_argument(
        "--build", metavar="CSV_FILE", required=True,
        help="Path to the CSV dataset to load (required for all commands)",
    )
    parser.add_argument(
        "--lookup", metavar="RECORD_ID", type=int,
        help="Look up a record by its record_id",
    )
    parser.add_argument(
        "--freq", metavar="FIELD",
        choices=["category", "region"],
        help="Show frequency count for 'category' or 'region'",
    )
    parser.add_argument(
        "--duplicates", action="store_true",
        help="Find and report duplicate (name, year) pairs",
    )
    parser.add_argument(
        "--topk", metavar="K", type=int,
        help="Show top-K records by score",
    )
    parser.add_argument(
        "--range", metavar=("LOW", "HIGH"), nargs=2, type=float,
        help="Return records where value is in [LOW, HIGH]",
    )

    return parser


# ─────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────

def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    # Check that at least one query command was given
    commands_given = any([
        args.lookup is not None,
        args.freq is not None,
        args.duplicates,
        args.topk is not None,
        args.range is not None,
    ])

    if not commands_given:
        print("[main] No query command given. Use --help to see available options.")
        parser.print_help()
        sys.exit(0)

    # ── Load data ────────────────────────────────────────────────────────────
    print(f"\nLoading dataset: {args.build}")
    records = load_csv(args.build)

    if not records:
        print("[main] No records loaded. Check your CSV file.")
        sys.exit(1)

    # ── Dispatch commands ────────────────────────────────────────────────────
    if args.lookup is not None:
        cmd_lookup(records, args.lookup)

    if args.freq is not None:
        cmd_freq(records, args.freq)

    if args.duplicates:
        cmd_duplicates(records)

    if args.topk is not None:
        cmd_topk(records, args.topk)

    if args.range is not None:
        cmd_range(records, args.range[0], args.range[1])

    print()  # trailing newline for clean terminal output


if __name__ == "__main__":
    main()
