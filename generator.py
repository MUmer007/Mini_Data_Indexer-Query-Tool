"""
generator.py — Dataset Generator for Mini Data Indexer and Query Tool

Generates a synthetic CSV dataset with realistic-looking records.
Supports controlled size, reproducibility via seed, and injected duplicates.

Usage:
    python generator.py --size 100000 --seed 7202 --output dataset_main.csv
    python generator.py --size 2000   --seed 42   --output dataset_small.csv
"""

import csv
import random
import argparse
import os
from typing import List, Dict


# ─────────────────────────────────────────────
# Domain data pools for realistic generation
# ─────────────────────────────────────────────

FIRST_NAMES: List[str] = [
    "Elif", "Can", "Zeynep", "Murat", "Ayse", "Emre", "Fatma", "Ali",
    "Sara", "Omar", "Lena", "Noah", "Mia", "Lucas", "Layla", "Hassan",
    "Nina", "Ivan", "Yuki", "Carlos", "Amina", "Diego", "Priya", "Wei",
]

LAST_NAMES: List[str] = [
    "Demir", "Kaya", "Yilmaz", "Celik", "Sahin", "Ozturk", "Arslan",
    "Smith", "Johnson", "Garcia", "Muller", "Rossi", "Tanaka", "Silva",
    "Nguyen", "Patel", "Khan", "Lee", "Andersen", "Dubois",
]

CATEGORIES: List[str] = [
    "electronics", "books", "clothing", "furniture",
    "sports", "toys", "food", "automotive",
]

REGIONS: List[str] = [
    "Marmara", "Aegean", "Central", "Black Sea",
    "Mediterranean", "Eastern", "Southeastern",
]

YEAR_RANGE = (2015, 2024)   # inclusive
SCORE_RANGE = (1, 100)      # integer score
VALUE_RANGE = (100, 9999)   # float rounded to 2 dp


# ─────────────────────────────────────────────
# Record generation
# ─────────────────────────────────────────────

def _random_name(rng: random.Random) -> str:
    """Return a random 'Firstname Lastname' string."""
    return f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}"


def generate_records(
    size: int,
    seed: int,
    duplicate_rate: float = 0.02,
) -> List[Dict]:
    """
    Generate `size` synthetic records as a list of dicts.

    Parameters
    ----------
    size           : total number of rows to produce
    seed           : random seed for full reproducibility
    duplicate_rate : fraction of rows that are intentional (name,year) dupes

    Returns
    -------
    List of dicts with keys:
        record_id, name, category, region, year, score, value

    Time complexity : O(n)
    Space complexity: O(n)

    Why seed matters
    ----------------
    Fixing the seed guarantees that every run with the same arguments produces
    the identical dataset. This is essential for fair benchmarking: you can run
    experiments on Monday and Tuesday and know the data is identical.
    """
    rng = random.Random(seed)   # isolated RNG — does not affect global state

    num_dupes = int(size * duplicate_rate)
    num_unique = size - num_dupes

    records: List[Dict] = []

    # ── Phase 1: unique records ──────────────────────────────────────────────
    # record_id starts at 1_000_001 (mirrors the spec example)
    base_id = 1_000_001

    for i in range(num_unique):
        record = {
            "record_id": base_id + i,
            "name": _random_name(rng),
            "category": rng.choice(CATEGORIES),
            "region": rng.choice(REGIONS),
            "year": rng.randint(*YEAR_RANGE),
            "score": rng.randint(*SCORE_RANGE),
            "value": round(rng.uniform(*VALUE_RANGE), 2),
        }
        records.append(record)

    # ── Phase 2: inject controlled duplicates ────────────────────────────────
    # A "duplicate" here means same (name, year) — the rule we will use in
    # Task 3.  We copy a random existing record but assign a fresh record_id
    # so the row is a new row, not a copy of the whole line.
    next_id = base_id + num_unique

    for j in range(num_dupes):
        source = rng.choice(records[:num_unique])   # pick from originals only
        dupe = {
            "record_id": next_id + j,               # unique ID for the row
            "name": source["name"],                 # ← same name
            "category": rng.choice(CATEGORIES),     # may differ
            "region": rng.choice(REGIONS),
            "year": source["year"],                 # ← same year  →  duplicate
            "score": rng.randint(*SCORE_RANGE),
            "value": round(rng.uniform(*VALUE_RANGE), 2),
        }
        records.append(dupe)

    # Shuffle so duplicates are not all at the end (more realistic)
    rng.shuffle(records)

    return records


# ─────────────────────────────────────────────
# CSV writer
# ─────────────────────────────────────────────

FIELDNAMES = ["record_id", "name", "category", "region", "year", "score", "value"]


def write_csv(records: List[Dict], output_path: str) -> None:
    """
    Write a list of record dicts to a CSV file.

    Creates any missing parent directories automatically.

    Time complexity : O(n)  — one write per record
    """
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    with open(output_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(records)

    print(f"[generator] Wrote {len(records):,} records → {output_path}")


# ─────────────────────────────────────────────
# CLI entry point
# ─────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a synthetic CSV dataset for the Mini Data Indexer."
    )
    parser.add_argument(
        "--size", type=int, default=1000,
        help="Number of records to generate (default: 1000)",
    )
    parser.add_argument(
        "--seed", type=int, default=7202,
        help="Random seed for reproducibility (default: 7202)",
    )
    parser.add_argument(
        "--output", type=str, default="dataset_small.csv",
        help="Output CSV file path (default: dataset_small.csv)",
    )
    parser.add_argument(
        "--duplicate-rate", type=float, default=0.02,
        help="Fraction of rows that are intentional (name,year) duplicates (default: 0.02)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    print(f"[generator] size={args.size:,}  seed={args.seed}  output={args.output}")
    print(f"[generator] duplicate_rate={args.duplicate_rate:.1%}")

    records = generate_records(
        size=args.size,
        seed=args.seed,
        duplicate_rate=args.duplicate_rate,
    )
    write_csv(records, args.output)


if __name__ == "__main__":
    main()
