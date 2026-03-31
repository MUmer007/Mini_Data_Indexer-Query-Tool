"""
loader.py — Data Loading System for Mini Data Indexer and Query Tool

Defines the Record data class and provides a clean CSV loader.
This module is the single entry point for raw data — every other
module receives Record objects, never raw dicts or strings.

Design decisions
----------------
* __slots__ on Record  → avoids per-object __dict__, saves ~35% memory at scale
* Typed fields         → catches bad data early, makes queries safer
* Loader returns list  → simple, indexable, iterable; callers build
                         their own indices on top (separation of concerns)
"""

import csv
from typing import List


# ─────────────────────────────────────────────
# The core data model
# ─────────────────────────────────────────────

class Record:
    """
    Represents one row from the dataset.

    Using __slots__ instead of a plain dict or a dataclass without slots
    gives us:
      - Attribute access instead of dict key lookups  (cleaner code)
      - No per-instance __dict__                      (lower memory at 100k+ rows)
      - Explicit field contract                       (typos raise AttributeError)

    Fields
    ------
    record_id : int    — unique (mostly) row identifier
    name      : str    — person / entity name
    category  : str    — product category
    region    : str    — geographic region
    year      : int    — year of the record
    score     : int    — integer score 1–100
    value     : float  — numeric value 100–9999
    """

    __slots__ = ("record_id", "name", "category", "region", "year", "score", "value")

    def __init__(
        self,
        record_id: int,
        name: str,
        category: str,
        region: str,
        year: int,
        score: int,
        value: float,
    ) -> None:
        self.record_id = record_id
        self.name      = name
        self.category  = category
        self.region    = region
        self.year      = year
        self.score     = score
        self.value     = value

    # ── Representation ────────────────────────────────────────────────────────

    def __repr__(self) -> str:
        """Compact single-line representation — useful in the REPL and logs."""
        return (
            f"Record(id={self.record_id}, name={self.name!r}, "
            f"category={self.category!r}, region={self.region!r}, "
            f"year={self.year}, score={self.score}, value={self.value})"
        )

    def to_dict(self) -> dict:
        """
        Convert to a plain dict.
        Useful for CSV writing, JSON serialisation, or display.
        """
        return {
            "record_id": self.record_id,
            "name":      self.name,
            "category":  self.category,
            "region":    self.region,
            "year":      self.year,
            "score":     self.score,
            "value":     self.value,
        }


# ─────────────────────────────────────────────
# CSV → Record conversion helpers
# ─────────────────────────────────────────────

def _parse_record(row: dict) -> Record:
    """
    Convert one raw CSV row (dict of strings) into a typed Record.

    All type-casting happens here so the rest of the codebase
    can safely assume correct types.

    Raises
    ------
    ValueError  if a required field is missing or cannot be cast.
    """
    try:
        return Record(
            record_id = int(row["record_id"]),
            name      = row["name"].strip(),
            category  = row["category"].strip().lower(),
            region    = row["region"].strip(),
            year      = int(row["year"]),
            score     = int(row["score"]),
            value     = float(row["value"]),
        )
    except (KeyError, ValueError) as exc:
        raise ValueError(f"Bad row {row!r}: {exc}") from exc


# ─────────────────────────────────────────────
# Public loader function
# ─────────────────────────────────────────────

def load_csv(filepath: str) -> List[Record]:
    """
    Load a CSV file and return a list of Record objects.

    Parameters
    ----------
    filepath : path to the CSV file (relative or absolute)

    Returns
    -------
    List[Record]  — one Record per data row, in file order

    Time complexity : O(n)  — single pass through the file
    Space complexity: O(n)  — all records held in memory

    Why a list?
    -----------
    A plain list is the most flexible container to hand off to callers.
    Each query module (queries.py) builds its own index a hash map,
    sorted list, or heap  suited to its specific operation.
    Returning a list keeps this module responsible for ONE thing only:
    reading and parsing the file.
    """
    records: List[Record] = []
    skipped = 0

    with open(filepath, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)

        for raw_row in reader:
            try:
                records.append(_parse_record(raw_row))
            except ValueError as exc:
                # Log bad rows but keep loading — resilience over crash
                print(f"[loader] Skipping invalid row: {exc}")
                skipped += 1

    print(f"[loader] Loaded {len(records):,} records from '{filepath}'"
          + (f"  ({skipped} skipped)" if skipped else ""))

    return records
