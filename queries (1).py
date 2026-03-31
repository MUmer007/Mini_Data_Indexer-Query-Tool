"""
queries.py — Query Implementations for Mini Data Indexer and Query Tool

Each task is implemented as a clean class with clearly separated methods.
No global state. Callers pass in the record list at construction time.

Implemented in this file (built up step by step):
    Step 3  → IDIndex        : linear scan  vs  hash-map lookup
    Step 4  → FrequencyIndex : frequency counting via hash map
    Step 5  → DuplicateFinder: duplicate detection via set
    Step 6  → TopKQuery      : sorting-based  vs  heap-based top-k
    Step 7  → RangeQuery     : linear scan    vs  sorted + binary search
"""

from __future__ import annotations

import bisect
import heapq
from typing import Dict, List, Optional, Tuple

from loader import Record


# ══════════════════════════════════════════════════════════════════════════════
# STEP 3 — ID Lookup
# ══════════════════════════════════════════════════════════════════════════════

class IDIndex:
    """
    Supports exact lookup of a Record by its record_id.

    Two strategies are provided so their performance can be compared:

    1. linear_search(record_id)
       Iterates through the list from the start until a match is found.
       Time : O(n) per query  — must read up to every record
       Space: O(1) extra      — no preprocessing needed

    2. build() + lookup(record_id)
       Pre-builds a dict mapping record_id → Record.
       Build time : O(n)      — one pass to construct the dict
       Query time : O(1)      — hash table lookup
       Space      : O(n)      — dict stores one reference per record

    When to prefer which
    --------------------
    * Single one-off lookup on a small list → linear scan is fine.
    * Any scenario with multiple queries     → build the dict once,
      amortise the O(n) build cost across all queries.
    * 1000 queries on 100k records:
        linear  → 1000 × O(n) = O(100,000,000) operations
        dict    → O(n) build  + 1000 × O(1)   = O(100,001) operations
    """

    def __init__(self, records: List[Record]) -> None:
        """
        Parameters
        ----------
        records : the full list of Record objects returned by load_csv()
        """
        self._records: List[Record] = records
        # The hash-map index — populated lazily by build()
        self._index: Dict[int, Record] = {}
        self._built: bool = False

    # ── Strategy 1 : Linear Search ───────────────────────────────────────────

    def linear_search(self, record_id: int) -> Optional[Record]:
        """
        Find a record by scanning the list from the beginning.

        Time complexity : O(n) — worst case reads every record
                          O(1) best case (first element matches)
                          O(n/2) average case

        Parameters
        ----------
        record_id : the ID to search for

        Returns
        -------
        The matching Record, or None if not found.
        """
        for record in self._records:
            if record.record_id == record_id:
                return record
        return None

    # ── Strategy 2 : Hash-Map Index ──────────────────────────────────────────

    def build(self) -> None:
        """
        Pre-build the hash-map index: record_id → Record.

        Must be called once before using lookup().

        Why a dict?
        -----------
        Python's dict is backed by a hash table.
        hash(int) for CPython is the integer itself (for small ints),
        so collisions are rare and lookups are genuinely O(1) average.

        Time complexity : O(n)  — one pass over the list
        Space complexity: O(n)  — one dict entry per record

        Note on duplicates
        ------------------
        If two records share a record_id (which generator.py avoids but
        real data may have), the later one wins. We log a warning so the
        caller is aware.
        """
        self._index.clear()

        for record in self._records:
            if record.record_id in self._index:
                print(f"[IDIndex] Warning: duplicate record_id {record.record_id} "
                      f"— later record overwrites earlier one.")
            self._index[record.record_id] = record

        self._built = True
        print(f"[IDIndex] Index built: {len(self._index):,} entries.")

    def lookup(self, record_id: int) -> Optional[Record]:
        """
        Look up a record by ID using the pre-built hash map.

        Time complexity : O(1) average — single hash-table probe
                          O(n) worst case (hash collision chain, extremely rare)

        Parameters
        ----------
        record_id : the ID to look up

        Returns
        -------
        The matching Record, or None if not found.

        Raises
        ------
        RuntimeError if build() has not been called yet.
        """
        if not self._built:
            raise RuntimeError(
                "IDIndex.lookup() called before build(). Call build() first."
            )
        return self._index.get(record_id)

    # ── Convenience ──────────────────────────────────────────────────────────

    @property
    def size(self) -> int:
        """Number of records in the index."""
        return len(self._index)


# ══════════════════════════════════════════════════════════════════════════════
# STEP 4 — Frequency Counting
# ══════════════════════════════════════════════════════════════════════════════

class FrequencyIndex:
    """
    Counts how many records fall into each value of a categorical field.

    Supported fields: 'category', 'region'

    Why a hash map?
    ---------------
    We need to map each distinct value (e.g. "books") to a running count.
    A dict gives O(1) insert and O(1) lookup per key, so a single O(n)
    pass produces the full frequency table.

    Sorting by count is O(k log k) where k = number of distinct values,
    which is tiny (e.g. 8 categories, 7 regions).
    """

    SUPPORTED_FIELDS = {"category", "region"}

    def __init__(self, records: List[Record]) -> None:
        self._records: List[Record] = records

    def count(self, field: str) -> Dict[str, int]:
        """
        Build a frequency table for the given categorical field.

        Parameters
        ----------
        field : 'category' or 'region'

        Returns
        -------
        Dict mapping each distinct value → its count, sorted descending.

        Time complexity : O(n)        — single pass over records
        Space complexity: O(k)        — k = number of distinct values (tiny)
        """
        if field not in self.SUPPORTED_FIELDS:
            raise ValueError(
                f"Unsupported field '{field}'. Choose from {self.SUPPORTED_FIELDS}."
            )

        freq: Dict[str, int] = {}

        for record in self._records:
            # getattr lets us treat the field name as a variable
            value: str = getattr(record, field)
            # If key exists increment, otherwise start at 1
            freq[value] = freq.get(value, 0) + 1

        # Return sorted by count descending for convenient display
        return dict(sorted(freq.items(), key=lambda kv: kv[1], reverse=True))

    def display(self, field: str) -> None:
        """Pretty-print the frequency table with a simple bar chart."""
        freq = self.count(field)
        total = sum(freq.values())
        print(f"\nFrequency of '{field}' ({total:,} records total):")
        print(f"{'Value':<20} {'Count':>8}  {'%':>6}  Bar")
        print("─" * 60)
        for value, cnt in freq.items():
            bar = "█" * (cnt * 30 // total)
            pct = cnt / total * 100
            print(f"{value:<20} {cnt:>8,}  {pct:>5.1f}%  {bar}")


# ══════════════════════════════════════════════════════════════════════════════
# STEP 5 — Duplicate Detection
# ══════════════════════════════════════════════════════════════════════════════

class DuplicateFinder:
    """
    Detects duplicate records using the (name, year) rule.

    Duplicate rule (clearly stated, as required by the spec)
    ---------------------------------------------------------
    Two records are considered duplicates if they share the same
    (name, year) pair, regardless of other fields.

    Why a set / dict and not nested loops?
    ---------------------------------------
    Naive approach  : compare every pair → O(n²)  — 10⁰ pairs at n=100k
    Optimised approach: use a dict keyed by (name, year) → O(n)
      - One pass: for each record, check if the key is already in the dict.
      - If yes  → duplicate found, add to result list.
      - If no   → store it and move on.

    Space: O(n) for the seen-dict in the worst case (no duplicates at all).
    """

    DUPLICATE_RULE = "(name, year)"   # documented for the report

    def __init__(self, records: List[Record]) -> None:
        self._records: List[Record] = records

    def find(self) -> List[Tuple[Record, Record]]:
        """
        Return all duplicate pairs according to the (name, year) rule.

        Each tuple contains (first_seen_record, duplicate_record).
        If a key appears 3+ times, the second and third are both paired
        against the first occurrence.

        Time complexity : O(n)  — single pass, O(1) dict ops per record
        Space complexity: O(n)  — seen dict + results list

        Returns
        -------
        List of (Record, Record) tuples where both share (name, year).
        """
        # Maps (name, year) → the first Record seen with that key
        seen: Dict[Tuple[str, int], Record] = {}
        duplicates: List[Tuple[Record, Record]] = []

        for record in self._records:
            key = (record.name, record.year)

            if key in seen:
                # This record is a duplicate of seen[key]
                duplicates.append((seen[key], record))
            else:
                seen[key] = record

        return duplicates

    def summary(self) -> None:
        """Print a concise duplicate report."""
        dupes = self.find()
        print(f"\nDuplicate detection rule : {self.DUPLICATE_RULE}")
        print(f"Total records            : {len(self._records):,}")
        print(f"Duplicate pairs found    : {len(dupes):,}")
        if dupes:
            print("\nFirst 5 duplicate pairs:")
            print(f"  {'ID-A':>10}  {'ID-B':>10}  {'Name':<20}  Year")
            print("  " + "─" * 50)
            for a, b in dupes[:5]:
                print(f"  {a.record_id:>10}  {b.record_id:>10}  {a.name:<20}  {a.year}")


# ══════════════════════════════════════════════════════════════════════════════
# STEP 6 — Top-K Query
# ══════════════════════════════════════════════════════════════════════════════

class TopKQuery:
    """
    Returns the top-k records ranked by score (highest first).

    Two methods are provided for comparison:

    1. sort_based(k)
       Sort all n records by score descending, take the first k.
       Time : O(n log n)  — full sort regardless of k
       Space: O(n)        — sorted copy

    2. heap_based(k)
       Maintain a min-heap of size k while scanning.
       Time : O(n log k)  — each of n records does one heap op of cost log k
       Space: O(k)        — heap never grows beyond k elements

    Why heap is better for large n, small k
    ----------------------------------------
    If n=100,000 and k=10:
      sort : O(100,000 × log 100,000) ≈ O(1,700,000) operations
      heap : O(100,000 × log 10)      ≈ O(330,000)   operations
    The heap does ~5× less work and uses far less memory.
    The gap widens as n grows.
    """

    def __init__(self, records: List[Record]) -> None:
        self._records: List[Record] = records

    def sort_based(self, k: int) -> List[Record]:
        """
        Return top-k records by score using full sort.

        Time complexity : O(n log n)
        Space complexity: O(n)  — sorted() creates a new list

        Parameters
        ----------
        k : number of top records to return

        Returns
        -------
        List of up to k Records, highest score first.
        """
        if k <= 0:
            raise ValueError(f"k must be positive, got {k}")

        sorted_records = sorted(
            self._records,
            key=lambda r: r.score,
            reverse=True,   # highest score first
        )
        return sorted_records[:k]

    def heap_based(self, k: int) -> List[Record]:
        """
        Return top-k records by score using a min-heap of size k.

        Strategy
        --------
        We keep a min-heap of the k largest scores seen so far.
        - For each record, if its score > heap minimum → push it and
          pop the current minimum (heap stays size k).
        - At the end the heap contains the k highest-scoring records.
        - We sort the heap result for consistent output order.

        Why a MIN-heap for top-k MAX?
        heapq in Python is a min-heap. By keeping the k LARGEST in the heap
        we can quickly compare each new record against the SMALLEST of the
        k winners (heap[0]). If the new record beats that minimum it earns
        a spot, and the old minimum is evicted.

        Time complexity : O(n log k)
        Space complexity: O(k)

        Parameters
        ----------
        k : number of top records to return

        Returns
        -------
        List of up to k Records, highest score first.
        """
        if k <= 0:
            raise ValueError(f"k must be positive, got {k}")

        # Heap stores (score, record_id, record) tuples.
        # record_id as tiebreaker avoids comparing Record objects directly.
        heap: List[Tuple[int, int, Record]] = []

        for record in self._records:
            entry = (record.score, record.record_id, record)

            if len(heap) < k:
                # Heap not full yet — just push
                heapq.heappush(heap, entry)
            elif record.score > heap[0][0]:
                # New record beats the current minimum — replace it
                heapq.heapreplace(heap, entry)
            # Otherwise this record can't make the top-k, skip it

        # Extract records and sort highest-score first
        result = [entry[2] for entry in heap]
        result.sort(key=lambda r: r.score, reverse=True)
        return result


# ══════════════════════════════════════════════════════════════════════════════
# STEP 7 — Range Query
# ══════════════════════════════════════════════════════════════════════════════

class RangeQuery:
    """
    Returns all records where a numeric field falls within [low, high].

    Chosen field: value  (float, range 100–9999)

    Two methods are provided:

    1. linear_scan(low, high)
       Iterate every record, include it if low <= record.value <= high.
       Time : O(n) per query  — reads every record
       Space: O(k) result list (k = matching records)

    2. build() + binary_search(low, high)
       Pre-sort by value once, then use bisect to find the slice.
       Build time : O(n log n)  — one-time sort
       Query time : O(log n + k) — two binary searches + slice copy
       Space      : O(n)         — sorted copy of records

    Why preprocessing helps
    -----------------------
    Sorting is a one-time O(n log n) investment.
    After that, every range query costs only O(log n + k) instead of O(n).
    With many queries this amortises quickly:
      100 queries × O(n) linear    = O(100n)
      Sort once O(n log n) + 100 × O(log n + k) ≈ O(n log n) total
    For n=100k that's ~1.7M ops vs 10M ops.
    """

    def __init__(self, records: List[Record]) -> None:
        self._records: List[Record] = records
        # Populated by build()
        self._sorted_by_value: List[Record] = []
        self._sorted_values: List[float] = []   # parallel list of just the values
        self._built: bool = False

    # ── Strategy 1 : Linear Scan ─────────────────────────────────────────────

    def linear_scan(self, low: float, high: float) -> List[Record]:
        """
        Return all records where low <= record.value <= high.

        Time complexity : O(n)
        Space complexity: O(k)  — k matching records

        Parameters
        ----------
        low  : inclusive lower bound
        high : inclusive upper bound
        """
        if low > high:
            raise ValueError(f"low ({low}) must be <= high ({high})")

        return [r for r in self._records if low <= r.value <= high]

    # ── Strategy 2 : Sorted List + Binary Search ─────────────────────────────

    def build(self) -> None:
        """
        Pre-sort records by value and build a parallel list of values.

        The parallel list of floats (_sorted_values) is what bisect operates
        on. bisect cannot search by key directly, so we maintain a companion
        list that mirrors _sorted_by_value.

        Time complexity : O(n log n)  — Python's Timsort
        Space complexity: O(n)        — two lists of length n
        """
        self._sorted_by_value = sorted(self._records, key=lambda r: r.value)
        self._sorted_values   = [r.value for r in self._sorted_by_value]
        self._built = True
        print(f"[RangeQuery] Sorted index built: {len(self._sorted_by_value):,} entries.")

    def binary_search(self, low: float, high: float) -> List[Record]:
        """
        Return all records where low <= record.value <= high,
        using binary search on the pre-sorted list.

        How it works
        ------------
        bisect_left (low)  → index of first value >= low
        bisect_right(high) → index of first value >  high
        The slice [left:right] contains exactly the matching records.

        Time complexity : O(log n + k)
                          log n for two binary searches
                          k     for copying the matching slice
        Space complexity: O(k)

        Parameters
        ----------
        low  : inclusive lower bound
        high : inclusive upper bound

        Raises
        ------
        RuntimeError if build() has not been called yet.
        """
        if not self._built:
            raise RuntimeError(
                "RangeQuery.binary_search() called before build(). "
                "Call build() first."
            )
        if low > high:
            raise ValueError(f"low ({low}) must be <= high ({high})")

        # bisect_left  → first index where value >= low
        left  = bisect.bisect_left(self._sorted_values, low)
        # bisect_right → first index where value > high
        right = bisect.bisect_right(self._sorted_values, high)

        return self._sorted_by_value[left:right]
