#!/usr/bin/env python3
"""
Count how often different parts of 42 CFR are mentioned in Rule documents.

Queries the documents table for Rules with cfr_part containing "42 CFR",
extracts part numbers from the cfr_part text (handling variations like
"42 CFR Part 405" or "42 CFR Parts 405, 417, 422, and 460"), and reports
counts in decreasing order with per-agency breakdown (e.g. CMS(150) CDC(32)).
"""
import re
import sqlite3
import sys
from pathlib import Path
from collections import Counter

DB_PATH = Path(__file__).resolve().parent / "documents.db"

# Match "Part" or "Parts" followed by numbers, commas, "and", spaces, decimals.
# We split by "42 CFR" first so we only capture parts in the 42 CFR context.
PART_PATTERN = re.compile(r"\bPart(?:s)?\s+([\d\s, and.]+)", re.IGNORECASE)
NUMBER_PATTERN = re.compile(r"\d+(?:\.\d+)?")


def extract_part_numbers(cfr_part: str | None) -> list[str]:
    """
    Extract 42 CFR part numbers from a cfr_part string.

    Handles formats like:
    - "42 CFR Part 405"
    - "42 CFR Parts 405, 417, 422, and 460"
    - "42 CFR Part 405; 42 CFR Part 422"
    - Other human-written variations
    """
    if not cfr_part or not isinstance(cfr_part, str):
        return []

    parts: list[str] = []
    # Split by "42 CFR" so we only capture part numbers in the 42 CFR context,
    # avoiding false positives from "42" in "42 CFR" or other CFR titles.
    segments = re.split(r"42\s+CFR", cfr_part, flags=re.IGNORECASE)

    for segment in segments[1:]:  # Skip the part before first "42 CFR"
        for match in PART_PATTERN.finditer(segment):
            group = match.group(1)
            # Extract all numbers from the captured group (e.g. "405, 417, 422, and 460")
            numbers = NUMBER_PATTERN.findall(group)
            parts.extend(numbers)

    return parts


def main() -> None:
    if not DB_PATH.exists():
        print(f"Database not found: {DB_PATH}", file=sys.stderr)
        print("Create it from documents_schema.sql and run insert_documents.py first.", file=sys.stderr)
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Use = for SQL equality (not ==). LIKE for pattern match.
    cursor = conn.execute(
        """
        SELECT agency_id, cfr_part
        FROM documents
        WHERE document_type = 'Rule'
          AND cfr_part LIKE '%42 CFR%'
        """,
    )

    # part -> Counter of agency_id (count per agency)
    part_agencies: dict[str, Counter[str]] = {}
    for row in cursor:
        agency_id = row["agency_id"] or "unknown"
        cfr_part = row["cfr_part"]
        part_numbers = extract_part_numbers(cfr_part)
        for p in part_numbers:
            if p not in part_agencies:
                part_agencies[p] = Counter()
            part_agencies[p][agency_id] += 1

    conn.close()

    if not part_agencies:
        print("No 42 CFR part numbers found in Rule documents.")
        return

    # Sort by total count descending, then by part number for ties
    sorted_parts = sorted(
        part_agencies.items(),
        key=lambda x: (-sum(x[1].values()), x[0]),
    )

    print("42 CFR Part mentions in Rule documents (by decreasing count):\n")
    print(f"{'Part':<12} {'Count':>8}  Agencies")
    print("-" * 60)
    for part, agency_counts in sorted_parts:
        total = sum(agency_counts.values())
        # Format agencies as "CMS(150) CDC(32)" sorted by count descending
        agency_str = " ".join(
            f"{ag}({n})" for ag, n in agency_counts.most_common()
        )
        print(f"{part:<12} {total:>8}  {agency_str}")


if __name__ == "__main__":
    main()
