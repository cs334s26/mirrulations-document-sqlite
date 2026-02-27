#!/usr/bin/env python3
"""
List dockets that have Rule documents mentioning 42 CFR Part 412,
sorted by modify_date (newest first).

Prints docket_id, document title, and modify_date.
"""
import sqlite3
import sys
from pathlib import Path

from cfr_part_normalization import extract_parts_for_title

DB_PATH = Path(__file__).resolve().parent / "documents.db"


def main() -> None:
    if not DB_PATH.exists():
        print(f"Database not found: {DB_PATH}", file=sys.stderr)
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    cursor = conn.execute(
        """
        SELECT docket_id, document_title, modify_date, cfr_part
        FROM documents
        WHERE document_type = 'Rule'
          AND cfr_part LIKE '%42 CFR%'
        """,
    )

    # Collect rows that include part 412; use modify_date for sort
    # Deduplicate by docket_id: keep the row with the earliest modify_date per docket
    docket_best: dict[str, tuple[str, str | None]] = {}  # docket_id -> (title, modify_date)
    for row in cursor:
        part_numbers = extract_parts_for_title(row["cfr_part"], 42)
        if "412" not in part_numbers:
            continue

        docket_id = row["docket_id"]
        title = row["document_title"] or "(no title)"
        modify_date = row["modify_date"]

        if docket_id not in docket_best:
            docket_best[docket_id] = (title, modify_date)
        else:
            existing_date = docket_best[docket_id][1]
            # Keep the one with the earlier date (empty string sorts after valid dates)
            if modify_date and (not existing_date or modify_date < existing_date):
                docket_best[docket_id] = (title, modify_date)

    conn.close()

    # Build sorted list: one row per docket
    rows = [(docket_id, title, modify_date) for docket_id, (title, modify_date) in docket_best.items()]

    # Sort by modify_date descending (newest first), nulls last
    def sort_key(item: tuple) -> tuple:
        docket_id, title, modify_date = item
        return (modify_date or "", docket_id)

    rows.sort(key=sort_key, reverse=True)

    print(f"Dockets with Rules mentioning 42 CFR Part 412 ({len(rows)} dockets):\n")
    print(f"{'Docket ID':<25} {'Modify Date':<22}  Title")
    print("-" * 100)
    for docket_id, title, modify_date in rows:
        date_str = modify_date if modify_date else "(null)"
        print(f"{docket_id:<25} {date_str:<22}  {title}")


if __name__ == "__main__":
    main()
