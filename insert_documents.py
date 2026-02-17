#!/usr/bin/env python3
"""
Walk data directory and insert every document JSON file into the documents table.
Reports any failures with filename and error. Safe to re-run (INSERT OR REPLACE).
"""
import json
import sqlite3
import sys
from pathlib import Path
from itertools import islice


# Default paths; override via CLI or edit here.
DATA_DIR = Path(__file__).resolve().parent / "data"
DB_PATH = Path(__file__).resolve().parent / "documents.db"


def _serialize(value):
    """Convert list/dict to JSON string for TEXT columns; pass through scalars."""
    if value is None:
        return None
    if isinstance(value, (list, dict)):
        return json.dumps(value)
    if isinstance(value, bool):
        return 1 if value else 0
    return value


def _row_from_json(data: dict) -> tuple | None:
    """Build a single row tuple from regulations.gov document JSON. Returns None if invalid."""
    try:
        root = data.get("data") or {}
        attrs = root.get("attributes") or {}
        links = root.get("links") or {}
        doc_id = root.get("id")
        if not doc_id:
            return None
        # document_api_link is required; use self link when we have it
        api_link = (links.get("self") or "").strip() or f"https://api.regulations.gov/v4/documents/{doc_id}"
        docket_id = attrs.get("docketId")
        if docket_id is None or docket_id == "":
            # Derive from document id (e.g. CMS-2025-0304-14109 -> CMS-2025-0304)
            parts = doc_id.rsplit("-", 1)
            docket_id = parts[0] if len(parts) == 2 else doc_id

        def g(key, default=None, serialize=False):
            v = attrs.get(key, default)
            if serialize and v is not None:
                v = _serialize(v)
            elif isinstance(v, bool):
                v = 1 if v else 0
            return v

        return (
            doc_id,
            api_link,
            g("address1"),
            g("address2"),
            g("city"),
            g("stateProvinceRegion"),
            g("zip"),
            g("country"),
            attrs.get("agencyId"),
            docket_id,
            attrs.get("documentType"),
            g("title"),
            g("subtype"),
            g("objectId"),
            g("pageCount"),
            g("paperLength"),
            g("paperWidth"),
            g("docAbstract"),
            g("subject"),
            g("startEndPage"),
            g("authors", serialize=True),
            g("additionalRins", serialize=True),
            g("cfrPart"),
            g("frDocNum"),
            g("frVolNum"),
            g("comment"),
            g("category"),
            g("firstName"),
            g("lastName"),
            g("email"),
            g("phone"),
            g("fax"),
            g("organization"),
            g("govAgency"),
            g("govAgencyType"),
            g("submitterRep"),
            g("authorDate"),
            g("commentStartDate"),
            g("commentEndDate"),
            g("effectiveDate"),
            g("implementationDate"),
            g("modifyDate"),
            g("postedDate"),
            g("postmarkDate"),
            g("receiveDate"),
            g("allowLateComments"),
            g("openForComment"),
            g("withdrawn"),
            g("withinCommentPeriod"),
            g("reasonWithdrawn"),
            g("restrictReason"),
            g("restrictReasonType"),
            g("field1"),
            g("field2"),
            g("regWriterInstruction"),
            g("legacyId"),
            g("originalDocumentId"),
            g("trackingNbr"),
            g("exhibitLocation"),
            g("exhibitType"),
            g("media", serialize=True),
            g("ombApproval"),
            g("sourceCitation"),
            g("topics", serialize=True),
        )
    except (KeyError, TypeError, AttributeError):
        return None


INSERT_SQL = """
INSERT OR REPLACE INTO documents (
    document_id, document_api_link,
    address1, address2, city, state_province_region, postal_code, country,
    agency_id, docket_id,
    document_type, document_title, subtype, object_id, page_count, paper_length, paper_width,
    doc_abstract, subject, start_end_page, authors, additional_rins,
    cfr_part, fr_doc_num, fr_vol_num,
    comment, comment_category,
    first_name, last_name, email, phone, fax,
    submitter_org, submitter_gov_agency, submitter_gov_agency_type, submitter_rep,
    author_date, comment_start_date, comment_end_date, effective_date, implementation_date,
    modify_date, posted_date, postmark_date, receive_date,
    is_late_comment, is_open_for_comment, is_withdrawn, within_comment_period,
    reason_withdrawn, restriction_reason, restriction_reason_type,
    flex_field1, flex_field2, reg_writer_instruction,
    legacy_id, original_document_id, tracking_nbr, exhibit_location, exhibit_type,
    media, omb_approval, source_citation,
    topics
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


def collect_document_paths(data_dir: Path):
    """Yield path to each document JSON under data_dir (data/**/documents/*.json)."""
    for path in data_dir.rglob("*.json"):
        if path.name.endswith(".json") and "documents" in path.parts:
            yield path


def run(data_dir: Path, db_path: Path, limit: int | None = None) -> tuple[int, int, list[tuple[str, str]]]:
    """
    Insert all document JSONs under data_dir into db_path.
    Returns (inserted_count, failed_count, list of (filename, error_message)).
    """
    failures: list[tuple[str, str]] = []
    inserted = 0
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    BATCH_SIZE = 5000
    batch: list[tuple] = []
    try:
        paths = collect_document_paths(data_dir)
        if limit is not None:
            paths = islice(paths, limit)
        for path in paths:
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                row = _row_from_json(data)
                if row is None:
                    failures.append((str(path), "Could not build row from JSON"))
                    continue
                batch.append(row)
                if len(batch) >= BATCH_SIZE:
                    conn.executemany(INSERT_SQL, batch)
                    inserted += len(batch)
                    conn.commit()
                    batch = []
            except json.JSONDecodeError as e:
                failures.append((str(path), f"JSON decode: {e}"))
            except sqlite3.Error as e:
                failures.append((str(path), f"SQLite: {e}"))
            except OSError as e:
                failures.append((str(path), f"IO: {e}"))
            except Exception as e:
                failures.append((str(path), f"{type(e).__name__}: {e}"))
        if batch:
            conn.executemany(INSERT_SQL, batch)
            inserted += len(batch)
        conn.commit()
    finally:
        conn.close()
    return inserted, len(failures), failures


def main():
    import argparse
    p = argparse.ArgumentParser(description="Insert document JSON files into documents table.")
    p.add_argument("--data-dir", type=Path, default=DATA_DIR, help="Root data directory")
    p.add_argument("--db", type=Path, default=DB_PATH, help="SQLite database path")
    p.add_argument("--limit", type=int, default=None, help="Max number of files to process (for testing)")
    args = p.parse_args()
    if not args.data_dir.is_dir():
        print(f"Data directory not found: {args.data_dir}", file=sys.stderr)
        sys.exit(1)
    if not args.db.exists():
        print(f"Database not found: {args.db}. Create it from documents_schema.sql first.", file=sys.stderr)
        sys.exit(1)
    print(f"Scanning {args.data_dir} ...")
    inserted, failed_count, failures = run(args.data_dir, args.db, limit=args.limit)
    print(f"Inserted: {inserted}, Failed: {failed_count}")
    if failures:
        print("\nFailures (filename, error):", file=sys.stderr)
        for path, err in failures:
            print(f"  {path}\n    {err}", file=sys.stderr)
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
