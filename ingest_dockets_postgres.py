#!/usr/bin/env python3
"""
Fetch one or more dockets from regulations.gov and upsert documents into Postgres.

This script is purpose-built for targeted ingestion (specific docket IDs), unlike
the SQLite bulk loaders that expect a full local mirror of JSON files.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any

import requests

from cfr_part_normalization import normalize_cfr_part, normalize_cfr_part_json

try:
    import dotenv

    dotenv.load_dotenv()
except ImportError:
    pass

try:
    import psycopg  # psycopg v3
except ImportError:
    psycopg = None

if psycopg is None:
    try:
        import psycopg2 as psycopg  # psycopg2 fallback
    except ImportError:
        psycopg = None

BASE_URL = "https://api.regulations.gov/v4"
DEFAULT_SCHEMA = Path(__file__).resolve().parent / "documents_schema_postgres.sql"

COLUMNS = [
    "document_id",
    "document_api_link",
    "address1",
    "address2",
    "city",
    "state_province_region",
    "postal_code",
    "country",
    "agency_id",
    "docket_id",
    "document_type",
    "document_title",
    "subtype",
    "object_id",
    "page_count",
    "paper_length",
    "paper_width",
    "doc_abstract",
    "subject",
    "start_end_page",
    "authors",
    "additional_rins",
    "cfr_part",
    "cfr_part_normalized",
    "cfr_part_parse_status",
    "fr_doc_num",
    "fr_vol_num",
    "comment",
    "comment_category",
    "first_name",
    "last_name",
    "email",
    "phone",
    "fax",
    "submitter_org",
    "submitter_gov_agency",
    "submitter_gov_agency_type",
    "submitter_rep",
    "author_date",
    "comment_start_date",
    "comment_end_date",
    "effective_date",
    "implementation_date",
    "modify_date",
    "posted_date",
    "postmark_date",
    "receive_date",
    "is_late_comment",
    "is_open_for_comment",
    "is_withdrawn",
    "within_comment_period",
    "reason_withdrawn",
    "restriction_reason",
    "restriction_reason_type",
    "flex_field1",
    "flex_field2",
    "reg_writer_instruction",
    "legacy_id",
    "original_document_id",
    "tracking_nbr",
    "exhibit_location",
    "exhibit_type",
    "media",
    "omb_approval",
    "source_citation",
    "topics",
]
JSONB_COLUMNS = {"authors", "additional_rins", "cfr_part_normalized", "media", "topics"}

UPSERT_SQL = f"""
INSERT INTO documents ({", ".join(COLUMNS)})
VALUES ({", ".join("%s::jsonb" if c in JSONB_COLUMNS else "%s" for c in COLUMNS)})
ON CONFLICT (document_id) DO UPDATE
SET {", ".join(f"{c}=EXCLUDED.{c}" for c in COLUMNS if c != "document_id")}
"""


def _to_json(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)


def _to_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "t", "1", "yes", "y"}:
            return True
        if lowered in {"false", "f", "0", "no", "n"}:
            return False
    return None


def _as_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def make_request(url: str, api_key: str, params: dict[str, Any]) -> requests.Response:
    """
    GET with basic retry + rate-limit handling.
    Waits on 429 and on exhausted X-RateLimit-Remaining.
    """
    headers = {"X-Api-Key": api_key}
    while True:
        resp = requests.get(url, headers=headers, params=params, timeout=60)
        limit = resp.headers.get("X-RateLimit-Limit", "?")
        remaining = _as_int(resp.headers.get("X-RateLimit-Remaining"))
        print(f"    API {resp.status_code} | rate limit {remaining}/{limit}")

        if resp.status_code == 429:
            retry_after = _as_int(resp.headers.get("Retry-After")) or 3600
            print(f"    Rate limit exceeded (429). Waiting {retry_after}s...")
            time.sleep(max(1, retry_after))
            continue

        if remaining is not None and remaining < 1:
            # Prefer reset epoch if available.
            reset_epoch = _as_int(resp.headers.get("X-RateLimit-Reset"))
            if reset_epoch:
                wait = max(1, reset_epoch - int(time.time()))
            else:
                wait = 3600
            print(f"    No requests remaining. Waiting {wait}s...")
            time.sleep(wait)
            continue

        if resp.status_code in {500, 502, 503, 504}:
            print(f"    Upstream error {resp.status_code}. Retrying in 5s...")
            time.sleep(5)
            continue

        return resp


def fetch_docket_documents(docket_id: str, api_key: str, page_size: int = 250) -> list[dict[str, Any]]:
    """Fetch all documents for one docket via pagination."""
    page = 1
    docs: list[dict[str, Any]] = []
    while True:
        params = {
            "filter[docketId]": docket_id,
            "page[number]": page,
            "page[size]": page_size,
        }
        resp = make_request(f"{BASE_URL}/documents", api_key=api_key, params=params)
        if resp.status_code != 200:
            raise RuntimeError(f"{docket_id}: API returned {resp.status_code}: {resp.text[:300]}")

        payload = resp.json()
        items = payload.get("data", [])
        total = payload.get("meta", {}).get("totalElements")
        docs.extend(items)
        print(f"    Page {page}: fetched {len(docs)}/{total if total is not None else '?'}")

        if not items or len(items) < page_size:
            break
        page += 1
    return docs


def _row_from_document(doc: dict[str, Any]) -> tuple[tuple[Any, ...], str, str | None] | None:
    root = doc or {}
    attrs = root.get("attributes") or {}
    links = root.get("links") or {}
    doc_id = root.get("id")
    if not doc_id:
        return None

    api_link = (links.get("self") or "").strip() or f"{BASE_URL}/documents/{doc_id}"
    docket_id = attrs.get("docketId")
    if docket_id is None or docket_id == "":
        parts = doc_id.rsplit("-", 1)
        docket_id = parts[0] if len(parts) == 2 else doc_id

    cfr_part = attrs.get("cfrPart")
    normalized = normalize_cfr_part(cfr_part)

    record: dict[str, Any] = {
        "document_id": doc_id,
        "document_api_link": api_link,
        "address1": attrs.get("address1"),
        "address2": attrs.get("address2"),
        "city": attrs.get("city"),
        "state_province_region": attrs.get("stateProvinceRegion"),
        "postal_code": attrs.get("zip"),
        "country": attrs.get("country"),
        "agency_id": attrs.get("agencyId") or "",
        "docket_id": docket_id,
        "document_type": attrs.get("documentType") or "",
        "document_title": attrs.get("title"),
        "subtype": attrs.get("subtype"),
        "object_id": attrs.get("objectId"),
        "page_count": attrs.get("pageCount"),
        "paper_length": attrs.get("paperLength"),
        "paper_width": attrs.get("paperWidth"),
        "doc_abstract": attrs.get("docAbstract"),
        "subject": attrs.get("subject"),
        "start_end_page": attrs.get("startEndPage"),
        "authors": _to_json(attrs.get("authors")),
        "additional_rins": _to_json(attrs.get("additionalRins")),
        "cfr_part": cfr_part,
        "cfr_part_normalized": normalize_cfr_part_json(cfr_part),
        "cfr_part_parse_status": normalized["status"],
        "fr_doc_num": attrs.get("frDocNum"),
        "fr_vol_num": attrs.get("frVolNum"),
        "comment": attrs.get("comment"),
        "comment_category": attrs.get("category"),
        "first_name": attrs.get("firstName"),
        "last_name": attrs.get("lastName"),
        "email": attrs.get("email"),
        "phone": attrs.get("phone"),
        "fax": attrs.get("fax"),
        "submitter_org": attrs.get("organization"),
        "submitter_gov_agency": attrs.get("govAgency"),
        "submitter_gov_agency_type": attrs.get("govAgencyType"),
        "submitter_rep": attrs.get("submitterRep"),
        "author_date": attrs.get("authorDate"),
        "comment_start_date": attrs.get("commentStartDate"),
        "comment_end_date": attrs.get("commentEndDate"),
        "effective_date": attrs.get("effectiveDate"),
        "implementation_date": attrs.get("implementationDate"),
        "modify_date": attrs.get("modifyDate"),
        "posted_date": attrs.get("postedDate"),
        "postmark_date": attrs.get("postmarkDate"),
        "receive_date": attrs.get("receiveDate"),
        "is_late_comment": _to_bool(attrs.get("allowLateComments")),
        "is_open_for_comment": _to_bool(attrs.get("openForComment")),
        "is_withdrawn": _to_bool(attrs.get("withdrawn")),
        "within_comment_period": _to_bool(attrs.get("withinCommentPeriod")),
        "reason_withdrawn": attrs.get("reasonWithdrawn"),
        "restriction_reason": attrs.get("restrictReason"),
        "restriction_reason_type": attrs.get("restrictReasonType"),
        "flex_field1": attrs.get("field1"),
        "flex_field2": attrs.get("field2"),
        "reg_writer_instruction": attrs.get("regWriterInstruction"),
        "legacy_id": attrs.get("legacyId"),
        "original_document_id": attrs.get("originalDocumentId"),
        "tracking_nbr": attrs.get("trackingNbr"),
        "exhibit_location": attrs.get("exhibitLocation"),
        "exhibit_type": attrs.get("exhibitType"),
        "media": _to_json(attrs.get("media")),
        "omb_approval": attrs.get("ombApproval"),
        "source_citation": attrs.get("sourceCitation"),
        "topics": _to_json(attrs.get("topics")),
    }

    return tuple(record[c] for c in COLUMNS), normalized["status"], cfr_part


def _connect(db_url: str):
    if psycopg is None:
        raise RuntimeError(
            "Missing PostgreSQL driver. Install one of:\n"
            "  pip install 'psycopg[binary]'\n"
            "  pip install psycopg2-binary"
        )
    conn = psycopg.connect(db_url)
    # Make per-row upserts independent and resilient to occasional bad rows.
    try:
        conn.autocommit = True
    except Exception:
        pass
    return conn


def _read_docket_ids(args: argparse.Namespace) -> list[str]:
    ids: list[str] = []
    ids.extend(args.dockets or [])
    if args.docket_file:
        text = args.docket_file.read_text(encoding="utf-8")
        ids.extend([x for x in re.split(r"[\s,]+", text) if x])
    cleaned = [i.strip() for i in ids if i and i.strip()]
    # Remove duplicates while preserving order.
    return list(dict.fromkeys(cleaned))


def _init_schema(conn, schema_path: Path) -> None:
    sql = schema_path.read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(sql)


def ingest_dockets(
    conn,
    docket_ids: list[str],
    api_key: str,
    page_size: int = 250,
    dry_run: bool = False,
) -> tuple[int, int, Counter[str], list[str]]:
    upserted = 0
    failed = 0
    parse_status = Counter()
    unparsed_examples: list[str] = []

    with conn.cursor() as cur:
        for i, docket_id in enumerate(docket_ids, 1):
            print(f"\n[{i}/{len(docket_ids)}] Docket {docket_id}")
            docs = fetch_docket_documents(docket_id, api_key=api_key, page_size=page_size)
            print(f"  Total docs fetched for {docket_id}: {len(docs)}")

            for doc in docs:
                row_info = _row_from_document(doc)
                if row_info is None:
                    failed += 1
                    continue

                row, status, raw_cfr = row_info
                parse_status[status] += 1
                if status in {"unparsed", "missing_title"} and raw_cfr and raw_cfr not in unparsed_examples:
                    if len(unparsed_examples) < 10:
                        unparsed_examples.append(raw_cfr)

                if dry_run:
                    continue

                try:
                    cur.execute(UPSERT_SQL, row)
                    upserted += 1
                except Exception as exc:
                    failed += 1
                    print(f"  Upsert failed for doc {doc.get('id')}: {type(exc).__name__}: {exc}", file=sys.stderr)

    return upserted, failed, parse_status, unparsed_examples


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ingest one or more regulations.gov dockets into PostgreSQL documents table."
    )
    parser.add_argument("dockets", nargs="*", help="Docket IDs to ingest (e.g. CMS-2025-0304)")
    parser.add_argument(
        "--docket-file",
        type=Path,
        help="Optional text file with docket IDs (newline or comma separated)",
    )
    parser.add_argument(
        "--database-url",
        default=os.getenv("DATABASE_URL") or os.getenv("POSTGRES_DSN"),
        help="PostgreSQL DSN/URL (default: DATABASE_URL or POSTGRES_DSN env var)",
    )
    parser.add_argument(
        "--api-key",
        default=os.getenv("API_KEY") or "DEMO_KEY",
        help="regulations.gov API key (default: API_KEY env var or DEMO_KEY)",
    )
    parser.add_argument("--page-size", type=int, default=250, help="API page size (max 250)")
    parser.add_argument(
        "--init-schema",
        action="store_true",
        help="Initialize documents table/indexes before ingest",
    )
    parser.add_argument(
        "--schema",
        type=Path,
        default=DEFAULT_SCHEMA,
        help=f"Schema SQL file for --init-schema (default: {DEFAULT_SCHEMA.name})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch + normalize only (no DB writes)",
    )
    args = parser.parse_args()

    docket_ids = _read_docket_ids(args)
    if not docket_ids:
        parser.error("Provide at least one docket ID (positional or --docket-file)")

    if args.page_size < 1 or args.page_size > 250:
        parser.error("--page-size must be between 1 and 250")

    conn = None
    try:
        if not args.dry_run:
            if not args.database_url:
                parser.error("Missing --database-url (or set DATABASE_URL / POSTGRES_DSN)")
            conn = _connect(args.database_url)
            if args.init_schema:
                if not args.schema.exists():
                    parser.error(f"Schema file not found: {args.schema}")
                print(f"Initializing schema: {args.schema}")
                _init_schema(conn, args.schema)
        else:
            # Use a temporary no-op connection object shape via sqlite-like guard:
            class _NoopConn:
                def cursor(self):
                    class _NoopCursor:
                        def __enter__(self):
                            return self

                        def __exit__(self, exc_type, exc, tb):
                            return False

                        def execute(self, *_args, **_kwargs):
                            return None

                    return _NoopCursor()

            conn = _NoopConn()

        print(f"Ingesting {len(docket_ids)} docket(s): {', '.join(docket_ids)}")
        upserted, failed, parse_status, unparsed_examples = ingest_dockets(
            conn=conn,
            docket_ids=docket_ids,
            api_key=args.api_key,
            page_size=args.page_size,
            dry_run=args.dry_run,
        )

        print("\nDone.")
        print(f"  Upserted rows: {upserted}" + (" (dry-run)" if args.dry_run else ""))
        print(f"  Failed rows:   {failed}")
        print("  cfrPart parse status counts:")
        for status, count in parse_status.most_common():
            print(f"    {status:<14} {count}")

        if unparsed_examples:
            print("\n  Sample cfrPart values requiring follow-up:")
            for sample in unparsed_examples[:10]:
                print(f"    - {sample}")

        # Exit non-zero if row-level failures occurred.
        sys.exit(1 if failed else 0)
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


if __name__ == "__main__":
    main()
