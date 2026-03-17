#!/usr/bin/env python3
"""
Download all Federal Register documents from the API (1994-present).

Uses monthly date ranges to minimize API calls (~1 hour total at 3.6 sec/call).
Output: documents.json (single array of all document objects).

Stops immediately on 429 (rate limit) or any non-200 response.
"""

from __future__ import annotations

import calendar
import json
import sys
import time
from datetime import date
from pathlib import Path

import requests

BASE_URL = "https://www.federalregister.gov/api/v1/documents.json"
RATE_LIMIT_SECONDS = 3.6
OUTPUT_FILE = Path("documents.json")

# All document fields per API documentation
ALL_FIELDS = [
    "abstract",
    "action",
    "agencies",
    "agency_names",
    "amendatory_instruction_count",
    "body_html_url",
    "cfr_references",
    "cfr_topics",
    "citation",
    "comment_url",
    "comments_close_on",
    "correction_of",
    "corrections",
    "dates",
    "disposition_notes",
    "docket_id",
    "docket_ids",
    "dockets",
    "document_number",
    "effective_on",
    "end_page",
    "excerpts",
    "executive_order_notes",
    "executive_order_number",
    "explanation",
    "full_text_xml_url",
    "html_url",
    "images",
    "images_metadata",
    "json_url",
    "mods_url",
    "not_received_for_publication",
    "page_length",
    "page_views",
    "pdf_url",
    "president",
    "presidential_document_number",
    "proclamation_number",
    "public_inspection_pdf_url",
    "publication_date",
    "raw_text_url",
    "regulation_id_number_info",
    "regulation_id_numbers",
    "regulations_dot_gov_info",
    "regulations_dot_gov_url",
    "related_documents",
    "significant",
    "signing_date",
    "start_page",
    "subtype",
    "title",
    "toc_doc",
    "toc_subject",
    "topics",
    "type",
    "volume",
]


def month_range(start: date, end: date):
    """Yield (year, month) tuples from start to end inclusive."""
    y, m = start.year, start.month
    end_y, end_m = end.year, end.month
    while (y, m) <= (end_y, end_m):
        yield y, m
        m += 1
        if m > 12:
            m = 1
            y += 1


def last_day_of_month(year: int, month: int) -> int:
    return calendar.monthrange(year, month)[1]


def fetch_page(year: int, month: int, page: int) -> requests.Response:
    """Fetch one page of documents for the given month."""
    last_day = last_day_of_month(year, month)
    gte = f"{year}-{month:02d}-01"
    lte = f"{year}-{month:02d}-{last_day:02d}"

    params = {
        "per_page": 1000,
        "order": "oldest",
        "page": page,
        "conditions[publication_date][gte]": gte,
        "conditions[publication_date][lte]": lte,
        "fields[]": ALL_FIELDS,
    }

    return requests.get(
        BASE_URL,
        params=params,
        headers={"User-Agent": "FederalRegisterDocumentDownloader/1.0 (research)"},
        timeout=60,
    )


def report_429(response: requests.Response) -> None:
    """Print detailed 429 rate limit report and exit."""
    print("\n" + "=" * 70)
    print("RATE LIMIT (429) DETECTED - STOPPING IMMEDIATELY")
    print("=" * 70)
    print(f"\nStatus code: {response.status_code}")
    print(f"URL: {response.url}")
    print(f"\nResponse headers:")
    for k, v in response.headers.items():
        print(f"  {k}: {v}")
    print(f"\nResponse body (first 2000 chars):")
    try:
        body = response.text[:2000]
        print(body)
        if len(response.text) > 2000:
            print("... [truncated]")
    except Exception as e:
        print(f"  (could not read: {e})")
    print("\n" + "=" * 70)
    print("Your IP may have been temporarily blocked.")
    print("Wait before retrying. Consider increasing RATE_LIMIT_SECONDS.")
    print("=" * 70)
    sys.exit(1)


def report_non_200(response: requests.Response, context: str) -> None:
    """Print thorough non-200 report and exit."""
    print("\n" + "=" * 70)
    print(f"NON-200 RESPONSE - STOPPING: {context}")
    print("=" * 70)
    print(f"\nStatus code: {response.status_code}")
    print(f"URL: {response.url}")
    print(f"\nResponse headers:")
    for k, v in response.headers.items():
        print(f"  {k}: {v}")
    print(f"\nResponse body (first 3000 chars):")
    try:
        body = response.text[:3000]
        print(body)
        if len(response.text) > 3000:
            print("... [truncated]")
    except Exception as e:
        print(f"  (could not read: {e})")
    print("\n" + "=" * 70)
    sys.exit(1)


def main() -> None:
    start_time = time.time()
    today = date.today()
    start_date = date(1994, 1, 1)
    end_date = today

    print(f"Federal Register document download")
    print(f"Date range: {start_date} to {end_date}")
    print(f"Output: {OUTPUT_FILE}")
    print(f"Rate limit: {RATE_LIMIT_SECONDS} sec between requests")
    print("-" * 50)

    all_documents = []
    total_requests = 0

    for year, month in month_range(start_date, end_date):
        month_label = f"{year}-{month:02d}"
        page = 1
        month_docs = 0

        print(f"\n{month_label} ({calendar.month_name[month]} {year}):")

        while True:
            total_requests += 1
            response = fetch_page(year, month, page)

            if response.status_code == 429:
                report_429(response)

            if response.status_code != 200:
                report_non_200(
                    response,
                    f"Month {month_label}, page {page}",
                )

            try:
                data = response.json()
            except json.JSONDecodeError as e:
                print(f"\nJSON decode error for {month_label} page {page}: {e}")
                print(f"Response text (first 500 chars): {response.text[:500]}")
                sys.exit(1)

            results = data.get("results", [])
            count = data.get("count", 0)
            total_pages = data.get("total_pages", 1)

            all_documents.extend(results)
            month_docs += len(results)

            print(
                f"  {month_label} page {page}/{total_pages}: "
                f"{len(results)} docs (total this month: {month_docs}, "
                f"overall: {len(all_documents):,})"
            )

            if page >= total_pages or len(results) == 0:
                break

            page += 1
            time.sleep(RATE_LIMIT_SECONDS)

        print(f"  {month_label} complete: {month_docs} documents")
        time.sleep(RATE_LIMIT_SECONDS)

    elapsed = time.time() - start_time
    print("-" * 50)
    print(
        f"Download complete: {len(all_documents):,} documents in {total_requests} requests "
        f"({elapsed/60:.1f} min elapsed)"
    )

    print(f"Writing {OUTPUT_FILE}...")
    with open(OUTPUT_FILE, "w") as f:
        json.dump(all_documents, f, indent=None, separators=(",", ":"))

    print(f"Done. File size: {OUTPUT_FILE.stat().st_size / 1e6:.1f} MB")


if __name__ == "__main__":
    main()
