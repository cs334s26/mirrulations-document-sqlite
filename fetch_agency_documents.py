#!/usr/bin/env python3
"""
Fetch documents from regulations.gov API for given agencies.
Saves document JSON to data/<agency>/<docket>/text-<docket>/documents/<doc_id>.json
so insert_documents.py can load them.

Waits 1hr on 429 or remaining < 1.
"""
import json
import os
import time
from pathlib import Path

import requests

# Load API key
try:
    import dotenv
    dotenv.load_dotenv()
except ImportError:
    pass
API_KEY = os.getenv("API_KEY") or "DEMO_KEY"
BASE_URL = "https://api.regulations.gov/v4"
DATA_DIR = Path(__file__).resolve().parent / "data"


def make_request(url: str, params: dict | None = None) -> requests.Response:
    """GET with rate limit handling. Retries after 1hr on 429 or remaining < 1."""
    headers = {"X-Api-Key": API_KEY}
    while True:
        resp = requests.get(url, headers=headers, params=params)
        limit = resp.headers.get("X-RateLimit-Limit", "?")
        remaining = int(resp.headers.get("X-RateLimit-Remaining", 0))
        print(f"  Rate limit: {remaining}/{limit} remaining")
        if resp.status_code == 429:
            print("Rate limit exceeded, waiting 1 hour...")
            time.sleep(3600)
            continue
        if remaining < 1:
            print("Only 1 request left, waiting 1 hour...")
            time.sleep(3600)
            continue
        return resp


def get_all_agency_dockets(agency_id: str) -> list[dict]:
    """Paginate all dockets for an agency."""
    results = []
    page = 1
    while True:
        params = {
            "filter[agencyId]": agency_id,
            "page[number]": page,
            "page[size]": 250,
        }
        resp = make_request(f"{BASE_URL}/dockets", params)
        if resp.status_code != 200:
            print(f"Error {resp.status_code}: {resp.text[:200]}")
            break
        data = resp.json()
        items = data.get("data", [])
        total = data.get("meta", {}).get("totalElements", 0)
        results.extend(items)
        print(f"  Fetched {len(results)}/{total} dockets for {agency_id}")
        if not items or len(results) >= total:
            break
        page += 1
    return results


def get_docket_documents(docket_id: str) -> list[dict]:
    """Fetch all documents for a docket."""
    results = []
    page = 1
    while True:
        params = {
            "filter[docketId]": docket_id,
            "page[number]": page,
            "page[size]": 250,
        }
        resp = make_request(f"{BASE_URL}/documents", params)
        if resp.status_code != 200:
            print(f"  Error {resp.status_code} for docket {docket_id}")
            break
        data = resp.json()
        items = data.get("data", [])
        results.extend(items)
        if len(items) < 250:
            break
        page += 1
    return results


def save_document(doc: dict, agency_id: str, docket_id: str) -> Path | None:
    """Save doc to data/agency/docket/text-docket/documents/doc_id.json."""
    doc_id = doc.get("id")
    if not doc_id:
        return None
    # Wrap as single-document format for insert_documents
    payload = {"data": doc}
    out_dir = DATA_DIR / agency_id / docket_id / f"text-{docket_id}" / "documents"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{doc_id}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    return out_path


def download_agency_documents(agency_id: str) -> int:
    """
    Fetch all dockets for agency, then all documents per docket.
    Saves to data/ for insert_documents.py.
    Returns count of documents saved.
    """
    print(f"\n--- {agency_id} ---")
    dockets = get_all_agency_dockets(agency_id)
    if not dockets:
        print(f"No dockets for {agency_id}")
        return 0
    saved = 0
    for i, docket in enumerate(dockets, 1):
        docket_id = docket.get("id")
        attrs = docket.get("attributes", {})
        agency = attrs.get("agencyId") or agency_id
        print(f"  [{i}/{len(dockets)}] {docket_id} ...")
        docs = get_docket_documents(docket_id)
        for doc in docs:
            doc_attrs = doc.get("attributes", {})
            doc_agency = doc_attrs.get("agencyId") or agency
            path = save_document(doc, doc_agency, docket_id)
            if path:
                saved += 1
        print(f"    Saved {len(docs)} documents")
    return saved


def main():
    import argparse
    p = argparse.ArgumentParser(description="Fetch agency documents from regulations.gov API")
    p.add_argument("agencies", nargs="+", help="Agency IDs e.g. CMS FDA")
    args = p.parse_args()
    total = 0
    for agency_id in args.agencies:
        total += download_agency_documents(agency_id)
    print(f"\nTotal documents saved: {total}")
    print("Run: python3 insert_documents.py")


if __name__ == "__main__":
    main()
