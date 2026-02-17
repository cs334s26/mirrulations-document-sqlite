#!/usr/bin/env python3
"""
Walk all document JSON files under data/ and report, for every field (key path):
- ALWAYS null: present in every document and null in every document
- NEVER null:  present in every document and never null
- SOMETIMES null: present in some docs with null and/or missing in some docs
"""

import json
import multiprocessing as mp
import os
import sys
from pathlib import Path


def collect_field_nullability(obj, prefix="", out=None):
    """Recursively collect each leaf path and whether its value is null. Mutates out."""
    if out is None:
        out = {}
    if obj is None:
        if prefix:
            out[prefix] = True
        return out
    if isinstance(obj, dict):
        for k, v in obj.items():
            path = f"{prefix}.{k}" if prefix else k
            if v is None:
                out[path] = True
            elif isinstance(v, (dict, list)):
                out[path] = False
                if isinstance(v, dict):
                    collect_field_nullability(v, path, out)
            else:
                out[path] = False  # scalar
        return out
    if isinstance(obj, list):
        if prefix:
            out[prefix] = False
        return out
    # Scalar (str, int, bool, etc.)
    if prefix:
        out[prefix] = False
    return out


def process_chunk(paths):
    """Process a chunk of JSON files; return (stats_dict, error_count)."""
    stats = {}
    errors = 0
    for path in paths:
        try:
            with open(path, "r", encoding="utf-8") as f:
                doc = json.load(f)
        except Exception:
            errors += 1
            continue
        fields = collect_field_nullability(doc)
        for field_path, is_null in fields.items():
            if field_path not in stats:
                stats[field_path] = [0, 0]
            stats[field_path][0] += 1
            if is_null:
                stats[field_path][1] += 1
    return stats, errors


def main():
    data_root = Path(__file__).resolve().parent / "data"
    if not data_root.is_dir():
        print("data/ not found", file=sys.stderr)
        sys.exit(1)

    # Find all document JSONs: data/<agency>/<docket>/text-<docket>/documents/*.json
    doc_files = []
    for agency_dir in sorted(data_root.iterdir()):
        if not agency_dir.is_dir():
            continue
        for docket_dir in agency_dir.iterdir():
            if not docket_dir.is_dir():
                continue
            text_dir = docket_dir / f"text-{docket_dir.name}"
            docs_dir = text_dir / "documents"
            if docs_dir.is_dir():
                doc_files.extend(docs_dir.glob("*.json"))

    total_docs = len(doc_files)
    print(f"Found {total_docs} document JSON files\n", file=sys.stderr)

    # Process in parallel (many smaller chunks for better load balancing)
    n_workers = max(1, (os.cpu_count() or 4) - 1)
    chunk_size = min(25000, max(1000, total_docs // (n_workers * 4)))
    chunks = [
        [str(p) for p in doc_files[i : i + chunk_size]]
        for i in range(0, total_docs, chunk_size)
    ]
    print(f"Using {n_workers} workers, {len(chunks)} chunks (size ~{chunk_size})\n", file=sys.stderr)

    stats = {}
    total_errors = 0
    with mp.Pool(n_workers) as pool:
        for i, (chunk_stats, chunk_errors) in enumerate(pool.imap_unordered(process_chunk, chunks)):
            total_errors += chunk_errors
            for field_path, (present, null_count) in chunk_stats.items():
                if field_path not in stats:
                    stats[field_path] = [0, 0]
                stats[field_path][0] += present
                stats[field_path][1] += null_count
            if (i + 1) % 10 == 0:
                print(f"  merged {i + 1} / {len(chunks)} chunks ...", file=sys.stderr)

    if total_errors:
        print(f"Total read errors: {total_errors}", file=sys.stderr)

    # Classify
    always_null = []
    never_null = []
    sometimes_null = []

    for path in sorted(stats.keys()):
        present, null_count = stats[path]
        if present == total_docs and null_count == total_docs:
            always_null.append(path)
        elif present == total_docs and null_count == 0:
            never_null.append(path)
        else:
            sometimes_null.append((path, present, null_count))

    # Report
    print("=" * 60)
    print("FIELDS ALWAYS NULL (in every document)")
    print("=" * 60)
    for p in always_null:
        print(p)
    print(f"\nTotal: {len(always_null)} fields\n")

    print("=" * 60)
    print("FIELDS NEVER NULL (in every document)")
    print("=" * 60)
    for p in never_null:
        print(p)
    print(f"\nTotal: {len(never_null)} fields\n")

    print("=" * 60)
    print("FIELDS SOMETIMES NULL (or missing in some documents)")
    print("  format: path  [present_count, null_count]")
    print("=" * 60)
    for item in sometimes_null:
        if len(item) == 3:
            p, pres, nul = item
            print(f"  {p}  [{pres}, {nul}]")
        else:
            print(item)
    print(f"\nTotal: {len(sometimes_null)} fields\n")

    # Summary to stderr
    print(
        f"Summary: {len(always_null)} always null, {len(never_null)} never null, {len(sometimes_null)} sometimes null",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
