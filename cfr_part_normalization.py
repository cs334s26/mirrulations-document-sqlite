#!/usr/bin/env python3
"""
Normalize messy regulations.gov cfrPart strings into structured references.

The raw cfrPart value can be highly inconsistent:
  - "42 CFR Part 412"
  - "42 CFR 412"
  - "42 CFR Parts 405, 412, and 489"
  - "42 CFR Parts 410-415"
  - "42 CFR Part 412; 45 CFR Part 155"

This module keeps raw text untouched while extracting a normalized list of
{title, part} references and a parse status for downstream review.
"""

from __future__ import annotations

import json
import re
from typing import Any

TITLE_SEGMENT_RE = re.compile(
    r"(?P<title>\d+)\s*CFR\b(?P<body>.*?)(?=(?:\b\d+\s*CFR\b)|$)",
    re.IGNORECASE | re.DOTALL,
)
PART_HINT_RE = re.compile(r"\bPart(?:s)?\b", re.IGNORECASE)
RANGE_RE = re.compile(
    r"(?<!\d)(\d{1,4}(?:\.\d+)?)\s*(?:-|to|through|thru)\s*(\d{1,4}(?:\.\d+)?)(?!\d)",
    re.IGNORECASE,
)
NUMBER_RE = re.compile(r"(?<!\d)(\d{1,4}(?:\.\d+)?)(?!\d)")
STOP_WORD_RE = re.compile(
    r"\b(?:subchapter|chapter|section|parts?\s+of|u\.?s\.?c\.?|fr\s+doc|rins?)\b",
    re.IGNORECASE,
)


def _canonical_number(raw: str) -> str:
    raw = raw.strip()
    if not raw:
        return raw
    if "." in raw:
        # Preserve decimal precision but trim trailing zeroes.
        return raw.rstrip("0").rstrip(".")
    try:
        return str(int(raw))
    except ValueError:
        return raw


def _expand_range(start: str, end: str) -> list[str]:
    if "." in start or "." in end:
        return [_canonical_number(start), _canonical_number(end)]
    try:
        s_i = int(start)
        e_i = int(end)
    except ValueError:
        return [_canonical_number(start), _canonical_number(end)]

    if s_i > e_i:
        s_i, e_i = e_i, s_i
    # Guardrail against accidental huge expansions from malformed text.
    if e_i - s_i > 300:
        return [str(s_i), str(e_i)]
    return [str(i) for i in range(s_i, e_i + 1)]


def _extract_parts_from_body(body: str) -> list[str]:
    text = body.replace("\u2013", "-").replace("\u2014", "-").strip()
    if not text:
        return []

    part_hint = PART_HINT_RE.search(text)
    candidate = text[part_hint.start() :] if part_hint else text[:120]

    stop_match = STOP_WORD_RE.search(candidate)
    if stop_match:
        candidate = candidate[: stop_match.start()]

    seen: set[str] = set()
    parts: list[str] = []

    for match in RANGE_RE.finditer(candidate):
        for expanded in _expand_range(match.group(1), match.group(2)):
            normalized = _canonical_number(expanded)
            if normalized and normalized not in seen:
                parts.append(normalized)
                seen.add(normalized)

    no_ranges = RANGE_RE.sub(" ", candidate)
    for token in NUMBER_RE.findall(no_ranges):
        normalized = _canonical_number(token)
        if normalized and normalized not in seen:
            parts.append(normalized)
            seen.add(normalized)

    return parts


def normalize_cfr_part(raw_cfr_part: str | None) -> dict[str, Any]:
    """
    Parse a raw cfrPart string into normalized references.

    Returns:
      {
        "raw": <raw input>,
        "status": "empty" | "parsed" | "missing_title" | "unparsed" | "no_cfr",
        "references": [{"title": "42", "part": "412"}, ...],
      }
    """
    if raw_cfr_part is None:
        return {"raw": None, "status": "empty", "references": []}

    raw = str(raw_cfr_part).strip()
    if not raw:
        return {"raw": raw, "status": "empty", "references": []}

    references: list[dict[str, str]] = []
    seen_refs: set[tuple[str, str]] = set()
    had_cfr_title = False

    for match in TITLE_SEGMENT_RE.finditer(raw):
        had_cfr_title = True
        title = _canonical_number(match.group("title"))
        for part in _extract_parts_from_body(match.group("body") or ""):
            key = (title, part)
            if key not in seen_refs:
                references.append({"title": title, "part": part})
                seen_refs.add(key)

    if references:
        return {"raw": raw, "status": "parsed", "references": references}

    if had_cfr_title:
        return {"raw": raw, "status": "unparsed", "references": []}

    # If we can infer parts but no explicit title, keep them as "missing_title".
    # Require a "Part/Parts" cue to avoid classifying unrelated numeric text.
    inferred_parts = _extract_parts_from_body(raw)
    if inferred_parts and PART_HINT_RE.search(raw):
        return {
            "raw": raw,
            "status": "missing_title",
            "references": [{"title": "", "part": p} for p in inferred_parts],
        }

    return {"raw": raw, "status": "no_cfr", "references": []}


def normalize_cfr_part_json(raw_cfr_part: str | None) -> str:
    """Serialize normalize_cfr_part() output as compact JSON."""
    return json.dumps(normalize_cfr_part(raw_cfr_part), separators=(",", ":"), sort_keys=True)


def extract_parts_for_title(raw_cfr_part: str | None, title: int | str) -> list[str]:
    """
    Convenience helper: return normalized parts for a specific CFR title.
    """
    title_s = str(title)
    normalized = normalize_cfr_part(raw_cfr_part)
    parts = [
        ref["part"]
        for ref in normalized["references"]
        if ref.get("title") == title_s and ref.get("part")
    ]
    # preserve insertion order but remove duplicates
    return list(dict.fromkeys(parts))
