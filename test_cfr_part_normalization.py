#!/usr/bin/env python3
import unittest

from cfr_part_normalization import extract_parts_for_title, normalize_cfr_part


class NormalizeCfrPartTests(unittest.TestCase):
    def test_single_part_with_word_part(self):
        result = normalize_cfr_part("42 CFR Part 412")
        self.assertEqual(result["status"], "parsed")
        self.assertEqual(result["references"], [{"title": "42", "part": "412"}])

    def test_single_part_without_word_part(self):
        result = normalize_cfr_part("42 CFR 412")
        self.assertEqual(result["status"], "parsed")
        self.assertEqual(result["references"], [{"title": "42", "part": "412"}])

    def test_multi_part_list(self):
        result = normalize_cfr_part("42 CFR Parts 405, 417, 422, and 460")
        self.assertEqual(result["status"], "parsed")
        self.assertEqual(
            result["references"],
            [
                {"title": "42", "part": "405"},
                {"title": "42", "part": "417"},
                {"title": "42", "part": "422"},
                {"title": "42", "part": "460"},
            ],
        )

    def test_range(self):
        result = normalize_cfr_part("42 CFR Parts 410-412")
        self.assertEqual(
            result["references"],
            [
                {"title": "42", "part": "410"},
                {"title": "42", "part": "411"},
                {"title": "42", "part": "412"},
            ],
        )

    def test_multiple_titles(self):
        result = normalize_cfr_part("42 CFR Part 412; 45 CFR Part 155")
        self.assertEqual(
            result["references"],
            [
                {"title": "42", "part": "412"},
                {"title": "45", "part": "155"},
            ],
        )

    def test_missing_title(self):
        result = normalize_cfr_part("Part 412")
        self.assertEqual(result["status"], "missing_title")
        self.assertEqual(result["references"], [{"title": "", "part": "412"}])

    def test_empty(self):
        result = normalize_cfr_part(None)
        self.assertEqual(result["status"], "empty")
        self.assertEqual(result["references"], [])

    def test_no_cfr_numeric_text(self):
        result = normalize_cfr_part("RIN 0938-AV01")
        self.assertEqual(result["status"], "no_cfr")
        self.assertEqual(result["references"], [])

    def test_extract_parts_for_title(self):
        parts = extract_parts_for_title("42 CFR Part 412; 45 CFR Part 155; 42 CFR 489", 42)
        self.assertEqual(parts, ["412", "489"])


if __name__ == "__main__":
    unittest.main()
