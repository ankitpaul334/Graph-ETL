import json
from pathlib import Path
import unittest

from graph_etl.extraction.dates import normalize_date
from graph_etl.extraction.models import NormalizationStatus


FIXTURES = Path(__file__).parent / "fixtures"


class DateNormalizationTests(unittest.TestCase):
    def test_supports_date_only_offset_aware_ambiguous_invalid_and_missing(self) -> None:
        cases = json.loads((FIXTURES / "date_cases.json").read_text())
        for case in cases:
            with self.subTest(case=case["name"]):
                result = normalize_date(case["raw_value"])
                self.assertEqual(case["status"], result.status.value)
                self.assertEqual(case["normalized_value"], result.normalized_value)
                self.assertEqual(case["source_timezone"], result.source_timezone)

    def test_blank_value_is_missing(self) -> None:
        result = normalize_date("   ")
        self.assertEqual(NormalizationStatus.MISSING, result.status)
        self.assertIsNone(result.normalized_value)


if __name__ == "__main__":
    unittest.main()
