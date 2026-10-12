from contextlib import redirect_stdout
from io import StringIO
import json
from pathlib import Path
import unittest

from graph_etl.cli import main
from graph_etl.evaluation import PairLabel, evaluate_pairs, load_labeled_pairs, predict_pair


FIXTURES = Path(__file__).parent / "fixtures"
LABELS = FIXTURES / "resolution_labels.json"


class EvaluationTests(unittest.TestCase):
    def test_labeled_dataset_has_no_false_merges(self) -> None:
        pairs = load_labeled_pairs(LABELS)
        metrics = evaluate_pairs(pairs)

        self.assertEqual(8, metrics.total)
        self.assertEqual(0, metrics.false_positive)
        self.assertEqual(0.0, metrics.false_merge_rate)
        self.assertEqual(3, metrics.true_positive)
        self.assertEqual(0, metrics.false_negative)
        self.assertEqual(1.0, metrics.precision)
        self.assertEqual(1.0, metrics.recall)
        self.assertGreater(metrics.uncertain_rate, 0.0)

    def test_non_event_classification_is_never_a_match(self) -> None:
        predicted = predict_pair(
            {
                "source_key": "feed:a",
                "external_id": "one",
                "title": "PyData London",
                "event_date": "2026-11-05",
                "classification": "NOT_EVENT",
            },
            {
                "source_key": "feed:a",
                "external_id": "one",
                "title": "PyData London",
                "event_date": "2026-11-05",
            },
        )
        self.assertEqual(PairLabel.NO_MATCH, predicted)

    def test_strong_identity_precedes_title(self) -> None:
        predicted = predict_pair(
            {
                "source_key": "feed:a",
                "external_id": "same",
                "title": "First Title",
                "event_date": "2026-11-05",
            },
            {
                "source_key": "feed:a",
                "external_id": "same",
                "title": "Corrected Title",
                "event_date": "2026-11-05",
            },
        )
        self.assertEqual(PairLabel.MATCH, predicted)

    def test_undefined_metrics_are_not_reported_as_perfect(self) -> None:
        metrics = evaluate_pairs([])

        self.assertIsNone(metrics.precision)
        self.assertIsNone(metrics.recall)
        self.assertIsNone(metrics.false_merge_rate)

    def test_evaluate_cli_reports_metrics(self) -> None:
        output = StringIO()
        with redirect_stdout(output):
            exit_code = main(["evaluate", "--labels", str(LABELS)])

        self.assertEqual(0, exit_code)
        metrics = json.loads(output.getvalue())
        self.assertEqual(0.0, metrics["false_merge_rate"])
        self.assertEqual(0, metrics["false_positive"])


if __name__ == "__main__":
    unittest.main()