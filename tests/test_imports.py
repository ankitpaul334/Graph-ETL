from pathlib import Path
import subprocess
import sys
import unittest


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class ImportOrderTests(unittest.TestCase):
    def test_persistence_imports_without_circular_dependency(self) -> None:
        completed = subprocess.run(
            [
                sys.executable,
                "-c",
                (
                    "from graph_etl.persistence import Database; "
                    "from graph_etl.ingestion import RssAtomFeedParser"
                ),
            ],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(0, completed.returncode, completed.stderr)


if __name__ == "__main__":
    unittest.main()
