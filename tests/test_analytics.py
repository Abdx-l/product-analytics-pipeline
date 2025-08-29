import tempfile
import unittest
from pathlib import Path
from analytics import connect, load, metrics


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = connect(str(Path(self.tmp.name) / "a.db"))

    def tearDown(self):
        self.db.close()
        self.tmp.cleanup()

    def test_incremental_and_metrics(self):
        rows = [dict(event_id=e, user_id=u, occurred_at=t, event_type=k) for e,u,t,k in [
            ("1","u1","2026-10-01T00:00:00Z","signup"),
            ("2","u1","2026-10-08T00:00:00Z","purchase"),
            ("3","u2","2026-10-01T01:00:00Z","signup")]]
        self.assertEqual(load(self.db, rows), 3)
        self.assertEqual(load(self.db, rows), 0)
        report = metrics(self.db)
        self.assertEqual(report["funnel"], {"signed_up": 2, "purchased_after_signup": 1})
        self.assertEqual(report["day7_retention"][0]["day7_retained"], 1)

    def test_invalid_row_rolls_back(self):
        with self.assertRaises(ValueError):
            load(self.db, [{"event_id":"1","user_id":"u","occurred_at":"bad","event_type":"signup"}])
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM fact_events").fetchone()[0], 0)

