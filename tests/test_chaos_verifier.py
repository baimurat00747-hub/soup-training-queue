import io
import unittest
from contextlib import closing, redirect_stdout
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from chaos import exercise_job, verify
from soup_queue.store import Store


class ChaosVerifierTest(unittest.TestCase):
    def setUp(self):
        tmp = TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.db = Path(tmp.name) / "verify.sqlite3"
        self.now = 1000
        self.store = Store(self.db, clock=lambda conn: self.now)
        self.store.initialize()

    def finish(self, key, outcome="succeeded", *, reclaim=False):
        job, _ = self.store.create(key, 50, outcome)
        with closing(self.store.connect()) as conn:
            claim = self.store.claim(conn, 1500)
            if reclaim:
                self.now = claim["lease_until_ms"]
                claim = self.store.claim(conn, 1500)
            self.assertEqual(job["id"], claim["id"])
            self.assertTrue(self.store.complete(conn, claim))
        return job["id"]

    def expect_rejection(self, expected, message):
        output = io.StringIO()
        with redirect_stdout(output), self.assertRaisesRegex(AssertionError, "Queue invariants failed"):
            verify(self.db, expected, expected_states=expected)
        self.assertIn("Invalid state or effect count: 0", output.getvalue())
        self.assertIn(message, output.getvalue())

    def test_valid_outcomes_and_matching_effect_pass(self):
        self.finish("success", reclaim=True)
        self.finish("failure", "failed")
        cancelled, _ = self.store.create("cancelled", 50, "succeeded")
        self.store.cancel(cancelled["id"])
        expected = {"success": "succeeded", "failure": "failed", "cancelled": "cancelled"}

        output = io.StringIO()
        with redirect_stdout(output):
            verify(self.db, expected, expected_states=expected)
        self.assertIn("Final state mismatches: 0", output.getvalue())
        self.assertIn("Effect attempt mismatches: 0", output.getvalue())

    def test_wrong_terminal_state_fails_despite_valid_effect_count(self):
        self.finish("job", "failed")
        self.expect_rejection({"job": "succeeded"}, "('job', 'succeeded', 'failed')")

    def test_completion_after_confirmed_cancellation_fails(self):
        self.finish("job")
        self.expect_rejection({"job": "cancelled"}, "('job', 'cancelled', 'succeeded')")

    def test_effect_from_older_attempt_fails_despite_valid_effect_count(self):
        job_id = self.finish("job", reclaim=True)
        with closing(self.store.connect()) as conn:
            conn.execute("UPDATE effects SET attempt = 1 WHERE job_id = ?", (job_id,))
        self.expect_rejection({"job": "succeeded"}, "('job', 2, 1)")

    def test_cancel_conflict_cannot_confirm_the_wrong_completion_outcome(self):
        with patch("chaos.post_json", side_effect=[
            (201, {"id": 1, "state": "queued"}),
            (409, {"id": 1, "state": "failed"}),
        ]):
            with self.assertRaisesRegex(AssertionError, "cancel returned 409"):
                exercise_job("http://unused", "job", {"outcome": "succeeded"}, cancel=True)
