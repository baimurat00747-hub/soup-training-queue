import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Barrier

from soup_queue.store import ELIGIBLE, PayloadConflict, Store


class StorePerformanceInvariantTest(unittest.TestCase):
    def setUp(self):
        tmp = TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.store = Store(Path(tmp.name) / "queue.sqlite3", clock=lambda conn: 1000)
        self.store.initialize()

    def test_existing_key_can_be_read_during_another_write(self):
        original, _ = self.store.create("existing", 50, "succeeded")

        with closing(self.store.connect()) as writer, self.store.write(writer):
            writer.execute(
                "INSERT INTO jobs (idempotency_key, run_ms, outcome) VALUES ('uncommitted', 50, 'succeeded')"
            )
            replay, created = self.store.create("existing", 50, "succeeded")
            self.assertEqual(replay["id"], original["id"])
            self.assertFalse(created)
            with self.assertRaises(PayloadConflict):
                self.store.create("existing", 51, "succeeded")

    def test_concurrent_creates_after_both_reads_missed_the_key(self):
        barrier = Barrier(2, timeout=5)

        class RacingStore(Store):
            @contextmanager
            def write(self, conn):
                barrier.wait()
                with super().write(conn):
                    yield

        store = RacingStore(self.store.path)
        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(store.create, "race", 50, "succeeded")
            second = pool.submit(store.create, "race", 50, "succeeded")
            results = [first.result(), second.result()]

        self.assertCountEqual([created for _, created in results], [True, False])
        self.assertEqual(results[0][0]["id"], results[1][0]["id"])
        self.assertEqual(len(store.page(0, 10)), 1)

    def test_lease_index_preserves_oldest_eligible_id(self):
        with closing(self.store.connect()) as conn:
            conn.executemany(
                """INSERT INTO jobs (idempotency_key, run_ms, outcome, state, attempt, lease_until_ms)
                   VALUES (?, 50, 'succeeded', ?, ?, ?)""",
                [
                    ("terminal", "succeeded", 1, None),
                    ("live", "running", 1, 2000),
                    ("oldest", "running", 1, 1000),
                    ("queued", "queued", 0, None),
                    ("later-but-earlier-expiry", "running", 1, 500),
                ],
            )
            plan = conn.execute("EXPLAIN QUERY PLAN " + ELIGIBLE, (1000,)).fetchall()
            self.assertTrue(any("leased_jobs" in row[3] for row in plan), plan)
            self.assertEqual(
                [self.store.claim(conn, 1500)["id"] for _ in range(3)],
                [3, 4, 5],
            )
            self.assertIsNone(self.store.claim(conn, 1500))

    def test_cancel_returns_committed_state_for_each_path(self):
        queued, _ = self.store.create("cancel-first", 50, "succeeded")
        finished, _ = self.store.create("complete-first", 50, "succeeded")
        queued_id, finished_id = queued["id"], finished["id"]

        self.assertEqual(self.store.cancel(queued_id)["state"], "cancelled")
        self.assertEqual(self.store.cancel(queued_id)["state"], "cancelled")

        with closing(self.store.connect()) as conn:
            attempt = self.store.claim(conn, 1500)
            self.assertEqual(attempt["id"], finished_id)
            self.assertTrue(self.store.complete(conn, attempt))

        self.assertEqual(self.store.cancel(finished_id)["state"], "succeeded")
        self.assertIsNone(self.store.cancel(finished_id + 1))
