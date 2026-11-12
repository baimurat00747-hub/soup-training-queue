import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Barrier

from soup_queue.store import Store


class ClaimInvariantTest(unittest.TestCase):
    def setUp(self):
        tmp = TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.path = Path(tmp.name) / "queue.sqlite3"
        self.now = 1000
        self.store = Store(self.path, clock=lambda conn: self.now)
        self.store.initialize()
        self.store.create("job", 50, "succeeded")

    def test_two_workers_pass_precheck_but_only_one_claims(self):
        start = Barrier(2, timeout=5)

        class RacingStore(Store):
            @contextmanager
            def write(self, conn):
                # Both workers have found the job in the initial unlocked read.
                start.wait()
                with super().write(conn):
                    yield

        store = RacingStore(self.path, clock=lambda conn: self.now)

        def claim():
            with closing(store.connect()) as conn:
                job = store.claim(conn, 1500)
                return (job["id"], job["attempt"]) if job is not None else None

        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(claim)
            second = pool.submit(claim)
            results = [first.result(), second.result()]

        print(f"Results after both workers passed precheck: {results}", flush=True)
        self.assertCountEqual(results, [(1, 1), None])

    def test_expired_attempt_is_rejected_before_reclaim(self):
        with closing(self.store.connect()) as conn:
            first = self.store.claim(conn, 1500)
            self.now = first["lease_until_ms"]
            renewed = self.store.renew(conn, first, 1500)
            completed = self.store.complete(conn, first)
            print(f"At expiration before reclaim: renewed={renewed}, completed={completed}", flush=True)
            self.assertFalse(renewed)
            self.assertFalse(completed)
            self.assertEqual(self.store.get(1)["attempt"], 1)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM effects").fetchone()[0], 0)

    def test_old_attempt_is_rejected_after_reclaim_even_when_clock_moves_back(self):
        with closing(self.store.connect()) as first_conn, closing(self.store.connect()) as second_conn:
            first = self.store.claim(first_conn, 1500)
            self.now = first["lease_until_ms"]
            second = self.store.claim(second_conn, 1500)
            self.now = 1100

            current = self.store.get(1)
            self.assertEqual((current["state"], current["attempt"]), ("running", 2))
            renewed = self.store.renew(first_conn, first, 1500)
            completed = self.store.complete(first_conn, first)
            print(f"Attempt 1 while attempt 2 is running: renewed={renewed}, completed={completed}", flush=True)
            self.assertFalse(renewed)
            self.assertFalse(completed)
            self.assertTrue(self.store.complete(second_conn, second))
            effects = [tuple(row) for row in second_conn.execute("SELECT job_id, attempt FROM effects")]
            self.assertEqual(effects, [(1, 2)])
            print(f"Committed effects: {effects}", flush=True)
