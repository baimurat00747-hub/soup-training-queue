import unittest
from contextlib import closing, contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory

from soup_queue.store import Store, is_contention


class SQLTimingTest(unittest.TestCase):
    def test_create_and_repeat_measure_only_the_phases_they_use(self):
        with TemporaryDirectory() as tmp:
            store = Store(Path(tmp) / "timing.sqlite3")
            store.initialize()
            create_timings = {}
            job, created = store.create("key", 50, "succeeded", timings=create_timings)
            self.assertTrue(created)
            self.assertEqual(set(create_timings), {"sql-connect", "sql-key-read", "sql-wait", "sql-txn"})
            self.assertTrue(all(duration >= 0 for duration in create_timings.values()))

            retry_timings = {}
            with closing(store.connect()) as writer, store.write(writer):
                replay, created = store.create("key", 50, "succeeded", timings=retry_timings)
                self.assertEqual(replay["id"], job["id"])
                self.assertFalse(created)
            self.assertEqual(set(retry_timings), {"sql-connect", "sql-key-read"})

    def test_busy_write_records_wait_without_transaction_time(self):
        class FastTimeoutStore(Store):
            def connect(self):
                conn = super().connect()
                conn.execute("PRAGMA busy_timeout = 20")
                return conn

        with TemporaryDirectory() as tmp:
            store = FastTimeoutStore(Path(tmp) / "busy.sqlite3")
            store.initialize()
            timings = {}
            with closing(store.connect()) as writer, store.write(writer):
                try:
                    store.create("new", 50, "succeeded", timings=timings)
                except Exception as exc:
                    self.assertTrue(is_contention(exc))
                else:
                    self.fail("expected writer lock contention")
            self.assertGreaterEqual(timings["sql-wait"], .01)
            self.assertNotIn("sql-txn", timings)
