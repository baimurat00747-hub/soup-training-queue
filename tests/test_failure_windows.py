"""Deterministic fault injection around the real queue store and worker."""
import os
import select
import signal
import sqlite3
import subprocess
import sys
import time
import unittest
from contextlib import closing, contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory

from soup_queue.store import Store, is_contention
from soup_queue.worker import LEASE_MS

CHILD = """
import os
import signal
import sys
from contextlib import closing, contextmanager

from soup_queue import worker
from soup_queue.store import Store

path, mode = sys.argv[1:]


class InstrumentedStore(Store):
    @contextmanager
    def write(self, conn):
        with super().write(conn):
            yield
            if mode in {"crash", "lock"}:
                effects = conn.execute("SELECT COUNT(*) FROM effects").fetchone()[0]
                assert effects == 1, effects
                print(f"Effect staged before COMMIT: {effects}", flush=True)
                sig = signal.SIGKILL if mode == "crash" else signal.SIGSTOP
                os.kill(os.getpid(), sig)

    def complete(self, conn, job):
        accepted = super().complete(conn, job)
        print(f"Completion accepted: {accepted}", flush=True)
        return accepted


store = InstrumentedStore(path)
with closing(store.connect()) as conn:
    if mode == "freeze":
        job = store.claim(conn, worker.LEASE_MS)
        assert job is not None and job["attempt"] == 1
        sleep = worker.time.sleep

        def freeze_on_first_sleep(seconds):
            worker.time.sleep = sleep
            print("Attempt 1 paused during execution", flush=True)
            os.kill(os.getpid(), signal.SIGSTOP)
            sleep(seconds)

        worker.time.sleep = freeze_on_first_sleep
        worker.execute(store, conn, job)
    else:
        store.complete(conn, store.get(1))
"""


@contextmanager
def process(*args, **kwargs):
    with subprocess.Popen([sys.executable, *args], **kwargs) as child:
        try:
            yield child
        finally:
            if child.poll() is None:
                child.kill()
                child.wait(timeout=10)


def wait_until(predicate, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.02)
    raise AssertionError("Timed out waiting for the expected worker state")


class FailureWindowTest(unittest.TestCase):
    def setUp(self):
        directory = self.enter_temp_directory()
        self.db = Path(directory) / "queue.sqlite3"
        self.store = Store(self.db)
        self.store.initialize()

    def enter_temp_directory(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        return directory.name

    def child(self, mode):
        return process(
            "-c", CHILD, str(self.db), mode,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )

    def observe_stop(self, child, expected):
        readable, _, _ = select.select([child.stdout], [], [], 10)
        self.assertTrue(readable, "child did not reach the fault injection point")
        line = child.stdout.readline().strip()
        print(line, flush=True)
        self.assertIn(expected, line)
        status = wait_until(
            lambda: os.waitpid(child.pid, os.WNOHANG | os.WUNTRACED)[1], 5,
        )
        self.assertTrue(os.WIFSTOPPED(status), status)
        self.assertEqual(os.WSTOPSIG(status), signal.SIGSTOP)

    def effects(self):
        with closing(self.store.connect()) as conn:
            return [tuple(row) for row in conn.execute(
                "SELECT job_id, attempt FROM effects ORDER BY job_id"
            )]

    def claim_first(self):
        job, _ = self.store.create("first", 50, "succeeded")
        with closing(self.store.connect()) as conn:
            first = self.store.claim(conn, LEASE_MS)
        self.assertEqual(first["attempt"], 1)
        return job["id"]

    def test_crash_before_commit_rolls_back_and_recovers(self):
        job_id = self.claim_first()
        with self.child("crash") as child:
            output, _ = child.communicate(timeout=10)
            print(output.strip(), flush=True)
            print(f"Crash exit: {child.returncode}", flush=True)
            self.assertIn("Effect staged before COMMIT: 1", output)
            self.assertEqual(child.returncode, -signal.SIGKILL)

        state = self.store.get(job_id)["state"]
        effects = self.effects()
        print(f"After crash: state={state}, effects={effects}", flush=True)
        self.assertEqual((state, effects), ("running", []))

        with closing(self.store.connect()) as conn:
            replacement = wait_until(lambda: self.store.claim(conn, LEASE_MS), 5)
            self.assertEqual(replacement["attempt"], 2)
            self.assertTrue(self.store.complete(conn, replacement))

        state = self.store.get(job_id)["state"]
        effects = self.effects()
        print(f"After recovery: state={state}, effects={effects}", flush=True)
        self.assertEqual((state, effects), ("succeeded", [(job_id, 2)]))

    def test_stopped_writer_blocks_other_writes_until_killed(self):
        job_id = self.claim_first()
        with self.child("lock") as child:
            self.observe_stop(child, "Effect staged before COMMIT: 1")
            self.assertEqual(self.store.get(job_id)["state"], "running")
            self.assertEqual(self.effects(), [])
            start = time.monotonic()
            with self.assertRaises(sqlite3.OperationalError) as error:
                self.store.create("second", 50, "succeeded")
            elapsed = time.monotonic() - start
            self.assertTrue(is_contention(error.exception))
            print(f"Concurrent create: {error.exception} after {elapsed:.3f}s", flush=True)
            child.kill()
            self.assertEqual(child.wait(timeout=10), -signal.SIGKILL)

        _, created = self.store.create("second", 50, "succeeded")
        self.assertTrue(created)
        self.assertEqual(self.effects(), [])
        print("After SIGKILL: creation works, staged effect rolled back", flush=True)

    def test_worker_resumes_after_five_minutes(self):
        job, _ = self.store.create("frozen", 2000, "succeeded")
        job_id = job["id"]
        with self.child("freeze") as frozen:
            self.observe_stop(frozen, "Attempt 1 paused during execution")
            stopped = time.monotonic()
            print(f"Frozen worker pid={frozen.pid}; holding SIGSTOP for 300s", flush=True)

            env = {**os.environ, "QUEUE_DB": str(self.db)}
            with process("-m", "soup_queue.worker", env=env) as replacement:
                def completed():
                    if replacement.poll() is not None:
                        raise AssertionError(f"Replacement exited: {replacement.returncode}")
                    current = self.store.get(job_id)
                    return current if current["state"] == "succeeded" else None

                current = wait_until(completed, 10)
                recovered = time.monotonic() - stopped
                self.assertEqual(current["attempt"], 2)
                self.assertEqual(self.effects(), [(job_id, 2)])
                print(f"Replacement completed attempt 2 after {recovered:.3f}s", flush=True)
                time.sleep(max(0, stopped + 300 - time.monotonic()))
                self.assertIsNone(frozen.poll())
                print(f"Resuming after {time.monotonic() - stopped:.3f}s", flush=True)
                frozen.send_signal(signal.SIGCONT)
                output, _ = frozen.communicate(timeout=10)
                print(output.strip(), flush=True)
                self.assertEqual(frozen.returncode, 0, output)
                self.assertIn("Completion accepted: False", output)
                effects = self.effects()
                print(f"After original resumed: effects={effects}", flush=True)
                self.assertEqual(effects, [(job_id, 2)])
