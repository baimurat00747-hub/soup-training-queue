import sqlite3
from contextlib import closing
import os
import subprocess
import sys
import time
from contextlib import contextmanager
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
import json
from concurrent.futures import ThreadPoolExecutor
from random import Random
from threading import Event

def verify(db_path, expected_keys, *, expected_states=None):
    expected = set(expected_keys)
    if expected_states is not None and expected_states.keys() != expected:
        raise ValueError("Expected states must cover exactly the planned keys")
    with closing(sqlite3.connect(db_path)) as conn:
        rows = conn.execute("""
            SELECT j.idempotency_key, j.state, j.attempt,
                   COUNT(e.job_id), MAX(e.attempt)
            FROM jobs AS j
            LEFT JOIN effects AS e ON e.job_id = j.id
            GROUP BY j.id, j.idempotency_key, j.state, j.attempt
        """).fetchall()

    actual = {row[0] for row in rows}
    missing = expected - actual
    extra = actual - expected
    duplicate_count = len(rows) - len(actual)
    invalid = [
        (key, state, effects)
        for key, state, _, effects, _ in rows
        if state not in {"succeeded", "failed", "cancelled"}
        or effects != (1 if state == "succeeded" else 0)
    ]
    state_mismatches = []
    if expected_states is not None:
        state_mismatches = [
            (key, expected_states[key], state)
            for key, state, _, _, _ in rows
            if key in expected_states and state != expected_states[key]
        ]
    attempt_mismatches = [
        (key, attempt, effect_attempt)
        for key, _, attempt, effects, effect_attempt in rows
        if effects and effect_attempt != attempt
    ]

    print(f"Expected keys: {len(expected)}, jobs in DB: {len(rows)}")
    print(f"Missing: {len(missing)}, examples: {sorted(missing)[:5]}")
    print(f"Extra: {len(extra)}, examples: {sorted(extra)[:5]}")
    print(f"Duplicate jobs: {duplicate_count}")
    print(f"Invalid state or effect count: {len(invalid)}, examples: {invalid[:5]}")

    if expected_states is not None:
        print(f"Final state mismatches: {len(state_mismatches)}, examples: {state_mismatches[:5]}")
    print(f"Effect attempt mismatches: {len(attempt_mismatches)}, examples: {attempt_mismatches[:5]}")

    if missing or extra or duplicate_count or invalid or state_mismatches or attempt_mismatches:
        raise AssertionError("Queue invariants failed")


def start_worker(db_path):
    env = {**os.environ, "QUEUE_DB": str(db_path.resolve())}
    return subprocess.Popen(
        [sys.executable, "-m", "soup_queue.worker"], env=env,
    )


def kill_and_replace(workers, db_path, rng):
    for worker in workers:
        if worker.poll() is not None:
            raise RuntimeError(
                f"Worker {worker.pid} exited unexpectedly: {worker.returncode}"
            )

    index = rng.randrange(len(workers))
    victim = workers[index]
    victim.kill()
    exit_code = victim.wait(timeout=5)
    if exit_code != -9:
        raise RuntimeError(f"Expected SIGKILL for {victim.pid}, got {exit_code}")

    replacement = start_worker(db_path)
    workers[index] = replacement
    print(
        f"SIGKILL pid={victim.pid} exit={exit_code}, "
        f"replacement pid={replacement.pid}",
        flush=True,
    )
@contextmanager
def start_services(db_path, port=8001, worker_count=8):
    env = {**os.environ, "QUEUE_DB": str(db_path.resolve())}
    base_url = f"http://127.0.0.1:{port}"
    api = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "soup_queue.api:app",
         "--host", "127.0.0.1", "--port", str(port), "--no-access-log"],
        env=env,
    )
    workers = []
    try:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if api.poll() is not None:
                raise RuntimeError(f"API exited with code {api.returncode}")
            try:
                with urlopen(f"{base_url}/healthz", timeout=0.5):
                    break
            except URLError:
                time.sleep(0.1)
        else:
            raise RuntimeError("API did not become ready")

        for _ in range(worker_count):
            workers.append(start_worker(db_path))

        yield base_url, workers
    finally:
        for process in [*workers, api]:
            if process.poll() is None:
                process.terminate()
        for process in [*workers, api]:
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
def post_json(base_url, path, payload=None, key=None, *, response_headers=None):
    headers = {}
    data = None
    if payload is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(payload).encode()
    if key is not None:
        headers["Idempotency-Key"] = key

    request = Request(base_url + path, data=data, headers=headers, method="POST")
    try:
        with urlopen(request, timeout=5) as response:
            if response_headers is not None:
                response_headers.update(response.headers)
            return response.status, json.load(response)
    except HTTPError as exc:
        if response_headers is not None:
            response_headers.update(exc.headers)
        if exc.code != 409:
            raise
        with exc:
            return exc.code, json.load(exc)


def exercise_job(base_url, key, payload, *, retry=False, cancel=False):
    status, job = post_json(base_url, "/jobs", payload, key)
    if status != 201:
        raise AssertionError(f"{key}: create returned {status}, {job}")
    job_id = job["id"]

    if retry:
        status, repeated = post_json(base_url, "/jobs", payload, key)
        if status != 200 or repeated.get("id") != job_id:
            raise AssertionError(f"{key}: retry returned {status}, {repeated}")

    if cancel:
        status, result = post_json(base_url, f"/jobs/{job_id}/cancel")
        if (
            result.get("id") != job_id
            or (status, result.get("state")) not in {
                (200, "cancelled"),
                (409, payload.get("outcome", "succeeded")),
            }
        ):
            raise AssertionError(f"{key}: cancel returned {status}, {result}")
        return job_id, status

    return job_id, None

def wait_for_drain(db_path, timeout=30):
    deadline = time.monotonic() + timeout
    with closing(sqlite3.connect(db_path)) as conn:
        while True:
            pending = conn.execute(
                "SELECT COUNT(*) FROM jobs WHERE state IN ('queued', 'running')"
            ).fetchone()[0]
            if pending == 0:
                return
            if time.monotonic() >= deadline:
                raise AssertionError(f"Drain timed out: {pending} unfinished jobs")
            time.sleep(0.2)


def run_chaos(db_path, jobs, seed):
    expected_outcomes = {
        f"chaos-{i}": "failed" if i % 17 == 0 else "succeeded"
        for i in range(jobs)
    }
    rng = Random(seed)
    stop = Event()
    print(f"Starting chaos: jobs={jobs}, workers=8, seed={seed}", flush=True)

    with start_services(db_path) as (url, workers):
        def kill_loop():
            kills = 0
            while not stop.wait(2):
                kill_and_replace(workers, db_path, rng)
                kills += 1
            return kills

        with ThreadPoolExecutor(max_workers=1) as faults:
            killer = faults.submit(kill_loop)
            try:
                with ThreadPoolExecutor(max_workers=8) as clients:
                    start = time.monotonic()
                    requests = []
                    for i, (key, outcome) in enumerate(expected_outcomes.items()):
                        delay = start + i / 50 - time.monotonic()
                        if delay > 0:
                            time.sleep(delay)
                        if killer.done():
                            killer.result()
                        payload = {
                            "run_ms": 750 if i % 31 == 0 else 120,
                            "outcome": outcome,
                        }
                        requests.append(clients.submit(
                            exercise_job, url, key, payload,
                            retry=i % 3 == 0, cancel=i % 4 == 0,
                        ))

                    results = [request.result() for request in requests]

                print(f"HTTP scenarios completed: {len(results)}", flush=True)
                wait_for_drain(db_path)
            finally:
                stop.set()
                kills = killer.result()

        print(f"Worker kills: {kills}", flush=True)
        expected_states = {
            key: "cancelled" if cancel_status == 200 else expected_outcomes[key]
            for key, (_, cancel_status) in zip(expected_outcomes, results)
        }
        verify(db_path, expected_outcomes, expected_states=expected_states)
