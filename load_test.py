import argparse
import json
import os
import math
import sqlite3
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, nullcontext
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.error import HTTPError
from urllib.request import urlopen
from chaos import post_json, start_services, verify, wait_for_drain

RATE = 100
SECONDS = 60
PAYLOAD = {"run_ms": 50, "outcome": "succeeded"}
FOLLOW_UPS = ("retry", "get", "retry", "cancel", "list",
              "retry", "get", "cancel", "retry", "get")


def send(url, operation, key, create_result, headers):
    if operation in {"create", "retry"}:
        status, job = post_json(url, "/jobs", PAYLOAD, key, response_headers=headers)
        expected = 201 if operation == "create" else 200
        if status != expected:
            raise AssertionError(f"{operation}: HTTP {status}, expected {expected}")
        if operation == "retry" and job["id"] != create_result.result()[2]:
            raise AssertionError(f"{key}: retry returned a different ID")
        return job["id"]

    if operation == "list":
        with urlopen(f"{url}/jobs?limit=20", timeout=5) as response:
            json.load(response)
        return None

    _, _, job_id, error, _ = create_result.result()
    if error:
        raise RuntimeError(f"{key}: create failed: {error}")

    if operation == "get":
        with urlopen(f"{url}/jobs/{job_id}", timeout=5) as response:
            job = json.load(response)
        if job["id"] != job_id:
            raise AssertionError(f"{key}: GET returned a different ID")
    else:
        status, job = post_json(url, f"/jobs/{job_id}/cancel")
        if job["id"] != job_id or (status, job["state"]) not in {
            (200, "cancelled"), (409, "succeeded"),
        }:
            raise AssertionError(f"{key}: invalid cancel response: {status}, {job}")


def timed_request(url, operation, key, create_result):
    started = None
    headers = {}
    try:
        if create_result is not None:
            _, _, _, error, _ = create_result.result()
            if error:
                raise RuntimeError(f"create failed: {error}")
        started = time.monotonic()
        job_id = send(url, operation, key, create_result, headers)
        return started, time.monotonic(), job_id, None, headers.get("server-timing", "")
    except Exception as exc:
        if started is None:
            category = "not dispatched"
        elif isinstance(exc, HTTPError):
            category = f"HTTP {exc.code}"
        else:
            category = type(exc).__name__
        error = category, f"{operation} {key}: {exc!r}"
        return started, time.monotonic(), None, error, headers.get("server-timing", "")


def percentile(values, fraction):
    ordered = sorted(values)
    return ordered[math.ceil(len(ordered) * fraction) - 1]


def report_operations(operations, results):
    grouped = defaultdict(list)
    for operation, (_, result) in zip(operations, results):
        grouped[operation].append(result)

    for operation, attempts in grouped.items():
        successful = [r for r in attempts if r[3] is None]
        latency = [(r[1] - r[0]) * 1000 for r in successful]
        errors = Counter(r[3][0] for r in attempts if r[3] is not None)
        summary = (
            f"p95={percentile(latency, .95):.2f} ms p99={percentile(latency, .99):.2f} ms"
            if latency else "no successful responses"
        )
        print(f"Operation {operation}: total={len(attempts)} errors={dict(errors)} {summary}")

        phases = defaultdict(list)
        for result in successful:
            for part in result[4].split(","):
                name, separator, duration = part.strip().partition(";dur=")
                if separator:
                    phases[name].append(float(duration))

        for name, times in phases.items():
            print(f"Phase {operation} {name}: n={len(times)} "
                  f"p95={percentile(times, .95):.3f} ms p99={percentile(times, .99):.3f} ms")


def run(
    db_path, *, base_url=None, rate=RATE, seconds=SECONDS, clients=16, workers=4, drain_timeout=30
):
    expected_keys = []
    requests = []
    operations = []
    print(f"Target: {rate} HTTP requests/s for {seconds}s, workers={workers}, clients={clients}")
    services = (
        start_services(db_path, worker_count=workers)
        if base_url is None
        else nullcontext((base_url, []))
    )
    with services as (url, _):
        with closing(sqlite3.connect(db_path)) as conn:
            existing = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
            if existing:
                raise RuntimeError(
                    f"Load test requires an empty database; found {existing} jobs. "
                    "Start a new Compose project with a fresh volume."
                )
            with ThreadPoolExecutor(max_workers=clients) as pool:
                start = time.monotonic()
                for i in range(rate * seconds):
                    scheduled = start + i / rate
                    time.sleep(max(0, scheduled - time.monotonic()))

                    if i % (5 * rate) == 0:
                        states = dict(conn.execute(
                            "SELECT state, COUNT(*) FROM jobs GROUP BY state"
                        ))
                        queued = states.get("queued", 0)
                        running = states.get("running", 0)
                        print(
                            f"t={time.monotonic() - start:.2f}s "
                            f"unfinished={queued + running} queued={queued} running={running} "
                            f"succeeded={states.get('succeeded', 0)} "
                            f"cancelled={states.get('cancelled', 0)}"
                        )

                    key = f"load-{i // 2}"
                    if i % 2 == 0:
                        operation = "create"
                        expected_keys.append(key)
                        created = pool.submit(
                            timed_request, url, "create", key, None
                        )
                        future = created
                    else:
                        operation = FOLLOW_UPS[(i // 2) % len(FOLLOW_UPS)]
                        dependency = None if operation == "list" else created
                        future = pool.submit(
                            timed_request, url, operation, key, dependency
                        )
                    requests.append((scheduled, future))
                    operations.append(operation)

                results = [(scheduled, future.result())
                           for scheduled, future in requests]

            states = dict(conn.execute("SELECT state, COUNT(*) FROM jobs GROUP BY state"))
            print(f"After all client responses: {states}")

        errors = [result[3] for _, result in results if result[3]]
        successful = [(scheduled, result) for scheduled, result in results
                      if result[3] is None]
        window_end = start + seconds
        dispatched = [result for _, result in results if result[0] is not None]
        completed_in_window = sum(result[1] < window_end for _, result in successful)
        started_in_window = sum(result[0] < window_end for result in dispatched)
        print(f"Scheduled: {len(results)}, successful: {len(successful)}")
        print(f"Requests started in {seconds}s: {started_in_window} ({started_in_window / seconds:.2f}/s)")
        print(f"Successful responses in {seconds}s: {completed_in_window}")
        print(f"Successful responses/s in window: {completed_in_window / seconds:.2f}")
        print(f"Errors: {len(errors)}, categories: {dict(Counter(category for category, _ in errors))}")
        print(f"Error examples: {[detail for _, detail in errors[:5]]}")
        for offset in range(0, seconds, 5):
            lower = start + offset
            upper = min(lower + 5, window_end)
            starts = sum(lower <= result[0] < upper for result in dispatched)
            successes = sum(lower <= result[1] < upper for _, result in successful)
            http_503 = sum(
                lower <= result[1] < upper and result[3] is not None
                and result[3][0] == "HTTP 503"
                for _, result in results
            )
            print(f"Window {offset}–{min(offset + 5, seconds)}s: "
                  f"starts={starts} successes={successes} HTTP 503={http_503}")

        if successful:
            latency = [(result[1] - result[0]) * 1000 for _, result in successful]
            lag = [(result[0] - scheduled) * 1000 for scheduled, result in successful]
            print(f"HTTP latency p95={percentile(latency, .95):.1f} ms, "
                  f"p99={percentile(latency, .99):.1f} ms")
            print(f"Client dispatch delay p95={percentile(lag, .95):.1f} ms")

        report_operations(operations, results)

        drain_start = time.monotonic()
        wait_for_drain(db_path, timeout=drain_timeout)
        print(f"Drain after last response: {time.monotonic() - drain_start:.2f}s")
        verify(db_path, expected_keys)

        if errors:
            raise AssertionError("Load test had request errors")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mixed HTTP load for the training queue")
    parser.add_argument("--rate", type=int, default=RATE)
    parser.add_argument("--seconds", type=int, default=SECONDS)
    parser.add_argument("--clients", type=int, default=16)
    parser.add_argument("--workers", type=int, default=4, help="worker count; match Compose scale in external mode")
    parser.add_argument("--drain-timeout", type=int, default=30)
    args = parser.parse_args()
    if min(vars(args).values()) < 1:
        parser.error("all load parameters must be positive")

    base_url = os.getenv("LOAD_BASE_URL")
    if base_url:
        run(Path(os.environ["QUEUE_DB"]), base_url=base_url, **vars(args))
    else:
        with TemporaryDirectory(dir=".") as tmp:
            run(Path(tmp) / "load.sqlite3", **vars(args))
