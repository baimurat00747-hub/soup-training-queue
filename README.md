# Training job queue

FastAPI accepts jobs and four workers process them. Training is simulated. A successful job leaves one row in SQLite that the tests can count.

I used AI to help me to write README and DESIGN.md, also he helped me with code, reduced time to design and to improve code quality. There was one moment: In Store.claim(), the code ran a separate SELECT after UPDATE to fetch the job with its new attempt number and lease. I replaced both queries with UPDATE … RETURNING, which returns the updated row directly.
## Why SQLite?

I chose SQLite because everything runs on one host. The API and workers share a Docker volume, and the job status and its effect commit together. WAL lets reads continue during writes. SQLite allows one write transaction at a time, so workers keep those transactions short.

**SHIP at 100 HTTP requests/s for the tested workload.** Across six successful Docker runs, the service returned 99.98–100.00 responses/s over 60 seconds with zero errors and p99 of 4.5–5.3 ms. I would test PostgreSQL if workers needed to run on multiple hosts or if write contention limited throughput after adding workers.

The [design note](DESIGN.md) explains claiming, recovery, cancellation, and the effect guarantee.

## Run it

```bash
docker compose up --build
```

Open [Swagger UI](http://127.0.0.1:8000/docs). The Compose smoke test started four workers and completed a job on attempt 1 with one effect.

| Request | What it does |
|---|---|
| `POST /jobs` | Send JSON such as `{"run_ms":50,"outcome":"succeeded"}` with an `Idempotency-Key`. A new job returns `201`. The same key and payload return `200` with the same ID. A different payload with the same key returns `409`. |
| `GET /jobs/{id}` | Read the current status. |
| `GET /jobs?after=0&limit=20` | List jobs. Pass `next_after` as `after` for the next page. |
| `POST /jobs/{id}/cancel` | Returns `200` if the job is cancelled, or `409` if completion already committed. |

## Run the tests

Install the Python dependencies with `python3 -m pip install -r requirements.txt`.

Run the [chaos harness](chaos.py) with 8 workers and a fresh temporary database. It creates about 50 jobs/s, mixes in retries and cancels, and kills and replaces a random worker about every two seconds. Change `seed` to vary which workers are picked.

```bash
python3 -u - <<'PYTHON'
from pathlib import Path
from tempfile import TemporaryDirectory
from chaos import run_chaos

with TemporaryDirectory() as tmp:
    run_chaos(Path(tmp) / "chaos.sqlite3", jobs=10_000, seed=42)
PYTHON
```

Run the [load test](load_test.py) natively with `python3 -u load_test.py`. To run it in Docker, use a new project so it gets an empty volume:

```bash
project="soup-load-$(date +%s)"
docker compose -p "$project" -f compose.yaml -f compose.load.yaml up --build -d --wait
docker compose -p "$project" -f compose.yaml -f compose.load.yaml   exec -T api python -u /app/load_test.py
docker compose -p "$project" -f compose.yaml -f compose.load.yaml down -v
```

The load test schedules 6,000 requests in 60 seconds with four workers. Per second, it sends 50 creates, 20 retries, 15 status reads, 10 cancels, and 5 list requests. Simulated jobs take 50 ms. In Docker, the generator calls the API over loopback inside the API container.

Run the focused tests on a POSIX host with `python3 -u -m unittest discover -s tests -v`. The suite takes about five minutes because it pauses a real process for that long.

## Results and logs

**Chaos.** The five 10,000-job runs each finished with 7,059 succeeded, 441 failed, and 2,500 cancelled jobs. Each run sent 99 SIGKILLs. No keys were missing or duplicated, no jobs were left unfinished, and effect counts matched the final states.

| Run | Jobs reclaimed |
|---|---:|
| [Seed 42](runs/chaos-10000-seed42.1GWNao) | 65 |
| [Seed 43](runs/chaos-10000-seed43.Tjrugn) | 69 |
| [Seed 42 after `UPDATE … RETURNING`](runs/chaos-10000-after-returning-seed42.wbbEQO) | 66 |
| [Seed 42 after query and polling changes](runs/chaos-10000-sql-tuned-seed42.EVTKpU) | 81 |
| [Latest run, seed 42](runs/chaos-10000-expected-states-seed42.qLA8Gf) | 71 |

The chaos harness checks each job's final state against its configured outcome and the cancellation response. A succeeded job must have one effect from its final attempt. Failed and cancelled jobs must have none. The [latest run](runs/chaos-10000-expected-states-seed42.qLA8Gf) reports zero state or attempt mismatches. The [five verifier tests](tests/test_chaos_verifier.py) check that invalid results are rejected. [Raw test output](runs/chaos-verifier-tests.PwRksg)

The [400-job shakedown](runs/chaos-400-seed42.console.txt) passed with four kills. Its file is a transcript of the console output posted during development.

**100 requests/s.** The table shows eleven successful runs. Each run completed all 6,000 requests, had zero errors, and accounted for all 3,000 jobs. The rate below counts successful responses inside the 60-second window.

| Log | Responses/s | p95 / p99 |
|---|---:|---:|
| [Native baseline](runs/load-100rps.iHlGgg) | 99.98 | 3.4 / 4.2 ms |
| [Native after SQL change](runs/load-100rps.6b3Jbb) | 99.98 | 3.4 / 4.0 ms |
| [Native repeat](runs/load-100rps.BWMRKw) | 99.98 | 3.5 / 4.0 ms |
| [Native repeat](runs/load-100rps-repeat.yEulkG) | 100.00 | 3.6 / 4.6 ms |
| [Native repeat](runs/load-100rps-repeat.LjvzEy) | 100.00 | 3.3 / 4.0 ms |
| [Docker](runs/load-docker-100rps.bmFxHD) | 100.00 | 3.6 / 4.8 ms |
| [Docker repeat](runs/load-docker-100rps-repeat.TteQsw) | 100.00 | 3.4 / 4.5 ms |
| [Docker after empty-volume check](runs/load-docker-clean-db.3jqoWB) | 99.98 | 3.6 / 4.7 ms |
| [Docker control](runs/load-docker-100rps-instrumented.lnyRjv) | 100.00 | 3.8 / 4.9 ms |
| [Docker after query and polling changes](runs/load-docker-sql-tuned-100rps-4w.ShGNxO) | 100.00 | 3.9 / 5.0 ms |
| [Docker with per-operation timings](runs/api-profile-100rps-4w.oOJjdg) | 100.00 | 4.2 / 5.3 ms |

The expired-lease lookup uses the partial index on running jobs. [Query plans and timings](runs/lease-query-plan.SFMbHT)

### Where the API spends time

I used AI to add per-operation latency and time the SQLite steps inside job creation.
Here are the matching 60-second Docker runs before and after adding that instrumentation.
All four had zero request errors.

| Workload | p95 before → after | p99 before → after | Successful responses/s before → after |
|---|---:|---:|---:|
| 100 requests/s, 4 workers | 3.9 → 4.2 ms | 5.0 → 5.3 ms | 100.00 → 100.00 |
| 1,000 requests/s target, 32 workers | 5.7 → 3.3 ms | 58.3 → 22.8 ms | 997.32 → 997.52 |

[100 baseline](runs/load-docker-sql-tuned-100rps-4w.ShGNxO) ·
[100 with timings](runs/api-profile-100rps-4w.oOJjdg) ·
[1,000 baseline](runs/load-docker-sql-tuned-1000rps-32w-60s.Aq3EUN) ·
[1,000 with timings](runs/api-profile-1000rps-32w.ys9hHf)

One run in each configuration gives a comparison, with run-to-run variation.
The timings showed where to look next. At the higher load, create p99 was
39.48 ms. The SQLite writer wait had a p99 of 37.52 ms, compared with 0.53 ms
for connection setup and 1.23 ms for the transaction including commit.


The two 60-second Docker profiles broke down latency by operation and split
create time into connection setup, key lookup, writer wait, and transaction
time including commit. Both used 20-job pages. The timing header is enabled
by the load-test Compose override.

[100 requests/s, 4 workers](runs/api-profile-100rps-4w.oOJjdg) · [1,000 target, 32 workers](runs/api-profile-1000rps-32w.ys9hHf) · [Timing tests](runs/api-timing-tests.wQtffH) · [Test source](tests/test_sql_timing.py)

The measured response rates were 100.00 and 997.52 responses/s.
Values below are p99 in milliseconds for successful requests.

| Operation | 100 target | 1,000 target |
|---|---:|---:|
| Create | 5.78 | 39.48 |
| Repeat POST | 3.56 | 2.36 |
| Get status | 3.47 | 2.27 |
| List jobs | 3.22 | 3.72 |
| Cancel | 5.97 | 38.43 |

| Create phase | 100 target | 1,000 target |
|---|---:|---:|
| Connection setup | 0.472 | 0.529 |
| Read existing key | 0.063 | 0.277 |
| Wait for SQLite writer | 1.954 | 37.520 |
| Transaction and commit | 1.784 | 1.234 |
| Full route handler | 3.370 | 38.336 |

The only SQL phases recorded for repeat POST were connection setup and key lookup.
These percentiles come from separate distributions and cannot be subtracted
to attribute a specific request's delay.

### Silent failures

| Risk | What I checked |
|---|---|
| Double claim | Both workers passed the initial read. Only one claimed the job. |
| Stale worker | Attempt 1 could neither renew nor complete while attempt 2 was running. An expired attempt was also rejected before reclaim. |
| Cancellation race | Cancel first left zero effects. Complete first left one effect and returned `409` on cancel. |
| Request retry | Eight concurrent POSTs with one key returned one `201`, seven `200`s, and the same ID. A direct duplicate insert also failed the unique constraint. |
| Isolation | A separate reader saw `running` and zero effects before commit, then `succeeded` and one effect. |
| Clock changes | Injected clock jumps changed when a job could be reclaimed. Moving the clock backward after reclaim did not let attempt 1 write. |

The [claim tests](tests/test_claim_invariants.py) and [their original log](runs/claim-invariants.KjuQHo) cover races and fencing. The [full focused suite](runs/focused-tests-sql-tuned.sdm7IL) also includes [query regressions](tests/test_store_performance_invariants.py). The [earlier regression run](runs/sql-optimization-tests.TJesfd) is retained. The other checks include terminal snippets. Those also showed that the verifier catches a missing key and that replacing killed processes restores the worker count to eight.

### Limits and reflection

The five-minute freeze stood out. A replacement finished the job in 3.533 seconds. When the original worker came back, it could no longer write the result. I still want to test sustained 100 requests/s and worker kills together. The chaos run was closer to 79 HTTP requests/s, and the separate 100 requests/s runs had no kills. The tests also leave out power loss, host loss, and external effects. Random kills can miss specific points in a transaction. The separate pre-commit test covers one such point.

## Questions and test evidence

### What happens if a worker freezes for five minutes?

I stopped a worker after it claimed a job. A replacement finished attempt 2 after **3.533 s**. The original resumed after **300.004 s**, its completion was rejected, and the database had one effect from attempt 2. [Fault tests](tests/test_failure_windows.py) · [Raw log](runs/failure-windows.tgtbIG)

### Where is the design weakest?

A stopped process can hold SQLite's only writer lock. I paused one inside a write transaction. Another connection could read, but a create failed after **2.002 s** with `database is locked`. Killing the stopped process rolled back its staged effect and restored writes. An expired lease cannot free that lock. [Raw log](runs/failure-windows.tgtbIG)

### What breaks at 10× load?

**The queue grew first.** Four workers can complete at most about 80 jobs/s with 50 ms jobs. The 1,000 HTTP requests/s mix schedules 500 creates/s. In the Docker stress run, the queue reached **1,609 waiting jobs** at the first sample and **5,071** after the last response. It took **67.90 s** to drain. The client started **974.20 requests/s** in the window, and one create got a `503`. **DON’T SHIP at this target with four workers.** I would bound the backlog first, then add execution capacity and measure again. [Stress log, exit code 1](runs/load-1000rps.9DZEAh)

I repeated that workload after tuning SQLite.

| Workers | Window | Starts/s | Successful responses/s | Errors | Queued after responses | Raw log |
|---:|---:|---:|---:|---:|---:|---|
| 4 | 15 s | 998.53 | 998.40 | 0 | 4842 | [Log, exit 0](runs/load-docker-sql-tuned-1000rps-4w.WlB0ky) |
| 32 | 15 s | 998.47 | 998.20 | 0 | 0 | [Log, exit 0](runs/load-docker-sql-tuned-1000rps-32w.uE1acC) |
| 32 | 60 s | 997.35 | 997.32 | 0 | 0 | [Log, exit 0](runs/load-docker-sql-tuned-1000rps-32w-60s.Aq3EUN) |

To reproduce it, append `--rate 1000 --seconds 15 --clients 48 --drain-timeout 180` to the Docker load command. For the 32-worker run, add `--scale worker=32` to Compose startup and `--workers 32` to the load command. Use `--seconds 60` for the longer run.

### How do I know 10,000 jobs was enough?

The latest 10,000-job run recorded **71 reclaims** and checked every planned key. It cannot tell me which instruction each random kill interrupted. In a separate test I killed the process after it inserted the effect but before commit. The database still showed `running` with zero effects. Attempt 2 then finished with one effect. [Latest chaos log](runs/chaos-10000-expected-states-seed42.qLA8Gf) · [Pre-commit crash log](runs/failure-windows.tgtbIG)
