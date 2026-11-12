# Training job queue



## Claiming and recovering a job

A worker starts a `BEGIN IMMEDIATE` transaction and looks for a queued
job or a running job whose lease has expired. It marks the job as
running, increments `attempt`, sets a 1.5-second lease, and commits.
Another worker checks for eligible jobs after it gets the write lock.

While a long job is running, the worker renews its lease every 0.5
seconds. If the worker dies or pauses long enough, another worker
can claim the job. If the original worker later comes back with
attempt 1 while the database says attempt 2, its write updates zero
rows. Both renewal and completion also check that the lease is valid.

Lease deadlines use SQLite's wall clock. A clock jump forward can
expire leases early. A jump backward can delay recovery.

## Cancellation and completion

If cancellation commits first, the job becomes `cancelled`. The
worker can no longer renew the lease or save a result because both
operations require `running`. If completion commits first, the
cancel endpoint returns `409` with the terminal job state.

## Guarantee

A succeeded job has exactly one committed row in `effects`. Failed
and cancelled jobs have none. The worker writes the success state
and the effect in one transaction, and `effects.job_id` is unique.
If the worker dies before commit, both writes roll back.

The computation may run again after a worker failure. The
exactly-once guarantee covers the SQLite row. An external side
effect would need its own protection against retries.