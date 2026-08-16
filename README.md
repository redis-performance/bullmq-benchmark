# bullmq-benchmark

[![CI](https://github.com/redis-performance/bullmq-benchmark/actions/workflows/ci.yml/badge.svg)](https://github.com/redis-performance/bullmq-benchmark/actions/workflows/ci.yml)
[![Docker Pulls](https://img.shields.io/docker/pulls/redis/bullmq-benchmark)](https://hub.docker.com/r/redis/bullmq-benchmark)
[![Docker Image Size](https://img.shields.io/docker/image-size/redis/bullmq-benchmark/latest)](https://hub.docker.com/r/redis/bullmq-benchmark)
[![Docker Platforms](https://img.shields.io/badge/platform-linux%2Famd64%20%7C%20linux%2Farm64-blue)](https://hub.docker.com/r/redis/bullmq-benchmark)

A BullMQ protocol load benchmark written in Rust. Measures job throughput and
full latency spectrum (p50→p99.99) against any Redis endpoint, using BullMQ's
own first-party Rust client so the wire traffic is the real thing — not a
hand-rolled approximation.

This is the sister tool to
[`sidekiq-benchmark`](https://github.com/redis-performance/sidekiq-benchmark):
same CLI shape, same JSON output schema, same HDR-histogram latency
methodology — so results from both are directly comparable. The two tools
model opposite queue topologies on purpose; see
[Protocol compatibility](#protocol-compatibility) below.

## Why Rust?

| | BullMQ (Node.js) | This tool |
|---|---|---|
| Concurrency model | Single-threaded event loop per process | Tokio async tasks — no event-loop contention |
| Latency recording | None built in | HDRHistogram per job (p50→p99.99) |
| Per-second time series | None | Throughput + latency percentiles + errors |
| Multi-queue | Requires N separate processes/scripts | `--num-queues N`, one binary, one run |
| Dependency | `bullmq` npm package + Node.js runtime | Single static binary |

## Protocol compatibility

This benchmark is built on
[`bullmq-official`](https://crates.io/crates/bullmq-official) — BullMQ's own
first-party Rust client, published by BullMQ's creator
([Manuel Astudillo](https://github.com/manast)). Source:
[`taskforcesh/bullmq`, `rust/` subdirectory](https://github.com/taskforcesh/bullmq/tree/master/rust).
Docs: [docs.bullmq.io/rust/introduction](https://docs.bullmq.io/rust/introduction).

**Crate maturity note:** `bullmq-official` is brand new — first published to
crates.io on 2026-07-12, with the version this benchmark is built and tested
against (**1.2.5**) published 2026-08-15 (1.2.5's only change since 1.2.4 was
an internal `SCRIPT EXISTS` check before loading Lua scripts —
[taskforcesh/bullmq#4563](https://github.com/taskforcesh/bullmq/pull/4563) —
no API or protocol changes). It is still evolving quickly. Every protocol
claim below was checked against that exact version's source and verified
empirically with `redis-cli MONITOR` (not just read off the docs) — see the
exact commands captured in [Verifying the protocol](#verifying-the-protocol).

**Known upstream bug (worked around here):** `Queue::obliterate(force, ..)`'s
`force` argument doesn't actually gate removal of ACTIVE (in-flight) jobs.
`obliterate-2.lua`'s own guard is `if ARGV[2] == "" then return -2 end`, but
the Rust binding always sends `force_str = if force {"1"} else {"0"}` — never
an empty string — so that branch can never fire; active jobs are
force-removed regardless of what `force` is set to. Verified empirically (a
job kept genuinely active by a slow processor survived `obliterate(false,
..)`). This benchmark works around it in `src/producer.rs::clear_queues` by
checking `Queue::get_active_count()` itself before ever calling `obliterate`
— see [Safety notes](#safety-notes) below for what that means for
`--allow-obliterate-active`.

- **Dequeue is `BZPOPMIN` on a shared marker key, not `BRPOP`.** Every BullMQ
  worker blocks on `BZPOPMIN bull:<queue>:marker <timeout>` — a sorted set
  shared by every worker on that queue, so N workers means N clients blocked
  on **one** key. This is the inverse of Sidekiq's topology (one client
  `BRPOP`-ing across potentially many queue keys). Source:
  [`worker.rs`, `wait_for_marker`](https://github.com/taskforcesh/bullmq/blob/master/rust/src/worker.rs)
  (search `BZPOPMIN`).
- **Claiming a job runs the `moveToActive` Lua script, not a bare command.**
  When the marker fires (or when jobs are already waiting), the worker
  doesn't just read a value — it calls a Lua script that atomically moves the
  job from `wait` to `active`, renews its lock, and returns the full job hash
  in one round trip. This benchmark uses the crate's real `Worker` for this,
  so the atomicity matches production exactly; it never fakes it with a raw
  `BZPOPMIN` + `GET`. Source: [`worker.rs`, `run_main_driver` /
  `try_fetch_job_fast`](https://github.com/taskforcesh/bullmq/blob/master/rust/src/worker.rs)
  (search `moveToActive`).
- **Completion runs the `moveToFinished` script automatically.** The crate's
  worker loop calls it internally the moment the processor callback returns
  `Ok(_)` — user code never calls it directly. This benchmark's processor
  always returns `Ok(_)`, so every job's full lifecycle is measured end to
  end: `BZPOPMIN` wakeup → `moveToActive` claim → processor → `moveToFinished`
  completion. A benchmark that stopped at the claim and skipped completion
  would understate real load, since the completion script is itself a Redis
  round trip production workers always pay.
- **Enqueue runs `addStandardJob` / `addPrioritizedJob` / `addDelayedJob`
  Lua scripts, not a raw `LPUSH`.** `Queue::add_bulk` (used by this
  benchmark's producer) executes one such script per job over the
  multiplexed connection. This is inherently heavier than Sidekiq's single
  `LPUSH` pipeline — see the `--jobs` default note below. Source:
  [`queue.rs`, `add_bulk`](https://github.com/taskforcesh/bullmq/blob/master/rust/src/queue.rs).
- **Latency is measured with a self-embedded timestamp, same pattern as
  sidekiq-benchmark.** `bullmq-official`'s `Job::timestamp()` exists but is
  millisecond-granularity and reflects when the Lua script stamped the job,
  not something cheaply diffable from inside the worker. This benchmark
  embeds its own nanosecond `enqueued_at_ns` directly in the job's `data`
  JSON (see `src/job.rs`) and reads it back in the worker's processor
  callback — giving full microsecond resolution end to end regardless of
  what the crate exposes.
- **N workers = N separate `Worker` instances, not one `Worker` at higher
  concurrency.** `bullmq-official`'s `Worker` runs exactly one blocking-fetch
  driver internally *regardless of its `concurrency` setting* — this
  deliberately mirrors Node.js BullMQ's single-driver `mainLoop` to avoid a
  thundering herd. One `Worker` at `concurrency = N` therefore gives you one
  `BZPOPMIN` waiter feeding N concurrent *processing* tasks, not N
  independent waiters. To reproduce the real production topology — N worker
  processes, each with its own connection, all blocked on the same marker
  key — this benchmark spawns N separate `Worker` instances at
  `concurrency = 1` each. See the full rationale in `src/worker.rs`.

  **Resource cost of this:** each `Worker` instance opens 2 Redis
  connections (one multiplexed command connection, one dedicated blocking
  connection for `BZPOPMIN`) and spawns 3 background tasks (main loop,
  stalled-job check, lock renewal). `--workers 500` therefore needs roughly
  1,000 Redis connections. This benchmark spawns workers in bounded batches
  of 64 (rather than firing all N connections at Redis simultaneously) and
  prints a warning above `--workers 256` reminding you to check `ulimit -n`
  and the Redis server's `maxclients` first. If a spawn batch still fails
  (fd/connection exhaustion), that concurrency level is cleanly closed and
  skipped — printed as a warning, with the process exiting non-zero — rather
  than the whole run losing every earlier level's already-collected results;
  see `run_trial`'s spawn loop and `main`'s per-level error boundary in
  `src/main.rs`.

### What `bullmq-official` doesn't implement yet (and why it doesn't affect this benchmark)

Checked against the crate's own
[`FEATURE_PARITY.md`](https://github.com/taskforcesh/bullmq/blob/master/rust/FEATURE_PARITY.md)
(upstream, dated 2026-06-18) at the pinned version:

- `Job.wait_until_finished()` is intentionally unimplemented (a Node.js
  testing convenience, prone to misuse in production). This benchmark
  doesn't use it — it drives completion detection through its own
  throughput/latency counters, same as sidekiq-benchmark does with Sidekiq.
- Redis Cluster and Sentinel support are not implemented. This benchmark
  only targets a single Redis endpoint anyway (matching sidekiq-benchmark's
  scope), so this is not a gap for our purposes.
- Legacy/maintenance queue methods (`remove_orphaned_jobs`, the legacy
  repeatable-job API) are not implemented. This benchmark doesn't use delay,
  repeat, or cron scheduling — only plain jobs — so these are out of scope.
- Everything this benchmark actually needs — `Queue::add_bulk`,
  `Queue::obliterate`, and the full `Worker` processing loop (concurrency,
  stalled-job detection, lock renewal, automatic completion) — is
  implemented and covered by the crate's own integration test suite.

### Verifying the protocol

Every claim above was checked live, not just read off the source. With a
worker genuinely starved for work (fewer jobs than workers, so at least one
worker has to block):

```bash
redis-cli MONITOR &
./target/release/bullmq-bench --workers 3 --jobs 2 --timeout 8 --quiet --tag check
```

`MONITOR` shows exactly:

```
"BZPOPMIN" "bull:default:marker" "5.0"
...
"EVALSHA" "<sha of moveToActive>" "11" "bull:default:wait" "bull:default:active" ...
...
"EVALSHA" "<sha of moveToFinished-family script>" ...
```

(5.0 is `WorkerOptions::drain_delay` in seconds — the default re-block
interval while a worker waits for new jobs.)

## Quick start

Three ways to get `bullmq-bench` running, in order of convenience:

### 1. Docker

The image is published to
[`redis/bullmq-benchmark`](https://hub.docker.com/r/redis/bullmq-benchmark)
on Docker Hub (`linux/amd64` + `linux/arm64`, rebuilt on every push to
`main`). This is the fastest way to run it — no toolchain required.

> **Memory:** BullMQ jobs are heavier than Sidekiq's — each job is a Redis
> hash (name, data, options, timestamps, …) plus wait/marker zset entries,
> versus Sidekiq's single serialized list entry. The default run enqueues
> 20,000 jobs (~600 B each) → **~12 MB** peak Redis memory per trial. Trials
> clean up after themselves via `Queue::obliterate` before the next one runs.

```bash
docker pull redis/bullmq-benchmark:latest

# Run against local Redis (default: db 13, 20k jobs, workers 10/50/100/200)
docker run --rm --network host redis/bullmq-benchmark:latest

# Lighter local run
docker run --rm --network host redis/bullmq-benchmark:latest \
  --workers 10,50 --jobs 5000

# Custom settings
docker run --rm --network host redis/bullmq-benchmark:latest \
  --url redis://127.0.0.1:6379/0 \
  --workers 10,50,100 \
  --jobs 50000 \
  --num-queues 4

# Point at a remote Redis
docker run --rm redis/bullmq-benchmark:latest \
  --url redis://myhost:6379/0 \
  --workers 50,100,200 \
  --jobs 200000 \
  --output -
```

**docker compose (Redis included):**

```bash
# Start Redis + run benchmark
docker compose run --rm bench

# Use a different Redis image
REDIS_IMAGE=redis:7.4 docker compose run --rm bench

# Point at an external Redis
REDIS_URL=redis://myhost:6379/0 docker compose run --rm bench
```

### 2. Install from a GitHub Release

Static Linux binaries (`x86_64`, `aarch64`) are attached to each
[GitHub Release](https://github.com/redis-performance/bullmq-benchmark/releases),
each as a tarball with a `.sha256` checksum alongside it.

```bash
# linux-x86_64
curl -fsSL -O https://github.com/redis-performance/bullmq-benchmark/releases/download/v0.1.0/bullmq-bench-v0.1.0-linux-x86_64-gnu.tar.gz \
  -O https://github.com/redis-performance/bullmq-benchmark/releases/download/v0.1.0/bullmq-bench-v0.1.0-linux-x86_64-gnu.tar.gz.sha256
sha256sum -c bullmq-bench-v0.1.0-linux-x86_64-gnu.tar.gz.sha256
tar -xzf bullmq-bench-v0.1.0-linux-x86_64-gnu.tar.gz
./bullmq-bench-v0.1.0-linux-x86_64-gnu/bullmq-bench --workers 10,50,100,200 --jobs 20000

# linux-aarch64
curl -fsSL -O https://github.com/redis-performance/bullmq-benchmark/releases/download/v0.1.0/bullmq-bench-v0.1.0-linux-aarch64-gnu.tar.gz \
  -O https://github.com/redis-performance/bullmq-benchmark/releases/download/v0.1.0/bullmq-bench-v0.1.0-linux-aarch64-gnu.tar.gz.sha256
sha256sum -c bullmq-bench-v0.1.0-linux-aarch64-gnu.tar.gz.sha256
tar -xzf bullmq-bench-v0.1.0-linux-aarch64-gnu.tar.gz
./bullmq-bench-v0.1.0-linux-aarch64-gnu/bullmq-bench --workers 10,50,100,200 --jobs 20000
```

### 3. Build from source

```bash
cargo build --release
./target/release/bullmq-bench --workers 5 --jobs 2000
```

## CLI flags

| Flag | Env | Default | Notes |
|---|---|---|---|
| `--url` | `REDIS_URL` | `redis://127.0.0.1:6379/13` | Full Redis URL |
| `--host` | — | — | Override host component of URL |
| `--port` | — | — | Override port component of URL |
| `--password` | `REDIS_PASSWORD` | — | Auth (prefer env var — CLI exposes it in `ps`) |
| `--tls` | `REDIS_TLS` | false | Enable TLS (`rediss://`) |
| `--db` | — | `13` | Database number (safety convention carried over from sidekiq-benchmark; BullMQ has no special default db) |
| `--workers` | — | `10,50,100,200` | Comma-separated concurrency levels — one trial each. Each level spawns that many separate `bullmq::Worker` instances |
| `--jobs` | — | `20000` | Total jobs per trial (lower than sidekiq-benchmark's 500,000 — see Protocol compatibility) |
| `--warmup-jobs` | — | `0` | Warmup pass before each trial (0 = skip) |
| `--queue` | — | `default` | Base BullMQ queue name |
| `--num-queues` | — | `1` | Number of independently-named queues; workers and jobs are distributed round-robin across them. Names are `<queue>_0…<queue>_{N-1}` when N > 1 |
| `--latency-percentiles` | — | `p50,p90,p99,p999,max` | Per-second latency series to record; supports `p50`, `p75`, `p90`, `p95`, `p99`, `p999`, `p9999`, `max`, `mean` |
| `--tag` | — | from Redis `INFO` | Label for output filename and JSON |
| `--output` | — | `bullmq_bench_<tag>.json` | JSON output path; `-` for stdout |
| `--timeout` | — | `300` | Per-trial timeout in seconds |
| `--quiet` | — | false | Suppress per-second progress dots |
| `--allow-flushdb` | `BULLMQ_BENCH_ALLOW_FLUSHDB` | false | `FLUSHDB` before each trial (default: `Queue::obliterate` on only the configured queue(s) — safe on shared Redis) |
| `--allow-obliterate-active` | `BULLMQ_BENCH_ALLOW_OBLITERATE_ACTIVE` | false | Allow pre-trial cleanup to force-remove ACTIVE (in-flight) jobs from the target queue(s). See [Safety notes](#safety-notes) |

### Multi-queue mode

Unlike Sidekiq's `BRPOP` — which can watch several queue keys from a single
connection — a BullMQ `Worker` binds to exactly one queue name. So
`--num-queues N` here means launching separate `Worker` instances per queue
(workers distributed round-robin across queue names), not one client
watching several keys. This is the opposite direction of
sidekiq-benchmark's multi-queue mode:

```bash
# Single queue
bullmq-bench --workers 100 --jobs 50000 --num-queues 1

# 4 independently-named queues, workers split 25 per queue
bullmq-bench --workers 100 --jobs 50000 --num-queues 4
```

If `--num-queues` exceeds the smallest `--workers` level, some queues get
zero workers in that trial — the tool prints a warning up front so this
doesn't happen silently.

## Output

**Console:**
```
=== bullmq-bench — redis-8.6.0 ===
    redis://127.0.0.1:6379/13  jobs=20,000  queues=default

  [  10 workers] ........  3,435 jobs/s  p50=557.1 ms  p99=863.2 ms  p99.9=871.9 ms  max=874.0 ms
  [  50 workers] ........  4,914 jobs/s  p50=497.4 ms  p99=728.6 ms  p99.9=736.8 ms  max=737.3 ms

--- Summary ---
+---------+--------+----------+----------+----------+----------+--------+
| Workers | jobs/s | p50      | p99      | p99.9    | max      | errors |
+---------+--------+----------+----------+----------+----------+--------+
| 10      | 3,435  | 557.1 ms | 863.2 ms | 871.9 ms | 874.0 ms | 0      |
| 50      | 4,914  | 497.4 ms | 728.6 ms | 736.8 ms | 737.3 ms | 0      |
+---------+--------+----------+----------+----------+----------+--------+
Results saved → bullmq_bench_redis-8.6.0.json
```

Progress shows `.` per second, or `[e:N]` when errors occur in that window so
nothing is silently swallowed.

**JSON** (`bullmq_bench_<tag>.json`) — schema-compatible with
sidekiq-benchmark's output, so both tools' results can be plotted side by
side:

```json
{
  "tag": "redis-8.6.0",
  "timestamp": "2026-08-13T14:00:00Z",
  "config": {
    "url": "redis://127.0.0.1:6379/13",
    "workers": [10, 50, 100, 200],
    "jobs_per_trial": 20000,
    "queues": ["default"],
    "warmup_jobs": 0
  },
  "results": [{
    "workers": 10,
    "total_jobs": 20000,
    "duration_s": 5.82,
    "jobs_per_sec": 3435.4,
    "timed_out": false,
    "throughput_per_sec": [3400, 3450, 3420],
    "errors_per_sec":     [0, 0, 0],
    "latency_per_sec_us": {
      "p50":  [556000, 558000, 557500],
      "p99":  [862000, 864500, 863800]
    },
    "latency_us": {
      "p50": 557100, "p75": 700000, "p90": 810000,
      "p95": 830000, "p99": 863200, "p99_9": 871900,
      "p99_99": 873500, "max": 874000,
      "mean": 561200.0, "total_count": 20000
    },
    "errors": 0
  }]
}
```

All latency values are in **microseconds**. `latency_per_sec_us` contains one
value per elapsed second of the trial, making it easy to plot latency
stability over time or spot degradation as the queue drains.

> **Note on latency:** the benchmark bulk-enqueues the full job count, then
> starts workers. Latency = time a job spends waiting until its
> `moveToActive` claim completes (wall-clock, same host as producer). Since
> the queue is pre-filled before workers start, most trials never actually
> block on `BZPOPMIN` — see [Verifying the protocol](#verifying-the-protocol)
> for how to observe the blocking path directly.

> **Password safety:** passwords passed via `--password` are visible in `ps aux`.
> Prefer the `REDIS_PASSWORD` environment variable. Passwords are redacted
> (`****`) in all output and JSON.

## Safety notes

### Default database: 13

The default Redis database is **13**, a convention carried over from
sidekiq-benchmark (which itself matches Ruby Sidekiq's own
`bin/sidekiqload` default) rather than anything BullMQ specifies. This
avoids colliding with application data and makes `--allow-flushdb` safe by
default. Always confirm the target db before running against a shared Redis.

### Shared / production Redis

Do **not** run this benchmark against a production Redis instance. Each
trial enqueues thousands of jobs and (optionally) flushes the entire
database. Use a dedicated benchmark instance or an isolated database number.

### Active-job removal is gated behind `--allow-obliterate-active`

Pre-trial cleanup uses `Queue::obliterate`, which only ever touches keys
under the target queue's own prefix — it cannot affect unrelated queues in
the same Redis/db. It *can* still affect the same-named queue of a real,
unrelated BullMQ application if `--queue` collides with a real production
queue name (`"default"`, this tool's own default, is also the single most
common real one).

By default, if the target queue currently has ACTIVE (in-flight) jobs —
jobs a real worker might be processing *right now* — pre-trial cleanup
refuses to touch that queue at all and fails with a clear error, instead of
destroying them. Pass `--allow-obliterate-active` (or set
`BULLMQ_BENCH_ALLOW_OBLITERATE_ACTIVE=1`) to override — the main legitimate
reason to need it is cleaning up after this benchmark's *own* trial timed
out and left active jobs behind. Non-active jobs (waiting/completed/failed/
delayed) are always cleared either way; this gate only concerns work
genuinely in flight.

This check is implemented ourselves (via `Queue::get_active_count()`)
rather than by relying on `Queue::obliterate`'s own `force` parameter — see
[Protocol compatibility](#protocol-compatibility) above for the upstream bug
that makes `force` alone insufficient. Covered by
`clear_queues_refuses_active_jobs_without_explicit_opt_in` in
`tests/integration_redis.rs`, which reproduces the upstream bug directly
(keeps a job active with a slow processor, confirms `force=false` behavior
without this gate would have removed it, then confirms the gate blocks it).

### Jobs are removed on completion — unlike BullMQ's own default

Node.js BullMQ's out-of-the-box default is to *keep* every completed job's
hash in Redis forever unless `removeOnComplete`/`removeOnFail` is
configured. For a benchmark that can process hundreds of thousands of jobs
per trial, that default would balloon Redis memory and skew later trials
with leftover state. This benchmark sets `removeOnComplete` /
`removeOnFail` to immediate removal for every job — a deliberate departure
from BullMQ's production default, made purely for benchmark hygiene.

## Building

Requires Rust stable (1.85+ — pinned by the `bullmq-official` dependency's MSRV).

```bash
cargo build --release
cargo test
cargo clippy --all-targets -- -D warnings
```

## Docker image

Multi-platform image (`linux/amd64`, `linux/arm64`) published to
[`redis/bullmq-benchmark`](https://hub.docker.com/r/redis/bullmq-benchmark)
on every push to `main`. Tagged `latest` on main; semver tags (`0.1.0`,
`0.1`) on `v*` git tags. See [Quick start](#quick-start) above for
`docker pull`/`docker run` usage.

```bash
# Build locally instead of pulling
docker build -t bullmq-bench .
docker run --rm bullmq-bench --url redis://host:6379/0 --workers 10 --jobs 5000
```

## License

Apache-2.0
