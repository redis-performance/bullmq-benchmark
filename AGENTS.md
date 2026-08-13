# Agent guidelines

Instructions for AI coding agents (Claude Code, Copilot, Cursor, etc.) working in this repo.

## Project overview

`bullmq-benchmark` is a BullMQ protocol load benchmark written in Rust. It measures job throughput (jobs/second) and full latency spectrum (p50 → p99.99) against any Redis endpoint. It is built on `bullmq-official` — BullMQ's own first-party Rust client — so the wire protocol (job add scripts, `BZPOPMIN` on a shared marker key, the `moveToActive` claim script, automatic completion) is the real thing, not a hand-rolled approximation. Latency is recorded per-job using HDRHistogram via a nanosecond timestamp embedded in each job's data payload. The tool supports multiple concurrency levels in a single run, multi-queue round-robin distribution, per-second time-series output, and emits results as both a formatted console table and a JSON file (schema-compatible with the sister tool `sidekiq-benchmark`, for side-by-side comparison). It is published as a Docker image (`redis/bullmq-benchmark`) and as a single static binary.

## Local setup

Requires Rust stable (1.85+ — pinned by the `bullmq-official` dependency). No git submodules — `bullmq-official` is a normal crates.io dependency.

```bash
git clone git@github.com:redis-performance/bullmq-benchmark.git
cd bullmq-benchmark
cargo build --release
```

Verify the build:

```bash
# Requires a running Redis on 127.0.0.1:6379
./target/release/bullmq-bench \
  --url redis://127.0.0.1:6379/0 \
  --workers 2 \
  --jobs 500 \
  --output -
```

## Branch naming

Same as human contributors: `<type>/<short-description>` (e.g. `fix/off-by-one-in-pipeline`).

## Coding standards

- Match the style already in the file you are editing.
- Prefer clear, minimal changes over large refactors unless explicitly asked.
- Do not add comments that describe *what* the code does — only add comments when the *why* is non-obvious.
- Do not introduce new dependencies without checking with the maintainer.
- Do not silently work around a `bullmq-official` API gap — if something in `FEATURE_PARITY.md` (upstream, `rust/FEATURE_PARITY.md` in `taskforcesh/bullmq`) is missing, say so explicitly in code comments and in the README rather than faking it with a raw Redis command.

## Running tests

Run the full suite before declaring a task complete:

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```

For a full end-to-end smoke test (requires Redis on `127.0.0.1:6379`):

```bash
cargo run --release -- \
  --url redis://127.0.0.1:6379/0 \
  --workers 2 \
  --jobs 500 \
  --timeout 60 \
  --output /tmp/smoke.json \
  --quiet \
  --tag smoke
```

To verify the wire protocol matches what's documented in the README, run a small trial while watching `redis-cli MONITOR` in another terminal — you should see `BZPOPMIN <prefix>:<queue>:marker <timeout>` calls interleaved with `EVALSHA` calls (the `moveToActive`/`moveToFinished` Lua scripts) whenever a worker has to wait for a job (e.g. `--workers 3 --jobs 2`, where the third worker starves and blocks).

Always run tests before declaring a task complete.

## How to submit changes

1. Create a branch: `git checkout -b <type>/<description>`.
2. Commit with a clear message focused on *why*, not *what*.
3. Open a pull request against `main`.
4. Do **not** push directly to `main`.

## What to avoid

- Do not reformat files unrelated to your change.
- Do not remove error handling or tests.
- Do not commit secrets, credentials, or large binary files.
- Do not amend published commits.
- Do not run the benchmark against a production Redis instance — it enqueues thousands of jobs per trial and can optionally flush the entire database.
- Do not bump the `bullmq-official` dependency to a new minor/major version without re-reading its `FEATURE_PARITY.md` and re-running the MONITOR-based protocol verification above — it is a brand-new (2026), still-evolving crate.
