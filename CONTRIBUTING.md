# Contributing

We treat this repo as "Open Source" within Redis: anyone who clears the bar below is welcome to contribute.

## Local setup

Requires Rust stable (1.85+, matching the `bullmq-official` dependency's MSRV) and a running Redis instance (6.2+, matching `bullmq-official`'s requirement).

```bash
git clone git@github.com:redis-performance/bullmq-benchmark.git
cd bullmq-benchmark
cargo build --release
```

To verify the build works end-to-end, spin up Redis and run a quick smoke test:

```bash
# Start Redis (or point REDIS_URL at an existing instance)
docker run --rm -d -p 6379:6379 redis:8

# Quick smoke test — 500 jobs, 2 workers, db 0
./target/release/bullmq-bench \
  --url redis://127.0.0.1:6379/0 \
  --workers 2 \
  --jobs 500 \
  --output -
```

## Branch naming

```
<type>/<short-description>
```

Types: `feat`, `fix`, `refactor`, `test`, `docs`, `chore`

Example: `feat/add-priority-jobs`

## Coding standards

- Keep changes focused; one logical change per PR.
- Follow the conventions already present in the codebase (formatting, naming, error handling).
- No dead code, no commented-out blocks.
- If a change touches protocol behavior (job add/claim/completion), verify it against `redis-cli MONITOR` on a small run — don't rely on reading `bullmq-official`'s source alone, since it's a new and fast-moving crate.

## Submitting changes

1. Fork or create a branch from `main`.
2. Make your changes with clear, atomic commits.
3. Open a pull request against `main` with a descriptive title and summary.
4. Address review comments promptly; force-push to the same branch to update.

## Testing

All new behaviour must be covered by tests. Existing tests must pass before opening a PR. Run the full suite locally:

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```

For a full end-to-end smoke test (requires a running Redis on `127.0.0.1:6379`):

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

Coverage should not decrease.

## Review process

- At least one maintainer approval is required before merge.
- CI must be green (format check, clippy, unit tests, smoke test all pass).
- Maintainers may request changes or close PRs that don't meet the bar — this is normal and not personal.
