# The complete mined GitHub history — redis-performance/bullmq-benchmark

Mined via `gh api repos/redis-performance/bullmq-benchmark/commits`, `gh pr list --state all`,
`gh api .../pulls/<n>/reviews`, `gh issue list --state all`, and `gh api .../issues/<n>/comments`
on 2026-08-26/27, roughly two weeks after the repo's creation (2026-08-13). This is not a sample
or a survey — it is the entire history that exists. Read this before writing any review; the whole
point of this file is to prevent inventing precedent this repo doesn't have.

## The numbers, in full

- **10 commits total**, all authored by `fcostaoliveira`.
- **2 pull requests**, both by `fcostaoliveira`, both merged, both with **zero PR comments** and
  **zero recorded reviews** (`gh api repos/redis-performance/bullmq-benchmark/pulls/<n>/reviews`
  returns `[]` for both #2 and #4). Both merged within about 15 minutes of being opened:
  - **#2** (`04d8c5b`, 2026-08-17 22:15) — "build: ship static musl release binaries instead of
    glibc-linked ones." Fixes #1. Opened and merged same day.
  - **#4** (`bd5e496`, 2026-08-18 08:56) — "fix: --db was ignored, so every connection selected db
    13." Fixes #3. Opened and merged the next morning.
- **3 issues**, all filed by `fcostaoliveira` against his own project, **zero comments on any of
  them**:
  - **#1** (closed by #2) — GLIBC_2.39 floor on the released binary breaks it on Ubuntu 22.04 /
    Debian 12 / RHEL 9; degrades to a silent "tool unavailable" skip in an automated harness rather
    than a clear error.
  - **#3** (closed by #4) — `--db` accepted and parsed but discarded; connection always issues
    `SELECT 13` regardless of the value passed.
  - **#5** (still **open**) — cluster-mode Redis isn't supported: plain `redis::Client` doesn't
    follow `MOVED`; hash-tagging queue names alone isn't sufficient without a cluster-aware
    connection; a non-zero `--db` needs to be rejected against a cluster endpoint with a clear
    message instead of surfacing the server's own error. Includes a concrete suggested shape
    (`cluster-async` feature, `INFO cluster` auto-detection, hash-tag-aware queue naming, fail-fast
    on `--db` + cluster) and an offer from the filer to send a PR.
- **No branch protection on `main`** (`gh api .../branches/main/protection` → `404 Branch not
  protected`).

## What this means, plainly

There is no multi-person review dialogue in this repo's history to mine a "voice" from — not a thin
one, none at all. Compare this honestly to a project like redisbench-admin, whose own maintainer-
review skill found exactly one real, evidenced multi-point human review (kei-nan, PR#541) to use as
a template: this repo's mined history has zero equivalents. If a future PR here does draw a real,
substantive human review comment, that will be genuinely new precedent worth folding into this file
— don't backfill one that doesn't exist yet.

The 8 commits that didn't go through a PR at all include a mid-size one (`6751573`, "Harden against
adversarial input, add real-Redis integration tests, release automation") and same-day hotfixes
following both real PRs (`299bf47` and `d8a032f` after #2; `5b56ac6` after #4) — i.e., direct pushes
to `main` are not a one-off exception in this history, they are most of it by commit count.

## Written process vs. real practice — two live gaps, stated plainly

Both of this repo's own governance documents state rules the real history (so far) doesn't match:

- `CONTRIBUTING.md`: *"At least one maintainer approval is required before merge."* Real practice:
  2 of 2 real PRs were opened and merged by the same person with no review of any kind recorded.
- `AGENTS.md`: *"Do not push directly to `main`."* Real practice: 8 of this repo's 10 commits went
  directly to `main`.

Treat both as genuine written intent from the project's own maintainer, not as demonstrated,
enforced practice — a normal state for a two-week-old, single-maintainer repo, but worth being
accurate about rather than asserting either rule is actively enforced today. Don't conclude the
opposite either (that the rules are dead or ignorable) — there's no evidence either way from n=2/n=10.

## The one real signal: fcostaoliveira's own descriptive rigor

Despite the total absence of review dialogue, both real PR descriptions (and the commit messages for
the direct-to-main pushes) share a consistent, evidenced structure worth treating as this project's
actual quality bar, even though it has only ever been self-applied:

- A clear problem statement with the exact code path at fault (PR#4 quotes the literal `if
  u.path().trim_matches('/').is_empty()` branch that never fired).
- A quantified account of real-world blast radius, not a vague "this could cause issues": PR#4 —
  *"five benchmark suites each dying within ~5 seconds against a single-database endpoint, having
  written no output file"*; PR#2 — *"five benchmark suites skipped on a jammy host, CI green,
  nothing measured."*
- Empirical, wire-level or binary-level verification, not just reading source: PR#4 verifies via
  `redis-cli MONITOR` with an explicit before/after table of `SELECT` calls; PR#2 verifies via
  `objdump`-equivalent GLIBC-symbol inspection and a fresh musl build's `file`/exit-code output.
- Regression tests scoped exactly to the paths that broke (PR#4: three tests, one per `--db` code
  path: explicit override, omitted-with-URL-db, omitted-with-no-URL-db).
- Explicit self-flagged uncertainty for a reviewer, even when none showed up: PR#2's "Two notes for
  review" section raises the asset-rename risk and questions whether `musl-tools` is even necessary,
  unprompted.

This is real, evidenced material — just be precise that it is the *author's own* self-review
discipline, not a maintainer's demonstrated standard for *other people's* PRs, since no other
contributor's PR exists yet in this repo's history to check that against.
