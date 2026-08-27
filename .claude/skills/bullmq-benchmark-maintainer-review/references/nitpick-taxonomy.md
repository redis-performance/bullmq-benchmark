# Cross-cutting nitpick taxonomy — bullmq-benchmark, real precedent only

Grounded in this repo's actual shipped bugs (issues #1 and #3, both fixed by real, merged PRs),
its one real, detailed, still-open gap (issue #5), and its own `AGENTS.md`/`CONTRIBUTING.md`/
`README.md` written norms. This repo's history is small (2 real bugs, 1 open gap) — that is being
honest about the actual size of the record, not a weakness to paper over. Do not treat a single
citation as broader precedent than it is.

1. **A CLI flag that's parsed but never threaded through to where it actually takes effect.** The
   single sharpest real bug in this repo's history: `--db` was accepted, stored on `Cli`, and simply
   never consulted by `build_redis_url` when the URL already carried a path — which the *default*
   URL (`redis://127.0.0.1:6379/13`) always does, so the flag was dead for every invocation,
   including the documented `--db 0` escape hatch (issue #3, fixed in PR#4). It was invisible without
   attaching `MONITOR` — no error, no warning, just a silently different database. Any PR that adds
   or touches a CLI flag should be traced end to end: does the parsed value actually reach the place
   it's supposed to change behavior, under every combination of "flag given" vs. "flag omitted" vs.
   "value implied by something else" (here, the URL's own path segment)? A flag that changes the
   `Cli` struct but not the code path it's named after is this project's own recurring failure mode.

2. **A failure that degrades to "looks unavailable" instead of "errors clearly."** Issue #1: the
   released `x86_64-unknown-linux-gnu` binary required `GLIBC_2.39`, so on any older host it failed
   to load at all — meaning `--version` produced a loader error, not a version string, which an
   automated harness reading `--version` to verify an installed build can't distinguish from "wrong
   version." Real result: five benchmark suites silently skipped on a Ubuntu 22.04 host while CI
   stayed green. A related, real second instance: a `v0.1.1` git tag shipped binaries that still
   reported `0.1.0` because the crate version wasn't bumped alongside the tag (see the `299bf47`
   commit message) — the same "unavailable and wrong-version are indistinguishable from outside"
   failure shape. Any PR touching build targets, release artifacts, or version reporting should be
   checked against this shape specifically: does a failure here produce a loud, specific error, or a
   generic "not found"/"wrong version" that a harness would read as absence rather than a bug?

3. **Any code relying on `bullmq-official` behavior must be checked against `MONITOR`, not just the
   crate's source — and any workaround for a crate gap or bug must be documented, never silent.**
   `bullmq-official` was first published 2026-07-12 and is explicitly called out (`AGENTS.md`,
   README's "Protocol compatibility" section) as new and fast-moving. `AGENTS.md` states this as a
   hard rule: *"Do not silently work around a `bullmq-official` API gap... say so explicitly in code
   comments and in the README rather than faking it."* The real precedent for following this rule
   correctly: `Queue::obliterate`'s `force` argument doesn't actually gate active-job removal (a real,
   verified upstream bug — `force_str` is never sent as an empty string, so `obliterate-2.lua`'s own
   guard clause can never fire), and the benchmark works around it itself via
   `Queue::get_active_count()` in `src/producer.rs::clear_queues`, documented in the README's
   "Protocol compatibility" and "Safety notes" sections and covered by a named regression test
   (`clear_queues_refuses_active_jobs_without_explicit_opt_in`). A PR that adds a new workaround for
   crate behavior without an equivalent code-comment + README explanation + test is a real regression
   against this project's own written and demonstrated norm — don't wave it through as a minor style
   point. Separately, `AGENTS.md` requires re-reading `FEATURE_PARITY.md` and re-running the
   MONITOR-based protocol check before bumping `bullmq-official` to a new minor/major version — flag
   a dependency bump PR that doesn't mention doing this.

4. **Redis Cluster / multi-key assumptions are a real, detailed, still-open gap — not yet fixed, but
   fully scoped.** Issue #5 (open) lays out three distinct, independent problems: (a) a plain
   `redis::Client` doesn't follow `MOVED`, so no cluster feature is enabled on the `redis` crate at
   all; (b) hash-tagging queue names is necessary but not sufficient without a cluster-aware
   connection that actually knows which node owns which slot; (c) a non-zero `--db` must be rejected
   with a clear message against a cluster endpoint, since cluster mode only exposes db 0 and
   currently just lets the server's own `SELECT is not allowed in cluster mode` error surface
   instead. If a PR claims to add or improve cluster support, check it against all three points, not
   just whichever one is most visible in the diff — issue #5 is explicit that hash tags alone were
   tried and found insufficient.

5. **Worker-spawn batching and the per-trial-level error boundary.** README documents, as a
   deliberate design choice, that `--workers N` spawns in bounded batches of 64 (rather than firing
   all connections at once) specifically because each `Worker` instance opens 2 Redis connections and
   spawns 3 background tasks, and warns above `--workers 256` to check `ulimit -n` and Redis's
   `maxclients`. It further documents that if a spawn batch fails at one concurrency level, that
   level alone is cleanly closed, skipped, and reported as a warning with a non-zero exit — without
   losing already-collected results from earlier, successful levels. A PR touching `run_trial`'s
   spawn loop or `main`'s per-level handling in `src/main.rs` should be checked against both
   properties (batching preserved; one level's failure doesn't discard prior levels' results), not
   just "does it still spawn the right number of workers."

6. **Destructive-operation gates must stay opt-in, default-safe, and tested against the specific
   failure they exist to prevent.** `--allow-flushdb` and `--allow-obliterate-active` are both
   documented as deliberately opt-in (env-var equivalents `BULLMQ_BENCH_ALLOW_FLUSHDB` /
   `BULLMQ_BENCH_ALLOW_OBLITERATE_ACTIVE`). The active-job gate specifically has a real regression
   test that reproduces the upstream `obliterate` `force`-flag bug this gate exists to work around
   (keeps a job active via a slow processor, confirms the ungated behavior would have removed it,
   confirms the gate blocks it). A PR touching either gate should preserve or extend the matching
   test, not just preserve the default boolean value — the value defaulting correctly doesn't prove
   the gate still does what its test says it does.

7. **Test coverage is real, explicit written doctrine, and — unlike some other repos in this org —
   has no counter-example yet showing it gets waived.** `CONTRIBUTING.md`: *"All new behaviour must
   be covered by tests... Coverage should not decrease."* The one substantive merged PR (#4) matches
   this exactly: three regression tests, one per `--db` resolution path. There is currently no merged
   PR in this repo's history with thin coverage to weigh against the written rule, so — unlike a
   project with a real history of low-coverage merges — there's no basis yet to say the rule is
   "written but not enforced" here. Apply it as real doctrine with a single, consistent data point
   behind it, and say so accurately rather than overclaiming a longer track record than one PR.

## What this taxonomy is honestly thin or silent on

- **Protocol-breaking or backward-incompatible CLI/output changes.** Both real PRs are pure bugfixes
  (a broken flag made to work as documented; a build target changed without changing CLI surface).
  Neither involved a deliberate breaking change to an existing flag's meaning or the JSON output
  schema (which README states is deliberately kept schema-compatible with `sidekiq-benchmark`). If a
  PR under review does this, there is no real precedent here to cite — reason about the tradeoff
  directly, and say plainly that this repo's own history doesn't yet give you a citable example.
- **Any actual reviewer pushback, requested changes, or back-and-forth on a real PR.** There is none
  in the mined history — see `review-history.md`. Do not imply otherwise.
- **Multi-contributor conflicts or diverging code style.** Every commit and PR to date has one
  author; there is no real precedent for reconciling two different people's conventions in this
  codebase yet.
