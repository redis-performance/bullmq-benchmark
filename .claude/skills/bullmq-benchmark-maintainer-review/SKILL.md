---
name: bullmq-benchmark-maintainer-review
description: Review a redis-performance/bullmq-benchmark pull request, branch, or diff in a way that is honestly grounded in this specific, brand-new repo's real (very thin) GitHub history — not generic Rust code-review advice, and not a borrowed "maintainer voice" from a different, older project. Use this whenever the user asks to review a bullmq-benchmark PR, asks whether a bullmq-benchmark PR would pass real review, wants a bullmq-benchmark-specific pre-merge check, or is deciding accept/reject on a redis-performance/bullmq-benchmark PR. Prefer this over a generic code-review skill for anything touching redis-performance/bullmq-benchmark — the generic skill doesn't know this project's real (sole) maintainer, its actual bug history, or where its written process and its practice diverge.
---

# bullmq-benchmark maintainer-style review

You're standing in for this repo's real review process — which, as of this writing, means one person:
**fcostaoliveira** (Filipe Oliveira). He is the author of every commit, every PR, and every issue in this
repo's entire history to date. This skill's honest job is to apply the *technical* rigor his own work
already demonstrates, to a PR he (or anyone else) hasn't reviewed yet — not to imitate a "reviewer voice"
built from real dialogue, because essentially none exists yet. `references/review-history.md` has the full,
literal mined record (every commit, PR, and issue); `references/nitpick-taxonomy.md` has the concrete,
evidenced bug-class checklist this repo's own real bugs support. Read both before writing anything.

## Why this matters: an honesty warning, sharper than usual

**This repo is two weeks old (created 2026-08-13) and has no review culture to mine yet — be explicit about
that rather than manufacturing one.** The entire history is: 10 commits, 2 pull requests, 3 issues. Both PRs
were opened, authored, and merged by fcostaoliveira himself, each within about 15 minutes of opening, with
**zero PR comments and zero recorded reviews** (`gh api .../pulls/<n>/reviews` returns `[]` for both). All
three issues were also filed by fcostaoliveira against his own project, and none has a single comment. There
is no branch protection on `main` (`gh api .../branches/main/protection` → 404). This is meaningfully thinner
than even a "thin" project: redisbench-admin's own maintainer-review skill found at least one real, multi-point
human review to use as a template (kei-nan, PR#541); the mined history here has **no equivalent at all**. Do
not borrow that project's dialogue, its voice profiles, or its "APPROVED with silence is the norm" conclusion
— that conclusion was earned by ~250 real PRs there; here it would be extrapolated from n=2 self-merges by the
one person who wrote the whole codebase, which isn't the same claim.

Also be honest about written-process-vs-practice gaps, since both exist here already despite the tiny sample:
- `CONTRIBUTING.md` states "At least one maintainer approval is required before merge" — real practice so far
  is 2/2 PRs self-merged with no review of any kind recorded.
- `AGENTS.md` states "Do not push directly to `main`" — real practice is 8 of this repo's 10 commits (including
  a mid-size "Harden against adversarial input, add real-Redis integration tests, release automation" commit
  and a same-day toolchain hotfix) went straight to `main`, not through a PR.

Neither observation means the rules are wrong — a brand-new, single-maintainer bootstrap phase is a normal
reason for both — but state them as "written intent, not yet demonstrated as enforced," the same way you'd
be accurate about a coverage gate that exists on paper but hasn't been tested by a real low-coverage PR yet.
Don't imply either rule is dead; just don't cite either as settled practice.

**What real signal *does* exist, and is worth imitating:** fcostaoliveira's own PR descriptions and commit
messages set a genuinely high, consistent technical bar, evidenced across all the real ones (`#2`, `#4`, and
several direct-to-main commits): a clear problem statement, a quantified account of the failure's real-world
blast radius ("five benchmark suites died within ~5s, having written no output"), a before/after table verified
empirically (via `redis-cli MONITOR` for anything protocol-touching, or `objdump -T` for the glibc-floor bug),
regression tests added for exactly the paths that broke, and — in PR#2 — an explicit "Two notes for review"
section where the author raises his own uncertainties (an asset-name rename, whether `musl-tools` is even
needed) for a reviewer who, in that instance, never actually showed up. Treat that self-review discipline as
the real bar this project holds itself to, and hold a PR under review to the same bar — not as "what a
maintainer historically demanded of others," since no maintainer has yet had the chance to.

**Scope gate, before anything else:** if the PR touches nothing under `src/`, `tests/`, `Cargo.toml`,
`Dockerfile`, or `.github/workflows/` — e.g. it's a pure `README.md`/`LICENSE` edit with no technical claim to
check — say so in one sentence and treat it as out of scope for the taxonomy below rather than force-fitting it.

## Process

1. **Get the material.** `gh pr view <n> --repo redis-performance/bullmq-benchmark --json body,commits,files,author`
   and `gh pr diff <n> --repo redis-performance/bullmq-benchmark`. Read the PR description in full first — if
   it already includes a MONITOR-verified before/after table or a quantified impact statement in the style
   above, acknowledge that rather than re-deriving it.

2. **Assess author trust and diff risk.** `gh pr list --author <login> --state merged --repo
   redis-performance/bullmq-benchmark` will almost certainly show either fcostaoliveira (the only author with
   any merged-PR history at all right now) or a first-time contributor — there is no real in-between precedent
   in this repo yet. Let diff risk (does it touch the wire protocol, a CLI flag's actual effect, a safety gate
   like `--allow-flushdb`/`--allow-obliterate-active`, or release/build artifacts?) drive scrutiny more than
   author history, since author history here is not yet a meaningful signal either way.

3. **Work the checklist** in `references/nitpick-taxonomy.md` — every category there is grounded in a real,
   already-shipped bug or an explicitly documented, still-open gap in this exact codebase, not generic Rust
   advice. Give real weight in particular to:
   - **A CLI flag that's parsed but never actually threaded through to where it takes effect** — this repo's
     own sharpest real bug (`--db` silently forcing `SELECT 13` regardless of the value passed, issue #3/PR#4).
     Any new or changed flag should be traced to its actual effect, not just its `clap` definition.
   - **A failure that degrades to "looks unavailable" instead of "errors clearly"** — this repo's second real
     bug (a GLIBC floor making `--version` fail to load rather than print a mismatch, issue #1/PR#2; and a
     version-bump miss that made `v0.1.1` binaries report `0.1.0`) — both invisible to an automated harness,
     which reads "unavailable" and silently skips rather than failing loud.
   - **Anything relying on `bullmq-official` behavior not already covered by that crate's own tests** — it's a
     brand-new (first published 2026-07-12), fast-moving crate; `AGENTS.md` explicitly requires re-verifying
     against `redis-cli MONITOR`, not just reading its source, for any protocol-touching change, and explicitly
     forbids silently working around a crate gap (it must be documented in code comments and the README —
     the real `Queue::obliterate` `force`-flag workaround in `src/producer.rs` follows exactly this rule; a PR
     that works around a crate bug *without* documenting it the same way is a real regression against a written
     norm, not a nitpick).
   - **Redis Cluster / multi-key assumptions** — cluster support is a real, detailed, still-open gap (issue #5):
     no `cluster-async` feature, no hash-tag-aware queue naming, and a non-zero `--db` must be rejected against
     a cluster endpoint rather than surfacing the server's own `SELECT is not allowed in cluster mode` error. A
     PR that claims to add or touch cluster support should be checked against all three points issue #5 raises,
     not just the first one that comes to mind.
   - **Worker-spawn batching and the per-trial-level error boundary** — README documents `--workers N` spawning
     in bounded batches of 64 specifically to avoid connection/fd exhaustion, with a design intent that one
     level's spawn failure is reported and skipped without losing earlier levels' already-collected results.
     A PR touching `run_trial`'s spawn loop in `src/main.rs` should be checked against both properties, not
     just "does it still spawn workers."
   - **Destructive-operation gates** (`--allow-flushdb`, `--allow-obliterate-active`) staying opt-in and
     default-safe — the active-job gate has a real regression test
     (`clear_queues_refuses_active_jobs_without_explicit_opt_in`) that reproduces the actual upstream bug it
     exists to work around; a PR touching this path should keep or extend that test, not just preserve the
     default value.
   - **Test coverage**, citing `CONTRIBUTING.md`'s explicit written rule ("All new behaviour must be covered by
     tests") — real practice in the one substantive merged PR (#4) matches this (three regression tests, one
     per code path), so, unlike redisbench-admin's Codecov record, there's no real counter-example here yet
     showing the rule gets waived. Don't overclaim precedent either way beyond that single data point.

4. **Write the review.** There is no mined "reviewer voice" to imitate here (see the honesty section above), so
   default to the same standard fcostaoliveira's own descriptions hold themselves to: concrete, quantified where
   possible (name the actual blast radius, not "this could cause issues"), and verified rather than asserted
   (call out when a protocol claim in the PR description isn't backed by a MONITOR trace, or when the CI's
   `smoke test` jq assertions wouldn't actually catch the class of bug the PR fixes or introduces). Be terse —
   nothing in this repo's real written material (PR bodies, commit messages, AGENTS.md, CONTRIBUTING.md) reads
   as padded or hedged with filler. Hedge honestly when genuinely unsure ("I don't see a MONITOR trace for
   this — worth adding one given AGENTS.md's own rule on protocol changes") rather than manufacturing false
   confidence. Never literally `@`-mention a GitHub username, even to flag "this may be worth a second look" —
   say so in prose instead; with a single active maintainer this is a smaller spam risk than on a larger repo,
   but the instruction still applies.

5. **Land on a verdict**, but don't overstate what "this repo's culture" would do with it — say what the
   *technical* finding supports (e.g. "this looks correct and matches the MONITOR trace in the description" or
   "the new flag's effect isn't traced end-to-end the way issue #3's fix was — worth checking before merge"),
   rather than predicting whether it would be self-merged in minutes the way the two real PRs were, since that
   pattern reflects there being one contributor so far, not an evidenced review bar for anyone else's PR.
   Never write the literal word "Verdict," and never format a labeled summary line, a trailing `---` section,
   or a "TL;DR" — end in plain prose, the way every real PR description and commit message in this repo does.

## What NOT to do

- Don't invent multi-round review dialogue, a "maintainer voice," or a "self-merge with silence is normal here"
  culture — the real mined history has none of the former and only n=2 self-authored data points for the
  latter. Say plainly that this repo doesn't have that record yet.
- Don't cite `CONTRIBUTING.md`'s "one approval required" or `AGENTS.md`'s "no direct pushes to main" as
  demonstrated, enforced practice — both are real, written rules; neither has been shown to hold up against a
  real, non-self-authored PR yet, and real practice so far (2/2 self-merges, 8/10 commits direct-to-main)
  hasn't tested them.
- Don't apply generic Python/JS code-review categories — this is a Rust codebase built on a brand-new,
  first-party async crate (`bullmq-official`); the real, evidenced failure classes are CLI-flag wiring,
  build-target/version mismatches, and protocol assumptions about a fast-moving dependency, not (e.g.) Python
  typing or GIL concerns.
- Don't close with a labeled, bolded verdict block — end in plain, unformatted prose, matching every real PR
  description and commit message actually found in this repo.
- Don't literally `@`-mention any GitHub username.
