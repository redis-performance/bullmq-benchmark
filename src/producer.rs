use crate::job::{JobPayload, JOB_NAME};
use anyhow::{Context, Result};
use bullmq::{BulkJob, Queue};

/// Jobs per `Queue::add_bulk` call. Unlike Sidekiq's raw `LPUSH` pipeline,
/// each BullMQ job add executes a Lua script (`addStandardJob` /
/// `addPrioritizedJob` / `addDelayedJob`) over the multiplexed connection, so
/// batches are kept smaller to bound the number of in-flight futures per
/// `add_bulk` call (see `Queue::add_bulk`, which `futures::join_all`s one
/// future per job in the batch).
const BATCH_SIZE: usize = 2000;

/// Wipe all state for the given queues via BullMQ's `obliterate` (deletes
/// every key under the queue's own prefix — job hashes,
/// wait/active/marker/completed/failed/delayed sets, meta, everything). This
/// only ever touches keys under each queue's own prefix, so it can't affect
/// unrelated queues sharing the same Redis instance/db.
///
/// It *can* still affect the same-named queue of a real, unrelated BullMQ
/// application if `--queue` collides with a real production queue name
/// (`"default"`, this tool's own default, is also the single most common
/// real one) — obliterate has no concept of "this benchmark's own jobs" vs.
/// "someone else's jobs" beyond the queue name. `allow_active_removal` is
/// this function's safety gate for the sharpest edge of that: without it, if
/// the queue currently has ACTIVE (in-flight) jobs — i.e. jobs a real worker
/// may be processing *right now* — we refuse to touch this queue at all
/// instead of destroying them. Non-active jobs (waiting/completed/failed/
/// delayed) are always removed either way — this gate only concerns work
/// truly in flight.
///
/// This check is done ourselves via `get_active_count()`, deliberately NOT
/// by trusting `Queue::obliterate`'s own `force` parameter: verified against
/// bullmq-official 1.2.5 that it doesn't work. `obliterate-2.lua`'s own
/// "don't touch active jobs" gate is `if ARGV[2] == "" then return -2 end`,
/// but the Rust binding always sends `force_str = if force {"1"} else {"0"}`
/// — never an empty string — so that branch can never fire and active jobs
/// are force-removed unconditionally, for *both* `force=true` and
/// `force=false`. (Confirmed empirically: a job kept genuinely active by a
/// slow processor survives everything else in `obliterate-2.lua` except
/// this check, and passing `force=false` did not save it.) There's a small
/// unavoidable TOCTOU window between this check and the `obliterate` call
/// below (a job could transition to active in between) — acceptable for a
/// benchmark tool's pre-trial cleanup, where the alternative is no check at
/// all.
pub async fn clear_queues(queues: &[Queue], allow_active_removal: bool) -> Result<()> {
    for q in queues {
        if !allow_active_removal {
            let active = q.get_active_count().await.with_context(|| {
                format!("failed to check active job count for queue '{}'", q.name())
            })?;
            anyhow::ensure!(
                active == 0,
                "queue '{}' has {active} active (in-flight) job(s) — refusing to clear it. If \
                 this queue name is shared with a real BullMQ application, those jobs may belong \
                 to a real worker processing them right now. Pass --allow-obliterate-active (env \
                 BULLMQ_BENCH_ALLOW_OBLITERATE_ACTIVE) to override — e.g. to clean up after this \
                 benchmark's own trial timed out and left active jobs behind — or use a --queue \
                 name unique to this benchmark.",
                q.name()
            );
        }

        // obliterate-2.lua requires the queue to already be paused
        // (unconditionally, regardless of `force`); the crate's own
        // `Queue::obliterate` only calls `pause()` for us when `force=true`,
        // so pause explicitly ourselves.
        let _ = q.pause().await;
        // `force` is passed as `true` here on purpose, not `allow_active_removal`
        // — see the doc comment above: the crate's `force` argument doesn't
        // actually gate active-job removal, our own check above is what does
        // that, so there is no real distinction left to preserve by passing
        // `false` through.
        let result = q.obliterate(true, 10_000).await;
        // obliterate deletes the queue's meta key (which holds the "paused"
        // flag) once it fully clears the queue, so resume() is a no-op in
        // that case. It matters if obliterate errors out instead and leaves
        // the queue paused — without this, this benchmark's own subsequent
        // `add_bulk` calls would route silently into the "paused" list
        // instead of "wait" and never be picked up by any worker.
        let _ = q.resume().await;

        result.with_context(|| format!("failed to obliterate queue '{}'", q.name()))?;
    }
    Ok(())
}

/// Flush the entire database. Only called when --allow-flushdb is explicitly set.
pub async fn flushdb(conn: &mut redis::aio::MultiplexedConnection) -> Result<()> {
    redis::cmd("FLUSHDB").query_async::<()>(conn).await?;
    Ok(())
}

/// Bulk-enqueue `n_jobs` jobs distributed round-robin across `queues`.
///
/// Each job's `data` payload embeds a nanosecond enqueue timestamp (see
/// `job::JobPayload`) for later latency measurement in the worker.
pub async fn bulk_enqueue(queues: &[Queue], n_jobs: u64) -> Result<()> {
    if queues.is_empty() {
        anyhow::bail!("bulk_enqueue called with no queues");
    }
    let n_queues = queues.len() as u64;
    let mut idx = 0u64;
    let mut remaining = n_jobs;

    while remaining > 0 {
        let batch = remaining.min(BATCH_SIZE as u64);

        // Group this batch's jobs by target queue so each queue gets exactly
        // one add_bulk call per outer batch.
        let mut per_queue: Vec<Vec<BulkJob>> = (0..n_queues).map(|_| Vec::new()).collect();
        for j in 0..batch {
            let seq = idx + j;
            let qi = (seq % n_queues) as usize;
            let payload = JobPayload::new(seq);
            let data = serde_json::to_value(&payload)?;
            per_queue[qi].push(BulkJob::new(JOB_NAME, data));
        }

        // Fire the per-queue add_bulk calls concurrently — each targets a
        // distinct queue (distinct Redis keys), so there's no ordering
        // dependency between them.
        let futures: Vec<_> = per_queue
            .into_iter()
            .enumerate()
            .filter(|(_, jobs)| !jobs.is_empty())
            .map(|(qi, jobs)| async move {
                queues[qi]
                    .add_bulk(jobs)
                    .await
                    .with_context(|| format!("add_bulk failed on queue '{}'", queues[qi].name()))
            })
            .collect();
        futures::future::try_join_all(futures).await?;

        idx += batch;
        remaining -= batch;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    // bulk_enqueue/clear_queues need a live Redis + real Queue objects, so
    // they're covered by the end-to-end smoke test (see AGENTS.md / CI)
    // rather than a unit test here. This module's only pure logic (batch
    // splitting math) is exercised indirectly by the smoke test's exact
    // total_jobs == n_jobs assertion.
}
