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

/// Wipe all state for the given queues via BullMQ's `obliterate` (force=true
/// pauses the queue first, then deletes every key under its prefix — job
/// hashes, wait/active/marker/completed/failed/delayed sets, meta, everything).
/// This is the default pre-trial cleanup — safe to use on shared Redis, since
/// it only touches keys under each queue's own prefix.
pub async fn clear_queues(queues: &[Queue]) -> Result<()> {
    for q in queues {
        q.obliterate(true, 10_000)
            .await
            .with_context(|| format!("failed to obliterate queue '{}'", q.name()))?;
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
