use crate::job::JobPayload;
use crate::metrics::Metrics;
use bullmq::options::RedisConnectionOptions;
use bullmq::types::RemoveOnFinish;
use bullmq::worker::CancellationToken;
use bullmq::{Job, Worker as BullMqWorker, WorkerOptions};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, watch};

/// Build the `WorkerOptions` shared by every spawned worker.
///
/// `concurrency: 1` is deliberate — see `spawn_worker`'s doc comment for why.
///
/// `remove_on_complete` / `remove_on_fail` are set to immediate removal
/// (`RemoveOnFinish::Bool(true)`). Node.js BullMQ's own default is to *keep*
/// every completed job's hash forever unless configured otherwise; for a
/// benchmark that can process hundreds of thousands of jobs per trial, that
/// default would balloon Redis memory and skew later trials' timings with
/// leftover state. This diverges from BullMQ's out-of-the-box default —
/// documented in README's Safety notes.
pub fn build_worker_options(url: &str, skip_version_check: bool) -> WorkerOptions {
    WorkerOptions {
        connection: RedisConnectionOptions {
            url: url.to_string(),
            ..RedisConnectionOptions::default()
        },
        concurrency: 1,
        autorun: true,
        remove_on_complete: Some(RemoveOnFinish::Bool(true)),
        remove_on_fail: Some(RemoveOnFinish::Bool(true)),
        skip_version_check,
        ..WorkerOptions::default()
    }
}

/// Spawn one dedicated `bullmq::Worker` instance bound to `queue_name`.
///
/// ## Why one `Worker` per logical worker, not one `Worker` at `concurrency = N`
///
/// bullmq-official's `Worker` runs a *single* fetch driver internally
/// regardless of its `concurrency` setting: exactly one blocking `BZPOPMIN`
/// is ever in flight per `Worker` instance. This is deliberate upstream —
/// `Worker::run_main_driver` mirrors Node.js BullMQ's single-driver
/// `mainLoop` specifically to avoid a thundering herd when a job arrives.
/// One `Worker` with `concurrency = N` therefore gives you one blocking
/// waiter on the marker key feeding N concurrent *processing* tasks — not N
/// independent waiters.
///
/// The topology this benchmark exists to measure is production BullMQ: N
/// separate worker processes (each with its own Redis connection) all
/// blocked on `BZPOPMIN` against the same shared "wait" marker key. To
/// reproduce that faithfully, this benchmark spawns N separate `Worker`
/// instances at `concurrency = 1` each, rather than one `Worker` at
/// `concurrency = N`. This is the direct inverse of sidekiq-benchmark's
/// topology, where a single `Processor` fans a connection pool out across N
/// worker tasks that each `BRPOP` across (potentially) many queue keys.
///
/// The processor always returns `Ok(_)`, so `bullmq-official`'s worker loop
/// runs the full lifecycle end to end: `BZPOPMIN` wakeup -> `moveToActive`
/// Lua claim -> processor callback -> automatic `moveToFinished` completion
/// script. See README's "Protocol compatibility" section for how this was
/// verified against `redis-cli MONITOR`.
#[allow(clippy::too_many_arguments)]
pub async fn spawn_worker(
    queue_name: &str,
    opts: WorkerOptions,
    metrics: Arc<Metrics>,
    latency_tx: mpsc::UnboundedSender<u64>,
    done_tx: Arc<watch::Sender<bool>>,
    target_jobs: u64,
) -> bullmq::Result<BullMqWorker> {
    let processor = move |job: Job, _token: CancellationToken| {
        let metrics = metrics.clone();
        let latency_tx = latency_tx.clone();
        let done_tx = done_tx.clone();
        Box::pin(async move {
            if let Some(enqueued_at_ns) = JobPayload::enqueued_at_ns(job.data()) {
                let now_ns = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("system clock is before UNIX_EPOCH")
                    .as_nanos() as u64;

                let latency_us = if now_ns >= enqueued_at_ns {
                    (now_ns - enqueued_at_ns) / 1_000
                } else {
                    // Clock skew: producer clock ahead of worker clock — record
                    // 1 µs to avoid saturating_sub giving 0, which would be
                    // silently discarded by the HDR histogram's lower bound.
                    1
                };
                let _ = latency_tx.send(latency_us.max(1));
            } else {
                metrics.inc_error();
            }

            let done = metrics.inc_completed();
            if done >= target_jobs {
                let _ = done_tx.send(true);
            }

            Ok(serde_json::json!({"ok": true}))
        })
    };

    BullMqWorker::with_options(queue_name, processor, opts).await
}
