//! Integration tests against a real, live Redis instance — exercising the
//! actual `bullmq-official` `Queue`/`Worker` plumbing this benchmark drives
//! in production, not a mock. Complements the unit tests (which cover pure
//! logic) and the CI smoke test (which covers the CLI end to end but only
//! asserts on the final JSON).
//!
//! Requires Redis reachable at `REDIS_URL` (default
//! `redis://127.0.0.1:6379/0`). CI provisions this via the `redis:8.6`
//! `services:` block in `.github/workflows/ci.yml`, which is up for the
//! whole job — including the `cargo test` step, not just the smoke-test
//! step. For local runs: `docker compose up -d redis` (see
//! docker-compose.yml), or any Redis 6.2+ on the default port.
//!
//! Every test uses a queue name unique to that test run (see
//! `unique_queue_name`) so tests can run concurrently (`cargo test`'s
//! default) without colliding with each other, and cleans up its own queue
//! on the way out.

use bullmq::options::RedisConnectionOptions;
use bullmq::worker::CancellationToken;
use bullmq::{Job, Queue, QueueOptions, Worker, WorkerOptions};
use bullmq_bench::{metrics::Metrics, producer, worker};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch};

fn redis_url() -> String {
    std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/0".to_string())
}

fn unique_queue_name(prefix: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock before epoch")
        .as_nanos();
    format!("it-{prefix}-{ts}-{n}")
}

async fn make_queue(name: &str) -> Queue {
    let opts = QueueOptions {
        connection: RedisConnectionOptions {
            url: redis_url(),
            ..Default::default()
        },
        ..Default::default()
    };
    Queue::with_options(name, opts)
        .await
        .expect("connect to Redis and create queue — is Redis running? set REDIS_URL to override")
}

/// Poll `check` every 25ms until it returns true or `timeout` elapses.
/// Returns whether it succeeded — callers assert on this so a failure names
/// the actual counts observed, not just "timed out".
async fn wait_until(mut check: impl FnMut() -> bool, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if check() {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// (a) Processed count matches enqueued count, including the completion leg:
/// every job goes through the full BZPOPMIN wakeup -> moveToActive claim ->
/// processor -> moveToFinished lifecycle, and both the completion counter
/// AND the per-job latency channel (populated only once a job's processor
/// has run) land at exactly the enqueued count.
#[tokio::test]
async fn processed_count_matches_enqueued_including_completion_leg() {
    let queue_name = unique_queue_name("full-lifecycle");
    let queue = make_queue(&queue_name).await;
    const N_JOBS: u64 = 300;
    const N_WORKERS: usize = 4;

    producer::bulk_enqueue(std::slice::from_ref(&queue), N_JOBS)
        .await
        .expect("enqueue");

    let metrics = Arc::new(Metrics::new());
    let (done_tx, mut done_rx) = watch::channel(false);
    let done_tx = Arc::new(done_tx);
    let (latency_tx, mut latency_rx) = mpsc::unbounded_channel::<u64>();

    // Count latency samples independently of `metrics.completed` — this is
    // only populated from inside the processor callback after a job's
    // `moveToFinished` completion leg has actually run (see worker.rs), so
    // it's a second, independent witness that completion (not just the
    // claim) happened for every job.
    let latency_count = Arc::new(AtomicU64::new(0));
    let latency_count_bg = latency_count.clone();
    let collector = tokio::spawn(async move {
        while latency_rx.recv().await.is_some() {
            latency_count_bg.fetch_add(1, Ordering::Relaxed);
        }
    });

    let mut workers = Vec::with_capacity(N_WORKERS);
    for i in 0..N_WORKERS {
        let opts = worker::build_worker_options(&redis_url(), i != 0);
        workers.push(
            worker::spawn_worker(
                &queue_name,
                opts,
                metrics.clone(),
                latency_tx.clone(),
                done_tx.clone(),
                N_JOBS,
            )
            .await
            .expect("spawn worker"),
        );
    }
    drop(latency_tx);

    let completed = tokio::time::timeout(Duration::from_secs(30), done_rx.wait_for(|v| *v)).await;
    assert!(
        completed.is_ok(),
        "trial did not reach target_jobs within 30s — completed={}",
        metrics.get_completed()
    );

    for w in &workers {
        w.close(5_000).await.ok();
    }
    drop(workers);
    let _ = collector.await;

    assert_eq!(
        metrics.get_completed(),
        N_JOBS,
        "completed count doesn't match enqueued count"
    );
    assert_eq!(
        metrics.get_errors(),
        0,
        "unexpected errors during processing"
    );
    assert_eq!(
        latency_count.load(Ordering::Relaxed),
        N_JOBS,
        "latency samples (recorded only after a job's completion leg runs) don't match enqueued \
         count — some job's moveToFinished never happened"
    );

    producer::clear_queues(&[queue], true).await.ok();
}

/// (b) Starvation: N workers with genuinely nothing to fetch (fresh, empty
/// queue) must block on BZPOPMIN, then wake and claim jobs as they arrive
/// one at a time — proving the wake is event-driven, not just polling
/// (`WorkerOptions::drain_delay` defaults to 5s; every arrival below is
/// claimed well under that), and that the moveToActive claim script hands
/// each single-job arrival to exactly one of the N racing workers (never
/// zero, never more than one).
#[tokio::test]
async fn starvation_wakes_blocked_workers_and_claims_exactly_once() {
    let queue_name = unique_queue_name("starvation");
    let queue = make_queue(&queue_name).await;
    const N_WORKERS: usize = 5;
    const N_ARRIVALS: u64 = 8;

    let metrics = Arc::new(Metrics::new());
    let (done_tx, _done_rx) = watch::channel(false);
    let done_tx = Arc::new(done_tx);
    let (latency_tx, _latency_rx) = mpsc::unbounded_channel::<u64>();

    // No jobs enqueued yet — every worker below must genuinely park on
    // BZPOPMIN with an empty queue. target_jobs is set far above what this
    // test will ever enqueue so `done_tx` never spuriously fires; this test
    // drives its own timeline via wait_until() on the raw completed counter.
    let mut workers = Vec::with_capacity(N_WORKERS);
    for i in 0..N_WORKERS {
        let opts = worker::build_worker_options(&redis_url(), i != 0);
        workers.push(
            worker::spawn_worker(
                &queue_name,
                opts,
                metrics.clone(),
                latency_tx.clone(),
                done_tx.clone(),
                u64::MAX,
            )
            .await
            .expect("spawn worker"),
        );
    }

    // Give all 5 workers time to genuinely issue their blocking BZPOPMIN
    // against the empty marker key before we send any work.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        metrics.get_completed(),
        0,
        "a job was processed before any job was enqueued — starvation setup is broken"
    );

    for arrival in 1..=N_ARRIVALS {
        producer::bulk_enqueue(std::slice::from_ref(&queue), 1)
            .await
            .expect("enqueue single job");

        // drain_delay defaults to 5s — a 2s bound proves this is a genuine
        // BZPOPMIN wakeup, not the fallback re-poll cycle catching up.
        let woke = wait_until(
            || metrics.get_completed() >= arrival,
            Duration::from_secs(2),
        )
        .await;
        assert!(
            woke,
            "arrival {arrival}/{N_ARRIVALS}: no worker claimed the job within 2s of enqueue \
             (completed={})",
            metrics.get_completed()
        );
    }

    // Exactly-once delivery: with 5 workers racing a single job on every
    // arrival, the completed count must land exactly on N_ARRIVALS — never
    // more (which would mean a job was claimed and processed twice) and,
    // per the loop above, never less.
    assert_eq!(
        metrics.get_completed(),
        N_ARRIVALS,
        "completed count overshot enqueued arrivals — a job was likely double-claimed"
    );
    assert_eq!(metrics.get_errors(), 0);

    for w in &workers {
        w.close(5_000).await.ok();
    }
    drop(workers);

    producer::clear_queues(&[queue], true).await.ok();
}

/// (c) The `--allow-obliterate-active` safety gate (src/producer.rs
/// `clear_queues`): a job kept genuinely ACTIVE by a slow processor must
/// survive `clear_queues(.., false)` and cause a clear error, then actually
/// be removed by `clear_queues(.., true)`.
///
/// This is the regression test for a real bug found while building this
/// gate: bullmq-official's `Queue::obliterate(force, ..)` sends `force` as
/// the literal string `"0"`/`"1"` over the wire, but `obliterate-2.lua`'s
/// own active-job guard is `if ARGV[2] == "" then return -2 end` — which
/// `"0"` never matches, so the crate's `force` argument does not actually
/// gate active-job removal at all (both `true` and `false` behave like
/// `force=true`). `clear_queues` works around this by checking
/// `Queue::get_active_count()` itself before ever calling `obliterate`.
#[tokio::test]
async fn clear_queues_refuses_active_jobs_without_explicit_opt_in() {
    let queue_name = unique_queue_name("active-gate");
    let queue = make_queue(&queue_name).await;

    producer::bulk_enqueue(std::slice::from_ref(&queue), 1)
        .await
        .expect("enqueue");

    let wopts = WorkerOptions {
        connection: RedisConnectionOptions {
            url: redis_url(),
            ..Default::default()
        },
        concurrency: 1,
        autorun: true,
        ..Default::default()
    };
    // A processor that sleeps well past this test's own checks, so the job
    // stays genuinely "active" in Redis for the duration of both
    // clear_queues() calls below.
    let processor = move |_job: Job, _token: CancellationToken| {
        Box::pin(async move {
            tokio::time::sleep(Duration::from_secs(5)).await;
            Ok(serde_json::json!({"ok": true}))
        })
    };
    let active_worker = Worker::with_options(&queue_name, processor, wopts)
        .await
        .expect("spawn slow worker");

    // active_count() is async, so this can't reuse the sync wait_until()
    // helper — poll it directly instead.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        if active_worker.active_count().await >= 1 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "worker never claimed the job within 3s"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let refused = producer::clear_queues(std::slice::from_ref(&queue), false).await;
    assert!(
        refused.is_err(),
        "clear_queues(force=false) should refuse a queue with an active job"
    );
    let msg = format!("{:#}", refused.unwrap_err());
    assert!(
        msg.contains("active") && msg.contains("--allow-obliterate-active"),
        "error message doesn't explain the refusal or how to override it: {msg}"
    );

    // The active job must still be there — refusing must not have partially
    // cleared anything.
    assert!(active_worker.active_count().await >= 1);

    let forced = producer::clear_queues(std::slice::from_ref(&queue), true).await;
    assert!(
        forced.is_ok(),
        "clear_queues(force=true) should succeed even with an active job: {:?}",
        forced.err()
    );

    active_worker.close(2_000).await.ok();
}
