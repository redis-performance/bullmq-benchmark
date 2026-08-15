mod report;

use anyhow::{Context, Result};
use bullmq::options::RedisConnectionOptions;
use bullmq::{Queue, QueueOptions};
use bullmq_bench::{metrics, producer, worker};
use clap::Parser;
use hdrhistogram::Histogram;
use metrics::{LatencyStats, Metrics, TrialResult};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot, watch};

// ── CLI ───────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(
    name = "bullmq-bench",
    version,
    about = "BullMQ protocol load benchmark — measures job throughput and latency against any Redis endpoint"
)]
struct Cli {
    /// Redis URL (takes precedence over --host/--port).
    /// Defaults to db 13 (matching sidekiq-benchmark's safety convention — BullMQ
    /// itself has no special default database) to avoid colliding with application
    /// data and to make --allow-flushdb safe by default.
    #[arg(long, env = "REDIS_URL", default_value = "redis://127.0.0.1:6379/13")]
    url: String,

    /// Override host in the Redis URL
    #[arg(long)]
    host: Option<String>,

    /// Override port in the Redis URL
    #[arg(long)]
    port: Option<u16>,

    /// Redis password — prefer REDIS_PASSWORD env var; passing on CLI exposes it in process list
    #[arg(long, env = "REDIS_PASSWORD")]
    password: Option<String>,

    /// Enable TLS (upgrades scheme to rediss://)
    #[arg(long, env = "REDIS_TLS")]
    tls: bool,

    /// Redis database number
    #[arg(long, default_value = "13")]
    db: u8,

    /// Comma-separated concurrency levels — each becomes a separate trial.
    /// Each level spawns that many separate BullMQ `Worker` instances (see
    /// src/worker.rs for why one Worker per logical worker, not one Worker at
    /// higher concurrency).
    #[arg(long, default_value = "10,50,100,200", value_delimiter = ',')]
    workers: Vec<usize>,

    /// Total jobs per trial.
    ///
    /// Lower than sidekiq-benchmark's default (500,000): BullMQ's add path runs
    /// a Lua script per job (writes a job hash + wait/marker zset entries) rather
    /// than a single raw LPUSH, so enqueuing is markedly more expensive per job.
    #[arg(long, default_value = "20000")]
    jobs: u64,

    /// Jobs for warmup run before each trial (0 = skip)
    #[arg(long, default_value = "0")]
    warmup_jobs: u64,

    /// Base BullMQ queue name
    #[arg(long, default_value = "default")]
    queue: String,

    /// Number of independently-named BullMQ queues to distribute jobs and workers
    /// across (1 = single queue). Queue names are generated as <queue>_0, <queue>_1,
    /// … when > 1.
    ///
    /// Unlike Sidekiq's BRPOP (which can block across multiple queue keys on one
    /// connection), a BullMQ `Worker` binds to exactly one queue name. So N queues
    /// here means launching separate `Worker` instances per queue (workers are
    /// distributed round-robin across queue names) rather than one client watching
    /// several keys — the opposite direction of sidekiq-benchmark's multi-queue mode.
    #[arg(long, default_value = "1")]
    num_queues: usize,

    /// Per-second latency percentiles to record (comma-separated).
    /// Supported values: p50, p75, p90, p95, p99, p999, p9999, max, mean.
    #[arg(long, default_value = "p50,p90,p99,p999,max", value_delimiter = ',')]
    latency_percentiles: Vec<String>,

    /// Label for output (defaults to redis_version from INFO)
    #[arg(long)]
    tag: Option<String>,

    /// Output file path, or '-' for stdout
    #[arg(long)]
    output: Option<String>,

    /// Per-trial timeout in seconds
    #[arg(long, default_value = "300")]
    timeout: u64,

    /// Suppress per-second progress output
    #[arg(long)]
    quiet: bool,

    /// Allow FLUSHDB before each trial (clears the entire database).
    /// Default: obliterate only the specific queue(s) under their key prefix,
    /// which is safe on shared Redis.
    #[arg(long, env = "BULLMQ_BENCH_ALLOW_FLUSHDB")]
    allow_flushdb: bool,

    /// Allow force-removing ACTIVE (in-flight) jobs from the target queue(s)
    /// during pre-trial cleanup.
    ///
    /// `Queue::obliterate`'s `force` flag doesn't just clear waiting jobs —
    /// it also deletes jobs that are *currently being processed*, which on a
    /// shared Redis could belong to a real, unrelated worker mid-job (e.g. if
    /// `--queue` collides with a real application's queue name, "default"
    /// being the single most common one). Without this flag, pre-trial
    /// cleanup refuses to touch a queue that has active jobs and fails with a
    /// clear error instead of silently destroying them. This flag exists
    /// purely to let re-runs proceed after our OWN trial times out and
    /// leaves active jobs behind — it is not needed for a first run against
    /// an empty/dedicated queue.
    #[arg(long, env = "BULLMQ_BENCH_ALLOW_OBLITERATE_ACTIVE")]
    allow_obliterate_active: bool,
}

// ── Redis URL helpers ─────────────────────────────────────────────────────────
// (Protocol-agnostic — copied from sidekiq-benchmark's main.rs verbatim.)

fn build_redis_url(cli: &Cli) -> Result<String> {
    // NOTE: every error path below must use `redact_url(&cli.url)`, never
    // `cli.url` directly — `--url` can carry an embedded password
    // (`redis://:secret@host/0`), and these messages can end up on stderr /
    // in CI logs via anyhow's default `main() -> Result<()>` error printing.
    let mut u = url::Url::parse(&cli.url)
        .with_context(|| format!("invalid Redis URL: {}", redact_url(&cli.url)))?;

    if let Some(host) = &cli.host {
        u.set_host(Some(host))
            .map_err(|_| anyhow::anyhow!("invalid --host: {host}"))?;
    }
    if let Some(port) = cli.port {
        u.set_port(Some(port))
            .map_err(|_| anyhow::anyhow!("cannot set port on URL: {}", redact_url(&cli.url)))?;
    }
    if cli.tls && u.scheme() == "redis" {
        u.set_scheme("rediss")
            .map_err(|_| anyhow::anyhow!("cannot upgrade scheme to rediss"))?;
    }
    if let Some(password) = &cli.password {
        // url::Url::set_password percent-encodes special characters (e.g. '@', '/', ':')
        u.set_password(Some(password))
            .map_err(|_| anyhow::anyhow!("cannot set password on URL: {}", redact_url(&cli.url)))?;
    }
    // Ensure db path is present
    if u.path().trim_matches('/').is_empty() {
        u.set_path(&format!("/{}", cli.db));
    }

    Ok(u.to_string())
}

/// Return the URL with any embedded password replaced by `****`, for
/// logging, error messages, and JSON output.
///
/// Falls back to a syntactic scrub (rather than returning the raw string
/// verbatim) when `url::Url::parse` rejects the input — which is exactly the
/// case in `build_redis_url`'s own error paths, where `--url` failed to
/// parse but may still contain a real, human-supplied password between
/// `://` and `@`.
fn redact_url(raw: &str) -> String {
    if let Ok(mut u) = url::Url::parse(raw) {
        if u.password().is_some() {
            let _ = u.set_password(Some("****"));
        }
        return u.to_string();
    }

    let Some(scheme_end) = raw.find("://") else {
        return raw.to_string();
    };
    let authority_start = scheme_end + 3;
    let authority_end = raw[authority_start..]
        .find(['/', '?', '#'])
        .map(|i| authority_start + i)
        .unwrap_or(raw.len());
    match raw[authority_start..authority_end].rfind('@') {
        Some(at_rel) => format!(
            "{}****{}",
            &raw[..authority_start],
            &raw[authority_start + at_rel..]
        ),
        None => raw.to_string(),
    }
}

/// Sanitize a tag string to characters safe for use in filenames.
fn sanitize_tag(tag: &str) -> String {
    let s: String = tag
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '.' || c == '-' || c == '_' {
                c
            } else {
                '-'
            }
        })
        .collect();
    if s.is_empty() {
        "unknown".to_string()
    } else {
        s
    }
}

/// Reject output paths containing '..' to prevent path traversal.
fn validate_output_path(path: &str) -> Result<()> {
    if path == "-" {
        return Ok(());
    }
    for component in std::path::Path::new(path).components() {
        if component == std::path::Component::ParentDir {
            anyhow::bail!("--output must not contain '..' segments: {path}");
        }
    }
    Ok(())
}

// ── Per-second latency percentile specs ──────────────────────────────────────
// (Protocol-agnostic — copied from sidekiq-benchmark's main.rs verbatim.)

#[derive(Clone)]
enum PercentileSpec {
    Quantile { name: String, q: f64 },
    Max,
    Mean,
}

impl PercentileSpec {
    fn name(&self) -> &str {
        match self {
            Self::Quantile { name, .. } => name,
            Self::Max => "max",
            Self::Mean => "mean",
        }
    }

    fn value(&self, hist: &Histogram<u64>) -> u64 {
        if hist.is_empty() {
            return 0;
        }
        match self {
            Self::Quantile { q, .. } => hist.value_at_quantile(*q),
            Self::Max => hist.max(),
            Self::Mean => hist.mean() as u64,
        }
    }
}

/// Parse a percentile spec string: "p50" → 0.50, "p999" → 0.999, "max", "mean".
fn parse_percentile_spec(s: &str) -> Result<PercentileSpec> {
    match s {
        "max" => Ok(PercentileSpec::Max),
        "mean" => Ok(PercentileSpec::Mean),
        s if s.starts_with('p') => {
            let digits = &s[1..];
            anyhow::ensure!(!digits.is_empty(), "invalid percentile spec: '{s}'");
            // Bounds digits.len() before it feeds 10u64.pow() below. u64::parse
            // already rejects most absurdly long inputs, but leading zeros let
            // digits.len() run arbitrarily long while still parsing to a small
            // `n` (e.g. "p" + 100 zeros + "50") — pow(100) overflows u64 (panics
            // in a debug/test build; silently wraps in release), so cap it well
            // above any legitimate percentile spec instead of relying on that.
            anyhow::ensure!(
                digits.len() <= 18,
                "invalid percentile spec: '{s}' (too many digits)"
            );
            let n: u64 = digits
                .parse()
                .with_context(|| format!("invalid percentile spec: '{s}'"))?;
            let divisor = 10u64.pow(digits.len() as u32);
            let q = n as f64 / divisor as f64;
            anyhow::ensure!(q > 0.0 && q <= 1.0, "percentile out of range (0, 1]: '{s}'");
            Ok(PercentileSpec::Quantile {
                name: s.to_string(),
                q,
            })
        }
        _ => anyhow::bail!("unknown percentile spec '{s}' — use p50, p99, p999, max, mean"),
    }
}

/// Generate queue names from a base name and count.
/// With n=1 returns `["default"]`; with n=4 returns `["default_0".."default_3"]`.
fn make_queue_names(base: &str, n: usize) -> Vec<String> {
    if n <= 1 {
        vec![base.to_string()]
    } else {
        (0..n).map(|i| format!("{base}_{i}")).collect()
    }
}

async fn fetch_tag(url: &str) -> String {
    let client = match redis::Client::open(url) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("warning: could not build Redis client for tag lookup: {e}");
            return "unknown".to_string();
        }
    };
    match client.get_multiplexed_async_connection().await {
        Ok(mut conn) => {
            match redis::cmd("INFO")
                .arg("server")
                .query_async::<String>(&mut conn)
                .await
            {
                Ok(info) => {
                    for line in info.lines() {
                        if let Some(v) = line.strip_prefix("redis_version:") {
                            return format!("redis-{}", v.trim());
                        }
                    }
                    "unknown".to_string()
                }
                Err(e) => {
                    eprintln!("warning: could not fetch Redis INFO for tag: {e}");
                    "unknown".to_string()
                }
            }
        }
        Err(e) => {
            eprintln!("warning: could not connect to Redis for tag lookup: {e}");
            "unknown".to_string()
        }
    }
}

// ── Trial execution ───────────────────────────────────────────────────────────

struct TrialConfig<'a> {
    url: &'a str,
    queue_names: &'a [String],
    jobs: u64,
    timeout_secs: u64,
    quiet: bool,
    percentile_specs: &'a [PercentileSpec],
}

fn empty_histogram() -> Histogram<u64> {
    // HDRHistogram requires low >= 1; values are clamped to .max(1) before recording
    Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).expect("valid histogram bounds")
}

async fn run_trial(cfg: &TrialConfig<'_>, n_workers: usize) -> Result<TrialResult> {
    let metrics = Arc::new(Metrics::new());
    let (done_tx, mut done_rx) = watch::channel(false);
    let done_tx = Arc::new(done_tx);
    let (latency_tx, latency_rx) = mpsc::unbounded_channel::<u64>();

    // Per-second latency windows are pulled by the monitor, not pushed on a
    // separate timer — see sidekiq-benchmark's main.rs for the full rationale
    // (kept identical here: this machinery is protocol-agnostic).
    let (snapshot_tx, snapshot_rx) = mpsc::unbounded_channel::<oneshot::Sender<Histogram<u64>>>();

    let collector = tokio::spawn(async move {
        let mut hist = empty_histogram();
        let mut per_sec_hist = empty_histogram();
        let mut rx = latency_rx;
        let mut snapshot_rx = snapshot_rx;
        loop {
            tokio::select! {
                maybe_us = rx.recv() => {
                    match maybe_us {
                        Some(us) => {
                            let v = us.max(1);
                            let _ = hist.record(v);
                            let _ = per_sec_hist.record(v);
                        }
                        None => break,
                    }
                }
                Some(resp) = snapshot_rx.recv() => {
                    let _ = resp.send(per_sec_hist.clone());
                    per_sec_hist.reset();
                }
            }
        }
        hist
    });

    // Spawn n_workers separate bullmq::Worker instances, round-robin across
    // cfg.queue_names. See src/worker.rs::spawn_worker for why this is N
    // Worker instances at concurrency=1 rather than one Worker at concurrency=N.
    let n_queues = cfg.queue_names.len();
    anyhow::ensure!(n_queues > 0, "no queues configured");

    // Each Worker instance opens 2 TCP connections (one multiplexed command
    // connection, one dedicated blocking connection for BZPOPMIN) and spawns
    // 3 background tasks (main loop, stalled-job check, lock renewal). Spawn
    // in bounded batches rather than firing all n_workers connections at
    // Redis simultaneously — this bounds the peak fd/connection burst and,
    // if the OS or Redis's maxclients rejects a connection partway through
    // (e.g. `--workers 1000` against a low ulimit -n), fails within one
    // batch's worth of partially-open connections instead of ~n_workers of
    // them, and gives us a clean point to close() everything spawned so far
    // (see the Err arm below) instead of leaking live connections/background
    // tasks — `Worker`'s own `Drop` only flags an internal `closing` bool, it
    // does not abort those tasks or close the sockets itself.
    const WORKER_SPAWN_BATCH_SIZE: usize = 64;
    const WORKER_CLOSE_TIMEOUT_MS: u64 = 5_000;

    let mut workers = Vec::with_capacity(n_workers);
    for batch_start in (0..n_workers).step_by(WORKER_SPAWN_BATCH_SIZE) {
        let batch_end = (batch_start + WORKER_SPAWN_BATCH_SIZE).min(n_workers);
        let mut spawn_futs = Vec::with_capacity(batch_end - batch_start);
        for i in batch_start..batch_end {
            let queue_name = cfg.queue_names[i % n_queues].clone();
            // Only the very first worker performs the Redis version check;
            // the rest skip it to avoid n_workers redundant round trips.
            let opts = worker::build_worker_options(cfg.url, i != 0);
            let metrics = metrics.clone();
            let latency_tx = latency_tx.clone(); // worker holds a clone; main keeps the sentinel
            let done_tx = done_tx.clone();
            let target_jobs = cfg.jobs;
            spawn_futs.push(async move {
                worker::spawn_worker(&queue_name, opts, metrics, latency_tx, done_tx, target_jobs)
                    .await
            });
        }
        match futures::future::try_join_all(spawn_futs).await {
            Ok(mut spawned) => workers.append(&mut spawned),
            Err(e) => {
                let spawned_so_far = workers.len();
                let _ = futures::future::join_all(
                    workers.iter().map(|w| w.close(WORKER_CLOSE_TIMEOUT_MS)),
                )
                .await;
                anyhow::bail!(
                    "failed to spawn BullMQ worker ({spawned_so_far} of {n_workers} spawned \
                     before the failure, now closed): {e}\n  hint: each worker opens 2 Redis \
                     connections — {n_workers} workers need roughly {approx} connections total. \
                     Check `ulimit -n` and the Redis server's `maxclients`, or lower --workers.",
                    approx = n_workers * 2
                );
            }
        }
    }

    // Per-second samples collected by the monitor task
    let throughput_samples: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let errors_samples: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let latency_sec_samples: Arc<Mutex<HashMap<String, Vec<u64>>>> =
        Arc::new(Mutex::new(HashMap::new()));

    let tput_for_monitor = throughput_samples.clone();
    let err_for_monitor = errors_samples.clone();
    let lat_for_monitor = latency_sec_samples.clone();
    let metrics_mon = metrics.clone();
    let specs_for_monitor = cfg.percentile_specs.to_vec();
    let quiet = cfg.quiet;

    let monitor = tokio::spawn(async move {
        let mut prev_completed = 0u64;
        let mut prev_errors = 0u64;
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;

            let cur = metrics_mon.get_completed();
            let tput_delta = cur - prev_completed;
            prev_completed = cur;
            if let Ok(mut v) = tput_for_monitor.lock() {
                v.push(tput_delta);
            }

            let cur_err = metrics_mon.get_errors();
            let err_delta = cur_err - prev_errors;
            prev_errors = cur_err;
            if let Ok(mut v) = err_for_monitor.lock() {
                v.push(err_delta);
            }

            let (resp_tx, resp_rx) = oneshot::channel();
            if snapshot_tx.send(resp_tx).is_ok() {
                if let Ok(snap) = resp_rx.await {
                    if let Ok(mut map) = lat_for_monitor.lock() {
                        for spec in &specs_for_monitor {
                            map.entry(spec.name().to_string())
                                .or_default()
                                .push(spec.value(&snap));
                        }
                    }
                }
            }

            if !quiet {
                if err_delta > 0 {
                    print!("[e:{err_delta}]");
                } else {
                    print!(".");
                }
                use std::io::Write;
                let _ = std::io::stdout().flush();
            }
        }
    });

    let start = Instant::now();
    let mut timed_out = false;

    // Wait for all jobs to complete, or timeout. Unlike sidekiq-benchmark's
    // Processor (a single tokio task we can join on to detect an early
    // crash), bullmq-official's Worker manages its own internal tasks per
    // instance with no single handle across all N — so there is no
    // equivalent "processor exited unexpectedly" branch here.
    tokio::select! {
        _ = done_rx.wait_for(|v| *v) => {},
        _ = tokio::time::sleep(Duration::from_secs(cfg.timeout_secs)) => {
            if !cfg.quiet { eprintln!(); }
            eprintln!("  [timeout after {}s]", cfg.timeout_secs);
            timed_out = true;
        }
    }

    let duration = start.elapsed();
    if !cfg.quiet && !timed_out {
        println!();
    }

    monitor.abort();

    // Graceful shutdown: ask every worker to close (waits for its one
    // in-flight job, if any, up to the timeout, then tears down its internal
    // tasks). Bounded so a stuck worker can't hang the whole trial.
    // (WORKER_CLOSE_TIMEOUT_MS is defined above, next to the spawn loop that
    // also uses it for cleanup-on-partial-spawn-failure.)
    let _ =
        futures::future::join_all(workers.iter().map(|w| w.close(WORKER_CLOSE_TIMEOUT_MS))).await;

    // Dropping the workers releases each one's processor closure — and with
    // it, its captured latency_tx clone. Combined with dropping our own
    // sentinel clone, this closes the channel so the collector can finish.
    drop(workers);
    drop(latency_tx);

    let hist = collector.await.unwrap_or_else(|_| empty_histogram());

    let total_jobs = metrics.get_completed();
    let errors = metrics.get_errors();
    let throughput_per_sec = throughput_samples
        .lock()
        .map(|v| v.clone())
        .unwrap_or_default();
    let errors_per_sec = errors_samples.lock().map(|v| v.clone()).unwrap_or_default();
    let latency_per_sec = latency_sec_samples
        .lock()
        .map(|v| v.clone())
        .unwrap_or_default();

    let jobs_per_sec = if duration.as_secs_f64() > 0.0 {
        total_jobs as f64 / duration.as_secs_f64()
    } else {
        0.0
    };

    Ok(TrialResult {
        workers: n_workers,
        total_jobs,
        duration_s: duration.as_secs_f64(),
        jobs_per_sec,
        throughput_per_sec,
        errors_per_sec,
        latency_per_sec,
        latency: LatencyStats::from_histogram(&hist),
        errors,
        timed_out,
    })
}

/// Clear queues before a trial. Uses `Queue::obliterate` by default; FLUSHDB
/// only when explicitly allowed. `allow_obliterate_active` gates whether
/// obliterate is allowed to force-remove ACTIVE (in-flight) jobs — see the
/// `--allow-obliterate-active` doc comment on `Cli` for why this matters.
async fn pre_trial_clear(
    queues: &[Queue],
    conn: &mut redis::aio::MultiplexedConnection,
    allow_flushdb: bool,
    allow_obliterate_active: bool,
) -> Result<()> {
    if allow_flushdb {
        producer::flushdb(conn).await
    } else {
        producer::clear_queues(queues, allow_obliterate_active).await
    }
}

/// Reject CLI values that can't produce a sane trial before any Redis
/// connection or spawning happens — fail fast and clearly rather than, e.g.,
/// `--workers 0` silently burning the full `--timeout` with zero throughput,
/// or `--timeout 0` making every trial an instant no-op.
fn validate_cli(cli: &Cli) -> Result<()> {
    anyhow::ensure!(cli.jobs > 0, "--jobs must be > 0");
    anyhow::ensure!(cli.num_queues > 0, "--num-queues must be > 0");
    anyhow::ensure!(cli.timeout > 0, "--timeout must be > 0");
    anyhow::ensure!(
        !cli.workers.is_empty(),
        "--workers must specify at least one concurrency level"
    );
    anyhow::ensure!(
        cli.workers.iter().all(|&w| w > 0),
        "--workers values must all be > 0 (got 0 — a 0-worker trial can never complete and \
         would just burn the full --timeout doing nothing)"
    );
    Ok(())
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    validate_cli(&cli)?;
    if let Some(&max_w) = cli.workers.iter().max() {
        if max_w > 256 {
            eprintln!(
                "warning: --workers up to {max_w} — each worker opens ~2 Redis connections and \
                 3 background tasks, so this trial needs roughly {approx} connections. Check \
                 `ulimit -n` and the Redis server's `maxclients` before running.",
                approx = max_w * 2
            );
        }
    }

    let url = build_redis_url(&cli)?;
    let display_url = redact_url(&url);

    // Warn loudly if FLUSHDB is enabled on db 0 — application data lives there by default.
    if cli.allow_flushdb {
        let db_in_url = url::Url::parse(&url)
            .ok()
            .and_then(|u| u.path().trim_matches('/').parse::<u8>().ok())
            .unwrap_or(0);
        if db_in_url == 0 {
            eprintln!(
                "warning: --allow-flushdb is set on db 0 — this will destroy ALL keys in the \
                 database. Use --db 13 (or any non-zero db) to isolate benchmark data."
            );
        }
    }

    if let Some(out) = &cli.output {
        validate_output_path(out)?;
    }

    if cli.num_queues > 1 {
        if let Some(&min_w) = cli.workers.iter().min() {
            if cli.num_queues > min_w {
                eprintln!(
                    "warning: --num-queues {} exceeds the smallest --workers level ({}) — \
                     some queues will have zero workers assigned in that trial",
                    cli.num_queues, min_w
                );
            }
        }
    }

    let tag = match &cli.tag {
        Some(t) => sanitize_tag(t),
        None => sanitize_tag(&fetch_tag(&url).await),
    };

    let output = cli
        .output
        .clone()
        .unwrap_or_else(|| format!("bullmq_bench_{tag}.json"));

    let queue_names = make_queue_names(&cli.queue, cli.num_queues);
    let queues_label = if queue_names.len() == 1 {
        queue_names[0].clone()
    } else {
        format!(
            "{} queues ({}…{})",
            queue_names.len(),
            queue_names[0],
            queue_names[queue_names.len() - 1]
        )
    };

    println!("\n=== bullmq-bench — {tag} ===");
    println!(
        "    {}  jobs={}  queues={}",
        display_url,
        report::format_n(cli.jobs),
        queues_label
    );
    println!();

    // Raw connection — used only for the INFO tag lookup (above) and FLUSHDB.
    let client = redis::Client::open(url.as_str()).context("invalid Redis URL")?;
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .context("failed to connect to Redis")?;

    // One bullmq::Queue per queue name, built once and reused across every
    // trial (Queue::with_options registers/refreshes queue meta on creation).
    let mut queues: Vec<Queue> = Vec::with_capacity(queue_names.len());
    for (i, name) in queue_names.iter().enumerate() {
        let opts = QueueOptions {
            connection: RedisConnectionOptions {
                url: url.clone(),
                ..RedisConnectionOptions::default()
            },
            skip_version_check: i != 0,
            ..QueueOptions::default()
        };
        let q = Queue::with_options(name, opts)
            .await
            .with_context(|| format!("failed to create BullMQ queue '{name}'"))?;
        queues.push(q);
    }

    let percentile_specs: Vec<PercentileSpec> = cli
        .latency_percentiles
        .iter()
        .map(|s| parse_percentile_spec(s))
        .collect::<Result<Vec<_>>>()?;

    let cfg = TrialConfig {
        url: &url,
        queue_names: &queue_names,
        jobs: cli.jobs,
        timeout_secs: cli.timeout,
        quiet: cli.quiet,
        percentile_specs: &percentile_specs,
    };
    let warmup_cfg = TrialConfig {
        jobs: cli.warmup_jobs,
        url: cfg.url,
        queue_names: cfg.queue_names,
        timeout_secs: cfg.timeout_secs,
        quiet: cfg.quiet,
        percentile_specs: cfg.percentile_specs,
    };

    let workers_list = cli.workers.clone();
    let mut results: Vec<TrialResult> = Vec::new();
    let mut any_timeout = false;
    let mut any_trial_failed = false;

    // Warn if the queue fill will likely use significant Redis memory. BullMQ
    // jobs are heavier than Sidekiq's: a job hash (name, data, opts, timestamps,
    // …) plus wait-list/marker-zset entries, vs. Sidekiq's single serialized
    // list entry — so the per-job estimate is bumped up accordingly.
    let estimated_mb = cli.jobs as f64 * 600.0 / (1024.0 * 1024.0);
    if estimated_mb > 100.0 {
        eprintln!(
            "warning: estimated peak Redis memory ~{:.0} MB ({} jobs × ~600 B/job)",
            estimated_mb,
            report::format_n(cli.jobs)
        );
    }

    // Each concurrency level's clear+enqueue+run is wrapped in its own error
    // boundary: a failure at one level (e.g. `--workers 1000` exhausting fds
    // partway through worker spawn — see run_trial's spawn loop) skips just
    // that level and moves on, instead of an unhandled `?` aborting the
    // whole process and discarding every earlier level's already-collected
    // results before they ever reach report::write_json below.
    for &n_workers in &workers_list {
        let outcome: Result<()> = async {
            if cli.warmup_jobs > 0 {
                pre_trial_clear(
                    &queues,
                    &mut conn,
                    cli.allow_flushdb,
                    cli.allow_obliterate_active,
                )
                .await?;
                producer::bulk_enqueue(&queues, cli.warmup_jobs).await?;
                if !cli.quiet {
                    print!("  [{n_workers:>4} workers] warmup … ");
                }
                run_trial(&warmup_cfg, n_workers).await?;
            }

            pre_trial_clear(
                &queues,
                &mut conn,
                cli.allow_flushdb,
                cli.allow_obliterate_active,
            )
            .await?;
            producer::bulk_enqueue(&queues, cli.jobs).await?;

            if !cli.quiet {
                print!("  [{n_workers:>4} workers] ");
            }

            let result = run_trial(&cfg, n_workers).await?;

            if result.timed_out {
                any_timeout = true;
            }
            report::print_trial_line(&result);
            results.push(result);
            Ok(())
        }
        .await;

        if let Err(e) = outcome {
            any_trial_failed = true;
            if !cli.quiet {
                println!();
            }
            eprintln!("warning: concurrency level {n_workers} failed and was skipped: {e:#}");
        }
    }

    report::print_summary(&results);

    report::write_json(
        &results,
        &tag,
        &display_url,
        &workers_list,
        cli.jobs,
        &queue_names,
        cli.warmup_jobs,
        &output,
    )?;

    if any_trial_failed {
        eprintln!(
            "warning: one or more concurrency levels failed to run — see warnings above; \
             results include only the levels that completed"
        );
    }
    if any_timeout {
        eprintln!("warning: one or more trials timed out — results are incomplete");
    }
    if any_trial_failed || any_timeout {
        std::process::exit(1);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_tag_strips_unsafe_chars() {
        assert_eq!(sanitize_tag("redis-8.0"), "redis-8.0"); // dots and dashes kept
        assert_eq!(sanitize_tag("redis/8.0"), "redis-8.0"); // slash → dash
                                                            // dots are valid in filenames; path-traversal is caught by validate_output_path
        assert_eq!(sanitize_tag("../evil"), "..-evil");
        assert_eq!(sanitize_tag("foo bar"), "foo-bar"); // space → dash
        assert_eq!(sanitize_tag(""), "unknown");
    }

    #[test]
    fn validate_output_path_rejects_traversal() {
        assert!(validate_output_path("../evil.json").is_err());
        assert!(validate_output_path("foo/../bar.json").is_err());
        assert!(validate_output_path("results/out.json").is_ok());
        assert!(validate_output_path("-").is_ok());
        assert!(validate_output_path("out.json").is_ok());
    }

    #[test]
    fn redact_url_hides_password() {
        let raw = "redis://:hunter2@127.0.0.1:6379/0";
        let redacted = redact_url(raw);
        assert!(
            !redacted.contains("hunter2"),
            "password still visible: {redacted}"
        );
        assert!(redacted.contains("****"), "no redaction marker: {redacted}");
    }

    #[test]
    fn redact_url_leaves_no_password_url_unchanged() {
        let raw = "redis://127.0.0.1:6379/0";
        assert_eq!(redact_url(raw), raw);
    }

    #[test]
    fn redact_url_scrubs_password_even_when_url_fails_to_parse() {
        // Deliberately malformed (space in the host) so url::Url::parse
        // rejects it — this is exactly the shape of input that reaches
        // redact_url from build_redis_url's own parse-failure error path.
        let raw = "redis://:hunter2@bad host/0";
        assert!(
            url::Url::parse(raw).is_err(),
            "test fixture should be malformed"
        );
        let redacted = redact_url(raw);
        assert!(
            !redacted.contains("hunter2"),
            "password leaked from a malformed URL: {redacted}"
        );
        assert!(redacted.contains("****"), "no redaction marker: {redacted}");
    }

    #[test]
    fn redact_url_malformed_without_userinfo_is_unchanged() {
        // No '@' before the path — nothing to scrub, and the fallback must
        // not corrupt the string.
        let raw = "redis://bad host/0";
        assert_eq!(redact_url(raw), raw);
    }

    #[test]
    fn build_redis_url_error_never_leaks_password() {
        // A malformed --url (space in the host) that still carries a real,
        // human-supplied password before the '@'. url::Url::parse rejects
        // this outright, hitting build_redis_url's *first* error path — the
        // one that used to interpolate cli.url raw into the error message,
        // leaking the password to stderr/CI logs via anyhow's default error
        // printing.
        let cli = Cli {
            url: "redis://:hunter2secret@bad host/0".into(),
            host: None,
            port: None,
            password: None,
            tls: false,
            db: 0,
            workers: vec![10],
            jobs: 1000,
            warmup_jobs: 0,
            queue: "default".into(),
            num_queues: 1,
            latency_percentiles: vec![],
            tag: None,
            output: None,
            timeout: 300,
            quiet: false,
            allow_flushdb: false,
            allow_obliterate_active: false,
        };
        let err = build_redis_url(&cli).unwrap_err();
        assert!(
            !format!("{err:#}").contains("hunter2secret"),
            "error message leaked a secret: {err:#}"
        );
    }

    #[test]
    fn build_redis_url_encodes_special_chars_in_password() {
        let cli = Cli {
            url: "redis://127.0.0.1:6379/0".into(),
            host: None,
            port: None,
            password: Some("p@ss/word".into()),
            tls: false,
            db: 0,
            workers: vec![10],
            jobs: 1000,
            warmup_jobs: 0,
            queue: "default".into(),
            num_queues: 1,
            latency_percentiles: vec![],
            tag: None,
            output: None,
            timeout: 300,
            quiet: false,
            allow_flushdb: false,
            allow_obliterate_active: false,
        };
        let url = build_redis_url(&cli).unwrap();
        let parsed = url::Url::parse(&url).unwrap();
        assert_eq!(parsed.host_str().unwrap(), "127.0.0.1");
        let raw_pw = parsed.password().unwrap();
        assert!(raw_pw.contains("%40"), "@ not percent-encoded: {raw_pw}");
        assert!(!url.contains(":p@ss"), "raw '@' leaked into URL: {url}");
    }

    #[test]
    fn build_redis_url_upgrades_scheme_with_tls() {
        let cli = Cli {
            url: "redis://127.0.0.1:6379/0".into(),
            host: None,
            port: None,
            password: None,
            tls: true,
            db: 0,
            workers: vec![10],
            jobs: 1000,
            warmup_jobs: 0,
            queue: "default".into(),
            num_queues: 1,
            latency_percentiles: vec![],
            tag: None,
            output: None,
            timeout: 300,
            quiet: false,
            allow_flushdb: false,
            allow_obliterate_active: false,
        };
        let url = build_redis_url(&cli).unwrap();
        assert!(url.starts_with("rediss://"), "expected rediss:// got {url}");
    }

    #[test]
    fn build_redis_url_host_port_override() {
        let cli = Cli {
            url: "redis://127.0.0.1:6379/0".into(),
            host: Some("10.0.0.1".into()),
            port: Some(6380),
            password: None,
            tls: false,
            db: 0,
            workers: vec![10],
            jobs: 1000,
            warmup_jobs: 0,
            queue: "default".into(),
            num_queues: 1,
            latency_percentiles: vec![],
            tag: None,
            output: None,
            timeout: 300,
            quiet: false,
            allow_flushdb: false,
            allow_obliterate_active: false,
        };
        let url = build_redis_url(&cli).unwrap();
        let parsed = url::Url::parse(&url).unwrap();
        assert_eq!(parsed.host_str().unwrap(), "10.0.0.1");
        assert_eq!(parsed.port().unwrap(), 6380);
    }

    #[test]
    fn parse_percentile_spec_valid() {
        let cases: &[(&str, f64)] = &[
            ("p50", 0.50),
            ("p90", 0.90),
            ("p99", 0.99),
            ("p999", 0.999),
            ("p9999", 0.9999),
            ("p75", 0.75),
        ];
        for &(s, expected_q) in cases {
            match parse_percentile_spec(s).unwrap() {
                PercentileSpec::Quantile { q, name } => {
                    assert!((q - expected_q).abs() < 1e-9, "{s}: got {q}");
                    assert_eq!(name, s);
                }
                other => panic!("{s} parsed as non-quantile: {}", other.name()),
            }
        }
        assert!(matches!(
            parse_percentile_spec("max").unwrap(),
            PercentileSpec::Max
        ));
        assert!(matches!(
            parse_percentile_spec("mean").unwrap(),
            PercentileSpec::Mean
        ));
    }

    #[test]
    fn parse_percentile_spec_invalid() {
        assert!(parse_percentile_spec("p0").is_err()); // 0/10 = 0.0 out of range
        assert!(parse_percentile_spec("p").is_err());
        assert!(parse_percentile_spec("pxyz").is_err());
        assert!(parse_percentile_spec("99").is_err());
        assert!(parse_percentile_spec("").is_err());
    }

    #[test]
    fn parse_percentile_spec_rejects_pathological_leading_zeros() {
        // Leading zeros let digits.len() run long while `n` stays tiny —
        // this used to reach `10u64.pow(digits.len())` unguarded and
        // overflow (panics in a debug/test build).
        let s = format!("p{}50", "0".repeat(100));
        assert!(parse_percentile_spec(&s).is_err());
    }

    #[test]
    fn make_queue_names_single_and_multi() {
        assert_eq!(make_queue_names("default", 1), vec!["default"]);
        assert_eq!(make_queue_names("q", 3), vec!["q_0", "q_1", "q_2"]);
    }

    fn base_cli() -> Cli {
        Cli {
            url: "redis://127.0.0.1:6379/13".into(),
            host: None,
            port: None,
            password: None,
            tls: false,
            db: 13,
            workers: vec![10, 50],
            jobs: 1000,
            warmup_jobs: 0,
            queue: "default".into(),
            num_queues: 1,
            latency_percentiles: vec!["p50".into()],
            tag: None,
            output: None,
            timeout: 300,
            quiet: false,
            allow_flushdb: false,
            allow_obliterate_active: false,
        }
    }

    #[test]
    fn validate_cli_accepts_defaults() {
        assert!(validate_cli(&base_cli()).is_ok());
    }

    #[test]
    fn validate_cli_rejects_zero_jobs() {
        let mut cli = base_cli();
        cli.jobs = 0;
        assert!(validate_cli(&cli).is_err());
    }

    #[test]
    fn validate_cli_rejects_zero_num_queues() {
        let mut cli = base_cli();
        cli.num_queues = 0;
        assert!(validate_cli(&cli).is_err());
    }

    #[test]
    fn validate_cli_rejects_zero_timeout() {
        let mut cli = base_cli();
        cli.timeout = 0;
        assert!(validate_cli(&cli).is_err());
    }

    #[test]
    fn validate_cli_rejects_empty_workers() {
        let mut cli = base_cli();
        cli.workers = vec![];
        assert!(validate_cli(&cli).is_err());
    }

    #[test]
    fn validate_cli_rejects_zero_in_workers_list() {
        let mut cli = base_cli();
        cli.workers = vec![10, 0, 50];
        assert!(validate_cli(&cli).is_err());
    }

    #[test]
    fn make_queue_names_zero_is_treated_as_one() {
        // n=0 can't happen in practice (main() validates --num-queues > 0
        // before calling this), but make_queue_names itself stays safe
        // regardless — it never returns an empty Vec, so any future caller
        // indexing queue_names[0] can't panic on this input.
        assert_eq!(make_queue_names("default", 0), vec!["default"]);
    }
}
