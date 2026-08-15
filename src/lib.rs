//! Library surface for `bullmq-bench`, split out from the binary purely so
//! integration tests under `tests/` (which need real `Queue`/`Worker`
//! plumbing against live Redis) can reuse the same enqueue/worker/payload
//! code the binary uses, instead of re-implementing it against
//! `bullmq-official` a second time and risking the two drifting apart.
//!
//! `main.rs` is the actual entry point and owns everything CLI-shaped (`Cli`,
//! trial orchestration, report formatting) — that stays binary-only.

pub mod job;
pub mod metrics;
pub mod producer;
pub mod worker;
