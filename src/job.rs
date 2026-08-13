use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

/// BullMQ job name used for every load-test job. BullMQ jobs are named
/// (unlike Sidekiq's `class`); the name has no benchmark significance here.
pub const JOB_NAME: &str = "load-job";

/// Payload embedded in every job's `data` field.
///
/// bullmq-official's `Job` does expose `timestamp()` (job creation time), but
/// it is millisecond-granularity and is the time the *Lua script* stamped the
/// job, not a value the worker can cheaply diff against without an extra
/// round trip. Mirroring sidekiq-benchmark's pattern (see its `job.rs`), we
/// embed our own nanosecond enqueue timestamp directly in the job `data` JSON
/// and read it back in the worker's processor callback — this works
/// regardless of what the crate exposes and gives full microsecond
/// resolution end to end.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct JobPayload {
    /// Monotonically increasing sequence number (0-based), for debugging/tracing.
    pub seq: u64,
    /// Nanoseconds since UNIX_EPOCH at the moment this payload was constructed
    /// (i.e. immediately before the job is handed to `Queue::add_bulk`).
    pub enqueued_at_ns: u64,
}

impl JobPayload {
    /// Build a payload for job `seq`, stamped with the current time.
    pub fn new(seq: u64) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before epoch");
        Self {
            seq,
            enqueued_at_ns: now.as_nanos() as u64,
        }
    }

    /// Extract `enqueued_at_ns` back out of a job's `data` value, as read by
    /// the worker via `Job::data()`.
    pub fn enqueued_at_ns(data: &serde_json::Value) -> Option<u64> {
        data.get("enqueued_at_ns")?.as_u64()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_roundtrips_through_json() {
        let payload = JobPayload::new(42);
        let value = serde_json::to_value(&payload).unwrap();
        assert_eq!(value["seq"], 42);
        let back = JobPayload::enqueued_at_ns(&value);
        assert_eq!(back, Some(payload.enqueued_at_ns));
    }

    #[test]
    fn enqueued_at_ns_missing_field_returns_none() {
        let value = serde_json::json!({"seq": 1});
        assert_eq!(JobPayload::enqueued_at_ns(&value), None);
    }

    #[test]
    fn enqueued_at_ns_wrong_type_returns_none() {
        let value = serde_json::json!({"enqueued_at_ns": "not-a-number"});
        assert_eq!(JobPayload::enqueued_at_ns(&value), None);
    }

    #[test]
    fn new_payloads_have_increasing_timestamps() {
        let a = JobPayload::new(0);
        std::thread::sleep(std::time::Duration::from_micros(10));
        let b = JobPayload::new(1);
        assert!(b.enqueued_at_ns >= a.enqueued_at_ns);
    }
}
