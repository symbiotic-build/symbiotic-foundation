//! Provider-neutral durable queue contracts.
//!
//! The queue is intentionally model-agnostic. Model/provider code chooses the
//! `queue_id`; a backend enforces durable work semantics for that id.
//!
//! The contracts (`QueueBackend`, `QueueEventSink`, items, events, telemetry)
//! need no storage, so this crate has no storage dependency. It ships the
//! in-process [`MemoryQueue`] backend; the local SQLite backend is the separate
//! `symbiotic-queue-sqlite` crate. The `conformance` feature exposes the checks
//! every backend must pass.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use symbiotic_core::{QueueId, QueueItemId};
use thiserror::Error;

#[cfg(feature = "conformance")]
pub mod conformance;
pub mod jobs;
mod memory;
pub mod runner;

pub use memory::{DEFAULT_RETAINED_TERMINAL_ITEMS, MemoryQueue};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueueStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
    Dead,
    /// Explicit terminal refusal; automatic retries and budget renewal are forbidden.
    Stopped,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueuePolicy {
    pub queue_id: QueueId,
    pub max_in_flight: usize,
    pub lease_seconds: u64,
    pub max_attempts: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EnqueueRequest {
    pub queue_id: QueueId,
    pub kind: String,
    pub payload: Value,
    pub idempotency_key: Option<String>,
    pub run_after: Option<DateTime<Utc>>,
    pub max_attempts: Option<u32>,
    #[serde(default)]
    pub force: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EnqueueDisposition {
    Inserted,
    ActiveDuplicate,
    TerminalDuplicate,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EnqueueOutcome {
    pub item: QueueItem,
    pub disposition: EnqueueDisposition,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueueItem {
    pub item_id: QueueItemId,
    pub queue_id: QueueId,
    pub kind: String,
    pub payload: Value,
    pub status: QueueStatus,
    pub attempt: u32,
    pub max_attempts: u32,
    pub run_after: DateTime<Utc>,
    pub lease_owner: Option<String>,
    pub lease_until: Option<DateTime<Utc>>,
    pub idempotency_key: Option<String>,
    pub last_error: Option<symbiotic_core::DiagnosticCode>,
    /// Stable class of `last_error` (for example `rate_limited`), recorded
    /// by [`QueueBackend::fail_with`]; `None` for failures recorded without
    /// one.
    #[serde(default)]
    pub last_error_class: Option<symbiotic_core::FailureClass>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClaimRequest {
    pub queue_id: QueueId,
    pub worker_id: String,
    pub limit: usize,
    pub lease_seconds: u64,
    #[serde(default)]
    pub max_in_flight: Option<usize>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueueEvent {
    pub item_id: QueueItemId,
    pub queue_id: QueueId,
    pub kind: String,
    pub status: QueueStatus,
    pub attempt: u32,
    pub timestamp: DateTime<Utc>,
    pub error: Option<symbiotic_core::DiagnosticCode>,
}

/// A failed attempt as [`QueueBackend::fail_with`] records it.
/// Provider or stored text cannot be attached to durable failure records.
/// ```compile_fail
/// use symbiotic_queue::Failure;
/// let key = "synthetic-queue-key";
/// let failure = Failure { error: format!("invalid key {key}"), error_class: None, run_after: None };
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Failure {
    pub error: symbiotic_core::DiagnosticCode,
    /// Stable class of the error, kept on the item as `last_error_class`.
    pub error_class: Option<symbiotic_core::FailureClass>,
    /// Earliest time of the next attempt. `None` stops the item permanently.
    pub run_after: Option<DateTime<Utc>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailOutcome {
    RetryScheduled,
    MovedToDead,
    /// The failure has no retry deadline and the item is permanently stopped.
    Stopped,
}

#[derive(Debug, Error)]
pub enum QueueError {
    #[error("queue item not found: {0}")]
    NotFound(symbiotic_core::DiagnosticCode),
    #[error("queue item is not leased by worker: {0}")]
    LeaseMismatch(symbiotic_core::DiagnosticCode),
    #[error("queue item is not running: {0}")]
    NotRunning(symbiotic_core::DiagnosticCode),
    #[error("queue backend unavailable: {0}")]
    Unavailable(symbiotic_core::DiagnosticCode),
    #[error("queue backend rejected request: {0}")]
    InvalidRequest(symbiotic_core::DiagnosticCode),
    #[error("queue storage failed: {0}")]
    Storage(symbiotic_core::DiagnosticCode),
}

impl QueueError {
    /// Static diagnostic for logs and runtime bookkeeping.
    pub const fn code(&self) -> symbiotic_core::DiagnosticCode {
        match self {
            Self::NotFound(code)
            | Self::LeaseMismatch(code)
            | Self::NotRunning(code)
            | Self::Unavailable(code)
            | Self::InvalidRequest(code)
            | Self::Storage(code) => *code,
        }
    }
}

/// Provider-neutral queue and generic job-store operations.
///
/// Job-store access performs blocking storage work. Async callers must run that
/// work off executor threads, as the runner does through its SQLite backend's
/// blocking-task dispatch in [`Self::jobs`].
#[async_trait]
pub trait QueueBackend: Send + Sync {
    /// Current queue time. Schedulers use the same clock as lease and retry deadlines.
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }

    /// Execute an atomic generic-job operation using the trusted host's clock
    /// and versioned configuration. Authority verification belongs to the host
    /// inside the acceptance transaction. Sample the clock once after acquiring
    /// the write transaction; use that time for every fence and deadline.
    /// Backends without a job store fail visibly.
    async fn jobs(
        &self,
        _scope: &jobs::JobScope,
        _config: &jobs::JobConfig,
        _clock: jobs::JobClock,
        _request: jobs::JobRequest,
    ) -> Result<jobs::JobResponse, jobs::JobError> {
        Err(jobs::JobError::Unavailable)
    }
    async fn enqueue(&self, request: EnqueueRequest) -> Result<EnqueueOutcome, QueueError>;
    /// Force-enqueue `request` only while `current` is still the newest item
    /// for its idempotency key; otherwise return that newest item unchanged,
    /// as `ActiveDuplicate` or `TerminalDuplicate`. The check and the insert
    /// are one step, so two callers holding the same superseded item cannot
    /// both replace it. The default is not atomic; both Foundation backends
    /// are.
    /// A Failed item reclaimed with LeaseExpired may also be replaced; the
    /// caller must establish that the expired claim never dispatched. Retire
    /// that old item as Stopped atomically so it cannot be claimed again.
    async fn enqueue_replacing(
        &self,
        mut request: EnqueueRequest,
        current: &QueueItemId,
    ) -> Result<EnqueueOutcome, QueueError> {
        request.force = false;
        let newest = self.enqueue(request.clone()).await?;
        if newest.item.item_id != *current
            || newest.disposition != EnqueueDisposition::TerminalDuplicate
        {
            return Ok(newest);
        }
        request.force = true;
        self.enqueue(request).await
    }
    async fn claim(&self, request: ClaimRequest) -> Result<Vec<QueueItem>, QueueError>;
    async fn claim_item(
        &self,
        item_id: &QueueItemId,
        worker_id: &str,
        lease_seconds: u64,
        max_in_flight: Option<usize>,
    ) -> Result<Option<QueueItem>, QueueError>;
    async fn get_item(&self, item_id: &QueueItemId) -> Result<Option<QueueItem>, QueueError>;
    async fn heartbeat(
        &self,
        item_id: &QueueItemId,
        worker_id: &str,
        lease_seconds: u64,
    ) -> Result<(), QueueError>;
    async fn complete(&self, item_id: &QueueItemId, worker_id: &str) -> Result<(), QueueError>;
    async fn fail(
        &self,
        item_id: &QueueItemId,
        worker_id: &str,
        error: symbiotic_core::DiagnosticCode,
        retry_after_seconds: Option<u64>,
    ) -> Result<FailOutcome, QueueError>;
    /// Record the error class and exact retry deadline. A failure without a
    /// deadline must become [`QueueStatus::Stopped`], regardless of attempts
    /// remaining. Backends must preserve this refusal for duplicate callers.
    async fn fail_with(
        &self,
        item_id: &QueueItemId,
        worker_id: &str,
        failure: Failure,
    ) -> Result<FailOutcome, QueueError>;
    /// Return items whose lease expired to `Failed`, or to `Dead` when that
    /// lease was their last allowed attempt.
    async fn reclaim_expired_leases(&self, queue_id: &QueueId) -> Result<usize, QueueError>;
    async fn cooldown_until(
        &self,
        _queue_id: &QueueId,
    ) -> Result<Option<DateTime<Utc>>, QueueError> {
        Ok(None)
    }
    async fn note_cooldown(
        &self,
        _queue_id: &QueueId,
        _until: DateTime<Utc>,
    ) -> Result<(), QueueError> {
        Ok(())
    }
}

#[async_trait]
pub trait QueueEventSink: Send + Sync {
    async fn record_queue_event(&self, event: QueueEvent);
}

/// Latency distribution over millisecond samples (nearest-rank percentiles).
///
/// Additive-only: `serde(default)` so persisted summaries keep loading as
/// fields are added.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct LatencyStats {
    pub count: u64,
    pub p50_ms: u64,
    pub p95_ms: u64,
    pub max_ms: u64,
}

impl LatencyStats {
    pub fn from_samples(samples: &[u64]) -> Self {
        if samples.is_empty() {
            return Self::default();
        }
        let mut sorted = samples.to_vec();
        sorted.sort_unstable();
        Self {
            count: sorted.len() as u64,
            p50_ms: nearest_rank(&sorted, 50),
            p95_ms: nearest_rank(&sorted, 95),
            max_ms: *sorted.last().expect("non-empty samples"),
        }
    }
}

/// Nearest-rank percentile over an ascending-sorted, non-empty sample set.
fn nearest_rank(sorted: &[u64], percentile: u64) -> u64 {
    let rank = (percentile * sorted.len() as u64).div_ceil(100).max(1);
    sorted[(rank - 1) as usize]
}

/// Per-queue latency summary separating the three phases of a queued provider
/// call: semaphore wait (`queue_wait`), rate-bucket wait (`throttle_wait`), and
/// the provider HTTP call itself (`provider`). Throttle-wait vs http-time is
/// the split that matters when diagnosing slow/serial queues — measure it here
/// instead of reasoning from configured rates.
///
/// Additive-only: `serde(default)` so persisted summaries keep loading as
/// fields are added.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct QueueTelemetrySummary {
    pub queue_id: String,
    pub queue_wait: LatencyStats,
    pub throttle_wait: LatencyStats,
    pub provider: LatencyStats,
}

#[derive(Clone, Debug, Default)]
struct QueueTelemetrySamples {
    queue_wait_ms: Vec<u64>,
    throttle_wait_ms: Vec<u64>,
    provider_ms: Vec<u64>,
}

/// Accumulates per-queue latency samples and folds them into
/// [`QueueTelemetrySummary`] rows. Pure in-memory math, deliberately decoupled
/// from any recorded trace type so hosts can feed it from whatever telemetry
/// they already persist (JSONL queue events, timing traces, ...). Keyed by the
/// plain queue-id string so both `QueueId` newtypes and raw strings fit.
#[derive(Clone, Debug, Default)]
pub struct QueueTelemetryAccumulator {
    queues: BTreeMap<String, QueueTelemetrySamples>,
}

impl QueueTelemetryAccumulator {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn observe_queue_wait(&mut self, queue_id: &str, ms: u64) {
        self.samples(queue_id).queue_wait_ms.push(ms);
    }

    pub fn observe_throttle_wait(&mut self, queue_id: &str, ms: u64) {
        self.samples(queue_id).throttle_wait_ms.push(ms);
    }

    pub fn observe_provider(&mut self, queue_id: &str, ms: u64) {
        self.samples(queue_id).provider_ms.push(ms);
    }

    /// Summaries for every observed queue, ordered by queue id.
    pub fn summaries(&self) -> Vec<QueueTelemetrySummary> {
        self.queues
            .iter()
            .map(|(queue_id, samples)| QueueTelemetrySummary {
                queue_id: queue_id.clone(),
                queue_wait: LatencyStats::from_samples(&samples.queue_wait_ms),
                throttle_wait: LatencyStats::from_samples(&samples.throttle_wait_ms),
                provider: LatencyStats::from_samples(&samples.provider_ms),
            })
            .collect()
    }

    fn samples(&mut self, queue_id: &str) -> &mut QueueTelemetrySamples {
        self.queues.entry(queue_id.to_string()).or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_telemetry_summaries_report_nearest_rank_percentiles() {
        let mut accumulator = QueueTelemetryAccumulator::new();
        for ms in 1..=100 {
            accumulator.observe_throttle_wait("chat:deepseek:pro", ms);
        }
        accumulator.observe_provider("chat:deepseek:pro", 40);
        accumulator.observe_provider("chat:deepseek:pro", 10);
        accumulator.observe_provider("chat:deepseek:pro", 20);
        accumulator.observe_queue_wait("chat:acme:other", 7);

        let summaries = accumulator.summaries();
        assert_eq!(summaries.len(), 2);
        // Ordered by queue id.
        assert_eq!(summaries[0].queue_id, "chat:acme:other");
        assert_eq!(summaries[1].queue_id, "chat:deepseek:pro");

        let deepseek = &summaries[1];
        assert_eq!(deepseek.throttle_wait.count, 100);
        assert_eq!(deepseek.throttle_wait.p50_ms, 50);
        assert_eq!(deepseek.throttle_wait.p95_ms, 95);
        assert_eq!(deepseek.throttle_wait.max_ms, 100);
        assert_eq!(deepseek.provider.count, 3);
        assert_eq!(deepseek.provider.p50_ms, 20);
        assert_eq!(deepseek.provider.p95_ms, 40);
        assert_eq!(deepseek.provider.max_ms, 40);
        // No queue-wait samples observed for this queue: empty stats, not a panic.
        assert_eq!(deepseek.queue_wait, LatencyStats::default());

        let other = &summaries[0];
        assert_eq!(other.queue_wait.count, 1);
        assert_eq!(other.queue_wait.p50_ms, 7);
        assert_eq!(other.queue_wait.p95_ms, 7);
    }
}
