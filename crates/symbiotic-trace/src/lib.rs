//! Normalized invocation traces and pluggable sinks.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use symbiotic_core::{DiagnosticCode, FailureClass, ModelIdentity, QueueId, QueueItemId, TraceId};
use symbiotic_queue::{QueueEvent, QueueStatus};
use thiserror::Error;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InvocationOutcome {
    Succeeded,
    Failed,
    RateLimited,
    BudgetExhausted,
    TimedOut,
    Cancelled,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheStatus {
    #[default]
    NotApplicable,
    Miss,
    Hit,
    PartialHit,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CacheTrace {
    pub response_cache: CacheStatus,
    pub prompt_cache: CacheStatus,
    pub cached_input_tokens: Option<u64>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct UsageTrace {
    /// Provider-reported prompt tokens served from its cache.
    #[serde(default)]
    pub cache_hit_tokens: Option<u64>,
    /// Prompt tokens not served from the provider cache, including cache writes.
    #[serde(default)]
    pub cache_miss_tokens: Option<u64>,
    /// Provider response identifier, checked by the credential boundary.
    #[serde(default)]
    pub response_id: Option<String>,
    /// Provider-reported served model, checked by the credential boundary.
    #[serde(default)]
    pub served_model: Option<String>,
    /// Provider-reported creation time in Unix seconds; never synthesized.
    #[serde(default)]
    pub created: Option<i64>,
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub reasoning_tokens: Option<u64>,
    pub media_units: Option<u64>,
    pub cost_micro_usd: Option<u64>,
    /// Explicit provider-reported USD cost, preserved as a validated decimal string.
    /// Independent of integer micro-USD accounting; never rounded or price-estimated.
    pub reported_cost_usd: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct TimingTrace {
    pub queued_ms: Option<u64>,
    pub provider_ms: Option<u64>,
    pub total_ms: Option<u64>,
    /// Split of `queued_ms` (whose semantics are unchanged): `throttle_wait_ms`
    /// is time waiting on cooldowns/rate buckets, `queue_wait_ms` is the rest —
    /// claim/lease wait plus any failed attempts on retries. Absent on traces
    /// recorded before the split existed.
    pub queue_wait_ms: Option<u64>,
    pub throttle_wait_ms: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ModelInvocationTrace {
    pub trace_id: TraceId,
    pub queue_item_id: Option<QueueItemId>,
    pub model: ModelIdentity,
    pub role_binding: Option<String>,
    pub source: Option<String>,
    pub request_hash: String,
    pub response_hash: Option<String>,
    pub cache: CacheTrace,
    pub usage: UsageTrace,
    pub timing: TimingTrace,
    pub outcome: InvocationOutcome,
    /// Adapters can supply only a closed failure class.
    ///
    /// ```compile_fail
    /// fn inject(mut trace: symbiotic_trace::ModelInvocationTrace, text: String) {
    ///     trace.error_class = Some(text);
    /// }
    /// ```
    pub error_class: Option<FailureClass>,
    pub audit_refs: Vec<String>,
    pub metadata: Value,
    pub timestamp: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueueEventTrace {
    pub trace_id: TraceId,
    pub item_id: QueueItemId,
    pub queue_id: QueueId,
    pub kind: String,
    pub status: QueueStatus,
    pub attempt: u32,
    /// Queue diagnostics cannot carry free-form text.
    ///
    /// ```compile_fail
    /// fn inject(mut trace: symbiotic_trace::QueueEventTrace, text: String) {
    ///     trace.error = Some(text);
    /// }
    /// ```
    pub error: Option<DiagnosticCode>,
    pub timestamp: DateTime<Utc>,
    pub metadata: Value,
}

impl From<QueueEvent> for QueueEventTrace {
    fn from(event: QueueEvent) -> Self {
        Self {
            trace_id: TraceId::new(),
            item_id: event.item_id,
            queue_id: event.queue_id,
            kind: event.kind,
            status: event.status,
            attempt: event.attempt,
            error: event.error,
            timestamp: event.timestamp,
            metadata: serde_json::json!({}),
        }
    }
}

#[derive(Debug, Error)]
pub enum TraceError {
    #[error("trace sink failed: {0}")]
    Sink(symbiotic_core::DiagnosticCode),
}

impl TraceError {
    /// Static diagnostic for logs and runtime bookkeeping.
    pub const fn code(&self) -> symbiotic_core::DiagnosticCode {
        match self {
            Self::Sink(code) => *code,
        }
    }
}

#[async_trait]
pub trait TraceSink: Send + Sync {
    async fn record_model_invocation(&self, trace: ModelInvocationTrace) -> Result<(), TraceError>;
}

#[async_trait]
pub trait QueueTraceSink: Send + Sync {
    async fn record_queue_event(&self, trace: QueueEventTrace) -> Result<(), TraceError>;
}

pub struct FanoutTraceSink {
    sinks: Vec<Box<dyn TraceSink>>,
}

impl FanoutTraceSink {
    pub fn new(sinks: Vec<Box<dyn TraceSink>>) -> Self {
        Self { sinks }
    }
}

#[async_trait]
impl TraceSink for FanoutTraceSink {
    async fn record_model_invocation(&self, trace: ModelInvocationTrace) -> Result<(), TraceError> {
        for sink in &self.sinks {
            sink.record_model_invocation(trace.clone()).await?;
        }
        Ok(())
    }
}

#[derive(Clone, Default)]
pub struct InMemoryTraceSink {
    records: Arc<Mutex<Vec<ModelInvocationTrace>>>,
}

impl InMemoryTraceSink {
    pub fn records(&self) -> Vec<ModelInvocationTrace> {
        self.records.lock().expect("trace sink lock").clone()
    }
}

#[async_trait]
impl TraceSink for InMemoryTraceSink {
    async fn record_model_invocation(&self, trace: ModelInvocationTrace) -> Result<(), TraceError> {
        self.records.lock().expect("trace sink lock").push(trace);
        Ok(())
    }
}

#[derive(Clone, Default)]
pub struct InMemoryQueueTraceSink {
    records: Arc<Mutex<Vec<QueueEventTrace>>>,
}

impl InMemoryQueueTraceSink {
    pub fn records(&self) -> Vec<QueueEventTrace> {
        self.records.lock().expect("queue trace sink lock").clone()
    }
}

#[async_trait]
impl QueueTraceSink for InMemoryQueueTraceSink {
    async fn record_queue_event(&self, trace: QueueEventTrace) -> Result<(), TraceError> {
        self.records
            .lock()
            .expect("queue trace sink lock")
            .push(trace);
        Ok(())
    }
}

/// Run synchronous work on the blocking pool, preserving panics and caller errors.
async fn run_blocking<T: Send + 'static, E: Send + 'static>(
    work: impl FnOnce() -> Result<T, E> + Send + 'static,
    cancelled: E,
) -> Result<T, E> {
    match tokio::task::spawn_blocking(work).await {
        Ok(output) => output,
        Err(err) if err.is_panic() => std::panic::resume_unwind(err.into_panic()),
        Err(_) => Err(cancelled),
    }
}

async fn append_jsonl(
    file: Arc<Mutex<std::fs::File>>,
    trace: impl Serialize + Send + 'static,
) -> Result<(), TraceError> {
    run_blocking(
        move || {
            use std::io::Write;
            let failed = || TraceError::Sink(DiagnosticCode::StorageFailure);
            let mut line = serde_json::to_vec(&trace).map_err(|_| failed())?;
            line.push(b'\n');
            let mut file = file.lock().map_err(|_| failed())?;
            file.write_all(&line).map_err(|_| failed())?;
            file.flush().map_err(|_| failed())
        },
        TraceError::Sink(DiagnosticCode::StorageFailure),
    )
    .await
}

pub struct JsonlTraceSink {
    path: PathBuf,
    file: Arc<Mutex<std::fs::File>>,
}

impl JsonlTraceSink {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, TraceError> {
        if let Some(parent) = path.as_ref().parent() {
            std::fs::create_dir_all(parent)
                .map_err(|_| TraceError::Sink(symbiotic_core::DiagnosticCode::StorageFailure))?;
        }
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path.as_ref())
            .map_err(|_| TraceError::Sink(symbiotic_core::DiagnosticCode::StorageFailure))?;
        Ok(Self {
            path: path.as_ref().to_path_buf(),
            file: Arc::new(Mutex::new(file)),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn read(path: impl AsRef<Path>) -> Result<Vec<ModelInvocationTrace>, TraceError> {
        if !path.as_ref().is_file() {
            return Ok(Vec::new());
        }
        let raw = std::fs::read_to_string(path)
            .map_err(|_| TraceError::Sink(symbiotic_core::DiagnosticCode::StorageFailure))?;
        raw.lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| {
                serde_json::from_str(line)
                    .map_err(|_| TraceError::Sink(symbiotic_core::DiagnosticCode::StorageFailure))
            })
            .collect()
    }
}

#[async_trait]
impl TraceSink for JsonlTraceSink {
    async fn record_model_invocation(&self, trace: ModelInvocationTrace) -> Result<(), TraceError> {
        append_jsonl(self.file.clone(), trace).await
    }
}

pub struct JsonlQueueTraceSink {
    path: PathBuf,
    file: Arc<Mutex<std::fs::File>>,
}

impl JsonlQueueTraceSink {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, TraceError> {
        if let Some(parent) = path.as_ref().parent() {
            std::fs::create_dir_all(parent)
                .map_err(|_| TraceError::Sink(symbiotic_core::DiagnosticCode::StorageFailure))?;
        }
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path.as_ref())
            .map_err(|_| TraceError::Sink(symbiotic_core::DiagnosticCode::StorageFailure))?;
        Ok(Self {
            path: path.as_ref().to_path_buf(),
            file: Arc::new(Mutex::new(file)),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn read(path: impl AsRef<Path>) -> Result<Vec<QueueEventTrace>, TraceError> {
        if !path.as_ref().is_file() {
            return Ok(Vec::new());
        }
        let raw = std::fs::read_to_string(path)
            .map_err(|_| TraceError::Sink(symbiotic_core::DiagnosticCode::StorageFailure))?;
        raw.lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| {
                serde_json::from_str(line)
                    .map_err(|_| TraceError::Sink(symbiotic_core::DiagnosticCode::StorageFailure))
            })
            .collect()
    }
}

#[async_trait]
impl QueueTraceSink for JsonlQueueTraceSink {
    async fn record_queue_event(&self, trace: QueueEventTrace) -> Result<(), TraceError> {
        append_jsonl(self.file.clone(), trace).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use chrono::Utc;
    use symbiotic_core::{
        DiagnosticCode, FailureClass, ModelIdentity, QueueId, QueueItemId, TraceId,
    };
    use symbiotic_queue::QueueStatus;

    use super::*;

    struct VecSink {
        records: Arc<Mutex<Vec<ModelInvocationTrace>>>,
    }

    #[async_trait]
    impl TraceSink for VecSink {
        async fn record_model_invocation(
            &self,
            trace: ModelInvocationTrace,
        ) -> Result<(), TraceError> {
            self.records.lock().unwrap().push(trace);
            Ok(())
        }
    }

    fn sample_trace() -> ModelInvocationTrace {
        ModelInvocationTrace {
            trace_id: TraceId::new(),
            queue_item_id: None,
            model: ModelIdentity::new("chat", "codex", "gpt-5.5-codex"),
            role_binding: Some("agent.plan".to_string()),
            source: Some("test".to_string()),
            request_hash: "req".to_string(),
            response_hash: Some("res".to_string()),
            cache: CacheTrace::default(),
            usage: UsageTrace {
                input_tokens: Some(10),
                output_tokens: Some(5),
                reasoning_tokens: None,
                media_units: None,
                cost_micro_usd: Some(42),
                reported_cost_usd: Some("0.000042123456789".into()),
                ..UsageTrace::default()
            },
            timing: TimingTrace {
                queued_ms: Some(1),
                provider_ms: Some(2),
                total_ms: Some(3),
                queue_wait_ms: None,
                throttle_wait_ms: None,
            },
            outcome: InvocationOutcome::Succeeded,
            error_class: None,
            audit_refs: vec!["audit:1".to_string()],
            metadata: serde_json::json!({}),
            timestamp: Utc::now(),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn jsonl_writes_leave_the_async_worker_available() {
        let dir = tempfile::tempdir().unwrap();
        let model = Arc::new(JsonlTraceSink::open(dir.path().join("model.jsonl")).unwrap());
        let queue = Arc::new(JsonlQueueTraceSink::open(dir.path().join("queue.jsonl")).unwrap());
        for queue_write in [false, true] {
            let model = model.clone();
            let queue = queue.clone();
            let (locked_tx, locked_rx) = std::sync::mpsc::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let locker_model = model.clone();
            let locker_queue = queue.clone();
            let locker = std::thread::spawn(move || {
                let _guard = if queue_write {
                    locker_queue.file.lock().unwrap()
                } else {
                    locker_model.file.lock().unwrap()
                };
                locked_tx.send(()).unwrap();
                // A watchdog releases the lock even when the old synchronous sink
                // stalls this test's only async worker.
                release_rx
                    .recv_timeout(std::time::Duration::from_secs(2))
                    .is_ok()
            });
            locked_rx.recv().unwrap();
            let write = tokio::spawn(async move {
                if queue_write {
                    queue.record_queue_event(sample_queue_trace()).await
                } else {
                    model.record_model_invocation(sample_trace()).await
                }
            });
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            let _ = release_tx.send(());
            assert!(
                locker.join().unwrap(),
                "JSONL write blocked the async worker"
            );
            write.await.unwrap().unwrap();
        }
    }

    #[tokio::test]
    async fn concurrent_jsonl_writes_produce_whole_lines() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("concurrent.jsonl");
        let sink = Arc::new(JsonlTraceSink::open(&path).unwrap());
        let mut writes = tokio::task::JoinSet::new();
        for index in 0..64 {
            let sink = sink.clone();
            writes.spawn(async move {
                let mut trace = sample_trace();
                trace.request_hash = index.to_string();
                sink.record_model_invocation(trace).await
            });
        }
        while let Some(write) = writes.join_next().await {
            write.unwrap().unwrap();
        }
        let raw = std::fs::read_to_string(&path).unwrap();
        assert!(raw.ends_with('\n'));
        let records: Vec<ModelInvocationTrace> = raw
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(records.len(), 64);
        let ids: std::collections::HashSet<_> = records
            .iter()
            .map(|trace| trace.request_hash.as_str())
            .collect();
        assert_eq!(ids.len(), 64);
    }

    #[tokio::test]
    async fn fanout_sends_trace_to_all_sinks() {
        let first = Arc::new(Mutex::new(Vec::new()));
        let second = Arc::new(Mutex::new(Vec::new()));
        let sink = FanoutTraceSink::new(vec![
            Box::new(VecSink {
                records: first.clone(),
            }),
            Box::new(VecSink {
                records: second.clone(),
            }),
        ]);

        sink.record_model_invocation(sample_trace()).await.unwrap();

        assert_eq!(first.lock().unwrap().len(), 1);
        assert_eq!(second.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn jsonl_sink_round_trips_trace() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("trace.jsonl");
        let sink = JsonlTraceSink::open(&path).unwrap();
        let mut trace = sample_trace();
        trace.outcome = InvocationOutcome::RateLimited;
        trace.error_class = Some(FailureClass::RateLimited);
        sink.record_model_invocation(trace).await.unwrap();

        let records = JsonlTraceSink::read(&path).unwrap();
        assert_eq!(records[0].error_class, Some(FailureClass::RateLimited));
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].usage.input_tokens, Some(10));
        assert_eq!(
            records[0].usage.reported_cost_usd.as_deref(),
            Some("0.000042123456789")
        );
    }

    #[test]
    fn jsonl_readers_refuse_unknown_diagnostics_without_returning_stored_text() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("trace.jsonl");
        let stored_text = "credential-text-in-stored-diagnostic";
        let mut model = serde_json::to_value(sample_trace()).unwrap();
        model["error_class"] = serde_json::json!(stored_text);
        std::fs::write(&path, model.to_string()).unwrap();
        let model_error = JsonlTraceSink::read(&path).unwrap_err();
        let mut queue = serde_json::to_value(sample_queue_trace()).unwrap();
        queue["error"] = serde_json::json!(stored_text);
        std::fs::write(&path, queue.to_string()).unwrap();
        let queue_error = JsonlQueueTraceSink::read(&path).unwrap_err();
        for error in [model_error, queue_error] {
            assert_eq!(error.code(), symbiotic_core::DiagnosticCode::StorageFailure);
            assert!(!format!("{error:?} {error}").contains(stored_text));
        }
    }

    fn sample_queue_trace() -> QueueEventTrace {
        QueueEventTrace {
            trace_id: TraceId::new(),
            item_id: QueueItemId::new(),
            queue_id: QueueId::new("model:deepseek:flash"),
            kind: "chat".to_string(),
            status: QueueStatus::Failed,
            attempt: 2,
            error: Some(DiagnosticCode::HttpRateLimited),
            timestamp: Utc::now(),
            metadata: serde_json::json!({}),
        }
    }

    #[tokio::test]
    async fn jsonl_queue_sink_round_trips_trace() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("queue-trace.jsonl");
        let sink = JsonlQueueTraceSink::open(&path).unwrap();
        sink.record_queue_event(sample_queue_trace()).await.unwrap();

        let records = JsonlQueueTraceSink::read(&path).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].kind, "chat");
        assert_eq!(records[0].status, QueueStatus::Failed);
        assert_eq!(records[0].error, Some(DiagnosticCode::HttpRateLimited));
    }
}
