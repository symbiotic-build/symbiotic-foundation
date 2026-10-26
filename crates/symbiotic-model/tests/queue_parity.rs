//! Queued-provider behaviour hosts depend on: shared admission, usage
//! receipts, retry timing and classification, eviction recovery, request
//! capture, the response-cache seam, and attempts that outlive a caller who
//! stopped waiting. Loopback providers only.
#![cfg(feature = "queue")]

use async_trait::async_trait;
use chrono::Utc;
use serde_json::{Value, json};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use symbiotic_core::{ModelIdentity, QueueId, QueueItemId, TraceId};
use symbiotic_model::{
    CacheEntry, ChatMessage, ChatProvider, ChatRequest, ChatResponse, InMemoryReceiptSink,
    ModelAdmission, ModelCapability, ModelError, ModelProvider, ModelQueueConfig, ProviderAuthMode,
    ProviderClass, ProviderDescriptor, QueuedChatProvider, ReceiptStatus, ResponseCache,
};
use symbiotic_queue::{
    ClaimRequest, EnqueueOutcome, EnqueueRequest, FailOutcome, Failure, MemoryQueue, QueueBackend,
    QueueError, QueueItem, QueueStatus,
};
use symbiotic_queue_sqlite::SqliteQueue;
use symbiotic_trace::{
    CacheTrace, InvocationOutcome, ModelInvocationTrace, TimingTrace, UsageTrace,
};
use tokio::time::Instant;

static MODEL_COUNTER: AtomicUsize = AtomicUsize::new(0);

/// A fresh model name per test: rate buckets and cooldowns are per model.
fn unique_identity() -> ModelIdentity {
    ModelIdentity::new(
        "chat",
        "loopback",
        format!("parity-{}", MODEL_COUNTER.fetch_add(1, Ordering::SeqCst)),
    )
}

#[path = "../../test-support/clock.rs"]
mod test_clock;

fn memory_queue() -> MemoryQueue {
    MemoryQueue::new().with_clock(test_clock::clock())
}

fn config() -> ModelQueueConfig {
    ModelQueueConfig {
        max_in_flight: 1,
        lease_seconds: 60,
        logical_retry_attempts: 3,
        retry_attempts: 3,
        retry_jitter_seconds: 0,
        request_timeout_seconds: Some(10),
        retry_base_delay_ms: 20,
        ..ModelQueueConfig::default()
    }
}

fn request(text: &str) -> ChatRequest {
    ChatRequest {
        messages: vec![ChatMessage {
            role: "user".to_string(),
            content: text.to_string(),
        }],
        max_output_tokens: None,
        temperature: Some(0.0),
        response_format: None,
        role_binding: None,
        source: None,
        metadata: json!({}),
    }
}

/// Loopback chat: echoes the last message, reports usage and a provider
/// receipt, optionally fails first with scripted errors, and records the
/// peak number of concurrent calls.
#[derive(Clone)]
struct Loopback {
    descriptor: ProviderDescriptor,
    calls: Arc<AtomicUsize>,
    active: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
    failures: Arc<Mutex<Vec<ModelError>>>,
    starts: Arc<Mutex<Vec<Instant>>>,
    delay: Duration,
    uncertain_failures: bool,
    completion_gate: Option<Arc<tokio::sync::Notify>>,
    credential: Option<Arc<symbiotic_model::OpenAiCompatibleChatProvider>>,
    raw_response: Option<Value>,
}

impl Loopback {
    fn new(identity: ModelIdentity) -> Self {
        Self {
            descriptor: ProviderDescriptor {
                identity,
                provider_class: ProviderClass::Cloud,
                capabilities: vec![ModelCapability::Chat],
                auth_mode: ProviderAuthMode::None,
                metadata: json!({}),
            },
            calls: Arc::new(AtomicUsize::new(0)),
            active: Arc::new(AtomicUsize::new(0)),
            peak: Arc::new(AtomicUsize::new(0)),
            failures: Arc::new(Mutex::new(Vec::new())),
            starts: Arc::new(Mutex::new(Vec::new())),
            delay: Duration::ZERO,
            uncertain_failures: false,
            completion_gate: None,
            credential: None,
            raw_response: None,
        }
    }

    fn failing_first(self, failures: Vec<ModelError>) -> Self {
        *self.failures.lock().unwrap() = failures;
        self
    }

    fn slow(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }
}

impl ModelProvider for Loopback {
    fn credential_fingerprint(&self) -> Option<String> {
        self.credential
            .as_ref()
            .and_then(|provider| provider.credential_fingerprint())
    }
    fn credential_boundary(&self) -> Option<&symbiotic_model::CredentialBoundary> {
        self.credential
            .as_ref()
            .and_then(|provider| provider.credential_boundary())
    }
    // Synthetic failures never perform transport or incur provider spend.
    fn failure_charge(&self, _: &ModelError) -> symbiotic_model::FailureCharge {
        if self.uncertain_failures {
            symbiotic_model::FailureCharge::Unknown
        } else {
            symbiotic_model::FailureCharge::KnownZero
        }
    }
    fn descriptor(&self) -> &ProviderDescriptor {
        &self.descriptor
    }
}

#[async_trait]
impl ChatProvider for Loopback {
    async fn chat(&self, request: ChatRequest) -> Result<ChatResponse, ModelError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.starts.lock().unwrap().push(Instant::now());
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(active, Ordering::SeqCst);
        // Count the call out even when it is cancelled mid-flight.
        let _active = Leaves(&self.active);
        tokio::time::sleep(self.delay).await;
        if let Some(gate) = &self.completion_gate {
            gate.notified().await;
        }
        drop(_active);
        let failure = {
            let mut failures = self.failures.lock().unwrap();
            (!failures.is_empty()).then(|| failures.remove(0))
        };
        if let Some(err) = failure {
            return Err(err);
        }
        Ok(ChatResponse {
            text: request.messages.last().unwrap().content.clone(),
            finish_reason: Some("stop".to_string()),
            trace: ModelInvocationTrace {
                trace_id: TraceId::new(),
                queue_item_id: None,
                model: self.descriptor.identity.clone(),
                role_binding: None,
                source: None,
                request_hash: String::new(),
                response_hash: None,
                cache: CacheTrace {
                    cached_input_tokens: Some(4),
                    ..CacheTrace::default()
                },
                usage: UsageTrace {
                    input_tokens: Some(10),
                    output_tokens: Some(2),
                    ..UsageTrace::default()
                },
                timing: TimingTrace::default(),
                outcome: InvocationOutcome::Succeeded,
                error_class: None,
                audit_refs: Vec::new(),
                metadata: json!({"provider": {"response_id": "loopback-1"}}),
                timestamp: Utc::now(),
            },
            raw_provider_response: self.raw_response.clone(),
        })
    }
}

/// Decrements a gauge when dropped.
struct Leaves<'a>(&'a AtomicUsize);

impl Drop for Leaves<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

fn queued(
    provider: Loopback,
    queue: Arc<dyn QueueBackend>,
    config: ModelQueueConfig,
) -> QueuedChatProvider<Loopback> {
    QueuedChatProvider::new(provider, queue, "worker", config)
        .with_spend_ledger(test_spend::ledger(), None)
}

fn queued_with_cache(
    provider: Loopback,
    queue: Arc<dyn QueueBackend>,
    config: ModelQueueConfig,
) -> QueuedChatProvider<Loopback> {
    queued(provider, queue, config).with_response_cache(Arc::new(TextKeyedCache::default()))
}

#[tokio::test]
async fn providers_sharing_admission_share_one_model_cap() {
    let queue: Arc<dyn QueueBackend> = Arc::new(memory_queue());
    let admission = ModelAdmission::new();
    let raw = Loopback::new(unique_identity()).slow(Duration::from_millis(20));
    let policy = ModelQueueConfig {
        max_in_flight: 2,
        ..config()
    };
    // Two roles bound to the same model.
    let first =
        queued(raw.clone(), queue.clone(), policy.clone()).with_admission(admission.clone());
    let second = queued(raw.clone(), queue.clone(), policy).with_admission(admission.clone());
    let calls = (0..12).map(|idx| {
        let provider = if idx % 2 == 0 {
            first.clone()
        } else {
            second.clone()
        };
        tokio::spawn(async move { provider.chat(request(&format!("call-{idx}"))).await })
    });
    for call in calls.collect::<Vec<_>>() {
        call.await.unwrap().unwrap();
    }
    assert_eq!(raw.calls.load(Ordering::SeqCst), 12);
    assert!(raw.peak.load(Ordering::SeqCst) <= 2);
}

#[tokio::test]
async fn a_second_cap_for_an_admitted_model_is_a_configuration_error() {
    let queue: Arc<dyn QueueBackend> = Arc::new(memory_queue());
    let admission = ModelAdmission::new();
    let raw = Loopback::new(unique_identity());
    let two = queued(
        raw.clone(),
        queue.clone(),
        ModelQueueConfig {
            max_in_flight: 2,
            ..config()
        },
    )
    .with_admission(admission.clone());
    let three = queued(
        raw,
        queue,
        ModelQueueConfig {
            max_in_flight: 3,
            ..config()
        },
    )
    .with_admission(admission);
    two.chat(request("first")).await.unwrap();
    let err = three.chat(request("second")).await.unwrap_err();
    assert!(matches!(err, ModelError::InvalidRequest(_)), "{err:?}");
}

#[tokio::test]
async fn receipts_cover_each_step_and_a_cache_hit_keeps_the_original_receipt() {
    let dir = tempfile::tempdir().unwrap();
    let queue: Arc<dyn QueueBackend> = Arc::new(memory_queue());
    let sink = Arc::new(InMemoryReceiptSink::default());
    let raw = Loopback::new(unique_identity());
    let provider = queued(
        raw.clone(),
        queue,
        ModelQueueConfig {
            response_cache_dir: Some(dir.path().to_path_buf()),
            ..config()
        },
    )
    .with_receipt_sink(sink.clone());

    provider.chat(request("hello")).await.unwrap();
    let replay = provider.chat(request("hello")).await.unwrap();
    assert_eq!(replay.text, "hello");
    assert_eq!(raw.calls.load(Ordering::SeqCst), 1);

    let receipts = sink.receipts();
    let statuses: Vec<_> = receipts.iter().map(|receipt| receipt.status).collect();
    assert_eq!(
        statuses,
        vec![
            ReceiptStatus::Queued,
            ReceiptStatus::Running,
            ReceiptStatus::Succeeded,
            ReceiptStatus::CacheHit,
        ]
    );
    let running = &receipts[1];
    assert_eq!(running.attempt, 1);
    assert!(running.queue_wait_ms.is_some() && running.throttle_wait_ms.is_some());
    let succeeded = &receipts[2];
    assert_eq!(succeeded.request_units, 1);
    assert!(succeeded.input_units >= 1);
    assert_eq!(succeeded.usage.as_ref().unwrap().input_tokens, Some(10));
    assert_eq!(
        succeeded.cache.as_ref().unwrap().cached_input_tokens,
        Some(4)
    );
    assert_eq!(succeeded.metadata["provider"]["response_id"], "loopback-1");
    assert!(succeeded.provider_ms.is_some());
    // The cache hit carries the original usage and receipt; it adds no call.
    let hit = &receipts[3];
    assert_eq!(hit.usage.as_ref().unwrap().output_tokens, Some(2));
    assert_eq!(hit.metadata["provider"]["response_id"], "loopback-1");
    assert!(hit.provider_ms.is_none());
}

#[tokio::test]
async fn retry_base_delay_allows_sub_second_backoff_and_receipts_record_the_failure() {
    let queue: Arc<dyn QueueBackend> = Arc::new(memory_queue());
    let sink = Arc::new(InMemoryReceiptSink::default());
    let raw = Loopback::new(unique_identity()).failing_first(vec![ModelError::Unavailable(
        symbiotic_core::DiagnosticCode::HttpUnavailable,
    )]);
    let provider = queued(raw.clone(), queue, config()).with_receipt_sink(sink.clone());

    let started = Instant::now();
    provider.chat(request("retry me")).await.unwrap();
    assert_eq!(raw.calls.load(Ordering::SeqCst), 2);
    // 20 ms backoff (plus the 20 ms x 2 cooldown) instead of the 1 s default.
    assert!(
        started.elapsed() < Duration::from_millis(900),
        "{:?}",
        started.elapsed()
    );
    let failed: Vec<_> = sink
        .receipts()
        .into_iter()
        .filter(|receipt| receipt.status == ReceiptStatus::Failed)
        .collect();
    assert_eq!(failed.len(), 1);
    assert_eq!(
        failed[0].error,
        Some(symbiotic_core::DiagnosticCode::HttpUnavailable)
    );
    assert_eq!(failed[0].attempt, 1);
}

#[tokio::test]
async fn provider_errors_retry_only_when_the_policy_opts_in() {
    let queue: Arc<dyn QueueBackend> = Arc::new(memory_queue());
    let strict = Loopback::new(unique_identity()).failing_first(vec![ModelError::Provider(
        symbiotic_core::DiagnosticCode::ProviderFailure,
    )]);
    let err = queued(strict.clone(), queue.clone(), config())
        .chat(request("strict"))
        .await
        .unwrap_err();
    assert!(matches!(err, ModelError::Provider(_)), "{err:?}");
    assert_eq!(strict.calls.load(Ordering::SeqCst), 1);

    let lenient = Loopback::new(unique_identity()).failing_first(vec![ModelError::Provider(
        symbiotic_core::DiagnosticCode::ProviderFailure,
    )]);
    let response = queued(
        lenient.clone(),
        queue,
        ModelQueueConfig {
            retry_provider_errors: true,
            ..config()
        },
    )
    .chat(request("lenient"))
    .await
    .unwrap();
    assert_eq!(response.text, "lenient");
    assert_eq!(lenient.calls.load(Ordering::SeqCst), 2);
}

/// A retention-bounded backend that evicts the caller's item just before
/// its first claim: another request finishes and pushes it out of a
/// one-item terminal window.
struct EvictsOnce {
    inner: MemoryQueue,
    evicted: Mutex<Option<QueueItemId>>,
}

impl EvictsOnce {
    fn new() -> Self {
        Self {
            inner: MemoryQueue::with_terminal_retention(1),
            evicted: Mutex::new(None),
        }
    }

    /// Finish `item_id` as another worker, then finish a filler item so the
    /// one-item terminal window drops it.
    async fn evict(&self, item_id: &QueueItemId) {
        self.inner
            .claim_item(item_id, "other-worker", 60, None)
            .await
            .unwrap()
            .expect("the target is claimable");
        self.inner.complete(item_id, "other-worker").await.unwrap();
        let filler = self
            .inner
            .enqueue(EnqueueRequest {
                queue_id: QueueId::new("chat:filler:filler"),
                kind: "chat".to_string(),
                payload: json!({}),
                idempotency_key: None,
                run_after: None,
                max_attempts: None,
                force: false,
            })
            .await
            .unwrap()
            .item;
        self.inner
            .claim_item(&filler.item_id, "other-worker", 60, None)
            .await
            .unwrap()
            .unwrap();
        self.inner
            .complete(&filler.item_id, "other-worker")
            .await
            .unwrap();
        assert!(self.inner.get_item(item_id).await.unwrap().is_none());
    }
}

#[async_trait]
impl QueueBackend for EvictsOnce {
    fn now(&self) -> chrono::DateTime<Utc> {
        self.inner.now()
    }
    async fn enqueue(&self, request: EnqueueRequest) -> Result<EnqueueOutcome, QueueError> {
        self.inner.enqueue(request).await
    }
    async fn claim(&self, request: ClaimRequest) -> Result<Vec<QueueItem>, QueueError> {
        self.inner.claim(request).await
    }
    async fn claim_item(
        &self,
        item_id: &QueueItemId,
        worker_id: &str,
        lease_seconds: u64,
        max_in_flight: Option<usize>,
    ) -> Result<Option<QueueItem>, QueueError> {
        let first = {
            let mut evicted = self.evicted.lock().unwrap();
            let first = evicted.is_none();
            if first {
                *evicted = Some(item_id.clone());
            }
            first
        };
        if first {
            self.evict(item_id).await;
        }
        self.inner
            .claim_item(item_id, worker_id, lease_seconds, max_in_flight)
            .await
    }
    async fn get_item(&self, item_id: &QueueItemId) -> Result<Option<QueueItem>, QueueError> {
        self.inner.get_item(item_id).await
    }
    async fn heartbeat(
        &self,
        item_id: &QueueItemId,
        worker_id: &str,
        lease_seconds: u64,
    ) -> Result<(), QueueError> {
        self.inner
            .heartbeat(item_id, worker_id, lease_seconds)
            .await
    }
    async fn complete(&self, item_id: &QueueItemId, worker_id: &str) -> Result<(), QueueError> {
        self.inner.complete(item_id, worker_id).await
    }
    async fn fail(
        &self,
        item_id: &QueueItemId,
        worker_id: &str,
        error: symbiotic_core::DiagnosticCode,
        retry_after_seconds: Option<u64>,
    ) -> Result<FailOutcome, QueueError> {
        self.inner
            .fail(item_id, worker_id, error, retry_after_seconds)
            .await
    }
    async fn fail_with(
        &self,
        item_id: &QueueItemId,
        worker_id: &str,
        failure: Failure,
    ) -> Result<FailOutcome, QueueError> {
        self.inner.fail_with(item_id, worker_id, failure).await
    }
    async fn reclaim_expired_leases(&self, queue_id: &QueueId) -> Result<usize, QueueError> {
        self.inner.reclaim_expired_leases(queue_id).await
    }
}

#[tokio::test]
async fn an_evicted_queue_item_is_queued_again_instead_of_failing() {
    let backend = Arc::new(EvictsOnce::new());
    let raw = Loopback::new(unique_identity());
    let response = queued(raw.clone(), backend.clone(), config())
        .chat(request("still here"))
        .await
        .unwrap();
    assert_eq!(response.text, "still here");
    assert_eq!(raw.calls.load(Ordering::SeqCst), 1);
    let evicted = backend.evicted.lock().unwrap().clone().unwrap();
    let replacement = response
        .trace
        .queue_item_id
        .expect("the call ran on a queue item");
    assert_ne!(replacement, evicted, "a replacement item was created");
    assert!(
        backend
            .inner
            .get_item(&replacement)
            .await
            .unwrap()
            .is_some()
    );
}

#[cfg(debug_assertions)]
#[tokio::test]
async fn request_debug_capture_writes_the_serialized_request() {
    let dir = tempfile::tempdir().unwrap();
    let queue: Arc<dyn QueueBackend> = Arc::new(memory_queue());
    let raw = Loopback::new(unique_identity());
    queued(
        raw,
        queue,
        ModelQueueConfig {
            request_debug_dir: Some(dir.path().to_path_buf()),
            ..config()
        },
    )
    .chat(request("capture me"))
    .await
    .unwrap();
    let captured: Vec<_> = std::fs::read_dir(
        std::fs::read_dir(dir.path().join("chat"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path(),
    )
    .unwrap()
    .map(|entry| entry.unwrap().path())
    .collect();
    assert_eq!(captured.len(), 1);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&captured[0])
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
    let body: Value = serde_json::from_slice(&std::fs::read(&captured[0]).unwrap()).unwrap();
    assert_eq!(body["messages"][0]["content"], "capture me");
}

/// A host cache keyed by raw message text. The runtime still verifies binding scope.
#[derive(Default)]
struct TextKeyedCache {
    entries: Mutex<std::collections::HashMap<String, Value>>,
    stores: AtomicUsize,
}

impl TextKeyedCache {
    fn key(entry: &CacheEntry<'_>) -> Option<String> {
        entry.request["messages"][0]["content"]
            .as_str()
            .map(str::to_string)
    }
}

impl ResponseCache for TextKeyedCache {
    fn load(&self, entry: &CacheEntry<'_>) -> Result<Option<Value>, ModelError> {
        Ok(Self::key(entry).and_then(|key| self.entries.lock().unwrap().get(&key).cloned()))
    }

    fn store(&self, entry: &CacheEntry<'_>, response: &Value) -> Result<(), ModelError> {
        self.stores.fetch_add(1, Ordering::SeqCst);
        if let Some(key) = Self::key(entry) {
            self.entries.lock().unwrap().insert(key, response.clone());
        }
        Ok(())
    }
}

#[tokio::test]
async fn a_host_response_cache_answers_before_the_queue() {
    let queue: Arc<dyn QueueBackend> = Arc::new(memory_queue());
    let raw = Loopback::new(unique_identity());
    let cache = Arc::new(TextKeyedCache::default());
    let provider = queued(raw.clone(), queue, config()).with_response_cache(cache.clone());
    // A runtime-scoped response already present under the host's own key.
    provider.chat(request("seeded")).await.unwrap();

    let hit = provider.chat(request("seeded")).await.unwrap();
    assert_eq!(hit.text, "seeded");
    assert_eq!(
        hit.trace.cache.response_cache,
        symbiotic_trace::CacheStatus::Hit
    );
    assert_eq!(raw.calls.load(Ordering::SeqCst), 1, "the seed call only");

    provider.chat(request("fresh")).await.unwrap();
    assert_eq!(raw.calls.load(Ordering::SeqCst), 2);
    assert_eq!(cache.stores.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn known_zero_retries_stop_at_an_uncertain_timeout() {
    let queue: Arc<dyn QueueBackend> = Arc::new(memory_queue());
    let raw = Loopback::new(unique_identity()).failing_first(vec![
        ModelError::RateLimited(symbiotic_core::DiagnosticCode::HttpRateLimited),
        ModelError::RateLimited(symbiotic_core::DiagnosticCode::HttpRateLimited),
        ModelError::Timeout(symbiotic_core::DiagnosticCode::HttpTimeout),
    ]);
    let err = queued(raw.clone(), queue, config())
        .chat(request("give up"))
        .await
        .unwrap_err();
    assert!(matches!(err, ModelError::Timeout(_)), "{err:?}");
    assert!(matches!(
        err,
        ModelError::Timeout(symbiotic_core::DiagnosticCode::HttpTimeout)
    ));
    assert_eq!(raw.calls.load(Ordering::SeqCst), 3);
}

#[derive(Clone)]
struct CountingClassifier {
    inner: symbiotic_model::StaticClassifierProvider,
    calls: Arc<AtomicUsize>,
}

impl ModelProvider for CountingClassifier {
    fn failure_charge(&self, _: &ModelError) -> symbiotic_model::FailureCharge {
        symbiotic_model::FailureCharge::KnownZero
    }
    fn descriptor(&self) -> &ProviderDescriptor {
        self.inner.descriptor()
    }
}

#[async_trait]
impl symbiotic_model::ClassifierProvider for CountingClassifier {
    async fn classify(
        &self,
        request: symbiotic_model::ClassifyRequest,
    ) -> Result<symbiotic_model::ClassifyResponse, ModelError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.inner.classify(request).await
    }
}

#[tokio::test]
async fn an_invalid_classify_request_takes_no_queue_slot() {
    use symbiotic_model::ClassifierProvider as _;
    let queue = Arc::new(memory_queue());
    let raw = CountingClassifier {
        inner: symbiotic_model::StaticClassifierProvider::new([
            symbiotic_model::ClassifierAnswer::noul("goal", 0.7),
        ]),
        calls: Arc::new(AtomicUsize::new(0)),
    };
    let classifier = symbiotic_model::QueuedClassifierProvider::new(
        raw.clone(),
        queue.clone(),
        "worker",
        config(),
    )
    .with_spend_ledger(test_spend::ledger(), None);
    let mut state = serde_json::Map::new();
    state.insert("message".into(), json!("synthetic"));
    let err = classifier
        .classify(symbiotic_model::ClassifyRequest::new(state, Vec::new()))
        .await
        .unwrap_err();
    assert!(matches!(err, ModelError::InvalidRequest(_)), "{err:?}");
    assert_eq!(raw.calls.load(Ordering::SeqCst), 0);
    assert!(queue.is_empty());
}

#[tokio::test]
async fn an_exhausted_budget_blocks_repeats_unless_the_policy_renews_it() {
    let down = || {
        Loopback::new(unique_identity()).failing_first(
            (0..4)
                .map(|_| ModelError::Unavailable(symbiotic_core::DiagnosticCode::HttpUnavailable))
                .collect(),
        )
    };
    let once = ModelQueueConfig {
        logical_retry_attempts: 1,
        retry_attempts: 1,
        ..config()
    };

    let kept = down();
    let provider = queued_with_cache(kept.clone(), Arc::new(memory_queue()), once.clone());
    provider.chat(request("same")).await.unwrap_err();
    let err = provider.chat(request("same")).await.unwrap_err();
    assert!(matches!(
        err,
        ModelError::Unavailable(symbiotic_core::DiagnosticCode::AttemptBudgetExhausted)
    ));
    assert_eq!(kept.calls.load(Ordering::SeqCst), 1, "no second paid call");

    let renewed = down();
    let provider = queued_with_cache(
        renewed.clone(),
        Arc::new(memory_queue()),
        ModelQueueConfig {
            budget_renewal_seconds: Some(0),
            ..once
        },
    );
    provider.chat(request("same")).await.unwrap_err();
    provider.chat(request("same")).await.unwrap_err();
    assert_eq!(
        renewed.calls.load(Ordering::SeqCst),
        2,
        "each call has its own budget"
    );
}

#[tokio::test(start_paused = true)]
async fn a_retry_waits_the_whole_delay_for_every_caller_of_the_request() {
    let queue: Arc<dyn QueueBackend> = Arc::new(memory_queue());
    let raw = Loopback::new(unique_identity()).failing_first(vec![ModelError::Provider(
        symbiotic_core::DiagnosticCode::ProviderFailure,
    )]);
    let provider = queued(
        raw.clone(),
        queue,
        ModelQueueConfig {
            retry_base_delay_ms: 1_500,
            retry_provider_errors: true,
            max_in_flight: 2,
            ..config()
        },
    )
    .with_binding_identity(symbiotic_core::BindingIdentity::new(
        "tenant", "provider", "1", "account",
    ))
    .with_invocation("retry-waiters".into());
    // Two callers of the same request: one runs the failing attempt, the
    // other waits on the same queue item.
    let (first, second) = tokio::join!(
        provider.chat(request("fractional")),
        provider.chat(request("fractional"))
    );
    first.unwrap();
    second.unwrap();
    let starts = raw.starts.lock().unwrap().clone();
    assert!(starts.len() >= 2, "{starts:?}");
    let waited = starts[1].duration_since(starts[0]);
    assert!(
        waited >= Duration::from_millis(1_490),
        "no attempt may start before the 1.5 s retry deadline: {waited:?}"
    );
    assert!(waited < Duration::from_millis(2_400), "{waited:?}");
}

/// Pauses the first enqueue after it is armed, after the backend answered
/// and before the caller sees the answer: a caller delayed while holding a
/// stale item.
struct PausesOneEnqueue {
    inner: MemoryQueue,
    armed: std::sync::atomic::AtomicBool,
    reached: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}

#[async_trait]
impl QueueBackend for PausesOneEnqueue {
    fn now(&self) -> chrono::DateTime<Utc> {
        self.inner.now()
    }
    async fn enqueue(&self, request: EnqueueRequest) -> Result<EnqueueOutcome, QueueError> {
        let outcome = self.inner.enqueue(request).await;
        if self.armed.swap(false, Ordering::SeqCst) {
            self.reached.notify_one();
            self.resume.notified().await;
        }
        outcome
    }
    async fn enqueue_replacing(
        &self,
        request: EnqueueRequest,
        current: &QueueItemId,
    ) -> Result<EnqueueOutcome, QueueError> {
        self.inner.enqueue_replacing(request, current).await
    }
    async fn claim(&self, request: ClaimRequest) -> Result<Vec<QueueItem>, QueueError> {
        self.inner.claim(request).await
    }
    async fn claim_item(
        &self,
        item_id: &QueueItemId,
        worker_id: &str,
        lease_seconds: u64,
        max_in_flight: Option<usize>,
    ) -> Result<Option<QueueItem>, QueueError> {
        self.inner
            .claim_item(item_id, worker_id, lease_seconds, max_in_flight)
            .await
    }
    async fn get_item(&self, item_id: &QueueItemId) -> Result<Option<QueueItem>, QueueError> {
        self.inner.get_item(item_id).await
    }
    async fn heartbeat(
        &self,
        item_id: &QueueItemId,
        worker_id: &str,
        lease_seconds: u64,
    ) -> Result<(), QueueError> {
        self.inner
            .heartbeat(item_id, worker_id, lease_seconds)
            .await
    }
    async fn complete(&self, item_id: &QueueItemId, worker_id: &str) -> Result<(), QueueError> {
        self.inner.complete(item_id, worker_id).await
    }
    async fn fail(
        &self,
        item_id: &QueueItemId,
        worker_id: &str,
        error: symbiotic_core::DiagnosticCode,
        retry_after_seconds: Option<u64>,
    ) -> Result<FailOutcome, QueueError> {
        self.inner
            .fail(item_id, worker_id, error, retry_after_seconds)
            .await
    }
    async fn fail_with(
        &self,
        item_id: &QueueItemId,
        worker_id: &str,
        failure: symbiotic_queue::Failure,
    ) -> Result<FailOutcome, QueueError> {
        self.inner.fail_with(item_id, worker_id, failure).await
    }
    async fn reclaim_expired_leases(&self, queue_id: &QueueId) -> Result<usize, QueueError> {
        self.inner.reclaim_expired_leases(queue_id).await
    }
}

#[tokio::test(start_paused = true)]
async fn a_delayed_caller_cannot_renew_over_a_budget_renewed_meanwhile() {
    let backend = Arc::new(PausesOneEnqueue {
        inner: memory_queue(),
        armed: std::sync::atomic::AtomicBool::new(false),
        reached: tokio::sync::Notify::new(),
        resume: tokio::sync::Notify::new(),
    });
    let raw = Loopback::new(unique_identity()).failing_first(
        (0..8)
            .map(|_| ModelError::Unavailable(symbiotic_core::DiagnosticCode::HttpUnavailable))
            .collect(),
    );
    let cache = tempfile::tempdir().unwrap();
    let provider = queued(
        raw.clone(),
        backend.clone(),
        ModelQueueConfig {
            logical_retry_attempts: 1,
            retry_attempts: 1,
            budget_renewal_seconds: Some(1),
            response_cache_dir: Some(cache.path().to_path_buf()),
            ..config()
        },
    );
    provider.chat(request("renewal race")).await.unwrap_err();
    assert_eq!(raw.calls.load(Ordering::SeqCst), 1);
    tokio::time::sleep(Duration::from_millis(1_100)).await;

    // The delayed caller sees the first, renewable, dead item and stalls.
    backend.armed.store(true, Ordering::SeqCst);
    let delayed = tokio::spawn({
        let provider = provider.clone();
        async move { provider.chat(request("renewal race")).await }
    });
    backend.reached.notified().await;
    // Meanwhile another caller renews the budget and exhausts it.
    provider.chat(request("renewal race")).await.unwrap_err();
    assert_eq!(raw.calls.load(Ordering::SeqCst), 2);

    backend.resume.notify_one();
    let err = delayed.await.unwrap().unwrap_err();
    assert!(matches!(
        err,
        ModelError::Unavailable(symbiotic_core::DiagnosticCode::AttemptBudgetExhausted)
    ));
    assert_eq!(
        raw.calls.load(Ordering::SeqCst),
        2,
        "the fresh budget is still inside its renewal interval"
    );
}

// ---------------------------------------------------------------------------
// Once an attempt holds its lease, the runtime owns it, not the caller
// ---------------------------------------------------------------------------

/// A 3 s lease, renewed every second, and a single attempt per request.
fn leased() -> ModelQueueConfig {
    ModelQueueConfig {
        lease_seconds: 3,
        logical_retry_attempts: 1,
        retry_attempts: 1,
        ..config()
    }
}

/// Forwards to a backend and counts lease renewals. Cooldown writes fail
/// while `fail_cooldown_writes` is set, and completions while
/// `fail_completions` is set.
struct CountsRenewals {
    inner: Arc<dyn QueueBackend>,
    renewals: AtomicUsize,
    fail_cooldown_writes: std::sync::atomic::AtomicBool,
    fail_completions: std::sync::atomic::AtomicBool,
    fail_transitions: std::sync::atomic::AtomicBool,
    fail_dead_reads: std::sync::atomic::AtomicBool,
    fail_replacements: std::sync::atomic::AtomicBool,
    fail_heartbeats: std::sync::atomic::AtomicBool,
    fail_heartbeat_at: AtomicUsize,
    heartbeat_entered: tokio::sync::Notify,
    completion_state: AtomicUsize,
    running_read: tokio::sync::Notify,
    pause_claim: std::sync::atomic::AtomicBool,
    claim_reached: tokio::sync::Notify,
    resume_claim: tokio::sync::Notify,
}

impl CountsRenewals {
    fn renewals(&self) -> usize {
        self.renewals.load(Ordering::SeqCst)
    }
}

fn counted(inner: Arc<dyn QueueBackend>) -> Arc<CountsRenewals> {
    Arc::new(CountsRenewals {
        inner,
        renewals: AtomicUsize::new(0),
        fail_cooldown_writes: std::sync::atomic::AtomicBool::new(false),
        fail_completions: std::sync::atomic::AtomicBool::new(false),
        fail_transitions: std::sync::atomic::AtomicBool::new(false),
        fail_dead_reads: std::sync::atomic::AtomicBool::new(false),
        fail_replacements: std::sync::atomic::AtomicBool::new(false),
        fail_heartbeats: std::sync::atomic::AtomicBool::new(false),
        fail_heartbeat_at: AtomicUsize::new(0),
        heartbeat_entered: tokio::sync::Notify::new(),
        completion_state: AtomicUsize::new(0),
        running_read: tokio::sync::Notify::new(),
        pause_claim: std::sync::atomic::AtomicBool::new(false),
        claim_reached: tokio::sync::Notify::new(),
        resume_claim: tokio::sync::Notify::new(),
    })
}

/// Wrapper behavior is backend-independent; backend contracts have their own conformance suite.
macro_rules! on_memory_backend {
    ($($scenario:ident),* $(,)?) => {
        mod memory {
            use super::*;
            $(
                #[tokio::test(start_paused = true)]
                async fn $scenario() {
                    super::$scenario("memory", counted(Arc::new(memory_queue()))).await;
                }
            )*
        }

    };
}

on_memory_backend!(
    close_storage_failures_preserve_provider_error,
    post_claim_recovery_errors_release_capacity_without_changing_accounting,
    an_abandoned_call_completes_and_answers_the_next_identical_request,
    an_abandoned_call_that_fails_records_its_class_and_releases_its_lease,
    lease_renewal_ends_with_its_call_when_the_caller_left,
    a_failed_cache_write_still_releases_the_lease,
    an_abandoned_call_keeps_its_model_slot_until_it_finishes,
    a_provider_panic_reaches_its_caller_and_ends_lease_renewal,
    a_slow_trace_write_keeps_the_lease_until_the_item_completes,
    a_slow_failure_receipt_keeps_the_lease_until_the_failure_is_recorded,
    a_slow_cache_write_keeps_the_lease_until_the_item_completes,
    a_waiters_slow_cache_read_does_not_stall_lease_renewal,
    a_failed_trace_write_still_completes_the_item,
    a_slow_reservation_renews_its_lease_before_dispatch,
    lease_loss_during_reservation_refuses_dispatch,
    a_reservation_storage_failure_preserves_the_last_provider_attempt,
    lease_loss_at_transport_boundary_preserves_unused_provider_attempts,
    a_crash_before_reservation_preserves_the_last_provider_attempt,
    a_heartbeat_failure_before_transport_preserves_the_last_provider_attempt,
    reconciliation_reopens_an_unknown_attempt_within_its_budget,
    reconciliation_preserves_exhausted_attempt_limits,
    reconciliation_permits_an_explicit_budget_renewal,
    a_reclaimed_unknown_attempt_does_not_spend_a_second_attempt_on_refusal,
    restored_account_allowance_reconsiders_refusals_without_spending_attempts,
    a_joined_waiter_recovers_durable_output_from_every_queue_state,
    uncertain_charge_cooldown_failure_is_visible_and_terminal,
    settlement_failure_is_visible_retains_unknown_and_refuses_redispatch,
    a_failed_cooldown_write_refuses_retry_and_records_the_failure,
    a_failed_cooldown_write_stops_logical_chain_continuation,
    a_failed_trace_write_reaches_caller_and_receipt_keeps_provider_error,
    a_failed_completion_still_returns_the_paid_answer,
    an_unusable_cache_directory_still_returns_the_paid_answer_and_its_usage,
    a_retryable_errors_backoff_spends_no_rate_budget,
    identical_concurrent_requests_spend_rate_budget_once_per_attempt,
    a_duplicate_waiting_for_rate_budget_takes_the_answer_at_once,
    a_running_receipt_counts_the_whole_rate_budget_wait,
    a_request_limit_admits_its_burst_then_paces,
    an_input_unit_limit_paces_large_requests,
);

#[async_trait]
impl QueueBackend for CountsRenewals {
    fn now(&self) -> chrono::DateTime<Utc> {
        self.inner.now()
    }
    async fn enqueue(&self, request: EnqueueRequest) -> Result<EnqueueOutcome, QueueError> {
        self.inner.enqueue(request).await
    }
    async fn enqueue_replacing(
        &self,
        request: EnqueueRequest,
        current: &QueueItemId,
    ) -> Result<EnqueueOutcome, QueueError> {
        if self.fail_replacements.load(Ordering::SeqCst) {
            return Err(QueueError::Storage(
                symbiotic_core::DiagnosticCode::StorageFailure,
            ));
        }
        self.inner.enqueue_replacing(request, current).await
    }
    async fn claim(&self, request: ClaimRequest) -> Result<Vec<QueueItem>, QueueError> {
        self.inner.claim(request).await
    }
    async fn claim_item(
        &self,
        item_id: &QueueItemId,
        worker_id: &str,
        lease_seconds: u64,
        max_in_flight: Option<usize>,
    ) -> Result<Option<QueueItem>, QueueError> {
        let item = self
            .inner
            .claim_item(item_id, worker_id, lease_seconds, max_in_flight)
            .await?;
        if item.is_some() && self.pause_claim.swap(false, Ordering::SeqCst) {
            self.claim_reached.notify_one();
            self.resume_claim.notified().await;
        }
        Ok(item)
    }
    async fn get_item(&self, item_id: &QueueItemId) -> Result<Option<QueueItem>, QueueError> {
        let item = self.inner.get_item(item_id).await?;
        if self.fail_dead_reads.load(Ordering::SeqCst)
            && item
                .as_ref()
                .is_some_and(|item| item.status == QueueStatus::Dead)
        {
            return Err(QueueError::Storage(
                symbiotic_core::DiagnosticCode::StorageFailure,
            ));
        }
        if item
            .as_ref()
            .is_some_and(|item| item.status == QueueStatus::Running)
        {
            self.running_read.notify_one();
        }
        Ok(item)
    }
    async fn heartbeat(
        &self,
        item_id: &QueueItemId,
        worker_id: &str,
        lease_seconds: u64,
    ) -> Result<(), QueueError> {
        let renewal = self.renewals.fetch_add(1, Ordering::SeqCst) + 1;
        self.heartbeat_entered.notify_waiters();
        if self.fail_heartbeats.load(Ordering::SeqCst)
            || self.fail_heartbeat_at.load(Ordering::SeqCst) == renewal
        {
            return Err(QueueError::LeaseMismatch(
                symbiotic_core::DiagnosticCode::QueueFailure,
            ));
        }
        self.inner
            .heartbeat(item_id, worker_id, lease_seconds)
            .await
    }
    async fn complete(&self, item_id: &QueueItemId, worker_id: &str) -> Result<(), QueueError> {
        if self.fail_completions.load(Ordering::SeqCst) {
            match self.completion_state.load(Ordering::SeqCst) {
                1 => {
                    self.inner
                        .fail(
                            item_id,
                            worker_id,
                            symbiotic_core::DiagnosticCode::StorageFailure,
                            None,
                        )
                        .await?;
                }
                2 => {
                    self.inner
                        .fail(
                            item_id,
                            worker_id,
                            symbiotic_core::DiagnosticCode::StorageFailure,
                            Some(0),
                        )
                        .await?;
                }
                _ => {}
            }
            return Err(QueueError::Storage(
                symbiotic_core::DiagnosticCode::StorageFailure,
            ));
        }
        self.inner.complete(item_id, worker_id).await
    }
    async fn fail(
        &self,
        item_id: &QueueItemId,
        worker_id: &str,
        error: symbiotic_core::DiagnosticCode,
        retry_after_seconds: Option<u64>,
    ) -> Result<FailOutcome, QueueError> {
        self.inner
            .fail(item_id, worker_id, error, retry_after_seconds)
            .await
    }
    async fn fail_with(
        &self,
        item_id: &QueueItemId,
        worker_id: &str,
        failure: Failure,
    ) -> Result<FailOutcome, QueueError> {
        if self.fail_transitions.load(Ordering::SeqCst) {
            return Err(QueueError::Storage(
                symbiotic_core::DiagnosticCode::StorageFailure,
            ));
        }
        self.inner.fail_with(item_id, worker_id, failure).await
    }
    async fn reclaim_expired_leases(&self, queue_id: &QueueId) -> Result<usize, QueueError> {
        self.inner.reclaim_expired_leases(queue_id).await
    }
    async fn cooldown_until(
        &self,
        queue_id: &QueueId,
    ) -> Result<Option<chrono::DateTime<Utc>>, QueueError> {
        self.inner.cooldown_until(queue_id).await
    }
    async fn note_cooldown(
        &self,
        queue_id: &QueueId,
        until: chrono::DateTime<Utc>,
    ) -> Result<(), QueueError> {
        if self.fail_cooldown_writes.load(Ordering::SeqCst) {
            return Err(QueueError::Unavailable(
                symbiotic_core::DiagnosticCode::HttpUnavailable,
            ));
        }
        self.inner.note_cooldown(queue_id, until).await
    }
}

/// Start `text` and stop waiting after `after`, as a job timeout would,
/// while the provider is still working.
async fn abandon(provider: &QueuedChatProvider<Loopback>, text: &str, after: Duration) {
    let waited = tokio::time::timeout(after, provider.chat(request(text))).await;
    assert!(
        waited.is_err(),
        "the caller must stop waiting while the provider works"
    );
}

/// The item of the first call `receipts` saw queued.
fn queued_item(receipts: &InMemoryReceiptSink) -> QueueItemId {
    receipts
        .receipts()
        .iter()
        .find(|receipt| receipt.status == ReceiptStatus::Queued)
        .and_then(|receipt| receipt.item_id.clone())
        .expect("the call was queued")
}

/// `item_id` once it has left `Running`.
async fn settled(queue: &CountsRenewals, item_id: &QueueItemId, within: Duration) -> QueueItem {
    let deadline = Instant::now() + within;
    loop {
        let item = queue.get_item(item_id).await.unwrap().expect("item exists");
        if item.status != QueueStatus::Running {
            return item;
        }
        assert!(
            Instant::now() < deadline,
            "the item is still Running after {within:?}: its lease was renewed {} times, \
             now until {:?}",
            queue.renewals(),
            item.lease_until
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// No lease is renewed from now on: waits over two renewal intervals.
async fn assert_no_more_renewals(queue: &CountsRenewals, backend: &str) {
    let before = queue.renewals();
    tokio::time::sleep(Duration::from_millis(2_500)).await;
    assert_eq!(
        queue.renewals(),
        before,
        "{backend}: a lease was renewed after its call ended"
    );
}

async fn an_abandoned_call_completes_and_answers_the_next_identical_request(
    backend: &str,
    queue: Arc<CountsRenewals>,
) {
    let cache = tempfile::tempdir().unwrap();
    let raw = Loopback::new(unique_identity()).slow(Duration::from_millis(1_500));
    let receipts = Arc::new(InMemoryReceiptSink::default());
    let provider = queued(
        raw.clone(),
        queue.clone(),
        ModelQueueConfig {
            response_cache_dir: Some(cache.path().to_path_buf()),
            ..leased()
        },
    )
    .with_receipt_sink(receipts.clone());

    abandon(&provider, "abandoned", Duration::from_millis(300)).await;
    let item = settled(&queue, &queued_item(&receipts), Duration::from_secs(5)).await;
    assert_eq!(item.status, QueueStatus::Succeeded, "{backend}");
    assert!(item.lease_owner.is_none(), "{backend}: {item:?}");

    let answer = tokio::time::timeout(Duration::from_secs(5), provider.chat(request("abandoned")))
        .await
        .unwrap_or_else(|_| panic!("{backend}: the identical request finishes"))
        .unwrap();
    assert_eq!(answer.text, "abandoned", "{backend}");
    assert_eq!(
        raw.calls.load(Ordering::SeqCst),
        1,
        "{backend}: answered from the cache without a second provider call"
    );
}

async fn an_abandoned_call_that_fails_records_its_class_and_releases_its_lease(
    backend: &str,
    queue: Arc<CountsRenewals>,
) {
    let raw = Loopback::new(unique_identity())
        .slow(Duration::from_millis(1_500))
        .failing_first(vec![ModelError::Unavailable(
            symbiotic_core::DiagnosticCode::HttpUnavailable,
        )]);
    let receipts = Arc::new(InMemoryReceiptSink::default());
    let provider =
        queued_with_cache(raw.clone(), queue.clone(), leased()).with_receipt_sink(receipts.clone());

    abandon(&provider, "doomed", Duration::from_millis(300)).await;
    let item = settled(&queue, &queued_item(&receipts), Duration::from_secs(5)).await;
    assert_eq!(
        item.status,
        QueueStatus::Dead,
        "{backend}: its only attempt failed"
    );
    assert_eq!(
        item.last_error_class,
        Some(symbiotic_core::FailureClass::Unavailable),
        "{backend}"
    );
    assert!(
        item.lease_owner.is_none() && item.lease_until.is_none(),
        "{backend}: {item:?}"
    );

    // The next identical request reports the recorded failure and does
    // not pay for another provider call.
    let err = tokio::time::timeout(Duration::from_secs(5), provider.chat(request("doomed")))
        .await
        .unwrap_or_else(|_| panic!("{backend}: the identical request finishes"))
        .unwrap_err();
    assert!(
        matches!(err, ModelError::Unavailable(_)),
        "{backend}: {err:?}"
    );
    assert!(matches!(
        err,
        ModelError::Unavailable(symbiotic_core::DiagnosticCode::AttemptBudgetExhausted)
    ));
    assert_eq!(raw.calls.load(Ordering::SeqCst), 1, "{backend}");
}

async fn lease_renewal_ends_with_its_call_when_the_caller_left(
    backend: &str,
    queue: Arc<CountsRenewals>,
) {
    let raw = Loopback::new(unique_identity()).slow(Duration::from_millis(2_200));
    let receipts = Arc::new(InMemoryReceiptSink::default());
    let provider = queued(raw.clone(), queue.clone(), leased()).with_receipt_sink(receipts.clone());

    abandon(&provider, "renewed", Duration::from_millis(300)).await;
    let item = settled(&queue, &queued_item(&receipts), Duration::from_secs(5)).await;
    assert_eq!(item.status, QueueStatus::Succeeded, "{backend}");
    assert!(
        queue.renewals() >= 1,
        "{backend}: the lease is renewed while the provider works"
    );
    assert_no_more_renewals(&queue, backend).await;
}

/// A response cache that cannot store.
struct FullCache;

impl ResponseCache for FullCache {
    fn load(&self, _entry: &CacheEntry<'_>) -> Result<Option<Value>, ModelError> {
        Ok(None)
    }

    fn store(&self, _entry: &CacheEntry<'_>, _response: &Value) -> Result<(), ModelError> {
        Err(ModelError::Cache(
            symbiotic_core::DiagnosticCode::CacheFailure,
        ))
    }
}

async fn a_failed_cache_write_still_releases_the_lease(backend: &str, queue: Arc<CountsRenewals>) {
    let raw = Loopback::new(unique_identity()).slow(Duration::from_millis(1_200));
    let receipts = Arc::new(InMemoryReceiptSink::default());
    let provider = queued(raw.clone(), queue.clone(), leased())
        .with_receipt_sink(receipts.clone())
        .with_response_cache(Arc::new(FullCache));

    let answer = tokio::time::timeout(
        Duration::from_secs(5),
        provider.chat(request("uncacheable")),
    )
    .await
    .unwrap_or_else(|_| panic!("{backend}: the call finishes"))
    .unwrap_or_else(|err| panic!("{backend}: a cache failure is not the call's failure: {err}"));
    assert_eq!(
        diagnostics(&answer.trace.metadata),
        ["response_cache_write_failed"],
        "{backend}"
    );
    let item = settled(&queue, &queued_item(&receipts), Duration::from_secs(1)).await;
    assert_eq!(
        item.status,
        QueueStatus::Succeeded,
        "{backend}: the provider answered"
    );
    assert!(item.lease_owner.is_none(), "{backend}: {item:?}");
    assert_no_more_renewals(&queue, backend).await;
}

async fn an_abandoned_call_keeps_its_model_slot_until_it_finishes(
    backend: &str,
    queue: Arc<CountsRenewals>,
) {
    let raw = Loopback::new(unique_identity()).slow(Duration::from_millis(1_000));
    let provider =
        queued(raw.clone(), queue.clone(), leased()).with_admission(ModelAdmission::new());

    abandon(&provider, "first", Duration::from_millis(300)).await;
    tokio::time::timeout(Duration::from_secs(5), provider.chat(request("second")))
        .await
        .unwrap_or_else(|_| panic!("{backend}: the next call gets the model's slot"))
        .unwrap();
    assert_eq!(raw.calls.load(Ordering::SeqCst), 2, "{backend}");
    assert_eq!(
        raw.peak.load(Ordering::SeqCst),
        1,
        "{backend}: the next call waited for the abandoned one"
    );
}

/// A chat provider with a bug: it panics mid-call.
#[derive(Clone)]
struct PanicsMidCall {
    descriptor: ProviderDescriptor,
    delay: Duration,
}

impl ModelProvider for PanicsMidCall {
    fn descriptor(&self) -> &ProviderDescriptor {
        &self.descriptor
    }
}

#[async_trait]
impl ChatProvider for PanicsMidCall {
    async fn chat(&self, _request: ChatRequest) -> Result<ChatResponse, ModelError> {
        tokio::time::sleep(self.delay).await;
        panic!("provider bug");
    }
}

async fn a_provider_panic_reaches_its_caller_and_ends_lease_renewal(
    backend: &str,
    queue: Arc<CountsRenewals>,
) {
    let provider = QueuedChatProvider::new(
        PanicsMidCall {
            descriptor: Loopback::new(unique_identity()).descriptor,
            delay: Duration::from_millis(1_600),
        },
        queue.clone(),
        "worker",
        leased(),
    )
    .with_spend_ledger(test_spend::ledger(), None);
    let call = tokio::spawn(async move { provider.chat(request("boom")).await });
    let err = call.await.expect_err("the call panics");
    assert!(err.is_panic(), "{backend}: {err:?}");
    assert!(
        queue.renewals() >= 1,
        "{backend}: the lease is renewed while the provider works"
    );
    assert_no_more_renewals(&queue, backend).await;
}

#[tokio::test]
async fn provider_panic_drains_a_contended_sqlite_heartbeat_before_propagating() {
    #[derive(Clone)]
    struct PanicsWhenSignalled {
        descriptor: ProviderDescriptor,
        entered: Arc<tokio::sync::Notify>,
        panic_now: Arc<tokio::sync::Notify>,
        panicked: Arc<tokio::sync::Notify>,
    }

    impl ModelProvider for PanicsWhenSignalled {
        fn descriptor(&self) -> &ProviderDescriptor {
            &self.descriptor
        }
    }

    #[async_trait]
    impl ChatProvider for PanicsWhenSignalled {
        async fn chat(&self, _request: ChatRequest) -> Result<ChatResponse, ModelError> {
            self.entered.notify_one();
            self.panic_now.notified().await;
            self.panicked.notify_one();
            panic!("provider bug during a pending heartbeat");
        }
    }

    tokio::time::timeout(Duration::from_secs(5), async {
        let dir = tempfile::tempdir_in(std::env::var_os("CARGO_MANIFEST_DIR").unwrap()).unwrap();
        let path = dir.path().join("queue.sqlite");
        let queue = counted(Arc::new(
            SqliteQueue::open(&path)
                .unwrap()
                .with_clock(test_clock::clock()),
        ));
        let entered = Arc::new(tokio::sync::Notify::new());
        let panic_now = Arc::new(tokio::sync::Notify::new());
        let panicked = Arc::new(tokio::sync::Notify::new());
        let provider = QueuedChatProvider::new(
            PanicsWhenSignalled {
                descriptor: Loopback::new(unique_identity()).descriptor,
                entered: entered.clone(),
                panic_now: panic_now.clone(),
                panicked: panicked.clone(),
            },
            queue.clone(),
            "worker",
            leased(),
        )
        .with_spend_ledger(test_spend::ledger(), None);
        let call = tokio::spawn(async move { provider.chat(request("contended panic")).await });
        entered.notified().await;
        let (locked_tx, locked_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let holder = std::thread::spawn(move || {
            let mut conn = rusqlite::Connection::open(path).unwrap();
            let tx = conn
                .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                .unwrap();
            locked_tx.send(()).unwrap();
            let released = release_rx.recv_timeout(Duration::from_secs(3)).is_ok();
            tx.rollback().unwrap();
            released
        });
        locked_rx.await.unwrap();
        // The real lock holder is ready before virtual time reaches the heartbeat.
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::time::resume();
        queue.heartbeat_entered.notified().await;
        panic_now.notify_one();
        panicked.notified().await;
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        let panic_escaped_while_locked = call.is_finished();
        release_tx.send(()).unwrap();
        let err = call
            .await
            .expect_err("provider panic must reach the caller");
        assert!(
            holder.join().unwrap(),
            "lock was released only by the guard"
        );
        assert!(err.is_panic());
        assert_eq!(
            *err.into_panic().downcast::<&str>().unwrap(),
            "provider bug during a pending heartbeat"
        );
        assert!(
            !panic_escaped_while_locked,
            "provider panic escaped before its pending heartbeat was drained"
        );
    })
    .await
    .expect("contended provider panic test exceeded its bound");
}

/// A synchronous collaborator held until the test has exercised lease renewal.
#[derive(Default)]
struct BlockingGate {
    entered: tokio::sync::Notify,
    released: Mutex<bool>,
    wake: std::sync::Condvar,
}
impl BlockingGate {
    fn wait(&self) {
        self.entered.notify_one();
        let (released, _) = self
            .wake
            .wait_timeout_while(
                self.released.lock().unwrap(),
                Duration::from_secs(5),
                |released| !*released,
            )
            .unwrap();
        assert!(*released, "blocking fixture was not released");
    }
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.wake.notify_all();
    }
}

async fn advance_while_blocked(duration: Duration) {
    // Advance through each heartbeat deadline while a blocking collaborator is parked.
    for _ in 0..duration.as_millis().div_ceil(100) {
        tokio::time::advance(Duration::from_millis(100)).await;
        tokio::task::yield_now().await;
    }
}

/// A trace sink whose first write takes `delay`.
struct SlowFirstTrace {
    delay: Duration,
    slowed: std::sync::atomic::AtomicBool,
}

impl SlowFirstTrace {
    fn new(delay: Duration) -> Arc<Self> {
        Arc::new(Self {
            delay,
            slowed: std::sync::atomic::AtomicBool::new(false),
        })
    }
}

#[async_trait]
impl symbiotic_trace::TraceSink for SlowFirstTrace {
    async fn record_model_invocation(
        &self,
        _trace: ModelInvocationTrace,
    ) -> Result<(), symbiotic_trace::TraceError> {
        if !self.slowed.swap(true, Ordering::SeqCst) {
            tokio::time::sleep(self.delay).await;
        }
        Ok(())
    }
}

/// A trace sink that cannot write.
struct BrokenTrace;

#[async_trait]
impl symbiotic_trace::TraceSink for BrokenTrace {
    async fn record_model_invocation(
        &self,
        _trace: ModelInvocationTrace,
    ) -> Result<(), symbiotic_trace::TraceError> {
        Err(symbiotic_trace::TraceError::Sink(
            symbiotic_core::DiagnosticCode::StorageFailure,
        ))
    }
}

/// Keeps receipts; recording the first `Failed` one takes `delay`.
struct SlowFirstFailureReceipt {
    receipts: InMemoryReceiptSink,
    delay: Duration,
    slowed: std::sync::atomic::AtomicBool,
}

#[async_trait]
impl symbiotic_model::QueueReceiptSink for SlowFirstFailureReceipt {
    async fn record_receipt(&self, receipt: symbiotic_model::QueueReceipt) {
        if receipt.status == ReceiptStatus::Failed && !self.slowed.swap(true, Ordering::SeqCst) {
            tokio::time::sleep(self.delay).await;
        }
        self.receipts.record_receipt(receipt).await;
    }
}

/// Past the 3 s lease of `leased()`, while the attempt is still recording:
/// how many expired leases a claim on `queue_id` would reclaim now.
async fn reclaimable_after_the_lease(queue: &CountsRenewals, queue_id: &QueueId) -> usize {
    tokio::time::sleep(Duration::from_millis(3_500)).await;
    queue.reclaim_expired_leases(queue_id).await.unwrap()
}

async fn a_slow_trace_write_keeps_the_lease_until_the_item_completes(
    backend: &str,
    queue: Arc<CountsRenewals>,
) {
    let cache = tempfile::tempdir().unwrap();
    let raw = Loopback::new(unique_identity()).slow(Duration::from_millis(100));
    let queue_id = raw.descriptor.queue_id();
    let receipts = Arc::new(InMemoryReceiptSink::default());
    // Two attempts per item: a reclaimed item would be claimed and paid for
    // again.
    let provider = queued(
        raw.clone(),
        queue.clone(),
        ModelQueueConfig {
            response_cache_dir: Some(cache.path().to_path_buf()),
            logical_retry_attempts: 2,
            retry_attempts: 2,
            ..leased()
        },
    )
    .with_receipt_sink(receipts.clone())
    .with_trace_sink(SlowFirstTrace::new(Duration::from_millis(4_000)));

    let call = tokio::spawn({
        let provider = provider.clone();
        async move { provider.chat(request("traced")).await }
    });
    assert_eq!(
        reclaimable_after_the_lease(&queue, &queue_id).await,
        0,
        "{backend}: the lease expired while the trace was written"
    );
    let answer = tokio::time::timeout(Duration::from_secs(10), call)
        .await
        .unwrap_or_else(|_| panic!("{backend}: the call finishes"))
        .unwrap()
        .unwrap();
    assert_eq!(answer.text, "traced", "{backend}");
    let item = queue
        .get_item(&queued_item(&receipts))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(item.status, QueueStatus::Succeeded, "{backend}: {item:?}");
    assert_eq!(item.attempt, 1, "{backend}: {item:?}");

    let again = tokio::time::timeout(Duration::from_secs(5), provider.chat(request("traced")))
        .await
        .unwrap_or_else(|_| panic!("{backend}: the identical request finishes"))
        .unwrap();
    assert_eq!(again.text, "traced", "{backend}");
    assert_eq!(
        raw.calls.load(Ordering::SeqCst),
        1,
        "{backend}: no second provider call"
    );
}

async fn a_slow_failure_receipt_keeps_the_lease_until_the_failure_is_recorded(
    backend: &str,
    queue: Arc<CountsRenewals>,
) {
    let raw = Loopback::new(unique_identity())
        .slow(Duration::from_millis(100))
        .failing_first(vec![ModelError::Unavailable(
            symbiotic_core::DiagnosticCode::HttpUnavailable,
        )]);
    let queue_id = raw.descriptor.queue_id();
    let receipts = Arc::new(SlowFirstFailureReceipt {
        receipts: InMemoryReceiptSink::default(),
        delay: Duration::from_millis(4_000),
        slowed: std::sync::atomic::AtomicBool::new(false),
    });
    let provider = queued(raw.clone(), queue.clone(), leased()).with_receipt_sink(receipts.clone());

    let call = tokio::spawn({
        let provider = provider.clone();
        async move { provider.chat(request("refused")).await }
    });
    assert_eq!(
        reclaimable_after_the_lease(&queue, &queue_id).await,
        0,
        "{backend}: the lease expired while the failure receipt was written"
    );
    let err = tokio::time::timeout(Duration::from_secs(10), call)
        .await
        .unwrap_or_else(|_| panic!("{backend}: the call finishes"))
        .unwrap()
        .unwrap_err();
    assert!(
        matches!(err, ModelError::Unavailable(_)),
        "{backend}: {err:?}"
    );
    let item = queue
        .get_item(&queued_item(&receipts.receipts))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(item.status, QueueStatus::Dead, "{backend}: {item:?}");
    assert_eq!(
        item.last_error_class,
        Some(symbiotic_core::FailureClass::Unavailable),
        "{backend}: {item:?}"
    );
    assert_eq!(
        item.last_error,
        Some(symbiotic_core::DiagnosticCode::HttpUnavailable),
        "{backend}: the provider's failure, not an expired lease"
    );
    assert_eq!(raw.calls.load(Ordering::SeqCst), 1, "{backend}");
}

async fn a_failed_trace_write_still_completes_the_item(backend: &str, queue: Arc<CountsRenewals>) {
    let cache = tempfile::tempdir().unwrap();
    let raw = Loopback::new(unique_identity()).slow(Duration::from_millis(1_200));
    let receipts = Arc::new(InMemoryReceiptSink::default());
    let provider = queued(
        raw.clone(),
        queue.clone(),
        ModelQueueConfig {
            response_cache_dir: Some(cache.path().to_path_buf()),
            ..leased()
        },
    )
    .with_receipt_sink(receipts.clone())
    .with_trace_sink(Arc::new(BrokenTrace));

    let answer = tokio::time::timeout(Duration::from_secs(5), provider.chat(request("untraced")))
        .await
        .unwrap_or_else(|_| panic!("{backend}: the call finishes"))
        .unwrap_or_else(|err| {
            panic!("{backend}: a trace failure is not the call's failure: {err}")
        });
    assert_eq!(
        diagnostics(&answer.trace.metadata),
        ["trace_write_failed"],
        "{backend}"
    );
    let item = settled(&queue, &queued_item(&receipts), Duration::from_secs(1)).await;
    assert_eq!(
        item.status,
        QueueStatus::Succeeded,
        "{backend}: the provider answered"
    );
    assert!(item.lease_owner.is_none(), "{backend}: {item:?}");
    assert_no_more_renewals(&queue, backend).await;

    // The answer was cached despite the trace failure, and a cache hit whose
    // trace fails is still an answer.
    let again = provider
        .chat(request("untraced"))
        .await
        .unwrap_or_else(|err| panic!("{backend}: {err}"));
    assert_eq!(again.text, "untraced", "{backend}");
    assert_eq!(
        diagnostics(&again.trace.metadata),
        ["trace_write_failed"],
        "{backend}"
    );
    assert_eq!(raw.calls.load(Ordering::SeqCst), 1, "{backend}");

    // Each receipt's metadata matches the response it stands for.
    let receipted = |status: ReceiptStatus| {
        receipts
            .receipts()
            .into_iter()
            .filter(|receipt| receipt.status == status)
            .map(|receipt| diagnostics(&receipt.metadata))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        receipted(ReceiptStatus::Succeeded),
        [["trace_write_failed"]],
        "{backend}"
    );
    assert_eq!(
        receipted(ReceiptStatus::CacheHit),
        [["trace_write_failed"]],
        "{backend}"
    );
}

async fn a_failed_cooldown_write_refuses_retry_and_records_the_failure(
    backend: &str,
    queue: Arc<CountsRenewals>,
) {
    failed_cooldown_is_terminal(backend, queue, 3).await;
}

async fn a_failed_cooldown_write_stops_logical_chain_continuation(
    backend: &str,
    queue: Arc<CountsRenewals>,
) {
    failed_cooldown_is_terminal(backend, queue, 1).await;
}

async fn failed_cooldown_is_terminal(
    backend: &str,
    queue: Arc<CountsRenewals>,
    retry_attempts: u32,
) {
    queue.fail_cooldown_writes.store(true, Ordering::SeqCst);
    let raw = Loopback::new(unique_identity())
        .slow(Duration::from_millis(100))
        .failing_first(vec![ModelError::Unavailable(
            symbiotic_core::DiagnosticCode::HttpUnavailable,
        )]);
    let receipts = Arc::new(InMemoryReceiptSink::default());
    let provider = queued_with_cache(
        raw.clone(),
        queue.clone(),
        ModelQueueConfig {
            retry_attempts,
            logical_retry_attempts: 3,
            budget_renewal_seconds: Some(0),
            ..config()
        },
    )
    .with_receipt_sink(receipts.clone());
    let (first, waiter) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(
            provider.chat(request("cooling")),
            provider.chat(request("cooling"))
        )
    })
    .await
    .unwrap_or_else(|_| panic!("{backend}: callers finish"));
    let errors = [first.unwrap_err(), waiter.unwrap_err()];
    let error = errors
        .iter()
        .find(|error| matches!(error, ModelError::Diagnostics { .. }))
        .expect("dispatching caller retains the provider failure");
    assert_eq!(
        error.code(),
        symbiotic_core::DiagnosticCode::HttpUnavailable,
        "{backend}: {error:?}"
    );
    assert_eq!(
        error.diagnostics(),
        [symbiotic_core::DiagnosticCode::QueueFailure]
    );
    assert_eq!(
        errors
            .iter()
            .filter(|error| matches!(error, ModelError::Queue(_)))
            .count(),
        1,
        "{backend}: the joined waiter sees the durable refusal: {errors:?}"
    );
    let item = queue
        .get_item(&queued_item(&receipts))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::to_value(item.status).unwrap(),
        json!("stopped"),
        "{backend}: {item:?}"
    );
    assert_eq!(
        item.last_error_class,
        Some(symbiotic_core::FailureClass::Queue),
        "{backend}: {item:?}"
    );
    assert!(
        item.lease_owner.is_none() && item.lease_until.is_none(),
        "{backend}: {item:?}"
    );
    assert!(
        queue
            .claim_item(&item.item_id, "later-worker", 60, None)
            .await
            .unwrap()
            .is_none()
    );
    // Recovering the limiter does not clear a durable refusal, even with immediate budget renewal.
    queue.fail_cooldown_writes.store(false, Ordering::SeqCst);
    assert!(matches!(
        provider.chat(request("cooling")).await,
        Err(ModelError::Queue(_))
    ));
    assert_eq!(raw.calls.load(Ordering::SeqCst), 1, "{backend}");
}

/// The directory cache, whose first store blocks its thread for `delay`, as
/// a slow disk would.
struct SlowFirstStore {
    gate: Arc<BlockingGate>,
    inner: symbiotic_model::DirResponseCache,
    slowed: std::sync::atomic::AtomicBool,
}

impl ResponseCache for SlowFirstStore {
    fn load(&self, entry: &CacheEntry<'_>) -> Result<Option<Value>, ModelError> {
        self.inner.load(entry)
    }

    fn store(&self, entry: &CacheEntry<'_>, response: &Value) -> Result<(), ModelError> {
        if !self.slowed.swap(true, Ordering::SeqCst) {
            self.gate.wait();
        }
        self.inner.store(entry, response)
    }
}

async fn a_slow_cache_write_keeps_the_lease_until_the_item_completes(
    backend: &str,
    queue: Arc<CountsRenewals>,
) {
    let dir = tempfile::tempdir().unwrap();
    let gate = Arc::new(BlockingGate::default());
    let raw = Loopback::new(unique_identity()).slow(Duration::from_millis(100));
    let queue_id = raw.descriptor.queue_id();
    let receipts = Arc::new(InMemoryReceiptSink::default());
    let provider = queued(
        raw.clone(),
        queue.clone(),
        ModelQueueConfig {
            logical_retry_attempts: 2,
            retry_attempts: 2,
            ..leased()
        },
    )
    .with_receipt_sink(receipts.clone())
    .with_response_cache(Arc::new(SlowFirstStore {
        gate: gate.clone(),
        inner: symbiotic_model::DirResponseCache::new(dir.path()),
        slowed: std::sync::atomic::AtomicBool::new(false),
    }));

    let call = tokio::spawn({
        let provider = provider.clone();
        async move { provider.chat(request("stored")).await }
    });
    gate.entered.notified().await;
    advance_while_blocked(Duration::from_millis(3_500)).await;
    let reclaimed = queue.reclaim_expired_leases(&queue_id).await.unwrap();
    gate.release();
    assert_eq!(
        reclaimed, 0,
        "{backend}: the lease expired while the response was stored"
    );
    let answer = tokio::time::timeout(Duration::from_secs(10), call)
        .await
        .unwrap_or_else(|_| panic!("{backend}: the call finishes"))
        .unwrap()
        .unwrap();
    assert_eq!(answer.text, "stored", "{backend}");
    let item = queue
        .get_item(&queued_item(&receipts))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(item.status, QueueStatus::Succeeded, "{backend}: {item:?}");
    assert_eq!(item.attempt, 1, "{backend}: {item:?}");

    let again = tokio::time::timeout(Duration::from_secs(5), provider.chat(request("stored")))
        .await
        .unwrap_or_else(|_| panic!("{backend}: the identical request finishes"))
        .unwrap();
    assert_eq!(again.text, "stored", "{backend}");
    assert_eq!(
        raw.calls.load(Ordering::SeqCst),
        1,
        "{backend}: no second provider call"
    );
}

/// The directory cache, whose next read after `slow_next_read` is set
/// blocks its thread for `delay`.
struct SlowNextRead {
    gate: Arc<BlockingGate>,
    inner: symbiotic_model::DirResponseCache,
    slow_next_read: std::sync::atomic::AtomicBool,
}

impl ResponseCache for SlowNextRead {
    fn load(&self, entry: &CacheEntry<'_>) -> Result<Option<Value>, ModelError> {
        if self.slow_next_read.swap(false, Ordering::SeqCst) {
            self.gate.wait();
        }
        self.inner.load(entry)
    }

    fn store(&self, entry: &CacheEntry<'_>, response: &Value) -> Result<(), ModelError> {
        self.inner.store(entry, response)
    }
}

async fn a_waiters_slow_cache_read_does_not_stall_lease_renewal(
    backend: &str,
    queue: Arc<CountsRenewals>,
) {
    let dir = tempfile::tempdir().unwrap();
    let gate = Arc::new(BlockingGate::default());
    let raw = Loopback::new(unique_identity()).slow(Duration::from_millis(5_000));
    let queue_id = raw.descriptor.queue_id();
    let cache = Arc::new(SlowNextRead {
        gate: gate.clone(),
        inner: symbiotic_model::DirResponseCache::new(dir.path()),
        slow_next_read: std::sync::atomic::AtomicBool::new(false),
    });
    let provider = queued(raw.clone(), queue.clone(), leased()).with_response_cache(cache.clone());

    let first = tokio::spawn({
        let provider = provider.clone();
        async move { provider.chat(request("read")).await }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    // An identical caller arrives while the first call holds the lease; its
    // first cache read takes 4 s.
    cache.slow_next_read.store(true, Ordering::SeqCst);
    let waiter = tokio::spawn({
        let provider = provider.clone();
        async move { provider.chat(request("read")).await }
    });
    gate.entered.notified().await;
    advance_while_blocked(Duration::from_millis(3_500)).await;
    let reclaimed = queue.reclaim_expired_leases(&queue_id).await.unwrap();
    gate.release();
    assert_eq!(
        reclaimed, 0,
        "{backend}: the lease expired while a waiter read the cache"
    );
    // A waiter that reclaimed the expired lease would make the first call's
    // completion fail.
    for call in [first, waiter] {
        let answer = tokio::time::timeout(Duration::from_secs(10), call)
            .await
            .unwrap_or_else(|_| panic!("{backend}: the call finishes"))
            .unwrap()
            .unwrap_or_else(|err| panic!("{backend}: {err}"));
        assert_eq!(answer.text, "read", "{backend}");
    }
    assert_eq!(
        raw.calls.load(Ordering::SeqCst),
        1,
        "{backend}: no second provider call"
    );
}

/// The kinds of the runtime diagnostics in a trace's metadata.
fn diagnostics(metadata: &Value) -> Vec<String> {
    metadata
        .get("runtime_diagnostics")
        .and_then(Value::as_array)
        .map(|list| {
            list.iter()
                .filter_map(|entry| entry.get("kind").and_then(Value::as_str))
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn succeeded_receipts(receipts: &InMemoryReceiptSink) -> Vec<symbiotic_model::QueueReceipt> {
    receipts
        .receipts()
        .into_iter()
        .filter(|receipt| receipt.status == ReceiptStatus::Succeeded)
        .collect()
}

async fn a_failed_trace_write_reaches_caller_and_receipt_keeps_provider_error(
    backend: &str,
    queue: Arc<CountsRenewals>,
) {
    let raw = Loopback::new(unique_identity()).failing_first(vec![ModelError::Provider(
        symbiotic_core::DiagnosticCode::ProviderFailure,
    )]);
    let receipts = Arc::new(InMemoryReceiptSink::default());
    let provider = queued(raw.clone(), queue, leased())
        .with_trace_sink(Arc::new(BrokenTrace))
        .with_receipt_sink(receipts.clone());

    let err = provider.chat(request("rejected")).await.unwrap_err();
    assert!(
        err.code() == symbiotic_core::DiagnosticCode::ProviderFailure
            && err.diagnostics() == [symbiotic_core::DiagnosticCode::StorageFailure],
        "{backend}: both failures must reach the caller: {err}"
    );
    assert!(
        receipts
            .receipts()
            .iter()
            .any(|receipt| receipt.status == ReceiptStatus::Failed
                && receipt.error == Some(symbiotic_core::DiagnosticCode::ProviderFailure))
    );
    assert_eq!(raw.calls.load(Ordering::SeqCst), 1);
}

async fn a_failed_completion_still_returns_the_paid_answer(
    backend: &str,
    queue: Arc<CountsRenewals>,
) {
    queue.fail_completions.store(true, Ordering::SeqCst);
    let raw = Loopback::new(unique_identity());
    let receipts = Arc::new(InMemoryReceiptSink::default());
    let provider = queued(raw.clone(), queue.clone(), leased()).with_receipt_sink(receipts.clone());

    let answer = provider
        .chat(request("paid"))
        .await
        .unwrap_or_else(|err| panic!("{backend}: {err}"));
    assert_eq!(answer.text, "paid", "{backend}");
    assert_eq!(
        diagnostics(&answer.trace.metadata),
        ["queue_complete_failed"],
        "{backend}"
    );
    assert_eq!(succeeded_receipts(&receipts).len(), 1, "{backend}");
}

async fn an_unusable_cache_directory_still_returns_the_paid_answer_and_its_usage(
    backend: &str,
    queue: Arc<CountsRenewals>,
) {
    // A cache directory below a regular file cannot be created.
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("a-file");
    std::fs::write(&file, b"").unwrap();
    let raw = Loopback::new(unique_identity()).slow(Duration::from_millis(100));
    let receipts = Arc::new(InMemoryReceiptSink::default());
    let provider = queued(
        raw.clone(),
        queue.clone(),
        ModelQueueConfig {
            response_cache_dir: Some(file.join("cache")),
            ..leased()
        },
    )
    .with_receipt_sink(receipts.clone());

    let answer = provider
        .chat(request("paid"))
        .await
        .unwrap_or_else(|err| panic!("{backend}: the paid answer is returned: {err}"));
    assert_eq!(answer.text, "paid", "{backend}");
    assert_eq!(
        diagnostics(&answer.trace.metadata),
        ["response_cache_write_failed"],
        "{backend}"
    );

    // Its usage is recorded once, and the receipt carries the diagnostic.
    let succeeded = succeeded_receipts(&receipts);
    assert_eq!(succeeded.len(), 1, "{backend}: {succeeded:?}");
    assert_eq!(
        succeeded[0]
            .usage
            .as_ref()
            .and_then(|usage| usage.input_tokens),
        Some(10),
        "{backend}"
    );
    assert_eq!(
        diagnostics(&succeeded[0].metadata),
        ["response_cache_write_failed"],
        "{backend}"
    );
    let item = queue
        .get_item(&queued_item(&receipts))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(item.status, QueueStatus::Succeeded, "{backend}: {item:?}");
    assert_eq!(raw.calls.load(Ordering::SeqCst), 1, "{backend}");
}

/// One request a minute with `burst` requests available at once: the
/// bucket barely refills during a test, so it counts what was charged.
fn slow_refill(burst: u64) -> ModelQueueConfig {
    ModelQueueConfig {
        requests_per_minute: Some(1),
        rate_burst_seconds: burst * 60,
        max_in_flight: 5,
        ..leased()
    }
}

async fn a_retryable_errors_backoff_spends_no_rate_budget(
    backend: &str,
    queue: Arc<CountsRenewals>,
) {
    let raw = Loopback::new(unique_identity())
        .slow(Duration::from_millis(20))
        .failing_first(vec![ModelError::Provider(
            symbiotic_core::DiagnosticCode::ProviderFailure,
        )]);
    // Three requests of budget: the failed attempt, its retry and one more.
    let provider = queued(
        raw.clone(),
        queue,
        ModelQueueConfig {
            retry_provider_errors: true,
            retry_base_delay_ms: 1_000,
            logical_retry_attempts: 3,
            retry_attempts: 3,
            ..slow_refill(3)
        },
    );

    // The retry waits a one-second backoff; polling through it is free.
    let answer = tokio::time::timeout(Duration::from_secs(5), provider.chat(request("retried")))
        .await
        .unwrap_or_else(|_| panic!("{backend}: the backoff spent the rate budget"))
        .unwrap();
    assert_eq!(answer.text, "retried", "{backend}");
    assert_eq!(raw.calls.load(Ordering::SeqCst), 2, "{backend}");

    tokio::time::timeout(Duration::from_secs(2), provider.chat(request("third")))
        .await
        .unwrap_or_else(|_| panic!("{backend}: the third request's budget was spent"))
        .unwrap();
    // Three provider attempts spent the three requests of budget.
    let fourth =
        tokio::time::timeout(Duration::from_secs(1), provider.chat(request("fourth"))).await;
    assert!(fourth.is_err(), "{backend}: the budget is spent");
    assert_eq!(raw.calls.load(Ordering::SeqCst), 3, "{backend}");
}

async fn identical_concurrent_requests_spend_rate_budget_once_per_attempt(
    backend: &str,
    queue: Arc<CountsRenewals>,
) {
    let cache = tempfile::tempdir().unwrap();
    let raw = Loopback::new(unique_identity()).slow(Duration::from_millis(300));
    let provider = queued(
        raw.clone(),
        queue,
        ModelQueueConfig {
            response_cache_dir: Some(cache.path().to_path_buf()),
            ..slow_refill(2)
        },
    );

    let identical: Vec<_> = (0..5)
        .map(|_| {
            let provider = provider.clone();
            tokio::spawn(async move { provider.chat(request("same")).await })
        })
        .collect();
    for call in identical {
        let answer = tokio::time::timeout(Duration::from_secs(5), call)
            .await
            .unwrap_or_else(|_| panic!("{backend}: waiting on a duplicate spent the budget"))
            .unwrap()
            .unwrap();
        assert_eq!(answer.text, "same", "{backend}");
    }
    assert_eq!(raw.calls.load(Ordering::SeqCst), 1, "{backend}");

    // One attempt spent one request of budget: one more request runs now,
    // the next waits for the refill.
    tokio::time::timeout(Duration::from_secs(2), provider.chat(request("other")))
        .await
        .unwrap_or_else(|_| panic!("{backend}: the second request's budget was spent"))
        .unwrap();
    let third = tokio::time::timeout(Duration::from_secs(1), provider.chat(request("third"))).await;
    assert!(third.is_err(), "{backend}: the budget is spent");
    assert_eq!(raw.calls.load(Ordering::SeqCst), 2, "{backend}");
}

/// Provider start times of `count` distinct requests sent at once, relative
/// to the first.
async fn start_offsets(
    raw: &Loopback,
    provider: &QueuedChatProvider<Loopback>,
    texts: Vec<String>,
) -> Vec<Duration> {
    let calls: Vec<_> = texts
        .into_iter()
        .map(|text| {
            let provider = provider.clone();
            tokio::spawn(async move { provider.chat(request(&text)).await })
        })
        .collect();
    for call in calls {
        tokio::time::timeout(Duration::from_secs(10), call)
            .await
            .expect("paced, not stuck")
            .unwrap()
            .unwrap();
    }
    let mut starts = raw.starts.lock().unwrap().clone();
    starts.sort();
    starts.iter().map(|start| *start - starts[0]).collect()
}

async fn a_request_limit_admits_its_burst_then_paces(backend: &str, queue: Arc<CountsRenewals>) {
    let raw = Loopback::new(unique_identity()).slow(Duration::from_millis(10));
    // 60 requests a minute, three seconds of it at once.
    let provider = queued(
        raw.clone(),
        queue,
        ModelQueueConfig {
            requests_per_minute: Some(60),
            rate_burst_seconds: 3,
            max_in_flight: 5,
            ..leased()
        },
    );
    let offsets = start_offsets(
        &raw,
        &provider,
        (0..5).map(|idx| format!("request {idx}")).collect(),
    )
    .await;
    assert!(
        offsets[2] < Duration::from_millis(300),
        "{backend}: {offsets:?}"
    );
    assert!(
        offsets[3] >= Duration::from_millis(900),
        "{backend}: {offsets:?}"
    );
    assert!(
        offsets[4] >= Duration::from_millis(1_900),
        "{backend}: {offsets:?}"
    );
    assert!(
        offsets[4] < Duration::from_millis(3_500),
        "{backend}: {offsets:?}"
    );
}

async fn an_input_unit_limit_paces_large_requests(backend: &str, queue: Arc<CountsRenewals>) {
    let raw = Loopback::new(unique_identity()).slow(Duration::from_millis(10));
    // Ten input units a second, two seconds of it at once: one 20-unit
    // request (80 characters) runs now, the next two seconds later.
    let provider = queued(
        raw.clone(),
        queue,
        ModelQueueConfig {
            input_units_per_minute: Some(600),
            rate_burst_seconds: 2,
            max_in_flight: 5,
            ..leased()
        },
    );
    let offsets = start_offsets(
        &raw,
        &provider,
        (0..2)
            .map(|idx| format!("{idx}{}", "x".repeat(79)))
            .collect(),
    )
    .await;
    assert!(
        offsets[1] >= Duration::from_millis(1_900),
        "{backend}: {offsets:?}"
    );
    assert!(
        offsets[1] < Duration::from_millis(3_500),
        "{backend}: {offsets:?}"
    );
}

async fn a_duplicate_waiting_for_rate_budget_takes_the_answer_at_once(
    backend: &str,
    queue: Arc<CountsRenewals>,
) {
    let cache = tempfile::tempdir().unwrap();
    let raw = Loopback::new(unique_identity()).slow(Duration::from_millis(300));
    // One request of budget, refilled once a minute: the first call spends
    // it, and the duplicates must not wait for the refill.
    let provider = queued(
        raw.clone(),
        queue,
        ModelQueueConfig {
            response_cache_dir: Some(cache.path().to_path_buf()),
            ..slow_refill(1)
        },
    );

    let started = Instant::now();
    let identical: Vec<_> = (0..5)
        .map(|_| {
            let provider = provider.clone();
            tokio::spawn(async move { provider.chat(request("same")).await })
        })
        .collect();
    for call in identical {
        let answer = tokio::time::timeout(Duration::from_secs(5), call)
            .await
            .unwrap_or_else(|_| panic!("{backend}: a duplicate waited for rate budget"))
            .unwrap()
            .unwrap();
        assert_eq!(answer.text, "same", "{backend}");
    }
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "{backend}: {:?}",
        started.elapsed()
    );
    assert_eq!(raw.calls.load(Ordering::SeqCst), 1, "{backend}");

    // The one request of budget went to the one provider call.
    let other = tokio::time::timeout(Duration::from_secs(1), provider.chat(request("other"))).await;
    assert!(other.is_err(), "{backend}: the budget is spent");
}

async fn a_running_receipt_counts_the_whole_rate_budget_wait(
    backend: &str,
    queue: Arc<CountsRenewals>,
) {
    let raw = Loopback::new(unique_identity());
    let receipts = Arc::new(InMemoryReceiptSink::default());
    // One request a second, none in reserve: the second call waits about a
    // second for budget, in several slices.
    let provider = queued(
        raw.clone(),
        queue,
        ModelQueueConfig {
            requests_per_minute: Some(60),
            ..leased()
        },
    )
    .with_receipt_sink(receipts.clone());
    provider.chat(request("first")).await.unwrap();
    provider.chat(request("second")).await.unwrap();

    let running: Vec<_> = receipts
        .receipts()
        .into_iter()
        .filter(|receipt| receipt.status == ReceiptStatus::Running)
        .collect();
    assert_eq!(running.len(), 2, "{backend}");
    let throttled = running[1].throttle_wait_ms.unwrap_or_default();
    assert!(
        throttled >= 800,
        "{backend}: the second attempt's receipt reports {throttled} ms of throttle"
    );
    let queued_ms = running[1].queue_wait_ms.unwrap_or(u64::MAX);
    assert!(
        queued_ms < 500,
        "{backend}: the wait is throttle, not queue wait: {queued_ms} ms"
    );
}

#[path = "support/spend.rs"]
mod test_spend;

/// Observes actual reservations and can emulate a slow synchronous accounting store.
struct ObservedSpend {
    inner: Arc<dyn symbiotic_model::SpendLedger>,
    reservations: Mutex<Vec<symbiotic_model::SpendReservation>>,
    gate: Option<Arc<BlockingGate>>,
    fail_reservations: std::sync::atomic::AtomicBool,
    panic_reservation: std::sync::atomic::AtomicBool,
    fail_settlement: std::sync::atomic::AtomicBool,
    fail_releases: std::sync::atomic::AtomicBool,
    fail_invocations: std::sync::atomic::AtomicBool,
    before_explicit_reserve: Option<Box<dyn Fn() + Send + Sync>>,
}

impl ObservedSpend {
    fn new(delay: Duration) -> Arc<Self> {
        Arc::new(Self {
            inner: test_spend::ledger(),
            reservations: Mutex::new(Vec::new()),
            gate: (!delay.is_zero()).then(|| Arc::new(BlockingGate::default())),
            fail_reservations: std::sync::atomic::AtomicBool::new(false),
            panic_reservation: std::sync::atomic::AtomicBool::new(false),
            fail_settlement: std::sync::atomic::AtomicBool::new(false),
            fail_releases: std::sync::atomic::AtomicBool::new(false),
            fail_invocations: std::sync::atomic::AtomicBool::new(false),
            before_explicit_reserve: None,
        })
    }

    fn last_reference(&self) -> symbiotic_model::SpendReceiptRef {
        self.reservations
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .reference
            .clone()
    }

    fn release_last(&self) {
        use symbiotic_model::SpendLedger;
        self.finish(
            &self.last_reference(),
            symbiotic_model::SpendState::Released,
            None,
            None,
            None,
        )
        .unwrap();
    }
}

impl symbiotic_model::SpendLedger for ObservedSpend {
    fn release_before_dispatch(
        &self,
        reference: &symbiotic_model::SpendReceiptRef,
    ) -> Result<(), ModelError> {
        self.inner.release_before_dispatch(reference)
    }
    fn reserve(&self, reservation: &symbiotic_model::SpendReservation) -> Result<bool, ModelError> {
        if let Some(gate) = &self.gate {
            gate.wait();
        }
        assert!(
            !self.panic_reservation.load(Ordering::SeqCst),
            "crashed before reservation"
        );
        if self.fail_reservations.load(Ordering::SeqCst) {
            return Err(ModelError::Queue(
                symbiotic_core::DiagnosticCode::SpendLedgerUnavailable,
            ));
        }
        let accepted = self.inner.reserve(reservation)?;
        if accepted {
            self.reservations.lock().unwrap().push(reservation.clone());
        }
        Ok(accepted)
    }

    fn reserve_explicit(
        &self,
        r: &symbiotic_model::SpendReservation,
        limit: u32,
    ) -> Result<bool, ModelError> {
        if let Some(hook) = &self.before_explicit_reserve {
            hook();
        }
        self.inner.reserve_explicit(r, limit)
    }
    fn discard_recovery(&self, a: &str, i: &str) -> Result<(), ModelError> {
        self.inner.discard_recovery(a, i)
    }
    fn purge_recovery(
        &self,
        matches: &dyn Fn(&Value) -> Result<bool, ModelError>,
    ) -> Result<usize, ModelError> {
        self.inner.purge_recovery(matches)
    }
    fn acquire_handoff(
        &self,
        handoff: &symbiotic_model::AcceptedSpendHandoff,
        account: &str,
        input_identity: &str,
        owner: &str,
    ) -> Result<(), ModelError> {
        self.inner
            .acquire_handoff(handoff, account, input_identity, owner)
    }

    fn receipt(
        &self,
        reference: &symbiotic_model::SpendReceiptRef,
    ) -> Result<Option<symbiotic_model::SpendReceipt>, ModelError> {
        self.inner.receipt(reference)
    }

    fn invocation(
        &self,
        account: &str,
        invocation: &str,
    ) -> Result<Option<symbiotic_model::SpendReceipt>, ModelError> {
        if self.fail_invocations.load(Ordering::SeqCst) {
            return Err(ModelError::Queue(
                symbiotic_core::DiagnosticCode::SpendLedgerUnavailable,
            ));
        }
        self.inner.invocation(account, invocation)
    }

    fn finish(
        &self,
        reference: &symbiotic_model::SpendReceiptRef,
        state: symbiotic_model::SpendState,
        usage: Option<UsageTrace>,
        output: Option<Value>,
        invocation: Option<&str>,
    ) -> Result<(), ModelError> {
        if (self.fail_settlement.load(Ordering::SeqCst) && output.is_some())
            || (self.fail_releases.load(Ordering::SeqCst)
                && state == symbiotic_model::SpendState::Released)
        {
            return Err(ModelError::Queue(
                symbiotic_core::DiagnosticCode::SpendLedgerUnavailable,
            ));
        }
        self.inner
            .finish(reference, state, usage, output, invocation)
    }
}

#[tokio::test]
async fn refused_explicit_replay_returns_parallel_completed_answer_and_receipt() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let queue = Arc::new(memory_queue());
        let receipts = Arc::new(InMemoryReceiptSink::default());
        let mut spend = ObservedSpend::new(Duration::ZERO);
        let ledger = spend.inner.clone();
        let reserve_reached = Arc::new(tokio::sync::Notify::new());
        let (resume, wait) = std::sync::mpsc::channel();
        let wait = Mutex::new(wait);
        let reached = reserve_reached.clone();
        Arc::get_mut(&mut spend).unwrap().before_explicit_reserve = Some(Box::new(move || {
            reached.notify_one();
            wait.lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(2))
                .expect("parallel replay must finish before reservation resumes");
        }));
        let credential_owner = |generation| {
            Arc::new(symbiotic_model::OpenAiCompatibleChatProvider::new(
                "loopback",
                "synthetic",
                "http://127.0.0.1:1",
                format!("synthetic-generation-{generation}"),
            ))
        };
        let mut first_raw = Loopback::new(unique_identity());
        first_raw.credential = Some(credential_owner(1));
        let mut second_raw = first_raw.clone();
        second_raw.credential = Some(credential_owner(2));
        let configure = |raw, ledger| {
            QueuedChatProvider::new(
                raw,
                queue.clone(),
                "worker",
                ModelQueueConfig {
                    max_in_flight: 2,
                    ..config()
                },
            )
            .with_spend_ledger(ledger, None)
            .with_binding_identity(symbiotic_core::BindingIdentity::new(
                "tenant", "provider", "1", "account",
            ))
            .with_invocation("reservation-race".into())
            .with_receipt_sink(receipts.clone())
        };
        let first_provider = configure(first_raw.clone(), ledger);
        let second_provider = configure(second_raw, spend);
        let second =
            tokio::spawn(async move { second_provider.chat(request("durable answer")).await });
        reserve_reached.notified().await;
        // B's post-claim lookup has returned no answer. A now completes
        // before B's atomic reservation checks the same invocation.
        let original = first_provider
            .chat(request("durable answer"))
            .await
            .unwrap();
        resume.send(()).unwrap();
        let recovered = second.await.unwrap().unwrap();
        assert_eq!(recovered.text, original.text);
        assert_eq!(first_raw.calls.load(Ordering::SeqCst), 1);

        let succeeded: Vec<_> = receipts
            .receipts()
            .into_iter()
            .filter(|r| r.status == ReceiptStatus::Succeeded)
            .collect();
        assert_eq!(succeeded.len(), 2);
        assert!(succeeded[0].spend_receipt.is_some());
        assert_eq!(succeeded[1].spend_receipt, succeeded[0].spend_receipt);
        assert_ne!(succeeded[0].item_id, succeeded[1].item_id);
        for receipt in succeeded {
            let item = queue
                .get_item(&receipt.item_id.unwrap())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(item.status, QueueStatus::Succeeded);
            assert!(item.lease_until.is_none());
        }
    })
    .await
    .expect("reservation-race regression must finish within three seconds");
}

async fn post_claim_recovery_errors_release_capacity_without_changing_accounting(
    backend: &str,
    queue: Arc<CountsRenewals>,
) {
    use symbiotic_core::DiagnosticCode;
    use symbiotic_model::SpendLedger;

    for error in [
        DiagnosticCode::InvocationCompleted,
        DiagnosticCode::SpendLedgerUnavailable,
    ] {
        let spend = ObservedSpend::new(Duration::ZERO);
        let receipts = Arc::new(InMemoryReceiptSink::default());
        let completion = Arc::new(tokio::sync::Notify::new());
        let mut first_raw = Loopback::new(unique_identity());
        let credential_owner = || {
            Arc::new(symbiotic_model::OpenAiCompatibleChatProvider::new(
                "loopback",
                "synthetic",
                "http://127.0.0.1:1",
                TraceId::new().0.to_string(),
            ))
        };
        first_raw.credential = Some(credential_owner());
        first_raw.completion_gate = Some(completion.clone());
        let mut second_raw = first_raw.clone();
        second_raw.credential = Some(credential_owner());
        let policy = ModelQueueConfig {
            max_in_flight: 2,
            ..config()
        };
        let configure = |raw| {
            queued(raw, queue.clone(), policy.clone())
                .with_spend_ledger(spend.clone(), None)
                .with_binding_identity(symbiotic_core::BindingIdentity::new(
                    "tenant", "provider", "1", "account",
                ))
                .with_invocation("overlapping-repeat".into())
                .with_receipt_sink(receipts.clone())
        };
        let first_provider = configure(first_raw.clone());
        let second_provider = configure(second_raw);
        let first =
            tokio::spawn(async move { first_provider.chat(request("durable answer")).await });
        tokio::time::timeout(Duration::from_secs(5), async {
            while first_raw.calls.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        // B passes all pre-claim recovery checks while A is still running.
        // Pause after B's actual backend claim, before its recovery check.
        queue.pause_claim.store(true, Ordering::SeqCst);
        let second =
            tokio::spawn(async move { second_provider.chat(request("durable answer")).await });
        tokio::time::timeout(Duration::from_secs(5), queue.claim_reached.notified())
            .await
            .unwrap();
        let queued_receipts: Vec<_> = receipts
            .receipts()
            .into_iter()
            .filter(|r| r.status == ReceiptStatus::Queued)
            .collect();
        assert_eq!(queued_receipts.len(), 2, "{backend}");
        let second_item = queued_receipts[1].item_id.as_ref().unwrap();
        assert_ne!(queued_receipts[0].item_id.as_ref().unwrap(), second_item);
        assert_eq!(
            queue.get_item(second_item).await.unwrap().unwrap().status,
            QueueStatus::Running,
            "{backend}"
        );

        completion.notify_one();
        let response = first.await.unwrap().unwrap();
        assert_eq!(response.text, "durable answer");
        let reference = receipts
            .receipts()
            .into_iter()
            .find(|r| r.status == ReceiptStatus::Succeeded)
            .unwrap()
            .spend_receipt
            .unwrap();
        let paid = spend.receipt(&reference).unwrap().unwrap();
        spend
            .discard_recovery(&paid.reservation.account, &paid.reservation.invocation)
            .unwrap();
        let accounting = serde_json::to_value(spend.receipt(&reference).unwrap().unwrap()).unwrap();
        assert_eq!(paid.state, symbiotic_model::SpendState::Settled);
        assert_eq!(paid.attempts_used, 1);
        assert!(paid.output.is_some());
        if error == DiagnosticCode::SpendLedgerUnavailable {
            spend.fail_invocations.store(true, Ordering::SeqCst);
        }
        queue.resume_claim.notify_one();
        let err = tokio::time::timeout(Duration::from_secs(5), second)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert_eq!(err.code(), error, "{backend}");
        let terminal = queue.get_item(second_item).await.unwrap().unwrap();
        assert_eq!(
            terminal.status,
            QueueStatus::Stopped,
            "{backend}: {error:?}"
        );
        assert_eq!(terminal.last_error, Some(error));
        assert!(terminal.lease_until.is_none());
        assert_eq!(first_raw.calls.load(Ordering::SeqCst), 1, "{backend}");
        assert_eq!(
            serde_json::to_value(spend.receipt(&reference).unwrap().unwrap()).unwrap(),
            accounting,
            "{backend}: B must not release or change A's accounting"
        );

        // A cap of one admits immediately only if neither A nor B is Running.
        let probe = queue
            .enqueue(EnqueueRequest {
                queue_id: first_raw.descriptor.queue_id(),
                kind: "chat".into(),
                payload: json!({}),
                idempotency_key: None,
                run_after: None,
                max_attempts: Some(1),
                force: false,
            })
            .await
            .unwrap();
        assert!(
            queue
                .claim_item(&probe.item.item_id, "capacity-probe", 60, Some(1))
                .await
                .unwrap()
                .is_some(),
            "{backend}: account capacity must be free before lease expiry"
        );
        queue
            .complete(&probe.item.item_id, "capacity-probe")
            .await
            .unwrap();
    }
}

async fn a_slow_reservation_renews_its_lease_before_dispatch(
    backend: &str,
    queue: Arc<CountsRenewals>,
) {
    let raw = Loopback::new(unique_identity());
    let spend = ObservedSpend::new(Duration::from_millis(1_400));
    let provider = queued(
        raw.clone(),
        queue.clone(),
        ModelQueueConfig {
            lease_seconds: 1,
            ..leased()
        },
    )
    .with_spend_ledger(spend.clone(), None);
    let call = tokio::spawn(async move { provider.chat(request("slow accounting")).await });
    let gate = spend.gate.as_ref().unwrap();
    gate.entered.notified().await;
    advance_while_blocked(Duration::from_millis(1_400)).await;
    gate.release();
    call.await.unwrap().unwrap();
    assert!(
        queue.renewals() > 0,
        "{backend}: reservation must renew even on a current-thread executor"
    );
    assert_eq!(raw.calls.load(Ordering::SeqCst), 1, "{backend}");
}

async fn lease_loss_during_reservation_refuses_dispatch(backend: &str, queue: Arc<CountsRenewals>) {
    queue.fail_heartbeats.store(true, Ordering::SeqCst);
    let raw = Loopback::new(unique_identity());
    let spend = ObservedSpend::new(Duration::from_millis(1_400));
    let provider = queued(
        raw.clone(),
        queue.clone(),
        ModelQueueConfig {
            lease_seconds: 1,
            ..leased()
        },
    )
    .with_spend_ledger(spend.clone(), None);
    let call_provider = provider.clone();
    let call =
        tokio::spawn(async move { call_provider.chat(request("lost reservation lease")).await });
    let gate = spend.gate.as_ref().unwrap();
    gate.entered.notified().await;
    advance_while_blocked(Duration::from_millis(1_400)).await;
    gate.release();
    let result = call.await.unwrap();
    assert!(
        matches!(result, Err(ModelError::Queue(_))),
        "{backend}: {result:?}"
    );
    assert_eq!(
        raw.calls.load(Ordering::SeqCst),
        0,
        "{backend}: lost ownership refuses pre-dispatch transport"
    );
    use symbiotic_model::SpendLedger;
    assert_eq!(
        spend
            .receipt(&spend.last_reference())
            .unwrap()
            .unwrap()
            .state,
        symbiotic_model::SpendState::Released,
        "{backend}: no transport incurred a charge"
    );
    queue.fail_heartbeats.store(false, Ordering::SeqCst);
    // Reservation finished beyond the real lease: reclaim must preserve the attempt.
    queue
        .reclaim_expired_leases(&raw.descriptor.queue_id())
        .await
        .unwrap();
    assert_eq!(
        provider
            .chat(request("lost reservation lease"))
            .await
            .unwrap()
            .text,
        "lost reservation lease",
        "{backend}"
    );
    assert_eq!(raw.calls.load(Ordering::SeqCst), 1, "{backend}");
}

async fn a_reservation_storage_failure_preserves_the_last_provider_attempt(
    backend: &str,
    queue: Arc<CountsRenewals>,
) {
    let raw = Loopback::new(unique_identity());
    let spend = ObservedSpend::new(Duration::ZERO);
    spend.fail_reservations.store(true, Ordering::SeqCst);
    let provider = queued(raw.clone(), queue, leased()).with_spend_ledger(spend.clone(), None);
    for _ in 0..2 {
        let err = provider
            .chat(request("storage recovers"))
            .await
            .unwrap_err();
        assert_eq!(
            err.code(),
            symbiotic_core::DiagnosticCode::SpendLedgerUnavailable,
            "{backend}"
        );
        assert_eq!(raw.calls.load(Ordering::SeqCst), 0, "{backend}");
        assert!(spend.reservations.lock().unwrap().is_empty());
    }
    spend.fail_reservations.store(false, Ordering::SeqCst);
    assert_eq!(
        provider
            .chat(request("storage recovers"))
            .await
            .unwrap()
            .text,
        "storage recovers",
        "{backend}"
    );
    assert_eq!(raw.calls.load(Ordering::SeqCst), 1, "{backend}");
}

async fn a_crash_before_reservation_preserves_the_last_provider_attempt(
    backend: &str,
    queue: Arc<CountsRenewals>,
) {
    let raw = Loopback::new(unique_identity());
    let spend = ObservedSpend::new(Duration::ZERO);
    spend.panic_reservation.store(true, Ordering::SeqCst);
    let provider = queued(
        raw.clone(),
        queue.clone(),
        ModelQueueConfig {
            lease_seconds: 1,
            ..leased()
        },
    )
    .with_spend_ledger(spend.clone(), None);
    let crashed = tokio::spawn({
        let provider = provider.clone();
        async move { provider.chat(request("crashed before accounting")).await }
    });
    assert!(crashed.await.unwrap_err().is_panic(), "{backend}");
    assert_eq!(raw.calls.load(Ordering::SeqCst), 0);
    assert!(spend.reservations.lock().unwrap().is_empty());
    tokio::time::sleep(Duration::from_millis(1100)).await;
    queue
        .reclaim_expired_leases(&raw.descriptor.queue_id())
        .await
        .unwrap();
    spend.panic_reservation.store(false, Ordering::SeqCst);
    assert_eq!(
        provider
            .chat(request("crashed before accounting"))
            .await
            .unwrap()
            .text,
        "crashed before accounting",
        "{backend}"
    );
    assert_eq!(raw.calls.load(Ordering::SeqCst), 1, "{backend}");
}

async fn a_heartbeat_failure_before_transport_preserves_the_last_provider_attempt(
    backend: &str,
    queue: Arc<CountsRenewals>,
) {
    // Both ownership checks: immediately after reserve and after Running telemetry.
    for heartbeat in [1, 2] {
        let queue = counted(queue.inner.clone());
        queue.fail_heartbeat_at.store(heartbeat, Ordering::SeqCst);
        let raw = Loopback::new(unique_identity());
        let spend = ObservedSpend::new(Duration::ZERO);
        let provider = queued(
            raw.clone(),
            queue,
            ModelQueueConfig {
                lease_seconds: 1,
                ..leased()
            },
        )
        .with_spend_ledger(spend.clone(), None);
        let input = format!("heartbeat {heartbeat}");
        provider.chat(request(&input)).await.unwrap_err();
        assert_eq!(raw.calls.load(Ordering::SeqCst), 0, "{backend}");
        // On the old implementation this expires into Dead at the final claim.
        tokio::time::sleep(Duration::from_millis(1100)).await;
        let answer = tokio::time::timeout(Duration::from_secs(3), provider.chat(request(&input)))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(answer.text, input, "{backend}");
        assert_eq!(raw.calls.load(Ordering::SeqCst), 1, "{backend}");
    }
}

async fn reconciliation_reopens_an_unknown_attempt_within_its_budget(
    backend: &str,
    queue: Arc<CountsRenewals>,
) {
    let mut raw = Loopback::new(unique_identity()).failing_first(vec![ModelError::Timeout(
        symbiotic_core::DiagnosticCode::HttpTimeout,
    )]);
    raw.uncertain_failures = true;
    let spend = ObservedSpend::new(Duration::ZERO);
    let provider = queued(
        raw.clone(),
        queue.clone(),
        ModelQueueConfig {
            logical_retry_attempts: 2,
            retry_attempts: 2,
            ..config()
        },
    )
    .with_spend_ledger(spend.clone(), None);
    provider.chat(request("reconciled")).await.unwrap_err();
    assert_eq!(
        raw.calls.load(Ordering::SeqCst),
        1,
        "{backend}: unknown attempt is not retried automatically"
    );
    provider.chat(request("reconciled")).await.unwrap_err();
    assert_eq!(
        raw.calls.load(Ordering::SeqCst),
        1,
        "{backend}: repeat requires evidence"
    );
    spend.release_last();
    assert_eq!(
        provider.chat(request("reconciled")).await.unwrap().text,
        "reconciled",
        "{backend}"
    );
    assert_eq!(
        raw.calls.load(Ordering::SeqCst),
        2,
        "{backend}: remaining provider attempt becomes available after reconciliation"
    );
}

async fn reconciliation_preserves_exhausted_attempt_limits(
    backend: &str,
    queue: Arc<CountsRenewals>,
) {
    let mut raw = Loopback::new(unique_identity()).failing_first(vec![ModelError::Timeout(
        symbiotic_core::DiagnosticCode::HttpTimeout,
    )]);
    raw.uncertain_failures = true;
    let spend = ObservedSpend::new(Duration::ZERO);
    let provider =
        queued_with_cache(raw.clone(), queue, leased()).with_spend_ledger(spend.clone(), None);
    provider.chat(request("last attempt")).await.unwrap_err();
    spend.release_last();
    provider.chat(request("last attempt")).await.unwrap_err();
    assert_eq!(
        raw.calls.load(Ordering::SeqCst),
        1,
        "{backend}: reconciliation is not a new attempt allowance"
    );
}

async fn restored_account_allowance_reconsiders_refusals_without_spending_attempts(
    backend: &str,
    queue: Arc<CountsRenewals>,
) {
    let mut raw = Loopback::new(unique_identity()).failing_first(vec![ModelError::Timeout(
        symbiotic_core::DiagnosticCode::HttpTimeout,
    )]);
    raw.uncertain_failures = true;
    let spend = ObservedSpend::new(Duration::ZERO);
    let receipts = Arc::new(InMemoryReceiptSink::default());
    let provider = queued(
        raw.clone(),
        queue,
        ModelQueueConfig {
            provider_request_limit: Some(1),
            ..leased()
        },
    )
    .with_spend_ledger(spend.clone(), None)
    .with_receipt_sink(receipts.clone());
    provider
        .chat(request("holds account allowance"))
        .await
        .unwrap_err();
    for _ in 0..3 {
        let err = provider
            .chat(request("waiting for allowance"))
            .await
            .unwrap_err();
        assert_eq!(
            err.code(),
            symbiotic_core::DiagnosticCode::SpendBudgetExhausted,
            "{backend}: denied reservation consumes no provider attempt"
        );
    }
    assert_eq!(raw.calls.load(Ordering::SeqCst), 1, "{backend}");
    assert!(
        receipts
            .receipts()
            .iter()
            .filter(|r| r.status == ReceiptStatus::Queued)
            .all(|r| r.spend_receipt.is_none()),
        "{backend}: denied claims must not invent receipt references"
    );
    spend.release_last();
    assert_eq!(
        provider
            .chat(request("waiting for allowance"))
            .await
            .unwrap()
            .text,
        "waiting for allowance",
        "{backend}"
    );
    assert_eq!(raw.calls.load(Ordering::SeqCst), 2, "{backend}");
}

async fn a_joined_waiter_recovers_durable_output_from_every_queue_state(
    backend: &str,
    queue: Arc<CountsRenewals>,
) {
    for bookkeeping_state in [0, 1, 2] {
        let queue = counted(queue.inner.clone());
        let receipts = Arc::new(InMemoryReceiptSink::default());
        queue.fail_completions.store(true, Ordering::SeqCst);
        queue
            .completion_state
            .store(bookkeeping_state, Ordering::SeqCst);
        let gate = Arc::new(tokio::sync::Notify::new());
        let mut raw = Loopback::new(unique_identity());
        raw.completion_gate = Some(gate.clone());
        // Without caching, the paid answer exists only in the canonical ledger.
        let provider = queued(
            raw.clone(),
            queue.clone(),
            ModelQueueConfig {
                max_in_flight: 2,
                retry_attempts: if bookkeeping_state == 1 { 1 } else { 2 },
                logical_retry_attempts: 2,
                ..config()
            },
        )
        .with_binding_identity(symbiotic_core::BindingIdentity::new(
            "tenant", "provider", "1", "account",
        ))
        .with_invocation("joined-waiter".into())
        .with_receipt_sink(receipts.clone());
        let first = tokio::spawn({
            let provider = provider.clone();
            async move { provider.chat(request("durable answer")).await }
        });
        while raw.calls.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let waiter = tokio::spawn({
            let provider = provider.clone();
            async move { provider.chat(request("durable answer")).await }
        });
        tokio::time::timeout(Duration::from_secs(2), queue.running_read.notified())
            .await
            .expect("waiter joined before durable completion");
        gate.notify_one();
        assert_eq!(
            first.await.unwrap().unwrap().text,
            "durable answer",
            "{backend}"
        );
        let item = queue
            .get_item(&queued_item(&receipts))
            .await
            .unwrap()
            .unwrap();
        let expected =
            [QueueStatus::Running, QueueStatus::Dead, QueueStatus::Failed][bookkeeping_state];
        assert_eq!(
            item.status, expected,
            "{backend}: interrupted queue bookkeeping"
        );
        let answer = tokio::time::timeout(Duration::from_secs(2), waiter).await.unwrap_or_else(|_| panic!("{backend}: waiter misses saved output under bookkeeping state {bookkeeping_state}")).unwrap().unwrap();
        assert_eq!(
            answer.text, "durable answer",
            "{backend}: state {bookkeeping_state}"
        );
        assert_eq!(
            raw.calls.load(Ordering::SeqCst),
            1,
            "{backend}: saved answer never redispatches"
        );
    }
}

async fn uncertain_charge_cooldown_failure_is_visible_and_terminal(
    backend: &str,
    queue: Arc<CountsRenewals>,
) {
    queue.fail_cooldown_writes.store(true, Ordering::SeqCst);
    let mut raw = Loopback::new(unique_identity()).failing_first(vec![ModelError::Unavailable(
        symbiotic_core::DiagnosticCode::HttpUnavailable,
    )]);
    raw.uncertain_failures = true;
    let spend = ObservedSpend::new(Duration::ZERO);
    let receipts = Arc::new(InMemoryReceiptSink::default());
    let provider = queued_with_cache(
        raw.clone(),
        queue.clone(),
        ModelQueueConfig {
            budget_renewal_seconds: Some(0),
            ..config()
        },
    )
    .with_spend_ledger(spend.clone(), None)
    .with_receipt_sink(receipts.clone());
    let err = provider
        .chat(request("uncertain cooldown"))
        .await
        .unwrap_err();
    assert_eq!(
        err.code(),
        symbiotic_core::DiagnosticCode::HttpUnavailable,
        "{backend}: {err:?}"
    );
    assert_eq!(
        err.diagnostics(),
        [symbiotic_core::DiagnosticCode::QueueFailure]
    );
    let failed: Vec<_> = receipts
        .receipts()
        .into_iter()
        .filter(|r| r.status == ReceiptStatus::Failed)
        .collect();
    assert_eq!(failed.len(), 1);
    assert_eq!(
        failed[0].error,
        Some(symbiotic_core::DiagnosticCode::HttpUnavailable)
    );
    let item = queue
        .get_item(&queued_item(&receipts))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(item.status, QueueStatus::Stopped, "{backend}");
    assert_eq!(
        item.last_error_class,
        Some(symbiotic_core::FailureClass::Queue),
        "{backend}"
    );
    use symbiotic_model::SpendLedger;
    assert_eq!(
        spend
            .receipt(&spend.last_reference())
            .unwrap()
            .unwrap()
            .state,
        symbiotic_model::SpendState::Unknown,
        "{backend}: storage failure cannot release uncertain spend"
    );
    queue.fail_cooldown_writes.store(false, Ordering::SeqCst);
    spend.release_last();
    assert!(
        matches!(
            provider.chat(request("uncertain cooldown")).await,
            Err(ModelError::Queue(_))
        ),
        "{backend}: cooldown persistence refusal remains terminal after reconciliation"
    );
    assert_eq!(raw.calls.load(Ordering::SeqCst), 1, "{backend}");
}

async fn reconciliation_permits_an_explicit_budget_renewal(
    backend: &str,
    queue: Arc<CountsRenewals>,
) {
    let mut raw = Loopback::new(unique_identity()).failing_first(vec![ModelError::Timeout(
        symbiotic_core::DiagnosticCode::HttpTimeout,
    )]);
    raw.uncertain_failures = true;
    let spend = ObservedSpend::new(Duration::ZERO);
    let provider = queued(
        raw.clone(),
        queue,
        ModelQueueConfig {
            budget_renewal_seconds: Some(0),
            ..leased()
        },
    )
    .with_spend_ledger(spend.clone(), None);
    provider
        .chat(request("renew reconciled attempt"))
        .await
        .unwrap_err();
    provider
        .chat(request("renew reconciled attempt"))
        .await
        .unwrap_err();
    assert_eq!(
        raw.calls.load(Ordering::SeqCst),
        1,
        "{backend}: renewal cannot bypass unknown charge"
    );
    spend.release_last();
    assert_eq!(
        provider
            .chat(request("renew reconciled attempt"))
            .await
            .unwrap()
            .text,
        "renew reconciled attempt",
        "{backend}"
    );
    assert_eq!(
        raw.calls.load(Ordering::SeqCst),
        2,
        "{backend}: reconciled attempt obeys explicit fresh budget policy"
    );
}

async fn a_reclaimed_unknown_attempt_does_not_spend_a_second_attempt_on_refusal(
    backend: &str,
    queue: Arc<CountsRenewals>,
) {
    let raw = Loopback::new(unique_identity());
    let spend = ObservedSpend::new(Duration::ZERO);
    let policy = ModelQueueConfig {
        lease_seconds: 1,
        logical_retry_attempts: 2,
        retry_attempts: 2,
        ..config()
    };
    let crashing = QueuedChatProvider::new(
        PanicsMidCall {
            descriptor: raw.descriptor.clone(),
            delay: Duration::ZERO,
        },
        queue.clone(),
        "crashed-worker",
        policy.clone(),
    )
    .with_spend_ledger(spend.clone(), None);
    let crashed =
        tokio::spawn(async move { crashing.chat(request("lost accepted attempt")).await });
    assert!(crashed.await.unwrap_err().is_panic(), "{backend}");
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    assert_eq!(
        queue
            .reclaim_expired_leases(&raw.descriptor.queue_id())
            .await
            .unwrap(),
        1,
        "{backend}: old attempt reclaimed"
    );
    let provider = queued(raw.clone(), queue, policy).with_spend_ledger(spend.clone(), None);
    let err = provider
        .chat(request("lost accepted attempt"))
        .await
        .unwrap_err();
    assert_eq!(
        err.code(),
        symbiotic_core::DiagnosticCode::SpendReconciliationRequired,
        "{backend}: reclaimed claim is refused without a new accepted attempt"
    );
    assert_eq!(
        raw.calls.load(Ordering::SeqCst),
        0,
        "{backend}: unknown attempt never redispatches"
    );
    assert_eq!(spend.reservations.lock().unwrap().len(), 1, "{backend}");
    spend.release_last();
    assert_eq!(
        provider
            .chat(request("lost accepted attempt"))
            .await
            .unwrap()
            .text,
        "lost accepted attempt",
        "{backend}: reservation refusal preserves the second provider attempt"
    );
    assert_eq!(raw.calls.load(Ordering::SeqCst), 1, "{backend}");
    assert_eq!(
        spend.reservations.lock().unwrap().len(),
        2,
        "{backend}: one crashed and one successful accepted attempt"
    );
}

async fn settlement_failure_is_visible_retains_unknown_and_refuses_redispatch(
    backend: &str,
    queue: Arc<CountsRenewals>,
) {
    use symbiotic_model::SpendLedger;
    let raw = Loopback::new(unique_identity());
    let spend = ObservedSpend::new(Duration::ZERO);
    spend.fail_settlement.store(true, Ordering::SeqCst);
    let receipts = Arc::new(InMemoryReceiptSink::default());
    let provider = queued(raw.clone(), queue.clone(), config())
        .with_spend_ledger(spend.clone(), None)
        .with_receipt_sink(receipts.clone());
    let error = provider
        .chat(request("settlement fails"))
        .await
        .unwrap_err();
    assert!(
        matches!(
            error,
            ModelError::Queue(symbiotic_core::DiagnosticCode::SpendLedgerUnavailable)
        ),
        "{backend}: {error:?}"
    );
    let receipt = spend.receipt(&spend.last_reference()).unwrap().unwrap();
    assert_eq!(
        receipt.state,
        symbiotic_model::SpendState::Unknown,
        "{backend}"
    );
    assert!(receipt.output.is_none(), "{backend}");
    spend.fail_settlement.store(false, Ordering::SeqCst);
    assert!(
        provider.chat(request("settlement fails")).await.is_err(),
        "{backend}"
    );
    assert_eq!(raw.calls.load(Ordering::SeqCst), 1, "{backend}");
    let item_id = receipts
        .receipts()
        .into_iter()
        .find_map(|r| r.item_id)
        .unwrap();
    assert_eq!(
        queue.get_item(&item_id).await.unwrap().unwrap().status,
        QueueStatus::Stopped,
        "{backend}"
    );
}

struct AbortAtRunning(Arc<CountsRenewals>);
#[async_trait]
impl symbiotic_model::QueueReceiptSink for AbortAtRunning {
    async fn record_receipt(&self, receipt: symbiotic_model::QueueReceipt) {
        if receipt.status == symbiotic_model::ReceiptStatus::Running
            && !self.0.fail_heartbeats.swap(true, Ordering::SeqCst)
        {
            tokio::time::sleep(Duration::from_millis(1_100)).await;
        }
    }
}
async fn lease_loss_at_transport_boundary_preserves_unused_provider_attempts(
    backend: &str,
    queue: Arc<CountsRenewals>,
) {
    for attempts in [1, 2] {
        queue.fail_heartbeats.store(false, Ordering::SeqCst);
        let raw = Loopback::new(unique_identity());
        let spend = ObservedSpend::new(Duration::ZERO);
        let policy = ModelQueueConfig {
            lease_seconds: 1,
            logical_retry_attempts: attempts,
            retry_attempts: attempts,
            ..leased()
        };
        let provider = queued(raw.clone(), queue.clone(), policy.clone())
            .with_spend_ledger(spend.clone(), None)
            .with_receipt_sink(Arc::new(AbortAtRunning(queue.clone())));
        provider
            .chat(request("aborted transport"))
            .await
            .unwrap_err();
        assert_eq!(raw.calls.load(Ordering::SeqCst), 0, "{backend}");
        use symbiotic_model::SpendLedger;
        assert!(
            spend
                .receipt(&spend.last_reference())
                .unwrap()
                .unwrap()
                .pre_dispatch_released
        );
        queue.fail_heartbeats.store(false, Ordering::SeqCst);
        // Retry without the aborting telemetry hook; runtime itself reclaims the lease.
        let provider = queued(raw.clone(), queue.clone(), policy).with_spend_ledger(spend, None);
        assert_eq!(
            provider
                .chat(request("aborted transport"))
                .await
                .unwrap()
                .text,
            "aborted transport",
            "{backend}: budget {attempts}"
        );
        assert_eq!(raw.calls.load(Ordering::SeqCst), 1, "{backend}");
    }
}

/// Advertise a custom backend identity at enqueue or only after claiming.
struct ReferenceIds {
    inner: MemoryQueue,
    reference_id: QueueItemId,
    original_id: Mutex<Option<QueueItemId>>,
    refusal: Mutex<Option<Failure>>,
    advertise_at_enqueue: bool,
    fail_settlement: bool,
    prior_attempts: u32,
}

impl ReferenceIds {
    fn new(id: String, advertise_at_enqueue: bool, fail_settlement: bool) -> Self {
        Self {
            inner: memory_queue(),
            reference_id: QueueItemId(id),
            original_id: Mutex::new(None),
            refusal: Mutex::new(None),
            advertise_at_enqueue,
            fail_settlement,
            prior_attempts: 0,
        }
    }

    fn stored_id(&self, item_id: &QueueItemId) -> QueueItemId {
        if *item_id == self.reference_id {
            self.original_id.lock().unwrap().clone().unwrap()
        } else {
            item_id.clone()
        }
    }
}

#[async_trait]
impl QueueBackend for ReferenceIds {
    async fn enqueue(&self, request: EnqueueRequest) -> Result<EnqueueOutcome, QueueError> {
        let mut outcome = self.inner.enqueue(request).await?;
        if self.prior_attempts > 0 {
            // Seed failed attempts through the real backend, with deadlines already
            // elapsed so only the target provider attempt needs to run.
            for _ in 0..self.prior_attempts {
                self.inner
                    .claim_item(&outcome.item.item_id, "fixture", 60, Some(1))
                    .await?
                    .expect("prior attempt is ready");
                self.inner
                    .fail_with(
                        &outcome.item.item_id,
                        "fixture",
                        Failure {
                            error: symbiotic_core::DiagnosticCode::HttpUnavailable,
                            error_class: Some(symbiotic_core::FailureClass::Provider),
                            run_after: Some(Utc::now() - chrono::Duration::seconds(1)),
                        },
                    )
                    .await?;
            }
            outcome.item = self.inner.get_item(&outcome.item.item_id).await?.unwrap();
        }
        *self.original_id.lock().unwrap() = Some(outcome.item.item_id.clone());
        if self.advertise_at_enqueue {
            outcome.item.item_id = self.reference_id.clone();
        }
        Ok(outcome)
    }
    async fn claim(&self, request: ClaimRequest) -> Result<Vec<QueueItem>, QueueError> {
        self.inner.claim(request).await
    }
    async fn claim_item(
        &self,
        item_id: &QueueItemId,
        worker_id: &str,
        lease_seconds: u64,
        max_in_flight: Option<usize>,
    ) -> Result<Option<QueueItem>, QueueError> {
        let mut item = self
            .inner
            .claim_item(
                &self.stored_id(item_id),
                worker_id,
                lease_seconds,
                max_in_flight,
            )
            .await?;
        if let Some(item) = &mut item {
            item.item_id = self.reference_id.clone();
        }
        Ok(item)
    }

    async fn get_item(&self, item_id: &QueueItemId) -> Result<Option<QueueItem>, QueueError> {
        let mut item = self.inner.get_item(&self.stored_id(item_id)).await?;
        if self.advertise_at_enqueue
            && let Some(item) = &mut item
        {
            item.item_id = self.reference_id.clone();
        }
        Ok(item)
    }
    async fn heartbeat(
        &self,
        item_id: &QueueItemId,
        worker_id: &str,
        lease_seconds: u64,
    ) -> Result<(), QueueError> {
        self.inner
            .heartbeat(&self.stored_id(item_id), worker_id, lease_seconds)
            .await
    }
    async fn complete(&self, item_id: &QueueItemId, worker_id: &str) -> Result<(), QueueError> {
        self.inner
            .complete(&self.stored_id(item_id), worker_id)
            .await
    }
    async fn fail(
        &self,
        item_id: &QueueItemId,
        worker_id: &str,
        error: symbiotic_core::DiagnosticCode,
        retry_after_seconds: Option<u64>,
    ) -> Result<FailOutcome, QueueError> {
        self.inner
            .fail(
                &self.stored_id(item_id),
                worker_id,
                error,
                retry_after_seconds,
            )
            .await
    }
    async fn fail_with(
        &self,
        item_id: &QueueItemId,
        worker_id: &str,
        failure: Failure,
    ) -> Result<FailOutcome, QueueError> {
        *self.refusal.lock().unwrap() = Some(failure.clone());
        if self.fail_settlement {
            return Err(QueueError::Storage(
                symbiotic_core::DiagnosticCode::StorageFailure,
            ));
        }
        self.inner
            .fail_with(&self.stored_id(item_id), worker_id, failure)
            .await
    }
    async fn reclaim_expired_leases(&self, queue_id: &QueueId) -> Result<usize, QueueError> {
        self.inner.reclaim_expired_leases(queue_id).await
    }
}

#[tokio::test]
async fn queued_retry_deadlines_preserve_pre_extraction_delays() {
    use symbiotic_core::DiagnosticCode;
    use symbiotic_model::{SpendLedger, SpendReceiptRef, SpendReservation, SpendState};

    // Apply the pre-extraction policy (90d3c27, lib.rs:6290-6337) to
    // item-a and the serialized request("retry compatibility"). Its request
    // hash differs from that private test's literal "same-request-shape".
    // Preserve attempts 1, 2 and 7 with a 1 000 ms base and 0..=20 s jitter.
    let mut scenarios = Vec::new();
    for (error, expected) in [
        (
            ModelError::RateLimited(DiagnosticCode::HttpRateLimited),
            &[(1, 6_000), (2, 20_000), (7, 46_000)][..],
        ),
        (
            ModelError::Unavailable(DiagnosticCode::HttpUnavailable),
            &[(1, 16_000), (2, 5_000), (7, 49_000)][..],
        ),
        (
            ModelError::Provider(DiagnosticCode::ProviderFailure),
            &[(1, 17_000), (2, 21_000), (7, 45_000)][..],
        ),
        (
            // Bare timeouts have uncertain spend and do not retry. This
            // synthetic HTTP failure carries the fixture's known-zero charge.
            ModelError::Http {
                primary: Box::new(ModelError::Timeout(DiagnosticCode::HttpTimeout)),
                status: 408,
                retry_after_seconds: None,
            },
            &[(1, 14_000)][..],
        ),
    ] {
        for &(attempt, expected_ms) in expected {
            for wrapped in [false, true] {
                let error = if wrapped {
                    ModelError::Diagnostics {
                        primary: Box::new(ModelError::Http {
                            primary: Box::new(error.clone()),
                            status: 503,
                            retry_after_seconds: None,
                        }),
                        secondary: vec![DiagnosticCode::StorageFailure],
                    }
                } else {
                    error.clone()
                };
                scenarios.push(async move {
                    let mut backend = ReferenceIds::new("item-a".into(), true, false);
                    backend.prior_attempts = attempt - 1;
                    let backend = Arc::new(backend);
                    let raw = Loopback::new(unique_identity()).failing_first(vec![error]);
                    let spend = ObservedSpend::new(Duration::ZERO);
                    // Match the queued runtime's implicit invocation identity.
                    let input = request("retry compatibility");
                    let request_hash = symbiotic_model::configuration_revision(&input).unwrap().0;
                    let mut descriptor = raw.descriptor().clone();
                    descriptor.metadata =
                        json!({"configuration": descriptor.metadata, "binding": null});
                    let binding = symbiotic_model::configuration_revision(&(
                        "chat",
                        &descriptor,
                        Option::<symbiotic_core::BindingIdentity>::None,
                        &request_hash,
                    ))
                    .unwrap()
                    .0;
                    for prior in 1..attempt {
                        let reference =
                            SpendReceiptRef::new(format!("runtime:item-a:{prior}")).unwrap();
                        assert!(
                            spend
                                .reserve(&SpendReservation {
                                    reference: reference.clone(),
                                    account: descriptor.queue_id().0,
                                    invocation: binding.clone(),
                                    binding: binding.clone(),
                                    request_limit: None,
                                })
                                .unwrap()
                        );
                        // A known-zero provider failure still consumes an attempt.
                        spend
                            .finish(&reference, SpendState::Released, None, None, None)
                            .unwrap();
                    }
                    let provider = QueuedChatProvider::new(
                        raw.clone(),
                        backend.clone(),
                        "worker",
                        ModelQueueConfig {
                            logical_retry_attempts: attempt,
                            retry_attempts: attempt,
                            retry_base_delay_ms: 1_000,
                            retry_jitter_seconds: 20,
                            retry_provider_errors: true,
                            ..config()
                        },
                    )
                    .with_spend_ledger(spend, None);
                    let before = Utc::now();
                    let started = Instant::now();
                    provider
                        .chat(request("retry compatibility"))
                        .await
                        .unwrap_err();
                    let after = Utc::now();
                    // Bound the target failure from its provider start.
                    let last_start = *raw.starts.lock().unwrap().last().unwrap();
                    let before = before
                        + chrono::Duration::from_std(last_start.duration_since(started)).unwrap();
                    let item = backend
                        .get_item(&QueueItemId("item-a".into()))
                        .await
                        .unwrap()
                        .unwrap();
                    let delay = chrono::Duration::milliseconds(expected_ms);
                    assert_eq!(item.attempt, attempt, "wrapped={wrapped}: {item:?}");
                    assert_eq!(
                        item.status,
                        QueueStatus::Dead,
                        "attempt={attempt}, expected {expected_ms} ms, wrapped={wrapped}: {item:?}"
                    );
                    assert!(
                        (before + delay..=after + delay).contains(&item.run_after),
                        "attempt={attempt}, expected {expected_ms} ms, wrapped={wrapped}: {before} <= start <= {after}, deadline={}",
                        item.run_after,
                    );
                });
            }
        }
    }
    futures::future::join_all(scenarios).await;
}

#[tokio::test]
async fn receipt_ref_item_ids_are_bounded_at_enqueue_for_every_attempt() {
    // Reserve the runtime prefix, separator and all ten u32 attempt digits.
    let max_item_bytes = 256 - "runtime:".len() - 1 - u32::MAX.to_string().len();
    for id in [
        "a".repeat(max_item_bytes + 1),
        "é".repeat(max_item_bytes / 2 + 1),
    ] {
        let backend = Arc::new(ReferenceIds::new(id, true, false));
        let raw = Loopback::new(unique_identity());
        let error = queued(raw.clone(), backend.clone(), config())
            .chat(request("overlong enqueue identity"))
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                ModelError::InvalidRequest(symbiotic_core::DiagnosticCode::SpendReceiptRefTooLong)
            ),
            "{error:?}"
        );
        let original = backend.original_id.lock().unwrap().clone().unwrap();
        let item = backend.inner.get_item(&original).await.unwrap().unwrap();
        assert_eq!(item.status, QueueStatus::Pending);
        assert_eq!(item.attempt, 0, "refuse before claiming");
        assert_eq!(raw.calls.load(Ordering::SeqCst), 0);
    }
    let backend = Arc::new(ReferenceIds::new("a".repeat(max_item_bytes), true, false));
    let raw = Loopback::new(unique_identity());
    queued(raw.clone(), backend, config())
        .chat(request("largest permitted identity"))
        .await
        .unwrap();
    assert_eq!(raw.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn receipt_ref_failure_after_claim_releases_lease_and_records_refusal() {
    let backend = Arc::new(ReferenceIds::new("a".repeat(256), false, false));
    let raw = Loopback::new(unique_identity());
    let spend = ObservedSpend::new(Duration::ZERO);
    let error = queued(raw.clone(), backend.clone(), config())
        .with_spend_ledger(spend.clone(), None)
        .chat(request("overlong claimed identity"))
        .await
        .unwrap_err();
    assert!(
        matches!(
            error,
            ModelError::InvalidRequest(symbiotic_core::DiagnosticCode::SpendReceiptRefTooLong)
        ),
        "{error:?}"
    );
    let original = backend.original_id.lock().unwrap().clone().unwrap();
    let item = backend.inner.get_item(&original).await.unwrap().unwrap();
    assert_eq!(item.status, QueueStatus::Stopped);
    assert_eq!(
        item.last_error,
        Some(symbiotic_core::DiagnosticCode::SpendReceiptRefTooLong)
    );
    assert_eq!(
        item.last_error_class,
        Some(symbiotic_core::FailureClass::InvalidRequest)
    );
    assert!(item.lease_owner.is_none());
    assert!(item.lease_until.is_none());
    assert_eq!(raw.calls.load(Ordering::SeqCst), 0);
    assert!(spend.reservations.lock().unwrap().is_empty());
}

#[tokio::test]
async fn receipt_ref_failure_after_claim_propagates_settlement_error() {
    let backend = Arc::new(ReferenceIds::new("a".repeat(256), false, true));
    let raw = Loopback::new(unique_identity());
    let error = queued(raw.clone(), backend.clone(), config())
        .chat(request("refused settlement"))
        .await
        .unwrap_err();
    assert!(
        matches!(
            error,
            ModelError::Queue(symbiotic_core::DiagnosticCode::QueueFailure)
        ),
        "{error:?}"
    );
    assert_eq!(
        backend.refusal.lock().unwrap().as_ref().unwrap().error,
        symbiotic_core::DiagnosticCode::SpendReceiptRefTooLong
    );
    assert_eq!(raw.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn trial_provider_and_trace_failures_visible_without_receipts() {
    let raw = Loopback::new(unique_identity()).failing_first(vec![ModelError::Provider(
        symbiotic_core::DiagnosticCode::ProviderFailure,
    )]);
    let provider =
        queued(raw, Arc::new(memory_queue()), config()).with_trace_sink(Arc::new(BrokenTrace));
    let error = provider.chat(request("both failures")).await.unwrap_err();
    assert!(matches!(&error, ModelError::Diagnostics { primary, .. }
        if matches!(primary.as_ref(), ModelError::Provider(symbiotic_core::DiagnosticCode::ProviderFailure))));
    assert_eq!(
        error.diagnostics(),
        [symbiotic_core::DiagnosticCode::StorageFailure]
    );
}

struct TrialJob {
    calls: Arc<AtomicUsize>,
    payload: Vec<u8>,
    renewals: AtomicUsize,
    recovered: Arc<tokio::sync::Notify>,
    finished: std::sync::atomic::AtomicBool,
}

struct AdmissionJob {
    payload: Vec<u8>,
    admitting: std::sync::atomic::AtomicBool,
    renewed: (Mutex<bool>, std::sync::Condvar),
    finished: std::sync::atomic::AtomicBool,
}
impl symbiotic_model::ModelJob for AdmissionJob {
    fn recover(&self, _: &symbiotic_model::SpendReservation) -> Result<bool, ModelError> {
        Ok(false)
    }
    fn claim(
        &self,
        _: &symbiotic_model::SpendReservation,
        _: u32,
    ) -> Result<Option<Vec<u8>>, ModelError> {
        Ok(Some(self.payload.clone()))
    }
    fn admit_request(&self, _: Option<String>) -> Result<(), ModelError> {
        self.admitting.store(true, Ordering::SeqCst);
        let (renewed, _) = self
            .renewed
            .1
            .wait_timeout_while(
                self.renewed.0.lock().unwrap(),
                Duration::from_secs(1),
                |renewed| !*renewed,
            )
            .unwrap();
        if *renewed {
            Ok(())
        } else {
            Err(ModelError::Queue(
                symbiotic_core::DiagnosticCode::QueueFailure,
            ))
        }
    }
    fn heartbeat(&self) -> Result<bool, ModelError> {
        if self.admitting.load(Ordering::SeqCst) {
            *self.renewed.0.lock().unwrap() = true;
            self.renewed.1.notify_one();
        }
        Ok(false)
    }
    fn finish(
        &self,
        state: symbiotic_model::SpendState,
        _: Option<UsageTrace>,
        _: Option<Value>,
        failure: Option<symbiotic_core::DiagnosticCode>,
        _: bool,
    ) -> Result<(), ModelError> {
        assert_eq!(state, symbiotic_model::SpendState::Settled);
        assert!(failure.is_none());
        self.finished.store(true, Ordering::SeqCst);
        Ok(())
    }
    fn release(&self) -> Result<(), ModelError> {
        panic!("live admission must proceed")
    }
    fn refuse(&self, _: symbiotic_core::DiagnosticCode) -> Result<(), ModelError> {
        panic!("valid request")
    }
    fn eligible(&self) -> Result<bool, ModelError> {
        Ok(true)
    }
    fn can_retry(&self, _: u32) -> Result<bool, ModelError> {
        Ok(false)
    }
    fn attempt(&self) -> Result<u32, ModelError> {
        Ok(1)
    }
    fn heartbeat_interval(&self) -> Duration {
        Duration::from_millis(10)
    }
}

#[tokio::test]
async fn request_admission_renews_job_lease_before_transport() {
    let raw = Loopback::new(unique_identity());
    let input = request("renew during admission");
    let job = Arc::new(AdmissionJob {
        payload: serde_json::to_vec(&input).unwrap(),
        admitting: std::sync::atomic::AtomicBool::new(false),
        renewed: (Mutex::new(false), std::sync::Condvar::new()),
        finished: std::sync::atomic::AtomicBool::new(false),
    });
    let provider = queued(raw.clone(), Arc::new(memory_queue()), config())
        .with_admission(ModelAdmission::new())
        .with_binding_identity(symbiotic_core::BindingIdentity::new(
            "tenant", "provider", "1", "account",
        ))
        .with_invocation("admission-heartbeat".into())
        .with_job_owner(job.clone());
    tokio::time::timeout(Duration::from_secs(2), provider.chat(input))
        .await
        .expect("bounded admission")
        .expect("renewed admission sends");
    assert!(*job.renewed.0.lock().unwrap());
    assert!(job.finished.load(Ordering::SeqCst));
    assert_eq!(raw.calls.load(Ordering::SeqCst), 1);
}

impl symbiotic_model::ModelJob for TrialJob {
    fn recover(&self, _: &symbiotic_model::SpendReservation) -> Result<bool, ModelError> {
        Ok(false)
    }
    fn claim(
        &self,
        _: &symbiotic_model::SpendReservation,
        _: u32,
    ) -> Result<Option<Vec<u8>>, ModelError> {
        Ok(Some(self.payload.clone()))
    }
    fn heartbeat(&self) -> Result<bool, ModelError> {
        if self.calls.load(Ordering::SeqCst) == 0 {
            return Ok(false);
        }
        match self.renewals.fetch_add(1, Ordering::SeqCst) {
            0 => Err(ModelError::Queue(
                symbiotic_core::DiagnosticCode::SpendLedgerUnavailable,
            )),
            1 => {
                self.recovered.notify_one();
                Ok(false)
            }
            _ => Ok(false),
        }
    }
    fn finish(
        &self,
        state: symbiotic_model::SpendState,
        _: Option<UsageTrace>,
        _: Option<Value>,
        failure: Option<symbiotic_core::DiagnosticCode>,
        _: bool,
    ) -> Result<(), ModelError> {
        assert_eq!(state, symbiotic_model::SpendState::Unknown);
        assert_eq!(
            failure,
            Some(symbiotic_core::DiagnosticCode::ProviderFailure)
        );
        self.finished.store(true, Ordering::SeqCst);
        Ok(())
    }
    fn release(&self) -> Result<(), ModelError> {
        panic!("dispatched attempt must settle")
    }
    fn refuse(&self, _: symbiotic_core::DiagnosticCode) -> Result<(), ModelError> {
        panic!("valid request")
    }
    fn eligible(&self) -> Result<bool, ModelError> {
        Ok(true)
    }
    fn can_retry(&self, _: u32) -> Result<bool, ModelError> {
        Ok(false)
    }
    fn attempt(&self) -> Result<u32, ModelError> {
        Ok(1)
    }
    fn heartbeat_interval(&self) -> Duration {
        Duration::from_millis(1)
    }
}

#[tokio::test]
async fn trial_heartbeat_provider_and_trace_failures_all_visible() {
    let recovered = Arc::new(tokio::sync::Notify::new());
    let mut raw = Loopback::new(unique_identity()).failing_first(vec![ModelError::Provider(
        symbiotic_core::DiagnosticCode::ProviderFailure,
    )]);
    raw.uncertain_failures = true;
    raw.completion_gate = Some(recovered.clone());
    let input = request("three failures");
    let job = Arc::new(TrialJob {
        calls: raw.calls.clone(),
        payload: serde_json::to_vec(&input).unwrap(),
        renewals: AtomicUsize::new(0),
        recovered,
        finished: std::sync::atomic::AtomicBool::new(false),
    });
    let provider = queued(raw, Arc::new(memory_queue()), config())
        .with_admission(ModelAdmission::new())
        .with_binding_identity(symbiotic_core::BindingIdentity::new(
            "tenant", "provider", "1", "account",
        ))
        .with_invocation("trial-heartbeat".into())
        .with_job_owner(job.clone())
        .with_trace_sink(Arc::new(BrokenTrace));
    let error = tokio::time::timeout(Duration::from_secs(5), provider.chat(input))
        .await
        .unwrap()
        .unwrap_err();
    assert!(
        job.finished.load(Ordering::SeqCst),
        "{error:?}; renewals={}",
        job.renewals.load(Ordering::SeqCst)
    );
    assert!(job.renewals.load(Ordering::SeqCst) >= 2);
    assert_eq!(
        error.code(),
        symbiotic_core::DiagnosticCode::ProviderFailure
    );
    assert_eq!(
        error.diagnostics(),
        [
            symbiotic_core::DiagnosticCode::StorageFailure,
            symbiotic_core::DiagnosticCode::SpendLedgerUnavailable
        ]
    );
}

async fn close_storage_failures_preserve_provider_error(backend: &str, queue: Arc<CountsRenewals>) {
    use symbiotic_core::DiagnosticCode;
    use symbiotic_model::{SpendLedger, SpendState};
    for fault in [
        "dead-read",
        "continuation",
        "transition",
        "cooldown",
        "release",
        "cooldown-and-transition",
        "release-and-transition",
    ] {
        for retryable in [false, true] {
            let continuing = matches!(fault, "dead-read" | "continuation");
            if continuing && !retryable {
                continue;
            }
            queue
                .fail_dead_reads
                .store(fault == "dead-read", Ordering::SeqCst);
            queue
                .fail_replacements
                .store(fault == "continuation", Ordering::SeqCst);
            queue
                .fail_transitions
                .store(fault.contains("transition"), Ordering::SeqCst);
            queue
                .fail_cooldown_writes
                .store(fault.contains("cooldown"), Ordering::SeqCst);
            let spend = ObservedSpend::new(Duration::ZERO);
            spend
                .fail_releases
                .store(fault.contains("release"), Ordering::SeqCst);
            let primary = if retryable || fault.contains("cooldown") {
                ModelError::Unavailable(DiagnosticCode::HttpUnavailable)
            } else {
                ModelError::Provider(DiagnosticCode::ProviderFailure)
            };
            let mut raw = Loopback::new(unique_identity()).failing_first(vec![primary.clone()]);
            raw.uncertain_failures = !retryable && fault.contains("cooldown");
            let receipts = Arc::new(InMemoryReceiptSink::default());
            let provider = queued_with_cache(
                raw.clone(),
                queue.clone(),
                ModelQueueConfig {
                    retry_attempts: if continuing { 1 } else { 3 },
                    ..config()
                },
            )
            .with_spend_ledger(spend.clone(), None)
            .with_receipt_sink(receipts.clone());
            let error = provider
                .chat(request("storage after provider failure"))
                .await
                .unwrap_err();
            assert_eq!(
                error.code(),
                primary.code(),
                "{backend}: {fault}, retryable={retryable}: {error:?}"
            );
            let ModelError::Diagnostics {
                primary: preserved, ..
            } = &error
            else {
                panic!("provider error lost diagnostics: {error:?}");
            };
            assert_eq!(
                std::mem::discriminant(preserved.as_ref()),
                std::mem::discriminant(&primary)
            );
            queue.fail_dead_reads.store(false, Ordering::SeqCst);
            queue.fail_replacements.store(false, Ordering::SeqCst);
            let expected = if fault.contains("release") {
                DiagnosticCode::SpendLedgerUnavailable
            } else {
                DiagnosticCode::QueueFailure
            };
            let count = if fault.contains("-and-") { 2 } else { 1 };
            assert_eq!(
                error.diagnostics(),
                &[expected, DiagnosticCode::QueueFailure][..count],
                "{backend}: {fault}"
            );
            let failed: Vec<_> = receipts
                .receipts()
                .into_iter()
                .filter(|r| r.status == ReceiptStatus::Failed)
                .collect();
            assert_eq!(failed.len(), 1, "{backend}: {fault}");
            assert_eq!(failed[0].error, Some(primary.code()));
            let receipt = spend.receipt(&spend.last_reference()).unwrap().unwrap();
            assert_eq!(
                receipt.state,
                if fault.contains("release") || raw.uncertain_failures {
                    SpendState::Unknown
                } else {
                    SpendState::Released
                },
                "{backend}: {fault}"
            );
            let item = queue
                .get_item(&queued_item(&receipts))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                item.status,
                if fault.contains("transition") {
                    QueueStatus::Running
                } else if continuing {
                    QueueStatus::Dead
                } else {
                    QueueStatus::Stopped
                },
                "{backend}: {fault}"
            );
            if !fault.contains("transition") && !continuing {
                queue.fail_cooldown_writes.store(false, Ordering::SeqCst);
                spend.fail_releases.store(false, Ordering::SeqCst);
                spend.release_last();
                assert!(
                    provider
                        .chat(request("storage after provider failure"))
                        .await
                        .is_err()
                );
            }
            assert_eq!(raw.calls.load(Ordering::SeqCst), 1, "{backend}: {fault}");
        }
    }
}

#[tokio::test]
async fn regression_keyless_runtime_discards_raw_before_cache_and_recovery() {
    tokio::time::timeout(Duration::from_secs(3), async {
        for explicit in [false, true] {
            let mut raw = Loopback::new(unique_identity());
            raw.raw_response = Some(json!({"reasoning_content":"PRIVATE_REASONING"}));
            let cache = Arc::new(TextKeyedCache::default());
            let receipts = Arc::new(InMemoryReceiptSink::default());
            let spend = test_spend::ledger();
            let mut provider = queued(raw.clone(), Arc::new(memory_queue()), config())
                .with_spend_ledger(spend.clone(), None)
                .with_response_cache(cache.clone())
                .with_receipt_sink(receipts.clone())
                .with_binding_identity(symbiotic_core::BindingIdentity::new(
                    "tenant", "provider", "1", "account",
                ));
            if explicit {
                provider = provider.with_invocation("raw-disposal".into());
            }
            let response = provider.chat(request("OK")).await.unwrap();
            assert_eq!(response.text, "OK");
            assert!(response.raw_provider_response.is_none());
            let saved = if explicit {
                let reference = receipts
                    .receipts()
                    .into_iter()
                    .find_map(|r| r.spend_receipt)
                    .unwrap();
                spend
                    .receipt(&reference)
                    .unwrap()
                    .unwrap()
                    .recovery
                    .unwrap()
            } else {
                cache.entries.lock().unwrap().get("OK").cloned().unwrap()
            };
            assert_eq!(saved["text"], "OK");
            assert!(saved["raw_provider_response"].is_null());
            assert!(!saved.to_string().contains("PRIVATE_REASONING"));
            let replay = provider.chat(request("OK")).await.unwrap();
            assert_eq!(replay.text, "OK");
            assert!(replay.raw_provider_response.is_none());
            assert_eq!(raw.calls.load(Ordering::SeqCst), 1);
        }
    })
    .await
    .expect("runtime raw disposal must finish within three seconds");
}
