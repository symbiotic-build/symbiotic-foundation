//! The runtime facade: shared limits across bindings, persistence with a
//! state directory, and cache scoping. Loopback providers only.

#[path = "../../test-support/clock.rs"]
mod test_clock;
use async_trait::async_trait;
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use symbiotic_ai_runtime::model::{ChatMessage, ModelCapability, ProviderAuthMode, ProviderClass};
use symbiotic_ai_runtime::{
    ChatProvider, ChatRequest, ChatResponse, DirResponseCache, InMemoryReceiptSink, ModelBinding,
    ModelError, ModelProvider, ModelQueueConfig, ProviderDescriptor, ReceiptStatus,
    ResponseCacheMode, Runtime, RuntimeConfig,
};
use symbiotic_core::{ModelIdentity, TraceId};
use symbiotic_trace::{InvocationOutcome, ModelInvocationTrace};

fn binding<P>(provider: P) -> ModelBinding<P> {
    ModelBinding::new(provider).with_identity(symbiotic_ai_runtime::BindingIdentity::new(
        "tenant", "provider", "1", "account",
    ))
}

static MODEL_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn unique_identity() -> ModelIdentity {
    ModelIdentity::new(
        "chat",
        "loopback",
        format!("runtime-{}", MODEL_COUNTER.fetch_add(1, Ordering::SeqCst)),
    )
}

fn policy() -> ModelQueueConfig {
    ModelQueueConfig {
        max_in_flight: 2,
        lease_seconds: 60,
        logical_retry_attempts: 1,
        retry_attempts: 1,
        retry_jitter_seconds: 0,
        request_timeout_seconds: Some(10),
        retry_base_delay_ms: 10,
        ..ModelQueueConfig::default()
    }
}

fn request(text: &str) -> ChatRequest {
    ChatRequest {
        messages: vec![ChatMessage {
            role: "user".to_string(),
            content: text.to_string(),
        }],
        max_output_tokens: Some(32),
        temperature: Some(0.0),
        response_format: None,
        role_binding: None,
        source: None,
        metadata: json!({}),
    }
}

#[derive(Clone)]
struct Loopback {
    descriptor: ProviderDescriptor,
    calls: Arc<AtomicUsize>,
    active: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
    fail: Option<Arc<ModelError>>,
    delay: Duration,
    credential: Option<Arc<symbiotic_ai_runtime::model::OpenAiCompatibleChatProvider>>,
    measured: bool,
    start_gate: Option<Arc<tokio::sync::Barrier>>,
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
            fail: None,
            delay: Duration::from_millis(15),
            credential: None,
            measured: false,
            start_gate: None,
        }
    }

    fn slow(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }

    fn unavailable(self) -> Self {
        self.failing(ModelError::Unavailable(
            symbiotic_core::DiagnosticCode::HttpUnavailable,
        ))
    }

    fn failing(mut self, err: ModelError) -> Self {
        self.fail = Some(Arc::new(err));
        self
    }
}

impl ModelProvider for Loopback {
    fn credential_fingerprint(&self) -> Option<String> {
        self.credential
            .as_ref()
            .and_then(|p| p.credential_fingerprint())
    }
    fn credential_boundary(&self) -> Option<&symbiotic_ai_runtime::model::CredentialBoundary> {
        self.credential
            .as_ref()
            .and_then(|p| p.credential_boundary())
    }
    fn failure_charge(&self, error: &ModelError) -> symbiotic_ai_runtime::model::FailureCharge {
        if matches!(error, ModelError::Timeout(_)) {
            symbiotic_ai_runtime::model::FailureCharge::Unknown
        } else {
            symbiotic_ai_runtime::model::FailureCharge::KnownZero
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
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(active, Ordering::SeqCst);
        if let Some(gate) = &self.start_gate {
            gate.wait().await;
        }
        tokio::time::sleep(self.delay).await;
        self.active.fetch_sub(1, Ordering::SeqCst);
        if let Some(err) = &self.fail {
            return Err(err.as_ref().clone());
        }
        Ok(ChatResponse {
            text: format!(
                "{}:{}",
                self.descriptor.identity.model.0, request.messages[0].content
            ),
            finish_reason: Some("stop".to_string()),
            trace: ModelInvocationTrace {
                trace_id: TraceId::new(),
                queue_item_id: None,
                model: self.descriptor.identity.clone(),
                role_binding: request.role_binding.clone(),
                source: request.source.clone(),
                request_hash: String::new(),
                response_hash: None,
                cache: Default::default(),
                usage: symbiotic_trace::UsageTrace {
                    input_tokens: self.measured.then_some(7),
                    ..Default::default()
                },
                timing: Default::default(),
                outcome: InvocationOutcome::Succeeded,
                error_class: None,
                audit_refs: Vec::new(),
                metadata: json!({}),
                timestamp: chrono::Utc::now(),
            },
            raw_provider_response: None,
        })
    }
}

/// A temporary directory closed to others, as a state directory must be.
fn private_tempdir() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    dir
}

fn persistent(dir: &std::path::Path) -> Runtime {
    Runtime::open(RuntimeConfig {
        state_dir: Some(dir.to_path_buf()),
        ..RuntimeConfig::default()
    })
    .unwrap()
}

#[tokio::test]
async fn spend_dispatch_requires_an_explicit_state_directory() {
    for runtime in [
        Runtime::in_memory().unwrap(),
        Runtime::open(RuntimeConfig::default()).unwrap(),
    ] {
        let raw = Loopback::new(unique_identity());
        let provider = runtime
            .chat(binding(raw.clone()).with_policy(policy()))
            .unwrap();
        assert!(matches!(
            provider.chat(request("no state")).await,
            Err(ModelError::Queue(
                symbiotic_core::DiagnosticCode::SpendLedgerUnavailable
            ))
        ));
        assert_eq!(raw.calls.load(Ordering::SeqCst), 0);
        assert!(!runtime.is_persistent());
    }
}

#[tokio::test(start_paused = true)]
async fn spend_timeout_never_uses_the_unused_attempt_allowance() {
    let raw = Loopback::new(unique_identity()).slow(Duration::from_secs(2));
    let sink = Arc::new(InMemoryReceiptSink::default());
    let dir = private_tempdir();
    let runtime = Runtime::open(RuntimeConfig {
        state_dir: Some(dir.path().into()),
        receipt_sink: Some(sink.clone()),
        ..RuntimeConfig::default()
    })
    .unwrap();
    let chat = runtime
        .chat(binding(raw.clone()).with_policy(ModelQueueConfig {
            request_timeout_seconds: Some(1),
            logical_retry_attempts: 2,
            retry_attempts: 2,
            ..policy()
        }))
        .unwrap();
    assert!(chat.chat(request("timeout")).await.is_err());
    assert_eq!(raw.calls.load(Ordering::SeqCst), 1);
    let reference = sink
        .receipts()
        .into_iter()
        .find(|r| r.status == ReceiptStatus::Failed)
        .unwrap()
        .spend_receipt
        .unwrap();
    drop((chat, runtime));
    let receipt = persistent(dir.path())
        .spend_receipt(&reference)
        .unwrap()
        .unwrap();
    assert_eq!(receipt.state, symbiotic_ai_runtime::SpendState::Unknown);
    assert!(receipt.usage.is_none());
}

#[tokio::test]
async fn spend_explicit_lost_success_reply_recovers_without_response_cache_after_restart() {
    let dir = private_tempdir();
    let raw = Loopback::new(unique_identity());
    let install = |runtime: &Runtime| {
        runtime
            .chat(
                binding(raw.clone())
                    .with_policy(policy())
                    .with_invocation("lost-reply")
                    .with_response_cache(ResponseCacheMode::Off),
            )
            .unwrap()
    };
    let runtime = persistent(dir.path());
    let original = install(&runtime).chat(request("lost reply")).await.unwrap();
    drop(runtime);
    let recovered = install(&persistent(dir.path()))
        .chat(request("lost reply"))
        .await
        .unwrap();
    assert_eq!(recovered.text, original.text);
    assert_eq!(raw.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn bindings_of_one_model_share_its_cap() {
    let state = private_tempdir();
    let runtime = persistent(state.path());
    let raw = Loopback::new(unique_identity());
    let answer = runtime
        .chat(binding(raw.clone()).with_policy(policy()))
        .unwrap();
    let judge = runtime
        .chat(binding(raw.clone()).with_policy(policy()))
        .unwrap();
    let calls: Vec<_> = (0..10)
        .map(|idx| {
            let provider = if idx % 2 == 0 {
                answer.clone()
            } else {
                judge.clone()
            };
            tokio::spawn(async move { provider.chat(request(&format!("q{idx}"))).await })
        })
        .collect();
    for call in calls {
        call.await.unwrap().unwrap();
    }
    assert_eq!(raw.calls.load(Ordering::SeqCst), 10);
    assert!(raw.peak.load(Ordering::SeqCst) <= 2);
}

#[tokio::test]
async fn conflicting_limits_for_one_model_are_rejected_and_other_models_are_independent() {
    let runtime = Runtime::in_memory().unwrap();
    let identity = unique_identity();
    runtime
        .chat(binding(Loopback::new(identity.clone())).with_policy(policy()))
        .unwrap();
    let conflicting = ModelQueueConfig {
        requests_per_minute: Some(60),
        ..policy()
    };
    let err = runtime
        .chat(binding(Loopback::new(identity)).with_policy(conflicting.clone()))
        .err()
        .expect("a second policy for one model is rejected");
    assert!(matches!(err, ModelError::InvalidRequest(_)), "{err:?}");
    // Retry settings may differ per binding; only shared limits must agree.
    runtime
        .chat(
            binding(Loopback::new(unique_identity()))
                .with_policy(conflicting)
                .with_identity(symbiotic_ai_runtime::BindingIdentity::new(
                    "tenant",
                    "provider",
                    "1",
                    "other-account",
                )),
        )
        .unwrap();
}

#[tokio::test]
async fn host_provider_objects_can_be_bound() {
    let state = private_tempdir();
    let runtime = persistent(state.path());
    let raw: Arc<dyn ChatProvider> = Arc::new(Loopback::new(unique_identity()));
    let chat = runtime.chat(binding(raw).with_policy(policy())).unwrap();
    assert!(
        chat.chat(request("hi"))
            .await
            .unwrap()
            .text
            .ends_with(":hi")
    );
}

#[tokio::test]
async fn a_persistent_runtime_keeps_state_owner_only() {
    let dir = private_tempdir();
    let state = dir.path().join("ai-runtime");
    let runtime = persistent(&state);
    assert!(runtime.is_persistent());
    assert!(state.join(symbiotic_ai_runtime::QUEUE_DATABASE).is_file());
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let dir_mode = std::fs::metadata(&state).unwrap().permissions().mode();
        let db_mode = std::fs::metadata(state.join(symbiotic_ai_runtime::QUEUE_DATABASE))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(dir_mode & 0o777, 0o700);
        assert_eq!(db_mode & 0o777, 0o600);
    }
}

#[tokio::test]
async fn cached_responses_survive_a_restart_and_stay_scoped_to_their_model() {
    let dir = private_tempdir();
    let first_model = Loopback::new(unique_identity());
    let second_model = Loopback::new(unique_identity());

    let runtime = persistent(dir.path());
    let chat = runtime
        .chat(binding(first_model.clone()).with_policy(policy()))
        .unwrap();
    let original = chat.chat(request("same question")).await.unwrap();
    drop((chat, runtime));

    let reopened = persistent(dir.path());
    let chat = reopened
        .chat(binding(first_model.clone()).with_policy(policy()))
        .unwrap();
    let replay = chat.chat(request("same question")).await.unwrap();
    assert_eq!(replay.text, original.text);
    assert_eq!(first_model.calls.load(Ordering::SeqCst), 1);

    // Another model asked the same question gets its own answer.
    let other = reopened
        .chat(binding(second_model.clone()).with_policy(policy()))
        .unwrap();
    let answer = other.chat(request("same question")).await.unwrap();
    assert_ne!(answer.text, original.text);
    assert_eq!(second_model.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn attempt_budgets_survive_a_restart_only_when_persistent() {
    let dir = private_tempdir();
    let down = Loopback::new(unique_identity()).unavailable();
    let binding = || {
        binding(down.clone())
            .with_policy(policy())
            .with_response_cache(ResponseCacheMode::Default)
    };

    let runtime = persistent(dir.path());
    runtime
        .chat(binding())
        .unwrap()
        .chat(request("doomed"))
        .await
        .unwrap_err();
    assert_eq!(down.calls.load(Ordering::SeqCst), 1);

    // Cache-enabled calls retain queue coordination across a restart.
    let reopened = persistent(dir.path());
    let err = reopened
        .chat(binding())
        .unwrap()
        .chat(request("doomed"))
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        ModelError::Unavailable(symbiotic_core::DiagnosticCode::AttemptBudgetExhausted)
    ));
    assert_eq!(down.calls.load(Ordering::SeqCst), 1);

    // A separate state directory starts with a fresh budget.
    let fresh = private_tempdir();
    persistent(fresh.path())
        .chat(binding())
        .unwrap()
        .chat(request("doomed"))
        .await
        .unwrap_err();
    assert_eq!(down.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn spend_cache_off_recovers_the_same_attempt_and_binding_sinks_apply() {
    let state = private_tempdir();
    let runtime = persistent(state.path());
    let raw = Loopback::new(unique_identity());
    let receipts = Arc::new(InMemoryReceiptSink::default());
    let chat = runtime
        .chat(
            binding(raw.clone())
                .with_policy(policy())
                .with_invocation("same-attempt")
                .with_receipt_sink(receipts.clone())
                .with_response_cache(ResponseCacheMode::Off),
        )
        .unwrap();
    chat.chat(request("again")).await.unwrap();
    chat.chat(request("again")).await.unwrap();
    assert_eq!(raw.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        receipts
            .receipts()
            .iter()
            .filter(|receipt| receipt.status == ReceiptStatus::Succeeded)
            .count(),
        1
    );
}

#[tokio::test]
async fn a_queue_id_isolates_a_role_or_pools_models() {
    let state = private_tempdir();
    let runtime = persistent(state.path());
    let mut raw = Loopback::new(unique_identity());
    raw.start_gate = Some(Arc::new(tokio::sync::Barrier::new(2)));
    let one_slot = ModelQueueConfig {
        max_in_flight: 1,
        ..policy()
    };
    // The same model on its own queue and on an isolated role queue: the two
    // queues do not share the one slot.
    let shared = runtime
        .chat(binding(raw.clone()).with_policy(one_slot.clone()))
        .unwrap();
    let isolated = runtime
        .chat(
            binding(raw.clone())
                .with_policy(one_slot.clone())
                .with_account_sharing(symbiotic_ai_runtime::AccountSharingKey::new(
                    "answer:isolated",
                )),
        )
        .unwrap();
    let (a, b) = tokio::join!(shared.chat(request("a")), isolated.chat(request("b")));
    a.unwrap();
    b.unwrap();
    assert_eq!(raw.peak.load(Ordering::SeqCst), 2);

    // Two models pooled on one queue share its slot: count their calls on
    // one gauge.
    let first = Loopback::new(unique_identity());
    let mut second = Loopback::new(unique_identity());
    second.active = first.active.clone();
    second.peak = first.peak.clone();
    let pool = symbiotic_ai_runtime::AccountSharingKey::new("chat:pool:shared");
    let first_chat = runtime
        .chat(
            binding(first.clone())
                .with_policy(one_slot.clone())
                .with_account_sharing(pool.clone()),
        )
        .unwrap();
    let second_chat = runtime
        .chat(
            binding(second.clone())
                .with_policy(one_slot)
                .with_account_sharing(pool),
        )
        .unwrap();
    let (a, b) = tokio::join!(
        first_chat.chat(request("x")),
        second_chat.chat(request("y"))
    );
    a.unwrap();
    b.unwrap();
    assert_eq!(first.calls.load(Ordering::SeqCst), 1);
    assert_eq!(second.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        first.peak.load(Ordering::SeqCst),
        1,
        "pooled models share one slot"
    );
}

#[tokio::test]
async fn pooled_models_keep_separate_budgets_for_the_same_request() {
    let state = private_tempdir();
    let runtime = persistent(state.path());
    let pool = symbiotic_ai_runtime::AccountSharingKey::new("chat:pool:budgets");
    let down = Loopback::new(unique_identity()).unavailable();
    let up = Loopback::new(unique_identity());
    let bind = |raw: Loopback| {
        runtime
            .chat(
                binding(raw)
                    .with_policy(policy())
                    .with_account_sharing(pool.clone()),
            )
            .unwrap()
    };
    bind(down.clone())
        .chat(request("identical"))
        .await
        .unwrap_err();
    // The other model's identical request is its own call, not the first
    // model's exhausted one.
    let answer = bind(up.clone()).chat(request("identical")).await.unwrap();
    assert!(answer.text.ends_with(":identical"));
    assert_eq!(up.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn an_exhausted_error_keeps_its_class_after_a_restart() {
    let dir = private_tempdir();
    let broken = Loopback::new(unique_identity()).failing(ModelError::Provider(
        symbiotic_core::DiagnosticCode::ProviderFailure,
    ));
    let binding = || {
        binding(broken.clone())
            .with_policy(ModelQueueConfig {
                retry_provider_errors: true,
                ..policy()
            })
            .with_response_cache(ResponseCacheMode::Default)
    };
    let first = persistent(dir.path())
        .chat(binding())
        .unwrap()
        .chat(request("unparsable"))
        .await
        .unwrap_err();
    assert!(matches!(first, ModelError::Provider(_)), "{first:?}");

    let again = persistent(dir.path())
        .chat(binding())
        .unwrap()
        .chat(request("unparsable"))
        .await
        .unwrap_err();
    assert!(matches!(again, ModelError::Provider(_)), "{again:?}");
    assert!(matches!(
        again,
        ModelError::Provider(symbiotic_core::DiagnosticCode::AttemptBudgetExhausted)
    ));
    assert_eq!(broken.calls.load(Ordering::SeqCst), 1);
}

/// A 3 s lease, renewed every second.
fn leased() -> ModelQueueConfig {
    ModelQueueConfig {
        lease_seconds: 3,
        ..policy()
    }
}

/// The reported defect: a job timeout gives up on the first call while the
/// provider works, and the identical call behind it must still finish.
async fn an_abandoned_call_does_not_block_the_identical_request_behind_it(runtime: Runtime) {
    let mut raw = Loopback::new(unique_identity()).slow(Duration::from_millis(60));
    let entered = Arc::new(tokio::sync::Barrier::new(2));
    raw.start_gate = Some(entered.clone());
    let chat = runtime
        .chat(binding(raw.clone()).with_policy(leased()))
        .unwrap();

    let caller = chat.clone();
    let first = tokio::spawn(async move { caller.chat(request("same")).await });
    entered.wait().await;
    first.abort();
    assert!(
        first.await.unwrap_err().is_cancelled(),
        "the first caller stops waiting"
    );

    let second = tokio::time::timeout(Duration::from_secs(20), chat.chat(request("same")))
        .await
        .expect("the identical request finishes")
        .unwrap();
    assert!(second.text.ends_with(":same"), "{}", second.text);
}

/// An identical caller already waiting, and one arriving later, both get the
/// abandoned call's answer from one provider call.
async fn a_waiter_and_a_later_caller_share_the_answer_of_an_abandoned_call(
    runtime: Runtime,
    cache: ResponseCacheMode,
) {
    let mut raw = Loopback::new(unique_identity()).slow(Duration::from_millis(60));
    let entered = Arc::new(tokio::sync::Barrier::new(2));
    raw.start_gate = Some(entered.clone());
    let chat = runtime
        .chat(
            binding(raw.clone())
                .with_policy(leased())
                .with_response_cache(cache),
        )
        .unwrap();

    let caller = chat.clone();
    let first = tokio::spawn(async move { caller.chat(request("shared")).await });
    entered.wait().await;
    let caller = chat.clone();
    let waiter = tokio::spawn(async move { caller.chat(request("shared")).await });
    first.abort();
    assert!(
        first.await.unwrap_err().is_cancelled(),
        "the first caller stops waiting"
    );

    let waited = tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .expect("the identical waiter finishes")
        .unwrap()
        .unwrap();
    let later = tokio::time::timeout(Duration::from_secs(5), chat.chat(request("shared")))
        .await
        .expect("the later identical request finishes")
        .unwrap();
    assert_eq!(waited.text, later.text);
    assert_eq!(
        raw.calls.load(Ordering::SeqCst),
        1,
        "one provider call answered all three callers"
    );
}

/// Persistent dispatch with a custom response cache.
mod custom_cache {
    use super::*;

    #[tokio::test]
    async fn an_abandoned_call_does_not_block_the_identical_request_behind_it() {
        let state = private_tempdir();
        super::an_abandoned_call_does_not_block_the_identical_request_behind_it(persistent(
            state.path(),
        ))
        .await;
    }

    #[tokio::test]
    async fn a_waiter_and_a_later_caller_share_the_answer_of_an_abandoned_call() {
        // A host cache answers later callers independently of the operational store.
        let state = private_tempdir();
        let cache = private_tempdir();
        super::a_waiter_and_a_later_caller_share_the_answer_of_an_abandoned_call(
            persistent(state.path()),
            ResponseCacheMode::Custom(Arc::new(DirResponseCache::new(cache.path()))),
        )
        .await;
    }
}

/// `SqliteQueue` in a state directory.
mod persistent {
    use super::*;

    #[tokio::test]
    async fn an_abandoned_call_does_not_block_the_identical_request_behind_it() {
        let dir = private_tempdir();
        super::an_abandoned_call_does_not_block_the_identical_request_behind_it(persistent(
            dir.path(),
        ))
        .await;
    }

    #[tokio::test]
    async fn a_waiter_and_a_later_caller_share_the_answer_of_an_abandoned_call() {
        let dir = private_tempdir();
        super::a_waiter_and_a_later_caller_share_the_answer_of_an_abandoned_call(
            persistent(dir.path()),
            ResponseCacheMode::Default,
        )
        .await;
    }
}

// ---------------------------------------------------------------------------
// Private state, credential rotation, the logical attempt cap, cache retention
// ---------------------------------------------------------------------------

/// Every directory and file under `root`, with its permission bits.
#[cfg(unix)]
fn modes(root: &std::path::Path) -> Vec<(std::path::PathBuf, u32)> {
    use std::os::unix::fs::PermissionsExt;
    let mut found = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(path) = pending.pop() {
        let meta = std::fs::symlink_metadata(&path).unwrap();
        found.push((path.clone(), meta.permissions().mode() & 0o777));
        if meta.is_dir() {
            for entry in std::fs::read_dir(&path).unwrap() {
                pending.push(entry.unwrap().path());
            }
        }
    }
    found
}

#[cfg(unix)]
fn assert_owner_only(root: &std::path::Path) {
    for (path, mode) in modes(root) {
        let expected = if path.is_dir() { 0o700 } else { 0o600 };
        assert_eq!(mode, expected, "{} is {mode:o}", path.display());
    }
}

#[cfg(unix)]
#[tokio::test]
async fn an_existing_state_dir_open_to_others_is_refused() {
    use std::os::unix::fs::PermissionsExt;
    let dir = private_tempdir();
    let state = dir.path().join("state");
    std::fs::create_dir(&state).unwrap();
    std::fs::set_permissions(&state, std::fs::Permissions::from_mode(0o755)).unwrap();
    let err = Runtime::open(RuntimeConfig {
        state_dir: Some(state.clone()),
        ..RuntimeConfig::default()
    })
    .err()
    .expect("a 0755 state directory is refused");
    assert!(matches!(err, ModelError::Queue(_)));
}

#[cfg(unix)]
#[tokio::test]
async fn a_symlinked_state_dir_is_refused() {
    let dir = private_tempdir();
    let real = dir.path().join("real");
    persistent(&real);
    let link = dir.path().join("link");
    std::os::unix::fs::symlink(&real, &link).unwrap();
    let err = Runtime::open(RuntimeConfig {
        state_dir: Some(link),
        ..RuntimeConfig::default()
    })
    .err()
    .expect("a symlinked state directory is refused");
    assert!(matches!(err, ModelError::Queue(_)));
}

#[cfg(unix)]
#[tokio::test]
async fn a_symlinked_cache_dir_is_refused() {
    let dir = private_tempdir();
    let state = dir.path().join("state");
    persistent(&state);
    let path = state.join(symbiotic_ai_runtime::QUEUE_DATABASE);
    let ledger = symbiotic_ai_runtime::spend::SqliteSpendLedger::open(&path)
        .unwrap()
        .with_retention(Duration::ZERO);
    let reservation = symbiotic_ai_runtime::SpendReservation {
        reference: symbiotic_ai_runtime::SpendReceiptRef::new("expired-at-open").unwrap(),
        account: "account".into(),
        invocation: "explicit".into(),
        binding: "input".into(),
        request_limit: None,
    };
    use symbiotic_ai_runtime::{SpendLedger, SpendState};
    ledger.reserve_explicit(&reservation, 1).unwrap();
    ledger
        .finish(
            &reservation.reference,
            SpendState::Unknown,
            None,
            Some(json!({"private":"answer"})),
            None,
        )
        .unwrap();
    let conn = rusqlite::Connection::open(path).unwrap();
    let payloads = || {
        conn.query_row("SELECT count(*) FROM spend_receipts WHERE recovery IS NOT NULL OR recovery_expires_at IS NOT NULL", [], |row| row.get::<_, usize>(0)).unwrap()
    };
    assert_eq!(payloads(), 1);
    let elsewhere = private_tempdir();
    let precious = elsewhere.path().join("precious");
    std::fs::write(&precious, "untouched").unwrap();
    let responses = state.join(symbiotic_ai_runtime::RESPONSES_DIR);
    let _ = std::fs::remove_dir_all(&responses);
    std::os::unix::fs::symlink(elsewhere.path(), &responses).unwrap();
    let err = Runtime::open(RuntimeConfig {
        state_dir: Some(state),
        ..RuntimeConfig::default()
    })
    .err()
    .expect("a symlinked response cache is refused");
    assert!(matches!(err, ModelError::Queue(_)));
    assert_eq!(
        payloads(),
        0,
        "unsafe cache must not block open-time expiry"
    );
    assert_eq!(std::fs::read_link(responses).unwrap(), elsewhere.path());
    assert_eq!(std::fs::read_to_string(precious).unwrap(), "untouched");
}

#[cfg(unix)]
#[tokio::test]
async fn runtime_state_and_cached_responses_are_owner_only() {
    let dir = private_tempdir();
    let state = dir.path().join("state");
    let runtime = persistent(&state);
    let raw = Loopback::new(unique_identity());
    runtime
        .chat(binding(raw).with_policy(policy()))
        .unwrap()
        .chat(request("private answer"))
        .await
        .unwrap();
    assert!(
        modes(&state)
            .iter()
            .any(|(path, _)| path.extension().is_some_and(|ext| ext == "json")),
        "the response was cached"
    );
    assert_owner_only(&state);
}

#[cfg(unix)]
#[tokio::test]
async fn state_written_by_an_earlier_version_is_tightened() {
    use std::os::unix::fs::PermissionsExt;
    let dir = private_tempdir();
    let state = dir.path().join("state");
    drop(persistent(&state));
    // An earlier version wrote the cache with default permissions.
    let entry_dir = state
        .join(symbiotic_ai_runtime::RESPONSES_DIR)
        .join("scope/chat");
    std::fs::create_dir_all(&entry_dir).unwrap();
    for dir in [
        state.join(symbiotic_ai_runtime::RESPONSES_DIR),
        state
            .join(symbiotic_ai_runtime::RESPONSES_DIR)
            .join("scope"),
        entry_dir.clone(),
    ] {
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    let entry = entry_dir.join("hash.json");
    std::fs::write(&entry, b"{}").unwrap();
    std::fs::set_permissions(&entry, std::fs::Permissions::from_mode(0o644)).unwrap();

    drop(persistent(&state));
    assert_owner_only(&state);
}

/// An OpenAI-compatible endpoint on loopback that answers only `good_key`
/// and counts the requests it gets.
fn chat_endpoint(good_key: &'static str) -> (String, Arc<AtomicUsize>) {
    use std::io::{Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let hits = Arc::new(AtomicUsize::new(0));
    let seen = hits.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else {
                return;
            };
            seen.fetch_add(1, Ordering::SeqCst);
            let mut bytes = Vec::new();
            let mut chunk = [0; 4096];
            let (head_end, length) = loop {
                let read = stream.read(&mut chunk).unwrap();
                bytes.extend_from_slice(&chunk[..read]);
                if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                    let head = String::from_utf8_lossy(&bytes[..end]).to_string();
                    let length = head
                        .lines()
                        .find_map(|line| {
                            let (key, value) = line.split_once(':')?;
                            key.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().unwrap())
                        })
                        .unwrap_or(0);
                    break (end + 4, length);
                }
            };
            while bytes.len() < head_end + length {
                let read = stream.read(&mut chunk).unwrap();
                bytes.extend_from_slice(&chunk[..read]);
            }
            let head = String::from_utf8_lossy(&bytes[..head_end]).to_ascii_lowercase();
            let authorized = head.contains(&format!("bearer {}", good_key.to_ascii_lowercase()));
            let (status, body) = if authorized {
                (
                    "200 OK",
                    json!({
                        "choices": [{"message": {"content": "OK"}, "finish_reason": "stop"}],
                        "usage": {"prompt_tokens": 3, "completion_tokens": 1}
                    }),
                )
            } else {
                (
                    "401 Unauthorized",
                    json!({"error": {"message": "invalid api key"}}),
                )
            };
            let body = body.to_string();
            let _ = write!(
                stream,
                "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
        }
    });
    (url, hits)
}

const KEY_A: &str = "sk-rotated-out-aaaaaaaaaaaaaaaaaaaa";
const KEY_B: &str = "sk-rotated-in-bbbbbbbbbbbbbbbbbbbbbb";

async fn a_rotated_credential_gets_a_fresh_budget(runtime: Runtime) {
    let (url, hits) = chat_endpoint(KEY_B);
    let model = unique_identity().model.0;
    let receipts = Arc::new(InMemoryReceiptSink::default());
    let traces = Arc::new(symbiotic_trace::InMemoryTraceSink::default());
    let bind = |key: &str| {
        runtime
            .chat(
                binding(
                    symbiotic_ai_runtime::model::OpenAiCompatibleChatProvider::new(
                        "loopback", &model, &url, key,
                    )
                    .with_request_limit(65536)
                    .with_response_limit(65536),
                )
                .with_policy(policy())
                .with_response_cache(ResponseCacheMode::Off)
                .with_receipt_sink(receipts.clone())
                .with_trace_sink(traces.clone()),
            )
            .unwrap()
    };

    let err = bind(KEY_A).chat(request("rotate")).await.unwrap_err();
    assert!(matches!(err, ModelError::Auth(_)), "{err:?}");
    // Credential rotation does not erase the first attempt's uncertain charge.
    let err = bind(KEY_A).chat(request("rotate")).await.unwrap_err();
    assert!(
        matches!(
            err,
            ModelError::Queue(symbiotic_core::DiagnosticCode::SpendReconciliationRequired)
        ),
        "{err:?}"
    );
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    let reference = receipts
        .receipts()
        .into_iter()
        .find_map(|r| r.spend_receipt)
        .unwrap();
    let err = bind(KEY_B).chat(request("rotate")).await.unwrap_err();
    assert!(
        matches!(
            err,
            ModelError::Queue(symbiotic_core::DiagnosticCode::SpendReconciliationRequired)
        ),
        "rotating an unreconciled credential must refuse dispatch: {err:?}"
    );
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    // This synthetic endpoint establishes that its rejected key incurred no charge.
    runtime
        .reconcile_spend(&reference, symbiotic_ai_runtime::SpendState::Released, None)
        .unwrap();

    // A reconciled zero-charge attempt permits the rotated key's new handoff.
    let answer = bind(KEY_B)
        .chat(request("rotate"))
        .await
        .unwrap_or_else(|err| panic!("the rotated key reaches the provider: {err}"));
    assert_eq!(answer.text, "OK");
    assert_eq!(hits.load(Ordering::SeqCst), 2);

    // Neither key is kept anywhere the runtime writes.
    let written = format!(
        "{} {}",
        serde_json::to_string(&receipts.receipts()).unwrap(),
        serde_json::to_string(&traces.records()).unwrap()
    );
    for key in [KEY_A, KEY_B] {
        assert!(!written.contains(key), "a key reached a receipt or trace");
        assert!(
            !written.contains(&key[3..15]),
            "part of a key reached a receipt or trace"
        );
    }
    if let Some(state) = runtime.state_dir() {
        for path in files_under(state) {
            let bytes = std::fs::read(&path).unwrap();
            for key in [KEY_A, KEY_B] {
                assert!(
                    !bytes.windows(key.len()).any(|part| part == key.as_bytes()),
                    "{} holds a key",
                    path.display()
                );
            }
        }
    }
}

async fn logical_attempts_cap_provider_calls(runtime: Runtime) {
    for (logical, per_item) in [(1, 3), (2, 3)] {
        let down = Loopback::new(unique_identity()).unavailable();
        let err = runtime
            .chat(
                binding(down.clone())
                    .with_policy(ModelQueueConfig {
                        logical_retry_attempts: logical,
                        retry_attempts: per_item,
                        ..policy()
                    })
                    .with_response_cache(ResponseCacheMode::Off),
            )
            .unwrap()
            .chat(request("capped"))
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            ModelError::Unavailable(symbiotic_core::DiagnosticCode::AttemptBudgetExhausted)
        ));
        assert_eq!(
            down.calls.load(Ordering::SeqCst),
            logical as usize,
            "logical {logical}, per item {per_item}"
        );
    }
}

mod rotation_and_cap {
    use super::*;

    #[tokio::test]
    async fn separate_state_a_rotated_credential_gets_a_fresh_budget() {
        let state = private_tempdir();
        a_rotated_credential_gets_a_fresh_budget(persistent(state.path())).await;
    }

    #[tokio::test]
    async fn persistent_a_rotated_credential_gets_a_fresh_budget() {
        let dir = private_tempdir();
        a_rotated_credential_gets_a_fresh_budget(persistent(dir.path())).await;
    }

    #[tokio::test]
    async fn separate_state_logical_attempts_cap_provider_calls() {
        let state = private_tempdir();
        logical_attempts_cap_provider_calls(persistent(state.path())).await;
    }

    #[tokio::test]
    async fn persistent_logical_attempts_cap_provider_calls() {
        let dir = private_tempdir();
        logical_attempts_cap_provider_calls(persistent(dir.path())).await;
    }
}

/// Every file under `root`, without following symlinks.
fn files_under(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut found = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(dir) = pending.pop() {
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            let meta = std::fs::symlink_metadata(&path).unwrap();
            if meta.is_dir() {
                pending.push(path);
            } else if meta.is_file() {
                found.push(path);
            }
        }
    }
    found
}

/// The cached response files under `state`.
fn cached_files(state: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut found = Vec::new();
    let mut pending = vec![state.join(symbiotic_ai_runtime::RESPONSES_DIR)];
    while let Some(path) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&path) else {
            continue;
        };
        for entry in entries {
            let path = entry.unwrap().path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|ext| ext == "json") {
                found.push(path);
            }
        }
    }
    found
}

fn age(path: &std::path::Path, by: Duration) {
    std::fs::File::options()
        .write(true)
        .open(path)
        .unwrap()
        .set_modified(std::time::SystemTime::now() - by)
        .unwrap();
}

#[tokio::test]
async fn an_expired_cached_response_misses_and_the_sweep_removes_it() {
    let dir = private_tempdir();
    let mut raw = Loopback::new(unique_identity());
    raw.measured = true;
    let ask = |runtime: &Runtime| {
        let chat = runtime
            .chat(binding(raw.clone()).with_policy(policy()))
            .unwrap();
        async move { chat.chat(request("aging")).await.unwrap() }
    };
    let runtime = persistent(dir.path());
    ask(&runtime).await;
    ask(&runtime).await;
    assert_eq!(raw.calls.load(Ordering::SeqCst), 1, "a fresh entry hits");

    // Past the default maximum age, the entry misses.
    let files = cached_files(dir.path());
    assert_eq!(files.len(), 1);
    age(&files[0], Duration::from_secs(31 * 24 * 60 * 60));
    ask(&runtime).await;
    assert_eq!(
        raw.calls.load(Ordering::SeqCst),
        2,
        "an implicit call cannot recover an expired cache entry from the ledger"
    );

    // The sweep at open removes expired entries.
    age(
        &cached_files(dir.path())[0],
        Duration::from_secs(31 * 24 * 60 * 60),
    );
    drop(runtime);
    drop(persistent(dir.path()));
    assert!(cached_files(dir.path()).is_empty(), "the sweep removed it");
}

#[tokio::test]
async fn the_sweep_keeps_the_newest_responses_within_the_size_limit() {
    let dir = private_tempdir();
    let raw = Loopback::new(unique_identity());
    let runtime = persistent(dir.path());
    let chat = runtime
        .chat(binding(raw.clone()).with_policy(policy()))
        .unwrap();
    for text in ["oldest", "middle", "newest"] {
        chat.chat(request(text)).await.unwrap();
    }
    drop((chat, runtime));
    // Distinct ages, oldest first.
    let mut files = cached_files(dir.path());
    files.sort();
    let mut by_age = Vec::new();
    for file in &files {
        let text = std::fs::read_to_string(file).unwrap();
        let rank = ["newest", "middle", "oldest"]
            .iter()
            .position(|word| text.contains(word))
            .unwrap();
        age(file, Duration::from_secs(60 * (rank as u64 + 1)));
        by_age.push((rank, file.clone()));
    }
    let entry_size = std::fs::metadata(&files[0]).unwrap().len();

    drop(
        Runtime::open(RuntimeConfig {
            state_dir: Some(dir.path().to_path_buf()),
            response_max_bytes: Some(entry_size * 2 + entry_size / 2),
            ..RuntimeConfig::default()
        })
        .unwrap(),
    );
    let kept: Vec<_> = by_age
        .iter()
        .filter(|(_, file)| file.exists())
        .map(|(rank, _)| *rank)
        .collect();
    assert_eq!(kept.len(), 2, "{kept:?}");
    assert!(!kept.contains(&2), "the oldest was removed: {kept:?}");
}

#[tokio::test]
async fn purging_a_source_removes_only_its_responses() {
    let dir = private_tempdir();
    let mut raw = Loopback::new(unique_identity());
    raw.measured = true;
    let runtime = persistent(dir.path());
    let chat = runtime
        .chat(binding(raw.clone()).with_policy(policy()))
        .unwrap();
    let from = |source: &str, text: &str| ChatRequest {
        source: Some(source.to_string()),
        ..request(text)
    };
    chat.chat(from("tenant-a/doc-1", "one")).await.unwrap();
    chat.chat(from("tenant-b/doc-2", "two")).await.unwrap();
    assert_eq!(raw.calls.load(Ordering::SeqCst), 2);

    let removed = runtime
        .purge_responses(|cached| {
            cached
                .source
                .as_deref()
                .is_some_and(|source| source.starts_with("tenant-a/"))
        })
        .unwrap();
    assert_eq!(removed, 1);
    // Erasure preserves accounting, while the erased implicit answer is fetched again.
    chat.chat(from("tenant-a/doc-1", "one")).await.unwrap();
    chat.chat(from("tenant-b/doc-2", "two")).await.unwrap();
    assert_eq!(raw.calls.load(Ordering::SeqCst), 3);
    let conn =
        rusqlite::Connection::open(dir.path().join(symbiotic_ai_runtime::QUEUE_DATABASE)).unwrap();
    assert_eq!(
        conn.query_row("SELECT used FROM spend_accounts", [], |r| r
            .get::<_, u64>(0))
            .unwrap(),
        3
    );
    assert_eq!(
        conn.query_row(
            "SELECT count(*) FROM spend_receipts WHERE recovery IS NOT NULL",
            [],
            |r| r.get::<_, u64>(0)
        )
        .unwrap(),
        0
    );
    assert_eq!(
        Runtime::in_memory()
            .unwrap()
            .purge_responses(|_| true)
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn binding_identity_partitions_results_and_purge_by_tenant() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = Runtime::open(RuntimeConfig {
        state_dir: Some(dir.path().join("state")),
        ..RuntimeConfig::default()
    })
    .unwrap();
    let raw = Loopback::new(unique_identity());
    for (tenant, principal, revision, account) in [
        ("a", "p", "1", "acct"),
        ("b", "p", "1", "acct"),
        ("a", "other", "1", "acct"),
        ("a", "p", "2", "acct"),
        ("a", "p", "1", "other"),
    ] {
        let provider = runtime
            .chat(binding(raw.clone()).with_policy(policy()).with_identity(
                symbiotic_ai_runtime::BindingIdentity::new(tenant, principal, revision, account),
            ))
            .unwrap();
        provider.chat(request("same")).await.unwrap();
        provider.chat(request("same")).await.unwrap();
    }
    assert_eq!(raw.calls.load(Ordering::SeqCst), 5);
    assert_eq!(
        runtime
            .purge_responses(|entry| entry.binding.as_ref().is_some_and(|b| b.tenant.0 == "b"))
            .unwrap(),
        1
    );
}

#[test]
fn effective_transport_settings_change_result_identity() {
    use symbiotic_ai_runtime::model::{OpenAiCompatibleChatProvider, ThinkingMode};
    let provider = || {
        OpenAiCompatibleChatProvider::new(
            "operator",
            "model",
            "https://one.example",
            "synthetic-key",
        )
        .with_request_limit(65536)
        .with_response_limit(65536)
    };
    let descriptor =
        |p: OpenAiCompatibleChatProvider| serde_json::to_value(p.descriptor()).unwrap();
    let base = descriptor(provider());
    assert_ne!(
        base,
        descriptor(
            OpenAiCompatibleChatProvider::new(
                "operator",
                "model",
                "https://two.example",
                "synthetic-key"
            )
            .with_request_limit(65536)
            .with_response_limit(65536)
        )
    );
    assert_ne!(
        base,
        descriptor(provider().with_thinking(Some(ThinkingMode::Enabled)))
    );
    assert_ne!(base, descriptor(provider().with_reasoning_effort("high")));
}

#[test]
fn runtime_refuses_missing_binding_identity() {
    let raw = Loopback::new(unique_identity());
    assert!(
        Runtime::in_memory()
            .unwrap()
            .chat(ModelBinding::new(raw).with_policy(policy()))
            .is_err()
    );
}

#[tokio::test]
async fn effective_configuration_partitions_cache_even_when_custom_cache_ignores_scope() {
    struct OneEntry(std::sync::Mutex<Option<serde_json::Value>>);
    impl symbiotic_ai_runtime::ResponseCache for OneEntry {
        fn load(
            &self,
            _: &symbiotic_ai_runtime::CacheEntry<'_>,
        ) -> Result<Option<serde_json::Value>, ModelError> {
            Ok(self.0.lock().unwrap().clone())
        }
        fn store(
            &self,
            _: &symbiotic_ai_runtime::CacheEntry<'_>,
            value: &serde_json::Value,
        ) -> Result<(), ModelError> {
            *self.0.lock().unwrap() = Some(value.clone());
            Ok(())
        }
    }
    let state = private_tempdir();
    let runtime = persistent(state.path());
    let raw = Loopback::new(unique_identity());
    let cache = Arc::new(OneEntry(std::sync::Mutex::new(None)));
    for settings in [
        json!({"endpoint": "one"}),
        json!({"endpoint": "two"}),
        json!({"endpoint": "two", "thinking": "enabled"}),
        json!({"endpoint": "two", "effort": "high"}),
    ] {
        let mut configured = raw.clone();
        configured.descriptor.metadata = settings;
        let provider = runtime
            .chat(
                binding(configured)
                    .with_policy(policy())
                    .with_response_cache(ResponseCacheMode::Custom(cache.clone())),
            )
            .unwrap();
        let mut variants = vec![request("same"), request("different")];
        let mut limited = request("same");
        limited.max_output_tokens = Some(16);
        variants.push(limited);
        let mut warmer = request("same");
        warmer.temperature = Some(0.5);
        variants.push(warmer);
        for request in variants {
            let first = provider.chat(request.clone()).await.unwrap();
            let hit = provider.chat(request).await.unwrap();
            assert_eq!(first.trace.request_hash, hit.trace.request_hash);
            assert_eq!(
                hit.trace.cache.response_cache,
                symbiotic_trace::CacheStatus::Hit
            );
        }
    }
    assert_eq!(raw.calls.load(Ordering::SeqCst), 16);
}

#[tokio::test]
async fn independent_accounts_and_runtimes_do_not_share_rate_budget() {
    let state = private_tempdir();
    let runtime = persistent(state.path());
    let other_state = private_tempdir();
    let other_runtime = persistent(other_state.path());
    let raw = Loopback::new(unique_identity());
    let policy = ModelQueueConfig {
        requests_per_minute: Some(1),
        ..policy()
    };
    let first = runtime
        .chat(binding(raw.clone()).with_policy(policy.clone()))
        .unwrap();
    first.chat(request("first")).await.unwrap();
    for (owner, tenant, account) in [
        (&runtime, "tenant", "other-account"),
        (&runtime, "other-tenant", "account"),
        (&other_runtime, "tenant", "account"),
    ] {
        let provider = owner
            .chat(
                binding(raw.clone())
                    .with_policy(policy.clone())
                    .with_identity(symbiotic_ai_runtime::BindingIdentity::new(
                        tenant, "provider", "1", account,
                    )),
            )
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), provider.chat(request("next")))
            .await
            .expect("independent account has its own initial budget")
            .unwrap();
    }
    assert_eq!(raw.calls.load(Ordering::SeqCst), 4);
}

#[tokio::test]
async fn explicit_account_sharing_enforces_one_rate_budget_across_tenants() {
    let state = private_tempdir();
    let runtime = persistent(state.path());
    let raw = Loopback::new(unique_identity());
    let policy = ModelQueueConfig {
        requests_per_minute: Some(1),
        ..policy()
    };
    let shared = symbiotic_ai_runtime::AccountSharingKey::new("shared-account");
    let first = runtime
        .chat(
            binding(raw.clone())
                .with_policy(policy.clone())
                .with_account_sharing(shared.clone()),
        )
        .unwrap();
    let second = runtime
        .chat(
            binding(raw.clone())
                .with_policy(policy)
                .with_account_sharing(shared)
                .with_identity(symbiotic_ai_runtime::BindingIdentity::new(
                    "other-tenant",
                    "provider",
                    "1",
                    "account",
                )),
        )
        .unwrap();
    first.chat(request("first")).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(150), second.chat(request("second")))
            .await
            .is_err()
    );
    assert_eq!(raw.calls.load(Ordering::SeqCst), 1);
}

fn registry_config() -> serde_json::Value {
    let mut config: serde_json::Value =
        serde_json::from_str(include_str!("../../../examples/model-registry.json")).unwrap();
    config["accounts"] = json!([{ "id": "policy", "policy": policy() }]);
    config["bindings"] = json!([{
        "identity": {"tenant": "tenant", "provider": "provider", "revision": "1", "account": "account"},
        "model": "example-chat-alias", "endpoint": "http://127.0.0.1:9/v1", "secret_ref": null,
        "account_policy": "policy", "account_sharing_key": null,
        "limits": {"max_request_bytes": 65536, "max_response_bytes": 65536, "max_output_tokens": 1024},
        "settings": {"thinking": null, "reasoning_effort": null, "dimensions": null, "served_model": null}
    }]);
    config
}
fn registry_runtime() -> Runtime {
    let config = registry_config();
    let registry = symbiotic_ai_runtime::model::ModelRegistry::from_json(
        &serde_json::to_vec(&config).unwrap(),
    )
    .unwrap();
    Runtime::open(RuntimeConfig {
        registry: Some(Arc::new(registry)),
        ..RuntimeConfig::default()
    })
    .unwrap()
}
struct NoCredentials;
#[async_trait]
impl symbiotic_ai_runtime::model::CredentialResolver for NoCredentials {
    async fn resolve_auth(
        &self,
        _: &ProviderAuthMode,
    ) -> Result<symbiotic_ai_runtime::model::ResolvedAuth, ModelError> {
        panic!("keyless binding must not resolve a secret")
    }
}
#[tokio::test]
async fn configured_embedding_and_classifier_apply_finite_transport_limits() {
    use symbiotic_ai_runtime::{ConfiguredProvider, model::ModelRegistry};
    use symbiotic_core::{ProviderPrincipalId, TenantId};
    for (adapter, operation, endpoint) in [
        (
            "gemini_embedding",
            "embedding",
            "https://generativelanguage.googleapis.com/v1beta",
        ),
        ("jev_classifier", "classify", "http://127.0.0.1:9"),
    ] {
        let mut config = registry_config();
        config["models"][0]["adapter"] = json!(adapter);
        config["models"][0]["operations"] = json!([operation]);
        config["models"][0]["identity"]["operation"] = json!(operation);
        if adapter == "gemini_embedding" {
            config["models"][0]["identity"]["operator"] = json!("google");
            config["bindings"][0]["settings"]["dimensions"] = json!(3);
        }
        config["bindings"][0]["endpoint"] = json!(endpoint);
        assert!(
            ModelRegistry::from_json(&serde_json::to_vec(&config).unwrap()).is_err(),
            "unsupported non-chat output option is refused"
        );
        config["bindings"][0]["limits"]["max_output_tokens"] = serde_json::Value::Null;
        let registry = ModelRegistry::from_json(&serde_json::to_vec(&config).unwrap()).unwrap();
        let runtime = Runtime::open(RuntimeConfig {
            registry: Some(Arc::new(registry)),
            ..RuntimeConfig::default()
        })
        .unwrap();
        let installed = runtime
            .configured_provider(
                &TenantId("tenant".into()),
                &ProviderPrincipalId("provider".into()),
                &NoCredentials,
            )
            .await
            .unwrap();
        let descriptor = match &installed {
            ConfiguredProvider::Embedding(provider) => provider.descriptor(),
            ConfiguredProvider::Classifier(provider) => provider.descriptor(),
            ConfiguredProvider::Chat(_) | ConfiguredProvider::Rerank(_) => {
                panic!("wrong installed adapter")
            }
        };
        assert_eq!(descriptor.metadata["max_request_bytes"], 65536);
        assert_eq!(descriptor.metadata["max_response_bytes"], 65536);
    }
}
#[tokio::test]
async fn configured_registry_builds_keyless_adapter_and_refuses_unknown_tenants() {
    use symbiotic_core::{ProviderPrincipalId, TenantId};
    let runtime = registry_runtime();
    let provider = runtime
        .configured_provider(
            &TenantId("tenant".into()),
            &ProviderPrincipalId("provider".into()),
            &NoCredentials,
        )
        .await
        .unwrap();
    let symbiotic_ai_runtime::ConfiguredProvider::Chat(provider) = provider else {
        panic!("chat configured");
    };
    assert_eq!(provider.descriptor().identity.model.0, "example-model");
    assert!(
        runtime
            .configured_provider(
                &TenantId("other".into()),
                &ProviderPrincipalId("provider".into()),
                &NoCredentials
            )
            .await
            .is_err()
    );
}
#[test]
fn registry_refuses_transport_or_policy_overrides_and_unconfigured_policy() {
    use symbiotic_ai_runtime::model::OpenAiCompatibleChatProvider;
    use symbiotic_core::{ProviderPrincipalId, TenantId};
    let runtime = registry_runtime();
    let tenant = TenantId("tenant".into());
    let principal = ProviderPrincipalId("provider".into());
    let raw = |endpoint| {
        OpenAiCompatibleChatProvider::new("example", "example-model", endpoint, "")
            .with_request_limit(65536)
            .with_response_limit(65536)
            .with_output_limit(1024)
    };
    let binding = runtime
        .registry_binding(&tenant, &principal, raw("http://127.0.0.1:10/v1"))
        .unwrap();
    assert!(matches!(
        runtime.chat(binding),
        Err(ModelError::InvalidRequest(
            symbiotic_core::DiagnosticCode::EffectiveTransportDiffersFromConfiguredBinding
        ))
    ));
    let binding = runtime
        .registry_binding(&tenant, &principal, raw("http://127.0.0.1:9/v1"))
        .unwrap()
        .with_policy(ModelQueueConfig {
            max_in_flight: 7,
            ..policy()
        });
    assert!(runtime.chat(binding).is_err());
    assert!(
        Runtime::in_memory()
            .unwrap()
            .chat(binding_without_policy(Loopback::new(unique_identity())))
            .is_err()
    );
}
fn binding_without_policy<P>(provider: P) -> ModelBinding<P> {
    binding(provider)
}

#[test]
fn supported_http_bindings_default_byte_limits_preserve_overrides_and_refuse_zero() {
    use symbiotic_ai_runtime::model::{
        DEFAULT_MAX_REQUEST_BYTES, DEFAULT_MAX_RESPONSE_BYTES, GeminiEmbeddingProvider,
        JevClassifierProvider, OpenAiCompatibleChatProvider,
    };
    let runtime = Runtime::in_memory().unwrap();
    for request_limit in [None, Some(0), Some(1024)] {
        for response_limit in [None, Some(0), Some(1024)] {
            let valid = request_limit != Some(0) && response_limit != Some(0);
            let mut chat = OpenAiCompatibleChatProvider::new(
                "synthetic",
                "synthetic",
                "http://127.0.0.1:9",
                "",
            );
            let mut embedding = GeminiEmbeddingProvider::new("gemini", "synthetic", "", 3);
            let mut classifier =
                JevClassifierProvider::new("synthetic", "synthetic", "http://127.0.0.1:9", "");
            if let Some(limit) = request_limit {
                chat = chat.with_request_limit(limit);
                embedding = embedding.with_request_limit(limit);
                classifier = classifier.with_request_limit(limit);
            }
            if let Some(limit) = response_limit {
                chat = chat.with_response_limit(limit);
                embedding = embedding.with_response_limit(limit);
                classifier = classifier.with_response_limit(limit);
            }
            for result in [
                runtime
                    .chat(binding(chat).with_policy(policy()))
                    .map(|provider| provider.descriptor().metadata.clone()),
                runtime
                    .embedding(binding(embedding).with_policy(policy()))
                    .map(|provider| provider.descriptor().metadata.clone()),
                runtime
                    .classifier(binding(classifier).with_policy(policy()))
                    .map(|provider| provider.descriptor().metadata.clone()),
            ] {
                assert_eq!(result.is_ok(), valid);
                if let Ok(metadata) = result {
                    assert_eq!(
                        metadata["max_request_bytes"],
                        request_limit.unwrap_or(DEFAULT_MAX_REQUEST_BYTES)
                    );
                    assert_eq!(
                        metadata["max_response_bytes"],
                        response_limit.unwrap_or(DEFAULT_MAX_RESPONSE_BYTES)
                    );
                }
            }
        }
    }
    let zero_output =
        OpenAiCompatibleChatProvider::new("synthetic", "synthetic", "http://127.0.0.1:9", "")
            .with_request_limit(1024)
            .with_response_limit(1024)
            .with_output_limit(0);
    assert!(
        runtime
            .chat(binding(zero_output).with_policy(policy()))
            .is_err()
    );
}
#[cfg(not(debug_assertions))]
#[test]
fn production_request_debug_dir_is_refused_at_bind_time() {
    let dir = tempfile::tempdir().unwrap();
    let dumps = dir.path().join("request-dumps");
    let raw = Loopback::new(unique_identity());
    let result = Runtime::in_memory()
        .unwrap()
        .chat(binding(raw).with_policy(ModelQueueConfig {
            request_debug_dir: Some(dumps.clone()),
            ..policy()
        }));
    assert!(matches!(result, Err(ModelError::InvalidRequest(_))));
    assert!(!dumps.exists());
}

#[test]
fn raw_and_registry_bindings_refuse_the_same_invalid_chat_settings() {
    use symbiotic_ai_runtime::model::{ModelRegistry, OpenAiCompatibleChatProvider, ThinkingMode};
    for (thinking, effort) in [(Some(ThinkingMode::Disabled), "low"), (None, "")] {
        let mut config = registry_config();
        config["bindings"][0]["settings"]["thinking"] = json!(thinking);
        config["bindings"][0]["settings"]["reasoning_effort"] = json!(effort);
        let configured_error =
            ModelRegistry::from_json(&serde_json::to_vec(&config).unwrap()).unwrap_err();
        let raw = OpenAiCompatibleChatProvider::new(
            "example",
            "example-model",
            "http://127.0.0.1:9/v1",
            "",
        )
        .with_request_limit(65536)
        .with_response_limit(65536)
        .with_output_limit(1024)
        .with_thinking(thinking)
        .with_reasoning_effort(effort);
        let raw_error = match Runtime::in_memory()
            .unwrap()
            .chat(binding(raw).with_policy(policy()))
        {
            Err(error) => error,
            Ok(_) => panic!("invalid raw binding was accepted"),
        };
        assert!(matches!(raw_error, ModelError::InvalidRequest(_)));
        assert!(matches!(raw_error, ModelError::InvalidRequest(symbiotic_core::DiagnosticCode::UnsupportedChatSettingsReasoningEffortRequiresThinkingAndMustBeNonempty)));
        assert!(matches!(configured_error, ModelError::InvalidRequest(symbiotic_core::DiagnosticCode::UnsupportedChatSettingsReasoningEffortRequiresThinkingAndMustBeNonempty)));
    }
}

#[tokio::test]
async fn injected_credential_results_without_a_boundary_are_refused_before_bookkeeping() {
    const KEY: &str = "synthetic-unprotected-credential-741";
    #[derive(Clone)]
    struct Unprotected {
        inner: Loopback,
        fail: bool,
    }
    impl ModelProvider for Unprotected {
        fn descriptor(&self) -> &ProviderDescriptor {
            self.inner.descriptor()
        }
    }
    #[async_trait]
    impl ChatProvider for Unprotected {
        async fn chat(&self, request: ChatRequest) -> Result<ChatResponse, ModelError> {
            if self.fail {
                return Err(ModelError::Auth(
                    symbiotic_core::DiagnosticCode::AuthenticationRejected,
                ));
            }
            let mut response = self.inner.chat(request).await?;
            response.text = KEY.into();
            response.raw_provider_response = Some(json!({"ignored": KEY}));
            Ok(response)
        }
    }
    for registry_path in [false, true] {
        for fail in [false, true] {
            let state = tempfile::tempdir().unwrap();
            let receipts = Arc::new(InMemoryReceiptSink::default());
            let traces = Arc::new(symbiotic_trace::InMemoryTraceSink::default());
            let runtime = Runtime::open(RuntimeConfig {
                state_dir: Some(state.path().join("state")),
                registry: Some(Arc::new(
                    symbiotic_ai_runtime::model::ModelRegistry::from_json(
                        &serde_json::to_vec(&registry_config()).unwrap(),
                    )
                    .unwrap(),
                )),
                receipt_sink: Some(receipts.clone()),
                trace_sink: Some(traces.clone()),
                ..RuntimeConfig::default()
            })
            .unwrap();
            let descriptor = symbiotic_ai_runtime::model::OpenAiCompatibleChatProvider::new(
                "example",
                "example-model",
                "http://127.0.0.1:9/v1",
                KEY,
            )
            .with_request_limit(65536)
            .with_response_limit(65536)
            .with_output_limit(1024)
            .descriptor()
            .clone();
            let mut inner = Loopback::new(descriptor.identity.clone());
            inner.descriptor = descriptor;
            let raw = Unprotected { inner, fail };
            let binding = if registry_path {
                runtime
                    .registry_binding(
                        &symbiotic_core::TenantId("tenant".into()),
                        &symbiotic_core::ProviderPrincipalId("provider".into()),
                        raw,
                    )
                    .unwrap()
            } else {
                binding(raw).with_policy(policy())
            };
            let provider = runtime.chat(binding).unwrap();
            let error = provider.chat(request("ordinary prompt")).await.unwrap_err();
            assert!(matches!(error, ModelError::Provider(_)));
            for text in [
                error.to_string(),
                serde_json::to_string(&receipts.receipts()).unwrap(),
                serde_json::to_string(&traces.records()).unwrap(),
            ] {
                assert!(!text.contains(KEY));
            }
            let mut dirs = vec![state.path().to_path_buf()];
            while let Some(dir) = dirs.pop() {
                for entry in std::fs::read_dir(dir).unwrap() {
                    let path = entry.unwrap().path();
                    if path.is_dir() {
                        dirs.push(path);
                    } else {
                        let bytes = std::fs::read(path).unwrap();
                        assert!(!bytes.windows(KEY.len()).any(|part| part == KEY.as_bytes()));
                    }
                }
            }
        }
    }
}

#[test]
fn credential_adapter_validation_errors_have_no_text_payload() {
    use symbiotic_ai_runtime::model::{CredentialBoundary, OpenAiCompatibleChatProvider};
    use symbiotic_core::DiagnosticCode;
    const KEY: &str = "synthetic-validation-key-741";
    #[derive(Clone)]
    struct InvalidAdapter(OpenAiCompatibleChatProvider);
    impl ModelProvider for InvalidAdapter {
        fn descriptor(&self) -> &ProviderDescriptor {
            self.0.descriptor()
        }
        fn credential_fingerprint(&self) -> Option<String> {
            self.0.credential_fingerprint()
        }
        fn credential_boundary(&self) -> Option<&CredentialBoundary> {
            self.0.credential_boundary()
        }
        fn validate_configuration(&self) -> Result<(), ModelError> {
            // The associated compile-fail test proves this cannot contain KEY.
            Err(ModelError::Auth(DiagnosticCode::AuthenticationRejected))
        }
    }
    #[async_trait]
    impl ChatProvider for InvalidAdapter {
        async fn chat(&self, _: ChatRequest) -> Result<ChatResponse, ModelError> {
            panic!("invalid configuration must refuse before dispatch")
        }
    }
    let adapter = InvalidAdapter(OpenAiCompatibleChatProvider::new(
        "fixture",
        "fixture",
        "http://localhost",
        KEY,
    ));
    let result = Runtime::in_memory()
        .unwrap()
        .chat(binding(adapter).with_policy(policy()));
    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("invalid configuration was accepted"),
    };
    assert!(matches!(
        error,
        ModelError::Auth(DiagnosticCode::AuthenticationRejected)
    ));
    assert!(!format!("{error:?} {error}").contains(KEY));
}

#[tokio::test]
async fn restored_stopped_and_exhausted_dead_items_cannot_surface_stored_text() {
    const KEY: &str = "synthetic-stored-provider-key-741";
    for stopped in [true, false] {
        let dir = private_tempdir();
        let failure = if stopped {
            ModelError::Auth(symbiotic_core::DiagnosticCode::AuthenticationRejected)
        } else {
            ModelError::Unavailable(symbiotic_core::DiagnosticCode::HttpUnavailable)
        };
        let broken = Loopback::new(unique_identity()).failing(failure.clone());
        let bind = || {
            binding(broken.clone())
                .with_policy(policy())
                .with_response_cache(ResponseCacheMode::Default)
        };
        let runtime = persistent(dir.path());
        let provider = runtime.chat(bind()).unwrap();
        let first = provider.chat(request("restored")).await.unwrap_err();
        assert_eq!(
            std::mem::discriminant(&first),
            std::mem::discriminant(&failure)
        );
        drop(provider);
        drop(runtime);
        let database = dir.path().join(symbiotic_ai_runtime::QUEUE_DATABASE);
        let conn = rusqlite::Connection::open(database).unwrap();
        let status: String = conn
            .query_row("select status from queue_items", [], |row| row.get(0))
            .unwrap();
        assert_eq!(status, if stopped { "stopped" } else { "dead" });
        let restored = persistent(dir.path())
            .chat(bind())
            .unwrap()
            .chat(request("restored"))
            .await
            .unwrap_err();
        assert_eq!(
            std::mem::discriminant(&restored),
            std::mem::discriminant(&failure)
        );
        assert!(!format!("{restored:?} {restored}").contains(KEY));
        if !stopped {
            assert_eq!(
                restored.code(),
                symbiotic_core::DiagnosticCode::AttemptBudgetExhausted
            );
        }
        // Corrupt current state: decoding must reject unknown codes without repeating the bytes.
        conn.execute("update queue_items set last_error = ?1", [KEY])
            .unwrap();
        drop(conn);
        let provider = persistent(dir.path()).chat(bind()).unwrap();
        let restored = provider.chat(request("restored")).await.unwrap_err();
        assert!(matches!(restored, ModelError::Queue(_)), "{restored:?}");
        assert!(!format!("{restored:?} {restored}").contains(KEY));
        assert_eq!(broken.calls.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn execution_timeout_returns_receipt_without_telemetry_and_lookup_survives_restart() {
    let dir = private_tempdir();
    let runtime = persistent(dir.path());
    let raw = Loopback::new(unique_identity()).failing(ModelError::Timeout(
        symbiotic_core::DiagnosticCode::HttpTimeout,
    ));
    let calls = raw.calls.clone();
    let identity = binding(raw.clone()).identity.unwrap();
    let error = runtime
        .execute_chat(
            binding(raw)
                .with_policy(policy())
                .with_response_cache(ResponseCacheMode::Off),
            "explicit-timeout-invocation",
            request("uncertain transport"),
        )
        .await
        .unwrap_err();
    assert!(matches!(error.source, ModelError::Timeout(_)));
    let status = error.attempt.unwrap().unwrap();
    assert_eq!(status.state, symbiotic_ai_runtime::SpendState::Unknown);
    assert!(!status.output_available);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    drop(runtime);

    let restarted = persistent(dir.path());
    assert_eq!(
        restarted
            .invocation_status(&identity, None, "explicit-timeout-invocation")
            .unwrap()
            .unwrap(),
        status
    );
    assert!(
        restarted
            .spend_receipt(&status.reference)
            .unwrap()
            .is_some()
    );
    let other_account =
        symbiotic_ai_runtime::BindingIdentity::new("other-tenant", "provider", "1", "account");
    assert!(
        restarted
            .invocation_status(&other_account, None, "explicit-timeout-invocation")
            .unwrap()
            .is_none()
    );
    restarted
        .reconcile_spend(
            &status.reference,
            symbiotic_ai_runtime::SpendState::Released,
            None,
        )
        .unwrap();
    assert_eq!(
        restarted
            .invocation_status(&identity, None, "explicit-timeout-invocation")
            .unwrap()
            .unwrap()
            .state,
        symbiotic_ai_runtime::SpendState::Released
    );
}

#[tokio::test]
async fn execution_success_returns_durable_receipt_and_same_invocation_recovers_output() {
    let dir = private_tempdir();
    let runtime = persistent(dir.path());
    let raw = Loopback::new(unique_identity());
    let calls = raw.calls.clone();
    let configured = binding(raw)
        .with_policy(policy())
        .with_response_cache(ResponseCacheMode::Off);
    let first = runtime
        .execute_chat(
            configured.clone(),
            "explicit-success",
            request("paid output"),
        )
        .await
        .unwrap();
    let status = first.attempt.unwrap().unwrap();
    assert!(status.output_available);
    // Loopback reports no usage: a successful output never fabricates zero charge.
    assert_eq!(status.state, symbiotic_ai_runtime::SpendState::Unknown);
    drop(runtime);
    let restarted = persistent(dir.path());
    let recovered = restarted
        .execute_chat(configured, "explicit-success", request("paid output"))
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(&recovered.output).unwrap(),
        serde_json::to_value(&first.output).unwrap()
    );
    assert_eq!(recovered.attempt.unwrap().unwrap(), status);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn execution_distinct_explicit_invocations_do_not_share_an_accepted_attempt() {
    let dir = private_tempdir();
    let runtime = persistent(dir.path());
    let raw = Loopback::new(unique_identity());
    let calls = raw.calls.clone();
    let configured = binding(raw)
        .with_policy(policy())
        .with_response_cache(ResponseCacheMode::Off);
    let first = runtime
        .execute_chat(
            configured.clone(),
            "first-invocation",
            request("same input"),
        )
        .await
        .unwrap();
    let second = runtime
        .execute_chat(configured, "second-invocation", request("same input"))
        .await
        .unwrap();
    assert_ne!(
        first.attempt.unwrap().unwrap().reference,
        second.attempt.unwrap().unwrap().reference
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn execution_reusing_explicit_invocation_with_changed_inputs_is_refused() {
    let dir = private_tempdir();
    let runtime = persistent(dir.path());
    let raw = Loopback::new(unique_identity());
    let calls = raw.calls.clone();
    let configured = binding(raw).with_policy(policy());
    runtime
        .execute_chat(
            configured.clone(),
            "fixed-invocation",
            request("accepted input"),
        )
        .await
        .unwrap();
    // Prime a valid cache entry for the changed input under another invocation.
    runtime
        .execute_chat(
            configured.clone(),
            "cache-primer",
            request("different input"),
        )
        .await
        .unwrap();
    let error = runtime
        .execute_chat(configured, "fixed-invocation", request("different input"))
        .await
        .unwrap_err();
    assert!(matches!(
        error.source,
        ModelError::Queue(symbiotic_core::DiagnosticCode::SpendReconciliationRequired)
    ));
    assert!(error.attempt.unwrap().is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn execution_known_zero_failure_returns_released_attempt_receipt() {
    let dir = private_tempdir();
    let runtime = persistent(dir.path());
    let raw = Loopback::new(unique_identity()).failing(ModelError::Auth(
        symbiotic_core::DiagnosticCode::InvalidConfiguration,
    ));
    let identity = binding(raw.clone()).identity.unwrap();
    let error = runtime
        .execute_chat(
            binding(raw)
                .with_policy(policy())
                .with_response_cache(ResponseCacheMode::Off),
            "known-zero-invocation",
            request("refused before transport"),
        )
        .await
        .unwrap_err();
    let status = error.attempt.unwrap().unwrap();
    assert_eq!(status.state, symbiotic_ai_runtime::SpendState::Released);
    assert!(!status.output_available);
    assert_eq!(
        runtime
            .invocation_status(&identity, None, "known-zero-invocation")
            .unwrap()
            .unwrap(),
        status
    );
}

struct ReconcileAndReserveOnFailure {
    ledger: Arc<symbiotic_ai_runtime::spend::SqliteSpendLedger>,
    newer_reference: symbiotic_ai_runtime::SpendReceiptRef,
}

#[async_trait]
impl symbiotic_ai_runtime::QueueReceiptSink for ReconcileAndReserveOnFailure {
    async fn record_receipt(&self, receipt: symbiotic_ai_runtime::QueueReceipt) {
        if receipt.status != ReceiptStatus::Failed {
            return;
        }
        let ledger = self.ledger.clone();
        let newer_reference = self.newer_reference.clone();
        tokio::task::spawn_blocking(move || {
            use symbiotic_ai_runtime::{SpendLedger, SpendState};
            let reference = receipt.spend_receipt.unwrap();
            let old = ledger.receipt(&reference).unwrap().unwrap();
            ledger
                .finish(&reference, SpendState::Released, None, None, None)
                .unwrap();
            let mut newer = old.reservation;
            newer.reference = newer_reference;
            assert!(ledger.reserve(&newer).unwrap());
        })
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn execution_error_keeps_its_exact_receipt_when_a_new_attempt_is_accepted_before_return() {
    let dir = private_tempdir();
    let runtime = persistent(dir.path());
    let raw = Loopback::new(unique_identity()).failing(ModelError::Timeout(
        symbiotic_core::DiagnosticCode::HttpTimeout,
    ));
    let identity = binding(raw.clone()).identity.unwrap();
    let newer_reference =
        symbiotic_ai_runtime::SpendReceiptRef::new("newer-accepted-attempt").unwrap();
    let sink = Arc::new(ReconcileAndReserveOnFailure {
        ledger: Arc::new(
            symbiotic_ai_runtime::spend::SqliteSpendLedger::open(
                &dir.path().join(symbiotic_ai_runtime::QUEUE_DATABASE),
            )
            .unwrap(),
        ),
        newer_reference: newer_reference.clone(),
    });
    let error = runtime
        .execute_chat(
            binding(raw)
                .with_policy(ModelQueueConfig {
                    logical_retry_attempts: 2,
                    ..policy()
                })
                .with_receipt_sink(sink)
                .with_response_cache(ResponseCacheMode::Off),
            "racing-invocation",
            request("original attempt"),
        )
        .await
        .unwrap_err();
    assert!(matches!(error.source, ModelError::Timeout(_)));
    let exact = error.attempt.unwrap().unwrap();
    assert_ne!(exact.reference, newer_reference);
    assert_eq!(exact.state, symbiotic_ai_runtime::SpendState::Released);
    let latest = runtime
        .invocation_status(&identity, None, "racing-invocation")
        .unwrap()
        .unwrap();
    assert_eq!(latest.reference, newer_reference);
    assert_eq!(latest.state, symbiotic_ai_runtime::SpendState::Unknown);
}

#[tokio::test]
async fn execution_explicit_invocations_are_isolated_by_binding_inside_a_shared_account() {
    let dir = private_tempdir();
    let runtime = persistent(dir.path());
    let raw = Loopback::new(unique_identity());
    let calls = raw.calls.clone();
    let sharing = symbiotic_ai_runtime::AccountSharingKey("explicit-shared-account".into());
    let identities = [
        symbiotic_ai_runtime::BindingIdentity::new("tenant-a", "provider-a", "1", "account"),
        symbiotic_ai_runtime::BindingIdentity::new("tenant-b", "provider-a", "1", "account"),
        symbiotic_ai_runtime::BindingIdentity::new("tenant-a", "provider-b", "1", "account"),
        symbiotic_ai_runtime::BindingIdentity::new("tenant-a", "provider-a", "2", "account"),
    ];
    let mut statuses = Vec::new();
    for identity in &identities {
        let result = runtime
            .execute_chat(
                ModelBinding::new(raw.clone())
                    .with_identity(identity.clone())
                    .with_account_sharing(sharing.clone())
                    .with_policy(policy())
                    .with_response_cache(ResponseCacheMode::Off),
                "same-caller-invocation",
                request("same input"),
            )
            .await
            .unwrap();
        let status = result.attempt.unwrap().unwrap();
        assert!(
            statuses.iter().all(
                |old: &symbiotic_ai_runtime::ExecutionAttemptStatus| old.reference
                    != status.reference
            )
        );
        statuses.push(status);
    }
    assert_eq!(calls.load(Ordering::SeqCst), identities.len());
    for (identity, status) in identities.iter().zip(statuses) {
        assert_eq!(
            runtime
                .invocation_status(identity, Some(&sharing), "same-caller-invocation")
                .unwrap()
                .unwrap(),
            status
        );
    }
}

#[tokio::test]
async fn execution_explicit_invocations_bypass_cache_and_recover_their_own_output_after_restart() {
    let dir = private_tempdir();
    let runtime = persistent(dir.path());
    let raw = Loopback::new(unique_identity());
    let configured = binding(raw.clone()).with_policy(policy());
    let primer = runtime
        .chat(configured.clone())
        .unwrap()
        .chat(request("X"))
        .await
        .unwrap();
    let selected = runtime
        .execute_chat(configured.clone(), "cached-invocation", request("X"))
        .await
        .unwrap();
    assert_eq!(
        selected.output.trace.cache.response_cache,
        symbiotic_trace::CacheStatus::NotApplicable
    );
    assert_ne!(
        selected.attempt.unwrap().unwrap().reference,
        serde_json::from_value::<symbiotic_ai_runtime::SpendReceiptRef>(
            primer.trace.metadata["spend_receipt"].clone()
        )
        .unwrap()
    );
    let identity = configured.identity.as_ref().unwrap();
    assert!(
        runtime
            .invocation_status(identity, None, "cached-invocation")
            .unwrap()
            .unwrap()
            .output_available
    );
    let error = runtime
        .execute_chat(configured.clone(), "cached-invocation", request("Y"))
        .await
        .unwrap_err();
    assert!(matches!(
        error.source,
        ModelError::Queue(symbiotic_core::DiagnosticCode::SpendReconciliationRequired)
    ));
    assert_eq!(raw.calls.load(Ordering::SeqCst), 2);
    // Evict the optional response cache while retaining this invocation's answer.
    DirResponseCache::new(dir.path().join(symbiotic_ai_runtime::RESPONSES_DIR))
        .purge(|_| true)
        .unwrap();
    drop(runtime);
    let restarted = persistent(dir.path());
    let recovered = restarted
        .execute_chat(configured, "cached-invocation", request("X"))
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(recovered.output).unwrap(),
        serde_json::to_value(selected.output).unwrap()
    );
    assert_eq!(raw.calls.load(Ordering::SeqCst), 2);
    let conn =
        rusqlite::Connection::open(dir.path().join(symbiotic_ai_runtime::QUEUE_DATABASE)).unwrap();
    assert_eq!(
        conn.query_row("SELECT used FROM spend_accounts", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        2
    );
    assert_eq!(
        conn.query_row("SELECT count(*) FROM spend_receipts", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        2
    );
}

#[tokio::test(start_paused = true)]
async fn spend_credential_rotation_cannot_bypass_an_unknown_timeout() {
    let dir = private_tempdir();
    let runtime = persistent(dir.path());
    let mut original = Loopback::new(unique_identity()).slow(Duration::from_secs(2));
    original.credential = Some(Arc::new(
        symbiotic_ai_runtime::model::OpenAiCompatibleChatProvider::new(
            "loopback",
            "synthetic",
            "http://localhost",
            KEY_A,
        ),
    ));
    let mut rotated = original.clone().slow(Duration::ZERO);
    rotated.credential = Some(Arc::new(
        symbiotic_ai_runtime::model::OpenAiCompatibleChatProvider::new(
            "loopback",
            "synthetic",
            "http://localhost",
            KEY_B,
        ),
    ));
    let configure = |raw| {
        binding(raw)
            .with_policy(ModelQueueConfig {
                request_timeout_seconds: Some(1),
                ..policy()
            })
            .with_response_cache(ResponseCacheMode::Off)
    };
    let first = runtime.chat(configure(original.clone())).unwrap();
    assert!(matches!(
        first.chat(request("rotation timeout")).await,
        Err(ModelError::Timeout(_))
    ));
    let second = runtime.chat(configure(rotated)).unwrap();
    let error = second.chat(request("rotation timeout")).await.unwrap_err();
    assert!(
        matches!(
            error,
            ModelError::Queue(symbiotic_core::DiagnosticCode::SpendReconciliationRequired)
        ),
        "{error:?}"
    );
    assert_eq!(original.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn execution_reconciled_invocation_retries_despite_a_warm_cache() {
    let dir = private_tempdir();
    let runtime = persistent(dir.path());
    let mut raw = Loopback::new(unique_identity()).failing(ModelError::Timeout(
        symbiotic_core::DiagnosticCode::HttpTimeout,
    ));
    let configured = binding(raw.clone()).with_policy(ModelQueueConfig {
        logical_retry_attempts: 2,
        retry_attempts: 2,
        ..policy()
    });
    let error = runtime
        .execute_chat(configured.clone(), "A", request("X"))
        .await
        .unwrap_err();
    let first = error.attempt.unwrap().unwrap();
    runtime
        .reconcile_spend(
            &first.reference,
            symbiotic_ai_runtime::SpendState::Released,
            None,
        )
        .unwrap();
    raw.fail = None;
    let configured = binding(raw.clone()).with_policy(ModelQueueConfig {
        logical_retry_attempts: 2,
        retry_attempts: 2,
        ..policy()
    });
    // An implicit invocation warms the cache for exactly the same binding and input.
    runtime
        .chat(configured.clone())
        .unwrap()
        .chat(request("X"))
        .await
        .unwrap();
    let retried = runtime
        .execute_chat(configured, "A", request("X"))
        .await
        .unwrap();
    assert_ne!(retried.attempt.unwrap().unwrap().reference, first.reference);
    assert_eq!(raw.calls.load(Ordering::SeqCst), 3);
}

struct RefuseCacheAccess;
impl symbiotic_ai_runtime::ResponseCache for RefuseCacheAccess {
    fn load(
        &self,
        _: &symbiotic_ai_runtime::model::CacheEntry<'_>,
    ) -> Result<Option<serde_json::Value>, ModelError> {
        panic!("explicit invocations must never read result cache");
    }
    fn store(
        &self,
        _: &symbiotic_ai_runtime::model::CacheEntry<'_>,
        _: &serde_json::Value,
    ) -> Result<(), ModelError> {
        panic!("explicit invocations must never write result cache");
    }
}

#[tokio::test]
async fn execution_with_invocation_never_accesses_custom_result_cache() {
    let dir = private_tempdir();
    let runtime = persistent(dir.path());
    let raw = Loopback::new(unique_identity());
    let configured = binding(raw.clone())
        .with_policy(policy())
        .with_invocation("A")
        .with_response_cache(ResponseCacheMode::Custom(Arc::new(RefuseCacheAccess)));
    let first = runtime
        .chat(configured.clone())
        .unwrap()
        .chat(request("X"))
        .await
        .unwrap();
    let recovered = runtime
        .chat(configured)
        .unwrap()
        .chat(request("X"))
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(first).unwrap(),
        serde_json::to_value(recovered).unwrap()
    );
    assert_eq!(raw.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn execution_changed_inputs_after_release_reports_no_accepted_attempt() {
    let dir = private_tempdir();
    let runtime = persistent(dir.path());
    let raw = Loopback::new(unique_identity()).failing(ModelError::Auth(
        symbiotic_core::DiagnosticCode::InvalidConfiguration,
    ));
    let configured = binding(raw.clone()).with_policy(policy());
    let first = runtime
        .execute_chat(configured.clone(), "A", request("X"))
        .await
        .unwrap_err();
    assert_eq!(
        first.attempt.unwrap().unwrap().state,
        symbiotic_ai_runtime::SpendState::Released
    );
    let refused = runtime
        .execute_chat(configured, "A", request("Y"))
        .await
        .unwrap_err();
    assert_eq!(
        refused.source.code(),
        symbiotic_core::DiagnosticCode::SpendReconciliationRequired
    );
    assert!(refused.attempt.unwrap().is_none());
    assert_eq!(raw.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn fdn_idle_runtime_deletes_expired_recovery_without_reopening() {
    let dir = private_tempdir();
    let runtime = Runtime::open(RuntimeConfig {
        clock: test_clock::clock(),
        state_dir: Some(dir.path().to_path_buf()),
        retention: Duration::from_secs(2),
        maintenance_interval: Duration::from_millis(100),
        ..RuntimeConfig::default()
    })
    .unwrap();
    let raw = Loopback::new(unique_identity());
    runtime
        .execute_chat(
            binding(raw.clone()).with_policy(policy()),
            "idle-expiry",
            request("private"),
        )
        .await
        .unwrap();
    let conn =
        rusqlite::Connection::open(dir.path().join(symbiotic_ai_runtime::QUEUE_DATABASE)).unwrap();
    let payload_count = || {
        conn.query_row(
            "SELECT count(*) FROM spend_receipts WHERE recovery IS NOT NULL OR recovery_expires_at IS NOT NULL",
            [],
            |r| r.get::<_, u64>(0),
        )
        .unwrap()
    };
    assert_eq!(payload_count(), 1);
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(3)).await;
    tokio::time::resume();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while payload_count() != 0 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(payload_count(), 0);
    assert_eq!(raw.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        conn.query_row("SELECT count(*) FROM spend_receipts", [], |r| r
            .get::<_, u64>(0))
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn fdn_idle_maintenance_failure_remains_visible_after_success() {
    let dir = private_tempdir();
    let runtime = Runtime::open(RuntimeConfig {
        state_dir: Some(dir.path().to_path_buf()),
        retention: Duration::from_secs(60),
        maintenance_interval: Duration::from_millis(10),
        ..RuntimeConfig::default()
    })
    .unwrap();
    runtime
        .execute_chat(
            binding(Loopback::new(unique_identity())).with_policy(policy()),
            "failure",
            request("private"),
        )
        .await
        .unwrap();
    assert!(runtime.last_maintenance_error().is_none());
    let conn =
        rusqlite::Connection::open(dir.path().join(symbiotic_ai_runtime::QUEUE_DATABASE)).unwrap();
    conn.execute_batch("CREATE TRIGGER refuse_expiry BEFORE UPDATE OF recovery ON spend_receipts WHEN NEW.recovery IS NULL BEGIN SELECT RAISE(ABORT, 'synthetic failure'); END;
        UPDATE spend_receipts SET recovery_expires_at='2000-01-01T00:00:00+00:00';").unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    while runtime.last_maintenance_error().is_none() && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        runtime.last_maintenance_error().unwrap().code(),
        symbiotic_core::DiagnosticCode::SpendLedgerUnavailable
    );
    let payload_count = || {
        conn.query_row(
            "SELECT count(*) FROM spend_receipts WHERE recovery IS NOT NULL",
            [],
            |r| r.get::<_, u64>(0),
        )
        .unwrap()
    };
    assert_eq!(payload_count(), 1);
    conn.execute_batch("DROP TRIGGER refuse_expiry;").unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    while payload_count() != 0 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(payload_count(), 0);
    assert_eq!(
        runtime.last_maintenance_error().unwrap().code(),
        symbiotic_core::DiagnosticCode::SpendLedgerUnavailable
    );
}

#[tokio::test]
async fn fdn_maintenance_lives_while_a_provider_survives_every_runtime() {
    let dir = private_tempdir();
    let runtime = Runtime::open(RuntimeConfig {
        clock: test_clock::clock(),
        state_dir: Some(dir.path().to_path_buf()),
        retention: Duration::from_secs(1),
        maintenance_interval: Duration::from_millis(100),
        ..RuntimeConfig::default()
    })
    .unwrap();
    let retained = runtime.clone();
    let provider = runtime
        .chat(
            binding(Loopback::new(unique_identity()))
                .with_policy(policy())
                .with_invocation("lifetime"),
        )
        .unwrap();
    drop(runtime);
    drop(retained);
    provider.chat(request("private")).await.unwrap();
    let conn =
        rusqlite::Connection::open(dir.path().join(symbiotic_ai_runtime::QUEUE_DATABASE)).unwrap();
    let payload_count = || {
        conn.query_row(
            "SELECT count(*) FROM spend_receipts WHERE recovery IS NOT NULL",
            [],
            |r| r.get::<_, u64>(0),
        )
        .unwrap()
    };
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(2)).await;
    tokio::time::resume();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while payload_count() != 0 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(payload_count(), 0);
    drop(provider);
}

#[test]
fn fdn_zero_maintenance_interval_is_refused() {
    assert!(matches!(
        Runtime::open(RuntimeConfig {
            maintenance_interval: Duration::ZERO,
            ..RuntimeConfig::default()
        }),
        Err(ModelError::InvalidRequest(
            symbiotic_core::DiagnosticCode::InvalidConfiguration
        ))
    ));
    assert_eq!(
        RuntimeConfig::default().maintenance_interval,
        Duration::from_secs(60)
    );
    assert!(
        Runtime::in_memory()
            .unwrap()
            .last_maintenance_error()
            .is_none()
    );
}

#[tokio::test]
async fn fdn_saved_answer_expires_at_retention_without_reopening() {
    let dir = private_tempdir();
    let runtime = Runtime::open(RuntimeConfig {
        state_dir: Some(dir.path().to_path_buf()),
        retention: Duration::ZERO,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let raw = Loopback::new(unique_identity());
    let configured = binding(raw.clone()).with_policy(policy());
    runtime
        .execute_chat(configured.clone(), "expiry", request("private"))
        .await
        .unwrap();
    let error = runtime
        .execute_chat(configured.clone(), "expiry", request("private"))
        .await
        .unwrap_err();
    assert_eq!(
        error.source.code(),
        symbiotic_core::DiagnosticCode::InvocationCompleted
    );
    assert!(!error.attempt.unwrap().unwrap().output_available);
    let identity = configured.identity.as_ref().unwrap();
    let conn =
        rusqlite::Connection::open(dir.path().join(symbiotic_ai_runtime::QUEUE_DATABASE)).unwrap();
    conn.execute_batch("CREATE TRIGGER refuse_status_writes BEFORE UPDATE ON spend_receipts BEGIN SELECT RAISE(ABORT, 'read must not write'); END;").unwrap();
    assert!(
        !runtime
            .invocation_status(identity, None, "expiry")
            .unwrap()
            .unwrap()
            .output_available
    );
    conn.execute_batch("DROP TRIGGER refuse_status_writes;")
        .unwrap();
    let ledger = symbiotic_ai_runtime::spend::SqliteSpendLedger::open(
        &dir.path().join(symbiotic_ai_runtime::QUEUE_DATABASE),
    )
    .unwrap();
    assert_eq!(ledger.expire_recovery().unwrap(), 1);
    assert_eq!(
        conn.query_row(
            "SELECT count(*) FROM spend_receipts WHERE recovery IS NOT NULL",
            [],
            |r| r.get::<_, u64>(0)
        )
        .unwrap(),
        0
    );
    assert_eq!(raw.calls.load(Ordering::SeqCst), 1);
    drop(runtime);
    let error = persistent(dir.path())
        .execute_chat(configured, "expiry", request("private"))
        .await
        .unwrap_err();
    assert_eq!(
        error.source.code(),
        symbiotic_core::DiagnosticCode::InvocationCompleted
    );
    assert_eq!(raw.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn execution_exhausted_invocation_preserves_failure_class_on_replay_and_restart() {
    let dir = private_tempdir();
    let runtime = persistent(dir.path());
    let raw = Loopback::new(unique_identity()).unavailable();
    let configured = binding(raw.clone())
        .with_policy(policy())
        .with_response_cache(ResponseCacheMode::Off);
    let first = runtime
        .execute_chat(
            configured.clone(),
            "exhausted-unavailable",
            request("retry"),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        first.source,
        ModelError::Unavailable(symbiotic_core::DiagnosticCode::AttemptBudgetExhausted)
    ));
    let status = first.attempt.unwrap().unwrap();
    assert_eq!(status.state, symbiotic_ai_runtime::SpendState::Released);
    assert!(!status.output_available);
    assert_eq!(raw.calls.load(Ordering::SeqCst), 1);

    let replay = runtime
        .execute_chat(
            configured.clone(),
            "exhausted-unavailable",
            request("retry"),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        replay.source,
        ModelError::Unavailable(symbiotic_core::DiagnosticCode::AttemptBudgetExhausted)
    ));
    assert_eq!(replay.attempt.unwrap().unwrap(), status);
    assert_eq!(raw.calls.load(Ordering::SeqCst), 1);
    drop(runtime);

    let restarted = persistent(dir.path());
    let replay = restarted
        .execute_chat(configured, "exhausted-unavailable", request("retry"))
        .await
        .unwrap_err();
    assert!(matches!(
        replay.source,
        ModelError::Unavailable(symbiotic_core::DiagnosticCode::AttemptBudgetExhausted)
    ));
    assert_eq!(replay.attempt.unwrap().unwrap(), status);
    assert_eq!(raw.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn fdn_explicit_limit_survives_pruning_and_never_renews() {
    let dir = private_tempdir();
    let raw = Loopback::new(unique_identity()).unavailable();
    let mut config = policy();
    config.budget_renewal_seconds = Some(0);
    let configured = binding(raw.clone()).with_policy(config);
    let runtime = persistent(dir.path());
    runtime
        .execute_chat(configured.clone(), "exhausted", request("retry"))
        .await
        .unwrap_err();
    runtime
        .execute_chat(configured.clone(), "exhausted", request("retry"))
        .await
        .unwrap_err();
    assert_eq!(raw.calls.load(Ordering::SeqCst), 1);
    drop(runtime);
    rusqlite::Connection::open(dir.path().join(symbiotic_ai_runtime::QUEUE_DATABASE))
        .unwrap()
        .execute("DELETE FROM queue_items", [])
        .unwrap();
    let error = persistent(dir.path())
        .execute_chat(configured, "exhausted", request("retry"))
        .await
        .unwrap_err();
    assert!(matches!(
        error.source,
        ModelError::BudgetExhausted(symbiotic_core::DiagnosticCode::AttemptBudgetExhausted)
    ));
    assert_eq!(raw.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn fdn_implicit_receipt_contains_only_completion_marker() {
    let dir = private_tempdir();
    let runtime = persistent(dir.path());
    let response = runtime
        .chat(
            binding(Loopback::new(unique_identity()))
                .with_policy(policy())
                .with_response_cache(ResponseCacheMode::Off),
        )
        .unwrap()
        .chat(request("private"))
        .await
        .unwrap();
    let reference =
        serde_json::from_value(response.trace.metadata["spend_receipt"].clone()).unwrap();
    assert_eq!(
        runtime.spend_receipt(&reference).unwrap().unwrap().output,
        Some(json!({"output_received": true}))
    );
}

#[tokio::test]
async fn fdn_discard_and_erasure_preserve_accounting_and_completion() {
    let dir = private_tempdir();
    let runtime = persistent(dir.path());
    let mut raw = Loopback::new(unique_identity());
    raw.measured = true;
    let configured = binding(raw.clone())
        .with_policy(policy())
        .with_response_cache(ResponseCacheMode::Off);
    let identity = configured.identity.clone().unwrap();
    let mut a = request("first source");
    a.source = Some("source-a".into());
    let mut b = request("second source");
    b.source = Some("source-b".into());
    let first = runtime
        .execute_chat(configured.clone(), "a", a.clone())
        .await
        .unwrap();
    runtime
        .execute_chat(configured.clone(), "a2", a.clone())
        .await
        .unwrap();
    let second = runtime
        .execute_chat(configured.clone(), "b", b.clone())
        .await
        .unwrap();
    let conn =
        rusqlite::Connection::open(dir.path().join(symbiotic_ai_runtime::QUEUE_DATABASE)).unwrap();
    let evidence = || {
        conn.prepare("SELECT reference,state,usage,output FROM spend_receipts ORDER BY rowid")
            .unwrap()
            .query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, Option<String>>(2)?,
                    r.get::<_, Option<String>>(3)?,
                ))
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    };
    let before = evidence();
    assert_eq!(
        runtime
            .purge_responses(|r| r.source.as_deref() == Some("source-a"))
            .unwrap(),
        2
    );
    assert_eq!(
        runtime
            .execute_chat(configured.clone(), "a2", a.clone())
            .await
            .unwrap_err()
            .source
            .code(),
        symbiotic_core::DiagnosticCode::InvocationCompleted
    );
    let error = runtime
        .execute_chat(configured.clone(), "a", a)
        .await
        .unwrap_err();
    assert_eq!(
        error.source.code(),
        symbiotic_core::DiagnosticCode::InvocationCompleted
    );
    assert_eq!(
        error.attempt.unwrap().unwrap().reference,
        first.attempt.unwrap().unwrap().reference
    );
    assert_eq!(
        runtime
            .execute_chat(configured.clone(), "b", b.clone())
            .await
            .unwrap()
            .attempt
            .unwrap(),
        second.attempt.unwrap()
    );
    runtime
        .discard_invocation_output(&identity, None, "b")
        .unwrap();
    runtime
        .discard_invocation_output(&identity, None, "b")
        .unwrap();
    assert_eq!(
        runtime
            .execute_chat(configured, "b", b)
            .await
            .unwrap_err()
            .source
            .code(),
        symbiotic_core::DiagnosticCode::InvocationCompleted
    );
    assert_eq!(raw.calls.load(Ordering::SeqCst), 3);
    assert_eq!(evidence(), before);
    assert_eq!(
        conn.query_row("SELECT used FROM spend_accounts", [], |r| r
            .get::<_, u64>(0))
            .unwrap(),
        3
    );
    assert_eq!(conn.query_row("SELECT count(*) FROM spend_receipts WHERE recovery IS NOT NULL OR recovery_expires_at IS NOT NULL", [], |r| r.get::<_,u64>(0)).unwrap(), 0);
}

#[tokio::test]
async fn fdn_implicit_success_is_never_recovered_and_unknown_still_blocks() {
    let dir = private_tempdir();
    let runtime = persistent(dir.path());
    let mut raw = Loopback::new(unique_identity());
    raw.measured = true;
    let chat = runtime
        .chat(
            binding(raw.clone())
                .with_policy(policy())
                .with_response_cache(ResponseCacheMode::Off),
        )
        .unwrap();
    let first = chat.chat(request("identical")).await.unwrap();
    let second = chat.chat(request("identical")).await.unwrap();
    assert_ne!(
        first.trace.metadata["spend_receipt"],
        second.trace.metadata["spend_receipt"]
    );
    assert_eq!(raw.calls.load(Ordering::SeqCst), 2);
    let conn =
        rusqlite::Connection::open(dir.path().join(symbiotic_ai_runtime::QUEUE_DATABASE)).unwrap();
    assert_eq!(conn.query_row("SELECT count(*) FROM spend_receipts WHERE attempt_limit IS NOT NULL OR recovery IS NOT NULL OR attempts_used!=0", [], |r| r.get::<_,u64>(0)).unwrap(), 0);
    let unknown = Loopback::new(unique_identity()).failing(ModelError::Timeout(
        symbiotic_core::DiagnosticCode::HttpTimeout,
    ));
    let chat = runtime
        .chat(
            binding(unknown.clone())
                .with_policy(policy())
                .with_response_cache(ResponseCacheMode::Off),
        )
        .unwrap();
    chat.chat(request("unknown")).await.unwrap_err();
    // Remove queue coordination; the exact Unknown ledger guard must still stop dispatch.
    conn.execute("DELETE FROM queue_items", []).unwrap();
    assert_eq!(
        chat.chat(request("unknown")).await.unwrap_err().code(),
        symbiotic_core::DiagnosticCode::SpendReconciliationRequired
    );
    assert_eq!(unknown.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn fdn_original_ceiling_survives_reconciliation_and_changed_configuration() {
    let dir = private_tempdir();
    let runtime = persistent(dir.path());
    let raw = Loopback::new(unique_identity()).failing(ModelError::Timeout(
        symbiotic_core::DiagnosticCode::HttpTimeout,
    ));
    let mut initial = policy();
    initial.logical_retry_attempts = 3;
    let error = runtime
        .execute_chat(
            binding(raw.clone()).with_policy(initial),
            "frozen",
            request("input"),
        )
        .await
        .unwrap_err();
    runtime
        .reconcile_spend(
            &error.attempt.unwrap().unwrap().reference,
            symbiotic_ai_runtime::SpendState::Released,
            None,
        )
        .unwrap();
    let mut higher = policy();
    higher.logical_retry_attempts = 20;
    higher.budget_renewal_seconds = Some(0);
    let retry = binding(raw.clone().unavailable()).with_policy(higher);
    runtime
        .execute_chat(retry.clone(), "frozen", request("input"))
        .await
        .unwrap_err();
    runtime
        .execute_chat(retry, "frozen", request("input"))
        .await
        .unwrap_err();
    assert_eq!(raw.calls.load(Ordering::SeqCst), 3);
    let conn =
        rusqlite::Connection::open(dir.path().join(symbiotic_ai_runtime::QUEUE_DATABASE)).unwrap();
    assert_eq!(
        conn.query_row(
            "SELECT attempt_limit, attempts_used FROM spend_receipts ORDER BY rowid DESC LIMIT 1",
            [],
            |r| Ok((r.get::<_, u32>(0)?, r.get::<_, u32>(1)?))
        )
        .unwrap(),
        (3, 3)
    );
    assert_eq!(conn.query_row("SELECT count(*) FROM queue_items WHERE json_type(payload_json, '$.logical_retry') IS NOT NULL", [], |r| r.get::<_,u64>(0)).unwrap(), 0);
}

#[tokio::test]
async fn fdn_completed_replay_ignores_higher_and_lower_ceilings() {
    let dir = private_tempdir();
    let runtime = persistent(dir.path());
    let raw = Loopback::new(unique_identity());
    let original = runtime
        .execute_chat(
            binding(raw.clone()).with_policy(policy()),
            "completed",
            request("input"),
        )
        .await
        .unwrap();
    for ceiling in [1, 10] {
        let mut config = policy();
        config.logical_retry_attempts = ceiling;
        let replay = runtime
            .execute_chat(
                binding(raw.clone()).with_policy(config),
                "completed",
                request("input"),
            )
            .await
            .unwrap();
        assert_eq!(
            serde_json::to_value(replay.output).unwrap(),
            serde_json::to_value(&original.output).unwrap()
        );
    }
    assert_eq!(raw.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn fdn_unknown_replay_attaches_to_its_receipt_after_queue_pruning() {
    let dir = private_tempdir();
    let runtime = persistent(dir.path());
    let raw = Loopback::new(unique_identity()).failing(ModelError::Timeout(
        symbiotic_core::DiagnosticCode::HttpTimeout,
    ));
    let configured = binding(raw.clone()).with_policy(policy());
    let first = runtime
        .execute_chat(configured.clone(), "unknown", request("input"))
        .await
        .unwrap_err();
    let reference = first.attempt.unwrap().unwrap().reference;
    let conn =
        rusqlite::Connection::open(dir.path().join(symbiotic_ai_runtime::QUEUE_DATABASE)).unwrap();
    conn.execute("DELETE FROM queue_items", []).unwrap();
    let replay = runtime
        .execute_chat(configured, "unknown", request("input"))
        .await
        .unwrap_err();
    assert_eq!(
        replay.source.code(),
        symbiotic_core::DiagnosticCode::SpendReconciliationRequired
    );
    assert_eq!(replay.attempt.unwrap().unwrap().reference, reference);
    assert_eq!(raw.calls.load(Ordering::SeqCst), 1);
}

#[cfg(unix)]
#[tokio::test]
async fn refused_purge_preserves_retained_answers_and_cache_files() {
    for linked_root in [true, false] {
        let dir = private_tempdir();
        let runtime = persistent(dir.path());
        let raw = Loopback::new(unique_identity());
        let configured = binding(raw.clone()).with_policy(policy());
        let paid = runtime
            .execute_chat(configured.clone(), "retained", request("private"))
            .await
            .unwrap();
        let reference = paid.attempt.unwrap().unwrap().reference;
        let before = runtime.spend_receipt(&reference).unwrap().unwrap();
        assert!(before.recovery.is_some());
        let responses = dir.path().join(symbiotic_ai_runtime::RESPONSES_DIR);
        let bystander = private_tempdir();
        let file = bystander.path().join("precious.json");
        std::fs::write(&file, "private cache answer").unwrap();
        if linked_root {
            std::os::unix::fs::symlink(bystander.path(), &responses).unwrap();
        } else {
            std::fs::create_dir(&responses).unwrap();
            std::fs::write(responses.join("own.json"), "private cache answer").unwrap();
            std::os::unix::fs::symlink(bystander.path(), responses.join("linked")).unwrap();
        }
        assert!(matches!(
            runtime.purge_responses(|_| true),
            Err(ModelError::Cache(
                symbiotic_core::DiagnosticCode::CachePathRefused
            ))
        ));
        let after = runtime.spend_receipt(&reference).unwrap().unwrap();
        assert_eq!(after.recovery, before.recovery);
        assert_eq!(after.state, before.state);
        assert_eq!(after.output, before.output);
        assert!(file.exists());
        if !linked_root {
            assert!(responses.join("own.json").exists());
        }
        runtime
            .execute_chat(configured, "retained", request("private"))
            .await
            .unwrap();
        assert_eq!(raw.calls.load(Ordering::SeqCst), 1);
    }
}

// A is held in the provider before B starts, so overlap is deterministic.
async fn identical_implicit_calls(cache_on: bool, abandon_a: bool) {
    let dir = private_tempdir();
    let runtime = persistent(dir.path());
    let gate = Arc::new(tokio::sync::Barrier::new(2));
    let mut raw = Loopback::new(unique_identity());
    raw.measured = true;
    raw.start_gate = Some(gate.clone());
    let calls = raw.calls.clone();
    let configured = binding(raw)
        .with_policy(ModelQueueConfig {
            max_in_flight: 1,
            ..policy()
        })
        .with_response_cache(if cache_on {
            ResponseCacheMode::Default
        } else {
            ResponseCacheMode::Off
        });
    let a = runtime.chat(configured.clone()).unwrap();
    let b = runtime.chat(configured).unwrap();
    let a = tokio::spawn(async move { a.chat(request("identical")).await });
    tokio::time::timeout(Duration::from_secs(1), async {
        while calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let b = b.chat(request("identical"));
    tokio::pin!(b);
    tokio::select! {
        result = &mut b => panic!("B finished before A: {result:?}"),
        _ = tokio::time::sleep(Duration::from_millis(50)) => {}
    }
    let a = if abandon_a {
        a.abort();
        assert!(a.await.unwrap_err().is_cancelled());
        None
    } else {
        Some(a)
    };
    gate.wait().await;
    if !cache_on {
        tokio::select! {
            result = &mut b => panic!("B must dispatch its own attempt: {result:?}"),
            result = tokio::time::timeout(Duration::from_secs(1), gate.wait()) => {
                result.expect("cache-off B must reach the provider after A releases its slot");
            }
        }
    }
    let b = tokio::time::timeout(Duration::from_secs(1), b)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), if cache_on { 1 } else { 2 });
    assert_eq!(
        b.trace.cache.response_cache == symbiotic_trace::CacheStatus::Hit,
        cache_on
    );
    if let Some(a) = a {
        let a = a.await.unwrap().unwrap();
        assert_eq!(a.text, b.text);
        assert_eq!(a.trace.queue_item_id == b.trace.queue_item_id, cache_on);
        if !cache_on {
            assert_ne!(
                a.trace.metadata["spend_receipt"],
                b.trace.metadata["spend_receipt"]
            );
        }
    }
}

#[tokio::test]
async fn proc1_cache_off_identical_running_calls_dispatch_independently() {
    identical_implicit_calls(false, false).await;
}

#[tokio::test]
async fn proc1_cache_off_b_dispatches_after_a_is_abandoned() {
    identical_implicit_calls(false, true).await;
}

#[tokio::test]
async fn proc1_cache_on_identical_calls_share_only_the_cached_response() {
    identical_implicit_calls(true, false).await;
}

#[tokio::test]
async fn proc1_distinct_accepted_handoffs_consume_their_own_reservations() {
    use symbiotic_ai_runtime::{
        AcceptedSpendHandoff, SpendLedger, SpendReceiptRef, SpendReservation, SpendState,
        spend::SqliteSpendLedger,
    };
    for explicit in [false, true] {
        let dir = private_tempdir();
        let runtime = persistent(dir.path());
        let path = dir.path().join(symbiotic_ai_runtime::QUEUE_DATABASE);
        let gate = Arc::new(tokio::sync::Barrier::new(3));
        let mut raw = Loopback::new(unique_identity());
        raw.measured = true;
        raw.start_gate = Some(gate.clone());
        let calls = raw.calls.clone();
        let mut configured = binding(raw).with_policy(policy());
        if explicit {
            configured = configured.with_invocation("same-runtime-invocation");
        }
        let identity = configured.identity.as_ref().unwrap();
        let account = symbiotic_ai_runtime::account_scope(identity, None).unwrap();
        let input = request("handoff");
        let request_hash = symbiotic_ai_runtime::model::configuration_revision(&input)
            .unwrap()
            .0;
        let input_identity = symbiotic_ai_runtime::model::handoff_input_identity(
            "chat",
            Some(identity),
            &request_hash,
        )
        .unwrap();
        let mut conn = rusqlite::Connection::open(&path).unwrap();
        let handoffs: Vec<_> = (0..2)
            .map(|n| AcceptedSpendHandoff {
                reservation: SpendReservation {
                    reference: SpendReceiptRef::new(format!("accepted-{n}")).unwrap(),
                    account: account.clone(),
                    invocation: format!("handoff-{n}"),
                    binding: "accepted-binding".into(),
                    request_limit: Some(2),
                },
                input_identity: input_identity.clone(),
            })
            .collect();
        let tx = conn.transaction().unwrap();
        for handoff in &handoffs {
            assert!(SqliteSpendLedger::reserve_handoff_in(&tx, handoff).unwrap());
        }
        tx.commit().unwrap();
        let providers: Vec<_> = handoffs
            .iter()
            .map(|handoff| {
                let mut configured = configured.clone();
                configured.accepted_spend = Some(handoff.clone());
                runtime.chat(configured).unwrap()
            })
            .collect();
        let a = tokio::spawn({
            let a = providers[0].clone();
            let input = input.clone();
            async move { a.chat(input).await }
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while calls.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let b = providers[1].chat(input.clone());
        tokio::pin!(b);
        tokio::select! {
            result = &mut b => panic!("B finished before release: {result:?}"),
            result = tokio::time::timeout(Duration::from_secs(1), async {
                while calls.load(Ordering::SeqCst) != 2 { tokio::task::yield_now().await; }
            }) => result.expect("each handoff must dispatch independently"),
        }
        gate.wait().await;
        let outputs = [a.await.unwrap().unwrap(), b.await.unwrap()];
        let ledger = SqliteSpendLedger::open(&path).unwrap();
        for ((handoff, provider), output) in handoffs.iter().zip(&providers).zip(&outputs) {
            assert_eq!(
                serde_json::from_value::<SpendReceiptRef>(
                    output.trace.metadata["spend_receipt"].clone()
                )
                .unwrap(),
                handoff.reservation.reference
            );
            assert_eq!(
                ledger
                    .receipt(&handoff.reservation.reference)
                    .unwrap()
                    .unwrap()
                    .state,
                SpendState::Unknown
            );
            assert_eq!(
                provider.chat(input.clone()).await.unwrap_err().code(),
                symbiotic_core::DiagnosticCode::SpendReconciliationRequired
            );
        }
        let (used, owners): (u64, u64) = conn.query_row(
            "SELECT (SELECT used FROM spend_accounts WHERE account=?1), count(DISTINCT dispatch_owner) FROM spend_receipts WHERE dispatch_owner IS NOT NULL",
            [&account], |row| Ok((row.get(0)?, row.get(1)?)),
        ).unwrap();
        assert_eq!((used, owners), (2, 2));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}

#[tokio::test]
async fn regression_answer_recovery_off_disables_caches_recovery_and_answer_telemetry() {
    assert_answer_recovery_off_privacy(true, None).await;
}

#[tokio::test]
async fn regression_answer_recovery_off_rejects_invalid_cost_telemetry() {
    assert_answer_recovery_off_privacy(false, Some("RuntimeAnswerOffMarker910b")).await;
}

#[tokio::test]
async fn regression_answer_recovery_off_preserves_valid_cost_telemetry() {
    assert_answer_recovery_off_privacy(false, Some("0.000042123456789")).await;
}

async fn assert_answer_recovery_off_privacy(
    provider_fields: bool,
    reported_cost: Option<&'static str>,
) {
    const MARKER: &str = "RuntimeAnswerOffMarker910b";
    #[derive(Clone)]
    struct Answer {
        inner: symbiotic_ai_runtime::model::StaticChatProvider,
        calls: Arc<AtomicUsize>,
        measured: bool,
        provider_fields: bool,
        reported_cost: Option<&'static str>,
    }
    impl ModelProvider for Answer {
        fn descriptor(&self) -> &ProviderDescriptor {
            self.inner.descriptor()
        }
    }
    #[async_trait]
    impl ChatProvider for Answer {
        async fn chat(&self, request: ChatRequest) -> Result<ChatResponse, ModelError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let mut response = self.inner.chat(request).await?;
            if self.provider_fields {
                response.trace.trace_id = TraceId(MARKER.into());
                response.trace.model = ModelIdentity::new(MARKER, MARKER, MARKER);
                response.trace.source = Some(MARKER.into());
                response.trace.role_binding = Some(MARKER.into());
                response.trace.audit_refs = vec![MARKER.into()];
            }
            response.trace.usage.reported_cost_usd = self.reported_cost.map(String::from);
            response.trace.response_hash = Some(MARKER.into());
            response.trace.metadata = json!({"diagnostic":MARKER});
            response.trace.usage.response_id = Some(MARKER.into());
            response.trace.usage.served_model = Some(MARKER.into());
            if self.measured {
                response.trace.usage.input_tokens = Some(7);
            }
            Ok(response)
        }
    }
    fn has_marker(path: &std::path::Path) -> bool {
        std::fs::read_dir(path).unwrap().any(|entry| {
            let path = entry.unwrap().path();
            if path.is_dir() {
                has_marker(&path)
            } else {
                std::fs::read(path)
                    .unwrap()
                    .windows(MARKER.len())
                    .any(|bytes| bytes == MARKER.as_bytes())
            }
        })
    }
    tokio::time::timeout(Duration::from_secs(10), async {
        // Changing the policy never reads an earlier retained answer through recovery.
        let dir = tempfile::tempdir().unwrap();
        let runtime = Runtime::open(RuntimeConfig {
            state_dir: Some(dir.path().join("state")),
            ..RuntimeConfig::default()
        })
        .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let retain = binding(Answer {
            inner: symbiotic_ai_runtime::model::StaticChatProvider::new(MARKER),
            calls: calls.clone(),
            measured: true,
            provider_fields,
            reported_cost,
        })
        .with_policy(policy());
        assert!(
            runtime
                .execute_chat(retain.clone(), "retained", request("input"))
                .await
                .unwrap()
                .attempt
                .unwrap()
                .unwrap()
                .output_available
        );
        let off = retain.with_answer_recovery(symbiotic_ai_runtime::AnswerRecovery::Off);
        assert_eq!(
            runtime
                .execute_chat(off, "retained", request("input"))
                .await
                .unwrap_err()
                .source
                .code(),
            symbiotic_core::DiagnosticCode::InvocationCompleted
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        for token_usage in [false, true] {
            let invalid_cost = reported_cost == Some(MARKER);
            let measured = token_usage || reported_cost.is_some_and(|_| !invalid_cost);
            for custom_cache in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let trace = Arc::new(symbiotic_trace::InMemoryTraceSink::default());
                let receipts = Arc::new(InMemoryReceiptSink::default());
                let persistent_trace =
                    symbiotic_trace::JsonlTraceSink::open(dir.path().join("traces.jsonl")).unwrap();
                let runtime = Runtime::open(RuntimeConfig {
                    state_dir: Some(dir.path().join("state")),
                    trace_sink: Some(Arc::new(symbiotic_trace::FanoutTraceSink::new(vec![
                        Box::new((*trace).clone()),
                        Box::new(persistent_trace),
                    ]))),
                    receipt_sink: Some(receipts.clone()),
                    ..RuntimeConfig::default()
                })
                .unwrap();
                let calls = Arc::new(AtomicUsize::new(0));
                let configured = binding(Answer {
                    inner: symbiotic_ai_runtime::model::StaticChatProvider::new(MARKER),
                    calls: calls.clone(),
                    measured: token_usage,
                    provider_fields,
                    reported_cost,
                })
                .with_policy(policy())
                .with_answer_recovery(symbiotic_ai_runtime::AnswerRecovery::Off)
                .with_response_cache(if custom_cache {
                    ResponseCacheMode::Custom(Arc::new(DirResponseCache::new(
                        dir.path().join("custom"),
                    )))
                } else {
                    ResponseCacheMode::Default
                });
                let provider = runtime.chat(configured.clone()).unwrap();
                let response = provider.chat(request("input")).await.unwrap();
                assert_eq!(response.text, MARKER);
                assert_eq!(
                    response.trace.usage.reported_cost_usd.as_deref(),
                    reported_cost
                );
                if invalid_cost {
                    assert!(
                        response.trace.metadata[symbiotic_ai_runtime::model::RUNTIME_DIAGNOSTICS]
                            .as_array()
                            .unwrap()
                            .iter()
                            .any(|entry| entry["error"]
                                == symbiotic_core::DiagnosticCode::InvalidResponse.code())
                    );
                }
                if measured {
                    assert_eq!(provider.chat(request("input")).await.unwrap().text, MARKER);
                } else {
                    assert_eq!(
                        provider.chat(request("input")).await.unwrap_err().code(),
                        symbiotic_core::DiagnosticCode::SpendReconciliationRequired
                    );
                }
                let expected_calls = if measured { 3 } else { 2 };
                let executed = runtime
                    .execute_chat(configured.clone(), "explicit", request("input"))
                    .await
                    .unwrap();
                assert_eq!(executed.output.text, MARKER);
                let attempt = executed.attempt.unwrap().unwrap();
                assert!(!attempt.output_available);
                assert_eq!(
                    attempt.state,
                    if measured {
                        symbiotic_ai_runtime::SpendState::Settled
                    } else {
                        symbiotic_ai_runtime::SpendState::Unknown
                    }
                );
                let receipt = runtime.spend_receipt(&attempt.reference).unwrap().unwrap();
                assert!(receipt.recovery.is_none());
                assert_eq!(receipt.output, Some(json!({"output_received":true})));
                assert_eq!(
                    receipt.usage.as_ref().and_then(|u| u.input_tokens),
                    token_usage.then_some(7)
                );
                assert_eq!(
                    receipt
                        .usage
                        .as_ref()
                        .and_then(|u| u.reported_cost_usd.as_deref()),
                    reported_cost.filter(|_| !invalid_cost)
                );
                if invalid_cost {
                    assert!(
                        executed.output.trace.metadata
                            [symbiotic_ai_runtime::model::RUNTIME_DIAGNOSTICS]
                            .as_array()
                            .unwrap()
                            .iter()
                            .any(|entry| entry["error"]
                                == symbiotic_core::DiagnosticCode::InvalidResponse.code())
                    );
                }
                assert_eq!(
                    runtime
                        .execute_chat(configured.clone(), "explicit", request("input"))
                        .await
                        .unwrap_err()
                        .source
                        .code(),
                    symbiotic_core::DiagnosticCode::InvocationCompleted
                );
                assert_eq!(calls.load(Ordering::SeqCst), expected_calls);
                assert!(
                    !std::fs::read_to_string(dir.path().join("traces.jsonl"))
                        .unwrap()
                        .contains(MARKER)
                );
                for stored in trace.records() {
                    assert_eq!(stored.model, provider.descriptor().identity);
                }
                assert!(
                    !serde_json::to_string(&trace.records())
                        .unwrap()
                        .contains(MARKER)
                );
                assert!(
                    !serde_json::to_string(&receipts.receipts())
                        .unwrap()
                        .contains(MARKER)
                );
                assert!(!has_marker(dir.path()));
                drop(provider);
                drop(runtime);
                let runtime = Runtime::open(RuntimeConfig {
                    state_dir: Some(dir.path().join("state")),
                    ..RuntimeConfig::default()
                })
                .unwrap();
                assert_eq!(
                    runtime
                        .execute_chat(configured, "explicit", request("input"))
                        .await
                        .unwrap_err()
                        .source
                        .code(),
                    symbiotic_core::DiagnosticCode::InvocationCompleted
                );
                assert_eq!(calls.load(Ordering::SeqCst), expected_calls);
                assert!(!has_marker(dir.path()));
            }
        }
    })
    .await
    .expect("bounded runtime privacy regression");
}
