//! Credential-owning WP14 daemon. Memory depends on `symbiotic-egress`, not this crate.
//! Credentials are resolved only after a single-use permit is durably consumed.

#[cfg(unix)]
mod process_security;
#[cfg(unix)]
pub use process_security::protect_process;

mod in_process;
pub use in_process::InProcessEgressClient;

mod jobs;
#[cfg(unix)]
pub mod login;
#[cfg(unix)]
pub mod passthrough;
#[cfg(unix)]
pub use passthrough::{PassthroughConfig, PassthroughMethod, PassthroughRoute};
mod provider;
mod registry;
pub mod secrets;
#[cfg(unix)]
pub mod server;
pub mod stream;

use in_process::AnswerValidator;
use registry::Registry;
use secrets::{Secret, SecretSource};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::{Arc, Mutex, Weak},
};
pub use symbiotic_ai_runtime::AnswerRecovery;
use symbiotic_ai_runtime::{Runtime, RuntimeConfig};
use symbiotic_egress::*;

/// Supported pinned provider transports.
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RouteProvider {
    /// Existing OpenAI-compatible chat adapter.
    OpenAiChat {
        /// Provider identity.
        operator: String,
        /// Omitted unless configured; encoded as {"type":"enabled"|"disabled"}.
        #[serde(skip_serializing_if = "Option::is_none")]
        thinking: Option<symbiotic_ai_runtime::model::ThinkingMode>,
        /// Omitted unless configured; refused with disabled thinking.
        #[serde(skip_serializing_if = "Option::is_none")]
        reasoning_effort: Option<ReasoningEffort>,
    },
    /// Anthropic Messages with explicit thinking mode.
    AnthropicChat {
        /// Provider identity.
        operator: String,
        /// Enabled maps to adaptive thinking; Disabled is explicit; None omits it.
        thinking: Option<symbiotic_ai_runtime::model::ThinkingMode>,
    },
    /// Existing System One transport with typed state and questions.
    JevClassifier {
        /// Provider identity; the expected served model is the route model.
        operator: String,
    },
    /// Existing Gemini embedding adapter, pinned to Google's service.
    GeminiEmbedding { dimensions: usize },
    /// Compatible embedding protocol and explicit dimensions, resolved by the registry.
    CompatibleEmbedding {
        /// OpenAI batch or Ollama single-input protocol.
        adapter: symbiotic_ai_runtime::model::ModelAdapter,
        /// Provider identity; never inferred from endpoint/model text.
        operator: String,
        /// Default output dimensions.
        dimensions: usize,
        /// Full-vector ceiling for reduced-dimension requests.
        embedding_full_dimensions: usize,
        /// Usable input token capacity after provider/model special/task tokens.
        embedding_input_tokens: usize,
    },
    /// Cohere-compatible rerank protocol with hard candidate/input bounds.
    CohereRerank {
        /// Provider identity.
        operator: String,
        /// Maximum sum of query and candidate UTF-8 bytes.
        rerank_input_bytes: usize,
        /// Maximum candidate count.
        rerank_candidates: usize,
        /// Usable query/document token capacity after provider/model overhead.
        rerank_context_tokens: usize,
        /// Provider/model query token capacity.
        rerank_query_tokens: usize,
    },
}

/// Optional reasoning effort for an OpenAI-compatible route.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ReasoningEffort {
    /// Low effort, as used by Rabbithole's deployed DeepSeek profile.
    Low,
    /// Medium effort.
    Medium,
    /// High effort.
    High,
}
impl ReasoningEffort {
    fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
        }
    }
}

fn answer_recovery_is_default(mode: &AnswerRecovery) -> bool {
    *mode == AnswerRecovery::default()
}

/// Allowance shared by identical direct-egress requests after failed or uncertain sends.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestBudget {
    /// Maximum failed or uncertain sends per tenant/route, credential fingerprint and input digest.
    pub attempts: u32,
    /// Renew after this many seconds since failure completion or an uncertain admission.
    /// None never renews. Zero gives every call a fresh budget.
    pub renewal_seconds: Option<u64>,
}

/// Route configured by the credential-process owner, with shared provider byte defaults.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteConfig {
    /// Tenant namespace.
    pub tenant: String,
    /// Concrete account identity, scoped to this tenant unless explicitly shared.
    pub account: String,
    /// Explicit account quota sharing across routes or tenants.
    pub account_sharing_key: Option<symbiotic_ai_runtime::AccountSharingKey>,
    /// Absolute durable request allowance for this account; no monetary ceiling.
    #[serde(default)]
    pub provider_request_limit: Option<u64>,
    /// Foundation-owned finite accepted-handoff allowance per invocation.
    pub max_attempts: u32,
    /// Optional shared failed-request allowance; omitted preserves per-call behavior.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_budget: Option<RequestBudget>,
    /// Retain answers by default; Off persists only completion and numeric spend evidence.
    #[serde(default, skip_serializing_if = "answer_recovery_is_default")]
    pub answer_recovery: AnswerRecovery,
    /// Provider route identifier.
    pub route: String,
    /// Opaque reference scoped to this tenant; empty only for `secret.backend: none`.
    pub secret_ref: String,
    /// Actual backend location, or explicit `none` for keyless execution.
    pub secret: SecretSource,
    /// Pinned base URL; userinfo, query, fragments and redirects are refused.
    pub destination: String,
    /// Exact provider model.
    pub model: String,
    /// Concrete transport.
    pub provider: RouteProvider,
    /// Permit loopback HTTP for local models/tests; otherwise HTTPS is required.
    pub allow_loopback_http: bool,
    /// Maximum typed payload and complete encoded provider body; defaults to
    /// [`symbiotic_ai_runtime::model::DEFAULT_MAX_REQUEST_BYTES`]. Zero is refused.
    #[serde(default = "symbiotic_ai_runtime::model::default_max_request_bytes")]
    pub max_input_bytes: usize,
    /// Maximum provider HTTP success body; defaults to
    /// [`symbiotic_ai_runtime::model::DEFAULT_MAX_RESPONSE_BYTES`]. Zero is refused.
    #[serde(default = "symbiotic_ai_runtime::model::default_max_response_bytes")]
    pub max_response_bytes: usize,
    /// Maximum metadata/identity field bytes in an attempt.
    pub max_field_bytes: usize,
    /// Maximum chat output tokens; requests must also specify their own bound.
    pub max_output_tokens: u32,
    /// Existing runtime model concurrency limit.
    pub max_in_flight: usize,
    /// Existing runtime request pacing, explicitly set or null.
    pub requests_per_minute: Option<u32>,
    /// Existing runtime input pacing, explicitly set or null.
    pub input_units_per_minute: Option<u64>,
    /// Provider timeout, finite and nonzero.
    pub timeout_seconds: u64,
}

/// Versioned deployment configuration; only locations/references, no credential values.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessConfig {
    /// Must equal protocol version 4.
    pub version: u16,
    /// Owner-only runtime directory (queue plus permit replay metadata).
    pub state_dir: PathBuf,
    /// Socket inside an existing owner-only directory.
    pub socket_path: PathBuf,
    /// Admission MAC key source, separate from provider credentials.
    pub admission_key: SecretSource,
    /// Finite upper bound on each secret, including the admission key.
    pub max_secret_bytes: usize,
    /// Maximum typed-frame or passthrough-header bytes.
    pub max_frame_bytes: u32,
    /// Maximum concurrent socket handlers; this is not a model scheduler.
    pub max_connections: usize,
    /// Time allowed for reading/writing a socket frame.
    pub io_timeout_seconds: u64,
    /// Reporting-only tolerance for signed attempt time ahead of this clock.
    /// In seconds; defaults to five. Does not change admission or authority checks.
    #[serde(default = "default_clock_rollback_warning_tolerance_seconds")]
    pub clock_rollback_warning_tolerance_seconds: u64,
    /// Versioned job-store policy (queue defaults).
    #[serde(default)]
    pub jobs: symbiotic_queue::jobs::JobConfig,
    /// Versioned job-runner policy (runner defaults).
    #[serde(default)]
    pub job_runner: symbiotic_queue::runner::RunnerConfig,
    /// Approved routes; no caller-supplied destinations or secret paths.
    pub routes: Vec<RouteConfig>,
    /// Optional same-UID HTTP passthrough server sharing the typed account limits.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub passthrough: Option<PassthroughConfig>,
}

impl ProcessConfig {
    /// Validate both route sets and their shared policies without opening files.
    pub fn validate_routes(&self) -> Result<(), EgressError> {
        validated_config_registry(self).map(|_| ())
    }

    /// Refuse callbacks before launching a child; closures cannot cross that boundary.
    pub fn validate_child_process(&self) -> Result<(), EgressError> {
        if std::iter::once(&self.admission_key)
            .chain(self.routes.iter().map(|route| &route.secret))
            .any(|source| matches!(source, SecretSource::Resolver { .. }))
        {
            return Err(EgressError::ResolverRequiresThreadMode);
        }
        Ok(())
    }
}

fn default_clock_rollback_warning_tolerance_seconds() -> u64 {
    5
}

// Fixed receipt/status/permit envelope allowance, excluding escaped identity
// strings and the existing fourfold provider-response allowance.
const REPLY_ENVELOPE_BYTES: usize = 4096;

/// Check deployment routes with the same configuration rules used at startup.
///
/// `max_frame_bytes` must accommodate each route's request and reply bounds.
/// This checks the entire set, including duplicate routes and shared account
/// policies, and returns the first refusal. Byte defaults and supported enum
/// values are applied when deserializing [`RouteConfig`].
///
/// No files or runtime are opened, no secret values are read and no resolvers
/// are invoked. Process-wide settings, OS protection, state availability and
/// credential availability are checked separately by [`CredentialProcess::open`].
pub fn validate_routes(routes: &[RouteConfig], max_frame_bytes: u32) -> Result<(), EgressError> {
    validated_registry(routes, max_frame_bytes).map(|_| ())
}

fn validated_config_registry(
    config: &ProcessConfig,
) -> Result<Option<symbiotic_ai_runtime::model::ModelRegistry>, EgressError> {
    let registry = if config.routes.is_empty() {
        validate_frame_size(config.max_frame_bytes)?;
        if config.passthrough.is_none() {
            return Err(EgressError::InvalidRequest);
        }
        None
    } else {
        Some(validated_registry(&config.routes, config.max_frame_bytes)?)
    };
    passthrough::validate(config)?;
    Ok(registry)
}

fn validated_registry(
    routes: &[RouteConfig],
    max_frame_bytes: u32,
) -> Result<symbiotic_ai_runtime::model::ModelRegistry, EgressError> {
    validate_frame_size(max_frame_bytes)?;
    if routes.is_empty() {
        return Err(EgressError::InvalidRequest);
    }
    let mut identities = HashSet::new();
    for route in routes {
        if matches!(route.secret, SecretSource::LoginFile { .. }) {
            return Err(EgressError::InvalidRequest);
        }
        validate_route(route, max_frame_bytes)?;
        if !identities.insert((&route.tenant, &route.route)) {
            return Err(EgressError::InvalidRequest);
        }
    }
    provider::configured_registry(routes)
}

fn validate_frame_size(max_frame_bytes: u32) -> Result<(), EgressError> {
    // Even the smallest route needs four one-byte identities and a
    // one-byte response in addition to the fixed envelope allowance.
    if (max_frame_bytes as usize) < REPLY_ENVELOPE_BYTES + 4 * 6 + 4 {
        return Err(EgressError::InvalidFrameConfiguration);
    }
    Ok(())
}

#[derive(Default)]
struct RequestBudgetDispatch(Mutex<HashMap<String, Weak<tokio::sync::Mutex<()>>>>);

struct RequestBudgetGate<'a> {
    owner: &'a RequestBudgetDispatch,
    key: String,
    gate: Option<Arc<tokio::sync::Mutex<()>>>,
}

impl<'a> RequestBudgetGate<'a> {
    async fn lock_owned(self) -> Result<(tokio::sync::OwnedMutexGuard<()>, Self), EgressError> {
        let gate = self.gate.as_ref().ok_or(EgressError::StateUnavailable)?;
        let guard = gate.clone().lock_owned().await;
        // Drop the owned mutex guard before the registration, so retirement sees
        // every holder and waiter leave, including cancellation while waiting.
        Ok((guard, self))
    }
}

impl Drop for RequestBudgetGate<'_> {
    fn drop(&mut self) {
        // Poison still refuses subsequent lookup. Cleanup alone can recover the
        // map: it cannot admit work or alter durable budget accounting.
        let mut gates = self
            .owner
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        // Release our reference under the lookup mutex; concurrent departures
        // must not both observe another reference and leave an idle entry.
        drop(self.gate.take());
        if gates
            .get(&self.key)
            .is_some_and(|gate| gate.strong_count() == 0)
        {
            gates.remove(&self.key);
        }
    }
}

impl RequestBudgetDispatch {
    fn gate(&self, key: &str) -> Result<RequestBudgetGate<'_>, EgressError> {
        let mut gates = self.0.lock().map_err(|_| EgressError::StateUnavailable)?;
        let entry = gates.entry(key.to_owned()).or_default();
        let gate = entry.upgrade().unwrap_or_else(|| {
            let gate = Arc::new(tokio::sync::Mutex::new(()));
            *entry = Arc::downgrade(&gate);
            gate
        });
        Ok(RequestBudgetGate {
            owner: self,
            key: key.to_owned(),
            gate: Some(gate),
        })
    }
}

struct Inner {
    config: ProcessConfig,
    clock: registry::Clock,
    key: AdmissionKey,
    routes: HashMap<(String, String), RouteConfig>,
    registry: Mutex<Registry>,
    // One gate per request-budget key, held from admission through durable
    // completion so snapshot undo and success clearing cannot race another debit.
    // The final holder/waiter retires only its key under the lookup mutex.
    request_budget_dispatch: RequestBudgetDispatch,
    runtime: Runtime,
    passthrough: Option<passthrough::Passthrough>,
    job_runners: tokio::sync::Mutex<HashMap<String, Option<symbiotic_queue::runner::JobRunner>>>,
    _process_lock: Arc<ProcessLock>,
}

/// Cloneable process handle. Started dispatch work survives dropped caller futures.
#[derive(Clone)]
pub struct CredentialProcess {
    inner: Arc<Inner>,
}

impl CredentialProcess {
    /// Open bounded, protected state and configured local secret backends.
    /// Embedded callers must also call [`protect_process`] before reading configuration.
    /// For resolver sources, first initialize [`secrets::initialize_resolver_panic_hook`].
    pub fn open(config: ProcessConfig) -> Result<Self, EgressError> {
        Self::open_with_clock(config, Arc::new(chrono::Utc::now))
    }

    /// Open with a trusted host clock for authorization and runtime deadlines.
    pub fn open_with_clock(
        config: ProcessConfig,
        clock: symbiotic_queue::jobs::JobClock,
    ) -> Result<Self, EgressError> {
        #[cfg(unix)]
        protect_process().map_err(|_| EgressError::StateUnavailable)?;
        if config.version != PROTOCOL_VERSION {
            return Err(EgressError::Version);
        }
        validate_frame_size(config.max_frame_bytes)?;
        if config.max_secret_bytes < 32
            || config.max_connections == 0
            || config.max_connections > tokio::sync::Semaphore::MAX_PERMITS
            || config.io_timeout_seconds == 0
        {
            return Err(EgressError::InvalidRequest);
        }
        // Validate every route and shared account policy before touching state.
        // A lock or IO failure must not mask invalid deployment configuration.
        let configured_registry = validated_config_registry(&config)?.map(Arc::new);
        let routes = config
            .routes
            .iter()
            .map(|route| ((route.tenant.clone(), route.route.clone()), route.clone()))
            .collect();
        symbiotic_ai_runtime::model::private_fs::ensure_private_dir(&config.state_dir)
            .map_err(|_| EgressError::StateUnavailable)?;
        let process_lock = lock_process(&config.state_dir)?;
        let key = AdmissionKey::new(config.admission_key.load(config.max_secret_bytes)?.to_vec())?;
        let runtime = Runtime::open(RuntimeConfig {
            clock: clock.clone(),
            registry: configured_registry,
            state_dir: Some(config.state_dir.clone()),
            ..RuntimeConfig::default()
        })
        .map_err(|_| EgressError::StateUnavailable)?;
        for route in &config.routes {
            provider::validate_binding(&runtime, route)?;
        }
        let passthrough = passthrough::Passthrough::open(&config, &runtime)?;
        // Replay and ledger acceptance share the runtime operational database.
        let clock: registry::Clock = Arc::new(move || {
            u64::try_from(clock().timestamp()).map_err(|_| EgressError::StateUnavailable)
        });
        let registry = Registry::open_with_clock(
            &config.state_dir.join(symbiotic_ai_runtime::QUEUE_DATABASE),
            clock.clone(),
        )?;
        Ok(Self {
            inner: Arc::new(Inner {
                config,
                clock,
                key,
                routes,
                registry: Mutex::new(registry),
                request_budget_dispatch: RequestBudgetDispatch::default(),
                runtime,
                passthrough,
                job_runners: tokio::sync::Mutex::new(HashMap::new()),
                _process_lock: Arc::new(process_lock),
            }),
        })
    }

    /// Open with an embedded-only retry decision hook. JSON cannot configure the hook.
    #[cfg(unix)]
    pub fn open_with_retry_hook(
        config: ProcessConfig,
        hook: passthrough::RetryHook,
    ) -> Result<Self, EgressError> {
        let mut process = Self::open(config)?;
        let inner = Arc::get_mut(&mut process.inner).ok_or(EgressError::StateUnavailable)?;
        let pass = inner
            .passthrough
            .as_mut()
            .ok_or(EgressError::InvalidRequest)?;
        pass.retry_hook = Some(hook);
        Ok(process)
    }

    /// Deployment settings without credential values.
    pub fn config(&self) -> &ProcessConfig {
        &self.inner.config
    }

    /// Handle the shared protocol in process (the socket server calls this method).
    pub async fn handle(&self, request: Request) -> Response {
        self.handle_with_answer_validation(request, None).await
    }

    async fn handle_with_answer_validation(
        &self,
        request: Request,
        answer_validator: Option<AnswerValidator>,
    ) -> Response {
        let result = if request.version != PROTOCOL_VERSION {
            Err(EgressError::Version)
        } else {
            self.operation(request.operation, answer_validator).await
        };
        Response {
            version: PROTOCOL_VERSION,
            result,
        }
    }

    async fn operation(
        &self,
        operation: Operation,
        answer_validator: Option<AnswerValidator>,
    ) -> Result<Reply, EgressError> {
        // Reject mismatched job envelopes before any expiry maintenance.
        let command_matches = match &operation {
            Operation::EnqueueJobs(signed) => {
                matches!(signed.request.command, JobsCommand::EnqueueJobs(_))
            }
            Operation::AdmitJob(signed) => {
                matches!(signed.request.command, JobsCommand::AdmitJob { .. })
            }
            Operation::Completions(signed) => {
                matches!(signed.request.command, JobsCommand::Completions { .. })
            }
            Operation::AckJobs(signed) => {
                matches!(signed.request.command, JobsCommand::AckJobs(_))
            }
            Operation::CancelJobs(signed) => {
                matches!(signed.request.command, JobsCommand::CancelJobs(_))
            }
            Operation::JobStatus(signed) => {
                matches!(signed.request.command, JobsCommand::JobStatus(_))
            }
            Operation::PurgeOwner(signed) => {
                matches!(signed.request.command, JobsCommand::PurgeOwner(_))
            }
            _ => true,
        };
        if !command_matches {
            return Err(EgressError::InvalidRequest);
        }
        // Owner erasure validates its MAC and scope before even expiry maintenance.
        if !matches!(&operation, Operation::PurgeOwner(_)) {
            self.purge_expired_results()?;
        }
        match operation {
            Operation::EnqueueJobs(signed)
            | Operation::AdmitJob(signed)
            | Operation::Completions(signed)
            | Operation::AckJobs(signed)
            | Operation::CancelJobs(signed)
            | Operation::JobStatus(signed)
            | Operation::PurgeOwner(signed) => self.jobs_operation(*signed).await,
            Operation::IssuePermit(signed) => {
                self.inner.key.verify_attempt(&signed)?;
                let foundation_now = (self.inner.clock)()?;
                // Release the registry mutex before invoking a tracing subscriber.
                let existing = self
                    .inner
                    .registry
                    .lock()
                    .map_err(|_| EgressError::StateUnavailable)?
                    .existing(&signed.attempt)?;
                if let Some(grant) = existing {
                    self.warn_if_attempt_time_ahead(signed.attempt.recorded_at, foundation_now);
                    return Ok(Reply::Permit(grant));
                }
                let max_attempts = self.validate_attempt(&signed.attempt)?.max_attempts;
                let permit = self
                    .inner
                    .registry
                    .lock()
                    .map_err(|_| EgressError::StateUnavailable)?
                    .issue(&signed.attempt, max_attempts)?;
                self.warn_if_attempt_time_ahead(signed.attempt.recorded_at, foundation_now);
                Ok(Reply::Permit(permit))
            }
            Operation::AttemptStatus(signed) => {
                self.inner.key.verify_attempt_id(&signed)?;
                let status = self
                    .inner
                    .registry
                    .lock()
                    .map_err(|_| EgressError::StateUnavailable)?
                    .attempt_status(&signed.attempt_id, (self.inner.clock)()?)?;
                Ok(Reply::AttemptStatus(status))
            }
            Operation::PublishGrantRevision(signed) => {
                self.inner.key.verify_grant_revision(&signed)?;
                let grant = &signed.grant;
                if grant.tenant.is_empty()
                    || grant.incarnation.is_empty()
                    || grant.revision == 0
                    || grant.revision > i64::MAX as u64
                {
                    return Err(EgressError::InvalidRequest);
                }
                self.inner
                    .registry
                    .lock()
                    .map_err(|_| EgressError::StateUnavailable)?
                    .publish_revision(grant)?;
                Ok(Reply::GrantRevisionPublished)
            }
            Operation::Receipt(signed) => {
                self.inner.key.verify_attempt_id(&signed)?;
                let receipt = self
                    .inner
                    .registry
                    .lock()
                    .map_err(|_| EgressError::StateUnavailable)?
                    .receipt(&signed.attempt_id)?;
                Ok(Reply::Receipt(receipt))
            }
            Operation::InjectProviderCredential(request) => {
                if request.operation_version != PROTOCOL_VERSION {
                    return Err(EgressError::Version);
                }
                self.inner.key.verify_attempt(&request.admission)?;
                let foundation_now = (self.inner.clock)()?;
                let route = self.validate_attempt(&request.admission.attempt)?.clone();
                validate_payload(&route, &request.payload)?;
                if request.payload.digest()? != request.admission.attempt.input_digest {
                    return Err(EgressError::InvalidRequest);
                }
                let mut payload = request.payload;
                provider::prepare_payload(&mut payload, &digest(&request.admission.attempt)?);
                let handoff =
                    provider::accepted_handoff(&request.admission.attempt, &route, &payload)?;
                let receipt = self
                    .inner
                    .registry
                    .lock()
                    .map_err(|_| EgressError::StateUnavailable)?
                    .consume(
                        &request.admission.attempt,
                        &request.permit,
                        &handoff,
                        route.max_attempts,
                    )?;
                // Spawning occurs immediately after consumption with no await in
                // between. Client cancellation cannot leave a consumed-but-cancelled
                // live task; a process crash leaves the durable unknown receipt.
                let process = self.clone();
                let input_digest = request.admission.attempt.input_digest;
                let task = tokio::spawn(async move {
                    process
                        .dispatch(
                            route,
                            payload,
                            receipt,
                            handoff,
                            input_digest,
                            answer_validator,
                        )
                        .await
                });
                self.warn_if_attempt_time_ahead(
                    request.admission.attempt.recorded_at,
                    foundation_now,
                );
                task.await
                    .map_err(|_| EgressError::Transport)
                    .map(Reply::Dispatched)
            }
        }
    }

    fn warn_if_attempt_time_ahead(&self, recorded_at: u64, foundation_now: u64) {
        let ahead_seconds = recorded_at.saturating_sub(foundation_now);
        let tolerance_seconds = self.inner.config.clock_rollback_warning_tolerance_seconds;
        if ahead_seconds > tolerance_seconds {
            tracing::warn!(
                event = "signed_attempt_time_ahead",
                recorded_at,
                foundation_now,
                ahead_seconds,
                tolerance_seconds,
            );
        }
    }

    /// Remove at most 64 expired recovery results, selected by the deadline index.
    /// Socket serving runs this every second even when idle.
    /// Embedded hosts must also call it periodically while idle; all operations purge first.
    pub fn purge_expired_results(&self) -> Result<(), EgressError> {
        self.inner
            .registry
            .lock()
            .map_err(|_| EgressError::StateUnavailable)?
            .purge_expired((self.inner.clock)()?)
    }

    fn validate_attempt(&self, a: &DurableAttempt) -> Result<&RouteConfig, EgressError> {
        let route = self
            .inner
            .routes
            .get(&(a.tenant.clone(), a.route.clone()))
            .ok_or(EgressError::RouteRefused)?;
        let fields = [
            &a.tenant,
            &a.incarnation,
            &a.invocation_id,
            &a.caller_binding,
            &a.route,
            &a.destination,
            &a.model,
            &a.method,
            &a.manifest_ref,
        ];
        if fields
            .iter()
            .any(|field| field.is_empty() || field.len() > route.max_field_bytes)
            || a.job_queue
                .as_ref()
                .is_some_and(|queue| queue.is_empty() || queue.len() > route.max_field_bytes)
            || a.secret_ref.len() > route.max_field_bytes
            || a.attempt_ordinal == 0
            || a.grant_revision == 0
            || a.grant_revision > i64::MAX as u64
            || a.record_sequence == 0
            || a.record_sequence > i64::MAX as u64
            || a.recorded_at >= a.expires_at
            || a.expires_at > i64::MAX as u64
            || a.recovery_expires_at == 0
            || route.answer_recovery != AnswerRecovery::Off
                && a.attempt_ordinal == 1
                && a.recovery_expires_at <= a.recorded_at
            || a.recovery_expires_at > i64::MAX as u64
            || !is_digest(&a.input_digest)
            || !is_digest(&a.input_manifest_digest)
        {
            return Err(EgressError::InvalidRequest);
        }
        if a.destination != route.destination
            || a.model != route.model
            || a.method != "POST"
            || a.secret_ref != route.secret_ref
        {
            return Err(EgressError::RouteRefused);
        }
        Ok(route)
    }

    async fn dispatch(
        &self,
        route: RouteConfig,
        payload: ProviderPayload,
        mut receipt: DispatchReceipt,
        handoff: symbiotic_ai_runtime::model::AcceptedSpendHandoff,
        input_digest: String,
        answer_validator: Option<AnswerValidator>,
    ) -> DispatchResult {
        // Resolve first: the credential fingerprint is part of the budget key.
        let mut _budget_guard = None;
        let source = route.secret.clone();
        let max = self.inner.config.max_secret_bytes;
        let secret = tokio::task::spawn_blocking(move || match source {
            SecretSource::None => Ok(Secret::keyless()),
            source => Secret::from_bytes(source.load(max)?),
        })
        .await;
        let mut output = None;
        let mut error = None;
        let mut diagnostics = Vec::new();
        let mut budget_admission = None;
        match secret {
            Ok(Ok(secret)) => {
                let request_key = request_key(
                    &route,
                    &input_digest,
                    symbiotic_ai_runtime::model::api_key_fingerprint(secret.value()),
                );
                let admission = async {
                    let key = request_key?;
                    if route.request_budget.is_some() {
                        let gate = self.inner.request_budget_dispatch.gate(&key)?;
                        // Never await a key or account while holding the map/registry
                        // mutex. Retain this key's guard until finish commits or fails.
                        _budget_guard = Some(gate.lock_owned().await?);
                    }
                    self.inner
                        .registry
                        .lock()
                        .map_err(|_| EgressError::StateUnavailable)?
                        .admit_request(&receipt.attempt_digest, key, route.request_budget.as_ref())
                }
                .await;
                let execution = match admission {
                    Ok(admission) => {
                        budget_admission = admission;
                        provider::execute(
                            &self.inner.runtime,
                            &route,
                            Arc::new(secret),
                            payload,
                            handoff,
                        )
                        .await
                    }
                    Err(code) => Err(code.into()),
                };
                match execution {
                    Ok((answer, usage, runtime_diagnostics)) => {
                        diagnostics = runtime_diagnostics;
                        receipt.status = DispatchStatus::Succeeded;
                        receipt.usage = usage;
                        if symbiotic_ai_runtime::model::has_measured_usage(&receipt.usage) {
                            receipt.spend_state = SpendState::Settled;
                        }
                        output = Some(answer);
                    }
                    Err(failure) => {
                        diagnostics = failure.diagnostics;
                        error = Some(failure.code);
                        if !failure.may_have_dispatched {
                            receipt.spend_state = SpendState::Released;
                        }
                    }
                }
            }
            _ => {
                error = Some(EgressError::CredentialUnavailable);
                receipt.status = DispatchStatus::CredentialUnavailable;
                receipt.spend_state = SpendState::Released;
            }
        }
        // Observation and caller acceptance are separate: even a rejected answer
        // is paid completion evidence, independently of whether usage was measured.
        let output_received = output.is_some();
        if let (Some(answer), Some(validate)) = (&output, answer_validator)
            && !validate(answer)
        {
            error = Some(EgressError::Provider { status: None });
            receipt.status = DispatchStatus::ProviderFailed;
            output = None;
        }
        // A paid answer is returned even if its bookkeeping write fails. The
        // previously committed Unknown remains conservative for restart/reconciliation.
        let mut result = DispatchResult {
            error,
            diagnostics,
            receipt,
            output,
            receipt_persisted: true,
        };
        result.receipt_persisted = self.inner.registry.lock().is_ok_and(|mut registry| {
            registry
                .finish(
                    route.answer_recovery,
                    &result,
                    budget_admission.as_ref(),
                    output_received,
                )
                .is_ok()
        });
        if !result.receipt_persisted {
            // Observed usage and output remain useful, but cannot claim a durable
            // settlement or release when the atomic completion transaction failed.
            result.receipt.spend_state = SpendState::Unknown;
        }
        result
    }
}

fn is_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn validate_route(route: &RouteConfig, max_frame: u32) -> Result<(), EgressError> {
    // A receipt can echo four identity strings; JSON can encode each input
    // byte as six bytes (\\u00xx). Preserve the existing fourfold response
    // allowance, plus room for the fixed receipt/status/permit envelopes.
    let reply_bound = route
        .max_field_bytes
        .checked_mul(4 * 6)
        .and_then(|identity| {
            route
                .max_response_bytes
                .checked_mul(4)
                .and_then(|response| identity.checked_add(response))
        })
        .and_then(|bytes| bytes.checked_add(REPLY_ENVELOPE_BYTES));
    if reply_bound.is_none_or(|bytes| bytes > max_frame as usize) {
        return Err(EgressError::InvalidFrameConfiguration);
    }
    if route.account.trim().is_empty()
        || route.tenant.is_empty()
        || route.route.is_empty()
        || (matches!(route.secret, SecretSource::None) != route.secret_ref.is_empty())
        || route.model.is_empty()
        || route.max_field_bytes == 0
        || route.max_input_bytes == 0
        || route.max_response_bytes == 0
        || route.max_output_tokens == 0
        || route.max_in_flight == 0
        || route.requests_per_minute == Some(0)
        || route.input_units_per_minute == Some(0)
        || route.max_attempts == 0
        || route
            .request_budget
            .as_ref()
            .is_some_and(|budget| budget.attempts == 0)
        || route.timeout_seconds == 0
        || route.max_input_bytes > max_frame as usize / 2
    {
        return Err(EgressError::InvalidRequest);
    }
    let url = reqwest::Url::parse(&route.destination).map_err(|_| EgressError::InvalidRequest)?;
    let loopback = url.host_str().is_some_and(|host| {
        host == "localhost"
            || host
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback())
    });
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.host_str().is_none()
        || !(url.scheme() == "https"
            || route.allow_loopback_http && loopback && url.scheme() == "http")
    {
        return Err(EgressError::InvalidRequest);
    }
    match &route.provider {
        RouteProvider::GeminiEmbedding { dimensions }
            if *dimensions == 0
                || route.destination != "https://generativelanguage.googleapis.com/v1beta"
                || !route
                    .model
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b)) =>
        {
            Err(EgressError::InvalidRequest)
        }
        RouteProvider::OpenAiChat { operator, .. }
        | RouteProvider::AnthropicChat { operator, .. }
        | RouteProvider::JevClassifier { operator }
            if operator.is_empty() =>
        {
            Err(EgressError::InvalidRequest)
        }
        _ => Ok(()),
    }
}

fn validate_payload(route: &RouteConfig, payload: &ProviderPayload) -> Result<(), EgressError> {
    // Route validation bounds this limit by half the u32-sized frame limit.
    let max_bytes =
        u32::try_from(route.max_input_bytes).map_err(|_| EgressError::InvalidRequest)?;
    encode_frame(payload, max_bytes)?;
    match (&route.provider, payload) {
        (RouteProvider::JevClassifier { .. }, ProviderPayload::Classify(request)) => {
            symbiotic_ai_runtime::model::wire::jev_classify_body(
                &route.model,
                request,
                Some(route.max_input_bytes),
            )
            .map(|_| ())
            .map_err(payload_error)
        }
        (RouteProvider::CompatibleEmbedding { .. }, ProviderPayload::Embedding(request)) => {
            let (adapter, _, settings) = provider::route_settings(route);
            symbiotic_ai_runtime::model::wire::compatible_embedding_body(
                adapter,
                &route.model,
                &settings,
                request,
                route.max_input_bytes,
            )
            .map(|_| ())
            .map_err(payload_error)
        }
        (RouteProvider::CohereRerank { .. }, ProviderPayload::Rerank(request)) => {
            let (_, _, settings) = provider::route_settings(route);
            symbiotic_ai_runtime::model::wire::cohere_rerank_body(
                &route.model,
                &settings,
                request,
                route.max_input_bytes,
            )
            .map(|_| ())
            .map_err(payload_error)
        }
        (
            RouteProvider::OpenAiChat { .. } | RouteProvider::AnthropicChat { .. },
            ProviderPayload::Chat(request),
        ) if !request.messages.is_empty()
            && request.messages.len() <= 128
            && request
                .messages
                .iter()
                .all(|message| message.role.len() <= route.max_field_bytes)
            && request
                .max_output_tokens
                .is_some_and(|limit| limit > 0 && limit <= route.max_output_tokens) =>
        {
            match &route.provider {
                RouteProvider::AnthropicChat { thinking, .. } => {
                    symbiotic_ai_runtime::model::wire::anthropic_chat_body(
                        &route.model,
                        request,
                        *thinking,
                        Some(route.max_input_bytes),
                    )
                }
                RouteProvider::OpenAiChat {
                    thinking,
                    reasoning_effort,
                    ..
                } => symbiotic_ai_runtime::model::wire::openai_chat_body(
                    &route.model,
                    request,
                    *thinking,
                    reasoning_effort.map(ReasoningEffort::as_str),
                    Some(route.max_input_bytes),
                ),
                _ => return Err(EgressError::InvalidRequest),
            }
            .map(|_| ())
            .map_err(payload_error)
        }
        (RouteProvider::GeminiEmbedding { dimensions }, ProviderPayload::Embedding(request))
            if !request.inputs.is_empty()
                && request.inputs.len() <= 128
                && request.dimensions.is_none_or(|value| value == *dimensions) =>
        {
            symbiotic_ai_runtime::model::wire::gemini_embedding_body(
                &route.model,
                *dimensions,
                request,
                Some(route.max_input_bytes),
            )
            .map(|_| ())
            .map_err(|_| EgressError::LimitExceeded)
        }
        _ => Err(EgressError::InvalidRequest),
    }
}

fn payload_error(error: symbiotic_ai_runtime::ModelError) -> EgressError {
    if error.code() == symbiotic_ai_runtime::model::DiagnosticCode::ProviderRequestLimitExceeded {
        EgressError::LimitExceeded
    } else {
        EgressError::InvalidRequest
    }
}

struct ProcessLock(std::fs::File);

impl Drop for ProcessLock {
    fn drop(&mut self) {
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            // Closing alone leaves the flock held while a concurrently spawned
            // child retains an inherited descriptor, even with O_CLOEXEC.
            // SAFETY: this guard owns the live descriptor; flock retains no pointer.
            let _ = unsafe { libc::flock(self.0.as_raw_fd(), libc::LOCK_UN) };
        }
    }
}

fn lock_process(dir: &std::path::Path) -> Result<ProcessLock, EgressError> {
    lock_file(&dir.join("credential-process.lock"), false)
}

fn lock_file(path: &std::path::Path, wait: bool) -> Result<ProcessLock, EgressError> {
    symbiotic_ai_runtime::model::private_fs::ensure_private_file(path)
        .map_err(|_| EgressError::StateUnavailable)?;
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .map_err(|_| EgressError::StateUnavailable)?;
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;
        // SAFETY: the descriptor belongs to this live File; flock retains no pointer.
        let operation = libc::LOCK_EX | if wait { 0 } else { libc::LOCK_NB };
        if unsafe { libc::flock(file.as_raw_fd(), operation) } != 0 {
            return Err(EgressError::StateUnavailable);
        }
    }
    #[cfg(not(unix))]
    return Err(EgressError::StateUnavailable);
    Ok(ProcessLock(file))
}

fn invocation_binding(a: &DurableAttempt) -> Result<String, EgressError> {
    let mut immutable = a.clone();
    immutable.attempt_ordinal = 0;
    immutable.record_sequence = 0;
    immutable.recorded_at = 0;
    // Authority is rechecked for each attempt; a renewed deadline does not
    // change the invocation's input/provider or immutable recovery binding.
    immutable.expires_at = 0;
    immutable.grant_revision = 0;
    digest(&immutable)
}

fn egress_reference(
    a: &DurableAttempt,
) -> Result<symbiotic_ai_runtime::SpendReceiptRef, EgressError> {
    symbiotic_ai_runtime::SpendReceiptRef::new(format!("egress:{}", digest(a)?))
        .map_err(|_| EgressError::LimitExceeded)
}

fn request_key(
    route: &RouteConfig,
    input_digest: &str,
    credential_fingerprint: Option<String>,
) -> Result<String, EgressError> {
    symbiotic_ai_runtime::model::configuration_revision(&(
        &route.tenant,
        &route.route,
        credential_fingerprint,
        input_digest,
    ))
    .map(|digest| digest.0)
    .map_err(|_| EgressError::StateUnavailable)
}

fn spend_reservation(
    a: &DurableAttempt,
    route: &RouteConfig,
) -> Result<symbiotic_ai_runtime::SpendReservation, EgressError> {
    Ok(symbiotic_ai_runtime::SpendReservation {
        reference: egress_reference(a)?,
        account: symbiotic_ai_runtime::account_scope(
            &symbiotic_ai_runtime::BindingIdentity::new(
                &route.tenant,
                &route.route,
                "ledger",
                &route.account,
            ),
            route.account_sharing_key.as_ref(),
        )
        .map_err(|_| EgressError::InvalidRequest)?,
        invocation: a.attempt_id().invocation_key()?,
        binding: invocation_binding(a)?,
        request_limit: route.provider_request_limit,
    })
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn request_budget_gates_retire_only_the_final_departing_key() {
        let gates = RequestBudgetDispatch::default();
        let unrelated: Vec<_> = (0..128)
            .map(|key| gates.gate(&format!("unrelated-{key}")).unwrap())
            .collect();
        let first = gates.gate("retiring").unwrap();
        let last = gates.gate("retiring").unwrap();
        drop(first);
        assert_eq!(gates.0.lock().unwrap().len(), 129);
        drop(last);
        assert_eq!(
            gates.0.lock().unwrap().len(),
            128,
            "the final departure retires its key without another admission"
        );
        drop(unrelated);
        assert!(gates.0.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn request_budget_gates_keep_same_key_waiters_together_and_allow_other_keys() {
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            let gates = RequestBudgetDispatch::default();
            let first = gates.gate("same").unwrap().lock_owned().await.unwrap();
            let waiter = gates.gate("same").unwrap().lock_owned();
            tokio::pin!(waiter);
            std::future::poll_fn(|cx| {
                assert!(std::future::Future::poll(waiter.as_mut(), cx).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
            let other = gates.gate("other").unwrap().lock_owned().await.unwrap();
            let same = gates.gate("same").unwrap();
            assert!(Arc::ptr_eq(
                same.gate.as_ref().unwrap(),
                gates.gate("same").unwrap().gate.as_ref().unwrap(),
            ));
            drop(first);
            let held = waiter.await.unwrap();
            drop(same);
            // The held guard and registration retain the gate during lookups.
            let later = gates.gate("same").unwrap();
            assert!(later.gate.as_ref().unwrap().try_lock().is_err());
            drop(later);
            drop(held);
            drop(other);
            assert!(gates.0.lock().unwrap().is_empty(), "idle keys retire");

            let first = gates.gate("cancelled").unwrap().lock_owned().await.unwrap();
            let mut waiter = Box::pin(gates.gate("cancelled").unwrap().lock_owned());
            std::future::poll_fn(|cx| {
                assert!(std::future::Future::poll(waiter.as_mut(), cx).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
            drop(first);
            assert_eq!(gates.0.lock().unwrap().len(), 1, "waiter retains its key");
            drop(waiter);
            assert!(
                gates.0.lock().unwrap().is_empty(),
                "cancelled waiter retires"
            );
        })
        .await
        .expect("bounded request budget gate regression");
    }

    #[test]
    fn startup_builds_the_validated_registry_once_before_state_access() {
        let dir = tempfile::tempdir().unwrap();
        let state_dir = dir.path().join("state-is-a-file");
        std::fs::write(&state_dir, b"untouched").unwrap();
        let route = serde_json::from_value(serde_json::json!({
            "tenant": "tenant", "account": "account", "account_sharing_key": null,
            "max_attempts": 3, "route": "chat", "secret_ref": "", "secret": {"backend": "none"},
            "destination": "https://example.com/v1", "model": "test-model",
            "provider": {"kind": "open_ai_chat", "operator": "test"},
            "allow_loopback_http": false, "max_field_bytes": 1024, "max_output_tokens": 100,
            "max_in_flight": 4, "requests_per_minute": null, "input_units_per_minute": null,
            "timeout_seconds": 1
        }))
        .unwrap();
        let config = ProcessConfig {
            version: PROTOCOL_VERSION,
            state_dir: state_dir.clone(),
            socket_path: dir.path().join("egress.sock"),
            admission_key: SecretSource::Resolver {
                name: "admission".into(),
                resolve: Arc::new(|_| panic!("state refusal must precede credential resolution")),
            },
            max_secret_bytes: 4096,
            max_frame_bytes: 8 * symbiotic_ai_runtime::model::DEFAULT_MAX_RESPONSE_BYTES as u32,
            max_connections: 8,
            io_timeout_seconds: 2,
            clock_rollback_warning_tolerance_seconds: 5,
            jobs: Default::default(),
            job_runner: Default::default(),
            passthrough: None,
            routes: vec![route],
        };
        provider::REGISTRY_BUILDS.set(0);
        assert_eq!(
            validate_routes(&config.routes, config.max_frame_bytes),
            Ok(())
        );
        assert_eq!(provider::REGISTRY_BUILDS.get(), 1);
        provider::REGISTRY_BUILDS.set(0);
        assert!(matches!(
            CredentialProcess::open(config),
            Err(EgressError::StateUnavailable)
        ));
        assert_eq!(provider::REGISTRY_BUILDS.get(), 1);
        assert_eq!(std::fs::read(state_dir).unwrap(), b"untouched");
    }

    #[test]
    fn dropping_process_lock_releases_it_with_an_inherited_descriptor() {
        let dir = tempfile::tempdir().unwrap();
        let lock = lock_process(dir.path()).unwrap();
        // dup and a child inheriting the descriptor retain the same flock.
        let inherited = lock.0.try_clone().unwrap();
        assert!(matches!(
            lock_process(dir.path()),
            Err(EgressError::StateUnavailable)
        ));
        drop(lock);
        let reopened = lock_process(dir.path());
        assert!(reopened.is_ok(), "inherited descriptor retained the lock");
        drop(inherited);
        assert!(matches!(
            lock_process(dir.path()),
            Err(EgressError::StateUnavailable)
        ));
    }
}

#[cfg(test)]
#[path = "../../test-support/request_allocations.rs"]
mod request_allocations;

#[cfg(test)]
mod request_encoding_tests {
    use super::*;

    #[test]
    fn request_encoding_typed_payload_stops_at_byte_limit() {
        let mut route = RouteConfig {
            tenant: "tenant".into(),
            account: "account".into(),
            account_sharing_key: None,
            provider_request_limit: None,
            max_attempts: 1,
            request_budget: None,
            answer_recovery: AnswerRecovery::default(),
            route: "chat".into(),
            secret_ref: String::new(),
            secret: SecretSource::None,
            destination: "http://127.0.0.1".into(),
            model: "model".into(),
            provider: RouteProvider::OpenAiChat {
                operator: "test".into(),
                thinking: None,
                reasoning_effort: None,
            },
            allow_loopback_http: true,
            max_input_bytes: 1024,
            max_response_bytes: 1024,
            max_field_bytes: 128,
            max_output_tokens: 10,
            max_in_flight: 1,
            requests_per_minute: None,
            input_units_per_minute: None,
            timeout_seconds: 1,
        };
        let mut payload = ProviderPayload::Chat(ChatRequest {
            messages: vec![ChatMessage {
                role: "user".into(),
                content: "hello\n\"é".into(),
            }],
            max_output_tokens: Some(10),
            temperature: None,
            response_format: None,
            role_binding: None,
            source: None,
            metadata: serde_json::Value::Null,
        });
        let before = serde_json::to_vec(&payload).unwrap();
        assert_eq!(encode_frame(&payload, before.len() as u32).unwrap(), before);
        route.max_input_bytes = before.len();
        assert_eq!(validate_payload(&route, &payload), Ok(()));
        route.max_input_bytes -= 1;
        assert_eq!(
            validate_payload(&route, &payload),
            Err(EgressError::LimitExceeded)
        );
        route.max_input_bytes = 1024;
        if let ProviderPayload::Chat(request) = &mut payload {
            request.messages[0].content = "x".repeat(128 * 1024);
        }
        let (result, allocated) =
            request_allocations::allocated(|| validate_payload(&route, &payload));
        assert_eq!(result, Err(EgressError::LimitExceeded));
        eprintln!(
            "typed payload rejected: {allocated} allocated bytes, cap {}",
            route.max_input_bytes
        );
        assert!(
            allocated <= 8 * route.max_input_bytes,
            "allocated {allocated} bytes for a {}-byte cap",
            route.max_input_bytes
        );
    }
}
