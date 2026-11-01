//! Memory attests durable input authorization; Foundation owns dispatch and accounting.
//! Sign admissions only after durability and ordered publication of the grant revision.
//! All protocol timestamps are absolute Unix seconds (UTC), never milliseconds.

pub use symbiotic_model::{
    ChatMessage, ChatRequest, ClassifierAnswer, ClassifierQuestion, ClassifyRequest,
    EmbeddingRequest, SpendReceiptRef, SpendState,
};

use async_trait::async_trait;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use zeroize::Zeroizing;

pub mod jobs_client;
pub use jobs_client::*;

#[cfg(unix)]
pub mod socket;

/// Encode a bounded JSON frame body using a capped writer.
pub fn encode_frame(value: &impl Serialize, max_bytes: u32) -> Result<Vec<u8>, EgressError> {
    struct Capped {
        bytes: Vec<u8>,
        max: usize,
    }
    impl std::io::Write for Capped {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            if self.bytes.len().saturating_add(buf.len()) > self.max {
                return Err(std::io::Error::other("frame limit"));
            }
            self.bytes.extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut buffer = Capped {
        bytes: Vec::new(),
        max: max_bytes as usize,
    };
    serde_json::to_writer(&mut buffer, value).map_err(|_| EgressError::LimitExceeded)?;
    Ok(buffer.bytes)
}

/// Current wire and operation version. Unknown versions fail closed.
pub const PROTOCOL_VERSION: u16 = 4;

/// Static, safe-to-log errors. Never carry transport/provider bodies or credentials.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[serde(rename_all = "snake_case")]
pub enum EgressError {
    /// Unsupported protocol/operation version.
    #[error("unsupported egress version")]
    Version,
    /// Invalid fields, digest or immutable invocation binding.
    #[error("invalid egress request")]
    InvalidRequest,
    /// Configured identity and response bounds cannot fit in the reply frame.
    #[error(
        "max_frame_bytes must be at least 4124 and at least 4096 + 24 * max_field_bytes + 4 * max_response_bytes"
    )]
    InvalidFrameConfiguration,
    /// App secret callbacks cannot be configured for a child process.
    #[error("app secret resolver requires thread mode; unavailable in child-process mode")]
    ResolverRequiresThreadMode,
    /// Admission authentication failed.
    #[error("invalid admission authentication")]
    Unauthorized,
    /// The route is unavailable or revoked for this attempt.
    #[error("route refused")]
    RouteRefused,
    /// Permit absent, mismatched, or already consumed.
    #[error("permit refused or already used")]
    PermitRefused,
    /// The signed exclusive authority deadline elapsed before acceptance; no charge.
    #[error("attempt authority expired before acceptance")]
    AuthorityExpired,
    /// A new attempt cannot reuse an unsettled uncertain dispatch.
    #[error("previous attempt requires reconciliation")]
    ReconciliationRequired,
    /// A successful invocation cannot acquire another dispatch attempt.
    #[error("invocation already completed")]
    InvocationComplete,
    /// Limits or enforceable reservation missing.
    #[error("egress budget refused")]
    BudgetRefused,
    /// Identical requests exhausted their shared failed-send allowance; no HTTP or charge.
    #[error("request failure budget exhausted; no provider send or charge")]
    RequestBudgetExhausted,
    /// Credential backend unavailable or incorrectly protected.
    #[error("credential unavailable")]
    CredentialUnavailable,
    /// Durable state failed; no new handoff is permitted.
    #[error("egress state unavailable")]
    StateUnavailable,
    /// The queue format is unsupported and requires an explicit database rebuild.
    #[error("unsupported_queue_schema: unsupported queue schema; rebuild the queue database")]
    UnsupportedQueueSchema,
    /// Frame or provider output exceeds configured limits.
    #[error("egress message exceeds configured limit")]
    LimitExceeded,
    /// Provider throttled the request; its delay is present only when reported.
    #[error("provider rate limited")]
    RateLimited {
        /// Provider Retry-After delay in seconds, with HTTP dates normalized.
        retry_after_seconds: Option<u64>,
    },
    /// Provider request or response read exceeded its configured deadline.
    #[error("provider request timed out; dispatch charge may be unknown")]
    Timeout,
    /// Provider rejected the request or returned an invalid/unsupported response.
    #[error("provider failed (HTTP status {status:?})")]
    Provider {
        /// HTTP status when the failure came from a non-success response.
        status: Option<u16>,
    },
    /// A successful HTTP response was not a valid provider answer: not JSON, or not the expected shape.
    /// Dispatch charge may be unknown.
    #[error("provider response is not a valid answer; dispatch charge may be unknown")]
    InvalidProviderJson,
    /// Protocol transport failed. A dispatch may already have incurred a charge.
    #[error("egress transport unavailable; dispatch charge may be unknown")]
    Transport,
}

/// Exact Memory K record, signed only after successful durability.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DurableAttempt {
    /// Tenant namespace.
    pub tenant: String,
    /// Restore incarnation; never reuse across restore.
    pub incarnation: String,
    /// Logical invocation, stable across retries.
    pub invocation_id: String,
    /// Authenticated queue namespace for a job invocation; None for unqueued work.
    /// Direct consumption of a job attempt carries the same namespace.
    pub job_queue: Option<String>,
    /// One-based attempt ordinal, strictly increasing; unissued attempts may leave gaps.
    pub attempt_ordinal: u32,
    /// Position of this attempt in Memory K's serialization order.
    pub record_sequence: u64,
    /// Trusted time at serialization, Unix seconds.
    pub recorded_at: u64,
    /// Exclusive authority deadline in Unix seconds, covering the earliest applicable
    /// expiry of caller/provider input authority. Signed and digested; Foundation
    /// checks its own clock inside the acceptance transaction before consuming/reserving.
    pub expires_at: u64,
    /// Exclusive Unix-second deadline for recovering terminal results. Signed and immutable.
    pub recovery_expires_at: u64,
    /// Verified caller binding.
    pub caller_binding: String,
    /// Configured provider principal/route identifier.
    pub route: String,
    /// Exact configured destination (base URL for chat; service URL for Gemini).
    pub destination: String,
    /// Exact provider model.
    pub model: String,
    /// Pinned HTTP method (`POST`).
    pub method: String,
    /// Tenant-scoped opaque secret reference; not a file path.
    pub secret_ref: String,
    /// Input manifest reference.
    pub manifest_ref: String,
    /// Lowercase SHA-256 of Memory's manifest bytes.
    pub input_manifest_digest: String,
    /// Lowercase SHA-256 from [`ProviderPayload::digest`].
    pub input_digest: String,
    /// Effective caller-and-provider input grant revision, checked at acceptance.
    pub grant_revision: u64,
}

/// Durable attempt identity, independent of its signed contents.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttemptId {
    /// Tenant namespace.
    pub tenant: String,
    /// Restore incarnation.
    pub incarnation: String,
    /// Logical invocation.
    pub invocation_id: String,
    /// Authenticated queue namespace for a job invocation; None for unqueued work.
    /// Direct consumption of a job attempt carries the same namespace.
    pub job_queue: Option<String>,
    /// One-based ordinal within the invocation.
    pub attempt_ordinal: u32,
}

impl AttemptId {
    /// Canonical signed invocation identity shared by issuance, consumption and lookup.
    pub fn invocation_key(&self) -> Result<String, EgressError> {
        digest(&(
            &self.tenant,
            &self.incarnation,
            &self.job_queue,
            &self.invocation_id,
        ))
    }
}

impl DurableAttempt {
    /// Identity used for idempotent issuance and authenticated status lookup.
    pub fn attempt_id(&self) -> AttemptId {
        AttemptId {
            tenant: self.tenant.clone(),
            incarnation: self.incarnation.clone(),
            invocation_id: self.invocation_id.clone(),
            job_queue: self.job_queue.clone(),
            attempt_ordinal: self.attempt_ordinal,
        }
    }
}

/// Authenticated lookup; knowing an attempt identity alone grants no result access.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignedAttemptId {
    /// Identity to query.
    pub attempt_id: AttemptId,
    /// Domain-separated HMAC-SHA256.
    pub authentication: String,
}

/// One supported provider call; no variant contains credentials or URLs.
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "request", rename_all = "snake_case")]
pub enum ProviderPayload {
    /// Chat through a configured OpenAI-compatible or Anthropic route.
    Chat(symbiotic_model::ChatRequest),
    /// Embeddings through a configured embedding adapter.
    Embedding(symbiotic_model::EmbeddingRequest),
    /// Reranking through a configured Cohere-compatible adapter.
    Rerank(symbiotic_model::RerankRequest),
    /// Typed state and questions through a configured Jev route.
    Classify(ClassifyRequest),
}

impl ProviderPayload {
    /// Digest the exact version-4 typed JSON representation; Memory uses this helper.
    pub fn digest(&self) -> Result<String, EgressError> {
        digest(self)
    }
}

/// Authenticated durable record. Signatures do not contain provider credentials.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignedAttempt {
    /// Complete immutable admission record.
    pub attempt: DurableAttempt,
    /// HMAC-SHA256 over domain-separated typed JSON, lowercase hex.
    pub authentication: String,
}

/// Ordered effective authorization revision for a tenant restore incarnation.
/// Covers caller and provider input authorization, including group dependencies.
/// Publish the initial revision before admissions and serialize updates with acceptance.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrantRevision {
    /// Tenant namespace.
    pub tenant: String,
    /// Restore incarnation.
    pub incarnation: String,
    /// Monotonically increasing effective revision; an exact replay is idempotent.
    pub revision: u64,
}

/// Authenticated revision publication; never a spend or settlement instruction.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignedGrantRevision {
    /// Effective revision to publish.
    pub grant: GrantRevision,
    /// Domain-separated HMAC-SHA256.
    pub authentication: String,
}

/// Admission MAC key, distinct from provider secrets. Non-Debug, non-Serialize.
/// Memory holds a signing copy; Foundation holds a verification copy.
pub struct AdmissionKey(Zeroizing<Vec<u8>>);

impl AdmissionKey {
    /// Construct from at least 32 bytes supplied by protected local configuration.
    pub fn new(bytes: Vec<u8>) -> Result<Self, EgressError> {
        let bytes = Zeroizing::new(bytes);
        if bytes.len() < 32 {
            return Err(EgressError::Unauthorized);
        }
        Ok(Self(bytes))
    }

    /// Attest an attempt after the Memory durability barrier (never before).
    pub fn sign_attempt(&self, attempt: DurableAttempt) -> Result<SignedAttempt, EgressError> {
        let authentication = self.sign(b"symbiotic-egress/v4/attempt\0", &attempt)?;
        Ok(SignedAttempt {
            attempt,
            authentication,
        })
    }

    /// Authorize result recovery for an attempt identity.
    pub fn sign_attempt_id(&self, attempt_id: AttemptId) -> Result<SignedAttemptId, EgressError> {
        let authentication = self.sign(b"symbiotic-egress/v4/attempt-status\0", &attempt_id)?;
        Ok(SignedAttemptId {
            attempt_id,
            authentication,
        })
    }

    /// Verify status authorization in constant time.
    pub fn verify_attempt_id(&self, signed: &SignedAttemptId) -> Result<(), EgressError> {
        self.verify(
            b"symbiotic-egress/v4/attempt-status\0",
            &signed.attempt_id,
            &signed.authentication,
        )
    }

    /// Publish a revision through the ordered trusted admission integration.
    pub fn sign_grant_revision(
        &self,
        grant: GrantRevision,
    ) -> Result<SignedGrantRevision, EgressError> {
        let authentication = self.sign(b"symbiotic-egress/v4/grant-revision\0", &grant)?;
        Ok(SignedGrantRevision {
            grant,
            authentication,
        })
    }

    /// Verify a durable admission attestation in constant time.
    pub fn verify_attempt(&self, signed: &SignedAttempt) -> Result<(), EgressError> {
        self.verify(
            b"symbiotic-egress/v4/attempt\0",
            &signed.attempt,
            &signed.authentication,
        )
    }

    /// Verify a revision publication in constant time.
    pub fn verify_grant_revision(&self, signed: &SignedGrantRevision) -> Result<(), EgressError> {
        self.verify(
            b"symbiotic-egress/v4/grant-revision\0",
            &signed.grant,
            &signed.authentication,
        )
    }

    fn mac(&self, domain: &[u8], value: &impl Serialize) -> Result<Hmac<Sha256>, EgressError> {
        let mut mac =
            Hmac::<Sha256>::new_from_slice(&self.0).map_err(|_| EgressError::Unauthorized)?;
        mac.update(domain);
        mac.update(&serde_json::to_vec(value).map_err(|_| EgressError::InvalidRequest)?);
        Ok(mac)
    }

    fn sign(&self, domain: &[u8], value: &impl Serialize) -> Result<String, EgressError> {
        Ok(hex::encode(
            self.mac(domain, value)?.finalize().into_bytes(),
        ))
    }

    fn verify(&self, domain: &[u8], value: &impl Serialize, tag: &str) -> Result<(), EgressError> {
        let tag = hex::decode(tag).map_err(|_| EgressError::Unauthorized)?;
        self.mac(domain, value)?
            .verify_slice(&tag)
            .map_err(|_| EgressError::Unauthorized)
    }
}

/// SHA-256 of typed JSON. Use the shared types, never independently serialize a map.
pub fn digest(value: &impl Serialize) -> Result<String, EgressError> {
    Ok(hex::encode(Sha256::digest(
        serde_json::to_vec(value).map_err(|_| EgressError::InvalidRequest)?,
    )))
}

/// Opaque single-use capability; deliberately has no Debug implementation.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DispatchPermit {
    /// Cryptographically random permit identifier.
    pub token: String,
    /// SHA-256 of the exact durable attempt record.
    pub attempt_digest: String,
}

/// The sole credential operation; values never cross this interface.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InjectProviderCredential {
    /// Must equal [`PROTOCOL_VERSION`].
    pub operation_version: u16,
    /// Signed durable attempt, including route, method, secret ref and input binding.
    pub admission: SignedAttempt,
    /// Permit issued for this exact record.
    pub permit: DispatchPermit,
    /// Bounded provider payload whose digest matches the record.
    pub payload: ProviderPayload,
}

/// Bounded normalized provider output, with no raw response/trace metadata.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProviderOutput {
    /// Model text and the provider's completion category when reported.
    Chat {
        /// Returned text, including output stopped at a token limit.
        text: String,
        /// None only when the provider did not report a finish reason.
        finish_reason: Option<FinishReason>,
    },
    /// Embedding vectors.
    Embedding {
        #[serde(deserialize_with = "deserialize_output_numbers")]
        vectors: Vec<Vec<f32>>,
        dimensions: usize,
    },
    /// Validated candidate indices and finite relevance scores.
    Rerank {
        #[serde(deserialize_with = "deserialize_output_numbers")]
        hits: Vec<symbiotic_model::RerankHit>,
    },
    /// Validated classification answers in request question order.
    Classify {
        /// Typed probabilities, choices and scores; no raw response or metadata.
        #[serde(deserialize_with = "deserialize_output_numbers")]
        answers: Vec<ClassifierAnswer>,
    },
}

// Internally tagged enums buffer fields through Serde's generic content model.
// With arbitrary_precision enabled for provider billing, JSON decimal numbers
// arrive there as maps. Restore JSON values before decoding typed output floats.
fn deserialize_output_numbers<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::de::DeserializeOwned,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    serde_json::from_value(value).map_err(serde::de::Error::custom)
}

/// Direct dispatch completion category; provider strings are normalized here.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FinishReason {
    /// Normal completion or a requested stop sequence.
    Stop,
    /// Output or context token capacity was exhausted.
    Length,
    /// Another provider-reported completion reason.
    Other,
}

/// Outcome of one attempted dispatch, safe to log (no provider error strings).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DispatchStatus {
    /// Successful provider answer.
    Succeeded,
    /// Provider failed; outcome/charge may be uncertain.
    ProviderFailed,
    /// No call made because credential loading failed.
    CredentialUnavailable,
}

/// Receipt without reusable cached response state.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DispatchReceipt {
    /// Exact attempt binding.
    pub attempt_digest: String,
    /// Accepted logical invocation and attempt identity.
    pub attempt_id: AttemptId,
    /// Canonical Foundation ledger reference retained in consumer provenance.
    pub reference: SpendReceiptRef,
    /// Typed completion state.
    pub status: DispatchStatus,
    /// Reported token/media/cost measurements, never inferred prices.
    pub usage: symbiotic_trace::UsageTrace,
    /// Foundation accounting observation; never a consumer settlement instruction.
    pub spend_state: SpendState,
}

/// Static provider-observation and runtime failures; never contain raw diagnostic text.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DispatchDiagnostic {
    /// The successful HTTP response exceeded the route's `max_response_bytes` bound.
    /// No provider body or response bytes are included; dispatch charge may be unknown.
    MaxResponseBytesExceeded,
    /// Malformed provider identity metadata was omitted from usage; the answer is retained.
    InvalidUsageIdentity,
    /// The provider supplied a malformed Retry-After hint; the HTTP class is preserved.
    InvalidRetryAfter,
    /// The paid response could not be recorded as complete in the runtime queue.
    QueueCompleteFailed,
    /// The runtime could not persist the invocation trace.
    TraceWriteFailed,
    /// The runtime could not persist its response cache.
    ResponseCacheWriteFailed,
}

/// Typed response to credential injection.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DispatchResult {
    /// Static failure code alongside accounting; absent only on success.
    pub error: Option<EgressError>,
    /// Additional provider-observation or runtime failures accompanying this result.
    pub diagnostics: Vec<DispatchDiagnostic>,
    /// False if completion/settlement failed; accounting remains unknown until recovery.
    pub receipt_persisted: bool,
    /// Accepted identity, receipt reference and accounting observation.
    pub receipt: DispatchReceipt,
    /// Present only on success; retained until the signed recovery deadline.
    pub output: Option<ProviderOutput>,
}

/// Durable execution state. A dispatched attempt must never be blindly resubmitted.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum AttemptStatus {
    /// No permit has been issued for this identity.
    NotIssued,
    /// Latest issued permit, not consumed, with current revision and unexpired authority.
    Permitted,
    /// Revision changed, authority expired or a successor was issued before handoff;
    /// no charge or allowance consumed. Supersession survives clock rollback.
    Invalidated,
    /// Permit consumed; completion is not durably known (including a process crash).
    Dispatched { receipt: DispatchReceipt },
    /// Attempt durably finished with answer recovery disabled; spend remains queryable.
    /// A caller needing an answer must start a new logical invocation.
    FinishedWithoutAnswer { receipt: DispatchReceipt },
    /// Recoverable typed output, usage and settlement.
    Completed { result: DispatchResult },
    /// Recoverable static safe error and settlement, possibly an unknown charge.
    Failed { result: DispatchResult },
    /// Terminal result recovery window elapsed; accounting remains available via Receipt.
    Expired,
}

/// New or existing capability with a snapshot of its execution state.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PermitGrant {
    /// Same capability on every replay of the exact signed attempt.
    pub permit: DispatchPermit,
    /// Current state; only Permitted can be dispatched.
    pub status: AttemptStatus,
}

/// Versioned request envelope. One frame/request/response per socket connection.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    /// Protocol version.
    pub version: u16,
    /// Operation.
    pub operation: Operation,
}

/// Protocol operations; no operation reads or returns a secret value.
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "operation", content = "body", rename_all = "snake_case")]
pub enum Operation {
    /// Atomic authenticated job enqueue.
    EnqueueJobs(Box<SignedJobsRequest>),
    /// Renew an unsent job's signed authority.
    AdmitJob(Box<SignedJobsRequest>),
    /// Bounded job delivery and admission notices.
    Completions(Box<SignedJobsRequest>),
    /// Fenced job confirmations.
    AckJobs(Box<SignedJobsRequest>),
    /// Scoped job cancellation.
    CancelJobs(Box<SignedJobsRequest>),
    /// Scoped erasure of an input owner's job copies and recovery answers.
    PurgeOwner(Box<SignedJobsRequest>),
    /// Content-free scoped job status.
    JobStatus(Box<SignedJobsRequest>),
    /// Verify durable admission and issue its single-use permit.
    IssuePermit(Box<SignedAttempt>),
    /// Recover execution state/results using an authenticated durable identity.
    AttemptStatus(SignedAttemptId),
    /// Inject credential and execute exactly one attempt.
    InjectProviderCredential(Box<InjectProviderCredential>),
    /// Publish the effective revision, serialized with dispatch acceptance.
    PublishGrantRevision(SignedGrantRevision),
    /// Query a consumed attempt's accounting; never replay its output.
    Receipt(SignedAttemptId),
}

/// Versioned response envelope.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Response {
    /// Protocol version.
    pub version: u16,
    /// Operation result or static safe error.
    pub result: Result<Reply, EgressError>,
}

/// Successful reply variants.
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "reply", content = "body", rename_all = "snake_case")]
pub enum Reply {
    /// Typed queue result; no ledger settlement instructions are accepted.
    Jobs(Result<JobsReply, JobError>),
    /// Newly issued or reattached capability and current execution state.
    Permit(PermitGrant),
    /// State and bounded recovery result for an authenticated attempt identity.
    AttemptStatus(AttemptStatus),
    /// Provider answer and usage.
    Dispatched(DispatchResult),
    /// Effective grant revision durably published.
    GrantRevisionPublished,
    /// Accounting only; absent while no handoff has occurred.
    Receipt(Option<DispatchReceipt>),
}

/// Memory's egress boundary: use a socket or in-process implementation, or a test double.
#[async_trait]
pub trait EgressClient: Send + Sync {
    /// Submit one versioned operation. Transport failures after dispatch are unknown charges.
    async fn exchange(&self, request: Request) -> Result<Response, EgressError>;

    /// Query an identity signed with AdmissionKey::sign_attempt_id, without issuing a permit.
    async fn attempt_status(
        &self,
        attempt_id: SignedAttemptId,
    ) -> Result<AttemptStatus, EgressError> {
        let response = self
            .exchange(Request {
                version: PROTOCOL_VERSION,
                operation: Operation::AttemptStatus(attempt_id),
            })
            .await?;
        if response.version != PROTOCOL_VERSION {
            return Err(EgressError::Version);
        }
        match response.result? {
            Reply::AttemptStatus(status) => Ok(status),
            _ => Err(EgressError::InvalidRequest),
        }
    }
}
