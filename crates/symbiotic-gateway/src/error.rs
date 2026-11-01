//! Static gateway failures; provider and credential data never enter diagnostics.

/// Failure at the gateway request or deployment boundary.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum GatewayError {
    /// Invalid fields, digest or immutable invocation binding.
    #[error("invalid egress request")]
    InvalidRequest,
    /// The configured header limit is below the minimum.
    #[error("max_header_bytes must be at least 4124")]
    InvalidHeaderLimit,
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
    /// Provider request or response read exceeded its configured deadline.
    #[error("provider request timed out; dispatch charge may be unknown")]
    Timeout,
    /// A successful HTTP response was not a valid provider answer: not JSON, or not the expected shape.
    /// Dispatch charge may be unknown.
    #[error("provider response is not a valid answer; dispatch charge may be unknown")]
    InvalidProviderJson,
    /// Protocol transport failed. A dispatch may already have incurred a charge.
    #[error("egress transport unavailable; dispatch charge may be unknown")]
    Transport,
}
