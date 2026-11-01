//! Canonical execution accounting, independent of optional trace sinks.
use crate::{DiagnosticCode, ModelError};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use symbiotic_trace::UsageTrace;

/// Opaque Foundation receipt identity for consumer provenance and status lookup.
/// At most [`Self::MAX_BYTES`] UTF-8 bytes; construction and decoding refuse overflow.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String")]
pub struct SpendReceiptRef(String);

impl SpendReceiptRef {
    /// Maximum stored reference size in UTF-8 bytes.
    // 256 gives generous headroom over emitted egress references (71 bytes)
    // and UUID runtime references (55 bytes at u32::MAX). Model enqueue checks
    // custom item IDs against the longest runtime reference before claiming.
    pub const MAX_BYTES: usize = 256;

    /// Construct a reference, refusing values longer than [`Self::MAX_BYTES`].
    pub fn new(value: impl Into<String>) -> Result<Self, ModelError> {
        let value = value.into();
        if value.len() > Self::MAX_BYTES {
            return Err(ModelError::InvalidRequest(
                DiagnosticCode::SpendReceiptRefTooLong,
            ));
        }
        Ok(Self(value))
    }

    /// Borrow the validated reference without permitting mutation.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for SpendReceiptRef {
    type Error = ModelError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

#[cfg(feature = "queue")]
pub(crate) fn runtime_reference(
    item_id: &str,
    attempt: u32,
) -> Result<SpendReceiptRef, ModelError> {
    SpendReceiptRef::new(format!("runtime:{item_id}:{attempt}"))
}

/// Per-execution receipt pointer; accounting remains in the canonical ledger.
#[doc(hidden)]
#[derive(Clone, Default)]
pub struct ExecutionAttemptContext(std::sync::Arc<std::sync::Mutex<Option<SpendReceiptRef>>>);
impl ExecutionAttemptContext {
    /// The exact accepted or recovered receipt observed by this execution.
    pub fn reference(&self) -> Result<Option<SpendReceiptRef>, ModelError> {
        self.0
            .lock()
            .map(|reference| reference.clone())
            .map_err(|_| storage())
    }
    #[cfg(feature = "queue")]
    pub(crate) fn clear(&self) -> Result<(), ModelError> {
        *self.0.lock().map_err(|_| storage())? = None;
        Ok(())
    }
    #[cfg(feature = "queue")]
    pub(crate) fn capture(&self, reference: &SpendReceiptRef) -> Result<(), ModelError> {
        *self.0.lock().map_err(|_| storage())? = Some(reference.clone());
        Ok(())
    }
}

/// Default runtime retention for queue state and explicit recovery answers.
pub const DEFAULT_RETENTION: std::time::Duration = std::time::Duration::from_secs(7 * 24 * 60 * 60);

/// Enforceable request accounting, separate from monetary observations.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpendState {
    /// Accepted, possibly dispatched: its request remains reserved.
    Unknown,
    /// Evidence establishes that no charge was incurred.
    Released,
    /// Successful request with measured provider usage.
    Settled,
}

/// Durable reservation identity. One request is reserved per accepted attempt.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpendReservation {
    pub reference: SpendReceiptRef,
    pub account: String,
    pub invocation: String,
    /// Immutable input/provider-binding digest for the logical invocation.
    /// Individual attempt identity belongs to `reference`.
    pub binding: String,
    /// Absolute account request allowance; no time window or monetary ceiling.
    pub request_limit: Option<u64>,
}

/// A single-use accepted attempt, bound to its account, input and runtime binding.
/// The ledger stores this identity before accepting the trusted handoff.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AcceptedSpendHandoff {
    /// Accepted account, logical invocation and exact attempt identity.
    pub reservation: SpendReservation,
    /// Hash of the operation, provider binding and exact queued request.
    pub input_identity: String,
}

/// Scope a caller's invocation to its exact tenant/provider/configuration binding.
pub fn execution_invocation_identity(
    binding: &symbiotic_core::BindingIdentity,
    invocation: &str,
) -> Result<String, ModelError> {
    crate::configuration_revision(&(binding, invocation)).map(|revision| revision.0)
}

/// Exact queued input identity, including operation and provider binding.
pub fn handoff_input_identity(
    kind: &str,
    binding: Option<&symbiotic_core::BindingIdentity>,
    request_hash: &str,
) -> Result<String, ModelError> {
    crate::configuration_revision(&(kind, binding, request_hash)).map(|revision| revision.0)
}

/// Authoritative receipt. Missing usage retains Unknown even with successful output.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SpendReceipt {
    pub reservation: SpendReservation,
    pub state: SpendState,
    /// Passthrough send time; absent for older receipts and typed calls.
    #[serde(default)]
    pub sent_at_ms: Option<i64>,
    /// Caller correlation identifier, never sent to the provider.
    #[serde(default)]
    pub caller_request_id: Option<String>,
    /// Settled cost in billionths of USD; absent when usage or price is unknown.
    #[serde(default)]
    pub cost_nano_usd: Option<u64>,
    /// Confirmed release before transport: this queue claim used no provider attempt.
    /// Ordinary Released accounting, including known-zero provider failures, still did.
    #[serde(default)]
    pub pre_dispatch_released: bool,
    pub usage: Option<UsageTrace>,
    /// Content-free completion evidence. Accounting survives recovery deletion.
    pub output: Option<Value>,
    /// Frozen ceiling, present only for runtime explicit invocations.
    pub attempt_limit: Option<u32>,
    /// Provider attempts carried forward on each explicit receipt.
    pub attempts_used: u32,
    /// Same-invocation answer, present only until discard or retention expiry.
    pub recovery: Option<Value>,
}

/// Accounting boundary used by every queued operation and credential handoff.
/// A successful reserve returns true only for a newly accepted attempt.
pub trait SpendLedger: Send + Sync {
    fn reserve(&self, reservation: &SpendReservation) -> Result<bool, ModelError>;
    /// Reserve an explicit invocation under its frozen provider-attempt ceiling.
    fn reserve_explicit(
        &self,
        reservation: &SpendReservation,
        limit: u32,
    ) -> Result<bool, ModelError>;
    /// Delete only the saved answer for an authenticated account/invocation.
    fn discard_recovery(&self, account: &str, invocation: &str) -> Result<(), ModelError>;
    /// Delete matching live recovery answers, preserving accounting evidence.
    /// The predicate runs under the ledger lock and must not call back into
    /// the runtime, including `spend_receipt` or `invocation_status`.
    fn purge_recovery(
        &self,
        matches: &dyn Fn(&Value) -> Result<bool, ModelError>,
    ) -> Result<usize, ModelError>;
    /// Atomically release a reservation and record that transport never started.
    /// Must not reclassify a provider attempt already settled or reconciled.
    fn release_before_dispatch(&self, reference: &SpendReceiptRef) -> Result<(), ModelError>;
    /// Atomically validate and consume dispatch ownership for an accepted handoff.
    /// Reuse, identity mismatch and non-unknown accounting are refused.
    fn acquire_handoff(
        &self,
        handoff: &AcceptedSpendHandoff,
        account: &str,
        input_identity: &str,
        owner: &str,
    ) -> Result<(), ModelError>;
    fn receipt(&self, reference: &SpendReceiptRef) -> Result<Option<SpendReceipt>, ModelError>;
    /// Latest accepted attempt for this account/invocation, including Released accounting.
    fn invocation(
        &self,
        account: &str,
        invocation: &str,
    ) -> Result<Option<SpendReceipt>, ModelError>;
    /// Settle accounting and recovery; supply the original key for explicit invocations.
    fn finish(
        &self,
        reference: &SpendReceiptRef,
        state: SpendState,
        usage: Option<UsageTrace>,
        output: Option<Value>,
        invocation: Option<&str>,
    ) -> Result<(), ModelError>;
}

pub(crate) fn storage() -> ModelError {
    ModelError::Queue(DiagnosticCode::SpendLedgerUnavailable)
}
#[cfg(feature = "queue")]
pub(crate) fn reconciliation() -> ModelError {
    ModelError::Queue(DiagnosticCode::SpendReconciliationRequired)
}

/// Refuse dispatch when no durable operational store was supplied.
#[doc(hidden)]
pub struct UnavailableSpendLedger;
impl SpendLedger for UnavailableSpendLedger {
    fn reserve(&self, _: &SpendReservation) -> Result<bool, ModelError> {
        Err(storage())
    }
    fn reserve_explicit(&self, _: &SpendReservation, _: u32) -> Result<bool, ModelError> {
        Err(storage())
    }
    fn discard_recovery(&self, _: &str, _: &str) -> Result<(), ModelError> {
        Err(storage())
    }
    fn purge_recovery(
        &self,
        _: &dyn Fn(&Value) -> Result<bool, ModelError>,
    ) -> Result<usize, ModelError> {
        Err(storage())
    }
    fn release_before_dispatch(&self, _: &SpendReceiptRef) -> Result<(), ModelError> {
        Err(storage())
    }
    fn acquire_handoff(
        &self,
        _: &AcceptedSpendHandoff,
        _: &str,
        _: &str,
        _: &str,
    ) -> Result<(), ModelError> {
        Err(storage())
    }
    fn receipt(&self, _: &SpendReceiptRef) -> Result<Option<SpendReceipt>, ModelError> {
        Err(storage())
    }
    fn invocation(&self, _: &str, _: &str) -> Result<Option<SpendReceipt>, ModelError> {
        Err(storage())
    }
    fn finish(
        &self,
        _: &SpendReceiptRef,
        _: SpendState,
        _: Option<UsageTrace>,
        _: Option<Value>,
        _: Option<&str>,
    ) -> Result<(), ModelError> {
        Err(storage())
    }
}

/// Whether usage was actually reported; absent fields never stand for zero.
pub fn has_measured_usage(u: &UsageTrace) -> bool {
    u.cache_hit_tokens.is_some()
        || u.cache_miss_tokens.is_some()
        || u.input_tokens.is_some()
        || u.output_tokens.is_some()
        || u.reasoning_tokens.is_some()
        || u.media_units.is_some()
        || u.cost_micro_usd.is_some()
        || u.reported_cost_usd.is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn receipt_ref_over_max_refused_at_construction_and_decode() {
        assert_eq!(SpendReceiptRef::MAX_BYTES, 256);
        for value in [
            "a".repeat(SpendReceiptRef::MAX_BYTES + 1),
            "é".repeat(SpendReceiptRef::MAX_BYTES / 2 + 1),
        ] {
            assert!(matches!(
                SpendReceiptRef::new(value.clone()),
                Err(ModelError::InvalidRequest(
                    DiagnosticCode::SpendReceiptRefTooLong
                ))
            ));
            let error =
                serde_json::from_value::<SpendReceiptRef>(serde_json::json!(value)).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains(DiagnosticCode::SpendReceiptRefTooLong.as_str())
            );
        }
        for value in [
            "a".repeat(SpendReceiptRef::MAX_BYTES),
            "é".repeat(SpendReceiptRef::MAX_BYTES / 2),
        ] {
            let reference = SpendReceiptRef::new(value.clone()).unwrap();
            assert_eq!(reference.as_str(), value);
            let encoded = serde_json::to_value(&reference).unwrap();
            assert_eq!(encoded, serde_json::json!(value));
            assert_eq!(
                serde_json::from_value::<SpendReceiptRef>(encoded).unwrap(),
                reference
            );
        }
    }

    #[cfg(feature = "queue")]
    #[test]
    fn emitted_egress_and_runtime_refs_within_max() {
        let item_id = symbiotic_core::QueueItemId::new();
        assert_eq!(item_id.0.len(), 36);
        let reference = runtime_reference(&item_id.0, u32::MAX).unwrap();
        assert_eq!(
            reference.as_str(),
            format!("runtime:{}:{}", item_id.0, u32::MAX)
        );
        assert_eq!(reference.as_str().len(), 55);
        let max_item_bytes = SpendReceiptRef::MAX_BYTES - "runtime:".len() - 1 - 10;
        assert_eq!(
            runtime_reference(&"a".repeat(max_item_bytes), u32::MAX)
                .unwrap()
                .as_str()
                .len(),
            SpendReceiptRef::MAX_BYTES
        );
        assert!(matches!(
            runtime_reference(&"a".repeat(max_item_bytes + 1), u32::MAX),
            Err(ModelError::InvalidRequest(
                DiagnosticCode::SpendReceiptRefTooLong
            ))
        ));
    }
}
