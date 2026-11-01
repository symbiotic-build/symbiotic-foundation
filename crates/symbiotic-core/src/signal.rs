//! Signal: the trigger classifier's reading of one operator message or agent reply.

use serde::{Deserialize, Serialize};

use crate::Timestamp;

/// Classifier-assigned signal identifier; a repeated id is the same signal.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct SignalId(pub String);

/// One classified message.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Signal {
    pub id: SignalId,
    pub observed_at: Timestamp,
    pub kind: SignalKind,
    /// The words that carry the signal, quoted exactly; never the whole message.
    pub quoted: String,
    /// The session the message belongs to.
    pub session: String,
    /// Zero-based turn index within the session.
    pub turn: u32,
    /// Classifier confidence, from 0 to 1.
    pub confidence: f32,
}

/// What a message carries.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum SignalKind {
    Correction,
    Preference,
    Problem,
    OperatorBlocker,
    /// The agent misread the operator.
    Confusion,
    /// The operator's words leave two readings.
    Clarification,
    None,
}
