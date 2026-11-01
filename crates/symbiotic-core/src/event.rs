//! System event: one typed fact a system emitted.
//!
//! Events come from the systems themselves, never from an agent's summary.
//! `detail` holds numbers and enumeration tokens only, so prompt, output and
//! transcript text cannot ride in an event.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{CampaignId, Timestamp};

/// Source-assigned event identifier; a repeated id is the same event.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct EventId(pub String);

/// Dotted event kind, subject first: `provider.refused`, `job.failed`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct EventKind(pub String);

/// One typed fact a system emitted.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SystemEvent {
    pub id: EventId,
    pub observed_at: Timestamp,
    pub source: EventSource,
    pub kind: EventKind,
    pub severity: EventSeverity,
    /// Without a campaign the event still records but triggers no campaign policy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub campaign_id: Option<CampaignId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    /// What the event is about: family, stage, provider, repository, pull request.
    pub subject: BTreeMap<String, String>,
    pub detail: BTreeMap<String, DetailValue>,
    /// Paths or URLs of the primary records.
    pub evidence_refs: Vec<String>,
}

/// The system that emitted an event.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EventSource {
    /// The kind of system, for example `warden` or `foundation-proxy`.
    pub kind: String,
    pub instance_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
}

/// How urgently an event needs attention.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum EventSeverity {
    Info,
    Attention,
    Blocking,
    Critical,
}

/// A detail value: a number or an enumeration token, never free text.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(untagged)]
pub enum DetailValue {
    Integer(i64),
    Number(f64),
    Token(EnumToken),
}

/// An enumeration value: 1 to 64 ASCII letters, digits, `_`, `.`, `:` or `-`.
/// The restriction keeps sentences out of event detail.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct EnumToken(String);

impl EnumToken {
    /// Longest accepted token, in bytes.
    pub const MAX_LEN: usize = 64;

    /// The token text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

// Written by hand: schemars drops field attributes behind `try_from`.
#[cfg(feature = "schema")]
impl schemars::JsonSchema for EnumToken {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "EnumToken".into()
    }

    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "description": "An enumeration value, never free text.",
            "type": "string",
            "pattern": "^[A-Za-z0-9_.:-]{1,64}$"
        })
    }
}

impl TryFrom<String> for EnumToken {
    type Error = EnumTokenError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let valid = !value.is_empty()
            && value.len() <= Self::MAX_LEN
            && value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b':' | b'-'));
        if valid {
            Ok(Self(value))
        } else {
            Err(EnumTokenError)
        }
    }
}

impl From<EnumToken> for String {
    fn from(token: EnumToken) -> Self {
        token.0
    }
}

/// A detail value that is not a number or an enumeration token. The refused
/// text is not retained.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("event detail value is not a number or an enumeration token")]
pub struct EnumTokenError;
