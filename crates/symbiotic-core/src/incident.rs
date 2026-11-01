//! Incident: one recorded failure, closed only with a guard and its test.

use serde::{Deserialize, Serialize};

use crate::Timestamp;

/// Stable incident identifier, for example `inc-20261031-20`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct IncidentId(pub String);

/// One recorded failure.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Incident {
    pub id: IncidentId,
    pub class: IncidentClass,
    pub title: String,
    pub what: String,
    pub started: Timestamp,
    pub detected: Timestamp,
    pub detected_by: String,
    /// Why the failure went unseen until `detected`.
    pub escape: String,
    pub refs: Vec<String>,
    pub status: IncidentStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fix: Option<String>,
    /// The check or tool that makes this class impossible or visible at once.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub guard: Option<String>,
    /// The replay test that proves the guard.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub guard_test: Option<String>,
}

/// Failure classes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum IncidentClass {
    Preflight,
    Observability,
    Stall,
    PlanDrift,
    ReportDrift,
    CodeDefect,
    HostCapacity,
    ProviderAccount,
}

/// Incident life cycle.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum IncidentStatus {
    Open,
    /// The fix landed; the guard or its test is missing.
    Fixed,
    /// The guard and its test landed.
    Closed,
}
