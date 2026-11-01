//! Campaign: a goal with the operator's quoted instructions as data.
//!
//! Constraints and policies are checked on every step. A step that departs
//! from an instruction needs a `DECISION` record before it runs.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::Timestamp;
use crate::decision::QuestionKind;
use crate::event::{DetailValue, EventKind};

/// Stable campaign identifier.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct CampaignId(pub String);

/// A goal and its plan.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Campaign {
    pub id: CampaignId,
    pub goal: String,
    pub instructions: Vec<Instruction>,
    pub constraints: Vec<Constraint>,
    /// Recorded deviations from instructions, oldest first.
    pub decisions: Vec<DecisionRecord>,
    pub policies: Vec<Policy>,
}

/// One operator instruction, quoted exactly.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Instruction {
    pub id: String,
    pub quote: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub given_at: Option<Timestamp>,
}

/// A checkable rule derived from one instruction.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Constraint {
    pub id: String,
    pub instruction_id: String,
    pub rule: ConstraintRule,
}

/// The kinds of campaign constraint.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(
    tag = "kind",
    rename_all = "snake_case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum ConstraintRule {
    /// Every step in `first` finishes before any step in `then` starts; an
    /// empty `then` means every other step.
    Ordering {
        first: Vec<String>,
        then: Vec<String>,
    },
    /// A setting never goes below `value`, for example concurrency 16.
    Minimum { setting: String, value: f64 },
    /// A field every report must carry.
    RequiredReportField { field: String },
    /// Spend ceiling in US dollars, for one provider or for all when absent.
    SpendLimit {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        provider: Option<String>,
        period: SpendPeriod,
        limit_usd: f64,
    },
    /// A build rule compiled into worker briefs, for example `--release`.
    BuildRule { rule: String },
}

/// The period a spend limit covers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum SpendPeriod {
    Day,
    Run,
}

/// A recorded deviation: the instruction quoted, what changes, and why.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DecisionRecord {
    pub quote: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instruction_id: Option<String>,
    pub change: String,
    pub reason: String,
    pub decided_at: Timestamp,
}

/// Maps matching events, past a threshold, to actions run in order.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Policy {
    pub name: String,
    pub trigger: TriggerPattern,
    pub threshold: Threshold,
    pub actions: Vec<PolicyAction>,
}

/// Matches events of one kind whose subject and detail contain every listed
/// entry with an equal value.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TriggerPattern {
    pub event_kind: EventKind,
    pub subject: BTreeMap<String, String>,
    pub detail: BTreeMap<String, DetailValue>,
}

/// When matching events fire a policy.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(
    tag = "kind",
    rename_all = "snake_case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum Threshold {
    /// `count` matches, counted per distinct value of the `group_by` subject keys.
    Count {
        count: u32,
        consecutive: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        within_minutes: Option<u32>,
        group_by: Vec<String>,
    },
    /// No matching event for `minutes` while the run is active.
    Silence { minutes: u32 },
    /// A matching state lasts `minutes`, for example a session left waiting.
    Age { minutes: u32 },
}

/// One policy action.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(
    tag = "kind",
    rename_all = "snake_case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum PolicyAction {
    Record,
    Stop,
    Notify,
    /// Raise a decision of this kind.
    Ask {
        question_kind: QuestionKind,
    },
    /// Move to the campaign's next ready step that the event does not block.
    Divert,
    /// Set the remaining matching steps aside with the reason.
    Shelve,
}
