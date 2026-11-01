//! Decision envelope: the one object that asks a person for anything.
//!
//! It merges the runtime's question group (recommendation, confidence and
//! severity per question; unblock key) with Mission Control's question envelope
//! (kind, why-now, evidence, options, safe default, cost of delay, audience,
//! state). Text fields follow the House Rules question format.

use serde::{Deserialize, Serialize};

use crate::event::EventId;
use crate::{CampaignId, Timestamp};

/// Stable decision identifier.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct DecisionId(pub String);

/// A group of questions that resolve together, delivered as one page.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DecisionEnvelope {
    pub id: DecisionId,
    /// Raised on every content change; an answer names the revision it answers,
    /// so an answer to a stale revision is refused.
    pub revision: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub company_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub campaign_id: Option<CampaignId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub work_item_id: Option<String>,
    /// What the answered group unblocks; absent for decisions a policy raised.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unblock_key: Option<UnblockKey>,
    pub questions: Vec<DecisionQuestion>,
    /// The events that caused this decision.
    pub source_events: Vec<EventId>,
    /// The people who may answer.
    pub audience: Vec<ActorRef>,
    /// The sessions woken with the answer.
    pub wakes: Vec<ActorRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decision_by: Option<Timestamp>,
    pub created_at: Timestamp,
    pub state: DecisionState,
}

/// One numbered question of a decision.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DecisionQuestion {
    /// Stable within the decision.
    pub id: String,
    /// The topic heading the question is listed under.
    pub topic: String,
    pub kind: QuestionKind,
    /// The question heading: names the decision and ends with a question mark.
    pub prompt: String,
    /// Context sentences, each opened by its claim label.
    pub context: Vec<ContextSentence>,
    /// What happens after the answer, when the reader cannot infer it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_answer: Option<String>,
    pub why_now: String,
    pub evidence_refs: Vec<String>,
    pub answer_format: AnswerFormat,
    /// Empty unless the answer format is `option`.
    pub options: Vec<DecisionOption>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recommended_option_id: Option<String>,
    /// Confidence in the recommendation, from 0 to 1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confidence: Option<f64>,
    pub severity: QuestionSeverity,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub safe_default: Option<SafeDefault>,
    pub cost_of_delay: String,
    /// Escalation tiers tried before the question reached a person.
    pub resolution_trail: ResolutionTrail,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub answer: Option<AnswerRecord>,
}

/// Why a question needs a person.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum QuestionKind {
    Authority,
    Ambiguity,
    Preference,
    Contradiction,
    Blocker,
}

/// How much a wrong or late answer costs; `critical` always waits for a person.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum QuestionSeverity {
    Trivial,
    Informational,
    Decision,
    Critical,
}

/// One context sentence with its claim label.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ContextSentence {
    pub label: ContextLabel,
    pub text: String,
}

/// Claim labels allowed in question context.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum ContextLabel {
    Fact,
    Assessment,
}

/// The shape of a regular answer; `more_context` is always allowed besides.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum AnswerFormat {
    Option,
    Text,
    Number,
}

/// One option: what changes and its side effect.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DecisionOption {
    pub id: String,
    pub label: String,
    pub consequence: String,
    pub reversible: bool,
}

/// The option taken without an answer. It may only pause, defer or take a
/// pre-granted reversible action.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SafeDefault {
    pub option_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execute_after: Option<Timestamp>,
}

/// Life cycle of a decision.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum DecisionState {
    Draft,
    Open,
    Answered,
    Expired,
    Superseded,
}

/// An answer bound to the revision it answers.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AnswerRecord {
    pub revision: u32,
    pub answer: Answer,
    pub answered_by: ActorRef,
    pub answered_at: Timestamp,
}

/// An answer. `more_context` reopens the question with the named parts added.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(
    tag = "kind",
    rename_all = "snake_case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum Answer {
    Option {
        option_id: String,
    },
    Text {
        text: String,
    },
    Number {
        value: f64,
    },
    MoreContext {
        missing: Vec<MissingContext>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        note: Option<String>,
    },
}

/// The part a `more_context` answer asks for.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum MissingContext {
    Evidence,
    Alternatives,
    Cost,
    Example,
    Consequences,
}

/// The escalation tiers tried, in order.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ResolutionTrail {
    pub attempts: Vec<TierAttempt>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub escalation_reason: Option<String>,
}

/// One tier tried and its outcome.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TierAttempt {
    pub tier: EscalationTier,
    pub outcome: TierOutcome,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confidence: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    pub evidence_refs: Vec<String>,
}

/// Escalation ladder tiers, cheapest first.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum EscalationTier {
    Recall,
    Peer,
    Council,
    Operator,
}

/// What one tier did with the question.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum TierOutcome {
    /// The tier answered above its threshold.
    Resolved,
    /// The tier answered below its threshold, or not at all.
    Escalated,
    /// The tier was not allowed for this question.
    Skipped,
}

/// What an answered group unblocks, and which backend runs it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(
    tag = "kind",
    rename_all = "snake_case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum UnblockKey {
    Exploratory {
        topic: String,
    },
    AttachedRepo {
        repo_id: String,
        branch_hint: String,
        requires_approval: bool,
    },
    ResearchOnly {
        question: String,
    },
    Composite {
        children: Vec<UnblockKey>,
    },
}

/// A person, agent session or service.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ActorRef {
    pub id: String,
    pub kind: ActorKind,
    pub display_name: String,
}

/// Kinds of actor.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum ActorKind {
    Human,
    Agent,
    Service,
}
