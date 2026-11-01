//! Contract fixtures parse and round-trip without loss.
//!
//! Each fixture has the shape of a record of 2026-10-31. Quoted words and
//! free text are synthetic; identifiers, sessions and times the records do not
//! hold are placeholders.

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use symbiotic_core::campaign::{Campaign, ConstraintRule, PolicyAction};
use symbiotic_core::decision::{
    Answer, AnswerFormat, DecisionEnvelope, MissingContext, QuestionKind, QuestionSeverity,
};
use symbiotic_core::event::{DetailValue, EventSeverity, SystemEvent};
use symbiotic_core::incident::{Incident, IncidentClass, IncidentStatus};
use symbiotic_core::signal::{Signal, SignalKind};

fn fixture<T: DeserializeOwned + Serialize>(name: &str) -> T {
    let path = format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"));
    let text = std::fs::read_to_string(&path).expect("fixture is readable");
    let raw: Value = serde_json::from_str(&text).expect("fixture is JSON");
    let parsed: T = serde_json::from_value(raw.clone()).expect("fixture matches its type");
    assert_eq!(
        serde_json::to_value(&parsed).expect("type serializes"),
        raw,
        "{name} round-trips without loss"
    );
    parsed
}

#[test]
fn deepseek_402_parses_as_a_critical_provider_refusal() {
    let event: SystemEvent = fixture("system-event.json");
    assert_eq!(event.kind.0, "provider.refused");
    assert_eq!(event.severity, EventSeverity::Critical);
    assert_eq!(event.detail["status"], DetailValue::Integer(402));
}

#[test]
fn hardware_key_blocker_parses_as_a_critical_option_decision() {
    let decision: DecisionEnvelope = fixture("decision-envelope.json");
    let question = &decision.questions[0];
    assert_eq!(question.kind, QuestionKind::Blocker);
    assert_eq!(question.severity, QuestionSeverity::Critical);
    assert_eq!(question.answer_format, AnswerFormat::Option);
    assert_eq!(
        question.recommended_option_id.as_deref(),
        Some("reauthenticate")
    );
}

#[test]
fn operator_words_parse_as_a_correction_signal() {
    let signal: Signal = fixture("signal.json");
    assert_eq!(signal.kind, SignalKind::Correction);
    assert_eq!(signal.quoted, "Do not stop that run");
}

#[test]
fn first_step_instruction_parses_as_an_ordering_constraint() {
    let campaign: Campaign = fixture("campaign.json");
    assert_eq!(
        campaign.constraints[0].rule,
        ConstraintRule::Ordering {
            first: vec!["alpha".into()],
            then: vec![]
        }
    );
    assert_eq!(
        campaign.policies[0].actions[1],
        PolicyAction::Ask {
            question_kind: QuestionKind::Authority
        }
    );
}

#[test]
fn ledger_incident_parses_with_its_class_and_status() {
    let incident: Incident = fixture("incident.json");
    assert_eq!(incident.class, IncidentClass::ReportDrift);
    assert_eq!(incident.status, IncidentStatus::Open);
    assert!(incident.guard.is_none());
}

#[test]
fn more_context_answer_names_the_missing_parts() {
    let answer: Answer = serde_json::from_value(json!({
        "kind": "more_context",
        "missing": ["evidence", "consequences"]
    }))
    .expect("more_context answer parses");
    assert_eq!(
        answer,
        Answer::MoreContext {
            missing: vec![MissingContext::Evidence, MissingContext::Consequences],
            note: None,
        }
    );
}

#[test]
fn event_detail_refuses_free_text() {
    let mut event: Value =
        serde_json::from_str(include_str!("fixtures/system-event.json")).expect("fixture is JSON");
    event["detail"]["reason"] = json!("the provider said the balance is too low");
    assert!(serde_json::from_value::<SystemEvent>(event).is_err());
}

#[test]
fn unknown_fields_are_refused() {
    let mut incident: Value =
        serde_json::from_str(include_str!("fixtures/incident.json")).expect("fixture is JSON");
    incident["detectionDelayMin"] = json!(447);
    assert!(serde_json::from_value::<Incident>(incident).is_err());
}

#[test]
fn tagged_variants_refuse_unknown_fields() {
    for action in [
        json!({ "kind": "ask", "questionKind": "authority", "extra": 1 }),
        json!({ "kind": "record", "extra": 1 }),
        json!({ "kind": "stop", "extra": 1 }),
        json!({ "kind": "notify", "extra": 1 }),
        json!({ "kind": "divert", "extra": 1 }),
        json!({ "kind": "shelve", "extra": 1 }),
    ] {
        assert!(
            serde_json::from_value::<PolicyAction>(action.clone()).is_err(),
            "{action} is refused"
        );
    }
}
