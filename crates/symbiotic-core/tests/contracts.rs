//! Contract fixtures parse and round-trip without loss.
//!
//! Each fixture has the shape of a record of 2026-10-31. Quoted words and
//! free text are synthetic; identifiers, sessions and times the records do not
//! hold are placeholders.

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use symbiotic_core::decision::{
    Answer, AnswerFormat, DecisionEnvelope, MissingContext, QuestionKind, QuestionSeverity,
};
use symbiotic_core::event::{DetailValue, EventSeverity, SystemEvent};

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
    let mut event: Value =
        serde_json::from_str(include_str!("fixtures/system-event.json")).expect("fixture is JSON");
    event["unknownField"] = json!(1);
    assert!(serde_json::from_value::<SystemEvent>(event).is_err());
}
