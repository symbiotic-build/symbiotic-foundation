#![allow(dead_code)]
use serde_json::{Value, json};
use symbiotic_core_service::{Store, dispatch};

pub fn store() -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    (dir, store)
}
pub fn call(store: &mut Store, call: &str, args: Value) -> Value {
    serde_json::from_str(&dispatch(
        store,
        &json!({"call":call,"args":args}).to_string(),
    ))
    .unwrap()
}
pub fn ok(store: &mut Store, name: &str, args: Value) -> Value {
    let response = call(store, name, args);
    assert!(response.get("ok").is_some(), "{response}");
    response["ok"].clone()
}
pub fn time() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}
pub fn record(id: &str, used: Option<f64>) -> Value {
    json!({"request_id":id,"job_id":"job","account":"account","route":"route","model":"model","status":200,"time":time(),
        "usage":{"normalized":{"input_tokens":10,"output_tokens":3},"provider":{"extra":{"kept":true}}},
        "meter":{"readings":used.map(|n|vec![json!({"name":"weekly","unit":"percent","used":n,"limit":100.0})]).unwrap_or_default(),"provider":{}}})
}
pub fn events(store: &mut Store) -> Vec<Value> {
    ok(store, "events.since", json!({}))["events"]
        .as_array()
        .unwrap()
        .clone()
}
pub fn budget(store: &mut Store) -> Value {
    ok(store, "budget.get", json!({"account":"account"}))["budgets"][0].clone()
}
pub fn answer(store: &mut Store, id: &str, option: &str) -> Value {
    ok(
        store,
        "decision.answer",
        json!({"decision_id":id,"revision":1,"answer":{"kind":"option","optionId":option},"answered_by":{"id":"operator","kind":"human","displayName":"Operator"}}),
    )
}
pub fn create(store: &mut Store, id: &str) -> Value {
    ok(
        store,
        "job.create",
        json!({"id":id,"kind":"review","product":"test"}),
    )
}
