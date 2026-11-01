mod common;
use common::*;
use serde_json::json;
#[test]
fn thresholds_open_one_decision_each_and_reset_withdraws_open_decisions() {
    let (_dir, mut store) = store();
    for (id, used, state, count) in [
        ("one", 85.0, "open", 0),
        ("two", 91.0, "warning", 1),
        ("three", 95.0, "warning", 1),
        ("four", 100.0, "exhausted", 2),
    ] {
        let b = ok(&mut store, "request.record", record(id, Some(used)))["budgets"][0].clone();
        assert_eq!(b["state"], state);
        assert_eq!(b["open_decision_ids"].as_array().unwrap().len(), count);
    }
    let b = budget(&mut store);
    let warning = b["open_decision_ids"][0].as_str().unwrap();
    let exhausted = b["open_decision_ids"][1].as_str().unwrap();
    answer(&mut store, exhausted, "allow_over_limit");
    assert_eq!(budget(&mut store)["over_limit_allowed"], true);
    ok(&mut store, "request.record", record("reset", Some(10.0)));
    let reset = budget(&mut store);
    assert_eq!(reset["state"], "open");
    assert_eq!(reset["over_limit_allowed"], false);
    assert_eq!(reset["open_decision_ids"], json!([]));
    let all = events(&mut store);
    let withdrawn = all
        .iter()
        .filter(|e| e["kind"] == "decision.withdrawn")
        .collect::<Vec<_>>();
    // The contract withdraws open decisions. An answered decision retains its answer.
    assert_eq!(withdrawn.len(), 1);
    assert_eq!(withdrawn[0]["subject_id"], warning);
    assert_eq!(
        call(
            &mut store,
            "decision.answer",
            json!({"decision_id":warning,"revision":1,"answer":{"kind":"option","optionId":"continue_to_limit"},"answered_by":{"id":"operator","kind":"human","displayName":"Operator"}})
        )["error"]["reason"],
        "decision_not_open"
    );
}
#[test]
fn old_reading_changes_nothing_and_reading_without_limit_is_not_a_budget() {
    let (_dir, mut store) = store();
    let mut newer = record("new", Some(91.0));
    newer["meter"]["readings"][0]["read_at"] = json!("2026-11-02T10:00:00Z");
    ok(&mut store, "request.record", newer);
    let before = budget(&mut store);
    let mut older = record("old", Some(10.0));
    older["meter"]["readings"][0]["read_at"] = json!("2026-11-02T09:00:00.999Z");
    ok(&mut store, "request.record", older);
    assert_eq!(budget(&mut store), before);
    let mut no_limit = record("no-limit", Some(10.0));
    no_limit["account"] = json!("other");
    no_limit["meter"]["readings"][0]
        .as_object_mut()
        .unwrap()
        .remove("limit");
    assert_eq!(
        ok(&mut store, "request.record", no_limit)["budgets"],
        json!([])
    );
}
#[test]
fn jump_to_limit_opens_warning_before_exhaustion_and_reset_withdraws_both_open_decisions() {
    let (_dir, mut store) = store();
    ok(&mut store, "request.record", record("half", Some(50.0)));
    ok(&mut store, "request.record", record("full", Some(100.0)));
    let relevant = events(&mut store)
        .into_iter()
        .filter(|e| {
            ["budget.warning", "budget.exhausted", "decision.opened"]
                .contains(&e["kind"].as_str().unwrap())
        })
        .map(|e| e["kind"].clone())
        .collect::<Vec<_>>();
    assert_eq!(
        relevant,
        json!([
            "budget.warning",
            "decision.opened",
            "budget.exhausted",
            "decision.opened"
        ])
        .as_array()
        .unwrap()
        .clone()
    );
    let b = budget(&mut store);
    for id in b["open_decision_ids"].as_array().unwrap() {
        let opened = events(&mut store)
            .into_iter()
            .find(|e| e["kind"] == "decision.opened" && e["subject_id"] == *id)
            .unwrap();
        let envelope: symbiotic_core::decision::DecisionEnvelope =
            serde_json::from_value(opened["payload"]["envelope"].clone()).unwrap();
        assert_eq!(envelope.revision, 1);
        assert_eq!(envelope.audience[0].id, "operator");
        assert_eq!(envelope.source_events.len(), 1);
    }
    ok(&mut store, "request.record", record("reset", Some(10.0)));
    assert_eq!(
        events(&mut store)
            .iter()
            .filter(|e| e["kind"] == "decision.withdrawn")
            .count(),
        2
    );
}
#[test]
fn more_context_keeps_decision_open_and_invalid_answers_fail() {
    let (_dir, mut store) = store();
    ok(&mut store, "request.record", record("warn", Some(91.0)));
    let id = budget(&mut store)["open_decision_ids"][0].clone();
    let args = json!({"decision_id":id,"revision":1,"answer":{"kind":"more_context","missing":["evidence"]},"answered_by":{"id":"operator","kind":"human","displayName":"Operator"}});
    let result = ok(&mut store, "decision.answer", args.clone());
    assert_eq!(result["decision"]["state"], "open");
    assert_eq!(
        result["decision"]["envelope"]["questions"][0]["answer"]["answer"],
        args["answer"]
    );
    for (field, value, reason) in [
        ("revision", json!(2), "decision_revision_stale"),
        (
            "answer",
            json!({"kind":"option","optionId":"nope"}),
            "answer_option_unknown",
        ),
        ("answer", json!({"kind":"text","text":"no"}), "args_invalid"),
    ] {
        let mut bad = args.clone();
        bad[field] = value;
        assert_eq!(
            call(&mut store, "decision.answer", bad)["error"]["reason"],
            reason
        );
    }
}
#[test]
fn remaining_reading_computes_spent_and_exhausted_to_warning_opens_no_new_decision() {
    let (_dir, mut store) = store();
    let mut request = record("full", Some(100.0));
    let reading = &mut request["meter"]["readings"][0];
    reading.as_object_mut().unwrap().remove("used");
    reading["remaining"] = json!(0.0);
    ok(&mut store, "request.record", request);
    assert_eq!(budget(&mut store)["spent"], 100.0);
    ok(&mut store, "request.record", record("warn", Some(95.0)));
    assert_eq!(budget(&mut store)["state"], "warning");
    assert_eq!(
        events(&mut store)
            .iter()
            .filter(|e| e["kind"] == "decision.opened")
            .count(),
        2
    );
}

#[test]
fn negative_remaining_is_preserved_and_marks_budget_exhausted() {
    let (_dir, mut store) = store();
    let mut request = record("overdrawn", None);
    request["meter"]["readings"] =
        json!([{"name":"weekly","unit":"credits","limit":100.0,"remaining":-5.0}]);
    let b = ok(&mut store, "request.record", request)["budgets"][0].clone();
    assert_eq!(b["spent"], 105.0);
    assert_eq!(b["state"], "exhausted");
    assert_eq!(b["reading"]["remaining"], -5.0);
}
