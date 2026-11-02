mod common;
use common::*;
use serde_json::json;
#[test]
fn unanswered_decision_blocks_running_until_answered() {
    let (_dir, mut store) = store();
    let b = ok(&mut store, "request.record", record("request", Some(91.0)))["budgets"][0].clone();
    let id = b["open_decision_ids"][0].as_str().unwrap();
    let created = ok(
        &mut store,
        "job.create",
        json!({"id":"job","kind":"review","product":"test","waiting_for":[id]}),
    );
    assert_eq!(created["job"]["state"], "waiting");
    assert_eq!(
        call(
            &mut store,
            "job.transition",
            json!({"id":"job","to":"running"})
        )["error"]["reason"],
        "job_waiting_for_decision"
    );
    answer(&mut store, id, "continue_to_limit");
    assert_eq!(
        ok(
            &mut store,
            "job.transition",
            json!({"id":"job","to":"running"})
        )["job"]["state"],
        "running"
    );
}
#[test]
fn done_requires_running_and_terminal_jobs_refuse_transitions() {
    let (_dir, mut store) = store();
    for terminal in ["done", "failed", "cancelled"] {
        create(&mut store, terminal);
        if terminal != "cancelled" {
            assert_eq!(
                call(
                    &mut store,
                    "job.transition",
                    json!({"id":terminal,"to":terminal})
                )["error"]["reason"],
                "job_transition_invalid"
            );
            ok(
                &mut store,
                "job.transition",
                json!({"id":terminal,"to":"running"}),
            );
        }
        assert_eq!(
            ok(
                &mut store,
                "job.transition",
                json!({"id":terminal,"to":terminal,"result_ref":"result"})
            )["job"]["state"],
            terminal
        );
        assert_eq!(
            call(
                &mut store,
                "job.transition",
                json!({"id":terminal,"to":"running"})
            )["error"]["reason"],
            "job_state_terminal"
        );
    }
}
#[test]
fn create_is_idempotent_after_transitions_and_conflicts_on_changed_body() {
    let (_dir, mut store) = store();
    let created = create(&mut store, "job");
    ok(
        &mut store,
        "job.transition",
        json!({"id":"job","to":"running"}),
    );
    let before = events(&mut store).len();
    let duplicate = create(&mut store, "job");
    assert_eq!(duplicate["duplicate"], true);
    assert_eq!(duplicate["seq"], created["seq"]);
    assert_eq!(duplicate["job"]["state"], "running");
    assert_eq!(
        call(
            &mut store,
            "job.create",
            json!({"id":"job","kind":"other","product":"test"})
        )["error"]["reason"],
        "job_id_conflict"
    );
    assert_eq!(events(&mut store).len(), before);
    assert_eq!(
        call(
            &mut store,
            "job.create",
            json!({"id":"missing","kind":"review","product":"test","waiting_for":["absent"]})
        )["error"]["reason"],
        "decision_not_found"
    );
}
#[test]
fn dispatch_names_invalid_fields_and_unknown_calls() {
    let (_dir, mut store) = store();
    assert_eq!(
        call(&mut store, "absent", json!({}))["error"]["reason"],
        "call_unknown"
    );
    let response = call(
        &mut store,
        "job.create",
        json!({"id":"job","kind":3,"product":"test"}),
    );
    assert_eq!(response["error"]["reason"], "args_invalid");
    assert!(
        response["error"]["message"]
            .as_str()
            .unwrap()
            .contains("kind")
    );
    assert!(symbiotic_core_service::dispatch(&mut store, "bad json").contains("args_invalid"));
}
