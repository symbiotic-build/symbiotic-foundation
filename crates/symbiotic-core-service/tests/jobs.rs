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

#[test]
fn running_job_heartbeat_stores_the_service_time_and_appends_no_event() {
    let (_dir, mut store) = store();
    let created = create(&mut store, "job");
    assert_eq!(
        created["job"].get("heartbeat_at"),
        Some(&serde_json::Value::Null)
    );
    let mut expected = ok(
        &mut store,
        "job.transition",
        json!({"id":"job","to":"running","result_ref":"result"}),
    )["job"]
        .clone();
    // Cost overflow must not prevent a liveness update.
    for id in ["request-1", "request-2"] {
        let mut request = record(id, None);
        request["usage"]["normalized"]["input_tokens"] = json!(u64::MAX);
        ok(&mut store, "request.record", request);
    }
    expected.as_object_mut().unwrap().remove("cost");
    let before = events(&mut store);
    for _ in 0..2 {
        let start = time();
        let heartbeat = ok(&mut store, "job.heartbeat", json!({"id":"job"}));
        let end = time();
        let timestamp = heartbeat["heartbeat_at"].as_str().unwrap();
        assert!((start.as_str()..=end.as_str()).contains(&timestamp));
        assert_eq!(heartbeat, json!({"id":"job","heartbeat_at":timestamp}));
        expected["heartbeat_at"] = json!(timestamp);
        assert_eq!(
            ok(&mut store, "job.list", json!({})),
            json!({"jobs":[expected],"next_after":null})
        );
    }
    assert_eq!(events(&mut store), before);
}

#[test]
fn heartbeat_rejects_missing_waiting_ready_and_final_jobs_without_writes() {
    let (_dir, mut store) = store();
    let b = ok(&mut store, "request.record", record("request", Some(91.0)))["budgets"][0].clone();
    ok(
        &mut store,
        "job.create",
        json!({"id":"waiting","kind":"review","product":"test","waiting_for":[b["open_decision_ids"][0]]}),
    );
    create(&mut store, "ready");
    for state in ["done", "failed", "cancelled"] {
        create(&mut store, state);
        ok(
            &mut store,
            "job.transition",
            json!({"id":state,"to":"running"}),
        );
        ok(&mut store, "job.heartbeat", json!({"id":state}));
        ok(&mut store, "job.transition", json!({"id":state,"to":state}));
    }
    let before_jobs = ok(&mut store, "job.list", json!({}));
    let before_events = events(&mut store);
    for (id, reason) in [
        ("missing", "job_not_found"),
        ("waiting", "job_not_running"),
        ("ready", "job_not_running"),
        ("done", "job_state_terminal"),
        ("failed", "job_state_terminal"),
        ("cancelled", "job_state_terminal"),
    ] {
        assert_eq!(
            call(&mut store, "job.heartbeat", json!({"id":id}))["error"]["reason"],
            reason
        );
    }
    for args in [
        json!({}),
        json!({"id":null}),
        json!({"id":1}),
        json!({"id":"ready","heartbeat_at":"2026-11-02T00:00:00Z"}),
    ] {
        assert_eq!(
            call(&mut store, "job.heartbeat", args)["error"]["reason"],
            "args_invalid"
        );
    }
    assert_eq!(ok(&mut store, "job.list", json!({})), before_jobs);
    assert_eq!(events(&mut store), before_events);
}

#[test]
fn job_events_never_carry_the_heartbeat() {
    let (_dir, mut store) = store();
    create(&mut store, "job");
    ok(
        &mut store,
        "job.transition",
        json!({"id":"job","to":"running"}),
    );
    let heartbeat = ok(&mut store, "job.heartbeat", json!({"id":"job"}));
    let mut finished = ok(
        &mut store,
        "job.transition",
        json!({"id":"job","to":"done"}),
    )["job"]
        .clone();
    assert_eq!(finished["heartbeat_at"], heartbeat["heartbeat_at"]);
    finished.as_object_mut().unwrap().remove("heartbeat_at");
    finished.as_object_mut().unwrap().remove("cost");
    let history = events(&mut store);
    assert_eq!(history.last().unwrap()["payload"], finished);
    for event in history {
        assert!(event["payload"].get("heartbeat_at").is_none());
        assert!(event["payload"].get("cost").is_none());
    }
}

#[test]
fn create_retry_after_heartbeat_returns_the_original_sequence() {
    let (_dir, mut store) = store();
    let created = create(&mut store, "job");
    let mut running = ok(
        &mut store,
        "job.transition",
        json!({"id":"job","to":"running"}),
    )["job"]
        .clone();
    let heartbeat = ok(&mut store, "job.heartbeat", json!({"id":"job"}));
    running["heartbeat_at"] = heartbeat["heartbeat_at"].clone();
    let before = events(&mut store);
    assert_eq!(
        create(&mut store, "job"),
        json!({"duplicate":true,"seq":created["seq"],"job":running})
    );
    assert_eq!(events(&mut store), before);
}

#[test]
fn job_list_filters_by_state_and_product_alone_and_together() {
    let (_dir, mut store) = store();
    let b = ok(&mut store, "request.record", record("request", Some(91.0)))["budgets"][0].clone();
    let mut all = Vec::new();
    for (index, state) in ["waiting", "ready", "running", "done", "failed", "cancelled"]
        .into_iter()
        .enumerate()
    {
        for product in ["test", "test-other"] {
            let id = format!("{index}-{product}");
            let mut args = json!({"id":id,"kind":"review","product":product});
            if state == "waiting" {
                args["waiting_for"] = json!([b["open_decision_ids"][0]]);
            }
            let mut job = ok(&mut store, "job.create", args)["job"].clone();
            if ["running", "done", "failed"].contains(&state) {
                job = ok(
                    &mut store,
                    "job.transition",
                    json!({"id":id,"to":"running"}),
                )["job"]
                    .clone();
            }
            if ["done", "failed", "cancelled"].contains(&state) {
                job = ok(&mut store, "job.transition", json!({"id":id,"to":state}))["job"].clone();
            }
            job.as_object_mut().unwrap().remove("cost");
            all.push(job);
        }
    }
    let before = events(&mut store);
    for args in [
        json!({}),
        json!({"product":"test"}),
        json!({"product":"TEST"}),
        json!({"product":"absent"}),
    ]
    .into_iter()
    .chain(
        ["waiting", "ready", "running", "done", "failed", "cancelled"]
            .into_iter()
            .flat_map(|state| {
                [
                    json!({"state":state}),
                    json!({"state":state,"product":"test"}),
                ]
            }),
    ) {
        let expected: Vec<_> = all
            .iter()
            .filter(|job| {
                args.get("state").is_none_or(|s| job["state"] == *s)
                    && args.get("product").is_none_or(|p| job["product"] == *p)
            })
            .collect();
        assert_eq!(
            ok(&mut store, "job.list", args.clone()),
            json!({"jobs":expected,"next_after":null}),
            "{args}"
        );
    }
    assert_eq!(events(&mut store), before);
}

#[test]
fn job_list_pages_by_id_and_reports_continuation() {
    let (_dir, mut store) = store();
    assert_eq!(
        ok(&mut store, "job.list", json!({})),
        json!({"jobs":[],"next_after":null})
    );
    let mut expected = Vec::new();
    for id in ["c", "a", "b"] {
        let mut job = create(&mut store, id)["job"].clone();
        job.as_object_mut().unwrap().remove("cost");
        expected.push(job);
    }
    expected.sort_by(|a, b| a["id"].as_str().cmp(&b["id"].as_str()));
    for (args, jobs, next) in [
        (json!({"limit":2}), expected[..2].to_vec(), json!("b")),
        (
            json!({"after":"b","limit":2}),
            expected[2..].to_vec(),
            json!(null),
        ),
        (
            json!({"after":"aa","limit":1}),
            expected[1..2].to_vec(),
            json!("b"),
        ),
        (json!({"limit":3}), expected.clone(), json!(null)),
        (json!({"limit":10000}), expected.clone(), json!(null)),
        (json!({"limit":0}), vec![], json!(null)),
        (json!({"after":"z"}), vec![], json!(null)),
        (
            json!({"state":"ready","product":"test","after":"a","limit":1}),
            expected[1..2].to_vec(),
            json!("b"),
        ),
    ] {
        assert_eq!(
            ok(&mut store, "job.list", args.clone()),
            json!({"jobs":jobs,"next_after":next}),
            "{args}"
        );
    }
    ok(
        &mut store,
        "job.transition",
        json!({"id":"c","to":"running"}),
    );
    assert_eq!(
        ok(&mut store, "job.list", json!({"state":"ready","limit":2})),
        json!({"jobs":expected[..2],"next_after":null})
    );
}

#[test]
fn job_list_returns_stored_fields_and_heartbeat_without_cost() {
    let (_dir, mut store) = store();
    let mut expected = ok(&mut store, "job.create", json!({"id":"job","kind":"review","product":"test","inputs":["input"],"rule_revision":"rules-1","model_route":"route","budget_id":"budget"}))["job"].clone();
    assert_eq!(expected.get("heartbeat_at"), Some(&serde_json::Value::Null));
    expected = ok(
        &mut store,
        "job.transition",
        json!({"id":"job","to":"running","result_ref":"result"}),
    )["job"]
        .clone();
    expected["heartbeat_at"] =
        ok(&mut store, "job.heartbeat", json!({"id":"job"}))["heartbeat_at"].clone();
    expected.as_object_mut().unwrap().remove("cost");
    ok(&mut store, "request.record", record("request", None));
    let before = events(&mut store);
    assert_eq!(
        ok(&mut store, "job.list", json!({})),
        json!({"jobs":[expected],"next_after":null})
    );
    assert_eq!(events(&mut store), before);
}

#[test]
fn job_list_rejects_invalid_filters_cursors_and_limits() {
    let (_dir, mut store) = store();
    for args in [
        json!({"state":"unknown"}),
        json!({"state":"RUNNING"}),
        json!({"state":null}),
        json!({"state":1}),
        json!({"product":null}),
        json!({"product":[]}),
        json!({"after":null}),
        json!({"after":1}),
        json!({"after":{}}),
        json!({"limit":null}),
        json!({"limit":-1}),
        json!({"limit":10001}),
        json!({"limit":1.5}),
        json!({"limit":"1"}),
        json!({"limit":u64::MAX}),
        json!({"unexpected":true}),
    ] {
        assert_eq!(
            call(&mut store, "job.list", args.clone())["error"]["reason"],
            "args_invalid",
            "{args}"
        );
    }
    assert_eq!(events(&mut store), Vec::<serde_json::Value>::new());
    assert_eq!(
        ok(&mut store, "job.list", json!({})),
        json!({"jobs":[],"next_after":null})
    );
}
