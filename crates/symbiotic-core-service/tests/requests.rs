mod common;
use common::*;
use serde_json::json;
#[test]
fn request_retry_uses_original_sequence_and_rejects_changed_body() {
    let (_dir, mut store) = store();
    let args = record("request", None);
    let first = ok(&mut store, "request.record", args.clone());
    let duplicate = ok(&mut store, "request.record", args.clone());
    assert_eq!(
        duplicate,
        json!({"duplicate":true,"seq":first["seq"],"budgets":[]})
    );
    let mut changed = args;
    changed["status"] = json!(500);
    assert_eq!(
        call(&mut store, "request.record", changed)["error"]["reason"],
        "request_id_conflict"
    );
    let all = events(&mut store);
    assert_eq!(all.len(), 1);
    assert_eq!(all[0]["kind"], "model.request");
}
#[test]
fn job_cost_sums_each_present_usage_field_and_counts_missing_usage() {
    let (_dir, mut store) = store();
    create(&mut store, "job");
    ok(&mut store, "request.record", record("one", None));
    let mut two = record("two", None);
    two["usage"]["normalized"] = json!({"input_tokens":20,"cached_input_tokens":9,"cache_write_tokens":2,"output_tokens":5,"reasoning_tokens":1});
    ok(&mut store, "request.record", two);
    let mut three = record("three", None);
    three["usage"] = json!(null);
    ok(&mut store, "request.record", three);
    assert_eq!(
        create(&mut store, "job")["job"]["cost"],
        json!({"input_tokens":30,"cached_input_tokens":9,"cache_write_tokens":2,"output_tokens":8,"reasoning_tokens":1,"requests":3,"requests_without_usage":1})
    );
}
#[test]
fn invalid_normalized_usage_and_readings_fail_without_writes() {
    let (_dir, mut store) = store();
    for normalized in [
        json!({"input_tokens":-1}),
        json!({"output_tokens":1.5}),
        json!({"unknown":3}),
    ] {
        let mut request = record("bad", None);
        request["usage"]["normalized"] = normalized;
        assert_eq!(
            call(&mut store, "request.record", request)["error"]["reason"],
            "args_invalid"
        );
    }
    let mut bad = record("bad", Some(10.0));
    bad["meter"]["readings"][0]["read_at"] = json!("invalid");
    assert_eq!(
        call(&mut store, "request.record", bad)["error"]["reason"],
        "args_invalid"
    );
    // A zero limit would make every reading exhaust the budget.
    let mut zero = record("bad", Some(0.0));
    zero["meter"]["readings"][0]["limit"] = json!(0.0);
    assert_eq!(
        call(&mut store, "request.record", zero)["error"]["reason"],
        "args_invalid"
    );
    assert!(events(&mut store).is_empty());
}

#[test]
fn empty_meter_object_is_recorded_without_a_budget() {
    let (_dir, mut store) = store();
    let mut args = record("no-meter", None);
    args["meter"] = json!({});
    let result = ok(&mut store, "request.record", args.clone());
    assert_eq!(result["budgets"], json!([]));
    assert_eq!(events(&mut store)[0]["payload"], args);
}

#[test]
fn event_reads_page_by_sequence_and_reject_limits_above_the_contract_maximum() {
    let (_dir, mut store) = store();
    let first = ok(&mut store, "request.record", record("first", None));
    let second = ok(&mut store, "request.record", record("second", None));
    let page = ok(
        &mut store,
        "events.since",
        json!({"after":first["seq"],"limit":1}),
    );
    assert_eq!(
        page["events"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| e["subject_id"].clone())
            .collect::<Vec<_>>(),
        vec![json!("second")]
    );
    assert_eq!(page["last_seq"], second["seq"]);
    assert_eq!(
        ok(&mut store, "events.since", json!({"limit":0})),
        json!({"events":[],"last_seq":0})
    );
    assert_eq!(
        call(&mut store, "events.since", json!({"limit":10001}))["error"]["reason"],
        "args_invalid"
    );
}

#[test]
fn invalid_derived_reading_rolls_back_request_and_prior_readings() {
    let (_dir, mut store) = store();
    create(&mut store, "job");
    let mut args = record("invalid", Some(91.0));
    args["meter"]["readings"]
        .as_array_mut()
        .unwrap()
        .push(json!({"name":"overflow","unit":"credits","limit":1e308,"remaining":-1e308}));
    assert_eq!(
        call(&mut store, "request.record", args)["error"]["reason"],
        "args_invalid"
    );
    assert_eq!(
        ok(&mut store, "budget.get", json!({"account":"account"})),
        json!({"budgets":[]})
    );
    assert_eq!(
        events(&mut store)
            .iter()
            .map(|e| e["kind"].clone())
            .collect::<Vec<_>>(),
        vec![json!("job.created")]
    );
    assert_eq!(
        create(&mut store, "job")["job"]["cost"],
        json!({"requests":0,"requests_without_usage":0})
    );
}

#[test]
fn request_time_is_stored_as_utc_milliseconds_while_event_preserves_original_args() {
    let (dir, mut store) = store();
    let conn = rusqlite::Connection::open(dir.path().join("core.sqlite")).unwrap();
    for (id, time, expected) in [
        (
            "offset",
            "2026-11-03T00:59:59.123456+01:00",
            "2026-11-02T23:59:59.123Z",
        ),
        (
            "seconds",
            "2026-11-02T00:00:00Z",
            "2026-11-02T00:00:00.000Z",
        ),
    ] {
        let mut args = record(id, None);
        args["time"] = json!(time);
        ok(&mut store, "request.record", args.clone());
        let stored: String = conn
            .query_row(
                "SELECT time FROM requests WHERE request_id=?1",
                [id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stored, expected);
        assert_eq!(events(&mut store).last().unwrap()["payload"], args);
        assert_eq!(ok(&mut store, "request.record", args)["duplicate"], true);
    }
}
