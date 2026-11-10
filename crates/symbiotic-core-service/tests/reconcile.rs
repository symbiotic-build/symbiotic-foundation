mod common;
use common::*;
use serde_json::{Value, json};
use std::{
    fs::{self, File},
    time::SystemTime,
};
const ID: &str = "review_exa-d4fc28e945359adec59c1157c20af722";
fn manifest(dir: &std::path::Path) {
    fs::create_dir_all(dir.join("nested")).unwrap();
    let path = dir.join("nested/sandbox.json");
    fs::write(
        &path,
        json!({"job_directory":"/jobs/review example","retirement_receipt":"receipt-1"})
            .to_string(),
    )
    .unwrap();
    File::options()
        .write(true)
        .open(path)
        .unwrap()
        .set_modified(SystemTime::now())
        .unwrap();
}
fn report(store: &mut symbiotic_core_service::Store, dir: &std::path::Path) -> Value {
    ok(
        store,
        "reconcile",
        json!({"day":chrono::Utc::now().format("%Y-%m-%d").to_string(),"warden_state":dir}),
    )
}
fn create_warden_job(store: &mut symbiotic_core_service::Store, id: &str) {
    ok(
        store,
        "job.create",
        json!({"id":id,"kind":"review","product":"warden"}),
    );
}
#[test]
fn warden_job_comparison_ignores_other_products() {
    for product in ["test", "Warden", "warden-other"] {
        let (_dir, mut store) = store();
        let warden = tempfile::tempdir().unwrap();
        ok(
            &mut store,
            "job.create",
            json!({"id":ID,"kind":"review","product":product}),
        );
        let result = report(&mut store, warden.path());
        assert_eq!(result["passed"], true, "{product}: {result}");
        assert_eq!(
            result["jobs"],
            json!({"in_core":0,"in_warden":0,"missing_from_core":[],"extra_in_core":[]})
        );
        manifest(warden.path());
        let result = report(&mut store, warden.path());
        assert_eq!(result["passed"], false, "{product}: {result}");
        assert_eq!(
            result["jobs"],
            json!({"in_core":0,"in_warden":1,"missing_from_core":[ID],"extra_in_core":[]})
        );
    }
}
#[test]
fn known_non_warden_requests_still_receive_usage_and_quota_checks() {
    let (_dir, mut store) = store();
    let warden = tempfile::tempdir().unwrap();
    create(&mut store, "job");
    let mut no_usage = record("without", None);
    no_usage["usage"] = Value::Null;
    no_usage["status"] = json!(500);
    ok(&mut store, "request.record", no_usage);
    let mut refused = record("refused", None);
    refused["account"] = json!("unmetered");
    refused["refused"] = json!("quota_exhausted");
    refused["usage"] = Value::Null;
    refused["status"] = json!(402);
    ok(&mut store, "request.record", refused);
    ok(&mut store, "request.record", record("full", Some(100.0)));
    ok(&mut store, "request.record", record("after", None));
    let result = report(&mut store, warden.path());
    assert_eq!(
        result["requests"],
        json!({"total":4,"without_usage":[{"request_id":"without","account":"account","route":"route","status":500}],"refused":[{"request_id":"refused","account":"unmetered","route":"route","status":402,"refused":"quota_exhausted"}],"unknown_job":[]})
    );
    assert_eq!(result["accounts_without_quota_meter"], json!(["unmetered"]));
    assert_eq!(
        result["quota"]["violations"],
        json!([{"budget_id":"account/weekly","request_id":"after","problem":"request sent while exhausted without an answered allow_over_limit decision"}])
    );
    assert_eq!(result["passed"], false);
    assert_eq!(
        result["jobs"],
        json!({"in_core":0,"in_warden":0,"missing_from_core":[],"extra_in_core":[]})
    );
}
#[test]
fn request_stops_being_unknown_when_any_product_creates_its_job() {
    let (_dir, mut store) = store();
    let warden = tempfile::tempdir().unwrap();
    ok(&mut store, "request.record", record("request", None));
    let result = report(&mut store, warden.path());
    assert_eq!(result["passed"], false);
    assert_eq!(result["requests"]["unknown_job"], json!(["request"]));
    create(&mut store, "job");
    let result = report(&mut store, warden.path());
    assert_eq!(
        result["requests"],
        json!({"total":1,"without_usage":[],"refused":[],"unknown_job":[]})
    );
    assert_eq!(result["passed"], true, "{result}");
    assert_eq!(
        result["jobs"],
        json!({"in_core":0,"in_warden":0,"missing_from_core":[],"extra_in_core":[]})
    );
}
#[test]
fn matching_warden_jobs_pass_and_model_job_id_matches_fixed_digest() {
    let (_dir, mut store) = store();
    let warden = tempfile::tempdir().unwrap();
    manifest(warden.path());
    // SHA-256('/jobs/review example' || 00 || 'receipt-1'), first 16 bytes.
    let empty = report(&mut store, warden.path());
    assert_eq!(empty["jobs"]["missing_from_core"], json!([ID]));
    create_warden_job(&mut store, ID);
    let result = report(&mut store, warden.path());
    assert_eq!(
        result,
        json!({"day":chrono::Utc::now().format("%Y-%m-%d").to_string(),"passed":true,"jobs":{"in_core":1,"in_warden":1,"missing_from_core":[],"extra_in_core":[]},"requests":{"total":0,"without_usage":[],"refused":[],"unknown_job":[]},"accounts_without_quota_meter":[],"quota":{"violations":[]},"warden_errors":[]})
    );
}
#[test]
fn missing_extra_without_usage_unknown_job_and_exhausted_requests_are_listed() {
    let (_dir, mut store) = store();
    let warden = tempfile::tempdir().unwrap();
    manifest(warden.path());
    create_warden_job(&mut store, "extra");
    create_warden_job(&mut store, "job");
    let mut no_usage = record("without", None);
    no_usage["usage"] = Value::Null;
    no_usage["status"] = json!(500);
    ok(&mut store, "request.record", no_usage);
    let mut unknown = record("unknown", None);
    unknown["job_id"] = json!("missing");
    ok(&mut store, "request.record", unknown);
    ok(&mut store, "request.record", record("full", Some(100.0)));
    ok(&mut store, "request.record", record("after", None));
    let result = report(&mut store, warden.path());
    assert_eq!(result["passed"], false);
    assert_eq!(result["jobs"]["missing_from_core"], json!([ID]));
    assert_eq!(result["jobs"]["extra_in_core"], json!(["extra", "job"]));
    assert_eq!(
        result["requests"]["without_usage"],
        json!([{"request_id":"without","account":"account","route":"route","status":500}])
    );
    assert_eq!(result["requests"]["unknown_job"], json!(["unknown"]));
    assert_eq!(
        result["quota"]["violations"],
        json!([{"budget_id":"account/weekly","request_id":"after","problem":"request sent while exhausted without an answered allow_over_limit decision"}])
    );
}
#[test]
fn account_without_budget_is_reported_without_failure_and_local_refusal_is_not_missing_usage() {
    let (_dir, mut store) = store();
    let warden = tempfile::tempdir().unwrap();
    // There must be a matching manifest for every Warden job in core.
    manifest(warden.path());
    create_warden_job(&mut store, ID);
    let mut request = record("refused", None);
    request["job_id"] = json!(ID);
    request["refused"] = json!("quota_exhausted");
    request["usage"] = Value::Null;
    request["status"] = json!(402);
    ok(&mut store, "request.record", request);
    let result = report(&mut store, warden.path());
    assert_eq!(result["accounts_without_quota_meter"], json!(["account"]));
    assert_eq!(result["requests"]["without_usage"], json!([]));
    assert_eq!(
        result["requests"]["refused"],
        json!([{"request_id":"refused","account":"account","route":"route","status":402,"refused":"quota_exhausted"}])
    );
    assert_eq!(result["passed"], true);
    assert_eq!(result["quota"]["violations"], json!([]));
    // A separate empty core file proves the unmetered account never changes passed.
    let (_other, mut clean) = common::store();
    create_warden_job(&mut clean, ID);
    let mut request = record("request", None);
    request["job_id"] = json!(ID);
    ok(&mut clean, "request.record", request);
    assert_eq!(report(&mut clean, warden.path())["passed"], true);
}
#[test]
fn allowance_is_replayed_and_reset_clears_it() {
    let (_dir, mut store) = store();
    let warden = tempfile::tempdir().unwrap();
    ok(&mut store, "request.record", record("full", Some(100.0)));
    let id = budget(&mut store)["open_decision_ids"][1]
        .as_str()
        .unwrap()
        .to_owned();
    answer(&mut store, &id, "allow_over_limit");
    ok(&mut store, "request.record", record("allowed", None));
    ok(&mut store, "request.record", record("reset", Some(10.0)));
    ok(
        &mut store,
        "request.record",
        record("full-again", Some(100.0)),
    );
    ok(&mut store, "request.record", record("violation", None));
    let result = report(&mut store, warden.path());
    assert_eq!(
        result["quota"]["violations"],
        json!([{"budget_id":"account/weekly","request_id":"violation","problem":"request sent while exhausted without an answered allow_over_limit decision"}])
    );
}
#[test]
fn malformed_manifests_fail_visibly_and_symlinks_are_not_followed() {
    let (_dir, mut store) = store();
    let warden = tempfile::tempdir().unwrap();
    let external = tempfile::tempdir().unwrap();
    manifest(external.path());
    std::os::unix::fs::symlink(external.path(), warden.path().join("linked")).unwrap();
    assert_eq!(report(&mut store, warden.path())["passed"], true);
    fs::write(warden.path().join("sandbox.json"), "{}").unwrap();
    let result = report(&mut store, warden.path());
    assert_eq!(result["passed"], false);
    assert_eq!(result["warden_errors"].as_array().unwrap().len(), 1);
    assert!(
        result["warden_errors"][0]["reason"]
            .as_str()
            .unwrap()
            .contains("job_directory")
    );
}

#[test]
fn warden_job_id_preserves_empty_basename_and_sanitizes_each_utf8_byte() {
    for (directory, receipt, expected) in [
        ("/", "receipt-root", "-a300a1fced95376f0cc3ec6ecbda54d7"),
        (
            "/jobs/abcdefghi😃",
            "receipt-2",
            "abcdefghi_-f906d798ddffa9d6f80013378bb917c2",
        ),
    ] {
        let (_dir, mut store) = store();
        let warden = tempfile::tempdir().unwrap();
        fs::write(
            warden.path().join("sandbox.json"),
            json!({"job_directory":directory,"retirement_receipt":receipt}).to_string(),
        )
        .unwrap();
        let result = report(&mut store, warden.path());
        assert_eq!(result["jobs"]["missing_from_core"], json!([expected]));
        assert_eq!(result["warden_errors"], json!([]));
    }
}

#[test]
fn exhaustion_without_warning_is_a_quota_violation() {
    let (dir, mut store) = store();
    let warden = tempfile::tempdir().unwrap();
    // An incomplete historical event stream must fail the reconciliation check.
    let conn = rusqlite::Connection::open(dir.path().join("core.sqlite")).unwrap();
    conn.execute("INSERT INTO events(id,time,subject_kind,subject_id,kind,payload) VALUES ('incomplete-history',?1,'budget','account/weekly','budget.exhausted',?2)",rusqlite::params![time(),json!({"budget_id":"account/weekly","account":"account","state":"exhausted"}).to_string()]).unwrap();
    let result = report(&mut store, warden.path());
    assert_eq!(result["passed"], false);
    assert_eq!(
        result["quota"]["violations"],
        json!([{"budget_id":"account/weekly","problem":"budget exhausted without a preceding warning since its last return to open"}])
    );
}

#[test]
fn yesterday_response_delivered_after_exhaustion_today_fails_yesterday_report() {
    let (_dir, mut store) = store();
    let warden = tempfile::tempdir().unwrap();
    ok(&mut store, "request.record", record("full", Some(100.0)));
    let yesterday = chrono::Utc::now().date_naive().pred_opt().unwrap();
    let mut late = record("late", None);
    late["time"] = json!(format!("{yesterday}T23:59:59.999Z"));
    ok(&mut store, "request.record", late);
    let result = ok(
        &mut store,
        "reconcile",
        json!({"day":yesterday.to_string(),"warden_state":warden.path()}),
    );
    assert_eq!(result["requests"]["total"], 1);
    assert_eq!(
        result["quota"]["violations"],
        json!([{"budget_id":"account/weekly","request_id":"late","problem":"request sent while exhausted without an answered allow_over_limit decision"}])
    );
    assert_eq!(result["passed"], false);
    assert_eq!(
        report(&mut store, warden.path())["quota"]["violations"],
        json!([])
    );
}

#[test]
fn utc_day_bounds_include_start_and_exclude_next_day_for_jobs_and_requests() {
    let (dir, mut store) = store();
    let warden = tempfile::tempdir().unwrap();
    let conn = rusqlite::Connection::open(dir.path().join("core.sqlite")).unwrap();
    for (id, created) in [
        ("before", "2026-11-01T23:59:59.999Z"),
        ("start", "2026-11-02T00:00:00.000Z"),
        ("end", "2026-11-02T23:59:59.999Z"),
        ("next", "2026-11-03T00:00:00.000Z"),
    ] {
        create_warden_job(&mut store, id);
        conn.execute(
            "UPDATE jobs SET created=?1 WHERE id=?2",
            rusqlite::params![created, id],
        )
        .unwrap();
    }
    for (id, time, job) in [
        ("before", "2026-11-02T00:59:59.999+01:00", "absent"),
        ("start", "2026-11-02T01:00:00+01:00", "start"),
        ("end", "2026-11-03T00:59:59.999999+01:00", "absent"),
        ("next", "2026-11-03T00:00:00Z", "absent"),
    ] {
        let mut args = record(id, None);
        args["time"] = json!(time);
        args["job_id"] = json!(job);
        ok(&mut store, "request.record", args);
    }
    let result = ok(
        &mut store,
        "reconcile",
        json!({"day":"2026-11-02","warden_state":warden.path()}),
    );
    assert_eq!(
        result["jobs"],
        json!({"in_core":2,"in_warden":0,"missing_from_core":[],"extra_in_core":["end","start"]})
    );
    assert_eq!(
        result["requests"],
        json!({"total":2,"without_usage":[],"refused":[],"unknown_job":["end"]})
    );
}
