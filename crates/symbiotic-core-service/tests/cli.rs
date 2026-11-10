mod common;
use serde_json::{Value, json};
use std::{
    fs,
    io::Write,
    os::unix::{fs::PermissionsExt, net::UnixStream},
    path::Path,
    process::{Child, Command, Output, Stdio},
    time::{Duration, Instant},
};
const BIN: &str = env!("CARGO_BIN_EXE_symbiotic-core");
struct Server(Child);
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn command(state: &Path, name: &str, args: &Value) -> Output {
    Command::new(BIN)
        .arg("--state")
        .arg(state)
        .arg(name)
        .arg(args.to_string())
        .output()
        .unwrap()
}
fn ok(state: &Path, name: &str, args: Value) -> Value {
    let output = command(state, name, &args);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}
#[test]
fn standalone_cli_serves_calls_reconciles_and_creates_private_socket_and_backup() {
    let state = tempfile::tempdir().unwrap();
    let warden = tempfile::tempdir().unwrap();
    let mut server = Server(
        Command::new(BIN)
            .arg("--state")
            .arg(state.path())
            .arg("serve")
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if UnixStream::connect(state.path().join("core.sock")).is_ok() {
            break;
        }
        assert!(
            server.0.try_wait().unwrap().is_none(),
            "server exited before binding"
        );
        assert!(Instant::now() < deadline, "socket never became ready");
        std::thread::yield_now();
    }
    assert_eq!(
        fs::metadata(state.path()).unwrap().permissions().mode() & 0o777,
        0o700
    );
    assert_eq!(
        fs::metadata(state.path().join("core.sock"))
            .unwrap()
            .permissions()
            .mode()
            & 0o077,
        0,
        "the socket gives group and others no access"
    );
    // These manifest strings have a separately computed SHA-256 prefix.
    let id = "review_exa-d4fc28e945359adec59c1157c20af722";
    fs::write(
        warden.path().join("sandbox.json"),
        json!({"job_directory":"/jobs/review example","retirement_receipt":"receipt-1"})
            .to_string(),
    )
    .unwrap();
    let job = ok(
        state.path(),
        "job.create",
        json!({"id":id,"kind":"review","product":"warden"}),
    );
    assert_eq!(job["job"]["state"], "ready");
    let mut request = common::record("request", Some(91.0));
    request["job_id"] = json!(id);
    let recorded = ok(state.path(), "request.record", request);
    assert_eq!(recorded["duplicate"], false);
    let budgets = ok(state.path(), "budget.get", json!({"account":"account"}));
    assert_eq!(budgets["budgets"][0]["state"], "warning");
    let decision = budgets["budgets"][0]["open_decision_ids"][0].clone();
    let answered = ok(
        state.path(),
        "decision.answer",
        json!({"decision_id":decision,"revision":1,"answer":{"kind":"option","optionId":"continue_to_limit"},"answered_by":{"id":"operator","kind":"human","displayName":"Operator"}}),
    );
    assert_eq!(answered["decision"]["state"], "answered");
    assert_eq!(
        ok(
            state.path(),
            "job.transition",
            json!({"id":id,"to":"running"})
        )["job"]["state"],
        "running"
    );
    let events = ok(state.path(), "events.since", json!({}));
    assert_eq!(
        events["events"].as_array().unwrap().last().unwrap()["kind"],
        "job.started"
    );
    let output = Command::new(BIN)
        .arg("--state")
        .arg(state.path())
        .args([
            "reconcile",
            "--day",
            &chrono::Utc::now().format("%Y-%m-%d").to_string(),
            "--warden-state",
        ])
        .arg(warden.path())
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(0));
    assert_eq!(
        serde_json::from_slice::<Value>(&output.stdout).unwrap()["passed"],
        true
    );
    let backups = fs::read_dir(state.path().join("backups"))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(backups.len(), 1);
    assert!(
        backups[0]
            .file_name()
            .to_string_lossy()
            .ends_with(".sqlite")
    );
    let failure = command(
        state.path(),
        "job.transition",
        &json!({"id":"absent","to":"running"}),
    );
    assert_eq!(failure.status.code(), Some(1));
    assert_eq!(
        serde_json::from_slice::<Value>(&failure.stderr).unwrap()["reason"],
        "job_not_found"
    );
    let mut stdin = Command::new(BIN)
        .arg("--state")
        .arg(state.path())
        .args(["budget.get", "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    stdin
        .stdin
        .take()
        .unwrap()
        .write_all(b"{\"account\":\"absent\"}")
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&stdin.wait_with_output().unwrap().stdout).unwrap(),
        json!({"budgets":[]})
    );
    server.0.kill().unwrap();
    server.0.wait().unwrap();
    assert_eq!(
        command(state.path(), "budget.get", &json!({"account":"account"}))
            .status
            .code(),
        Some(2)
    );
}
#[test]
fn help_and_usage_have_distinct_exit_codes() {
    let output = Command::new(BIN).arg("--help").output().unwrap();
    assert_eq!(output.status.code(), Some(0));
    assert!(
        String::from_utf8(output.stdout)
            .unwrap()
            .contains("SYMBIOTIC_CORE_STATE")
    );
    let output = Command::new(BIN)
        .env_remove("SYMBIOTIC_CORE_STATE")
        .arg("serve")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
}
