#![cfg(unix)]
use base64::{Engine, engine::general_purpose};
use std::{
    os::unix::fs::PermissionsExt,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use symbiotic_ai_runtime::model::{DEFAULT_MAX_REQUEST_BYTES, DEFAULT_MAX_RESPONSE_BYTES};
use symbiotic_credential_process::{
    CredentialProcess, InProcessEgressClient, ProcessConfig, RouteConfig, RouteProvider,
    secrets::{SecretSource, initialize_resolver_panic_hook},
    server,
};
use symbiotic_egress::*;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

const SECRET: &str = "synthetic-WP14-credential-\"/+?=é-canary";
const KEY: &[u8] = b"synthetic-admission-key-at-least-32-bytes";

fn initialize_panic_reporting() {
    static INITIALIZE: std::sync::Once = std::sync::Once::new();
    INITIALIZE.call_once(|| initialize_resolver_panic_hook(std::panic::take_hook()));
}

// Paths are read when the tests run, not compiled in: a compiled-in path makes this test build
// specific to one checkout, so no other worktree can reuse it from the build cache.
fn credential_process() -> std::path::PathBuf {
    std::env::var_os("CARGO_BIN_EXE_symbiotic-credential-process")
        .expect("cargo test sets CARGO_BIN_EXE_symbiotic-credential-process")
        .into()
}

#[cfg(target_os = "macos")]
fn manifest_dir() -> std::path::PathBuf {
    std::env::var_os("CARGO_MANIFEST_DIR")
        .expect("cargo test sets CARGO_MANIFEST_DIR")
        .into()
}

#[tokio::test]
async fn configuration_refuses_removed_secret_backend_before_startup() {
    let fixture = Fixture::new(200, "ok".into(), Duration::ZERO).await;
    for admission in [true, false] {
        let mut config = serde_json::to_value(&fixture.config).unwrap();
        let source = serde_json::json!({
            "backend": "macos_keychain",
            "service": "synthetic-service",
            "account": "synthetic-account"
        });
        if admission {
            config["admission_key"] = source;
        } else {
            config["routes"][0]["secret"] = source;
        }
        let error = serde_json::from_value::<ProcessConfig>(config.clone())
            .err()
            .expect("removed secret backend must be refused during deserialization");
        let diagnostic = error.to_string();
        assert!(diagnostic.contains("unknown variant `macos_keychain`"));
        assert!(diagnostic.contains("`none`"));
        assert!(diagnostic.contains("`owner_only_file`"));
        assert!(diagnostic.contains("`login_file`"));

        let path = fixture.dir.path().join("config.json");
        std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        let output = std::process::Command::new(credential_process())
            .arg(&path)
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert_eq!(
            String::from_utf8(output.stderr).unwrap().trim(),
            "invalid configuration: supported secret backends are `none`, `owner_only_file`, and `login_file`"
        );
        assert!(!fixture.config.state_dir.exists());
        assert!(!fixture.config.socket_path.exists());
    }
}

struct Child(std::process::Child);
impl Drop for Child {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

const OBSERVATION_HANG_GUARD: Duration = Duration::from_secs(60);

async fn wait_for_startup<T, F>(mut check_alive: impl FnMut(), mut probe: impl FnMut() -> F) -> T
where
    F: std::future::Future<Output = Result<T, EgressError>>,
{
    // The socket path can appear before the server accepts requests.
    tokio::time::timeout(OBSERVATION_HANG_GUARD, async {
        loop {
            check_alive();
            match probe().await {
                Ok(reply) => break reply,
                Err(EgressError::Transport) => tokio::time::sleep(Duration::from_millis(10)).await,
                Err(error) => panic!("child startup failed: {error:?}"),
            }
        }
    })
    .await
    .expect("child did not accept requests within the startup hang guard")
}

async fn wait_for_child_startup<T, F>(child: &mut Child, probe: impl FnMut() -> F) -> T
where
    F: std::future::Future<Output = Result<T, EgressError>>,
{
    wait_for_startup(
        || {
            if let Some(status) = child.0.try_wait().unwrap() {
                let mut stderr = Vec::new();
                if let Some(mut pipe) = child.0.stderr.take() {
                    std::io::Read::read_to_end(&mut pipe, &mut stderr).unwrap();
                }
                panic!(
                    "child exited before accepting requests: {status:?} stderr={}",
                    String::from_utf8_lossy(&stderr)
                );
            }
        },
        probe,
    )
    .await
}

async fn wait_for_engine_startup(pid: u32, ready_path: &std::path::Path) {
    wait_for_startup(
        || {
            // SAFETY: signal 0 only checks the first child, which the supervisor reaps.
            assert_eq!(
                unsafe { libc::kill(pid as i32, 0) },
                0,
                "engine exited before readiness: {}",
                std::io::Error::last_os_error()
            );
        },
        || async {
            if ready_path.exists() {
                Ok(())
            } else {
                Err(EgressError::Transport)
            }
        },
    )
    .await;
}

#[tokio::test(start_paused = true)]
#[should_panic(expected = "engine exited before readiness")]
async fn regression_engine_startup_fails_when_first_supervised_child_exits() {
    let dir = tempfile::tempdir().unwrap();
    let supervisor = symbiotic_supervise::Supervisor::start(
        || std::process::Command::new("/usr/bin/true"),
        symbiotic_supervise::Policy {
            max_restarts: 0,
            ..supervision_policy()
        },
    )
    .unwrap();
    let symbiotic_supervise::Event::Started(pid) = supervisor.next_event().unwrap() else {
        panic!("missing first engine");
    };
    assert!(matches!(
        supervisor.next_event().unwrap(),
        symbiotic_supervise::Event::Exited(_)
    ));
    // Reaping the first child must fail startup before the paused clock advances.
    wait_for_engine_startup(pid, &dir.path().join("engine.ready")).await;
}

#[tokio::test(start_paused = true)]
async fn regression_child_startup_wait_allows_delayed_readiness() {
    let mut child = Child(
        std::process::Command::new("/bin/sleep")
            .arg("60")
            .spawn()
            .unwrap(),
    );
    let started = tokio::time::Instant::now();
    let reply = wait_for_child_startup(&mut child, || async {
        if started.elapsed() < Duration::from_secs(4) {
            Err(EgressError::Transport)
        } else {
            Ok(Reply::GrantRevisionPublished)
        }
    })
    .await;
    assert!(matches!(reply, Reply::GrantRevisionPublished));
}

#[tokio::test(start_paused = true)]
#[should_panic(expected = "child exited before accepting requests")]
async fn regression_child_startup_wait_fails_before_probing_exited_child() {
    let mut child = Child(std::process::Command::new("/usr/bin/true").spawn().unwrap());
    assert!(child.0.wait().unwrap().success());
    wait_for_child_startup::<(), _>(&mut child, || async {
        panic!("an exited child must be detected before any readiness probe");
    })
    .await;
}

#[tokio::test(start_paused = true)]
#[should_panic(expected = "child did not accept requests within the startup hang guard")]
async fn regression_child_startup_wait_bounds_unready_child() {
    let mut child = Child(
        std::process::Command::new("/bin/sleep")
            .arg("60")
            .spawn()
            .unwrap(),
    );
    wait_for_child_startup(&mut child, || async {
        Err::<(), _>(EgressError::Transport)
    })
    .await;
}

fn process_config(dir: &std::path::Path) -> ProcessConfig {
    ProcessConfig {
        log: None,
        version: PROTOCOL_VERSION,
        state_dir: dir.join("state"),
        socket_path: dir.join("egress.sock"),
        admission_key: SecretSource::OwnerOnlyFile {
            path: dir.join("admission"),
        },
        max_secret_bytes: 4096,
        max_frame_bytes: 262144,
        max_connections: 8,
        io_timeout_seconds: 2,
        clock_rollback_warning_tolerance_seconds: 5,
        jobs: Default::default(),
        job_runner: Default::default(),
        passthrough: None,
        routes: Vec::new(),
    }
}

struct Fixture {
    dir: tempfile::TempDir,
    config: ProcessConfig,
    calls: Arc<AtomicUsize>,
    arrivals: Arc<tokio::sync::Semaphore>,
    requests: Arc<std::sync::Mutex<Vec<(String, serde_json::Value)>>>,
}
impl Fixture {
    async fn new(status: u16, output: String, delay: Duration) -> Self {
        Self::with_cost(status, output, delay, r#""0.00001234567890123456789""#).await
    }
    async fn with_cost(
        status: u16,
        output: String,
        delay: Duration,
        cost_json: &'static str,
    ) -> Self {
        Self::with_http_response(status, output, delay, cost_json, false, false).await
    }
    async fn with_http_response(
        status: u16,
        output: String,
        delay: Duration,
        cost_json: &'static str,
        raw_response: bool,
        keyless: bool,
    ) -> Self {
        Self::with_response_gate(
            status,
            output,
            delay,
            cost_json,
            raw_response,
            keyless,
            None,
        )
        .await
    }
    async fn with_response_gate(
        status: u16,
        output: String,
        delay: Duration,
        cost_json: &'static str,
        raw_response: bool,
        keyless: bool,
        response_gate: Option<Arc<tokio::sync::Semaphore>>,
    ) -> Self {
        Self::with_response_options(
            status,
            output,
            delay,
            cost_json,
            raw_response,
            keyless,
            (response_gate, "7"),
        )
        .await
    }
    async fn with_response_options(
        status: u16,
        output: String,
        delay: Duration,
        cost_json: &'static str,
        raw_response: bool,
        keyless: bool,
        response_options: (Option<Arc<tokio::sync::Semaphore>>, &'static str),
    ) -> Self {
        let (response_gate, retry_hint) = response_options;
        let dir = tempfile::tempdir().unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        for (name, value) in [("provider", SECRET.as_bytes()), ("admission", KEY)] {
            let path = dir.path().join(name);
            std::fs::write(&path, value).unwrap();
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let arrivals = Arc::new(tokio::sync::Semaphore::new(0));
        let arrived = arrivals.clone();
        let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
        let captured = requests.clone();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let output = output.clone();
                let count = count.clone();
                let arrived = arrived.clone();
                let captured = captured.clone();
                let response_gate = response_gate.clone();
                tokio::spawn(async move {
                    let mut data = Vec::new();
                    loop {
                        let mut chunk = [0u8; 4096];
                        let read = stream.read(&mut chunk).await.unwrap();
                        if read == 0 {
                            return;
                        }
                        data.extend_from_slice(&chunk[..read]);
                        if let Some(header_end) =
                            data.windows(4).position(|window| window == b"\r\n\r\n")
                        {
                            let headers = String::from_utf8_lossy(&data[..header_end]);
                            let len = headers
                                .lines()
                                .find_map(|line| {
                                    line.to_ascii_lowercase()
                                        .strip_prefix("content-length: ")
                                        .and_then(|len| len.parse::<usize>().ok())
                                })
                                .unwrap();
                            if data.len() >= header_end + 4 + len {
                                // Credential injection occurs solely in the HTTP header.
                                if keyless {
                                    assert!(
                                        !headers.to_ascii_lowercase().contains("authorization:")
                                    );
                                } else if headers.starts_with("POST /v1/messages ") {
                                    assert!(headers.contains(&format!("x-api-key: {SECRET}")));
                                    assert!(headers.contains("anthropic-version: 2023-06-01"));
                                } else {
                                    assert!(headers.contains(&format!("Bearer {SECRET}")));
                                }
                                assert!(
                                    !String::from_utf8_lossy(&data[header_end + 4..])
                                        .contains("synthetic-WP14-credential")
                                );
                                captured.lock().unwrap().push((
                                    headers.lines().next().unwrap().to_owned(),
                                    serde_json::from_slice(
                                        &data[header_end + 4..header_end + 4 + len],
                                    )
                                    .unwrap(),
                                ));
                                break;
                            }
                        }
                    }
                    count.fetch_add(1, Ordering::SeqCst);
                    arrived.add_permits(1);
                    if let Some(gate) = response_gate {
                        gate.acquire().await.unwrap().forget();
                    }
                    if !delay.is_zero() {
                        tokio::time::sleep(delay).await;
                    }
                    let body = if status == 200 && !raw_response {
                        // Insert the raw JSON literal so the test's own serde_json
                        // feature set cannot round a numeric cost before transmission.
                        format!(
                            r#"{{"choices":[{{"message":{{"content":{}}},"finish_reason":"stop"}}],"usage":{{"prompt_tokens":7,"completion_tokens":3,"cost":{cost_json}}}}}"#,
                            serde_json::to_string(&output).unwrap()
                        )
                    } else {
                        output
                    };
                    let response = format!(
                        "HTTP/1.1 {status} test\r\nContent-Type: application/json\r\nLocation: /not-approved\r\nRetry-After: {retry_hint}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = stream.write_all(response.as_bytes()).await;
                });
            }
        });
        let mut config = process_config(dir.path());
        config.routes = vec![RouteConfig {
            answer_recovery: Default::default(),
            tenant: "tenant".into(),
            account: "account".into(),
            account_sharing_key: None,
            provider_request_limit: None,
            max_attempts: 3,
            request_budget: None,
            route: "chat".into(),
            secret_ref: "provider-key".into(),
            secret: SecretSource::OwnerOnlyFile {
                path: dir.path().join("provider"),
            },
            destination: format!("http://{address}/v1"),
            model: "test-model".into(),
            provider: RouteProvider::OpenAiChat {
                operator: "test".into(),
                thinking: None,
                reasoning_effort: None,
            },
            allow_loopback_http: true,
            max_input_bytes: 32768,
            max_response_bytes: 32768,
            max_field_bytes: 1024,
            max_output_tokens: 100,
            max_in_flight: 4,
            requests_per_minute: None,
            input_units_per_minute: None,
            timeout_seconds: 1,
        }];
        Self {
            dir,
            config,
            calls,
            arrivals,
            requests,
        }
    }
    async fn process(&self) -> CredentialProcess {
        let process = CredentialProcess::open(self.config.clone()).unwrap();
        // Initial publication is explicit; restart cannot roll back a newer revision.
        match exchange(&process, publish_revision(1)).await {
            Ok(Reply::GrantRevisionPublished) | Err(EgressError::RouteRefused) => (),
            _ => panic!("initial revision publication failed"),
        }
        process
    }
    fn attempt(
        &self,
        invocation: &str,
        ordinal: u32,
        sequence: u64,
    ) -> (SignedAttempt, ProviderPayload) {
        let payload = ProviderPayload::Chat(ChatRequest {
            messages: vec![ChatMessage {
                role: "user".into(),
                content: "private test input".into(),
            }],
            max_output_tokens: Some(10),
            temperature: None,
            response_format: None,
            role_binding: None,
            source: None,
            metadata: serde_json::Value::Null,
        });
        let attempt = DurableAttempt {
            tenant: "tenant".into(),
            incarnation: "incarnation".into(),
            invocation_id: invocation.into(),
            job_queue: None,
            attempt_ordinal: ordinal,
            record_sequence: sequence,
            recorded_at: 100,
            expires_at: unix_seconds() + 3600,
            recovery_expires_at: 4_000_000_000,
            caller_binding: "caller".into(),
            route: "chat".into(),
            destination: self.config.routes[0].destination.clone(),
            model: "test-model".into(),
            method: "POST".into(),
            secret_ref: self.config.routes[0].secret_ref.clone(),
            manifest_ref: "manifest".into(),
            input_manifest_digest: "a".repeat(64),
            input_digest: payload.digest().unwrap(),
            grant_revision: 1,
        };
        (
            AdmissionKey::new(KEY.to_vec())
                .unwrap()
                .sign_attempt(attempt)
                .unwrap(),
            payload,
        )
    }
    fn job_attempt(
        &self,
        invocation: &str,
        ordinal: u32,
        sequence: u64,
    ) -> (SignedAttempt, ProviderPayload) {
        let (signed, payload) = self.attempt(invocation, ordinal, sequence);
        let mut attempt = signed.attempt;
        attempt.job_queue = Some(jobs_scope().queue);
        (
            AdmissionKey::new(KEY.to_vec())
                .unwrap()
                .sign_attempt(attempt)
                .unwrap(),
            payload,
        )
    }
}
fn unix_seconds() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

async fn exchange_wire(
    process: &CredentialProcess,
    operation: Operation,
) -> Result<Reply, EgressError> {
    let bytes = serde_json::to_vec(&Request {
        version: PROTOCOL_VERSION,
        operation,
    })
    .unwrap();
    let response = InProcessEgressClient::new(process.clone())
        .exchange(serde_json::from_slice(&bytes).unwrap())
        .await?;
    serde_json::from_slice::<Response>(&serde_json::to_vec(&response).unwrap())
        .unwrap()
        .result
}

#[tokio::test]
async fn authority_deadline_passed_between_check_and_acceptance_allows_reauthorization() {
    for restart in [false, true] {
        let mut fixture = Fixture::new(200, "reauthorized answer".into(), Duration::ZERO).await;
        fixture.config.routes[0].max_attempts = 1;
        fixture.config.routes[0].provider_request_limit = Some(1);
        let mut process = fixture.process().await;
        let key = AdmissionKey::new(KEY.to_vec()).unwrap();
        let (first, payload) = fixture.attempt("authority-deadline", 1, 10);
        let mut first = first.attempt;
        first.recorded_at = unix_seconds();
        first.expires_at = first.recorded_at + 2;
        let first = key.sign_attempt(first).unwrap();
        let granted = permit(&process, &first).await;
        assert!(unix_seconds() < first.attempt.expires_at);
        // Serialized after the consumer's successful check, but delayed until
        // Foundation's clock has reached the exclusive authority deadline.
        let bytes = serde_json::to_vec(&Request {
            version: PROTOCOL_VERSION,
            operation: inject(first.clone(), payload.clone(), granted.clone()),
        })
        .unwrap();
        while unix_seconds() < first.attempt.expires_at {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let response = InProcessEgressClient::new(process.clone())
            .exchange(serde_json::from_slice(&bytes).unwrap())
            .await
            .unwrap();
        let response: Response =
            serde_json::from_slice(&serde_json::to_vec(&response).unwrap()).unwrap();
        assert!(matches!(
            response.result,
            Err(EgressError::AuthorityExpired)
        ));
        assert!(matches!(
            status(&process, &first).await,
            AttemptStatus::Invalidated
        ));
        assert!(matches!(
            exchange(&process, Operation::Receipt(signed_id(&first))).await,
            Ok(Reply::Receipt(None))
        ));
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
        assert_eq!(ledger_totals(&fixture), (0, 0));
        let db = rusqlite::Connection::open(
            fixture
                .config
                .state_dir
                .join(symbiotic_ai_runtime::QUEUE_DATABASE),
        )
        .unwrap();
        assert_eq!(
            db.query_row(
                "SELECT consumed, accepted_attempts FROM egress_permits",
                [],
                |r| Ok((r.get::<_, bool>(0)?, r.get::<_, u32>(1)?))
            )
            .unwrap(),
            (false, 0)
        );
        drop(db);
        if restart {
            drop(process);
            process = fixture.process().await;
        }
        let reattached = permit(&process, &first).await;
        assert_eq!(reattached.token, granted.token);
        assert!(matches!(
            exchange_wire(&process, inject(first.clone(), payload.clone(), reattached)).await,
            Err(EgressError::AuthorityExpired)
        ));
        let mut second = first.attempt.clone();
        second.recorded_at = unix_seconds();
        second.expires_at = second.recorded_at + 3600;
        // A newly signed deadline cannot mutate an existing attempt identity.
        assert!(matches!(
            exchange_wire(
                &process,
                Operation::IssuePermit(key.sign_attempt(second.clone()).unwrap().into())
            )
            .await,
            Err(EgressError::InvalidRequest)
        ));
        second.attempt_ordinal += 1;
        second.record_sequence += 1;
        let second = key.sign_attempt(second).unwrap();
        let second_permit = permit(&process, &second).await;
        let result = dispatched(
            exchange_wire(&process, inject(second.clone(), payload, second_permit))
                .await
                .unwrap(),
        );
        assert_eq!(result.receipt.attempt_id, second.attempt.attempt_id());
        assert_eq!(result.receipt.status, DispatchStatus::Succeeded);
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
        assert_eq!(ledger_totals(&fixture), (1, 1));
    }
}

#[tokio::test]
async fn accepted_handoff_completes_and_recovers_after_authority_deadline() {
    let mut fixture = Fixture::new(200, "accepted answer".into(), Duration::from_secs(2)).await;
    fixture.config.routes[0].timeout_seconds = 4;
    let process = fixture.process().await;
    let (mut admission, payload) = fixture.attempt("accepted-deadline", 1, 10);
    admission.attempt.recorded_at = unix_seconds();
    admission.attempt.expires_at = admission.attempt.recorded_at + 2;
    let admission = AdmissionKey::new(KEY.to_vec())
        .unwrap()
        .sign_attempt(admission.attempt)
        .unwrap();
    let granted = permit(&process, &admission).await;
    let result = dispatched(
        exchange_wire(
            &process,
            inject(admission.clone(), payload.clone(), granted.clone()),
        )
        .await
        .unwrap(),
    );
    assert!(unix_seconds() >= admission.attempt.expires_at);
    assert_eq!(result.receipt.status, DispatchStatus::Succeeded);
    assert_eq!(result.receipt.spend_state, SpendState::Settled);
    drop(process);
    let process = fixture.process().await;
    let AttemptStatus::Completed { result: recovered } = status(&process, &admission).await else {
        panic!("accepted result was withdrawn after authority expiry");
    };
    assert_eq!(
        serde_json::to_value(result).unwrap(),
        serde_json::to_value(recovered).unwrap()
    );
    assert!(matches!(
        exchange_wire(&process, inject(admission, payload, granted)).await,
        Err(EgressError::PermitRefused)
    ));
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
    assert_eq!(ledger_totals(&fixture), (1, 1));
}

async fn exchange(process: &CredentialProcess, operation: Operation) -> Result<Reply, EgressError> {
    exchange_client(&InProcessEgressClient::new(process.clone()), operation).await
}

async fn exchange_client(
    client: &dyn EgressClient,
    operation: Operation,
) -> Result<Reply, EgressError> {
    client
        .exchange(Request {
            version: PROTOCOL_VERSION,
            operation,
        })
        .await?
        .result
}
async fn permit(process: &CredentialProcess, admission: &SignedAttempt) -> DispatchPermit {
    match exchange(process, Operation::IssuePermit(admission.clone().into()))
        .await
        .unwrap()
    {
        Reply::Permit(grant) => grant.permit,
        _ => panic!("wrong reply"),
    }
}
fn inject(admission: SignedAttempt, payload: ProviderPayload, permit: DispatchPermit) -> Operation {
    Operation::InjectProviderCredential(Box::new(InjectProviderCredential {
        operation_version: PROTOCOL_VERSION,
        admission,
        permit,
        payload,
    }))
}
fn dispatched(reply: Reply) -> DispatchResult {
    match reply {
        Reply::Dispatched(result) => result,
        _ => panic!("wrong reply"),
    }
}
fn signed_id(admission: &SignedAttempt) -> SignedAttemptId {
    AdmissionKey::new(KEY.to_vec())
        .unwrap()
        .sign_attempt_id(admission.attempt.attempt_id())
        .unwrap()
}
fn publish_revision(revision: u64) -> Operation {
    Operation::PublishGrantRevision(
        AdmissionKey::new(KEY.to_vec())
            .unwrap()
            .sign_grant_revision(GrantRevision {
                tenant: "tenant".into(),
                incarnation: "incarnation".into(),
                revision,
            })
            .unwrap(),
    )
}
async fn revoke(process: &CredentialProcess, revision: u64) {
    assert!(matches!(
        exchange(process, publish_revision(revision)).await.unwrap(),
        Reply::GrantRevisionPublished
    ));
}

#[tokio::test]
async fn permit_replay_refused_concurrently_and_after_restart() {
    let fixture = Fixture::new(200, "answer".into(), Duration::ZERO).await;
    let process = fixture.process().await;
    let (admission, payload) = fixture.attempt("replay", 1, 10);
    let permit = permit(&process, &admission).await;
    let request = inject(admission.clone(), payload, permit);
    let (first, second) = tokio::join!(
        exchange(&process, request.clone()),
        exchange(&process, request.clone())
    );
    assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
    assert!(
        matches!(first, Err(EgressError::PermitRefused))
            || matches!(second, Err(EgressError::PermitRefused))
    );
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
    drop(process);
    let process = fixture.process().await;
    assert!(matches!(
        exchange(&process, request).await,
        Err(EgressError::PermitRefused)
    ));
    assert!(matches!(
        exchange(&process, Operation::IssuePermit(admission.into())).await,
        Ok(Reply::Permit(PermitGrant {
            status: AttemptStatus::Completed { .. },
            ..
        }))
    ));
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn grant_updates_refuse_old_admissions_and_cannot_roll_back_after_restart() {
    let fixture = Fixture::new(200, "answer".into(), Duration::ZERO).await;
    let process = fixture.process().await;
    revoke(&process, 11).await;
    let (old, _) = fixture.attempt("old", 1, 10);
    assert!(matches!(
        exchange(&process, Operation::IssuePermit(old.into())).await,
        Err(EgressError::RouteRefused)
    ));
    assert!(matches!(
        exchange(&process, publish_revision(10)).await,
        Err(EgressError::RouteRefused)
    ));
    let (mut current, payload) = fixture.attempt("current", 1, 12);
    current.attempt.grant_revision = 11;
    current = AdmissionKey::new(KEY.to_vec())
        .unwrap()
        .sign_attempt(current.attempt)
        .unwrap();
    let granted = permit(&process, &current).await;
    let result = dispatched(
        exchange(&process, inject(current, payload, granted))
            .await
            .unwrap(),
    );
    assert_eq!(result.receipt.status, DispatchStatus::Succeeded);
    drop(process);
    let process = fixture.process().await;
    let (old, _) = fixture.attempt("old-restart", 1, 12);
    assert!(matches!(
        exchange(&process, Operation::IssuePermit(old.into())).await,
        Err(EgressError::RouteRefused)
    ));
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn grant_revocation_between_admission_and_dispatch_refuses_all_pending_admissions() {
    let fixture = Fixture::new(200, "answer".into(), Duration::ZERO).await;
    let mut process = fixture.process().await;
    let (early, early_payload) = fixture.attempt("early", 1, 10);
    let early_permit = permit(&process, &early).await;
    let mut refused = Vec::new();
    for sequence in [12, 11] {
        let (admission, payload) = fixture.attempt(&format!("late-{sequence}"), 1, sequence);
        let granted = permit(&process, &admission).await;
        refused.push((admission, payload, granted));
    }
    revoke(&process, 11).await;
    for restart in [false, true] {
        if restart {
            drop(process);
            process = fixture.process().await;
        }
        for (admission, payload, granted) in &refused {
            assert!(matches!(
                exchange(
                    &process,
                    inject(admission.clone(), payload.clone(), granted.clone())
                )
                .await,
                Err(EgressError::RouteRefused)
            ));
            assert!(matches!(
                status(&process, admission).await,
                AttemptStatus::Invalidated
            ));
            assert!(matches!(
                exchange(&process, Operation::Receipt(signed_id(admission))).await,
                Ok(Reply::Receipt(None))
            ));
            let reattached = permit(&process, admission).await;
            assert_eq!(reattached.token, granted.token);
            assert_eq!(reattached.attempt_digest, granted.attempt_digest);
        }
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
    }
    assert!(matches!(
        exchange(&process, inject(early, early_payload, early_permit)).await,
        Err(EgressError::RouteRefused)
    ));
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn invalidated_permit_allows_reauthorized_handoff_at_max_attempts_one() {
    for restart in [false, true] {
        let mut fixture = Fixture::new(200, "reauthorized answer".into(), Duration::ZERO).await;
        fixture.config.routes[0].max_attempts = 1;
        let mut process = fixture.process().await;
        let (first, payload) = fixture.attempt("reauthorized", 1, 10);
        let first_permit = permit(&process, &first).await;
        revoke(&process, 2).await;
        assert!(matches!(
            status(&process, &first).await,
            AttemptStatus::Invalidated
        ));
        assert!(matches!(
            exchange(
                &process,
                inject(first.clone(), payload.clone(), first_permit.clone())
            )
            .await,
            Err(EgressError::RouteRefused)
        ));
        assert_eq!(ledger_totals(&fixture), (0, 0));
        if restart {
            drop(process);
            process = fixture.process().await;
        }
        let key = AdmissionKey::new(KEY.to_vec()).unwrap();
        let mut second = first.attempt.clone();
        second.grant_revision = 2;
        // Reauthorization cannot mutate an existing attempt identity.
        assert!(matches!(
            exchange(
                &process,
                Operation::IssuePermit(key.sign_attempt(second.clone()).unwrap().into())
            )
            .await,
            Err(EgressError::InvalidRequest)
        ));
        second.attempt_ordinal = 2;
        second.record_sequence = 11;
        let second = key.sign_attempt(second).unwrap();
        let second_permit = permit(&process, &second).await;
        assert_ne!(second_permit.token, first_permit.token);
        assert!(matches!(
            status(&process, &first).await,
            AttemptStatus::Invalidated
        ));
        assert!(matches!(
            exchange(&process, Operation::Receipt(signed_id(&first))).await,
            Ok(Reply::Receipt(None))
        ));
        let reattached = permit(&process, &first).await;
        assert_eq!(reattached.token, first_permit.token);
        assert!(matches!(
            exchange(&process, inject(first, payload.clone(), reattached)).await,
            Err(EgressError::RouteRefused)
        ));
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
        let result = dispatched(
            exchange(&process, inject(second.clone(), payload, second_permit))
                .await
                .unwrap(),
        );
        assert_eq!(result.receipt.attempt_id, second.attempt.attempt_id());
        assert_eq!(result.receipt.status, DispatchStatus::Succeeded);
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
        assert_eq!(ledger_totals(&fixture), (1, 1));
    }
}

#[tokio::test]
async fn invalidated_permits_preserve_accepted_attempt_allowance_after_restart() {
    let mut fixture = Fixture::new(200, "unused".into(), Duration::ZERO).await;
    fixture.config.routes[0].max_attempts = 2;
    fixture.config.routes[0].secret = SecretSource::OwnerOnlyFile {
        path: fixture.dir.path().join("missing-provider"),
    };
    let mut process = fixture.process().await;
    let key = AdmissionKey::new(KEY.to_vec()).unwrap();
    for (ordinal, revision, consume) in [(1, 1, true), (2, 1, false), (3, 2, false), (4, 3, true)] {
        let (mut admission, payload) = fixture.attempt("allowance", ordinal, u64::from(ordinal));
        admission.attempt.grant_revision = revision;
        let admission = key.sign_attempt(admission.attempt).unwrap();
        let granted = permit(&process, &admission).await;
        if consume {
            let result = dispatched(
                exchange(&process, inject(admission, payload, granted))
                    .await
                    .unwrap(),
            );
            assert_eq!(result.receipt.spend_state, SpendState::Released);
        } else {
            revoke(&process, revision + 1).await;
            assert!(matches!(
                exchange(&process, inject(admission, payload, granted)).await,
                Err(EgressError::RouteRefused)
            ));
        }
        drop(process);
        process = fixture.process().await;
    }
    let (mut excess, _) = fixture.attempt("allowance", 5, 5);
    excess.attempt.grant_revision = 3;
    let excess = key.sign_attempt(excess.attempt).unwrap();
    assert!(matches!(
        exchange(&process, Operation::IssuePermit(excess.into())).await,
        Err(EgressError::BudgetRefused)
    ));
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
    assert_eq!(ledger_totals(&fixture), (0, 2));
}

#[tokio::test]
async fn wrong_input_or_attempt_cannot_spend_a_permit() {
    let fixture = Fixture::new(200, "answer".into(), Duration::ZERO).await;
    let process = fixture.process().await;
    let (admission, payload) = fixture.attempt("binding", 1, 10);
    let granted = permit(&process, &admission).await;
    let mut changed = payload.clone();
    if let ProviderPayload::Chat(request) = &mut changed {
        request.messages[0].content = "changed".into();
    }
    assert!(matches!(
        exchange(
            &process,
            inject(admission.clone(), changed, granted.clone())
        )
        .await,
        Err(EgressError::InvalidRequest)
    ));
    let (other, other_payload) = fixture.attempt("other", 1, 10);
    assert!(matches!(
        exchange(&process, inject(other, other_payload, granted.clone())).await,
        Err(EgressError::PermitRefused)
    ));
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        dispatched(
            exchange(&process, inject(admission, payload, granted))
                .await
                .unwrap()
        )
        .receipt
        .status,
        DispatchStatus::Succeeded
    );
}

#[tokio::test]
async fn unsigned_or_expired_at_record_authority_never_gets_a_permit() {
    let fixture = Fixture::new(200, "answer".into(), Duration::ZERO).await;
    let process = fixture.process().await;
    let (mut admission, _) = fixture.attempt("tamper", 1, 10);
    admission.attempt.route = "forged".into();
    assert!(matches!(
        exchange(&process, Operation::IssuePermit(admission.into())).await,
        Err(EgressError::Unauthorized)
    ));
    let (mut admission, _) = fixture.attempt("expired", 1, 10);
    admission.attempt.recorded_at = admission.attempt.expires_at;
    let admission = AdmissionKey::new(KEY.to_vec())
        .unwrap()
        .sign_attempt(admission.attempt)
        .unwrap();
    assert!(matches!(
        exchange(&process, Operation::IssuePermit(admission.into())).await,
        Err(EgressError::InvalidRequest)
    ));
    let (admission, _) = fixture.attempt("deadline-bounds", 1, 10);
    for expires_at in [unix_seconds(), i64::MAX as u64 + 1] {
        let mut attempt = admission.attempt.clone();
        attempt.expires_at = expires_at;
        let signed = AdmissionKey::new(KEY.to_vec())
            .unwrap()
            .sign_attempt(attempt)
            .unwrap();
        let expected = if expires_at > i64::MAX as u64 {
            EgressError::InvalidRequest
        } else {
            EgressError::AuthorityExpired
        };
        assert!(
            matches!(exchange(&process, Operation::IssuePermit(signed.into())).await, Err(error) if error == expected)
        );
    }
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn retry_is_a_new_attempt_same_invocation_without_response_cache() {
    let fixture = Fixture::new(200, "answer".into(), Duration::ZERO).await;
    let process = fixture.process().await;
    let key_path = fixture.dir.path().join("provider");
    std::fs::remove_file(&key_path).unwrap();
    let (first, payload) = fixture.attempt("retry", 1, 1);
    let granted = permit(&process, &first).await;
    let failed = dispatched(
        exchange(&process, inject(first, payload, granted))
            .await
            .unwrap(),
    );
    assert_eq!(failed.receipt.status, DispatchStatus::CredentialUnavailable);
    assert_eq!(failed.error, Some(EgressError::CredentialUnavailable));
    std::fs::write(&key_path, SECRET).unwrap();
    std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600)).unwrap();
    // The retry and a different invocation have byte-identical provider input.
    // Both must dispatch; no result cache may answer the second invocation.
    for (invocation, ordinal, sequence) in [("retry", 2, 2), ("other-invocation", 1, 3)] {
        let (admission, payload) = fixture.attempt(invocation, ordinal, sequence);
        let granted = permit(&process, &admission).await;
        let result = dispatched(
            exchange(&process, inject(admission, payload, granted))
                .await
                .unwrap(),
        );
        assert_eq!(result.receipt.status, DispatchStatus::Succeeded);
        assert_eq!(result.receipt.usage.input_tokens, Some(7));
        assert_eq!(result.receipt.usage.output_tokens, Some(3));
        assert!(result.receipt_persisted);
    }
    let (completed, _) = fixture.attempt("retry", 3, 4);
    assert!(matches!(
        exchange(&process, Operation::IssuePermit(completed.into())).await,
        Err(EgressError::InvocationComplete)
    ));
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 2);
    assert!(!fixture.config.state_dir.join("responses").exists());
    for entry in std::fs::read_dir(&fixture.config.state_dir).unwrap() {
        let bytes = std::fs::read(entry.unwrap().path()).unwrap();
        for canary in [SECRET.as_bytes(), b"private test input"] {
            assert!(!bytes.windows(canary.len()).any(|window| window == canary));
        }
    }
}

#[tokio::test]
async fn unknown_charge_stays_reserved_and_prevents_blind_retry_after_restart() {
    let fixture = Fixture::new(200, "late answer".into(), Duration::from_secs(2)).await;
    let process = fixture.process().await;
    let (admission, payload) = fixture.attempt("timeout", 1, 1);
    let granted = permit(&process, &admission).await;
    let result = dispatched(
        exchange(&process, inject(admission.clone(), payload, granted))
            .await
            .unwrap(),
    );
    assert_eq!(result.receipt.status, DispatchStatus::ProviderFailed);
    assert_eq!(result.error, Some(EgressError::Timeout));
    assert_eq!(result.receipt.spend_state, SpendState::Unknown);
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
    assert_eq!(ledger_totals(&fixture), (1, 1));
    drop(process);
    let process = fixture.process().await;
    let receipt = exchange(&process, Operation::Receipt(signed_id(&admission)))
        .await
        .unwrap();
    assert!(matches!(
        receipt,
        Reply::Receipt(Some(DispatchReceipt {
            spend_state: SpendState::Unknown,
            ..
        }))
    ));
    let (retry, _) = fixture.attempt("timeout", 2, 2);
    assert!(matches!(
        exchange(&process, Operation::IssuePermit(retry.into())).await,
        Err(EgressError::ReconciliationRequired)
    ));
}

#[tokio::test]
async fn caller_cancellation_does_not_cancel_started_dispatch() {
    let fixture = Fixture::new(200, "answer".into(), Duration::from_millis(80)).await;
    let process = fixture.process().await;
    let (admission, payload) = fixture.attempt("cancel", 1, 1);
    let granted = permit(&process, &admission).await;
    let cloned = process.clone();
    let operation = inject(admission.clone(), payload, granted);
    let task = tokio::spawn(async move { exchange(&cloned, operation).await });
    tokio::time::timeout(Duration::from_secs(2), async {
        while fixture.calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    task.abort();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if matches!(
                exchange(&process, Operation::Receipt(signed_id(&admission)))
                    .await
                    .unwrap(),
                Reply::Receipt(Some(DispatchReceipt {
                    status: DispatchStatus::Succeeded,
                    ..
                }))
            ) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn credentials_and_declared_encodings_never_return_in_success_or_error() {
    let mut encodings = vec![
        SECRET.to_owned(),
        serde_json::to_string(SECRET)
            .unwrap()
            .trim_matches('"')
            .to_owned(),
    ];
    for upper in [false, true] {
        encodings.push(
            SECRET
                .bytes()
                .map(|b| {
                    if upper {
                        format!("%{b:02X}")
                    } else {
                        format!("%{b:02x}")
                    }
                })
                .collect(),
        );
    }
    for engine in [
        general_purpose::STANDARD,
        general_purpose::STANDARD_NO_PAD,
        general_purpose::URL_SAFE,
        general_purpose::URL_SAFE_NO_PAD,
    ] {
        encodings.push(engine.encode(SECRET));
    }
    for status in [200, 400] {
        for encoded in &encodings {
            let fixture = Fixture::new(status, format!("echo {encoded}"), Duration::ZERO).await;
            let process = fixture.process().await;
            let (admission, payload) = fixture.attempt("isolation", 1, 1);
            let granted = permit(&process, &admission).await;
            let result = dispatched(
                exchange(&process, inject(admission, payload, granted))
                    .await
                    .unwrap(),
            );
            let serialized = serde_json::to_string(&result).unwrap();
            assert_eq!(result.receipt.status, DispatchStatus::ProviderFailed);
            assert_eq!(
                result.error,
                Some(EgressError::Provider {
                    status: (status != 200).then_some(status)
                })
            );
            assert!(result.output.is_none());
            for encoding in &encodings {
                assert!(!serialized.contains(encoding));
            }
            assert!(!format!("{result:?}").contains("synthetic-WP14-credential"));
            assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
        }
    }
}

#[tokio::test]
async fn owner_only_socket_roundtrip_and_oversized_frame_refusal() {
    use symbiotic_egress::socket::UnixEgressClient;
    let fixture = Fixture::new(200, "answer".into(), Duration::ZERO).await;
    let process = fixture.process().await;
    let listener = server::bind(&process).unwrap();
    assert_eq!(
        std::fs::metadata(&fixture.config.socket_path)
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    let task = tokio::spawn(server::serve(process, listener));
    let client = UnixEgressClient {
        path: fixture.config.socket_path.clone(),
        max_frame_bytes: fixture.config.max_frame_bytes,
        timeout: Duration::from_secs(3),
    };
    let (admission, payload) = fixture.attempt("socket", 1, 1);
    let response = client
        .exchange(Request {
            version: PROTOCOL_VERSION,
            operation: Operation::IssuePermit(admission.clone().into()),
        })
        .await
        .unwrap();
    let Reply::Permit(granted) = response.result.unwrap() else {
        panic!("wrong reply")
    };
    let response = client
        .exchange(Request {
            version: PROTOCOL_VERSION,
            operation: inject(admission, payload, granted.permit),
        })
        .await
        .unwrap();
    assert_eq!(
        dispatched(response.result.unwrap()).receipt.status,
        DispatchStatus::Succeeded
    );
    let mut stream = tokio::net::UnixStream::connect(&fixture.config.socket_path)
        .await
        .unwrap();
    stream
        .write_u32(fixture.config.max_frame_bytes + 1)
        .await
        .unwrap();
    let response: Response = socket::read_frame(&mut stream, fixture.config.max_frame_bytes)
        .await
        .unwrap();
    assert!(matches!(response.result, Err(EgressError::LimitExceeded)));
    task.abort();
    let _ = task.await;
}

#[tokio::test]
async fn missing_secret_releases_foundation_reservation() {
    let fixture = Fixture::new(200, "answer".into(), Duration::ZERO).await;
    let process = fixture.process().await;
    std::fs::remove_file(fixture.dir.path().join("provider")).unwrap();
    let (admission, payload) = fixture.attempt("missing", 1, 1);
    let granted = permit(&process, &admission).await;
    let result = dispatched(
        exchange(&process, inject(admission, payload, granted))
            .await
            .unwrap(),
    );
    assert_eq!(result.receipt.status, DispatchStatus::CredentialUnavailable);
    assert_eq!(result.receipt.spend_state, SpendState::Released);
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn redirects_and_oversized_provider_responses_fail_without_a_second_call() {
    for (status, body) in [(302, "redirect".to_owned()), (200, "x".repeat(40000))] {
        let fixture = Fixture::new(status, body, Duration::ZERO).await;
        let process = fixture.process().await;
        let (admission, payload) = fixture.attempt("bounded", 1, 1);
        let granted = permit(&process, &admission).await;
        let result = dispatched(
            exchange(&process, inject(admission, payload, granted))
                .await
                .unwrap(),
        );
        assert_eq!(result.receipt.status, DispatchStatus::ProviderFailed);
        assert_eq!(
            result.error,
            Some(EgressError::Provider {
                status: (status != 200).then_some(status)
            })
        );
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
        assert!(result.output.is_none());
    }
}

#[tokio::test]
async fn immutable_invocation_inputs_and_destination_cannot_change_on_retry() {
    let fixture = Fixture::new(200, "answer".into(), Duration::ZERO).await;
    let process = fixture.process().await;
    let (admission, payload) = fixture.attempt("immutable", 1, 1);
    let granted = permit(&process, &admission).await;
    exchange(&process, inject(admission, payload, granted))
        .await
        .unwrap();
    let (mut retry, _) = fixture.attempt("immutable", 2, 2);
    retry.attempt.input_manifest_digest = "b".repeat(64);
    let retry = AdmissionKey::new(KEY.to_vec())
        .unwrap()
        .sign_attempt(retry.attempt)
        .unwrap();
    assert!(matches!(
        exchange(&process, Operation::IssuePermit(retry.into())).await,
        Err(EgressError::InvalidRequest)
    ));
    let (mut changed, _) = fixture.attempt("destination", 1, 1);
    changed.attempt.destination = "https://unapproved.invalid".into();
    let changed = AdmissionKey::new(KEY.to_vec())
        .unwrap()
        .sign_attempt(changed.attempt)
        .unwrap();
    assert!(matches!(
        exchange(&process, Operation::IssuePermit(changed.into())).await,
        Err(EgressError::RouteRefused)
    ));
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
}

#[derive(Clone, Default)]
struct CapturedLogs(
    Arc<std::sync::Mutex<String>>,
    Option<Arc<dyn Fn() + Send + Sync>>,
);
impl tracing::field::Visit for CapturedLogs {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        use std::fmt::Write;
        let _ = writeln!(self.0.lock().unwrap(), "{}={value:?}", field.name());
    }
}
impl tracing::Subscriber for CapturedLogs {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, attrs: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        attrs.record(&mut self.clone());
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, values: &tracing::span::Record<'_>) {
        values.record(&mut self.clone());
    }
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        event.record(&mut self.clone());
        if event.metadata().fields().field("ahead_seconds").is_some()
            && let Some(on_warning) = &self.1
        {
            assert_eq!(*event.metadata().level(), tracing::Level::WARN);
            on_warning();
        }
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

#[tokio::test]
async fn provider_credential_errors_never_reach_runtime_logs() {
    let logs = CapturedLogs::default();
    tracing::subscriber::set_global_default(logs.clone()).unwrap();
    let fixture = Fixture::new(500, format!("backend failure: {SECRET}"), Duration::ZERO).await;
    let process = fixture.process().await;
    let (admission, payload) = fixture.attempt("logs", 1, 1);
    let granted = permit(&process, &admission).await;
    let result = dispatched(
        exchange(&process, inject(admission, payload, granted))
            .await
            .unwrap(),
    );
    tracing::info!(status = ?result.receipt.status, "safe completion evidence");
    let captured = logs.0.lock().unwrap();
    assert!(captured.contains("safe completion evidence"));
    assert!(!captured.contains("synthetic-WP14-credential"));
    assert!(!captured.contains(&general_purpose::STANDARD.encode(SECRET)));
}

#[tokio::test]
async fn executable_recovery_lost_permit_and_completion_replies() {
    executable_dispatch(None, None, None).await;
}

#[tokio::test]
async fn executable_preserves_numeric_provider_cost_after_restart() {
    // Run this package alone (also a separate CI step): workspace tests unify
    // serde_json dev features that the production executable does not inherit.
    executable_dispatch(None, Some("0.1234567890123456789"), None).await;
}

#[tokio::test]
async fn ambient_proxies_cannot_receive_credentials_or_private_inputs() {
    let proxy = Fixture::new(200, "process answer".into(), Duration::ZERO).await;
    executable_dispatch(Some(&proxy.config.routes[0].destination), None, None).await;
    assert_eq!(proxy.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn signed_attempt_time_ahead_beyond_tolerance_warns_and_accepts() {
    signed_attempt_time_warning(3600, 5, true).await;
}

#[tokio::test]
async fn signed_attempt_time_within_tolerance_accepts_without_warning() {
    signed_attempt_time_warning(5, 5, false).await;
}

#[tokio::test]
async fn signed_attempt_time_uses_configured_warning_tolerance() {
    signed_attempt_time_warning(3600, 7200, false).await;
}

async fn signed_attempt_time_warning(
    ahead_seconds: u64,
    tolerance_seconds: u64,
    should_warn: bool,
) {
    use tracing::instrument::WithSubscriber;

    let mut fixture = Fixture::new(200, "process answer".into(), Duration::ZERO).await;
    fixture.config.clock_rollback_warning_tolerance_seconds = tolerance_seconds;
    let process = fixture.process().await;
    let reentrant_process = process.clone();
    let logs = CapturedLogs(
        Arc::default(),
        Some(Arc::new(move || {
            // A subscriber can reenter the process. Delivery must hold no registry lock.
            let process = reentrant_process.clone();
            let (send, receive) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                let _ = send.send(process.purge_expired_results());
            });
            assert_eq!(
                receive.recv_timeout(Duration::from_secs(1)).unwrap(),
                Ok(())
            );
        })),
    );
    let (mut admission, payload) = fixture.attempt("clock-warning", 1, 1);
    admission.attempt.recorded_at = unix_seconds() + ahead_seconds;
    admission.attempt.expires_at = admission.attempt.recorded_at + 3600;
    admission = AdmissionKey::new(KEY.to_vec())
        .unwrap()
        .sign_attempt(admission.attempt)
        .unwrap();
    // These handler logging/reentrancy checks use a scoped subscriber, which
    // Tokio does not inherit in the in-process client's detached task.
    let handle = |operation| {
        let process = &process;
        async move {
            process
                .handle(Request {
                    version: PROTOCOL_VERSION,
                    operation,
                })
                .await
                .result
                .unwrap()
        }
    };
    async {
        let Reply::Permit(granted) = handle(Operation::IssuePermit(admission.clone().into())).await
        else {
            panic!("missing permit")
        };
        let Reply::Permit(replayed) =
            handle(Operation::IssuePermit(admission.clone().into())).await
        else {
            panic!("missing replayed permit")
        };
        assert!(replayed.permit.token == granted.permit.token);
        assert_eq!(
            replayed.permit.attempt_digest,
            granted.permit.attempt_digest
        );
        let result = dispatched(handle(inject(admission, payload, granted.permit)).await);
        assert_eq!(result.receipt.status, DispatchStatus::Succeeded);
        assert!(result.receipt_persisted);
    }
    .with_subscriber(logs.clone())
    .await;
    let captured = logs.0.lock().unwrap();
    // Fresh issuance, replayed issuance, and accepted dispatch all report.
    assert_eq!(
        captured
            .matches("event=\"signed_attempt_time_ahead\"")
            .count(),
        if should_warn { 3 } else { 0 }
    );
    if should_warn {
        assert_eq!(captured.matches("recorded_at=").count(), 3);
        assert_eq!(captured.matches("foundation_now=").count(), 3);
        assert_eq!(captured.matches("ahead_seconds=").count(), 3);
        assert_eq!(
            captured
                .matches(&format!("tolerance_seconds={tolerance_seconds}\n"))
                .count(),
            3
        );
        let lines = captured.lines().collect::<Vec<_>>();
        let (warnings, remainder) = lines.as_chunks::<5>();
        assert!(remainder.is_empty());
        for warning in warnings {
            let value = |name: &str| {
                warning
                    .iter()
                    .find_map(|line| line.strip_prefix(&format!("{name}=")))
                    .unwrap()
                    .parse::<u64>()
                    .unwrap()
            };
            assert_eq!(
                value("ahead_seconds"),
                value("recorded_at") - value("foundation_now")
            );
            assert!(value("ahead_seconds") > tolerance_seconds);
        }
    }
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn signed_attempt_time_open_log_sink_reports_executable_warnings() {
    let logs = executable_dispatch(None, None, Some((3600, 5, false))).await;
    // Fresh issuance, replayed issuance, and accepted dispatch each warn on stderr,
    // one JSON line each.
    let warnings: Vec<serde_json::Value> = logs
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .filter(|line: &serde_json::Value| line["event"] == "signed_attempt_time_ahead")
        .collect();
    assert_eq!(warnings.len(), 3, "{logs}");
    for warning in &warnings {
        assert_eq!(warning["level"], "WARN");
        assert_eq!(warning["tolerance_seconds"], 5);
        for field in ["recorded_at", "foundation_now", "ahead_seconds"] {
            assert!(warning[field].is_u64(), "{warning}");
        }
    }
    assert!(!logs.contains(SECRET));
    assert!(!logs.contains(&general_purpose::STANDARD.encode(SECRET)));
}

#[tokio::test]
async fn signed_attempt_time_closed_log_sink_preserves_permit_and_dispatch() {
    executable_dispatch(None, None, Some((3600, 5, true))).await;
}

#[tokio::test]
async fn signed_attempt_time_warning_tolerance_defaults_to_five_seconds() {
    let fixture = Fixture::new(200, "unused".into(), Duration::ZERO).await;
    let mut value = serde_json::to_value(&fixture.config).unwrap();
    value
        .as_object_mut()
        .unwrap()
        .remove("clock_rollback_warning_tolerance_seconds");
    let config: ProcessConfig = serde_json::from_value(value).unwrap();
    assert_eq!(config.clock_rollback_warning_tolerance_seconds, 5);
}

async fn executable_dispatch(
    proxy: Option<&str>,
    numeric_cost: Option<&'static str>,
    clock_case: Option<(u64, u64, bool)>,
) -> String {
    async fn ready_status(
        child: &mut Child,
        client: &socket::UnixEgressClient,
        signed_id: &SignedAttemptId,
    ) -> AttemptStatus {
        wait_for_child_startup(child, || client.attempt_status(signed_id.clone())).await
    }
    let mut fixture = Fixture::with_cost(
        200,
        "process answer".into(),
        Duration::ZERO,
        numeric_cost.unwrap_or(r#""0.00001234567890123456789""#),
    )
    .await;
    if let Some((_, tolerance_seconds, _)) = clock_case {
        fixture.config.clock_rollback_warning_tolerance_seconds = tolerance_seconds;
    }
    let config_path = fixture.dir.path().join("config.json");
    std::fs::write(&config_path, serde_json::to_vec(&fixture.config).unwrap()).unwrap();
    std::fs::set_permissions(&config_path, std::fs::Permissions::from_mode(0o600)).unwrap();
    let mut command = std::process::Command::new(credential_process());
    // Isolate environment changes in the child; parallel tests retain their environment.
    for name in [
        "HTTP_PROXY",
        "http_proxy",
        "HTTPS_PROXY",
        "https_proxy",
        "ALL_PROXY",
        "all_proxy",
        "NO_PROXY",
        "no_proxy",
    ] {
        command.env_remove(name);
    }
    if let Some(proxy) = proxy {
        for name in [
            "HTTP_PROXY",
            "http_proxy",
            "HTTPS_PROXY",
            "https_proxy",
            "ALL_PROXY",
            "all_proxy",
        ] {
            command.env(name, proxy);
        }
        command.env("NO_PROXY", "").env("no_proxy", "");
    }
    let mut child = Child(
        command
            .arg(&config_path)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap(),
    );
    if matches!(clock_case, Some((_, _, true))) {
        // Closing the reader makes stderr writes fail with BrokenPipe.
        drop(child.0.stderr.take());
    }
    let client = socket::UnixEgressClient {
        path: fixture.config.socket_path.clone(),
        max_frame_bytes: fixture.config.max_frame_bytes,
        timeout: Duration::from_secs(3),
    };
    let (mut admission, payload) = fixture.attempt("executable", 1, 1);
    if let Some((ahead_seconds, _, _)) = clock_case {
        admission.attempt.recorded_at = unix_seconds() + ahead_seconds;
        admission.attempt.expires_at = admission.attempt.recorded_at + 3600;
        admission = AdmissionKey::new(KEY.to_vec())
            .unwrap()
            .sign_attempt(admission.attempt)
            .unwrap();
    }
    let signed_id = AdmissionKey::new(KEY.to_vec())
        .unwrap()
        .sign_attempt_id(admission.attempt.attempt_id())
        .unwrap();
    assert!(matches!(
        ready_status(&mut child, &client, &signed_id).await,
        AttemptStatus::NotIssued
    ));
    assert!(matches!(
        client
            .exchange(Request {
                version: PROTOCOL_VERSION,
                operation: publish_revision(1),
            })
            .await
            .unwrap()
            .result
            .unwrap(),
        Reply::GrantRevisionPublished
    ));
    // Never read the permit reply. Observe its commit via a separate connection
    // before dropping it, so this proves loss after commit rather than before accept.
    let lost_permit_reply =
        send_without_reading(&client, Operation::IssuePermit(admission.clone().into())).await;
    tokio::time::timeout(OBSERVATION_HANG_GUARD, async {
        loop {
            if matches!(
                client.attempt_status(signed_id.clone()).await.unwrap(),
                AttemptStatus::Permitted
            ) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    drop(lost_permit_reply);
    let Reply::Permit(granted) = client
        .exchange(Request {
            version: PROTOCOL_VERSION,
            operation: Operation::IssuePermit(admission.clone().into()),
        })
        .await
        .unwrap()
        .result
        .unwrap()
    else {
        panic!("wrong reply")
    };
    assert!(matches!(granted.status, AttemptStatus::Permitted));
    // Lose the completion reply as well. No second injection is needed to settle.
    let lost_completion_reply =
        send_without_reading(&client, inject(admission, payload, granted.permit)).await;
    let result = tokio::time::timeout(OBSERVATION_HANG_GUARD, async {
        loop {
            if let AttemptStatus::Completed { result } =
                client.attempt_status(signed_id.clone()).await.unwrap()
            {
                break result;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    drop(lost_completion_reply);
    assert!(result.receipt_persisted);
    assert_eq!(result.receipt.usage.input_tokens, Some(7));
    assert_eq!(result.receipt.usage.output_tokens, Some(3));
    assert_eq!(
        result.receipt.usage.reported_cost_usd.as_deref(),
        Some(numeric_cost.unwrap_or("0.00001234567890123456789"))
    );
    assert_eq!(result.receipt.status, DispatchStatus::Succeeded);
    assert!(
        matches!(&result.output, Some(ProviderOutput::Chat { text, .. }) if text == "process answer")
    );
    child.0.kill().unwrap();
    child.0.wait().unwrap();
    let mut logs = String::new();
    if let Some(mut stderr) = child.0.stderr.take() {
        use std::io::Read;
        stderr.read_to_string(&mut logs).unwrap();
    }
    drop(child);
    // SIGKILL leaves the socket behind: restart must remove it under the state lock.
    assert!(fixture.config.socket_path.exists());
    let mut child = Child(command.spawn().unwrap());
    let recovered = ready_status(&mut child, &client, &signed_id).await;
    let AttemptStatus::Completed { result: recovered } = recovered else {
        panic!("missing result after executable restart");
    };
    assert_eq!(
        serde_json::to_value(&result).unwrap(),
        serde_json::to_value(recovered).unwrap()
    );
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
    logs
}

#[tokio::test]
async fn embeddings_share_attempt_binding_and_credential_boundary() {
    let mut fixture = Fixture::new(200, "unused".into(), Duration::ZERO).await;
    fixture.config.routes[0].provider = RouteProvider::GeminiEmbedding { dimensions: 8 };
    fixture.config.routes[0].destination =
        "https://generativelanguage.googleapis.com/v1beta".into();
    fixture.config.routes[0].model = "gemini-embedding-001".into();
    let process = fixture.process().await;
    std::fs::remove_file(fixture.dir.path().join("provider")).unwrap();
    let (mut admission, _) = fixture.attempt("embedding", 1, 1);
    let payload = ProviderPayload::Embedding(EmbeddingRequest {
        inputs: vec!["incoming input without a stored revision".into()],
        dimensions: Some(8),
        task: None,
        role_binding: None,
        source: None,
        metadata: serde_json::Value::Null,
    });
    admission.attempt.model = fixture.config.routes[0].model.clone();
    admission.attempt.input_digest = payload.digest().unwrap();
    let admission = AdmissionKey::new(KEY.to_vec())
        .unwrap()
        .sign_attempt(admission.attempt)
        .unwrap();
    let granted = permit(&process, &admission).await;
    let mut changed = payload.clone();
    if let ProviderPayload::Embedding(request) = &mut changed {
        request.inputs[0] = "substituted".into();
    }
    assert!(matches!(
        exchange(
            &process,
            inject(admission.clone(), changed, granted.clone())
        )
        .await,
        Err(EgressError::InvalidRequest)
    ));
    let result = dispatched(
        exchange(&process, inject(admission, payload, granted))
            .await
            .unwrap(),
    );
    assert_eq!(result.receipt.status, DispatchStatus::CredentialUnavailable);
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn regression_http_connect_failure_releases_spend_and_allows_bounded_retry() {
    tokio::time::timeout(Duration::from_secs(5), async {
        for classifier in [false, true] {
            let mut fixture = if classifier {
                rabbithole_jev_fixture(None).await
            } else {
                Fixture::new(200, "unused".into(), Duration::ZERO).await
            };
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            fixture.config.routes[0].destination =
                format!("http://{}/v1", listener.local_addr().unwrap());
            drop(listener);
            fixture.config.routes[0].provider_request_limit = Some(1);
            fixture.config.routes[0].timeout_seconds = 1;
            let process = fixture.process().await;
            let (first, payload) = if classifier {
                rabbithole_classify_attempt(&fixture)
            } else {
                fixture.attempt("connect-failure", 1, 1)
            };
            for ordinal in 1..=fixture.config.routes[0].max_attempts {
                let mut attempt = first.attempt.clone();
                attempt.attempt_ordinal = ordinal;
                attempt.record_sequence = u64::from(ordinal);
                let admission = AdmissionKey::new(KEY.to_vec())
                    .unwrap()
                    .sign_attempt(attempt)
                    .unwrap();
                let granted = permit(&process, &admission).await;
                let result = dispatched(
                    exchange(
                        &process,
                        inject(admission.clone(), payload.clone(), granted),
                    )
                    .await
                    .unwrap(),
                );
                assert_eq!(result.receipt.status, DispatchStatus::ProviderFailed);
                assert_eq!(result.error, Some(EgressError::Transport));
                assert_eq!(result.receipt.spend_state, SpendState::Released);
                assert!(result.receipt_persisted);
                assert_eq!(ledger_totals(&fixture), (0, u64::from(ordinal)));
                let AttemptStatus::Failed { result: recovered } =
                    status(&process, &admission).await
                else {
                    panic!("refused connection must remain a visible failed attempt");
                };
                assert_eq!(recovered.receipt.spend_state, SpendState::Released);
                // Simulate expiry of the existing durable provider cooldown so
                // this accounting regression need not wait for production jitter.
                let db = rusqlite::Connection::open(
                    fixture
                        .config
                        .state_dir
                        .join(symbiotic_ai_runtime::QUEUE_DATABASE),
                )
                .unwrap();
                assert_eq!(
                    db.execute(
                        "UPDATE queue_cooldowns SET cooldown_until = ?1",
                        [(chrono::Utc::now() - chrono::Duration::seconds(1)).to_rfc3339()]
                    )
                    .unwrap(),
                    1
                );
            }
            let mut fourth = first.attempt;
            fourth.attempt_ordinal = 4;
            fourth.record_sequence = 4;
            let fourth = AdmissionKey::new(KEY.to_vec())
                .unwrap()
                .sign_attempt(fourth)
                .unwrap();
            assert!(matches!(
                exchange(&process, Operation::IssuePermit(fourth.into())).await,
                Err(EgressError::BudgetRefused)
            ));
            assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
        }
    })
    .await
    .expect("connect failure retries must finish within five seconds");
}

#[tokio::test]
async fn known_zero_charge_releases_reservation_for_next_attempt() {
    let fixture = Fixture::new(200, "unused".into(), Duration::ZERO).await;
    let process = fixture.process().await;
    std::fs::remove_file(fixture.dir.path().join("provider")).unwrap();
    for ordinal in 1..=3 {
        let (admission, payload) = fixture.attempt("zero-charge", ordinal, u64::from(ordinal));
        let admission = AdmissionKey::new(KEY.to_vec())
            .unwrap()
            .sign_attempt(admission.attempt)
            .unwrap();
        let granted = permit(&process, &admission).await;
        let result = dispatched(
            exchange(&process, inject(admission, payload, granted))
                .await
                .unwrap(),
        );
        assert_eq!(result.receipt.spend_state, SpendState::Released);
    }
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
    let (fourth, _) = fixture.attempt("zero-charge", 4, 4);
    assert!(matches!(
        exchange(&process, Operation::IssuePermit(fourth.into())).await,
        Err(EgressError::BudgetRefused)
    ));
    assert_eq!(ledger_totals(&fixture), (0, 3));
}

#[tokio::test]
async fn expanded_gemini_wire_payload_is_refused_before_consumption() {
    expanded_wire_payload_is_refused(0).await;
}

#[tokio::test]
async fn expanded_chat_wire_payload_is_refused_before_consumption() {
    expanded_wire_payload_is_refused(1).await;
}

async fn expanded_wire_payload_is_refused(adapter: u8) {
    let mut fixture = Fixture::new(200, "unused".into(), Duration::ZERO).await;
    fixture.config.routes[0].max_input_bytes = 1024;
    let payload = if adapter == 0 {
        fixture.config.routes[0].provider = RouteProvider::GeminiEmbedding { dimensions: 8 };
        fixture.config.routes[0].destination =
            "https://generativelanguage.googleapis.com/v1beta".into();
        fixture.config.routes[0].model = "gemini-embedding-001".into();
        ProviderPayload::Embedding(EmbeddingRequest {
            inputs: vec!["x".into(); 128],
            dimensions: Some(8),
            task: None,
            role_binding: None,
            source: None,
            metadata: serde_json::Value::Null,
        })
    } else {
        if adapter == 2 {
            fixture.config.routes[0].provider = RouteProvider::AnthropicChat {
                operator: "anthropic".into(),
                thinking: Some(symbiotic_ai_runtime::model::ThinkingMode::Enabled),
            };
        }
        // The configured model is absent from the typed payload but present on the wire.
        fixture.config.routes[0].model = "m".repeat(1024);
        fixture.attempt("wire-limit", 1, 1).1
    };
    assert!(serde_json::to_vec(&payload).unwrap().len() < 1024);
    let process = fixture.process().await;
    // A regression must stop before loading this missing secret, and cannot reach Google.
    std::fs::remove_file(fixture.dir.path().join("provider")).unwrap();
    let (mut admission, _) = fixture.attempt("wire-limit", 1, 1);
    admission.attempt.model = fixture.config.routes[0].model.clone();
    admission.attempt.input_digest = payload.digest().unwrap();
    let admission = AdmissionKey::new(KEY.to_vec())
        .unwrap()
        .sign_attempt(admission.attempt)
        .unwrap();
    let granted = permit(&process, &admission).await;
    let result = exchange(&process, inject(admission.clone(), payload, granted)).await;
    assert!(
        matches!(result, Err(EgressError::LimitExceeded)),
        "oversized wire body accepted (adapter={adapter})"
    );
    assert!(matches!(
        exchange(&process, Operation::Receipt(signed_id(&admission))).await,
        Ok(Reply::Receipt(None))
    ));
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn zero_request_pacing_is_refused_at_route_validation() {
    let mut fixture = Fixture::new(200, "answer".into(), Duration::ZERO).await;
    fixture.config.routes[0].requests_per_minute = Some(0);
    assert_eq!(
        symbiotic_credential_process::validate_routes(
            &fixture.config.routes,
            fixture.config.max_frame_bytes,
        ),
        Err(EgressError::InvalidRequest)
    );
    assert!(matches!(
        CredentialProcess::open(fixture.config.clone()),
        Err(EgressError::InvalidRequest)
    ));
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
    fixture.config.routes[0].requests_per_minute = None;
    assert!(CredentialProcess::open(fixture.config).is_ok());
}

#[tokio::test]
async fn zero_input_pacing_is_refused_at_route_validation() {
    let mut fixture = Fixture::new(200, "answer".into(), Duration::ZERO).await;
    fixture.config.routes[0].input_units_per_minute = Some(0);
    assert_eq!(
        symbiotic_credential_process::validate_routes(
            &fixture.config.routes,
            fixture.config.max_frame_bytes,
        ),
        Err(EgressError::InvalidRequest)
    );
    assert!(matches!(
        CredentialProcess::open(fixture.config.clone()),
        Err(EgressError::InvalidRequest)
    ));
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
    fixture.config.routes[0].input_units_per_minute = None;
    assert!(CredentialProcess::open(fixture.config).is_ok());
}

#[tokio::test]
async fn runtime_bookkeeping_failure_retains_paid_output_and_safe_diagnostic() {
    let fixture = Fixture::new(200, "paid answer".into(), Duration::ZERO).await;
    let process = fixture.process().await;
    let db = rusqlite::Connection::open(fixture.config.state_dir.join("queue.sqlite")).unwrap();
    db.execute_batch(
        "CREATE TRIGGER refuse_completion BEFORE UPDATE OF status ON queue_items
        WHEN NEW.status = 'succeeded'
        BEGIN SELECT RAISE(FAIL, 'private bookkeeping failure'); END;",
    )
    .unwrap();
    let (admission, payload) = fixture.attempt("completion-failure", 1, 1);
    let granted = permit(&process, &admission).await;
    let result = dispatched(
        exchange(&process, inject(admission.clone(), payload, granted))
            .await
            .unwrap(),
    );
    let wire = serde_json::to_value(&result).unwrap();
    assert_eq!(
        wire["diagnostics"],
        serde_json::json!(["queue_complete_failed"])
    );
    assert!(!wire.to_string().contains("private bookkeeping failure"));
    assert!(!wire.to_string().contains(SECRET));
    assert_eq!(result.error, None);
    assert_eq!(result.receipt.status, DispatchStatus::Succeeded);
    assert!(result.receipt_persisted);
    assert!(
        matches!(result.output, Some(ProviderOutput::Chat { text, .. }) if text == "paid answer")
    );
    assert_eq!(result.receipt.usage.input_tokens, Some(7));
    assert_eq!(result.receipt.usage.output_tokens, Some(3));
    assert_eq!(result.receipt.spend_state, SpendState::Settled);
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
    drop(process);
    let process = fixture.process().await;
    let AttemptStatus::Completed { result } = status(&process, &admission).await else {
        panic!("missing completed result");
    };
    assert_eq!(serde_json::to_value(result).unwrap(), wire);
}

#[tokio::test]
async fn conflicting_shared_route_limits_are_refused_at_startup() {
    let fixture = Fixture::new(200, "answer".into(), Duration::ZERO).await;
    for field in ["concurrency", "requests", "input"] {
        let mut config = fixture.config.clone();
        config.routes[0].account_sharing_key =
            Some(symbiotic_ai_runtime::AccountSharingKey::new("shared"));
        let mut second = config.routes[0].clone();
        second.route = "second-route".into();
        second.tenant = "other-tenant".into();
        match field {
            "concurrency" => second.max_in_flight += 1,
            "requests" => second.requests_per_minute = Some(60),
            _ => second.input_units_per_minute = Some(1000),
        }
        config.routes.push(second);
        assert_eq!(
            symbiotic_credential_process::validate_routes(&config.routes, config.max_frame_bytes),
            Err(EgressError::InvalidRequest)
        );
        let opened = CredentialProcess::open(config);
        assert!(
            matches!(&opened, Err(EgressError::InvalidRequest)),
            "{field}: unexpected startup result {:?}",
            opened.err()
        );
    }
    let mut config = fixture.config.clone();
    let mut second = config.routes[0].clone();
    second.route = "same-limits".into();
    config.routes.push(second);
    assert!(CredentialProcess::open(config).is_ok());
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn conflicting_shared_route_limits_are_validated_before_state_in_every_order() {
    let fixture = Fixture::new(200, "answer".into(), Duration::ZERO).await;
    let process = fixture.process().await;
    let unopened = fixture.dir.path().join("unopened-state");
    let blocked = fixture.dir.path().join("state-is-a-file");
    std::fs::write(&blocked, b"untouched").unwrap();

    // Exhaust all 24 permutations of four routes, including two unrelated
    // account groups that must not hide the conflicting pair.
    let mut orders = vec![Vec::new()];
    for index in 0..4 {
        orders = orders
            .into_iter()
            .flat_map(|order| {
                (0..=order.len()).map(move |position| {
                    let mut next = order.clone();
                    next.insert(position, index);
                    next
                })
            })
            .collect();
    }
    assert_eq!(orders.len(), 24);
    for explicitly_shared in [false, true] {
        for field in ["concurrency", "requests", "input"] {
            // Cover both unrestricted-versus-paced and two distinct paced limits.
            for already_paced in [false, true] {
                let mut first = fixture.config.routes[0].clone();
                if explicitly_shared {
                    first.account_sharing_key =
                        Some(symbiotic_ai_runtime::AccountSharingKey::new("shared"));
                }
                if already_paced {
                    first.requests_per_minute = Some(30);
                    first.input_units_per_minute = Some(500);
                }
                let mut second = first.clone();
                second.route = "conflicting-route".into();
                if explicitly_shared {
                    second.tenant = "other-tenant".into();
                    second.account = "other-account".into();
                }
                match field {
                    "concurrency" => second.max_in_flight += 1,
                    "requests" => second.requests_per_minute = Some(60),
                    _ => second.input_units_per_minute = Some(1000),
                }
                let mut independent = first.clone();
                independent.route = "independent-route".into();
                independent.account = "independent-account".into();
                independent.account_sharing_key = None;
                let mut other_pool = first.clone();
                other_pool.route = "other-pool-route".into();
                other_pool.account_sharing_key =
                    Some(symbiotic_ai_runtime::AccountSharingKey::new("other-pool"));
                let routes = [first, second, independent, other_pool];
                for order in &orders {
                    let mut config = fixture.config.clone();
                    config.routes = order.iter().map(|&i| routes[i].clone()).collect();
                    assert_eq!(
                        symbiotic_credential_process::validate_routes(
                            &config.routes,
                            config.max_frame_bytes,
                        ),
                        Err(EgressError::InvalidRequest)
                    );
                    // Rebuilding the registry also varies its HashMaps' random seeds.
                    for state_dir in [&fixture.config.state_dir, &unopened, &blocked] {
                        let mut config = config.clone();
                        config.state_dir = state_dir.clone();
                        let opened = CredentialProcess::open(config);
                        assert!(
                            matches!(&opened, Err(EgressError::InvalidRequest)),
                            "{field}, shared={explicitly_shared}, paced={already_paced}, \
                             order={order:?}, state={state_dir:?}: {:?}",
                            opened.err()
                        );
                        assert!(!unopened.exists(), "invalid routes created state");
                        assert_eq!(std::fs::read(&blocked).unwrap(), b"untouched");
                    }
                }
            }
        }
    }
    // Valid configuration still reports actual lock/IO failures as state errors.
    assert!(matches!(
        CredentialProcess::open(fixture.config.clone()),
        Err(EgressError::StateUnavailable)
    ));
    let mut config = fixture.config.clone();
    config.state_dir = blocked;
    assert!(matches!(
        CredentialProcess::open(config),
        Err(EgressError::StateUnavailable)
    ));
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
    drop(process);
}

#[tokio::test]
async fn failure_before_dispatch_returns_safe_error_and_releases_reservation() {
    let fixture = Fixture::new(200, "answer".into(), Duration::ZERO).await;
    let process = fixture.process().await;
    let db = rusqlite::Connection::open(fixture.config.state_dir.join("queue.sqlite")).unwrap();
    // Inject a real queue write failure after permit consumption, before HTTP.
    db.execute_batch(
        "CREATE TRIGGER refuse_queue BEFORE INSERT ON queue_items
        BEGIN SELECT RAISE(FAIL, 'synthetic queue unavailable'); END;",
    )
    .unwrap();
    let (first, payload) = fixture.attempt("pre-dispatch", 1, 1);
    let granted = permit(&process, &first).await;
    let result = dispatched(
        exchange(&process, inject(first.clone(), payload, granted))
            .await
            .unwrap(),
    );
    let wire = serde_json::to_value(&result).unwrap();
    assert_eq!(wire["error"], "state_unavailable");
    let decoded: DispatchResult = serde_json::from_value(wire.clone()).unwrap();
    assert_eq!(decoded.error, Some(EgressError::StateUnavailable));
    assert!(!wire.to_string().contains(SECRET));
    assert!(result.output.is_none());
    assert!(result.receipt_persisted);
    assert_eq!(result.receipt.spend_state, SpendState::Released);
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
    drop(process);
    db.execute_batch("DROP TRIGGER refuse_queue").unwrap();
    let process = fixture.process().await;
    let stored = exchange(&process, Operation::Receipt(signed_id(&first)))
        .await
        .unwrap();
    assert!(matches!(
        stored,
        Reply::Receipt(Some(DispatchReceipt {
            spend_state: SpendState::Released,
            ..
        }))
    ));
    let (retry, payload) = fixture.attempt("pre-dispatch", 2, 2);
    let granted = permit(&process, &retry).await;
    let result = dispatched(
        exchange(&process, inject(retry, payload, granted))
            .await
            .unwrap(),
    );
    assert_eq!(result.receipt.status, DispatchStatus::Succeeded);
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn recovery_lost_permit_reply_reattaches_after_restart() {
    let fixture = Fixture::new(200, "answer".into(), Duration::ZERO).await;
    let process = fixture.process().await;
    let (admission, payload) = fixture.attempt("lost-permit", 1, 10);
    let committed = permit(&process, &admission).await;
    drop(process);
    let process = fixture.process().await;
    let recovered = permit(&process, &admission).await;
    assert_eq!(committed.token, recovered.token);
    assert_eq!(committed.attempt_digest, recovered.attempt_digest);
    let result = dispatched(
        exchange(&process, inject(admission, payload, recovered))
            .await
            .unwrap(),
    );
    assert!(result.error.is_none());
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
}

async fn send_without_reading(
    client: &socket::UnixEgressClient,
    operation: Operation,
) -> tokio::net::UnixStream {
    let mut stream = tokio::net::UnixStream::connect(&client.path).await.unwrap();
    socket::write_frame(
        &mut stream,
        &Request {
            version: PROTOCOL_VERSION,
            operation,
        },
        client.max_frame_bytes,
    )
    .await
    .unwrap();
    stream
}

async fn status(process: &CredentialProcess, admission: &SignedAttempt) -> AttemptStatus {
    let id = AdmissionKey::new(KEY.to_vec())
        .unwrap()
        .sign_attempt_id(admission.attempt.attempt_id())
        .unwrap();
    match exchange(process, Operation::AttemptStatus(id))
        .await
        .unwrap()
    {
        Reply::AttemptStatus(status) => status,
        _ => panic!("wrong reply"),
    }
}

#[tokio::test]
async fn recovery_lost_completion_reply_survives_restart_with_output_and_usage() {
    let fixture = Fixture::new(200, "retained answer".into(), Duration::ZERO).await;
    let process = fixture.process().await;
    let (admission, payload) = fixture.attempt("lost-completion", 1, 10);
    let granted = permit(&process, &admission).await;
    let result = dispatched(
        exchange(
            &process,
            inject(admission.clone(), payload, granted.clone()),
        )
        .await
        .unwrap(),
    );
    assert!(result.receipt_persisted);
    assert_eq!(
        serde_json::to_value(&result.receipt.usage).unwrap()["reported_cost_usd"],
        "0.00001234567890123456789"
    );
    assert_eq!(result.receipt.usage.cost_micro_usd, None);
    assert!(matches!(result.receipt.spend_state, SpendState::Settled));
    drop(process);
    let process = fixture.process().await;
    let AttemptStatus::Completed { result: recovered } = status(&process, &admission).await else {
        panic!("missing result");
    };
    assert_eq!(
        serde_json::to_value(&result).unwrap(),
        serde_json::to_value(recovered).unwrap()
    );
    let Reply::Permit(reattached) = exchange(&process, Operation::IssuePermit(admission.into()))
        .await
        .unwrap()
    else {
        panic!("missing permit");
    };
    assert_eq!(reattached.permit.token, granted.token);
    assert!(matches!(reattached.status, AttemptStatus::Completed { .. }));
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
    assert_eq!(ledger_totals(&fixture), (1, 1));
}

#[tokio::test]
async fn recovery_same_identity_with_different_signed_digest_is_refused() {
    let fixture = Fixture::new(200, "answer".into(), Duration::ZERO).await;
    let process = fixture.process().await;
    let (admission, _) = fixture.attempt("digest-mismatch", 1, 10);
    permit(&process, &admission).await;
    let key = AdmissionKey::new(KEY.to_vec()).unwrap();
    for field in 0..4 {
        let mut changed = admission.attempt.clone();
        match field {
            0 => changed.record_sequence += 1,
            1 => changed.input_digest = "c".repeat(64),
            2 => changed.recovery_expires_at += 1,
            _ => changed.expires_at += 1,
        }
        assert!(matches!(
            exchange(
                &process,
                Operation::IssuePermit(key.sign_attempt(changed).unwrap().into())
            )
            .await,
            Err(EgressError::InvalidRequest)
        ));
    }
    let mut signed_id = key.sign_attempt_id(admission.attempt.attempt_id()).unwrap();
    signed_id.attempt_id.tenant = "other".into();
    assert!(matches!(
        exchange(&process, Operation::AttemptStatus(signed_id.clone())).await,
        Err(EgressError::Unauthorized)
    ));
    assert!(matches!(
        exchange(&process, Operation::Receipt(signed_id)).await,
        Err(EgressError::Unauthorized)
    ));
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn recovery_failed_status_preserves_safe_error_and_charge() {
    let fixture = Fixture::new(500, SECRET.into(), Duration::ZERO).await;
    let process = fixture.process().await;
    let (admission, payload) = fixture.attempt("failed-status", 1, 10);
    let granted = permit(&process, &admission).await;
    exchange(&process, inject(admission.clone(), payload, granted))
        .await
        .unwrap();
    let AttemptStatus::Failed { result } = status(&process, &admission).await else {
        panic!("missing failure");
    };
    assert!(result.error.is_some());
    assert!(result.output.is_none());
    assert!(matches!(result.receipt.spend_state, SpendState::Unknown));
    assert!(!serde_json::to_string(&result).unwrap().contains(SECRET));
}

#[tokio::test]
async fn recovery_expired_status_does_not_retain_late_completion() {
    let fixture = Fixture::new(200, "expired answer".into(), Duration::ZERO).await;
    let process = fixture.process().await;
    let (mut admission, payload) = fixture.attempt("expired-status", 1, 10);
    admission.attempt.recovery_expires_at = 102; // Declared window already elapsed.
    admission = AdmissionKey::new(KEY.to_vec())
        .unwrap()
        .sign_attempt(admission.attempt)
        .unwrap();
    let granted = permit(&process, &admission).await;
    let result = dispatched(
        exchange(&process, inject(admission.clone(), payload, granted))
            .await
            .unwrap(),
    );
    assert!(result.receipt_persisted);
    assert!(matches!(
        status(&process, &admission).await,
        AttemptStatus::Expired
    ));
    drop(process);
    let process = fixture.process().await;
    assert!(matches!(
        status(&process, &admission).await,
        AttemptStatus::Expired
    ));
    assert!(matches!(
        exchange(&process, Operation::Receipt(signed_id(&admission)))
            .await
            .unwrap(),
        Reply::Receipt(Some(_))
    ));
    let Reply::Permit(grant) = exchange(&process, Operation::IssuePermit(admission.into()))
        .await
        .unwrap()
    else {
        panic!("missing permit");
    };
    assert!(matches!(grant.status, AttemptStatus::Expired));
    let db = rusqlite::Connection::open(
        fixture
            .config
            .state_dir
            .join(symbiotic_ai_runtime::QUEUE_DATABASE),
    )
    .unwrap();
    assert_eq!(
        db.query_row(
            "SELECT count(*) FROM egress_permits WHERE result IS NOT NULL",
            [],
            |row| row.get::<_, u64>(0)
        )
        .unwrap(),
        0
    );
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn recovery_idle_server_purges_completed_results_at_deadline() {
    let fixture = Fixture::new(200, "short-lived answer".into(), Duration::ZERO).await;
    let process = fixture.process().await;
    let (admission, payload) = fixture.attempt("idle-expiry", 1, 10);
    let granted = permit(&process, &admission).await;
    exchange(&process, inject(admission.clone(), payload, granted))
        .await
        .unwrap();
    assert!(matches!(
        status(&process, &admission).await,
        AttemptStatus::Completed { .. }
    ));
    let listener = server::bind(&process).unwrap();
    let task = tokio::spawn(server::serve(process.clone(), listener));
    let db = rusqlite::Connection::open(
        fixture
            .config
            .state_dir
            .join(symbiotic_ai_runtime::QUEUE_DATABASE),
    )
    .unwrap();
    // Advance the stored deadline in this fixture so slow debug builds cannot
    // expire the answer before the test observes completion. The registry test
    // separately checks the exact signed deadline boundary with an injected time.
    db.execute("UPDATE egress_permits SET recovery_expires_at=0", [])
        .unwrap();
    // No further IPC/handle requests: the daemon must expire results while idle.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let retained: u64 = db
                .query_row(
                    "SELECT count(*) FROM egress_permits WHERE result IS NOT NULL",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            if retained == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    assert!(matches!(
        status(&process, &admission).await,
        AttemptStatus::Expired
    ));
    task.abort();
    let _ = task.await;
}

#[tokio::test]
async fn recovery_concurrent_permit_requests_share_one_capability() {
    let fixture = Fixture::new(200, "answer".into(), Duration::ZERO).await;
    let process = fixture.process().await;
    let (admission, _) = fixture.attempt("concurrent-issue", 1, 10);
    let (first, second) = tokio::join!(permit(&process, &admission), permit(&process, &admission));
    assert_eq!(first.token, second.token);
    assert_eq!(first.attempt_digest, second.attempt_digest);
    assert!(matches!(
        status(&process, &admission).await,
        AttemptStatus::Permitted
    ));
    let request = Request {
        version: 1,
        operation: Operation::IssuePermit(admission.into()),
    };
    assert!(matches!(
        InProcessEgressClient::new(process.clone())
            .exchange(request)
            .await
            .unwrap()
            .result,
        Err(EgressError::Version)
    ));
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn recovery_disconnected_peer_does_not_stop_socket_server() {
    let fixture = Fixture::new(200, "unused".into(), Duration::ZERO).await;
    let process = fixture.process().await;
    let listener = server::bind(&process).unwrap();
    // Disconnect before accept/peer authentication, as can happen with a lost reply.
    let stream = tokio::net::UnixStream::connect(&fixture.config.socket_path)
        .await
        .unwrap();
    drop(stream);
    let task = tokio::spawn(server::serve(process, listener));
    tokio::task::yield_now().await;
    assert!(
        !task.is_finished(),
        "one disconnected peer stopped the server: {:?}",
        task.await
    );
    task.abort();
    let _ = task.await;
}

#[tokio::test]
async fn independent_tenant_routes_accept_different_account_policies() {
    let fixture = Fixture::new(200, "answer".into(), Duration::ZERO).await;
    let mut config = fixture.config.clone();
    let mut second = config.routes[0].clone();
    second.tenant = "other-tenant".into();
    second.max_in_flight = 1;
    second.requests_per_minute = Some(60);
    config.routes.push(second);
    assert!(CredentialProcess::open(config).is_ok());
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
}

fn ledger_totals(fixture: &Fixture) -> (u64, u64) {
    let conn = rusqlite::Connection::open(
        fixture
            .config
            .state_dir
            .join(symbiotic_ai_runtime::QUEUE_DATABASE),
    )
    .unwrap();
    (
        conn.query_row(
            "SELECT coalesce(sum(used), 0) FROM spend_accounts",
            [],
            |r| r.get(0),
        )
        .unwrap(),
        conn.query_row("SELECT count(*) FROM spend_receipts", [], |r| r.get(0))
            .unwrap(),
    )
}

#[tokio::test]
async fn spend_concurrent_dispatches_on_one_account_share_one_durable_budget() {
    let mut fixture = Fixture::new(200, "answer".into(), Duration::from_millis(30)).await;
    fixture.config.routes[0].provider_request_limit = Some(1);
    let process = fixture.process().await;
    let (a, pa) = fixture.attempt("first-account-call", 1, 1);
    let (b, pb) = fixture.attempt("second-account-call", 1, 2);
    let ap = permit(&process, &a).await;
    let bp = permit(&process, &b).await;
    let (ra, rb) = tokio::join!(
        exchange(&process, inject(a, pa, ap)),
        exchange(&process, inject(b, pb, bp))
    );
    assert_eq!([&ra, &rb].iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(
        [&ra, &rb]
            .iter()
            .filter(|r| matches!(r, Err(EgressError::BudgetRefused)))
            .count(),
        1
    );
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
    assert_eq!(ledger_totals(&fixture), (1, 1));
}

#[tokio::test]
async fn retrieval_dispatch_supports_keyless_permits_and_one_provider_request() {
    use symbiotic_ai_runtime::model::{ModelAdapter, RerankRequest};
    for adapter in [
        ModelAdapter::OpenAiEmbedding,
        ModelAdapter::OllamaEmbedding,
        ModelAdapter::CohereRerank,
    ] {
        for keyless in [false, true] {
            for measured_usage in [false, true] {
                let rerank = adapter == ModelAdapter::CohereRerank;
                let body = match adapter {
                    ModelAdapter::CohereRerank => {
                        r#"{"results":[{"index":0,"relevance_score":0.8}]}"#
                    }
                    ModelAdapter::OllamaEmbedding => r#"{"embedding":[1,2]}"#,
                    _ => r#"{"data":[{"index":0,"embedding":[1,2]}]}"#,
                };
                let mut body: serde_json::Value = serde_json::from_str(body).unwrap();
                if measured_usage {
                    body["usage"] = serde_json::json!({"cost": "0.001"});
                }
                let mut fixture = Fixture::with_http_response(
                    200,
                    body.to_string(),
                    Duration::ZERO,
                    "null",
                    true,
                    keyless,
                )
                .await;
                let route = &mut fixture.config.routes[0];
                route.provider_request_limit = Some(1);
                if keyless {
                    route.secret = SecretSource::None;
                    route.secret_ref.clear();
                }
                route.provider = if rerank {
                    RouteProvider::CohereRerank {
                        operator: "test".into(),
                        rerank_input_bytes: 64,
                        rerank_candidates: 2,
                        rerank_context_tokens: 16,
                        rerank_query_tokens: 8,
                    }
                } else {
                    RouteProvider::CompatibleEmbedding {
                        adapter,
                        operator: "test".into(),
                        dimensions: 2,
                        embedding_full_dimensions: 1024,
                        embedding_input_tokens: 16,
                    }
                };
                if !rerank {
                    let mut collision = fixture.config.clone();
                    let mut other = collision.routes[0].clone();
                    other.route = "other".into();
                    if let RouteProvider::CompatibleEmbedding { adapter, .. } = &mut other.provider
                    {
                        *adapter = if *adapter == ModelAdapter::OpenAiEmbedding {
                            ModelAdapter::OllamaEmbedding
                        } else {
                            ModelAdapter::OpenAiEmbedding
                        };
                    }
                    collision.routes.push(other);
                    assert!(CredentialProcess::open(collision).is_err());
                }
                let process = fixture.process().await;
                let (admission, _) = fixture.attempt("retrieval", 1, 10);
                let payload = if rerank {
                    ProviderPayload::Rerank(RerankRequest {
                        query: "query".into(),
                        documents: vec!["candidate".into()],
                        top_k: Some(1),
                        role_binding: None,
                        source: None,
                        metadata: serde_json::Value::Null,
                    })
                } else {
                    ProviderPayload::Embedding(EmbeddingRequest {
                        inputs: vec!["input".into()],
                        dimensions: Some(2),
                        task: None,
                        role_binding: None,
                        source: None,
                        metadata: serde_json::Value::Null,
                    })
                };
                let mut attempt = admission.attempt;
                {
                    let mut oversized = payload.clone();
                    match &mut oversized {
                        ProviderPayload::Rerank(request) => request.documents[0] = "x".repeat(17),
                        ProviderPayload::Embedding(request) => request.inputs[0] = "x".repeat(17),
                        _ => unreachable!(),
                    }
                    let (oversized_admission, _) =
                        fixture.attempt("retrieval-over-capacity", 1, 10);
                    let mut oversized_attempt = oversized_admission.attempt;
                    oversized_attempt.input_digest = oversized.digest().unwrap();
                    let oversized_admission = AdmissionKey::new(KEY.to_vec())
                        .unwrap()
                        .sign_attempt(oversized_attempt)
                        .unwrap();
                    let oversized_permit = permit(&process, &oversized_admission).await;
                    assert!(matches!(
                        exchange(
                            &process,
                            inject(oversized_admission.clone(), oversized, oversized_permit)
                        )
                        .await,
                        Err(EgressError::LimitExceeded)
                    ));
                    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
                    assert_eq!(ledger_totals(&fixture), (0, 0));
                    assert!(matches!(
                        status(&process, &oversized_admission).await,
                        AttemptStatus::Permitted
                    ));
                }
                attempt.input_digest = payload.digest().unwrap();
                let admission = AdmissionKey::new(KEY.to_vec())
                    .unwrap()
                    .sign_attempt(attempt)
                    .unwrap();
                let token = permit(&process, &admission).await;
                let operation = inject(admission.clone(), payload.clone(), token);
                let result = dispatched(exchange(&process, operation.clone()).await.unwrap());
                assert_eq!(result.receipt.status, DispatchStatus::Succeeded);
                assert_eq!(
                    result.receipt.spend_state,
                    if measured_usage {
                        SpendState::Settled
                    } else {
                        SpendState::Unknown
                    }
                );
                assert!(result.error.is_none());
                assert!(result.output.is_some());
                assert!(result.receipt_persisted);
                let encoded = serde_json::to_string(&result).unwrap();
                serde_json::from_str::<DispatchResult>(&encoded).unwrap_or_else(|error| {
                    panic!(
                        "{adapter:?}, keyless={keyless}, measured_usage={measured_usage}: {error}"
                    )
                });
                assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
                assert!(exchange(&process, operation).await.is_err());
                assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
                assert_eq!(ledger_totals(&fixture), (1, 1));
                let ledger = symbiotic_ai_runtime::spend::SqliteSpendLedger::open(
                    &fixture
                        .config
                        .state_dir
                        .join(symbiotic_ai_runtime::QUEUE_DATABASE),
                )
                .unwrap();
                use symbiotic_ai_runtime::model::SpendLedger;
                let canonical = ledger.receipt(&result.receipt.reference).unwrap().unwrap();
                assert_eq!(
                    canonical.state,
                    if measured_usage {
                        symbiotic_ai_runtime::SpendState::Settled
                    } else {
                        symbiotic_ai_runtime::SpendState::Unknown
                    }
                );
                let mut next_attempt = admission.attempt.clone();
                next_attempt.invocation_id = "retrieval-next".into();
                let next = AdmissionKey::new(KEY.to_vec())
                    .unwrap()
                    .sign_attempt(next_attempt)
                    .unwrap();
                let next_permit = permit(&process, &next).await;
                assert!(matches!(
                    exchange(&process, inject(next, payload, next_permit)).await,
                    Err(EgressError::BudgetRefused)
                ));
                assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
                assert_eq!(ledger_totals(&fixture), (1, 1));
                drop(process);
                let reopened = fixture.process().await;
                let AttemptStatus::Completed { result: recovered } =
                    status(&reopened, &admission).await
                else {
                    panic!("retrieval completion missing after restart");
                };
                assert_eq!(
                    serde_json::to_value(&result).unwrap(),
                    serde_json::to_value(recovered).unwrap()
                );
                assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
                assert!(!serde_json::to_string(&result).unwrap().contains(SECRET));
            }
        }
    }
}

#[tokio::test]
async fn grant_revocation_preserves_accepted_handoff_and_accounting() {
    let fixture = Fixture::new(200, "accepted answer".into(), Duration::from_millis(250)).await;
    let process = fixture.process().await;
    let (admission, payload) = fixture.attempt("accepted-before-revocation", 1, 10);
    let granted = permit(&process, &admission).await;
    let operation = inject(admission.clone(), payload, granted);
    let worker = process.clone();
    let task = tokio::spawn(async move { dispatched(exchange(&worker, operation).await.unwrap()) });
    let retained = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let AttemptStatus::Dispatched { receipt } = status(&process, &admission).await {
                break receipt;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(retained.attempt_id, admission.attempt.attempt_id());
    assert_eq!(retained.spend_state, SpendState::Unknown);
    revoke(&process, 11).await;
    let result = task.await.unwrap();
    assert_eq!(result.receipt.reference, retained.reference);
    assert_eq!(result.receipt.status, DispatchStatus::Succeeded);
    assert_eq!(result.receipt.spend_state, SpendState::Settled);
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
    assert_eq!(ledger_totals(&fixture), (1, 1));
    drop(process);
    let process = fixture.process().await;
    let AttemptStatus::Completed { result: recovered } = status(&process, &admission).await else {
        panic!("accepted handoff lost after revision change");
    };
    assert_eq!(recovered.receipt.reference, retained.reference);
    let Reply::Receipt(Some(receipt)) =
        exchange(&process, Operation::Receipt(signed_id(&admission)))
            .await
            .unwrap()
    else {
        panic!("accepted receipt lost");
    };
    assert_eq!(receipt.reference, retained.reference);
    assert_eq!(receipt.spend_state, SpendState::Settled);
    assert_eq!(ledger_totals(&fixture), (1, 1));
}

#[tokio::test]
async fn grant_revision_requires_authenticated_initial_publication() {
    let fixture = Fixture::new(200, "unused".into(), Duration::ZERO).await;
    let process = CredentialProcess::open(fixture.config.clone()).unwrap();
    let (admission, _) = fixture.attempt("no-publication", 1, 10);
    assert!(matches!(
        exchange(&process, Operation::IssuePermit(admission.clone().into())).await,
        Err(EgressError::RouteRefused)
    ));
    let Operation::PublishGrantRevision(mut signed) = publish_revision(1) else {
        unreachable!()
    };
    signed.grant.revision = 2;
    assert!(matches!(
        exchange(&process, Operation::PublishGrantRevision(signed)).await,
        Err(EgressError::Unauthorized)
    ));
    revoke(&process, 1).await;
    revoke(&process, 1).await;
    permit(&process, &admission).await;
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
    assert_eq!(ledger_totals(&fixture), (0, 0));
}

#[tokio::test]
async fn failed_settlement_keeps_typed_receipt_unknown_and_preserves_paid_output() {
    let fixture = Fixture::new(200, "paid answer".into(), Duration::ZERO).await;
    let process = fixture.process().await;
    let (admission, payload) = fixture.attempt("failed-settlement", 1, 10);
    let permit = permit(&process, &admission).await;
    let conn = rusqlite::Connection::open(
        fixture
            .config
            .state_dir
            .join(symbiotic_ai_runtime::QUEUE_DATABASE),
    )
    .unwrap();
    conn.execute_batch(
        "CREATE TRIGGER reject_completion BEFORE UPDATE OF finished ON egress_permits
        BEGIN SELECT RAISE(ABORT, 'synthetic completion failure'); END;",
    )
    .unwrap();
    let operation = inject(admission.clone(), payload, permit);
    let result = dispatched(exchange(&process, operation.clone()).await.unwrap());
    assert!(result.output.is_some());
    assert_eq!(result.receipt.usage.input_tokens, Some(7));
    assert!(!result.receipt_persisted);
    assert_eq!(result.receipt.spend_state, SpendState::Unknown);
    assert_eq!(ledger_totals(&fixture), (1, 1));
    let reference = result.receipt.reference;
    drop(conn);
    drop(process);
    let process = fixture.process().await;
    let AttemptStatus::Dispatched { receipt } = status(&process, &admission).await else {
        panic!("failed settlement lost its accepted receipt");
    };
    assert_eq!(receipt.reference, reference);
    assert_eq!(receipt.spend_state, SpendState::Unknown);
    assert!(matches!(
        exchange(&process, operation).await,
        Err(EgressError::PermitRefused)
    ));
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
    assert_eq!(ledger_totals(&fixture), (1, 1));
}

#[tokio::test]
async fn frame_config_refuses_identity_fields_that_cannot_fit_with_the_response() {
    let mut fixture = Fixture::new(200, "answer".into(), Duration::ZERO).await;
    let response_bytes = fixture.config.routes[0].max_response_bytes;
    let largest_field = (fixture.config.max_frame_bytes as usize - 4096 - 4 * response_bytes) / 24;
    fixture.config.routes[0].max_field_bytes = largest_field;
    let process = CredentialProcess::open(fixture.config.clone()).unwrap();
    drop(process);
    for field_bytes in [
        largest_field + 1,
        fixture.config.max_frame_bytes as usize,
        usize::MAX,
    ] {
        fixture.config.routes[0].max_field_bytes = field_bytes;
        let error = CredentialProcess::open(fixture.config.clone())
            .err()
            .expect("oversized route must be refused");
        assert_eq!(
            symbiotic_credential_process::validate_routes(
                &fixture.config.routes,
                fixture.config.max_frame_bytes,
            ),
            Err(error)
        );
        for setting in ["max_frame_bytes", "max_field_bytes", "max_response_bytes"] {
            assert!(
                error.to_string().contains(setting),
                "missing {setting}: {error}"
            );
        }
    }
    fixture.config.routes[0].max_field_bytes = 1;
    fixture.config.routes[0].max_response_bytes = 1;
    fixture.config.routes[0].max_input_bytes = 1;
    for frame_bytes in [4096, 4123] {
        fixture.config.max_frame_bytes = frame_bytes;
        let error = CredentialProcess::open(fixture.config.clone())
            .err()
            .expect("frame must fit envelope and one field");
        assert_eq!(
            symbiotic_credential_process::validate_routes(
                &fixture.config.routes,
                fixture.config.max_frame_bytes,
            ),
            Err(error)
        );
        assert!(error.to_string().contains("max_frame_bytes"));
    }
    fixture.config.max_frame_bytes = 4124;
    CredentialProcess::open(fixture.config.clone()).unwrap();
}

#[tokio::test]
async fn supervision_concurrent_binders_preserve_the_winning_stale_socket_replacement() {
    use std::sync::{Condvar, Mutex, mpsc};
    let mut fixture = Fixture::new(200, "answer".into(), Duration::ZERO).await;
    // This valid socket name must not collide with the lifetime state lock,
    // credential-process.lock, and make the binder wait on its own process.
    fixture.config.socket_path = fixture.config.state_dir.join("credential-process");
    let process = fixture.process().await;
    let mut other_config = fixture.config.clone();
    other_config.state_dir = fixture.dir.path().join("other-state");
    let other = CredentialProcess::open(other_config).unwrap();
    // Exercise cloned state owners and different state directories naming one socket.
    // Repeating the simultaneous start exposes the probe/unlink/bind race without
    // introducing sleeps or test hooks into socket ownership.
    for round in 0..512 {
        let stale = std::os::unix::net::UnixListener::bind(&fixture.config.socket_path).unwrap();
        drop(stale);
        let (ready, started) = mpsc::channel();
        let (tx, rx) = mpsc::channel();
        let start = Arc::new((Mutex::new(false), Condvar::new()));
        let contenders = [
            process.clone(),
            if round % 2 == 0 {
                process.clone()
            } else {
                other.clone()
            },
        ];
        let mut workers = Vec::new();
        for contender in contenders {
            let start = start.clone();
            let ready = ready.clone();
            let tx = tx.clone();
            let runtime = tokio::runtime::Handle::current();
            workers.push(std::thread::spawn(move || {
                let _runtime = runtime.enter();
                let go = start.0.lock().unwrap();
                ready.send(()).unwrap();
                let (go, timeout) = start
                    .1
                    .wait_timeout_while(go, Duration::from_secs(60), |go| !*go)
                    .unwrap();
                assert!(*go && !timeout.timed_out(), "start hang guard fired");
                drop(go);
                tx.send(server::bind(&contender)).unwrap();
            }));
        }
        drop(tx);
        for _ in 0..2 {
            started
                .recv_timeout(Duration::from_secs(60))
                .expect("ready hang guard fired");
        }
        *start.0.lock().unwrap() = true;
        start.1.notify_all();
        let results: Vec<_> = (0..2)
            .map(|_| {
                rx.recv_timeout(Duration::from_secs(60))
                    .expect("binder hang guard fired")
            })
            .collect();
        for worker in workers {
            worker.join().unwrap();
        }
        let mut listeners = Vec::new();
        for result in results {
            match result {
                Ok(listener) => listeners.push(listener),
                Err(error) => assert!(matches!(error, EgressError::StateUnavailable)),
            }
        }
        assert_eq!(
            listeners.len(),
            1,
            "only one binder may replace the stale socket, round {round}"
        );
        let listener = listeners.pop().unwrap();
        let client = tokio::time::timeout(
            Duration::from_secs(60),
            tokio::net::UnixStream::connect(&fixture.config.socket_path),
        )
        .await
        .expect("connect hang guard fired")
        .expect("winning socket must remain reachable");
        let peer = tokio::time::timeout(Duration::from_secs(60), listener.accept())
            .await
            .expect("accept hang guard fired")
            .unwrap();
        drop((client, peer, listener));
        std::fs::remove_file(&fixture.config.socket_path).unwrap();
    }
    assert_eq!(
        std::fs::metadata(
            fixture
                .config
                .state_dir
                .join("credential-process.bind.lock")
        )
        .unwrap()
        .permissions()
        .mode()
            & 0o777,
        0o600,
        "the retained socket lock must be owner-only"
    );
}

#[tokio::test]
async fn supervision_socket_recovery_preserves_live_owner_and_non_socket_paths() {
    let fixture = Fixture::new(200, "answer".into(), Duration::ZERO).await;
    let process = fixture.process().await;
    let listener = server::bind(&process).unwrap();
    assert!(
        server::bind(&process).is_err(),
        "a clone must not unlink its own live listener"
    );
    assert!(matches!(
        CredentialProcess::open(fixture.config.clone()),
        Err(EgressError::StateUnavailable)
    ));
    let mut other_config = fixture.config.clone();
    other_config.state_dir = fixture.dir.path().join("other-state");
    let other = CredentialProcess::open(other_config).unwrap();
    assert!(
        server::bind(&other).is_err(),
        "another state lock must not authorize unlinking an active listener"
    );
    drop(other);
    let task = tokio::spawn(server::serve(process, listener));
    let client = socket::UnixEgressClient {
        path: fixture.config.socket_path.clone(),
        max_frame_bytes: fixture.config.max_frame_bytes,
        timeout: Duration::from_secs(2),
    };
    let (admission, _) = fixture.attempt("readiness", 1, 1);
    let id = AdmissionKey::new(KEY.to_vec())
        .unwrap()
        .sign_attempt_id(admission.attempt.attempt_id())
        .unwrap();
    assert!(matches!(
        client.attempt_status(id).await.unwrap(),
        AttemptStatus::NotIssued
    ));
    task.abort();
    let _ = task.await;
    let process = CredentialProcess::open(fixture.config.clone()).unwrap();
    std::fs::remove_file(&fixture.config.socket_path).unwrap();
    std::fs::write(&fixture.config.socket_path, b"keep").unwrap();
    assert!(server::bind(&process).is_err());
    assert_eq!(std::fs::read(&fixture.config.socket_path).unwrap(), b"keep");
    std::fs::remove_file(&fixture.config.socket_path).unwrap();
    std::os::unix::fs::symlink(
        fixture.dir.path().join("admission"),
        &fixture.config.socket_path,
    )
    .unwrap();
    assert!(server::bind(&process).is_err());
    assert!(
        std::fs::symlink_metadata(&fixture.config.socket_path)
            .unwrap()
            .file_type()
            .is_symlink()
    );
}

fn supervision_policy() -> symbiotic_supervise::Policy {
    symbiotic_supervise::Policy {
        version: 1,
        max_restarts: 2,
        crash_window_ms: 10_000,
        backoff_ms: 10,
        stop_grace_ms: 100,
        poll_ms: 5,
    }
}

#[test]
fn supervised_credential_parent_entrypoint() {
    let Some(config) = std::env::var_os("CREDENTIAL_PARENT_CONFIG") else {
        return;
    };
    let pid_path = std::env::var_os("CREDENTIAL_CHILD_PID").unwrap();
    let supervisor = symbiotic_supervise::Supervisor::start(
        move || {
            let mut command = std::process::Command::new(credential_process());
            command.arg("--child").arg(&config);
            command
        },
        supervision_policy(),
    )
    .unwrap();
    let symbiotic_supervise::Event::Started(pid) = supervisor.next_event().unwrap() else {
        panic!("expected child");
    };
    std::fs::write(pid_path, pid.to_string()).unwrap();
    loop {
        match supervisor
            .next_event()
            .expect("credential supervision failed")
        {
            symbiotic_supervise::Event::Started(_) => {}
            symbiotic_supervise::Event::Exited(status) => {
                panic!("credential child exited: {status}");
            }
        }
    }
}

#[tokio::test]
async fn regression_supervised_credential_parent_exits_on_child_startup_failure() {
    let dir = tempfile::tempdir().unwrap();
    let mut parent = Child(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "supervised_credential_parent_entrypoint",
                "--nocapture",
            ])
            .env(
                "CREDENTIAL_PARENT_CONFIG",
                dir.path().join("missing-config.json"),
            )
            .env("CREDENTIAL_CHILD_PID", dir.path().join("child.pid"))
            .stdout(std::process::Stdio::null())
            .spawn()
            .unwrap(),
    );
    let status = tokio::time::timeout(OBSERVATION_HANG_GUARD, async {
        loop {
            if let Some(status) = parent.0.try_wait().unwrap() {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("parent stayed alive after credential child startup failure");
    assert!(
        dir.path().join("child.pid").exists(),
        "child must have started"
    );
    assert!(
        !status.success(),
        "child startup failure must fail the parent"
    );
}

#[tokio::test]
async fn supervision_credential_child_exits_when_app_is_killed() {
    let fixture = Fixture::new(200, "answer".into(), Duration::ZERO).await;
    let config = fixture.dir.path().join("config.json");
    std::fs::write(&config, serde_json::to_vec(&fixture.config).unwrap()).unwrap();
    std::fs::set_permissions(&config, std::fs::Permissions::from_mode(0o600)).unwrap();
    let pid_path = fixture.dir.path().join("child.pid");
    let mut parent = Child(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "supervised_credential_parent_entrypoint",
                "--nocapture",
            ])
            .env("CREDENTIAL_PARENT_CONFIG", &config)
            .env("CREDENTIAL_CHILD_PID", &pid_path)
            .stdout(std::process::Stdio::null())
            .spawn()
            .unwrap(),
    );
    let client = socket::UnixEgressClient {
        path: fixture.config.socket_path.clone(),
        max_frame_bytes: fixture.config.max_frame_bytes,
        timeout: Duration::from_secs(2),
    };
    let (attempt, _) = fixture.attempt("child-ready", 1, 1);
    let id = AdmissionKey::new(KEY.to_vec())
        .unwrap()
        .sign_attempt_id(attempt.attempt.attempt_id())
        .unwrap();
    assert!(matches!(
        wait_for_child_startup(&mut parent, || client.attempt_status(id.clone())).await,
        AttemptStatus::NotIssued
    ));
    let pid: u32 = std::fs::read_to_string(pid_path).unwrap().parse().unwrap();
    parent.0.kill().unwrap();
    parent.0.wait().unwrap();
    // Lock release, rather than PID disappearance, also works with Linux orphan zombies.
    tokio::time::timeout(OBSERVATION_HANG_GUARD, async {
        loop {
            if let Ok(process) = CredentialProcess::open(fixture.config.clone()) {
                assert!(server::bind(&process).is_ok());
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    // SAFETY: signal 0 checks only; on Linux the exited child may await PID 1's reap.
    #[cfg(not(target_os = "linux"))]
    assert_ne!(unsafe { libc::kill(pid as i32, 0) }, 0);
    #[cfg(target_os = "linux")]
    let _ = pid;
}

#[test]
fn supervision_child_mode_requires_parent_pipe_before_configuration() {
    let output = std::process::Command::new(credential_process())
        .args(["--child", "/nonexistent-config"])
        .env_remove("SYMBIOTIC_PARENT_FD")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert_eq!(
        String::from_utf8(output.stderr).unwrap().trim(),
        EgressError::StateUnavailable.to_string()
    );
}

#[tokio::test]
async fn supervised_engine_entrypoint() {
    let Some(dir) = std::env::var_os("SUPERVISED_ENGINE_DIR") else {
        return;
    };
    symbiotic_supervise::watch_parent(|| Ok(())).unwrap();
    let dir = std::path::PathBuf::from(dir);
    let id: SignedAttemptId =
        serde_json::from_slice(&std::fs::read(dir.join("status.json")).unwrap()).unwrap();
    let client = socket::UnixEgressClient {
        path: dir.join("egress.sock"),
        max_frame_bytes: 262144,
        timeout: Duration::from_secs(2),
    };
    std::fs::write(dir.join("engine.ready"), "ready").unwrap();
    loop {
        match client.attempt_status(id.clone()).await.unwrap() {
            AttemptStatus::Completed { result } => {
                std::fs::write(
                    dir.join("engine.result.pending"),
                    serde_json::to_vec(&result).unwrap(),
                )
                .unwrap();
                std::fs::rename(dir.join("engine.result.pending"), dir.join("engine.result"))
                    .unwrap();
                std::future::pending::<()>().await;
            }
            AttemptStatus::NotIssued
            | AttemptStatus::Permitted
            | AttemptStatus::Dispatched { .. } => {}
            _ => panic!("unexpected engine recovery state"),
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[tokio::test]
async fn supervision_engine_restart_consumes_saved_completion_without_second_payment() {
    let response_gate = Arc::new(tokio::sync::Semaphore::new(0));
    let fixture = Fixture::with_response_gate(
        200,
        "saved completion".into(),
        Duration::ZERO,
        r#""0.00001234567890123456789""#,
        false,
        false,
        Some(response_gate.clone()),
    )
    .await;
    let process = fixture.process().await;
    let listener = server::bind(&process).unwrap();
    let task = tokio::spawn(server::serve(process, listener));
    let client = socket::UnixEgressClient {
        path: fixture.config.socket_path.clone(),
        max_frame_bytes: fixture.config.max_frame_bytes,
        timeout: Duration::from_secs(2),
    };
    let (admission, payload) = fixture.attempt("engine-restart", 1, 1);
    let id = AdmissionKey::new(KEY.to_vec())
        .unwrap()
        .sign_attempt_id(admission.attempt.attempt_id())
        .unwrap();
    std::fs::write(
        fixture.dir.path().join("status.json"),
        serde_json::to_vec(&id).unwrap(),
    )
    .unwrap();
    let dir = fixture.dir.path().to_owned();
    let supervisor = symbiotic_supervise::Supervisor::start(
        move || {
            let mut command = std::process::Command::new(std::env::current_exe().unwrap());
            command
                .args(["--exact", "supervised_engine_entrypoint", "--nocapture"])
                .env("SUPERVISED_ENGINE_DIR", &dir)
                .stdout(std::process::Stdio::null());
            command
        },
        supervision_policy(),
    )
    .unwrap();
    let symbiotic_supervise::Event::Started(first_pid) = supervisor.next_event().unwrap() else {
        panic!("missing engine");
    };
    wait_for_engine_startup(first_pid, &fixture.dir.path().join("engine.ready")).await;
    let response = client
        .exchange(Request {
            version: PROTOCOL_VERSION,
            operation: Operation::IssuePermit(admission.clone().into()),
        })
        .await
        .unwrap();
    let Reply::Permit(grant) = response.result.unwrap() else {
        panic!("missing permit");
    };
    let dispatch_client = client.clone();
    let dispatch = tokio::spawn(async move {
        dispatch_client
            .exchange(Request {
                version: PROTOCOL_VERSION,
                operation: inject(admission, payload, grant.permit),
            })
            .await
            .unwrap()
    });
    tokio::time::timeout(OBSERVATION_HANG_GUARD, async {
        while fixture.calls.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(
        !dispatch.is_finished(),
        "engine crash must occur during the paid call"
    );
    // SAFETY: the supervisor still owns this child; SIGKILL models a native engine fault.
    assert_eq!(unsafe { libc::kill(first_pid as i32, libc::SIGKILL) }, 0);
    assert!(matches!(
        supervisor.next_event().unwrap(),
        symbiotic_supervise::Event::Exited(_)
    ));
    assert!(
        !fixture.dir.path().join("engine.result").exists(),
        "first engine cannot consume a gated response"
    );
    assert!(
        !dispatch.is_finished(),
        "paid call must remain pending until the first engine is reaped"
    );
    response_gate.add_permits(1);
    let symbiotic_supervise::Event::Started(second_pid) = supervisor.next_event().unwrap() else {
        panic!("engine not restarted");
    };
    assert_ne!(first_pid, second_pid);
    let result = dispatched(dispatch.await.unwrap().result.unwrap());
    assert!(
        matches!(&result.output, Some(ProviderOutput::Chat { text, .. }) if text == "saved completion")
    );
    tokio::time::timeout(OBSERVATION_HANG_GUARD, async {
        while !fixture.dir.path().join("engine.result").exists() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let recovered: DispatchResult =
        serde_json::from_slice(&std::fs::read(fixture.dir.path().join("engine.result")).unwrap())
            .unwrap();
    assert_eq!(
        serde_json::to_value(result).unwrap(),
        serde_json::to_value(recovered).unwrap()
    );
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
    supervisor.stop().unwrap();
    task.abort();
    let _ = task.await;
}

#[test]
fn protection_refusal_embedded_entrypoint() {
    if let Some(path) = std::env::var_os("PROTECTED_READ_PROBE") {
        std::fs::read(path).unwrap();
        return;
    }
    let Ok(config) = std::env::var("REFUSED_PROCESS_CONFIG") else {
        return;
    };
    let config: ProcessConfig = serde_json::from_str(&config).unwrap();
    assert!(matches!(
        CredentialProcess::open(config),
        Err(EgressError::StateUnavailable)
    ));
}

#[cfg(target_os = "linux")]
fn refuse_protection(command: &mut std::process::Command, syscall: libc::c_long) {
    use std::os::unix::process::CommandExt;
    // SAFETY: pre_exec performs only prctl on preallocated BPF data.
    unsafe {
        command.pre_exec(move || {
            let filter = [
                libc::sock_filter {
                    code: 0x20,
                    jt: 0,
                    jf: 0,
                    k: 0,
                },
                libc::sock_filter {
                    code: 0x15,
                    jt: 0,
                    jf: 1,
                    k: syscall as u32,
                },
                libc::sock_filter {
                    code: 0x06,
                    jt: 0,
                    jf: 0,
                    k: libc::SECCOMP_RET_ERRNO | libc::EPERM as u32,
                },
                libc::sock_filter {
                    code: 0x06,
                    jt: 0,
                    jf: 0,
                    k: libc::SECCOMP_RET_ALLOW,
                },
            ];
            let program = libc::sock_fprog {
                len: filter.len() as u16,
                filter: filter.as_ptr() as *mut _,
            };
            if libc::prctl(
                libc::PR_SET_NO_NEW_PRIVS,
                1 as libc::c_ulong,
                0 as libc::c_ulong,
                0 as libc::c_ulong,
                0 as libc::c_ulong,
            ) != 0
                || libc::prctl(
                    libc::PR_SET_SECCOMP,
                    libc::SECCOMP_MODE_FILTER as libc::c_ulong,
                    &program,
                ) != 0
            {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[tokio::test]
async fn protection_refusal_aborts_embedded_and_executable_startup_before_protected_reads() {
    let fixture = Fixture::new(200, "unused".into(), Duration::ZERO).await;
    let config_path = fixture.dir.path().join("config.json");
    let config_json = serde_json::to_string(&fixture.config).unwrap();
    std::fs::write(&config_path, &config_json).unwrap();
    std::fs::set_permissions(&config_path, std::fs::Permissions::from_mode(0o600)).unwrap();
    #[cfg(target_os = "macos")]
    let library = {
        let library = fixture.dir.path().join("refuse-protection.dylib");
        let compiler = std::env::var("CC").expect("configured compiler cache");
        let mut compiler = compiler.split_whitespace();
        let output = std::process::Command::new(compiler.next().unwrap())
            .args(compiler)
            .args(["-O2", "-dynamiclib"])
            .arg(manifest_dir().join("tests/refuse_protection.c"))
            .arg("-o")
            .arg(&library)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        library
    };
    #[cfg(target_os = "macos")]
    let protected_root = fixture.dir.path().canonicalize().unwrap();
    #[cfg(target_os = "macos")]
    {
        // Positive control: the observer must record an actual protected read.
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "protection_refusal_embedded_entrypoint",
                "--nocapture",
            ])
            .env("PROTECTED_READ_PROBE", &config_path)
            .env("DYLD_INSERT_LIBRARIES", &library)
            .env("PROTECTED_READ_ROOT", &protected_root)
            .output()
            .unwrap();
        assert!(output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains("protected read"));
    }
    #[cfg(target_os = "linux")]
    let refusals = [libc::SYS_prctl, libc::SYS_prlimit64];
    #[cfg(target_os = "macos")]
    let refusals = [library.as_path()];
    for refusal in refusals {
        for embedded in [true, false] {
            let mut command = if embedded {
                let mut command = std::process::Command::new(std::env::current_exe().unwrap());
                command
                    .args([
                        "--exact",
                        "protection_refusal_embedded_entrypoint",
                        "--nocapture",
                    ])
                    .env("REFUSED_PROCESS_CONFIG", &config_json);
                command
            } else {
                let mut command = std::process::Command::new(credential_process());
                command.arg(&config_path);
                command
            };
            #[cfg(target_os = "linux")]
            refuse_protection(&mut command, refusal);
            #[cfg(target_os = "linux")]
            let monitor = {
                use std::os::fd::{FromRawFd, OwnedFd};
                // Watch only protected files; startup may read ordinary runtime libraries.
                let fd = unsafe { libc::inotify_init1(libc::IN_NONBLOCK | libc::IN_CLOEXEC) };
                assert!(fd >= 0);
                let monitor = unsafe { OwnedFd::from_raw_fd(fd) };
                for path in [
                    &config_path,
                    &fixture.dir.path().join("admission"),
                    &fixture.dir.path().join("provider"),
                ] {
                    use std::os::unix::ffi::OsStrExt;
                    let path = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
                    assert!(
                        unsafe {
                            libc::inotify_add_watch(
                                fd,
                                path.as_ptr(),
                                libc::IN_OPEN | libc::IN_ACCESS,
                            )
                        } >= 0
                    );
                }
                monitor
            };
            #[cfg(target_os = "macos")]
            {
                command
                    .env("DYLD_INSERT_LIBRARIES", refusal)
                    .env("PROTECTED_READ_ROOT", &protected_root);
            }
            let output = command.output().unwrap();
            assert_eq!(
                output.status.success(),
                embedded,
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            if !embedded {
                assert_eq!(
                    String::from_utf8_lossy(&output.stderr).trim(),
                    EgressError::StateUnavailable.to_string()
                );
            }
            assert!(
                !fixture.config.state_dir.exists(),
                "refusal must precede state and secret access"
            );
            assert!(!String::from_utf8_lossy(&output.stderr).contains("protected read"));
            #[cfg(target_os = "linux")]
            {
                use std::os::fd::AsRawFd;
                let mut events = [0_u8; 4096];
                assert_eq!(
                    unsafe {
                        libc::read(
                            monitor.as_raw_fd(),
                            events.as_mut_ptr().cast(),
                            events.len(),
                        )
                    },
                    -1
                );
                assert_eq!(
                    std::io::Error::last_os_error().kind(),
                    std::io::ErrorKind::WouldBlock,
                    "protected files must remain unopened"
                );
            }
        }
    }
}

#[tokio::test]
async fn anthropic_route_dispatches_only_through_the_credential_permit_and_recovers() {
    for thinking in [
        None,
        Some(symbiotic_ai_runtime::model::ThinkingMode::Enabled),
        Some(symbiotic_ai_runtime::model::ThinkingMode::Disabled),
    ] {
        for stop_reason in ["end_turn", "model_context_window_exceeded"] {
            let mut fixture = Fixture::with_http_response(200,
            serde_json::json!({"content":[{"type":"text","text":"answer"}],"stop_reason":stop_reason,
                "usage":{"input_tokens":7,"output_tokens":348,"output_tokens_details":{"thinking_tokens":312},"cost_usd":"0.000000125"}}).to_string(),
            Duration::ZERO, "0", true, false).await;
            fixture.config.routes[0].provider = RouteProvider::AnthropicChat {
                operator: "anthropic".into(),
                thinking,
            };
            let process = fixture.process().await;
            let (admission, payload) = fixture.attempt("anthropic", 1, 1);
            let granted = permit(&process, &admission).await;
            let result = dispatched(
                exchange(&process, inject(admission.clone(), payload, granted))
                    .await
                    .unwrap(),
            );
            assert!(
                matches!(&result.output, Some(ProviderOutput::Chat {text, ..}) if text == "answer")
            );
            assert_eq!(result.receipt.usage.input_tokens, Some(7));
            assert_eq!(result.receipt.usage.output_tokens, Some(348));
            assert_eq!(result.receipt.usage.reasoning_tokens, Some(312));
            assert_eq!(
                result.receipt.usage.reported_cost_usd.as_deref(),
                Some("0.000000125")
            );
            assert_eq!(result.receipt.status, DispatchStatus::Succeeded);
            assert_eq!(result.receipt.spend_state, SpendState::Settled);
            assert!(result.receipt_persisted);
            assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
            drop(process);
            let process = fixture.process().await;
            let AttemptStatus::Completed { result: recovered } = status(&process, &admission).await
            else {
                panic!("missing recovered answer");
            };
            assert_eq!(
                recovered.receipt.usage.reported_cost_usd.as_deref(),
                Some("0.000000125")
            );
            assert_eq!(
                serde_json::to_value(&result).unwrap(),
                serde_json::to_value(recovered).unwrap()
            );
            assert!(
                matches!(exchange(&process, Operation::Receipt(signed_id(&admission))).await,
            Ok(Reply::Receipt(Some(receipt))) if receipt.usage.reasoning_tokens == Some(312) && receipt.usage.output_tokens == Some(348) && receipt.usage.reported_cost_usd.as_deref() == Some("0.000000125") && receipt.spend_state == SpendState::Settled)
            );
            let recovered = exchange(&process, Operation::IssuePermit(admission.into()))
                .await
                .unwrap();
            assert!(matches!(
                recovered,
                Reply::Permit(PermitGrant {
                    status: AttemptStatus::Completed { .. },
                    ..
                })
            ));
            assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
        }
    }
}

#[tokio::test]
async fn expanded_anthropic_wire_payload_is_refused_before_consumption() {
    expanded_wire_payload_is_refused(2).await;
}

#[tokio::test]
async fn anthropic_enabled_thinking_temperature_is_refused_before_consumption() {
    anthropic_invalid_conversation_is_refused(
        false,
        Some(0.0),
        Some(symbiotic_ai_runtime::model::ThinkingMode::Enabled),
    )
    .await;
}

#[tokio::test]
async fn anthropic_assistant_prefill_is_refused_before_consumption() {
    anthropic_invalid_conversation_is_refused(
        true,
        None,
        Some(symbiotic_ai_runtime::model::ThinkingMode::Enabled),
    )
    .await;
}

#[tokio::test]
async fn anthropic_invalid_temperatures_are_refused_before_consumption() {
    for thinking in [
        None,
        Some(symbiotic_ai_runtime::model::ThinkingMode::Disabled),
    ] {
        for temperature in [-0.1, 1.5, f32::NAN, f32::NEG_INFINITY, f32::INFINITY] {
            anthropic_invalid_conversation_is_refused(false, Some(temperature), thinking).await;
        }
    }
}

#[tokio::test]
async fn in_process_nonfinite_temperatures_cannot_alias_an_admitted_request() {
    let mut fixture = Fixture::with_http_response(
        200,
        serde_json::json!({
            "content": [{"type": "text", "text": "answer"}],
            "stop_reason": "end_turn",
            "usage": {"input_tokens": 7, "output_tokens": 1}
        })
        .to_string(),
        Duration::ZERO,
        "0",
        true,
        false,
    )
    .await;
    fixture.config.routes[0].provider = RouteProvider::AnthropicChat {
        operator: "anthropic".into(),
        thinking: None,
    };
    let process = fixture.process().await;
    let client = InProcessEgressClient::new(process.clone());
    let (admission, payload) = fixture.attempt("nonfinite-temperature", 1, 1);
    let granted = permit(&process, &admission).await;
    for temperature in [f32::NAN, f32::NEG_INFINITY, f32::INFINITY] {
        let mut invalid_payload = payload.clone();
        let ProviderPayload::Chat(chat) = &mut invalid_payload else {
            panic!("chat expected")
        };
        chat.temperature = Some(temperature);
        // JSON maps nonfinite floats to null, so the digest alone cannot
        // distinguish these invalid inputs from the admitted absent temperature.
        assert_eq!(invalid_payload.digest().unwrap(), payload.digest().unwrap());
        assert!(matches!(
            exchange_client(
                &client,
                inject(admission.clone(), invalid_payload, granted.clone()),
            )
            .await,
            Err(EgressError::InvalidRequest)
        ));
        assert!(matches!(
            status(&process, &admission).await,
            AttemptStatus::Permitted
        ));
        assert!(matches!(
            exchange(&process, Operation::Receipt(signed_id(&admission))).await,
            Ok(Reply::Receipt(None))
        ));
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
        assert_eq!(ledger_totals(&fixture), (0, 0));
    }
    let result = dispatched(
        exchange_client(&client, inject(admission, payload, granted))
            .await
            .unwrap(),
    );
    assert_eq!(result.receipt.status, DispatchStatus::Succeeded);
    assert!(matches!(result.output, Some(ProviderOutput::Chat { text, .. }) if text == "answer"));
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
    assert_eq!(ledger_totals(&fixture), (1, 1));
}

async fn anthropic_invalid_conversation_is_refused(
    prefill: bool,
    temperature: Option<f32>,
    thinking: Option<symbiotic_ai_runtime::model::ThinkingMode>,
) {
    let mut fixture = Fixture::new(200, "unused".into(), Duration::ZERO).await;
    fixture.config.routes[0].provider = RouteProvider::AnthropicChat {
        operator: "anthropic".into(),
        thinking,
    };
    fixture.config.routes[0].secret = SecretSource::OwnerOnlyFile {
        path: fixture.dir.path().join("missing-provider"),
    };
    let process = fixture.process().await;
    let (mut admission, mut payload) = fixture.attempt("invalid-anthropic", 1, 1);
    let ProviderPayload::Chat(request) = &mut payload else {
        panic!("chat expected")
    };
    if prefill {
        request.messages.push(ChatMessage {
            role: "assistant".into(),
            content: "Answer:".into(),
        });
    }
    request.temperature = temperature;
    admission.attempt.input_digest = payload.digest().unwrap();
    let admission = AdmissionKey::new(KEY.to_vec())
        .unwrap()
        .sign_attempt(admission.attempt)
        .unwrap();
    let granted = permit(&process, &admission).await;
    assert!(matches!(
        exchange(&process, inject(admission.clone(), payload, granted)).await,
        Err(EgressError::InvalidRequest)
    ));
    assert!(matches!(
        exchange(&process, Operation::Receipt(signed_id(&admission))).await,
        Ok(Reply::Receipt(None))
    ));
    assert!(matches!(
        status(&process, &admission).await,
        AttemptStatus::Permitted
    ));
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
    assert_eq!(ledger_totals(&fixture), (0, 0));
}

#[tokio::test]
async fn both_clients_refuse_deeply_nested_requests_before_permit_consumption() {
    let fixture = Fixture::new(200, "answer".into(), Duration::ZERO).await;
    let process = fixture.process().await;
    let listener = server::bind(&process).unwrap();
    let task = tokio::spawn(server::serve(process.clone(), listener));
    let socket_client = socket::UnixEgressClient {
        path: fixture.config.socket_path.clone(),
        max_frame_bytes: fixture.config.max_frame_bytes,
        timeout: Duration::from_secs(3),
    };
    let in_process_client = InProcessEgressClient::new(process.clone());
    let (admission, mut payload) = fixture.attempt("nested-metadata", 1, 1);
    let ProviderPayload::Chat(chat) = &mut payload else {
        panic!("wrong payload")
    };
    let mut metadata = serde_json::json!(0);
    for _ in 0..200 {
        metadata = serde_json::Value::Array(vec![metadata]);
    }
    chat.metadata = metadata;
    assert!(
        serde_json::to_vec(&payload).unwrap().len() <= fixture.config.routes[0].max_input_bytes
    );
    let mut attempt = admission.attempt;
    attempt.input_digest = payload.digest().unwrap();
    let admission = AdmissionKey::new(KEY.to_vec())
        .unwrap()
        .sign_attempt(attempt)
        .unwrap();
    let granted = permit(&process, &admission).await;
    let request = Request {
        version: PROTOCOL_VERSION,
        operation: inject(admission.clone(), payload, granted.clone()),
    };
    encode_frame(&request, fixture.config.max_frame_bytes).unwrap();
    let db = rusqlite::Connection::open(
        fixture
            .config
            .state_dir
            .join(symbiotic_ai_runtime::QUEUE_DATABASE),
    )
    .unwrap();
    for client in [
        &socket_client as &dyn EgressClient,
        &in_process_client as &dyn EgressClient,
    ] {
        let response = client.exchange(request.clone()).await.unwrap();
        assert_eq!(response.version, PROTOCOL_VERSION);
        assert!(matches!(response.result, Err(EgressError::InvalidRequest)));
        assert!(matches!(
            status(&process, &admission).await,
            AttemptStatus::Permitted
        ));
        assert_eq!(
            db.query_row(
                "SELECT consumed, accepted_attempts FROM egress_permits WHERE token=?1",
                [&granted.token],
                |row| Ok((row.get::<_, bool>(0)?, row.get::<_, u32>(1)?))
            )
            .unwrap(),
            (false, 0)
        );
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
        assert_eq!(ledger_totals(&fixture), (0, 0));
    }
    task.abort();
    let _ = task.await;
}

#[tokio::test]
async fn in_process_oversized_requests_are_refused_before_handler_execution() {
    let fixture = Fixture::new(200, "answer".into(), Duration::ZERO).await;
    let process = fixture.process().await;
    let client = InProcessEgressClient::new(process.clone());
    let tenant = "t".repeat(fixture.config.max_frame_bytes as usize + 1);
    let key = AdmissionKey::new(KEY.to_vec()).unwrap();
    let oversized_grant = key
        .sign_grant_revision(GrantRevision {
            tenant: tenant.clone(),
            incarnation: "incarnation".into(),
            revision: 1,
        })
        .unwrap();
    assert!(matches!(
        exchange_client(&client, Operation::PublishGrantRevision(oversized_grant)).await,
        Err(EgressError::LimitExceeded)
    ));
    // A rejected publication must not have reached the canonical grant registry.
    let db = rusqlite::Connection::open(
        fixture
            .config
            .state_dir
            .join(symbiotic_ai_runtime::QUEUE_DATABASE),
    )
    .unwrap();
    let published: bool = db
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM egress_grant_revisions WHERE grant_key=?1)",
            [digest(&(tenant, "incarnation")).unwrap()],
            |row| row.get(0),
        )
        .unwrap();
    assert!(!published);
    let (admission, mut payload) = fixture.attempt("oversized-frame", 1, 1);
    let ProviderPayload::Chat(chat) = &mut payload else {
        panic!("wrong payload")
    };
    chat.messages[0].content = "x".repeat(fixture.config.max_frame_bytes as usize + 1);
    let mut attempt = admission.attempt;
    attempt.input_digest = payload.digest().unwrap();
    let admission = key.sign_attempt(attempt).unwrap();
    let granted = permit(&process, &admission).await;
    assert!(matches!(
        exchange_client(&client, inject(admission.clone(), payload, granted)).await,
        Err(EgressError::LimitExceeded)
    ));
    assert!(matches!(
        status(&process, &admission).await,
        AttemptStatus::Permitted
    ));
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
    assert_eq!(ledger_totals(&fixture), (0, 0));
}

#[tokio::test]
async fn in_process_oversized_recovered_response_is_refused_without_losing_the_answer() {
    let answer = "a".repeat(20 * 1024);
    let fixture = Fixture::new(200, answer.clone(), Duration::ZERO).await;
    let process = fixture.process().await;
    let (admission, payload) = fixture.attempt("large-recovery", 1, 1);
    let granted = permit(&process, &admission).await;
    let result = dispatched(
        exchange(&process, inject(admission.clone(), payload, granted))
            .await
            .unwrap(),
    );
    assert!(result.receipt_persisted);
    assert!(matches!(&result.output, Some(ProviderOutput::Chat { text, .. }) if text == &answer));
    drop(process);

    let mut smaller = fixture.config.clone();
    smaller.max_frame_bytes = 8192;
    smaller.routes[0].max_field_bytes = 64;
    smaller.routes[0].max_response_bytes = 256;
    smaller.routes[0].max_input_bytes = 4096;
    let client = InProcessEgressClient::new(CredentialProcess::open(smaller).unwrap());
    for operation in [
        Operation::AttemptStatus(signed_id(&admission)),
        Operation::IssuePermit(admission.clone().into()),
    ] {
        let response = client
            .exchange(Request {
                version: PROTOCOL_VERSION,
                operation,
            })
            .await
            .unwrap();
        assert_eq!(response.version, PROTOCOL_VERSION);
        assert!(matches!(response.result, Err(EgressError::LimitExceeded)));
    }
    drop(client);

    let client = InProcessEgressClient::new(fixture.process().await);
    let AttemptStatus::Completed { result: recovered } =
        client.attempt_status(signed_id(&admission)).await.unwrap()
    else {
        panic!("missing stored answer")
    };
    assert_eq!(
        serde_json::to_value(result).unwrap(),
        serde_json::to_value(recovered).unwrap()
    );
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
    assert_eq!(ledger_totals(&fixture), (1, 1));
}

#[tokio::test]
async fn in_process_exchange_survives_a_dropped_caller_and_recovers_without_resending() {
    use std::{future::Future, task::Poll};
    use symbiotic_credential_process::InProcessEgressClient;

    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let mut fixture = Fixture::with_response_gate(
        200,
        "thread answer".into(),
        Duration::ZERO,
        "0",
        false,
        true,
        Some(gate.clone()),
    )
    .await;
    fixture.config.routes[0].secret = SecretSource::None;
    fixture.config.routes[0].secret_ref.clear();
    let process = fixture.process().await;
    let client = InProcessEgressClient::new(process.clone());
    let (admission, payload) = fixture.attempt("thread-cancel", 1, 1);
    let response = client
        .exchange(Request {
            version: PROTOCOL_VERSION,
            operation: Operation::IssuePermit(admission.clone().into()),
        })
        .await
        .unwrap();
    let Reply::Permit(grant) = response.result.unwrap() else {
        panic!("missing permit")
    };
    let mut caller = Box::pin(client.exchange(Request {
        version: PROTOCOL_VERSION,
        operation: inject(admission.clone(), payload, grant.permit),
    }));
    // Poll once on this single-thread runtime, then drop before the spawned
    // Foundation handler can run. The exchange must still reach acceptance.
    let polled = std::future::poll_fn(|cx| Poll::Ready(caller.as_mut().poll(cx))).await;
    assert!(polled.is_pending());
    // No runtime yield has occurred: acceptance must not have run in the caller.
    let consumed: bool = rusqlite::Connection::open_with_flags(
        fixture
            .config
            .state_dir
            .join(symbiotic_ai_runtime::QUEUE_DATABASE),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap()
    .query_row(
        "SELECT consumed FROM egress_permits WHERE attempt_digest=?1",
        [digest(&admission.attempt).unwrap()],
        |row| row.get(0),
    )
    .unwrap();
    assert!(
        !consumed,
        "acceptance ran on the caller instead of the Foundation task"
    );
    drop(caller);
    tokio::time::timeout(Duration::from_secs(2), async {
        while fixture.calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    gate.add_permits(1);
    let id = signed_id(&admission);
    let result = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match client.attempt_status(id.clone()).await.unwrap() {
                AttemptStatus::Completed { result } => break result,
                AttemptStatus::Dispatched { .. } => tokio::task::yield_now().await,
                _ => panic!("unexpected recovery status"),
            }
        }
    })
    .await
    .unwrap();
    assert!(
        matches!(&result.output, Some(ProviderOutput::Chat { text, .. }) if text == "thread answer")
    );
    assert!(result.receipt_persisted);
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
    drop(client);
    drop(process);
    let reopened = InProcessEgressClient::new(fixture.process().await);
    let AttemptStatus::Completed { result: recovered } = reopened.attempt_status(id).await.unwrap()
    else {
        panic!("missing recovered answer")
    };
    assert_eq!(result.receipt.reference, recovered.receipt.reference);
    assert!(
        matches!(recovered.output, Some(ProviderOutput::Chat { text, .. }) if text == "thread answer")
    );
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn resolver_is_lazy_and_provider_uses_its_key_in_thread_mode() {
    initialize_panic_reporting();
    let mut fixture = Fixture::new(200, "resolver answer".into(), Duration::ZERO).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    fixture.config.routes[0].secret = SecretSource::Resolver {
        name: "named-provider-key".into(),
        resolve: Arc::new(move |name| {
            assert_eq!(name, "named-provider-key");
            // The regression in secrets checks refusal before OS protection.
            count.fetch_add(1, Ordering::SeqCst);
            Ok(symbiotic_ai_runtime::model::SecretValue::new(
                SECRET.as_bytes().to_vec(),
            ))
        }),
    };
    let process = fixture.process().await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let (admission, payload) = fixture.attempt("resolver", 1, 1);
    let granted = permit(&process, &admission).await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let result = dispatched(
        exchange(&process, inject(admission, payload, granted))
            .await
            .unwrap(),
    );
    assert_eq!(result.receipt.status, DispatchStatus::Succeeded);
    assert!(
        matches!(result.output, Some(ProviderOutput::Chat { text, .. }) if text == "resolver answer")
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn regression_slow_resolver_delays_dispatch_beyond_provider_timeout() {
    initialize_panic_reporting();
    tokio::time::timeout(Duration::from_secs(60), async {
        let mut fixture = Fixture::new(200, "resolver released".into(), Duration::ZERO).await;
        fixture.config.routes[0].timeout_seconds = 1;
        fixture.config.routes[0].max_in_flight = 1;
        let entered = Arc::new(tokio::sync::Notify::new());
        let entered_resolver = entered.clone();
        let (release, receive) = std::sync::mpsc::channel();
        let receive = std::sync::Mutex::new(receive);
        fixture.config.routes[0].secret = SecretSource::Resolver {
            name: "provider-key".into(),
            resolve: Arc::new(move |_| {
                entered_resolver.notify_one();
                // Bound the blocking callback independently of async/runtime cleanup.
                // Dropping the test's sender also releases it on assertion failure.
                receive.lock().unwrap().recv_timeout(Duration::from_secs(60))?;
                Ok(symbiotic_ai_runtime::model::SecretValue::new(
                    SECRET.as_bytes().to_vec(),
                ))
            }),
        };
        let process = fixture.process().await;
        let (admission, payload) = fixture.attempt("slow-resolver", 1, 1);
        let granted = permit(&process, &admission).await;
        let mut dispatch = Box::pin(exchange(
            &process,
            inject(admission.clone(), payload, granted),
        ));
        tokio::select! {
            _ = entered.notified() => (),
            _ = &mut dispatch => panic!("dispatch completed before resolver entry"),
        }

        // Observe past the one-second provider timeout while keeping the callback held.
        assert!(
            tokio::time::timeout(Duration::from_millis(1100), &mut dispatch)
                .await
                .is_err(),
            "provider timeout must not complete dispatch while its resolver is blocked"
        );
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
        assert!(matches!(
            status(&process, &admission).await,
            AttemptStatus::Dispatched { .. }
        ));

        release.send(()).unwrap();
        let result = dispatched(dispatch.await.unwrap());
        assert_eq!(result.receipt.status, DispatchStatus::Succeeded);
        assert!(result.receipt_persisted);
        assert!(
            matches!(result.output, Some(ProviderOutput::Chat { text, .. }) if text == "resolver released")
        );
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
    })
    .await
    .expect("controlled resolver dispatch must finish within the sixty-second hang guard");
}

#[test]
fn resolver_debug_redacts_captured_key_without_invoking_callback() {
    let key = symbiotic_ai_runtime::model::SecretValue::new(SECRET.as_bytes().to_vec());
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let source = SecretSource::Resolver {
        name: "provider-key".into(),
        resolve: Arc::new(move |_| {
            count.fetch_add(1, Ordering::SeqCst);
            Ok(key.clone())
        }),
    };
    let diagnostic = format!("{source:?}");
    assert!(diagnostic.contains("Resolver { .. }"));
    assert!(!diagnostic.contains("synthetic-WP14-credential"));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn resolver_error_is_redacted_and_never_reaches_provider() {
    initialize_panic_reporting();
    let mut fixture = Fixture::new(200, "unused".into(), Duration::ZERO).await;
    fixture.config.routes[0].secret = SecretSource::Resolver {
        name: "provider-key".into(),
        resolve: Arc::new(|_| Err(std::io::Error::other(SECRET).into())),
    };
    let process = fixture.process().await;
    let (admission, payload) = fixture.attempt("resolver-error", 1, 1);
    let granted = permit(&process, &admission).await;
    let result = dispatched(
        exchange(&process, inject(admission.clone(), payload, granted))
            .await
            .unwrap(),
    );
    assert_eq!(result.error, Some(EgressError::CredentialUnavailable));
    assert_eq!(result.receipt.status, DispatchStatus::CredentialUnavailable);
    assert!(
        !serde_json::to_string(&result)
            .unwrap()
            .contains("synthetic-WP14-credential")
    );
    assert!(!format!("{result:?}").contains("synthetic-WP14-credential"));
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
    let AttemptStatus::Failed { result: recovered } = status(&process, &admission).await else {
        panic!("expected retained redacted failure");
    };
    assert_eq!(recovered.error, Some(EgressError::CredentialUnavailable));
    assert!(
        !serde_json::to_string(&recovered)
            .unwrap()
            .contains("synthetic-WP14-credential")
    );
}

#[tokio::test]
async fn resolver_panic_child() {
    fn lookup() -> std::io::Result<symbiotic_ai_runtime::model::SecretValue<Vec<u8>>> {
        Err(std::io::Error::other(SECRET))
    }

    let Some(mode) = std::env::var_os("RESOLVER_PANIC_TEST") else {
        return;
    };
    // Use Rust's default reporter, rather than the test harness's hook.
    drop(std::panic::take_hook());
    initialize_resolver_panic_hook(std::panic::take_hook());
    let mut fixture = Fixture::new(200, "unused".into(), Duration::ZERO).await;
    let worker = mode == "worker";
    let source = SecretSource::Resolver {
        name: "provider-key".into(),
        resolve: Arc::new(move |_| {
            if worker {
                return std::thread::spawn(|| lookup().expect("worker lookup failed"))
                    .join()
                    .map_err(|_| std::io::Error::other("lookup worker panicked").into());
            }
            Ok(lookup().expect("lookup failed"))
        }),
    };
    if mode == "admission" {
        fixture.config.admission_key = source;
        assert!(matches!(
            CredentialProcess::open(fixture.config.clone()),
            Err(EgressError::CredentialUnavailable)
        ));
    } else {
        assert!(mode == "provider" || mode == "worker" || mode == "reporter");
        if mode == "reporter" {
            fixture.config.admission_key = SecretSource::Resolver {
                name: "admission-key".into(),
                resolve: Arc::new(|_| {
                    Ok(symbiotic_ai_runtime::model::SecretValue::new(KEY.to_vec()))
                }),
            };
        }
        fixture.config.routes[0].secret = source;
        let process = fixture.process().await;
        if mode == "reporter" {
            // App reporter initialization follows a successful admission-key lookup.
            initialize_resolver_panic_hook(|info| eprintln!("app reporter: {info}"));
        }
        let (admission, payload) = fixture.attempt("resolver-panic", 1, 1);
        let granted = permit(&process, &admission).await;
        let result = dispatched(
            exchange(&process, inject(admission.clone(), payload, granted))
                .await
                .unwrap(),
        );
        assert_eq!(result.error, Some(EgressError::CredentialUnavailable));
        assert_eq!(result.receipt.status, DispatchStatus::CredentialUnavailable);
        assert_eq!(result.receipt.spend_state, SpendState::Released);
        assert!(result.output.is_none());
        assert!(!serde_json::to_string(&result).unwrap().contains(SECRET));
        let AttemptStatus::Failed { result: recovered } = status(&process, &admission).await else {
            panic!("expected retained redacted failure");
        };
        assert_eq!(recovered.error, Some(EgressError::CredentialUnavailable));
        assert!(!serde_json::to_string(&recovered).unwrap().contains(SECRET));
    }
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
    // Unrelated panics must retain ordinary reporting after resolution too.
    assert!(std::panic::catch_unwind(|| panic!("unrelated panic after resolution")).is_err());
}

fn assert_resolver_panic_is_redacted(mode: &str) {
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "resolver_panic_child", "--nocapture"])
        .env("RESOLVER_PANIC_TEST", mode)
        .env("RUST_BACKTRACE", "1")
        .output()
        .unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(!stderr.contains("synthetic-WP14-credential"));
    assert!(output.status.success(), "{stderr}");
    assert_eq!(stderr.matches("credential resolver panicked").count(), 1);
    assert!(stderr.contains("unrelated panic after resolution"));
    if mode == "reporter" {
        assert!(stderr.contains("app reporter:"));
    }
}

#[test]
fn resolver_panics_are_redacted_at_startup() {
    assert_resolver_panic_is_redacted("admission");
}

#[test]
fn resolver_panics_are_redacted_at_dispatch() {
    assert_resolver_panic_is_redacted("provider");
}

#[test]
fn resolver_panics_are_redacted_after_reporter_initialization() {
    assert_resolver_panic_is_redacted("reporter");
}

#[test]
fn resolver_worker_panics_are_redacted() {
    assert_resolver_panic_is_redacted("worker");
}

#[tokio::test]
async fn resolver_configuration_is_refused_in_child_mode_without_invoking_callback() {
    let fixture = Fixture::new(200, "unused".into(), Duration::ZERO).await;
    fixture.config.validate_child_process().unwrap();
    for admission in [true, false] {
        let mut config = fixture.config.clone();
        let source = SecretSource::Resolver {
            name: "provider-key".into(),
            resolve: Arc::new(|_| panic!("child configuration must never resolve a key")),
        };
        if admission {
            config.admission_key = source;
        } else {
            config.routes[0].secret = source;
        }
        assert_eq!(
            config.validate_child_process(),
            Err(EgressError::ResolverRequiresThreadMode)
        );
        assert!(serde_json::to_vec(&config).is_err());
    }
    let mut json = serde_json::to_value(&fixture.config).unwrap();
    json["routes"][0]["secret"] =
        serde_json::json!({"backend": "resolver", "name": "provider-key"});
    assert!(serde_json::from_value::<ProcessConfig>(json).is_err());
}

fn jobs_scope() -> JobScope {
    JobScope {
        tenant: "tenant".into(),
        incarnation: "incarnation".into(),
        queue: "derivation".into(),
    }
}
async fn job_call(
    client: &impl EgressClient,
    command: JobsCommand,
) -> Result<JobsReply, JobsClientError> {
    struct BorrowedClient<'a, C>(&'a C);
    #[async_trait::async_trait]
    impl<C: EgressClient> EgressClient for BorrowedClient<'_, C> {
        async fn exchange(&self, request: Request) -> Result<Response, EgressError> {
            self.0.exchange(request).await
        }
    }
    JobsClient::new(
        BorrowedClient(client),
        jobs_scope(),
        AdmissionKey::new(KEY.to_vec()).unwrap(),
    )
    .request(command)
    .await
}
fn queued(fixture: &Fixture, key: &str) -> EnqueueJob {
    let (admission, payload) = fixture.job_attempt(key, 1, 1);
    EnqueueJob {
        group: Some("task".into()),
        owners: vec!["input-owner".into()],
        admission,
        payload,
    }
}
async fn enqueue_id(client: &impl EgressClient, input: EnqueueJob) -> JobId {
    let JobsReply::Enqueued(items) = job_call(client, JobsCommand::EnqueueJobs(vec![input]))
        .await
        .unwrap()
    else {
        panic!("enqueue reply")
    };
    match &items[0] {
        Enqueued::Inserted(id) | Enqueued::Joined(id) => id.clone(),
        _ => panic!("already done"),
    }
}
async fn wait_job(client: &impl EgressClient, job: &JobId, state: JobState) -> Box<JobRecord> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let JobsReply::Status(row) = job_call(client, JobsCommand::JobStatus(job.clone()))
                .await
                .unwrap()
            else {
                panic!("status")
            };
            assert!(row.payload.is_none() && row.admission.is_none() && row.output.is_none());
            if row.state == state {
                return row;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn jobs_authenticated_owner_purge_removes_only_owners_copies() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut fixture = Fixture::new(200, "saved answer".into(), Duration::ZERO).await;
        let mut foreign_route = fixture.config.routes[0].clone();
        foreign_route.tenant = "foreign-tenant".into();
        fixture.config.routes.push(foreign_route);
        let process = fixture.process().await;
        let client = InProcessEgressClient::new(process.clone());
        let runtime = symbiotic_ai_runtime::Runtime::open(symbiotic_ai_runtime::RuntimeConfig {
            state_dir: Some(fixture.config.state_dir.clone()),
            ..Default::default()
        })
        .unwrap();
        let jobs = runtime
            .model_jobs(jobs_scope(), fixture.config.jobs.clone())
            .unwrap();
        let conn = rusqlite::Connection::open(
            fixture
                .config
                .state_dir
                .join(symbiotic_ai_runtime::QUEUE_DATABASE),
        )
        .unwrap();
        let receipt = |reference: &str| -> (String, Option<String>, Option<String>) {
            conn.query_row(
                "SELECT state,usage,recovery FROM spend_receipts WHERE reference=?1",
                [reference],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap()
        };
        let mut ids = Vec::new();
        for owner in ["input-owner", "other-owner"] {
            for waiting in [false, true] {
                let mut input = queued(&fixture, &format!("{owner}-{waiting}"));
                input.owners = vec![owner.into()];
                if waiting {
                    input.admission.attempt.expires_at = unix_seconds();
                    input.admission = AdmissionKey::new(KEY.to_vec())
                        .unwrap()
                        .sign_attempt(input.admission.attempt)
                        .unwrap();
                }
                let id = enqueue_id(&client, input).await;
                let row = wait_job(
                    &client,
                    &id,
                    if waiting {
                        JobState::AwaitingAdmission
                    } else {
                        JobState::Succeeded
                    },
                )
                .await;
                let accounting = row.receipt.as_deref().map(receipt);
                if let Some((state, usage, recovery)) = &accounting {
                    assert_eq!(state, "settled");
                    assert!(usage.is_some() && recovery.is_some());
                }
                ids.push((owner, waiting, id, row.receipt, accounting));
            }
        }
        // The same owner in another authenticated queue must remain untouched.
        let mut foreign_scope = jobs_scope();
        foreign_scope.queue = "other-queue".into();
        let foreign_client = JobsClient::new(
            client.clone(),
            foreign_scope.clone(),
            AdmissionKey::new(KEY.to_vec()).unwrap(),
        );
        let mut foreign_input = queued(&fixture, "foreign-owner-copy");
        foreign_input.admission.attempt.job_queue = Some(foreign_scope.queue.clone());
        foreign_input.admission.attempt.expires_at = unix_seconds();
        foreign_input.admission = AdmissionKey::new(KEY.to_vec())
            .unwrap()
            .sign_attempt(foreign_input.admission.attempt)
            .unwrap();
        let foreign_ids = foreign_client.enqueue(vec![foreign_input]).await.unwrap();
        let Enqueued::Inserted(foreign_id) = &foreign_ids[0] else {
            panic!("foreign queue enqueue")
        };
        let foreign_jobs = runtime
            .model_jobs(foreign_scope, fixture.config.jobs.clone())
            .unwrap();

        // Leave another tenant's answer expired between independent maintenance sweeps.
        let key = AdmissionKey::new(KEY.to_vec()).unwrap();
        exchange(
            &process,
            Operation::PublishGrantRevision(
                key.sign_grant_revision(GrantRevision {
                    tenant: "foreign-tenant".into(),
                    incarnation: "incarnation".into(),
                    revision: 1,
                })
                .unwrap(),
            ),
        )
        .await
        .unwrap();
        let (admission, payload) = fixture.attempt("foreign-expired-answer", 1, 1);
        let mut attempt = admission.attempt;
        attempt.tenant = "foreign-tenant".into();
        let admission = key.sign_attempt(attempt).unwrap();
        let granted = permit(&process, &admission).await;
        let foreign_digest = granted.attempt_digest.clone();
        dispatched(
            exchange(&process, inject(admission, payload, granted))
                .await
                .unwrap(),
        );
        assert_eq!(
            conn.execute(
                "UPDATE egress_permits SET recovery_expires_at=0 WHERE attempt_digest=?1",
                [&foreign_digest],
            )
            .unwrap(),
            1
        );
        let foreign_result = || -> String {
            conn.query_row(
                "SELECT result FROM egress_permits WHERE attempt_digest=?1",
                [&foreign_digest],
                |row| row.get(0),
            )
            .expect("owner purge must retain the expired foreign answer")
        };
        let before = foreign_result();

        // Decode the new public wire command so this regression also runs on the base.
        let command = serde_json::from_value(
            serde_json::json!({"operation": "purge_owner", "body": "input-owner"}),
        )
        .expect("owner purge must be a public job command");
        let reply = job_call(&client, command).await.unwrap();
        assert_eq!(
            serde_json::to_value(reply).unwrap(),
            serde_json::json!({"reply": "purged", "body": 2})
        );
        assert_eq!(foreign_result(), before);
        let symbiotic_queue::jobs::JobResponse::Job(Some(foreign)) = foreign_jobs
            .request(symbiotic_queue::jobs::JobRequest::Get(foreign_id.clone()))
            .await
            .unwrap()
        else {
            panic!("foreign queue copy")
        };
        assert!(!foreign.purged);
        assert_eq!(foreign.owners, ["input-owner"]);
        assert!(foreign.payload.is_some() && foreign.admission.is_some());
        assert_eq!(
            JobsClient::new(
                client,
                jobs_scope(),
                AdmissionKey::new(KEY.to_vec()).unwrap()
            )
            .purge_owner("input-owner".into())
            .await
            .unwrap(),
            0
        );
        assert_eq!(foreign_result(), before);
        for (owner, waiting, id, reference, accounting) in ids {
            let symbiotic_queue::jobs::JobResponse::Job(Some(row)) = jobs
                .request(symbiotic_queue::jobs::JobRequest::Get(id))
                .await
                .unwrap()
            else {
                panic!("job")
            };
            if owner == "input-owner" {
                assert_eq!(row.state, JobState::Purged);
                assert!(row.purged && row.owners.is_empty());
                assert!(row.payload.is_none() && row.admission.is_none() && row.output.is_none());
                if let Some(reference) = reference {
                    let (state, usage, recovery) = receipt(&reference);
                    let before = accounting.unwrap();
                    assert_eq!((state, usage), (before.0, before.1));
                    assert!(recovery.is_none());
                }
            } else {
                assert!(!row.purged);
                assert_eq!(row.owners, ["other-owner"]);
                assert_eq!(
                    row.state,
                    if waiting {
                        JobState::AwaitingAdmission
                    } else {
                        JobState::Succeeded
                    }
                );
                if waiting {
                    assert!(row.payload.is_some() && row.admission.is_some());
                }
                if let Some(reference) = reference {
                    assert_eq!(Some(receipt(&reference)), accounting);
                }
            }
        }
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 3);
    })
    .await
    .expect("bounded authenticated owner purge");
}

#[tokio::test]
async fn jobs_owner_purge_refuses_unsigned_and_wrong_scope_before_mutation() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let fixture = Fixture::new(200, "answer".into(), Duration::ZERO).await;
        let process = fixture.process().await;
        let client = InProcessEgressClient::new(process);
        let mut input = queued(&fixture, "purge-refused");
        input.admission.attempt.expires_at = unix_seconds();
        input.admission = AdmissionKey::new(KEY.to_vec())
            .unwrap()
            .sign_attempt(input.admission.attempt)
            .unwrap();
        let id = enqueue_id(&client, input).await;
        wait_job(&client, &id, JobState::AwaitingAdmission).await;
        let saved = enqueue_id(&client, queued(&fixture, "purge-refused-saved")).await;
        wait_job(&client, &saved, JobState::Succeeded).await;
        let conn = rusqlite::Connection::open(
            fixture
                .config
                .state_dir
                .join(symbiotic_ai_runtime::QUEUE_DATABASE),
        )
        .unwrap();
        // A refused purge must not even perform generic expiry maintenance.
        assert_eq!(
            conn.execute(
                "UPDATE egress_permits SET recovery_expires_at=0,result='synthetic expired result'",
                []
            )
            .unwrap(),
            1
        );
        let key = AdmissionKey::new(KEY.to_vec()).unwrap();
        let command: JobsCommand = serde_json::from_value(
            serde_json::json!({"operation": "purge_owner", "body": "input-owner"}),
        )
        .expect("owner purge must be a public job command");
        for tamper in ["unsigned", "tenant", "incarnation", "queue", "owner"] {
            let mut signed = key
                .sign_jobs(JobsRequest {
                    scope: jobs_scope(),
                    command: command.clone(),
                })
                .unwrap();
            match tamper {
                "unsigned" => signed.authentication.clear(),
                "tenant" => signed.request.scope.tenant = "other".into(),
                "incarnation" => signed.request.scope.incarnation = "other".into(),
                "queue" => signed.request.scope.queue = "other".into(),
                _ => {
                    signed.request.command = serde_json::from_value(
                        serde_json::json!({"operation": "purge_owner", "body": "other-owner"}),
                    )
                    .unwrap()
                }
            }
            assert!(
                matches!(
                    client
                        .exchange(Request {
                            version: PROTOCOL_VERSION,
                            operation: signed.operation()
                        })
                        .await
                        .unwrap()
                        .result,
                    Err(EgressError::Unauthorized)
                ),
                "{tamper}"
            );
        }
        let mut foreign = jobs_scope();
        foreign.tenant = "other".into();
        let empty_queue = JobScope {
            queue: String::new(),
            ..jobs_scope()
        };
        for scope in [foreign, empty_queue] {
            let signed = key
                .sign_jobs(JobsRequest {
                    scope,
                    command: command.clone(),
                })
                .unwrap();
            assert!(matches!(
                client
                    .exchange(Request {
                        version: PROTOCOL_VERSION,
                        operation: signed.operation()
                    })
                    .await
                    .unwrap()
                    .result,
                Err(EgressError::Unauthorized)
            ));
        }
        let mismatched = key
            .sign_jobs(JobsRequest {
                scope: jobs_scope(),
                command: JobsCommand::JobStatus(id.clone()),
            })
            .unwrap();
        assert!(matches!(
            client
                .exchange(Request {
                    version: PROTOCOL_VERSION,
                    operation: Operation::PurgeOwner(Box::new(mismatched))
                })
                .await
                .unwrap()
                .result,
            Err(EgressError::InvalidRequest)
        ));
        let mut mismatched = key
            .sign_jobs(JobsRequest {
                scope: jobs_scope(),
                command,
            })
            .unwrap();
        mismatched.authentication.clear();
        assert!(matches!(
            client
                .exchange(Request {
                    version: PROTOCOL_VERSION,
                    operation: Operation::CancelJobs(Box::new(mismatched))
                })
                .await
                .unwrap()
                .result,
            Err(EgressError::InvalidRequest)
        ));
        assert_eq!(
            conn.query_row("SELECT result FROM egress_permits", [], |r| r
                .get::<_, String>(0))
                .unwrap(),
            "synthetic expired result"
        );
        let row = wait_job(&client, &id, JobState::AwaitingAdmission).await;
        assert!(!row.purged);
        assert_eq!(row.owners, ["input-owner"]);
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
    })
    .await
    .expect("bounded owner purge authentication rejection");
}

#[tokio::test]
async fn jobs_seven_operations_round_trip_on_both_transports_and_join_by_key() {
    for socket_mode in [false, true] {
        let fixture = Fixture::new(200, "job answer".into(), Duration::ZERO).await;
        let process = fixture.process().await;
        let server_task;
        let client: Box<dyn EgressClient> = if socket_mode {
            let listener = server::bind(&process).unwrap();
            server_task = Some(tokio::spawn(server::serve(process.clone(), listener)));
            Box::new(socket::UnixEgressClient {
                path: fixture.config.socket_path.clone(),
                max_frame_bytes: fixture.config.max_frame_bytes,
                timeout: Duration::from_secs(3),
            })
        } else {
            server_task = None;
            Box::new(InProcessEgressClient::new(process.clone()))
        };
        // Trait objects are wrapped by a concrete forwarding adapter for shared helpers.
        struct Client(Box<dyn EgressClient>);
        #[async_trait::async_trait]
        impl EgressClient for Client {
            async fn exchange(&self, request: Request) -> Result<Response, EgressError> {
                self.0.exchange(request).await
            }
        }
        let client = Client(client);
        let mut input = queued(&fixture, "six");
        if let ProviderPayload::Chat(request) = &mut input.payload {
            request.source = Some("private-source".into());
            request.role_binding = Some("private-role".into());
            request.metadata = serde_json::json!({"private": "private-payload-metadata"});
        }
        input.admission.attempt.input_digest = input.payload.digest().unwrap();
        input.admission = AdmissionKey::new(KEY.to_vec())
            .unwrap()
            .sign_attempt(input.admission.attempt)
            .unwrap();
        let id = enqueue_id(&client, input.clone()).await;
        assert_eq!(enqueue_id(&client, input.clone()).await, id);
        let mut conflicting = input;
        conflicting.admission.attempt.manifest_ref = "changed-manifest".into();
        conflicting.admission = AdmissionKey::new(KEY.to_vec())
            .unwrap()
            .sign_attempt(conflicting.admission.attempt)
            .unwrap();
        assert!(matches!(
            job_call(&client, JobsCommand::EnqueueJobs(vec![conflicting])).await,
            Err(JobsClientError::Job(JobError::KeyConflict))
        ));
        wait_job(&client, &id, JobState::Succeeded).await;
        assert!(matches!(
            job_call(
                &client,
                JobsCommand::Completions {
                    after: None,
                    limit: 1,
                    max_bytes: 40,
                    wait_seconds: 0
                }
            )
            .await,
            Err(JobsClientError::Job(JobError::CompletionTooLarge { .. }))
        ));
        assert_eq!(
            wait_job(&client, &id, JobState::Succeeded)
                .await
                .delivery_generation,
            0
        );
        let JobsReply::Completions(page) = job_call(
            &client,
            JobsCommand::Completions {
                after: None,
                limit: 8,
                max_bytes: 65536,
                wait_seconds: 0,
            },
        )
        .await
        .unwrap() else {
            panic!("completions")
        };
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].output.as_ref().unwrap()["text"], "job answer");
        let encoded = serde_json::to_string(&page.items[0].output).unwrap();
        for private in ["private-source", "private-role", "private-payload-metadata"] {
            assert!(!encoded.contains(private));
        }
        let token = page.items[0].delivery.token.clone();
        assert!(
            matches!(job_call(&client, JobsCommand::AckJobs(vec![(token.clone(), Disposition::Accepted)])).await.unwrap(), JobsReply::Acked(ref results) if results == &[AckResult::Acked(Disposition::Accepted)])
        );
        assert!(
            matches!(job_call(&client, JobsCommand::AckJobs(vec![(token, Disposition::Discarded)])).await.unwrap(), JobsReply::Acked(ref results) if results == &[AckResult::AlreadyAcked(Disposition::Accepted)])
        );
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
        // Expired authority is stored and noticed, never silently dropped.
        let mut waiting = queued(&fixture, "cancel-awaiting");
        waiting.admission.attempt.expires_at = unix_seconds();
        waiting.admission = AdmissionKey::new(KEY.to_vec())
            .unwrap()
            .sign_attempt(waiting.admission.attempt)
            .unwrap();
        let mut successor = waiting.admission.attempt.clone();
        successor.attempt_ordinal = 2;
        successor.record_sequence = 2;
        let successor = AdmissionKey::new(KEY.to_vec())
            .unwrap()
            .sign_attempt(successor)
            .unwrap();
        let waiting = enqueue_id(&client, waiting).await;
        wait_job(&client, &waiting, JobState::AwaitingAdmission).await;
        assert!(matches!(
            job_call(
                &client,
                JobsCommand::AdmitJob {
                    job: waiting.clone(),
                    admission: Box::new(successor)
                }
            )
            .await
            .unwrap(),
            JobsReply::Admitted
        ));
        wait_job(&client, &waiting, JobState::AwaitingAdmission).await;
        assert!(matches!(
            job_call(
                &client,
                JobsCommand::CancelJobs(Selector::Group("task".into()))
            )
            .await
            .unwrap(),
            JobsReply::Cancelled(1)
        ));
        wait_job(&client, &waiting, JobState::Cancelled).await;
        // The confirmed job already has no owner metadata or copies.
        assert!(matches!(
            job_call(&client, JobsCommand::PurgeOwner("input-owner".into()))
                .await
                .unwrap(),
            JobsReply::Purged(1)
        ));
        wait_job(&client, &waiting, JobState::Purged).await;
        assert!(matches!(
            job_call(&client, JobsCommand::PurgeOwner("input-owner".into()))
                .await
                .unwrap(),
            JobsReply::Purged(0)
        ));
        if let Some(task) = server_task {
            task.abort();
        }
    }
}

#[tokio::test]
async fn jobs_notice_polls_without_cursor_preserve_wire_and_byte_budget() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let fixture = Fixture::new(200, "answer".into(), Duration::ZERO).await;
        let process = fixture.process().await;
        let client = InProcessEgressClient::new(process);
        let mut input = queued(&fixture, "notice-without-cursor");
        input.admission.attempt.expires_at = unix_seconds();
        input.admission = AdmissionKey::new(KEY.to_vec())
            .unwrap()
            .sign_attempt(input.admission.attempt)
            .unwrap();
        let id = enqueue_id(&client, input).await;
        wait_job(&client, &id, JobState::AwaitingAdmission).await;
        let poll = |max_bytes| {
            job_call(
                &client,
                serde_json::from_value(serde_json::json!({
                    "operation": "completions",
                    "body": { "limit": 1, "max_bytes": max_bytes, "wait_seconds": 0 }
                }))
                .unwrap(),
            )
        };
        let JobsReply::Completions(page) = poll(65536).await.unwrap() else {
            panic!("notice page")
        };
        assert_eq!(page.notices.len(), 1);
        assert_eq!(page.notices[0].id, id);
        let old_body = serde_json::json!({ "items": page.items, "notices": page.notices });
        let max_bytes = serde_json::to_vec(&old_body).unwrap().len();
        let JobsReply::Completions(exact) = poll(max_bytes).await.unwrap() else {
            panic!("exact notice page")
        };
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct PreviousCompletions {
            items: Vec<JobDelivery>,
            notices: Vec<symbiotic_queue::jobs::JobDiagnostic>,
        }
        let bytes = serde_json::to_vec(&exact).unwrap();
        assert_eq!(bytes.len(), max_bytes);
        let previous: PreviousCompletions = serde_json::from_slice(&bytes).unwrap();
        assert!(previous.items.is_empty());
        assert_eq!(previous.notices[0].id, id);
        assert!(exact.after.is_none());
        assert!(matches!(
            poll(max_bytes - 1).await,
            Err(JobsClientError::Job(JobError::CompletionTooLarge { job, bytes }))
                if job == id && bytes == max_bytes
        ));
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
    })
    .await
    .expect("bounded no-cursor completion compatibility");
}

#[tokio::test]
async fn jobs_notice_cursor_reaches_unresolved_notices_within_page_bounds() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let fixture = Fixture::new(200, "answer".into(), Duration::ZERO).await;
        let process = fixture.process().await;
        let client = InProcessEgressClient::new(process);
        let mut expected = Vec::new();
        for i in 0..5 {
            let mut input = queued(&fixture, &format!("notice-{i}"));
            input.admission.attempt.expires_at = unix_seconds();
            input.admission = AdmissionKey::new(KEY.to_vec())
                .unwrap()
                .sign_attempt(input.admission.attempt)
                .unwrap();
            let id = enqueue_id(&client, input).await;
            wait_job(&client, &id, JobState::AwaitingAdmission).await;
            expected.push(id);
        }
        expected.sort_by(|a, b| a.id.cmp(&b.id));
        // Exercise the optional cursor through the public wire encoding.
        let poll = |after: Option<String>, limit, max_bytes| {
            let client = &client;
            async move {
                let command = serde_json::from_value(serde_json::json!({
                    "operation": "completions",
                    "body": { "after": after, "limit": limit,
                        "max_bytes": max_bytes, "wait_seconds": 0 }
                }))
                .unwrap();
                let JobsReply::Completions(page) = job_call(client, command).await.unwrap() else {
                    panic!("notice page")
                };
                let wire = serde_json::to_value(&page).unwrap();
                let next = wire
                    .get("after")
                    .and_then(|v| v.as_str())
                    .map(str::to_owned);
                (page, next)
            }
        };
        let (first, next) = poll(Some(String::new()), 2, 65536).await;
        assert_eq!(
            first
                .notices
                .iter()
                .map(|n| n.id.clone())
                .collect::<Vec<_>>(),
            expected[..2]
        );
        assert_eq!(next.as_deref(), Some(expected[1].id.as_str()));
        let (repeated, repeated_next) = poll(None, 2, 65536).await;
        assert_eq!(repeated.notices[0].id, expected[0]);
        assert!(repeated_next.is_none());
        let mut seen = first
            .notices
            .iter()
            .map(|n| n.id.clone())
            .collect::<Vec<_>>();
        let mut after = next;
        for count in [2, 1, 0] {
            let (page, next) = poll(after.clone(), 2, 65536).await;
            assert!(page.items.is_empty());
            assert_eq!(page.notices.len(), count);
            seen.extend(page.notices.iter().map(|n| n.id.clone()));
            if let Some(last) = page.notices.last() {
                assert_eq!(next.as_deref(), Some(last.id.id.as_str()));
            } else {
                assert_eq!(next, after);
            }
            after = next;
        }
        assert_eq!(seen, expected);
        // Use an exact one-notice byte budget while the count allows two.
        let (one, _) = poll(Some(String::new()), 1, 65536).await;
        let max_bytes = serde_json::to_vec(&one).unwrap().len();
        assert!(matches!(
            job_call(&client, JobsCommand::Completions {
                after: Some(String::new()), limit: 2, max_bytes: max_bytes - 1, wait_seconds: 0,
            }).await,
            Err(JobsClientError::Job(JobError::CompletionTooLarge { job, bytes }))
                if job == expected[0] && bytes == max_bytes
        ));
        let mut after = Some(String::new());
        let mut seen = Vec::new();
        for id in &expected {
            let (page, next) = poll(after, 2, max_bytes).await;
            assert!(page.items.is_empty());
            assert_eq!(page.notices.len(), 1);
            assert_eq!(&page.notices[0].id, id);
            assert_eq!(serde_json::to_vec(&page).unwrap().len(), max_bytes);
            seen.push(page.notices[0].id.clone());
            after = next;
        }
        assert_eq!(seen, expected);
        let final_id = enqueue_id(&client, queued(&fixture, "notice-final")).await;
        wait_job(&client, &final_id, JobState::Succeeded).await;
        let after = Some(expected[0].id.clone());
        let (final_page, next) = poll(after.clone(), 1, 65536).await;
        assert_eq!(final_page.items[0].delivery.completion.id, final_id);
        assert!(final_page.notices.is_empty());
        assert_eq!(next, after);
        let (page, _) = poll(next, 2, 65536).await;
        assert_eq!(
            page.notices
                .iter()
                .map(|n| n.id.clone())
                .collect::<Vec<_>>(),
            expected[1..3]
        );
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
    })
    .await
    .expect("bounded admission-notice pagination");
}

#[tokio::test]
async fn jobs_recheck_grants_after_account_wait_and_readmit_without_renewing_ceiling() {
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let mut fixture = Fixture::with_response_gate(
        200,
        "answer".into(),
        Duration::ZERO,
        "null",
        false,
        false,
        Some(gate.clone()),
    )
    .await;
    fixture.config.routes[0].max_in_flight = 1;
    fixture.config.routes[0].max_attempts = 1;
    fixture.config.routes[0].timeout_seconds = 5;
    let process = fixture.process().await;
    let client = InProcessEgressClient::new(process.clone());
    let first = enqueue_id(&client, queued(&fixture, "first")).await;
    tokio::time::timeout(Duration::from_secs(3), async {
        while fixture.calls.load(Ordering::SeqCst) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let waiting = queued(&fixture, "waiting");
    let second = enqueue_id(&client, waiting.clone()).await;
    revoke(&process, 2).await;
    gate.add_permits(1);
    wait_job(&client, &first, JobState::Succeeded).await;
    let row = wait_job(&client, &second, JobState::AwaitingAdmission).await;
    assert_eq!(row.generation, 0);
    assert!(row.receipt.is_none());
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
    // A final and a notice coexist: finals are delivered first and notices cannot be acked.
    let JobsReply::Completions(page) = job_call(
        &client,
        JobsCommand::Completions {
            after: None,
            limit: 1,
            max_bytes: 65536,
            wait_seconds: 0,
        },
    )
    .await
    .unwrap() else {
        panic!("page")
    };
    assert_eq!(page.items.len(), 1);
    assert!(page.notices.is_empty());
    let JobsReply::Completions(page) = job_call(
        &client,
        JobsCommand::Completions {
            after: None,
            limit: 1,
            max_bytes: 65536,
            wait_seconds: 0,
        },
    )
    .await
    .unwrap() else {
        panic!("notice")
    };
    assert_eq!(page.notices[0].id, second);
    assert!(matches!(
        job_call(
            &client,
            JobsCommand::AckJobs(vec![(
                DeliveryToken {
                    job: second.clone(),
                    generation: 1
                },
                Disposition::Accepted
            )])
        )
        .await,
        Err(JobsClientError::Job(JobError::NotFinal))
    ));
    let mut successor = waiting.admission.attempt;
    successor.grant_revision = 2;
    successor.attempt_ordinal = 2;
    successor.record_sequence = 2;
    successor.recorded_at = unix_seconds();
    successor.expires_at = successor.recorded_at + 3600;
    let admission = AdmissionKey::new(KEY.to_vec())
        .unwrap()
        .sign_attempt(successor)
        .unwrap();
    assert!(matches!(
        job_call(
            &client,
            JobsCommand::AdmitJob {
                job: second.clone(),
                admission: Box::new(admission)
            }
        )
        .await
        .unwrap(),
        JobsReply::Admitted
    ));
    gate.add_permits(1);
    let row = wait_job(&client, &second, JobState::Succeeded).await;
    assert_eq!(row.generation, 1);
    assert_eq!(row.max_attempts, 1);
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn jobs_expired_recovery_keeps_waiting_input_and_settles_without_output() {
    let fixture = Fixture::new(200, "answer".into(), Duration::ZERO).await;
    let process = fixture.process().await;
    let client = InProcessEgressClient::new(process);
    let mut input = queued(&fixture, "expired-recovery");
    input.admission.attempt.recovery_expires_at = unix_seconds() - 1;
    input.admission.attempt.expires_at = unix_seconds();
    input.admission = AdmissionKey::new(KEY.to_vec())
        .unwrap()
        .sign_attempt(input.admission.attempt)
        .unwrap();
    let id = enqueue_id(&client, input.clone()).await;
    wait_job(&client, &id, JobState::AwaitingAdmission).await;
    let mut successor = input.admission.attempt;
    successor.attempt_ordinal = 2;
    successor.record_sequence = 2;
    // Renew authority after recovery expiry; recovery never controls execution.
    successor.recorded_at = unix_seconds();
    successor.expires_at = unix_seconds() + 3600;
    let admission = AdmissionKey::new(KEY.to_vec())
        .unwrap()
        .sign_attempt(successor)
        .unwrap();
    job_call(
        &client,
        JobsCommand::AdmitJob {
            job: id.clone(),
            admission: Box::new(admission),
        },
    )
    .await
    .unwrap();
    let row = wait_job(&client, &id, JobState::Succeeded).await;
    assert!(row.result_expired && row.receipt.is_some());
    let JobsReply::Completions(page) = job_call(
        &client,
        JobsCommand::Completions {
            after: None,
            limit: 1,
            max_bytes: 65536,
            wait_seconds: 0,
        },
    )
    .await
    .unwrap() else {
        panic!("completion")
    };
    assert!(page.items[0].output.is_none());
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn jobs_scope_and_mac_are_checked_before_any_control_operation() {
    let fixture = Fixture::new(200, "answer".into(), Duration::ZERO).await;
    let process = fixture.process().await;
    let client = InProcessEgressClient::new(process);
    let mut input = queued(&fixture, "foreign");
    input.admission.attempt.expires_at = unix_seconds();
    input.admission = AdmissionKey::new(KEY.to_vec())
        .unwrap()
        .sign_attempt(input.admission.attempt)
        .unwrap();
    let id = enqueue_id(&client, input.clone()).await;
    wait_job(&client, &id, JobState::AwaitingAdmission).await;
    for field in ["tenant", "incarnation", "queue"] {
        let mut foreign = id.clone();
        match field {
            "tenant" => foreign.scope.tenant = "other".into(),
            "incarnation" => foreign.scope.incarnation = "other".into(),
            _ => foreign.scope.queue = "other".into(),
        }
        for command in [
            JobsCommand::JobStatus(foreign.clone()),
            JobsCommand::CancelJobs(Selector::Ids(vec![foreign.clone()])),
            JobsCommand::AckJobs(vec![(
                DeliveryToken {
                    job: foreign.clone(),
                    generation: 1,
                },
                Disposition::Accepted,
            )]),
            JobsCommand::AdmitJob {
                job: foreign,
                admission: Box::new(input.admission.clone()),
            },
        ] {
            assert!(matches!(
                job_call(&client, command).await,
                Err(JobsClientError::Egress(EgressError::Unauthorized))
            ));
        }
    }
    let mut signed = AdmissionKey::new(KEY.to_vec())
        .unwrap()
        .sign_jobs(JobsRequest {
            scope: jobs_scope(),
            command: JobsCommand::JobStatus(id.clone()),
        })
        .unwrap();
    signed.request.scope.incarnation = "tampered".into();
    assert!(matches!(
        client
            .exchange(Request {
                version: PROTOCOL_VERSION,
                operation: signed.operation()
            })
            .await
            .unwrap()
            .result,
        Err(EgressError::Unauthorized)
    ));
    wait_job(&client, &id, JobState::AwaitingAdmission).await;
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn jobs_kill_after_dispatch_recovers_uncertain_without_resending() {
    let mut fixture = Fixture::new(200, "late answer".into(), Duration::from_secs(10)).await;
    fixture.config.jobs.claim_lease_seconds = 1;
    fixture.config.routes[0].timeout_seconds = 30;
    let config = fixture.dir.path().join("jobs-config.json");
    std::fs::write(&config, serde_json::to_vec(&fixture.config).unwrap()).unwrap();
    std::fs::set_permissions(&config, std::fs::Permissions::from_mode(0o600)).unwrap();
    let mut command = std::process::Command::new(credential_process());
    command
        .arg(&config)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    for name in [
        "HTTP_PROXY",
        "http_proxy",
        "HTTPS_PROXY",
        "https_proxy",
        "ALL_PROXY",
        "all_proxy",
    ] {
        command.env_remove(name);
    }
    let mut child = Child(command.spawn().unwrap());
    let client = socket::UnixEgressClient {
        path: fixture.config.socket_path.clone(),
        max_frame_bytes: fixture.config.max_frame_bytes,
        timeout: Duration::from_secs(3),
    };
    async fn ready(child: &mut Child, client: &socket::UnixEgressClient) {
        assert!(matches!(
            wait_for_child_startup(child, || exchange_client(client, publish_revision(1))).await,
            Reply::GrantRevisionPublished
        ));
    }
    ready(&mut child, &client).await;
    let input = queued(&fixture, "crashed");
    let id = enqueue_id(&client, input.clone()).await;
    tokio::time::timeout(OBSERVATION_HANG_GUARD, async {
        while fixture.calls.load(Ordering::SeqCst) != 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    child.0.kill().unwrap();
    child.0.wait().unwrap();
    drop(child);
    // Lease expiry triggers ledger-first recovery; the paid answer never committed.
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let mut child = Child(command.spawn().unwrap());
    ready(&mut child, &client).await;
    assert_eq!(enqueue_id(&client, input).await, id);
    let row = wait_job(&client, &id, JobState::Uncertain).await;
    assert_eq!(row.generation, 1);
    assert!(row.receipt.is_some());
    // A successor cannot turn uncertain accounting into another paid dispatch.
    let (admission, _) = fixture.job_attempt("crashed", 2, 2);
    assert!(matches!(
        job_call(
            &client,
            JobsCommand::AdmitJob {
                job: id.clone(),
                admission: Box::new(admission)
            }
        )
        .await,
        Err(JobsClientError::Job(JobError::InvalidRequest))
    ));
    wait_job(&client, &id, JobState::Uncertain).await;
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn jobs_cancel_sent_call_keeps_answer_and_ack_discards_only_recovery() {
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let fixture = Fixture::with_response_gate(
        200,
        "sent answer".into(),
        Duration::ZERO,
        "null",
        false,
        false,
        Some(gate.clone()),
    )
    .await;
    let process = fixture.process().await;
    let client = InProcessEgressClient::new(process);
    let id = enqueue_id(&client, queued(&fixture, "sent-cancel")).await;
    tokio::time::timeout(Duration::from_secs(3), async {
        while fixture.calls.load(Ordering::SeqCst) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    job_call(
        &client,
        JobsCommand::CancelJobs(Selector::Ids(vec![id.clone()])),
    )
    .await
    .unwrap();
    gate.add_permits(1);
    let row = wait_job(&client, &id, JobState::Cancelled).await;
    let reference = row.receipt.unwrap();
    let JobsReply::Completions(page) = job_call(
        &client,
        JobsCommand::Completions {
            after: None,
            limit: 1,
            max_bytes: 65536,
            wait_seconds: 0,
        },
    )
    .await
    .unwrap() else {
        panic!("completion")
    };
    assert_eq!(
        page.items[0].output.as_ref().unwrap()["text"],
        "sent answer"
    );
    job_call(
        &client,
        JobsCommand::AckJobs(vec![(
            page.items[0].delivery.token.clone(),
            Disposition::Discarded,
        )]),
    )
    .await
    .unwrap();
    let conn = rusqlite::Connection::open(
        fixture
            .config
            .state_dir
            .join(symbiotic_ai_runtime::QUEUE_DATABASE),
    )
    .unwrap();
    let (state, usage, recovery): (String, Option<String>, Option<String>) = conn
        .query_row(
            "SELECT state,usage,recovery FROM spend_receipts WHERE reference=?1",
            [reference],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!(state, "settled");
    assert!(usage.is_some() && recovery.is_none());
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn jobs_owner_erasure_during_transport_preserves_settlement_and_retires_binding() {
    tokio::time::timeout(Duration::from_secs(5), async {
        for measured in [true, false] {
            let gate = Arc::new(tokio::sync::Semaphore::new(0));
            let output = if measured {
                r#"{"choices":[{"message":{"content":"erased answer"},"finish_reason":"stop"}],"usage":{"prompt_tokens":7,"completion_tokens":3,"cost":"0.1"}}"#
            } else {
                r#"{"choices":[{"message":{"content":"erased answer"},"finish_reason":"stop"}]}"#
            };
            let fixture = Fixture::with_response_gate(
                200, output.into(), Duration::ZERO, "null", true, false, Some(gate.clone()),
            ).await;
            let process = fixture.process().await;
            let client = InProcessEgressClient::new(process.clone());
            let input = queued(&fixture, "erased-during-send");
            let attempt_digest = digest(&input.admission.attempt).unwrap();
            let id = enqueue_id(&client, input).await;
            while fixture.calls.load(Ordering::SeqCst) != 1 {
                tokio::task::yield_now().await;
            }

            let runtime = symbiotic_ai_runtime::Runtime::open(symbiotic_ai_runtime::RuntimeConfig {
                state_dir: Some(fixture.config.state_dir.clone()),
                ..Default::default()
            }).unwrap();
            let jobs = runtime.model_jobs(jobs_scope(), fixture.config.jobs.clone()).unwrap();
            assert!(matches!(
                job_call(&client, JobsCommand::PurgeOwner("input-owner".into())).await.unwrap(),
                JobsReply::Purged(1)
            ));
            let symbiotic_queue::jobs::JobResponse::Job(Some(erased)) =
                jobs.request(symbiotic_queue::jobs::JobRequest::Get(id.clone())).await.unwrap()
            else { panic!("erased running job") };
            assert_eq!(erased.state, JobState::Running);
            assert!(erased.purged && erased.owners.is_empty());
            assert!(erased.admission.is_none() && erased.payload.is_none() && erased.output.is_none());
            let reference = erased.receipt.unwrap();
            let conn = rusqlite::Connection::open(fixture.config.state_dir.join(symbiotic_ai_runtime::QUEUE_DATABASE)).unwrap();
            let binding: Option<String> = conn.query_row(
                "SELECT request_key FROM egress_permits WHERE attempt_digest=?1",
                [&attempt_digest], |row| row.get(0),
            ).unwrap();
            assert!(binding.is_some());

            gate.add_permits(1);
            let row = wait_job(&client, &id, JobState::Purged).await;
            assert_eq!(row.receipt.as_deref(), Some(reference.as_str()));
            assert!(row.purged && row.owners.is_empty(), "{row:?}");
            assert_eq!(row.diagnostic, Some(symbiotic_ai_runtime::model::DiagnosticCode::InvocationCompleted));
            let (state, usage, recovery): (String, Option<String>, Option<String>) = conn.query_row(
                "SELECT state,usage,recovery FROM spend_receipts WHERE reference=?1",
                [&reference], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            ).unwrap();
            assert_eq!(state, if measured { "settled" } else { "unknown" });
            if measured {
                let usage: symbiotic_trace::UsageTrace = serde_json::from_str(&usage.unwrap()).unwrap();
                assert_eq!(usage.input_tokens, Some(7));
                assert_eq!(usage.output_tokens, Some(3));
            } else {
                assert!(usage.is_none());
            }
            assert!(recovery.is_none());
            let binding: Option<String> = conn.query_row(
                "SELECT request_key FROM egress_permits WHERE attempt_digest=?1",
                [&attempt_digest], |row| row.get(0),
            ).unwrap();
            assert!(binding.is_none());
            assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
        }
    }).await.expect("bounded owner erasure during signed-job transport");
}

#[tokio::test]
async fn jobs_only_local_credential_failures_are_known_zero_charge() {
    for missing_secret in [false, true] {
        let fixture = Fixture::new(401, "refused".into(), Duration::ZERO).await;
        if missing_secret {
            std::fs::remove_file(fixture.dir.path().join("provider")).unwrap();
        }
        let process = fixture.process().await;
        let client = InProcessEgressClient::new(process);
        let id = enqueue_id(&client, queued(&fixture, "credential-failure")).await;
        let state = if missing_secret {
            JobState::Failed
        } else {
            JobState::Uncertain
        };
        let row = wait_job(&client, &id, state).await;
        let conn = rusqlite::Connection::open(
            fixture
                .config
                .state_dir
                .join(symbiotic_ai_runtime::QUEUE_DATABASE),
        )
        .unwrap();
        let accounting: String = conn
            .query_row(
                "SELECT state FROM spend_receipts WHERE reference=?1",
                [row.receipt.unwrap()],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            accounting,
            if missing_secret {
                "released"
            } else {
                "unknown"
            }
        );
        assert_eq!(
            fixture.calls.load(Ordering::SeqCst),
            usize::from(!missing_secret)
        );
        if !missing_secret {
            let (admission, _) = fixture.job_attempt("credential-failure", 2, 2);
            assert!(matches!(
                job_call(
                    &client,
                    JobsCommand::AdmitJob {
                        job: id,
                        admission: Box::new(admission)
                    }
                )
                .await,
                Err(JobsClientError::Job(JobError::InvalidRequest))
            ));
            assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
        }
    }
}

async fn reopen_jobs(fixture: &Fixture) -> CredentialProcess {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match CredentialProcess::open(fixture.config.clone()) {
                Ok(process) => return process,
                Err(EgressError::StateUnavailable) => tokio::task::yield_now().await,
                Err(error) => panic!("reopen failed: {error:?}"),
            }
        }
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn jobs_stored_operations_survive_execution_config_changes_and_misses_do_not_attach() {
    let mut fixture = Fixture::new(200, "saved answer".into(), Duration::ZERO).await;
    let client = InProcessEgressClient::new(fixture.process().await);
    let id = enqueue_id(&client, queued(&fixture, "saved")).await;
    wait_job(&client, &id, JobState::Succeeded).await;
    let mut waiting = queued(&fixture, "waiting");
    waiting.admission.attempt.expires_at = unix_seconds();
    waiting.admission = AdmissionKey::new(KEY.to_vec())
        .unwrap()
        .sign_attempt(waiting.admission.attempt)
        .unwrap();
    let waiting = enqueue_id(&client, waiting).await;
    wait_job(&client, &waiting, JobState::AwaitingAdmission).await;
    drop(client);
    fixture.config.routes[0].timeout_seconds += 1;
    fixture.config.routes[0].max_in_flight += 1;
    let client = InProcessEgressClient::new(reopen_jobs(&fixture).await);
    wait_job(&client, &id, JobState::Succeeded).await;
    // Renewal stores unfinished work, but the changed execution binding refuses
    // attachment. That refusal must not gate metadata or saved completions.
    let (admission, _) = fixture.job_attempt("waiting", 2, 2);
    assert!(matches!(
        job_call(
            &client,
            JobsCommand::AdmitJob {
                job: waiting.clone(),
                admission: Box::new(admission)
            }
        )
        .await,
        Err(JobsClientError::Egress(EgressError::StateUnavailable))
    ));
    wait_job(&client, &waiting, JobState::Pending).await;

    let JobsReply::Completions(page) = job_call(
        &client,
        JobsCommand::Completions {
            after: None,
            limit: 1,
            max_bytes: 65536,
            wait_seconds: 0,
        },
    )
    .await
    .unwrap() else {
        panic!("completions")
    };
    assert_eq!(
        page.items[0].output.as_ref().unwrap()["text"],
        "saved answer"
    );
    job_call(
        &client,
        JobsCommand::AckJobs(vec![(
            page.items[0].delivery.token.clone(),
            Disposition::Accepted,
        )]),
    )
    .await
    .unwrap();
    job_call(
        &client,
        JobsCommand::CancelJobs(Selector::Ids(vec![waiting.clone()])),
    )
    .await
    .unwrap();
    wait_job(&client, &waiting, JobState::Cancelled).await;
    for n in 0..4 {
        let mut scope = jobs_scope();
        scope.queue = format!("missing-{n}");
        let jobs = JobsClient::new(
            client.clone(),
            scope.clone(),
            AdmissionKey::new(KEY.to_vec()).unwrap(),
        );
        let missing = JobId {
            scope,
            id: "missing".into(),
        };
        assert!(matches!(
            jobs.request(JobsCommand::JobStatus(missing.clone())).await,
            Err(JobsClientError::Job(JobError::NotFound))
        ));
        let (mut admission, _) = fixture.attempt("missing", 2, 2);
        admission.attempt.job_queue = Some(missing.scope.queue.clone());
        admission = AdmissionKey::new(KEY.to_vec())
            .unwrap()
            .sign_attempt(admission.attempt)
            .unwrap();
        assert!(matches!(
            jobs.request(JobsCommand::AdmitJob {
                job: missing,
                admission: Box::new(admission)
            })
            .await,
            Err(JobsClientError::Job(JobError::NotFound))
        ));
        jobs.request(JobsCommand::Completions {
            after: None,
            limit: 1,
            max_bytes: 65536,
            wait_seconds: 0,
        })
        .await
        .unwrap();
    }
    let conn = rusqlite::Connection::open(
        fixture
            .config
            .state_dir
            .join(symbiotic_ai_runtime::QUEUE_DATABASE),
    )
    .unwrap();
    assert_eq!(
        conn.query_row("SELECT count(*) FROM model_job_bindings", [], |r| r
            .get::<_, usize>(0))
            .unwrap(),
        1
    );
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn regression_jobs_share_unresolved_request_admission_and_completion() {
    tokio::time::timeout(Duration::from_secs(10), async {
        for original_job in [false, true] {
            let mut fixture = Fixture::with_http_response(200,
                r#"{"choices":[{"message":{"content":"paid once"},"finish_reason":"stop"}]}"#.into(),
                Duration::ZERO, "null", true, false).await;
            configure_request_budget(&mut fixture, 3, None);
            let process = fixture.process().await;
            let mut conn = rusqlite::Connection::open(fixture.config.state_dir.join(symbiotic_ai_runtime::QUEUE_DATABASE)).unwrap();
            let (original, reference) = if original_job {
                conn.execute_batch("CREATE TRIGGER reject_completion BEFORE UPDATE OF state ON spend_receipts BEGIN SELECT RAISE(ABORT, 'synthetic completion failure'); END;").unwrap();
                let input = queued(&fixture, "unfinished-job");
                let client = InProcessEgressClient::new(process.clone());
                let id = enqueue_id(&client, input.clone()).await;
                loop {
                    if matches!(job_call(&client, JobsCommand::JobStatus(id.clone())).await,
                        Err(JobsClientError::Egress(EgressError::StateUnavailable))) { break; }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                conn.execute_batch("DROP TRIGGER reject_completion").unwrap();
                let AttemptStatus::Dispatched { receipt } = status(&process, &input.admission).await else { panic!("unfinished job receipt") };
                (input.admission, receipt.reference)
            } else {
                conn.execute_batch("CREATE TRIGGER reject_completion BEFORE UPDATE OF finished ON egress_permits BEGIN SELECT RAISE(ABORT, 'synthetic completion failure'); END;").unwrap();
                let (signed, _) = fixture.attempt("unfinished-direct", 1, 1);
                let result = request_budget_call(&fixture, &process, "unfinished-direct", false, false).await;
                assert!(!result.receipt_persisted);
                conn.execute_batch("DROP TRIGGER reject_completion").unwrap();
                (signed, result.receipt.reference)
            };
            drop(process);
            let process = reopen_jobs(&fixture).await;
            let client = InProcessEgressClient::new(process.clone());
            assert_reconciliation_required(&request_budget_call(
                &fixture, &process, "retry-direct", false, false).await);
            let id = enqueue_id(&client, queued(&fixture, "retry-job")).await;
            let row = loop {
                let JobsReply::Status(row) = job_call(&client, JobsCommand::JobStatus(id.clone())).await.unwrap() else { panic!("status") };
                if !row.state.unfinished() { break row }
                tokio::time::sleep(Duration::from_millis(10)).await;
            };
            assert_eq!(row.state, JobState::Failed);
            let refused_state: String = conn.query_row("SELECT state FROM spend_receipts WHERE reference=?1", [row.receipt.as_deref().unwrap()], |r| r.get(0)).unwrap();
            assert_eq!(refused_state, "released");
            assert_eq!(row.diagnostic, Some(symbiotic_ai_runtime::model::DiagnosticCode::SpendReconciliationRequired));
            assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
            let AttemptStatus::Dispatched { receipt } = status(&process, &original).await else { panic!("original recovery") };
            assert_eq!(receipt.reference, reference);
            assert_eq!(receipt.spend_state, SpendState::Unknown);
            let mut different = queued(&fixture, "different-job");
            let ProviderPayload::Chat(request) = &mut different.payload else { panic!("chat") };
            request.messages[0].content = "different input".into();
            different.admission.attempt.input_digest = different.payload.digest().unwrap();
            different.admission = AdmissionKey::new(KEY.to_vec()).unwrap().sign_attempt(different.admission.attempt).unwrap();
            let different_id = enqueue_id(&client, different).await;
            wait_job(&client, &different_id, JobState::Succeeded).await;
            // Reconciliation writes after reading the receipt; take the writer first,
            // as production does, so a concurrent runner cannot invalidate the snapshot.
            let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate).unwrap();
            symbiotic_ai_runtime::spend::SqliteSpendLedger::finish_in(&tx, &reference, SpendState::Settled, Some(symbiotic_trace::UsageTrace { input_tokens: Some(7), ..Default::default() }), None).unwrap();
            tx.commit().unwrap();
            let id = enqueue_id(&client, queued(&fixture, "resolved-job")).await;
            wait_job(&client, &id, JobState::Succeeded).await;
            // Completion with missing usage retires the binding while keeping
            // unknown spend; signed jobs retain their separate per-job allowance.
            let id = enqueue_id(&client, queued(&fixture, "completed-job")).await;
            wait_job(&client, &id, JobState::Succeeded).await;
            let completed = request_budget_call(&fixture, &process, "after-completed-job", false, false).await;
            assert!(completed.error.is_none() && completed.output.is_some());
            assert_eq!(fixture.calls.load(Ordering::SeqCst), 5);
        }
    }).await.expect("bounded signed-job request admission regression");
}

#[tokio::test]
async fn jobs_refuse_signed_attempt_already_dispatched_directly_after_restart() {
    let fixture = Fixture::new(200, "paid once".into(), Duration::ZERO).await;
    let process = fixture.process().await;
    let input = queued(&fixture, "shared-attempt");
    let permit = permit(&process, &input.admission).await;
    assert!(
        dispatched(
            exchange(
                &process,
                inject(input.admission.clone(), input.payload.clone(), permit)
            )
            .await
            .unwrap()
        )
        .output
        .is_some()
    );
    drop(process);
    let client = InProcessEgressClient::new(reopen_jobs(&fixture).await);
    let id = enqueue_id(&client, input).await;
    let row = wait_job(&client, &id, JobState::Refused).await;
    assert_eq!(
        row.diagnostic,
        Some(symbiotic_ai_runtime::model::DiagnosticCode::InvocationCompleted)
    );
    assert!(row.receipt.is_none());
    assert_eq!(row.generation, 0);
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn jobs_keyless_completions_discard_raw_json_for_every_response_type() {
    use symbiotic_ai_runtime::model::{ModelAdapter, RerankRequest};
    for kind in ["chat", "embedding", "rerank"] {
        let body = match kind {
            "chat" => serde_json::json!({"choices":[{"message":{"content":"selected answer"}}]}),
            "embedding" => serde_json::json!({"data":[{"index":0,"embedding":[1,2]}]}),
            _ => serde_json::json!({"results":[{"index":0,"relevance_score":0.8}]}),
        };
        let mut body = body;
        body["debug"] = serde_json::json!({"internal_prompt":"private-provider-debug"});
        let mut fixture =
            Fixture::with_http_response(200, body.to_string(), Duration::ZERO, "null", true, true)
                .await;
        fixture.config.routes[0].secret = SecretSource::None;
        fixture.config.routes[0].secret_ref.clear();
        if kind == "embedding" {
            fixture.config.routes[0].provider = RouteProvider::CompatibleEmbedding {
                adapter: ModelAdapter::OpenAiEmbedding,
                operator: "test".into(),
                dimensions: 2,
                embedding_full_dimensions: 1024,
                embedding_input_tokens: 16,
            };
        } else if kind == "rerank" {
            fixture.config.routes[0].provider = RouteProvider::CohereRerank {
                operator: "test".into(),
                rerank_input_bytes: 64,
                rerank_candidates: 2,
                rerank_context_tokens: 16,
                rerank_query_tokens: 8,
            };
        }
        let mut input = queued(&fixture, kind);
        if kind == "embedding" {
            input.payload = ProviderPayload::Embedding(EmbeddingRequest {
                inputs: vec!["input".into()],
                dimensions: Some(2),
                task: None,
                role_binding: None,
                source: None,
                metadata: serde_json::Value::Null,
            });
        } else if kind == "rerank" {
            input.payload = ProviderPayload::Rerank(RerankRequest {
                query: "query".into(),
                documents: vec!["candidate".into()],
                top_k: Some(1),
                role_binding: None,
                source: None,
                metadata: serde_json::Value::Null,
            });
        }
        input.admission.attempt.input_digest = input.payload.digest().unwrap();
        input.admission = AdmissionKey::new(KEY.to_vec())
            .unwrap()
            .sign_attempt(input.admission.attempt)
            .unwrap();
        let client = InProcessEgressClient::new(fixture.process().await);
        let id = enqueue_id(&client, input).await;
        wait_job(&client, &id, JobState::Succeeded).await;
        let JobsReply::Completions(page) = job_call(
            &client,
            JobsCommand::Completions {
                after: None,
                limit: 1,
                max_bytes: 65536,
                wait_seconds: 0,
            },
        )
        .await
        .unwrap() else {
            panic!("completions")
        };
        let output = page.items[0].output.as_ref().unwrap();
        assert!(
            output["raw_provider_response"].is_null(),
            "raw response escaped for {kind}"
        );
        assert!(!output.to_string().contains("private-provider-debug"));
        match kind {
            "chat" => assert_eq!(output["text"], "selected answer"),
            "embedding" => assert_eq!(output["vectors"], serde_json::json!([[1.0, 2.0]])),
            _ => assert_eq!(output["hits"][0]["index"], 0),
        }
        let conn = rusqlite::Connection::open(
            fixture
                .config
                .state_dir
                .join(symbiotic_ai_runtime::QUEUE_DATABASE),
        )
        .unwrap();
        let recovery: String = conn
            .query_row(
                "SELECT recovery FROM spend_receipts WHERE recovery IS NOT NULL",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(!recovery.contains("private-provider-debug"));
    }
}

#[tokio::test]
async fn jobs_and_direct_dispatch_share_atomic_signed_attempt_acceptance() {
    for concurrent in [false, true] {
        let fixture = Fixture::new(200, "one dispatch".into(), Duration::ZERO).await;
        let process = fixture.process().await;
        let client = InProcessEgressClient::new(process.clone());
        let input = queued(&fixture, "atomic-attempt");
        // Issuance alone does not accept a dispatch; either path may consume it.
        let permit = permit(&process, &input.admission).await;
        let operation = inject(
            input.admission.clone(),
            input.payload.clone(),
            permit.clone(),
        );
        let id = if concurrent {
            let (id, direct) = tokio::join!(
                enqueue_id(&client, input.clone()),
                exchange(&process, operation)
            );
            assert!(matches!(
                direct,
                Ok(Reply::Dispatched(_)) | Err(EgressError::PermitRefused)
            ));
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let JobsReply::Status(row) =
                        job_call(&client, JobsCommand::JobStatus(id.clone()))
                            .await
                            .unwrap()
                    else {
                        panic!("status")
                    };
                    if matches!(row.state, JobState::Succeeded | JobState::Refused) {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            id
        } else {
            let id = enqueue_id(&client, input.clone()).await;
            wait_job(&client, &id, JobState::Succeeded).await;
            assert!(matches!(
                exchange(&process, operation).await,
                Err(EgressError::PermitRefused)
            ));
            id
        };
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
        drop(client);
        drop(process);
        let process = reopen_jobs(&fixture).await;
        assert!(matches!(
            exchange(&process, inject(input.admission, input.payload, permit)).await,
            Err(EgressError::PermitRefused)
        ));
        let client = InProcessEgressClient::new(process);
        let JobsReply::Status(row) = job_call(&client, JobsCommand::JobStatus(id)).await.unwrap()
        else {
            panic!("status")
        };
        assert!(matches!(row.state, JobState::Succeeded | JobState::Refused));
        let conn = rusqlite::Connection::open(
            fixture
                .config
                .state_dir
                .join(symbiotic_ai_runtime::QUEUE_DATABASE),
        )
        .unwrap();
        assert_eq!(
            conn.query_row("SELECT count(*) FROM spend_receipts", [], |r| r
                .get::<_, usize>(0))
                .unwrap(),
            1
        );
        assert_eq!(
            conn.query_row(
                "SELECT count(*) FROM egress_permits WHERE consumed=1",
                [],
                |r| r.get::<_, usize>(0)
            )
            .unwrap(),
            1
        );
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn jobs_cancel_during_credential_resolution_releases_without_http() {
    initialize_panic_reporting();
    let mut fixture = Fixture::new(200, "unsent".into(), Duration::ZERO).await;
    fixture.config.job_runner.heartbeat_interval_ms = Some(10);
    let entered = Arc::new(tokio::sync::Notify::new());
    let entered_resolver = entered.clone();
    let resolved = Arc::new(tokio::sync::Notify::new());
    let resolved_resolver = resolved.clone();
    let (send, receive) = std::sync::mpsc::channel();
    let receive = std::sync::Mutex::new(receive);
    fixture.config.routes[0].secret = SecretSource::Resolver {
        name: "provider-key".into(),
        resolve: Arc::new(move |_| {
            entered_resolver.notify_one();
            receive.lock().unwrap().recv().unwrap();
            // Use the existing protected fixture source; no credential is logged.
            let secret = symbiotic_ai_runtime::model::SecretValue::new(SECRET.as_bytes().to_vec());
            resolved_resolver.notify_one();
            Ok(secret)
        }),
    };
    let process = fixture.process().await;
    let client = InProcessEgressClient::new(process);
    let id = enqueue_id(&client, queued(&fixture, "resolve-cancel")).await;
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    job_call(
        &client,
        JobsCommand::CancelJobs(Selector::Ids(vec![id.clone()])),
    )
    .await
    .unwrap();
    // Keep resolution blocked until the heartbeat observes cancellation.
    let result = tokio::time::timeout(
        Duration::from_secs(1),
        wait_job(&client, &id, JobState::Cancelled),
    )
    .await;
    send.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), resolved.notified())
        .await
        .unwrap();
    let row = result.expect("cancellation must finish while the resolver is blocked");
    let conn = rusqlite::Connection::open(
        fixture
            .config
            .state_dir
            .join(symbiotic_ai_runtime::QUEUE_DATABASE),
    )
    .unwrap();
    let state: String = conn
        .query_row(
            "SELECT state FROM spend_receipts WHERE reference=?1",
            [row.receipt.unwrap()],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(state, "released");
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn jobs_signed_queue_scopes_execute_independently_and_fence_replay() {
    let fixture = Fixture::new(200, "scoped".into(), Duration::ZERO).await;
    let process = fixture.process().await;
    let client = InProcessEgressClient::new(process.clone());
    let first = queued(&fixture, "same-key");
    let id = enqueue_id(&client, first.clone()).await;
    wait_job(&client, &id, JobState::Succeeded).await;
    let mut second = first.clone();
    second.admission.attempt.job_queue = Some("another-queue".into());
    // Different signed contents must not conflict either.
    second.admission.attempt.caller_binding = "another-caller".into();
    second.admission = AdmissionKey::new(KEY.to_vec())
        .unwrap()
        .sign_attempt(second.admission.attempt)
        .unwrap();
    let scope = JobScope {
        queue: "another-queue".into(),
        ..jobs_scope()
    };
    let scoped_client = JobsClient::new(
        InProcessEgressClient::new(process.clone()),
        scope,
        AdmissionKey::new(KEY.to_vec()).unwrap(),
    );
    let permit = permit(&process, &second.admission).await;
    let JobsReply::Enqueued(items) = scoped_client
        .request(JobsCommand::EnqueueJobs(vec![second.clone()]))
        .await
        .unwrap()
    else {
        panic!("enqueue")
    };
    let Enqueued::Inserted(second_id) = &items[0] else {
        panic!("inserted")
    };
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let JobsReply::Status(row) = scoped_client
                .request(JobsCommand::JobStatus(second_id.clone()))
                .await
                .unwrap()
            else {
                panic!("status")
            };
            assert_ne!(row.state, JobState::Refused);
            if row.state == JobState::Succeeded {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 2);
    assert!(matches!(
        scoped_client
            .request(JobsCommand::EnqueueJobs(vec![first.clone()]))
            .await,
        Err(JobsClientError::Egress(EgressError::Unauthorized))
    ));
    assert!(matches!(
        exchange(
            &process,
            inject(second.admission.clone(), second.payload, permit)
        )
        .await,
        Err(EgressError::PermitRefused)
    ));
    for admission in [&first.admission, &second.admission] {
        let id = signed_id(admission);
        assert!(matches!(
            exchange(&process, Operation::AttemptStatus(id.clone())).await,
            Ok(Reply::AttemptStatus(AttemptStatus::Dispatched { .. }))
        ));
        let Ok(Reply::Receipt(Some(receipt))) = exchange(&process, Operation::Receipt(id)).await
        else {
            panic!("scoped receipt");
        };
        assert_eq!(receipt.attempt_id, admission.attempt.attempt_id());
    }
}

#[tokio::test]
async fn jobs_finished_worker_failure_remains_visible_after_acknowledgement() {
    let logs = CapturedLogs::default();
    let _subscriber = tracing::subscriber::set_default(logs.clone());
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let mut fixture = Fixture::with_response_gate(
        200,
        "paid".into(),
        Duration::ZERO,
        "null",
        false,
        false,
        Some(gate.clone()),
    )
    .await;
    fixture.config.job_runner.heartbeat_interval_ms = Some(10);
    let process = fixture.process().await;
    let client = InProcessEgressClient::new(process);
    let id = enqueue_id(&client, queued(&fixture, "heartbeat-failure")).await;
    tokio::time::timeout(Duration::from_secs(5), async {
        while fixture.calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let conn = rusqlite::Connection::open(
        fixture
            .config
            .state_dir
            .join(symbiotic_ai_runtime::QUEUE_DATABASE),
    )
    .unwrap();
    conn.execute_batch("CREATE TRIGGER reject_job_heartbeat BEFORE UPDATE OF lease_until ON jobs WHEN NEW.state='\"Running\"' BEGIN SELECT RAISE(ABORT, 'heartbeat refused'); END;").unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !logs
            .0
            .lock()
            .unwrap()
            .contains("model job heartbeat failed")
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("heartbeat fails before the provider finishes");
    gate.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let state: String = conn
                .query_row("SELECT state FROM jobs WHERE id=?1", [&id.id], |r| r.get(0))
                .unwrap();
            if state == "\"Succeeded\"" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        loop {
            if matches!(
                job_call(&client, JobsCommand::JobStatus(id.clone())).await,
                Err(JobsClientError::Egress(EgressError::StateUnavailable))
            ) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("finished workers must report their heartbeat failure");
    assert!(matches!(
        job_call(
            &client,
            JobsCommand::Completions {
                after: None,
                limit: 1,
                max_bytes: 65536,
                wait_seconds: 0
            }
        )
        .await,
        Err(JobsClientError::Egress(EgressError::StateUnavailable))
    ));
    // Read canonical completion through the trusted runtime to obtain an ack token.
    let runtime = symbiotic_ai_runtime::Runtime::open(symbiotic_ai_runtime::RuntimeConfig {
        state_dir: Some(fixture.config.state_dir.clone()),
        ..Default::default()
    })
    .unwrap();
    let jobs = runtime
        .model_jobs(jobs_scope(), fixture.config.jobs.clone())
        .unwrap();
    let page = jobs.completions(1, 65536).await.unwrap();
    let token = page[0].delivery.token.clone();
    for command in [
        JobsCommand::JobStatus(id.clone()),
        JobsCommand::AckJobs(vec![(token.clone(), Disposition::Accepted)]),
        JobsCommand::CancelJobs(Selector::Ids(vec![id.clone()])),
        JobsCommand::Completions {
            after: None,
            limit: 1,
            max_bytes: 65536,
            wait_seconds: 0,
        },
    ] {
        assert!(matches!(
            job_call(&client, command).await,
            Err(JobsClientError::Egress(EgressError::StateUnavailable))
        ));
    }
    jobs.request(symbiotic_queue::jobs::JobRequest::Ack(vec![(
        token,
        Disposition::Accepted,
    )]))
    .await
    .unwrap();
    assert!(matches!(
        job_call(
            &client,
            JobsCommand::Completions {
                after: None,
                limit: 1,
                max_bytes: 65536,
                wait_seconds: 0
            }
        )
        .await,
        Err(JobsClientError::Egress(EgressError::StateUnavailable))
    ));
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn rabbithole_unset_settings_preserve_route_identity_and_job_reattachment() {
    let fixture = Fixture::new(200, "reattached answer".into(), Duration::ZERO).await;
    let route = &fixture.config.routes[0];
    // This is the provider encoding before optional OpenAI settings existed.
    assert_eq!(
        serde_json::to_string(&route.provider).unwrap(),
        r#"{"kind":"open_ai_chat","operator":"test"}"#
    );
    let revision = symbiotic_ai_runtime::model::configuration_revision(route).unwrap();
    for (thinking, reasoning_effort) in [(Some("enabled"), None), (None, Some("low"))] {
        let mut configured = route.clone();
        configured.provider = serde_json::from_value(serde_json::json!({
            "kind": "open_ai_chat", "operator": "test",
            "thinking": thinking, "reasoning_effort": reasoning_effort
        }))
        .unwrap();
        assert_ne!(
            symbiotic_ai_runtime::model::configuration_revision(&configured).unwrap(),
            revision
        );
    }

    let client = InProcessEgressClient::new(fixture.process().await);
    let mut input = queued(&fixture, "reattach");
    input.admission.attempt.expires_at = unix_seconds();
    input.admission = AdmissionKey::new(KEY.to_vec())
        .unwrap()
        .sign_attempt(input.admission.attempt)
        .unwrap();
    let id = enqueue_id(&client, input.clone()).await;
    wait_job(&client, &id, JobState::AwaitingAdmission).await;
    drop(client);

    let client = InProcessEgressClient::new(reopen_jobs(&fixture).await);
    assert_eq!(enqueue_id(&client, input).await, id);
    let (admission, _) = fixture.job_attempt("reattach", 2, 2);
    job_call(
        &client,
        JobsCommand::AdmitJob {
            job: id.clone(),
            admission: Box::new(admission),
        },
    )
    .await
    .unwrap();
    wait_job(&client, &id, JobState::Succeeded).await;
    let new_id = enqueue_id(&client, queued(&fixture, "new-job")).await;
    wait_job(&client, &new_id, JobState::Succeeded).await;
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn unset_thinking_retry_preserves_prechange_spend_binding_after_reopen() {
    use symbiotic_ai_runtime::{
        BindingIdentity, ModelBinding, ModelProvider, SpendLedger, SpendReceiptRef,
        SpendReservation, SpendState,
        jobs::model_job_payload,
        model::{OpenAiCompatibleChatProvider, configuration_revision},
        spend::SqliteSpendLedger,
    };
    use symbiotic_queue::jobs::{
        Execution, JobLimits, JobRequest, JobResolution, JobResponse, JobSpec,
    };
    use symbiotic_queue_sqlite::jobs::jobs_in_transaction;

    tokio::time::timeout(Duration::from_secs(5), async {
        let fixture = Fixture::new(200, "authorized retry".into(), Duration::ZERO).await;
        // Initialize the same route registry and grant revision, without starting a worker.
        drop(fixture.process().await);
        let route = &fixture.config.routes[0];
        let input = queued(&fixture, "prechange-reservation");
        let scope = jobs_scope();
        let invocation =
            serde_json::to_string(&(&scope, &input.admission.attempt.invocation_id)).unwrap();
        let ProviderPayload::Chat(mut request) = input.payload else {
            panic!("chat payload")
        };
        request.source = Some(invocation.clone());

        // Historical adapter construction: optional thinking/effort setters were absent.
        let legacy =
            OpenAiCompatibleChatProvider::new("test", &route.model, &route.destination, "")
                .with_timeout(route.timeout_seconds)
                .unwrap()
                .with_request_limit(route.max_input_bytes)
                .with_response_limit(route.max_response_bytes)
                .with_output_limit(route.max_output_tokens);
        let binding = ModelBinding::new(legacy).with_identity(BindingIdentity::new(
            &route.tenant,
            &route.route,
            configuration_revision(route).unwrap().0,
            &route.account,
        ));
        let mut descriptor = binding.provider.descriptor().clone();
        descriptor.metadata = serde_json::json!({
            "configuration": descriptor.metadata, "binding": binding.identity
        });
        let reservation = SpendReservation {
            reference: SpendReceiptRef::new("job:prechange-reservation").unwrap(),
            account: symbiotic_ai_runtime::account_scope(binding.identity.as_ref().unwrap(), None)
                .unwrap(),
            invocation: symbiotic_ai_runtime::model::execution_invocation_identity(
                binding.identity.as_ref().unwrap(),
                &invocation,
            )
            .unwrap(),
            binding: configuration_revision(&(
                "chat",
                &descriptor,
                &binding.identity,
                configuration_revision(&request).unwrap().0,
            ))
            .unwrap()
            .0,
            request_limit: route.provider_request_limit,
        };
        let path = fixture
            .config
            .state_dir
            .join(symbiotic_ai_runtime::QUEUE_DATABASE);
        let ledger = SqliteSpendLedger::open(&path).unwrap();
        assert!(
            ledger
                .reserve_explicit(&reservation, route.max_attempts)
                .unwrap()
        );
        drop(ledger);
        let mut conn = rusqlite::Connection::open(&path).unwrap();
        let mut tx = conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .unwrap();
        let JobResponse::Enqueued(items) = jobs_in_transaction(
            &mut tx,
            &scope,
            &fixture.config.jobs,
            chrono::Utc::now(),
            JobRequest::Enqueue(vec![JobSpec {
                key: input.admission.attempt.invocation_id.clone(),
                kind: route.route.clone(),
                group: input.group,
                owners: input.owners,
                execution: Execution::Model,
                payload: model_job_payload(&binding, &request).unwrap(),
                admission: Some(serde_json::to_vec(&input.admission).unwrap()),
                limits: JobLimits {
                    max_attempts: route.max_attempts,
                },
                recovery_until: chrono::DateTime::from_timestamp(
                    input.admission.attempt.recovery_expires_at as i64,
                    0,
                ),
            }]),
        )
        .unwrap() else {
            panic!("historical enqueue")
        };
        let Enqueued::Inserted(id) = &items[0] else {
            panic!("historical job")
        };
        let id = id.clone();
        assert!(matches!(
            jobs_in_transaction(
                &mut tx,
                &scope,
                &fixture.config.jobs,
                chrono::Utc::now(),
                JobRequest::ClaimPaid {
                    job: id.clone(),
                    receipt: reservation.reference.as_str().into(),
                },
            )
            .unwrap(),
            JobResponse::Job(Some(_))
        ));
        tx.commit().unwrap();
        drop(conn); // Crash after reservation, before HTTP.
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);

        // Trusted evidence that transport never started releases the historical receipt.
        let ledger = SqliteSpendLedger::open(&path).unwrap();
        ledger
            .release_before_dispatch(&reservation.reference)
            .unwrap();
        let receipt = ledger.receipt(&reservation.reference).unwrap().unwrap();
        assert_eq!(receipt.state, SpendState::Released);
        assert!(receipt.pre_dispatch_released);
        assert_eq!(receipt.attempt_limit, Some(route.max_attempts));
        assert_eq!(receipt.attempts_used, 0);
        drop(ledger);
        let mut conn = rusqlite::Connection::open(&path).unwrap();
        let mut tx = conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .unwrap();
        jobs_in_transaction(
            &mut tx,
            &scope,
            &fixture.config.jobs,
            chrono::Utc::now(),
            JobRequest::Resolve {
                job: id.clone(),
                generation: 1,
                resolution: JobResolution::KnownZeroCharge {
                    receipt: reservation.reference.as_str().into(),
                },
            },
        )
        .unwrap();
        tx.commit().unwrap();
        drop(conn);

        let client = InProcessEgressClient::new(reopen_jobs(&fixture).await);
        let waiting = wait_job(&client, &id, JobState::AwaitingAdmission).await;
        assert_eq!(
            waiting.receipt.as_deref(),
            Some(reservation.reference.as_str())
        );
        assert_eq!(waiting.generation, 1);
        let (admission, _) = fixture.job_attempt("prechange-reservation", 2, 2);
        job_call(
            &client,
            JobsCommand::AdmitJob {
                job: id.clone(),
                admission: Box::new(admission),
            },
        )
        .await
        .unwrap();
        let completed = wait_job(&client, &id, JobState::Succeeded).await;
        assert_eq!(completed.generation, 2);
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
        let ledger = SqliteSpendLedger::open(&path).unwrap();
        let receipt = ledger
            .receipt(&SpendReceiptRef::new(completed.receipt.unwrap()).unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(receipt.state, SpendState::Settled);
        assert_eq!(receipt.reservation.binding, reservation.binding);
        let new_id = enqueue_id(&client, queued(&fixture, "after-retry")).await;
        wait_job(&client, &new_id, JobState::Succeeded).await;
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 2);
    })
    .await
    .expect("released historical reservation must retry within five seconds");
}

#[tokio::test]
async fn rabbithole_jobs_preserve_canonical_completion_shapes() {
    for classify in [false, true] {
        let mut fixture = if classify {
            rabbithole_jev_fixture(None).await
        } else {
            Fixture::with_http_response(
                200,
                serde_json::json!({"choices":[{"message":{"content":"answer"},
                    "finish_reason":"content_filter"}],
                    "usage":{"prompt_tokens":7,"completion_tokens":3}})
                .to_string(),
                Duration::ZERO,
                "0",
                true,
                false,
            )
            .await
        };
        fixture.config.routes[0].provider_request_limit = None;
        let process = fixture.process().await;
        let (admission, payload) = if classify {
            rabbithole_classify_attempt(&fixture)
        } else {
            fixture.attempt("direct-shape", 1, 1)
        };
        let granted = permit(&process, &admission).await;
        let result = dispatched(
            exchange(&process, inject(admission, payload.clone(), granted))
                .await
                .unwrap(),
        );
        assert!(result.error.is_none());
        let direct = serde_json::to_value(result.output).unwrap();
        if classify {
            assert_eq!(direct.as_object().unwrap().len(), 2); // kind and answers
        } else {
            assert_eq!(direct["finish_reason"], "other");
        }

        let client = InProcessEgressClient::new(process);
        let mut input = queued(&fixture, "job-shape");
        input.payload = payload;
        input.admission.attempt.input_digest = input.payload.digest().unwrap();
        input.admission = AdmissionKey::new(KEY.to_vec())
            .unwrap()
            .sign_attempt(input.admission.attempt)
            .unwrap();
        let id = enqueue_id(&client, input).await;
        wait_job(&client, &id, JobState::Succeeded).await;
        let JobsReply::Completions(page) = job_call(
            &client,
            JobsCommand::Completions {
                after: None,
                limit: 1,
                max_bytes: 65536,
                wait_seconds: 0,
            },
        )
        .await
        .unwrap() else {
            panic!("completions")
        };
        let output = page.items[0].output.as_ref().unwrap();
        assert!(output["raw_provider_response"].is_null());
        assert!(output["trace"].is_object());
        let metadata = output["trace"]["metadata"].as_object().unwrap();
        assert!(metadata["value"].is_null());
        assert!(metadata.get("provider").is_none());
        assert!(metadata["spend_receipt"].is_string());
        if classify {
            assert_eq!(output["served_model"], "test-model");
            assert_eq!(output["answers"], direct["answers"]);
        } else {
            assert_eq!(output["finish_reason"], "content_filter");
            assert_eq!(output["text"], direct["text"]);
        }
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 2);
    }
}

#[tokio::test]
async fn rabbithole_deepseek_settings_reach_wire_only_when_configured() {
    for configured in [false, true] {
        let mut fixture = Fixture::new(200, "answer".into(), Duration::ZERO).await;
        if configured {
            fixture.config.routes[0].provider = serde_json::from_value(serde_json::json!({
                "kind":"open_ai_chat", "operator":"test",
                "thinking":"enabled", "reasoning_effort":"low"
            }))
            .unwrap();
        }
        let process = fixture.process().await;
        let (admission, payload) = fixture.attempt("deepseek", 1, 1);
        let granted = permit(&process, &admission).await;
        let result = dispatched(
            exchange(&process, inject(admission, payload, granted))
                .await
                .unwrap(),
        );
        assert!(result.error.is_none());
        let requests = fixture.requests.lock().unwrap();
        let body = &requests[0].1;
        if configured {
            assert_eq!(body["thinking"], serde_json::json!({"type":"enabled"}));
            assert_eq!(body["reasoning_effort"], "low");
        } else {
            assert!(body.get("thinking").is_none());
            assert!(body.get("reasoning_effort").is_none());
        }
    }
}

#[tokio::test]
async fn rabbithole_chat_finish_reasons_survive_in_process_and_recovery() {
    for (anthropic, reason, expected) in [
        (false, "stop", "stop"),
        (false, "length", "length"),
        (false, "content_filter", "other"),
        (false, "", "absent"),
        (true, "end_turn", "stop"),
        (true, "stop_sequence", "stop"),
        (true, "max_tokens", "length"),
        (true, "model_context_window_exceeded", "length"),
        (true, "tool_use", "refused"),
    ] {
        let mut body = if anthropic {
            serde_json::json!({"content":[{"type":"text","text":"answer"}],"stop_reason":reason,
                "usage":{"input_tokens":7,"output_tokens":3}})
        } else {
            serde_json::json!({"choices":[{"message":{"content":"answer"},"finish_reason":reason}],
                "usage":{"prompt_tokens":7,"completion_tokens":3}})
        };
        if expected == "absent" {
            body["choices"][0]
                .as_object_mut()
                .unwrap()
                .remove("finish_reason");
        }
        let mut fixture =
            Fixture::with_http_response(200, body.to_string(), Duration::ZERO, "0", true, false)
                .await;
        if anthropic {
            fixture.config.routes[0].provider = RouteProvider::AnthropicChat {
                operator: "test".into(),
                thinking: None,
            };
        }
        let process = fixture.process().await;
        let (admission, payload) = fixture.attempt("finish", 1, 1);
        let granted = permit(&process, &admission).await;
        let result = dispatched(
            exchange(&process, inject(admission.clone(), payload, granted))
                .await
                .unwrap(),
        );
        if expected == "refused" {
            assert_eq!(result.error, Some(EgressError::Provider { status: None }));
            assert!(result.output.is_none());
            assert_eq!(result.receipt.spend_state, SpendState::Unknown);
            drop(process);
            let process = fixture.process().await;
            assert!(matches!(
                status(&process, &admission).await,
                AttemptStatus::Failed { .. }
            ));
            assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
            continue;
        }
        assert!(result.error.is_none(), "{reason}: {:?}", result.error);
        let output = serde_json::to_value(&result.output).unwrap();
        if expected == "absent" {
            assert!(output["finish_reason"].is_null());
        } else {
            assert_eq!(output["finish_reason"], expected);
        }
        assert!(result.receipt_persisted);
        drop(process);
        let process = fixture.process().await;
        let AttemptStatus::Completed { result: recovered } = status(&process, &admission).await
        else {
            panic!("missing chat completion")
        };
        assert_eq!(serde_json::to_value(recovered.output).unwrap(), output);
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
    }
}

async fn rabbithole_jev_fixture(gate: Option<Arc<tokio::sync::Semaphore>>) -> Fixture {
    let body = serde_json::json!({"model":"test-model", "id":"classification-fixture", "created":1789603200, "answers":{
        "continue":{"type":"noul","noul":0.1200},
        "parent":{"type":"choice","choice":"none","probabilities":{"previous":0.25,"none":0.75}},
        "strength":{"type":"score","score":0.75,"probabilities":{"0":0.25,"1":0.75}}
    }, "usage":{"input_tokens":612,"output_tokens":20,"cache_hit_tokens":600,"cache_miss_tokens":12,"cost":"0.001"}, "debug":"private-provider-debug"});
    let mut fixture = Fixture::with_response_gate(
        200,
        body.to_string(),
        Duration::ZERO,
        "0",
        true,
        false,
        gate,
    )
    .await;
    fixture.config.routes[0].provider =
        serde_json::from_value(serde_json::json!({"kind":"jev_classifier","operator":"test"}))
            .unwrap();
    fixture.config.routes[0].provider_request_limit = Some(1);
    fixture.config.routes[0].timeout_seconds = 5;
    fixture
}

fn rabbithole_classify_attempt(fixture: &Fixture) -> (SignedAttempt, ProviderPayload) {
    let request = ClassifyRequest::new(
        serde_json::from_value(serde_json::json!({"lines":{"m000":{"text":"hello","seconds_since_previous":12,"weight":0.25}}})).unwrap(),
        vec![
            ClassifierQuestion::noul("continue", "Does m000 continue an exchange?", None, None),
            ClassifierQuestion::choice(
                "parent",
                "Which line?",
                [("previous", "Earlier line"), ("none", "No line")],
            ),
            ClassifierQuestion::score("strength", "How strong?", ["Weak", "Strong"]),
        ],
    );
    let payload: ProviderPayload =
        serde_json::from_value(serde_json::json!({"kind":"classify","request":request})).unwrap();
    let (signed, _) = fixture.attempt("classification", 1, 1);
    let mut attempt = signed.attempt;
    attempt.input_digest = payload.digest().unwrap();
    (
        AdmissionKey::new(KEY.to_vec())
            .unwrap()
            .sign_attempt(attempt)
            .unwrap(),
        payload,
    )
}

#[tokio::test]
async fn rabbithole_classification_charges_once_and_recovers_typed_answers() {
    let fixture = rabbithole_jev_fixture(None).await;
    let process = fixture.process().await;
    let (admission, payload) = rabbithole_classify_attempt(&fixture);
    let granted = permit(&process, &admission).await;
    let result = dispatched(
        exchange(
            &process,
            inject(admission.clone(), payload.clone(), granted.clone()),
        )
        .await
        .unwrap(),
    );
    assert!(result.error.is_none());
    assert!(result.receipt_persisted);
    assert_eq!(result.receipt.spend_state, SpendState::Settled);
    assert_eq!(result.receipt.usage.input_tokens, Some(612));
    assert_eq!(
        result.receipt.usage.reported_cost_usd.as_deref(),
        Some("0.001")
    );
    let output = serde_json::to_value(&result.output).unwrap();
    assert_eq!(output["kind"], "classify");
    assert_eq!(output["answers"][0]["question_id"], "continue");
    assert_eq!(output["answers"][0]["value"]["noul"]["probability"], 0.12);
    assert_eq!(output["answers"][1]["value"]["choice"]["chosen"], "none");
    assert_eq!(output["answers"][2]["value"]["score"]["value"], 0.75);
    assert!(output.get("raw_provider_response").is_none());
    {
        let requests = fixture.requests.lock().unwrap();
        assert!(requests[0].0.starts_with("POST /v1/systemone "));
        assert_eq!(requests[0].1["state"]["lines"]["m000"]["text"], "hello");
        assert_eq!(
            requests[0].1["state"]["lines"]["m000"]["seconds_since_previous"],
            12
        );
        assert_eq!(requests[0].1["state"]["lines"]["m000"]["weight"], 0.25);
        assert_eq!(requests[0].1["questions"]["continue"]["type"], "noul");
        assert!(requests[0].1.get("metadata").is_none());
    }
    drop(process);
    let process = fixture.process().await;
    let AttemptStatus::Completed { result: recovered } = status(&process, &admission).await else {
        panic!("missing classification")
    };
    assert_eq!(serde_json::to_value(recovered.output).unwrap(), output);
    assert!(matches!(
        exchange(&process, inject(admission, payload, granted)).await,
        Err(EgressError::PermitRefused)
    ));
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
    assert_eq!(ledger_totals(&fixture), (1, 1));
}

#[tokio::test]
async fn rabbithole_classification_crash_never_resends_or_charges_again() {
    let fixture = rabbithole_jev_fixture(Some(Arc::new(tokio::sync::Semaphore::new(0)))).await;
    let config = fixture.dir.path().join("config.json");
    std::fs::write(&config, serde_json::to_vec(&fixture.config).unwrap()).unwrap();
    std::fs::set_permissions(&config, std::fs::Permissions::from_mode(0o600)).unwrap();
    let mut command = std::process::Command::new(credential_process());
    command
        .arg(&config)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    for name in [
        "HTTP_PROXY",
        "http_proxy",
        "HTTPS_PROXY",
        "https_proxy",
        "ALL_PROXY",
        "all_proxy",
    ] {
        command.env_remove(name);
    }
    let mut child = Child(command.spawn().unwrap());
    let client = socket::UnixEgressClient {
        path: fixture.config.socket_path.clone(),
        max_frame_bytes: fixture.config.max_frame_bytes,
        timeout: Duration::from_secs(2),
    };
    assert!(matches!(
        wait_for_child_startup(&mut child, || exchange_client(&client, publish_revision(1))).await,
        Reply::GrantRevisionPublished
    ));
    let (admission, payload) = rabbithole_classify_attempt(&fixture);
    let Reply::Permit(grant) =
        exchange_client(&client, Operation::IssuePermit(admission.clone().into()))
            .await
            .unwrap()
    else {
        panic!("missing permit")
    };
    let peer = send_without_reading(
        &client,
        inject(admission.clone(), payload.clone(), grant.permit.clone()),
    )
    .await;
    tokio::time::timeout(OBSERVATION_HANG_GUARD, async {
        while fixture.calls.load(Ordering::SeqCst) != 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    child.0.kill().unwrap();
    child.0.wait().unwrap();
    drop(peer);
    let before = ledger_totals(&fixture);
    let process = fixture.process().await;
    let AttemptStatus::Dispatched { receipt } = status(&process, &admission).await else {
        panic!("crash must retain unknown dispatch")
    };
    assert_eq!(receipt.spend_state, SpendState::Unknown);
    assert!(matches!(
        exchange(&process, inject(admission.clone(), payload, grant.permit)).await,
        Err(EgressError::PermitRefused)
    ));
    let reattached = permit(&process, &admission).await;
    assert!(!reattached.token.is_empty());
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
    assert_eq!(ledger_totals(&fixture), before);
}

#[tokio::test]
async fn rabbithole_classification_validation_precedes_consumption() {
    let fixture = rabbithole_jev_fixture(None).await;
    let process = fixture.process().await;
    for (invocation, questions) in [
        ("empty", vec![]),
        (
            "options",
            vec![ClassifierQuestion::choice(
                "parent",
                "Which line?",
                (0..256).map(|i| (format!("m{i}"), "Earlier line")),
            )],
        ),
    ] {
        let request = ClassifyRequest::new(serde_json::Map::new(), questions);
        let payload: ProviderPayload =
            serde_json::from_value(serde_json::json!({"kind":"classify","request":request}))
                .unwrap();
        let (signed, _) = fixture.attempt(invocation, 1, 1);
        let mut attempt = signed.attempt;
        attempt.input_digest = payload.digest().unwrap();
        let signed = AdmissionKey::new(KEY.to_vec())
            .unwrap()
            .sign_attempt(attempt)
            .unwrap();
        let granted = permit(&process, &signed).await;
        assert!(matches!(
            exchange(&process, inject(signed.clone(), payload, granted)).await,
            Err(EgressError::InvalidRequest)
        ));
        assert!(matches!(
            status(&process, &signed).await,
            AttemptStatus::Permitted
        ));
    }
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
    assert_eq!(ledger_totals(&fixture), (0, 0));
}

#[tokio::test]
async fn rabbithole_classification_jobs_reuse_runner_and_strip_raw_response() {
    let fixture = rabbithole_jev_fixture(None).await;
    let client = InProcessEgressClient::new(fixture.process().await);
    let (signed, payload) = rabbithole_classify_attempt(&fixture);
    let mut attempt = signed.attempt;
    attempt.job_queue = Some(jobs_scope().queue);
    let input = EnqueueJob {
        group: None,
        owners: vec![],
        payload,
        admission: AdmissionKey::new(KEY.to_vec())
            .unwrap()
            .sign_attempt(attempt)
            .unwrap(),
    };
    let id = enqueue_id(&client, input.clone()).await;
    assert_eq!(enqueue_id(&client, input).await, id);
    wait_job(&client, &id, JobState::Succeeded).await;
    let JobsReply::Completions(page) = job_call(
        &client,
        JobsCommand::Completions {
            after: None,
            limit: 1,
            max_bytes: 65536,
            wait_seconds: 0,
        },
    )
    .await
    .unwrap() else {
        panic!("missing completion")
    };
    let output = page.items[0].output.as_ref().unwrap();
    assert_eq!(output["answers"][0]["question_id"], "continue");
    assert_eq!(output["answers"][1]["value"]["choice"]["chosen"], "none");
    assert!(output["raw_provider_response"].is_null());
    assert!(!output.to_string().contains("private-provider-debug"));
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
    assert_eq!(ledger_totals(&fixture), (1, 1));
}

#[tokio::test]
async fn route_byte_defaults_and_overrides_work_in_process() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut fixture = Fixture::new(200, "default answer".into(), Duration::ZERO).await;
        let mut config = serde_json::to_value(&fixture.config).unwrap();
        // Keep the existing frame boundary: fourfold response plus identities/envelope.
        config["max_frame_bytes"] = serde_json::json!(8 * DEFAULT_MAX_RESPONSE_BYTES);
        for omitted in [
            vec!["max_input_bytes"],
            vec!["max_response_bytes"],
            vec!["max_input_bytes", "max_response_bytes"],
        ] {
            let mut value = config.clone();
            let route = value["routes"][0].as_object_mut().unwrap();
            for field in &omitted {
                route.remove(*field);
            }
            let parsed: ProcessConfig =
                serde_json::from_value(value).expect("route limits may be omitted");
            assert_eq!(
                symbiotic_credential_process::validate_routes(
                    &parsed.routes,
                    parsed.max_frame_bytes,
                ),
                Ok(())
            );
            assert_eq!(
                parsed.routes[0].max_input_bytes,
                if omitted.contains(&"max_input_bytes") {
                    DEFAULT_MAX_REQUEST_BYTES
                } else {
                    32768
                }
            );
            assert_eq!(
                parsed.routes[0].max_response_bytes,
                if omitted.contains(&"max_response_bytes") {
                    DEFAULT_MAX_RESPONSE_BYTES
                } else {
                    32768
                }
            );
        }
        for field in ["max_input_bytes", "max_response_bytes"] {
            config["routes"][0].as_object_mut().unwrap().remove(field);
        }
        fixture.config = serde_json::from_value(config).unwrap();
        for field in ["max_input_bytes", "max_response_bytes"] {
            let mut zero = serde_json::to_value(&fixture.config).unwrap();
            zero["routes"][0][field] = serde_json::json!(0);
            let invalid: ProcessConfig = serde_json::from_value(zero).unwrap();
            assert_eq!(
                symbiotic_credential_process::validate_routes(
                    &invalid.routes,
                    invalid.max_frame_bytes,
                ),
                Err(EgressError::InvalidRequest)
            );
            assert!(matches!(
                CredentialProcess::open(invalid),
                Err(EgressError::InvalidRequest)
            ));
            assert!(
                !fixture.config.state_dir.exists(),
                "invalid limits must fail before state creation"
            );
        }
        let process = fixture.process().await;
        let client = InProcessEgressClient::new(process.clone());
        let (admission, payload) = fixture.attempt("default-byte-limits", 1, 1);
        let granted = permit(&process, &admission).await;
        let result = dispatched(
            exchange_client(&client, inject(admission, payload, granted))
                .await
                .unwrap(),
        );
        assert!(matches!(result.output,
            Some(ProviderOutput::Chat { text, .. }) if text == "default answer"));
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
    })
    .await
    .expect("default-limit in-process dispatch must finish within five seconds");
}

#[tokio::test]
async fn rabbithole_usage_metadata_round_trips_in_process_and_recovery() {
    tokio::time::timeout(Duration::from_secs(5), async {
        for anthropic in [false, true] {
            let body = if anthropic {
                serde_json::json!({"id":"response-fixture","model":"test-model",
                    "content":[{"type":"text","text":""}],"stop_reason":"max_tokens",
                    "usage":{"input_tokens":20,"cache_read_input_tokens":70,
                        "cache_creation_input_tokens":10,"output_tokens":3}})
            } else {
                serde_json::json!({"id":"response-fixture","model":"test-model","created":1789603200,
                    "choices":[{"message":{"content":"","reasoning_content":"private reasoning"},"finish_reason":"length"}],
                    "usage":{"prompt_tokens":100,"prompt_cache_hit_tokens":70,"prompt_cache_miss_tokens":30,
                        "completion_tokens":3,"completion_tokens_details":{"reasoning_tokens":2}}})
            };
            let mut fixture = Fixture::with_http_response(200, body.to_string(), Duration::ZERO, "0", true, false).await;
            if anthropic {
                fixture.config.routes[0].provider = RouteProvider::AnthropicChat {operator:"test".into(), thinking:None};
            }
            let process = fixture.process().await;
            let client = InProcessEgressClient::new(process.clone());
            let (admission, payload) = fixture.attempt("usage", 1, 1);
            let granted = permit(&process, &admission).await;
            let result = dispatched(exchange_client(&client, inject(admission.clone(), payload, granted)).await.unwrap());
            assert!(result.error.is_none());
            let usage = serde_json::to_value(&result.receipt.usage).unwrap();
            assert_eq!(usage["input_tokens"], 100);
            assert_eq!(usage["output_tokens"], 3);
            assert_eq!(usage["cache_hit_tokens"], 70);
            assert_eq!(usage["cache_miss_tokens"], 30);
            assert_eq!(usage["response_id"], "response-fixture");
            assert_eq!(usage["served_model"], "test-model");
            if anthropic { assert!(usage["created"].is_null()); }
            else { assert_eq!(usage["created"], 1789603200); assert_eq!(usage["reasoning_tokens"], 2); }
            assert!(matches!(&result.output, Some(ProviderOutput::Chat {text, finish_reason:Some(FinishReason::Length)}) if text.is_empty()));
            let serialized = serde_json::to_string(&result).unwrap();
            assert!(!serialized.contains("private reasoning"));
            assert!(!serialized.contains(SECRET));
            drop(client);
            drop(process);
            let process = fixture.process().await;
            let AttemptStatus::Completed {result:recovered} = status(&process, &admission).await else {panic!("missing result")};
            assert_eq!(serde_json::to_value(recovered.receipt.usage).unwrap(), usage);
            assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
        }
    }).await.expect("usage dispatch and recovery must finish within five seconds");
}

#[tokio::test]
async fn rabbithole_failure_classes_round_trip_in_process_and_recovery() {
    tokio::time::timeout(Duration::from_secs(30), async {
        for anthropic in [false, true] {
            for (http_status, delay, expected) in [
                (0, Duration::ZERO, serde_json::json!("transport")),
                (
                    429,
                    Duration::ZERO,
                    serde_json::json!({"rate_limited":{"retry_after_seconds":7}}),
                ),
                (
                    500,
                    Duration::ZERO,
                    serde_json::json!({"provider":{"status":500}}),
                ),
                (200, Duration::from_secs(2), serde_json::json!("timeout")),
            ] {
                let mut fixture =
                    Fixture::new(http_status, "private provider error".into(), delay).await;
                if anthropic {
                    fixture.config.routes[0].provider = RouteProvider::AnthropicChat {
                        operator: "test".into(),
                        thinking: None,
                    };
                }
                let process = fixture.process().await;
                let client = InProcessEgressClient::new(process.clone());
                let (admission, payload) = fixture.attempt("failure-class", 1, 1);
                let granted = permit(&process, &admission).await;
                let result = dispatched(
                    exchange_client(&client, inject(admission.clone(), payload, granted))
                        .await
                        .unwrap(),
                );
                assert_eq!(serde_json::to_value(result.error).unwrap(), expected);
                assert_eq!(result.receipt.spend_state, SpendState::Unknown);
                let serialized = serde_json::to_string(&result).unwrap();
                assert!(!serialized.contains("private provider error"));
                assert!(!serialized.contains(SECRET));
                drop(client);
                drop(process);
                let process = fixture.process().await;
                let AttemptStatus::Failed { result: recovered } =
                    status(&process, &admission).await
                else {
                    panic!("missing failure")
                };
                assert_eq!(serde_json::to_value(recovered.error).unwrap(), expected);
                assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
            }
        }
    })
    .await
    .expect("failure dispatch and recovery must finish within thirty seconds (hang guard)");
}

#[tokio::test]
async fn rabbithole_classification_usage_metadata_survives_projection() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let fixture = rabbithole_jev_fixture(None).await;
        let process = fixture.process().await;
        let client = InProcessEgressClient::new(process.clone());
        let (admission, payload) = rabbithole_classify_attempt(&fixture);
        let granted = permit(&process, &admission).await;
        let result = dispatched(
            exchange_client(&client, inject(admission, payload, granted))
                .await
                .unwrap(),
        );
        assert!(result.error.is_none());
        let usage = serde_json::to_value(&result.receipt.usage).unwrap();
        assert_eq!(usage["input_tokens"], 612);
        assert_eq!(usage["output_tokens"], 20);
        assert_eq!(usage["served_model"], "test-model");
        assert_eq!(usage["response_id"], "classification-fixture");
        assert_eq!(usage["created"], 1789603200);
        assert_eq!(usage["cache_hit_tokens"], 600);
        assert_eq!(usage["cache_miss_tokens"], 12);
        assert!(
            !serde_json::to_string(&result)
                .unwrap()
                .contains("private-provider-debug")
        );
    })
    .await
    .expect("classification usage must finish within five seconds");
}

#[tokio::test]
async fn rabbithole_response_identity_without_token_usage_recovers() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let body = serde_json::json!({"id":"response-fixture","model":"test-model","created":1789603200,
            "choices":[{"message":{"content":"answer"},"finish_reason":"stop"}]});
        let fixture = Fixture::with_http_response(200,body.to_string(),Duration::ZERO,"0",true,false).await;
        let process = fixture.process().await;
        let client = InProcessEgressClient::new(process.clone());
        let (admission,payload) = fixture.attempt("identity-only",1,1);
        let granted = permit(&process,&admission).await;
        let result = dispatched(exchange_client(&client,inject(admission.clone(),payload,granted)).await.unwrap());
        assert!(result.error.is_none());
        assert_eq!(result.receipt.spend_state,SpendState::Unknown);
        assert_eq!(serde_json::to_value(&result.receipt.usage).unwrap()["response_id"], "response-fixture");
        assert!(result.receipt.usage.input_tokens.is_none());
        assert!(result.receipt.usage.output_tokens.is_none());
        let usage = serde_json::to_value(result.receipt.usage).unwrap();
        drop(client);
        drop(process);
        let process = fixture.process().await;
        let AttemptStatus::Completed {result} = status(&process,&admission).await else {panic!("missing completion")};
        assert_eq!(serde_json::to_value(result.receipt.usage).unwrap(),usage);
        assert_eq!(result.receipt.spend_state,SpendState::Unknown);
        assert_eq!(fixture.calls.load(Ordering::SeqCst),1);
    }).await.expect("identity-only recovery must finish within five seconds");
}

// Each fixture reports completion through its task; the timeout only detects a hung fixture.
// No elapsed-time limit covers the cumulative setup and durable writes of the whole matrix.
async fn identity_fixture_completion(
    completion: tokio::task::JoinHandle<()>,
) -> Result<(), tokio::time::error::Elapsed> {
    tokio::time::timeout(Duration::from_secs(30), completion)
        .await?
        .expect("identity fixture task panicked");
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn regression_identity_fixture_completion_outlives_old_deadline() {
    let started = tokio::time::Instant::now();
    let completion = tokio::spawn(async {
        tokio::time::sleep(Duration::from_secs(6)).await;
    });
    identity_fixture_completion(completion).await.unwrap();
    assert_eq!(started.elapsed(), Duration::from_secs(6));
}

#[tokio::test(start_paused = true)]
async fn regression_identity_fixture_completion_reports_hang() {
    let started = tokio::time::Instant::now();
    let completion = tokio::spawn(std::future::pending::<()>());
    assert!(identity_fixture_completion(completion).await.is_err());
    assert_eq!(started.elapsed(), Duration::from_secs(30));
}

#[tokio::test]
async fn regression_egress_drops_malformed_usage_identity_and_recovers_diagnostic() {
    for anthropic in [false, true] {
        for (field, value) in [
            ("id", serde_json::json!("answer\nprivate text")),
            ("id", serde_json::json!({})),
            ("id", serde_json::json!("a".repeat(129))),
            ("model", serde_json::json!("answer with spaces")),
            ("created", serde_json::json!("invalid")),
            ("created", serde_json::json!(-1)),
        ] {
            let completion = tokio::spawn(async move {
                let mut body = if anthropic {
                    serde_json::json!({"content":[{"type":"text","text":"answer"}],"stop_reason":"end_turn"})
                } else {
                    serde_json::json!({"choices":[{"message":{"content":"answer"}}]})
                };
                body[field] = value;
                let mut fixture = Fixture::with_http_response(
                    200,
                    body.to_string(),
                    Duration::ZERO,
                    "0",
                    true,
                    false,
                )
                .await;
                if anthropic {
                    fixture.config.routes[0].provider = RouteProvider::AnthropicChat {
                        operator: "test".into(),
                        thinking: None,
                    };
                }
                let process = fixture.process().await;
                let (admission, payload) = fixture.attempt("invalid-identity", 1, 1);
                let granted = permit(&process, &admission).await;
                let result = dispatched(
                    exchange(&process, inject(admission.clone(), payload, granted))
                        .await
                        .unwrap(),
                );
                assert_eq!(result.error, None, "{field}, anthropic={anthropic}");
                assert!(
                    matches!(&result.output, Some(ProviderOutput::Chat {text, ..}) if text == "answer")
                );
                assert_eq!(
                    serde_json::to_value(&result.receipt.usage).unwrap()[match field {
                        "id" => "response_id",
                        "model" => "served_model",
                        _ => "created",
                    }],
                    serde_json::Value::Null
                );
                assert_eq!(
                    serde_json::to_value(&result.diagnostics).unwrap(),
                    serde_json::json!(["invalid_usage_identity"])
                );
                let AttemptStatus::Completed { result: recovered } =
                    status(&process, &admission).await
                else {
                    panic!("missing answer")
                };
                assert_eq!(recovered.diagnostics, result.diagnostics);
            });
            identity_fixture_completion(completion)
                .await
                .unwrap_or_else(|_| {
                    panic!("identity fixture hung: {field}, anthropic={anthropic}")
                });
        }
    }
}

#[tokio::test]
async fn regression_egress_cache_only_usage_settles_and_recovers() {
    tokio::time::timeout(Duration::from_secs(5), async {
        for field in ["prompt_cache_hit_tokens", "prompt_cache_miss_tokens"] {
            let mut body =
                serde_json::json!({"choices":[{"message":{"content":"answer"}}],"usage":{}});
            body["usage"][field] = serde_json::json!(7);
            let fixture = Fixture::with_http_response(
                200,
                body.to_string(),
                Duration::ZERO,
                "0",
                true,
                false,
            )
            .await;
            let process = fixture.process().await;
            let (admission, payload) = fixture.attempt("cache-only", 1, 1);
            let granted = permit(&process, &admission).await;
            let result = dispatched(
                exchange(&process, inject(admission.clone(), payload, granted))
                    .await
                    .unwrap(),
            );
            assert!(result.error.is_none());
            assert_eq!(result.receipt.spend_state, SpendState::Settled);
            let usage = serde_json::to_value(result.receipt.usage).unwrap();
            assert!(usage["input_tokens"].is_null());
            drop(process);
            let process = fixture.process().await;
            let AttemptStatus::Completed { result } = status(&process, &admission).await else {
                panic!("missing completion")
            };
            assert_eq!(result.receipt.spend_state, SpendState::Settled);
            assert_eq!(serde_json::to_value(result.receipt.usage).unwrap(), usage);
        }
    })
    .await
    .expect("cache-only fixtures must finish within five seconds");
}

#[tokio::test]
async fn regression_egress_classification_rejects_cache_contradictions() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let body = serde_json::json!({"model":"test-model", "answers":{
            "continue":{"type":"noul","noul":0.12},
            "parent":{"type":"choice","choice":"none","probabilities":{"previous":0.25,"none":0.75}},
            "strength":{"type":"score","score":0.75,"probabilities":{"0":0.25,"1":0.75}}
        },"usage":{"input_tokens":100,"cache_hit_tokens":80,"cache_miss_tokens":30}});
        let mut fixture = Fixture::with_http_response(200,body.to_string(),Duration::ZERO,"0",true,false).await;
        fixture.config.routes[0].provider = RouteProvider::JevClassifier {operator:"test".into()};
        let process = fixture.process().await;
        let (admission,payload) = rabbithole_classify_attempt(&fixture);
        let granted = permit(&process,&admission).await;
        let result = dispatched(exchange(&process,inject(admission,payload,granted)).await.unwrap());
        assert_eq!(result.error,Some(EgressError::Provider {status:None}));
        assert!(result.output.is_none());
    }).await.expect("contradictory classification must finish within five seconds");
}

#[tokio::test]
async fn regression_wrong_shape_matches_unparsable_send_count_openai() {
    let wrong_shape = invalid_provider_json_preserves_charge_and_budget(false, "{}").await;
    let unparsable = invalid_provider_json_preserves_charge_and_budget(false, "invalid JSON").await;
    assert_eq!(wrong_shape, unparsable);
}

#[tokio::test]
async fn regression_wrong_shape_matches_unparsable_send_count_anthropic() {
    let wrong_shape = invalid_provider_json_preserves_charge_and_budget(true, "{}").await;
    let unparsable = invalid_provider_json_preserves_charge_and_budget(true, "invalid JSON").await;
    assert_eq!(wrong_shape, unparsable);
}

async fn invalid_provider_json_preserves_charge_and_budget(anthropic: bool, body: &str) -> usize {
    tokio::time::timeout(Duration::from_secs(60), async {
        let mut fixture =
            Fixture::with_http_response(200, body.into(), Duration::ZERO, "0", true, false).await;
        if anthropic {
            fixture.config.routes[0].provider = RouteProvider::AnthropicChat {
                operator: "test".into(),
                thinking: None,
            };
        }
        configure_request_budget(&mut fixture, 3, None);
        let mut process = fixture.process().await;
        let validations = Arc::new(AtomicUsize::new(0));
        let mut failures = Vec::new();
        for call in 0..6 {
            if call == 2 {
                drop(process);
                process = fixture.process().await;
            }
            let observed = validations.clone();
            let invocation = format!("invalid-json-{call}");
            let result =
                validated_request_budget_call(&fixture, &process, &invocation, move |_| {
                    observed.fetch_add(1, Ordering::SeqCst);
                    false
                })
                .await;
            if call < 3 {
                assert_eq!(result.error, Some(EgressError::InvalidProviderJson));
                assert_eq!(result.receipt.spend_state, SpendState::Unknown);
                assert_eq!(result.receipt.status, DispatchStatus::ProviderFailed);
                assert!(result.receipt_persisted && result.output.is_none());
                assert!(result.diagnostics.is_empty());
                let (admission, _) = fixture.attempt(&invocation, 1, 1);
                let AttemptStatus::Failed { result: recovered } =
                    status(&process, &admission).await
                else {
                    panic!("missing failure");
                };
                assert_eq!(
                    serde_json::to_value(&recovered).unwrap(),
                    serde_json::to_value(&result).unwrap()
                );
                failures.push(result);
            } else {
                assert_request_budget_refused(&result);
            }
        }
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 3);
        assert_eq!(
            validations.load(Ordering::SeqCst),
            0,
            "no parsed answer to validate"
        );
        let conn = rusqlite::Connection::open(
            fixture
                .config
                .state_dir
                .join(symbiotic_ai_runtime::QUEUE_DATABASE),
        )
        .unwrap();
        let count: u32 = conn
            .query_row(
                "SELECT failed_sends FROM egress_request_failures",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            count, 3,
            "invalid provider answers consume exactly one failed-send debit"
        );
        drop(process);
        let process = fixture.process().await;
        for (call, result) in failures.iter().enumerate() {
            let (admission, _) = fixture.attempt(&format!("invalid-json-{call}"), 1, 1);
            let AttemptStatus::Failed { result: recovered } = status(&process, &admission).await
            else {
                panic!("missing failure after restart");
            };
            assert_eq!(
                serde_json::to_value(&recovered).unwrap(),
                serde_json::to_value(result).unwrap()
            );
            assert_eq!(
                serde_json::to_value(result.error).unwrap(),
                serde_json::json!("invalid_provider_json")
            );
            let wire = serde_json::to_string(&recovered).unwrap();
            assert!(!wire.contains("invalid JSON"));
            assert!(!wire.contains(SECRET));
        }
        fixture.calls.load(Ordering::SeqCst)
    })
    .await
    .expect("invalid provider answer fixtures hang guard: fixture did not finish within 60 seconds")
}

#[tokio::test]
async fn regression_egress_invalid_retry_hint_preserves_failure_and_diagnostic() {
    tokio::time::timeout(Duration::from_secs(5), async {
        for (code, expected) in [
            (
                429,
                EgressError::RateLimited {
                    retry_after_seconds: None,
                },
            ),
            (504, EgressError::Timeout),
        ] {
            let fixture = Fixture::with_response_options(
                code,
                "private provider body".into(),
                Duration::ZERO,
                "0",
                true,
                false,
                (None, "invalid"),
            )
            .await;
            let process = fixture.process().await;
            let (admission, payload) = fixture.attempt("invalid-hint", 1, 1);
            let granted = permit(&process, &admission).await;
            let result = dispatched(
                exchange(&process, inject(admission.clone(), payload, granted))
                    .await
                    .unwrap(),
            );
            assert_eq!(result.error, Some(expected));
            assert_eq!(
                serde_json::to_value(&result.diagnostics).unwrap(),
                serde_json::json!(["invalid_retry_after"])
            );
            drop(process);
            let process = fixture.process().await;
            let AttemptStatus::Failed { result } = status(&process, &admission).await else {
                panic!("missing failure")
            };
            assert_eq!(result.error, Some(expected));
            assert_eq!(
                serde_json::to_value(result.diagnostics).unwrap(),
                serde_json::json!(["invalid_retry_after"])
            );
        }
    })
    .await
    .expect("invalid retry hint fixtures must finish within five seconds");
}

const ANSWER_RECOVERY_MARKER: &str = "AnswerRecoveryOffMarker7e41";
const CLASSIFY_RECOVERY_MARKER: &str = "0.3141592653589793";

fn state_has_bytes(dir: &std::path::Path, marker: &str) -> bool {
    std::fs::read_dir(dir).unwrap().any(|entry| {
        let path = entry.unwrap().path();
        if path.is_dir() {
            state_has_bytes(&path, marker)
        } else {
            std::fs::read(&path)
                .unwrap()
                .windows(marker.len())
                .any(|bytes| bytes == marker.as_bytes())
        }
    })
}

async fn answer_recovery_fixture(classify: bool) -> Fixture {
    let body = if classify {
        serde_json::json!({"model":"test-model", "id":ANSWER_RECOVERY_MARKER,
            "answers":{"continue":{"type":"noul","noul":0.3141592653589793},
            "parent":{"type":"choice","choice":"none","probabilities":{"previous":0.25,"none":0.75}},
            "strength":{"type":"score","score":0.75,"probabilities":{"0":0.25,"1":0.75}}},
            "usage":{"input_tokens":7,"output_tokens":3,"cost":"0.001"},
            "debug":ANSWER_RECOVERY_MARKER})
    } else {
        serde_json::json!({"model":ANSWER_RECOVERY_MARKER, "id":ANSWER_RECOVERY_MARKER,
            "choices":[{"message":{"content":ANSWER_RECOVERY_MARKER,"reasoning_content":ANSWER_RECOVERY_MARKER},
                "finish_reason":ANSWER_RECOVERY_MARKER}],
            "usage":{"prompt_tokens":7,"completion_tokens":3,"cost":"0.001"}})
    };
    let mut fixture =
        Fixture::with_http_response(200, body.to_string(), Duration::ZERO, "0", true, false).await;
    if classify {
        fixture.config.routes[0].provider = RouteProvider::JevClassifier {
            operator: "test".into(),
        };
    }
    fixture
}

#[tokio::test]
async fn regression_answer_recovery_direct_never_writes_answers_or_usage_text() {
    tokio::time::timeout(Duration::from_secs(10), async {
        for classify in [false, true] {
            for off in [false, true] {
                let mut fixture = answer_recovery_fixture(classify).await;
                if off {
                    fixture.config.routes[0].answer_recovery =
                        symbiotic_credential_process::AnswerRecovery::Off;
                }
                let process = fixture.process().await;
                let (mut admission, payload) = if classify {
                    rabbithole_classify_attempt(&fixture)
                } else {
                    fixture.attempt("answer-recovery", 1, 1)
                };
                // Off does not require a future answer recovery window.
                if off {
                    admission.attempt.recovery_expires_at = admission.attempt.recorded_at;
                    admission = AdmissionKey::new(KEY.to_vec())
                        .unwrap()
                        .sign_attempt(admission.attempt)
                        .unwrap();
                }
                let granted = permit(&process, &admission).await;
                let result = dispatched(
                    exchange(
                        &process,
                        inject(admission.clone(), payload.clone(), granted.clone()),
                    )
                    .await
                    .unwrap(),
                );
                assert!(result.error.is_none() && result.receipt_persisted);
                let live = serde_json::to_string(&result.output).unwrap();
                assert!(live.contains(if classify {
                    CLASSIFY_RECOVERY_MARKER
                } else {
                    ANSWER_RECOVERY_MARKER
                }));
                let reference = result.receipt.reference.clone();
                assert_eq!(result.receipt.usage.input_tokens, Some(7));
                assert_eq!(
                    result.receipt.usage.reported_cost_usd.as_deref(),
                    Some("0.001")
                );
                // Check while WAL and shared-memory files exist, then after reopen too.
                assert_eq!(
                    state_has_bytes(&fixture.config.state_dir, ANSWER_RECOVERY_MARKER),
                    !off
                );
                if classify {
                    assert_eq!(
                        state_has_bytes(&fixture.config.state_dir, CLASSIFY_RECOVERY_MARKER),
                        !off
                    );
                }
                drop(result);
                drop(process);
                let process = fixture.process().await;
                let receipt = match status(&process, &admission).await {
                    AttemptStatus::FinishedWithoutAnswer { receipt } if off => receipt,
                    AttemptStatus::Completed { result } if !off => {
                        assert!(serde_json::to_string(&result.output).unwrap().contains(
                            if classify {
                                CLASSIFY_RECOVERY_MARKER
                            } else {
                                ANSWER_RECOVERY_MARKER
                            }
                        ));
                        result.receipt
                    }
                    _ => panic!("wrong recovered state"),
                };
                assert_eq!(receipt.reference, reference);
                assert_eq!(receipt.status, DispatchStatus::Succeeded);
                assert_eq!(receipt.spend_state, SpendState::Settled);
                assert_eq!(receipt.usage.input_tokens, Some(7));
                assert_eq!(receipt.usage.output_tokens, Some(3));
                assert_eq!(receipt.usage.reported_cost_usd.as_deref(), Some("0.001"));
                // The chat fixture's id and model repeat its reasoning_content, so they are never
                // stored as usage identity, with or without answer recovery (#93).
                let echoes_reasoning = !classify;
                assert_eq!(receipt.usage.response_id.is_none(), off || echoes_reasoning);
                assert_eq!(
                    receipt.usage.served_model.is_none(),
                    off || echoes_reasoning
                );
                assert_eq!(
                    state_has_bytes(&fixture.config.state_dir, ANSWER_RECOVERY_MARKER),
                    !off
                );
                assert!(matches!(
                    exchange(&process, inject(admission, payload, granted)).await,
                    Err(EgressError::PermitRefused)
                ));
                assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
                assert_eq!(ledger_totals(&fixture), (1, 1));
            }
        }
    })
    .await
    .expect("bounded direct answer recovery test");
}

#[tokio::test]
async fn regression_answer_recovery_jobs_never_write_paid_results() {
    tokio::time::timeout(Duration::from_secs(10), async {
        for classify in [false, true] {
            let mut fixture = answer_recovery_fixture(classify).await;
            fixture.config.routes[0].answer_recovery =
                symbiotic_credential_process::AnswerRecovery::Off;
            let client = InProcessEgressClient::new(fixture.process().await);
            let mut input = queued(&fixture, "no-answer-job");
            if classify {
                let (_, payload) = rabbithole_classify_attempt(&fixture);
                input.payload = payload;
                input.admission.attempt.input_digest = input.payload.digest().unwrap();
                input.admission = AdmissionKey::new(KEY.to_vec())
                    .unwrap()
                    .sign_attempt(input.admission.attempt)
                    .unwrap();
            }
            let id = enqueue_id(&client, input.clone()).await;
            let row = wait_job(&client, &id, JobState::Succeeded).await;
            let reference = row.receipt.clone().unwrap();
            assert!(row.result_expired && row.output.is_none());
            assert!(!state_has_bytes(
                &fixture.config.state_dir,
                ANSWER_RECOVERY_MARKER
            ));
            assert!(!state_has_bytes(
                &fixture.config.state_dir,
                CLASSIFY_RECOVERY_MARKER
            ));
            drop(client);
            let client = InProcessEgressClient::new(reopen_jobs(&fixture).await);
            let JobsReply::Completions(page) = job_call(
                &client,
                JobsCommand::Completions {
                    after: None,
                    limit: 1,
                    max_bytes: 65536,
                    wait_seconds: 0,
                },
            )
            .await
            .unwrap() else {
                panic!("completions")
            };
            assert_eq!(page.items.len(), 1);
            assert!(page.items[0].output.is_none());
            assert!(page.items[0].delivery.completion.result_expired);
            assert_eq!(page.items[0].delivery.completion.state, JobState::Succeeded);
            assert_eq!(
                page.items[0].delivery.completion.receipt.as_deref(),
                Some(reference.as_str())
            );
            let conn = rusqlite::Connection::open(
                fixture
                    .config
                    .state_dir
                    .join(symbiotic_ai_runtime::QUEUE_DATABASE),
            )
            .unwrap();
            let (usage, recovery, output): (String, Option<String>, String) = conn
                .query_row(
                    "SELECT usage,recovery,output FROM spend_receipts WHERE reference=?1",
                    [&reference],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .unwrap();
            let usage: serde_json::Value = serde_json::from_str(&usage).unwrap();
            assert_eq!(usage["input_tokens"], 7);
            assert_eq!(usage["output_tokens"], 3);
            assert_eq!(usage["reported_cost_usd"], "0.001");
            assert!(usage["response_id"].is_null() && usage["served_model"].is_null());
            assert!(recovery.is_none());
            assert_eq!(output, r#"{"output_received":true}"#);
            assert_eq!(enqueue_id(&client, input).await, id);
            assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
            assert_eq!(ledger_totals(&fixture), (1, 1));
            assert!(!state_has_bytes(
                &fixture.config.state_dir,
                ANSWER_RECOVERY_MARKER
            ));
        }
    })
    .await
    .expect("bounded queued answer recovery test");
}

#[tokio::test]
async fn regression_answer_recovery_crash_after_answer_before_consumption() {
    for classify in [false, true] {
        let mut fixture = answer_recovery_fixture(classify).await;
        fixture.config.routes[0].answer_recovery =
            symbiotic_credential_process::AnswerRecovery::Off;
        let config = fixture.dir.path().join("config.json");
        std::fs::write(&config, serde_json::to_vec(&fixture.config).unwrap()).unwrap();
        std::fs::set_permissions(&config, std::fs::Permissions::from_mode(0o600)).unwrap();
        let mut child = Child(
            std::process::Command::new(credential_process())
                .arg(config)
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()
                .unwrap(),
        );
        let client = socket::UnixEgressClient {
            path: fixture.config.socket_path.clone(),
            max_frame_bytes: fixture.config.max_frame_bytes,
            timeout: Duration::from_secs(2),
        };
        assert!(matches!(
            wait_for_child_startup(&mut child, || exchange_client(&client, publish_revision(1)))
                .await,
            Reply::GrantRevisionPublished
        ));
        let (admission, payload) = if classify {
            rabbithole_classify_attempt(&fixture)
        } else {
            fixture.attempt("lost-answer", 1, 1)
        };
        let Reply::Permit(grant) =
            exchange_client(&client, Operation::IssuePermit(admission.clone().into()))
                .await
                .unwrap()
        else {
            panic!("permit")
        };
        let peer = send_without_reading(
            &client,
            inject(admission.clone(), payload.clone(), grant.permit.clone()),
        )
        .await;
        tokio::time::timeout(OBSERVATION_HANG_GUARD, async {
            loop {
                let conn = rusqlite::Connection::open(
                    fixture
                        .config
                        .state_dir
                        .join(symbiotic_ai_runtime::QUEUE_DATABASE),
                )
                .unwrap();
                let finished: u8 = conn
                    .query_row("SELECT finished FROM egress_permits", [], |r| r.get(0))
                    .unwrap();
                if finished == 2 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("answer must be durably finished before killing child");
        assert!(!state_has_bytes(
            &fixture.config.state_dir,
            ANSWER_RECOVERY_MARKER
        ));
        child.0.kill().unwrap();
        child.0.wait().unwrap();
        drop(peer);
        let process = fixture.process().await;
        let AttemptStatus::FinishedWithoutAnswer { receipt } = status(&process, &admission).await
        else {
            panic!("finished without answer")
        };
        assert_eq!(receipt.spend_state, SpendState::Settled);
        assert_eq!(receipt.usage.input_tokens, Some(7));
        assert_eq!(ledger_totals(&fixture), (1, 1));
        assert!(matches!(
            exchange(&process, inject(admission, payload, grant.permit)).await,
            Err(EgressError::PermitRefused)
        ));
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
        assert!(!state_has_bytes(
            &fixture.config.state_dir,
            ANSWER_RECOVERY_MARKER
        ));
        assert!(!state_has_bytes(
            &fixture.config.state_dir,
            CLASSIFY_RECOVERY_MARKER
        ));
    }
}

fn configure_request_budget(fixture: &mut Fixture, attempts: u32, renewal: Option<u64>) {
    let mut config = serde_json::to_value(&fixture.config).unwrap();
    config["routes"][0]["request_budget"] =
        serde_json::json!({"attempts": attempts, "renewal_seconds": renewal});
    fixture.config = serde_json::from_value(config).unwrap();
}

async fn request_budget_call(
    fixture: &Fixture,
    process: &CredentialProcess,
    invocation: &str,
    classify: bool,
    different_input: bool,
) -> DispatchResult {
    let (signed, mut payload) = if classify {
        rabbithole_classify_attempt(fixture)
    } else {
        fixture.attempt(invocation, 1, 1)
    };
    if different_input {
        match &mut payload {
            ProviderPayload::Chat(request) => request.messages[0].content.push_str(" different"),
            ProviderPayload::Classify(request) => {
                request
                    .state
                    .insert("different".into(), serde_json::json!(true));
            }
            _ => panic!("unsupported test payload"),
        }
    }
    let mut attempt = signed.attempt;
    attempt.invocation_id = invocation.into();
    attempt.input_digest = payload.digest().unwrap();
    let admission = AdmissionKey::new(KEY.to_vec())
        .unwrap()
        .sign_attempt(attempt)
        .unwrap();
    let granted = permit(process, &admission).await;
    dispatched(
        exchange_wire(process, inject(admission, payload, granted))
            .await
            .unwrap(),
    )
}

async fn validated_request_budget_call<F>(
    fixture: &Fixture,
    process: &CredentialProcess,
    invocation: &str,
    validate: F,
) -> DispatchResult
where
    F: Fn(&ProviderOutput) -> bool + Send + Sync + 'static,
{
    let (admission, payload) = fixture.attempt(invocation, 1, 1);
    let granted = permit(process, &admission).await;
    let client = InProcessEgressClient::new(process.clone()).with_answer_validation(validate);
    dispatched(
        client
            .exchange(Request {
                version: PROTOCOL_VERSION,
                operation: inject(admission, payload, granted),
            })
            .await
            .unwrap()
            .result
            .unwrap(),
    )
}

#[tokio::test]
async fn regression_request_budget_answer_rejections_share_allowance_across_restart() {
    tokio::time::timeout(Duration::from_secs(10), async {
        for restart in [true, false] {
            for (text, reason) in [
                ("{}".to_owned(), "length"),
                (
                    format!("{{\"invalid\":\"{ANSWER_RECOVERY_MARKER}\"}}"),
                    "stop",
                ),
            ] {
                let body = serde_json::json!({
                    "choices": [{"message": {"content": text}, "finish_reason": reason}]
                })
                .to_string();
                let mut fixture =
                    Fixture::with_http_response(200, body, Duration::ZERO, "null", true, false)
                        .await;
                fixture.config.routes[0].answer_recovery =
                    symbiotic_credential_process::AnswerRecovery::Off;
                configure_request_budget(&mut fixture, 3, None);
                let mut process = fixture.process().await;
                let mut results = Vec::new();
                for call in 0..6 {
                    if restart && call == 2 {
                        drop(process);
                        process = fixture.process().await;
                    }
                    let result = validated_request_budget_call(
                        &fixture,
                        &process,
                        &format!("invalid-{call}"),
                        |answer| {
                            matches!(answer, ProviderOutput::Chat {
                                text, finish_reason: Some(FinishReason::Stop),
                            } if text == "valid")
                        },
                    )
                    .await;
                    results.push(result);
                }
                assert_eq!(
                    fixture.calls.load(Ordering::SeqCst),
                    3,
                    "six rejected answers share three sends, restart={restart}"
                );
                for (call, result) in results.iter().enumerate() {
                    if call < 3 {
                        assert_eq!(result.error, Some(EgressError::Provider { status: None }));
                        assert_eq!(result.receipt.status, DispatchStatus::ProviderFailed);
                        assert_eq!(result.receipt.spend_state, SpendState::Unknown);
                        assert!(result.receipt_persisted && result.output.is_none());
                    } else {
                        assert_request_budget_refused(result);
                    }
                }
                let conn = rusqlite::Connection::open(
                    fixture
                        .config
                        .state_dir
                        .join(symbiotic_ai_runtime::QUEUE_DATABASE),
                )
                .unwrap();
                let failures: u32 = conn
                    .query_row(
                        "SELECT failed_sends FROM egress_request_failures",
                        [],
                        |row| row.get(0),
                    )
                    .unwrap();
                assert_eq!(failures, 3);
                let observed: u32 = conn.query_row(
                    "SELECT count(*) FROM spend_receipts WHERE output='{\"output_received\":true}'",
                    [], |row| row.get(0),
                ).unwrap();
                assert_eq!(
                    observed, 3,
                    "rejection preserves paid completion evidence without usage"
                );
                assert!(!state_has_bytes(
                    &fixture.config.state_dir,
                    ANSWER_RECOVERY_MARKER
                ));
                assert!(!state_has_bytes(
                    &fixture.config.state_dir,
                    "private test input"
                ));
                assert!(!state_has_bytes(&fixture.config.state_dir, SECRET));
            }
        }
    })
    .await
    .expect("bounded caller answer rejection regression");
}

#[tokio::test]
async fn regression_request_budget_answer_validation_precedes_completion_and_updates_renewal() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut fixture = Fixture::new(200, "invalid".into(), Duration::ZERO).await;
        configure_request_budget(&mut fixture, 1, Some(60));
        let process = fixture.process().await;
        let database = fixture
            .config
            .state_dir
            .join(symbiotic_ai_runtime::QUEUE_DATABASE);
        let during_validation = database.clone();
        let completion_floor = unix_seconds();
        let result = validated_request_budget_call(&fixture, &process, "rejected", move |_| {
            let conn = rusqlite::Connection::open(&during_validation).unwrap();
            let finished: u32 = conn
                .query_row("SELECT finished FROM egress_permits", [], |row| row.get(0))
                .unwrap();
            assert_eq!(
                finished, 0,
                "validator must run before egress completion commits"
            );
            // Simulate elapsed time without sleeping: completion must replace
            // the older admission timestamp, preserving the existing renewal rule.
            assert_eq!(
                conn.execute(
                    "UPDATE egress_request_failures SET last_failure=?1",
                    [completion_floor - 60]
                )
                .unwrap(),
                1
            );
            false
        })
        .await;
        assert_eq!(result.error, Some(EgressError::Provider { status: None }));
        assert!(result.receipt_persisted);
        let conn = rusqlite::Connection::open(&database).unwrap();
        let (failures, last_failure): (u32, u64) = conn
            .query_row(
                "SELECT failed_sends, last_failure FROM egress_request_failures",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(failures, 1);
        assert!(
            last_failure >= completion_floor,
            "renewal starts at rejected completion"
        );
        assert_request_budget_refused(
            &validated_request_budget_call(&fixture, &process, "refused", |_| {
                panic!("exhausted budget must not reach validation")
            })
            .await,
        );
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
    })
    .await
    .expect("bounded validation completion-order regression");
}

#[tokio::test]
async fn regression_request_budget_answer_validation_success_clears_allowance() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut fixture = Fixture::new(200, "valid".into(), Duration::ZERO).await;
        configure_request_budget(&mut fixture, 1, None);
        let process = fixture.process().await;
        for call in 0..6 {
            let result = validated_request_budget_call(
                &fixture,
                &process,
                &format!("valid-{call}"),
                |answer| matches!(answer, ProviderOutput::Chat { text, .. } if text == "valid"),
            )
            .await;
            assert!(result.error.is_none() && result.output.is_some() && result.receipt_persisted);
        }
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 6);
        let conn = rusqlite::Connection::open(
            fixture
                .config
                .state_dir
                .join(symbiotic_ai_runtime::QUEUE_DATABASE),
        )
        .unwrap();
        let rows: u32 = conn
            .query_row("SELECT count(*) FROM egress_request_failures", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(rows, 0);
    })
    .await
    .expect("bounded accepted answer regression");
}

#[tokio::test]
async fn regression_request_budget_answer_rejected_then_valid_resets_allowance() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut fixture = Fixture::new(200, "answer".into(), Duration::ZERO).await;
        configure_request_budget(&mut fixture, 2, None);
        let process = fixture.process().await;
        let conn = rusqlite::Connection::open(
            fixture
                .config
                .state_dir
                .join(symbiotic_ai_runtime::QUEUE_DATABASE),
        )
        .unwrap();
        for (call, accepted, failures) in
            [(0, false, 1), (1, true, 0), (2, false, 1), (3, false, 2)]
        {
            let result = validated_request_budget_call(
                &fixture,
                &process,
                &format!("sequence-{call}"),
                move |_| accepted,
            )
            .await;
            assert_eq!(result.error.is_none(), accepted);
            assert_eq!(
                result.receipt.spend_state,
                SpendState::Settled,
                "rejection keeps measured usage"
            );
            assert_eq!(result.receipt.usage.input_tokens, Some(7));
            let count: u32 = conn
                .query_row(
                    "SELECT coalesce(sum(failed_sends), 0) FROM egress_request_failures",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(count, failures);
        }
        assert_request_budget_refused(
            &validated_request_budget_call(&fixture, &process, "sequence-exhausted", |_| {
                panic!("no answer to validate when exhausted")
            })
            .await,
        );
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 4);
    })
    .await
    .expect("bounded rejected then accepted answer regression");
}

#[tokio::test]
async fn regression_request_budget_answer_rejections_preserve_per_call_routes() {
    tokio::time::timeout(Duration::from_secs(60), async {
        for configured in [false, true] {
            let mut fixture = Fixture::new(200, "invalid".into(), Duration::ZERO).await;
            if configured {
                configure_request_budget(&mut fixture, 3, Some(0));
            }
            let process = fixture.process().await;
            for call in 0..6 {
                let result = validated_request_budget_call(
                    &fixture,
                    &process,
                    &format!("per-call-{call}"),
                    |_| false,
                )
                .await;
                assert_eq!(result.error, Some(EgressError::Provider { status: None }));
                assert!(result.output.is_none() && result.receipt_persisted);
            }
            assert_eq!(fixture.calls.load(Ordering::SeqCst), 6);
        }
    })
    .await
    .expect("bounded unchanged per-call answer validation regression");
}

fn assert_request_budget_refused(result: &DispatchResult) {
    assert_eq!(
        serde_json::to_value(result.error).unwrap(),
        "request_budget_exhausted"
    );
    assert_eq!(result.receipt.spend_state, SpendState::Released);
    assert!(result.output.is_none());
    assert!(result.receipt_persisted);
}

fn assert_reconciliation_required(result: &DispatchResult) {
    assert_eq!(result.error, Some(EgressError::ReconciliationRequired));
    assert_eq!(result.receipt.spend_state, SpendState::Released);
    assert!(result.output.is_none() && result.receipt_persisted);
}

#[tokio::test]
async fn regression_unresolved_request_different_input_proceeds_until_reconciliation() {
    tokio::time::timeout(Duration::from_secs(5), async {
        for policy in [None, Some(None), Some(Some(0))] {
            let mut fixture = Fixture::new(200, "answer".into(), Duration::ZERO).await;
            if let Some(renewal) = policy {
                configure_request_budget(&mut fixture, 3, renewal);
            }
            let process = fixture.process().await;
            let mut conn = rusqlite::Connection::open(
                fixture
                    .config
                    .state_dir
                    .join(symbiotic_ai_runtime::QUEUE_DATABASE),
            )
            .unwrap();
            conn.execute_batch(
                "CREATE TRIGGER reject_completion BEFORE UPDATE OF finished ON egress_permits
                 BEGIN SELECT RAISE(ABORT, 'synthetic completion failure'); END;",
            )
            .unwrap();
            let original =
                request_budget_call(&fixture, &process, "unfinished", false, false).await;
            assert!(!original.receipt_persisted);
            assert_eq!(original.receipt.spend_state, SpendState::Unknown);
            conn.execute_batch("DROP TRIGGER reject_completion")
                .unwrap();
            let different =
                request_budget_call(&fixture, &process, "different-input", false, true).await;
            assert!(different.error.is_none() && different.output.is_some());
            assert_reconciliation_required(
                &request_budget_call(&fixture, &process, "same-input", false, false).await,
            );
            assert_eq!(fixture.calls.load(Ordering::SeqCst), 2);
            // Existing canonical reconciliation resolves the original reservation.
            // Reconciliation writes after reading the receipt; take the writer first,
            // as production does, so a concurrent runner cannot invalidate the snapshot.
            let tx = conn
                .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                .unwrap();
            symbiotic_ai_runtime::spend::SqliteSpendLedger::finish_in(
                &tx,
                &original.receipt.reference,
                SpendState::Settled,
                Some(original.receipt.usage.clone()),
                None,
            )
            .unwrap();
            tx.commit().unwrap();
            let resolved = request_budget_call(&fixture, &process, "resolved", false, false).await;
            assert!(resolved.error.is_none() && resolved.output.is_some());
            assert_eq!(fixture.calls.load(Ordering::SeqCst), 3);
        }
    })
    .await
    .expect("bounded unresolved request identity regression");
}

#[tokio::test]
async fn regression_request_budget_durable_completion_failure_keeps_allowance_consumed() {
    tokio::time::timeout(Duration::from_secs(10), async {
        for (status_code, pre_send_failure, reject_answer) in [
            (400, false, false),
            (200, false, false),
            (400, true, false),
            (200, false, true),
        ] {
            for restart in [false, true] {
                let mut fixture = Fixture::new(status_code, "answer".into(), Duration::ZERO).await;
                configure_request_budget(&mut fixture, 3, None);
                if pre_send_failure {
                    // A trusted pre-send failure must keep its debit if the
                    // transaction that would undo it cannot commit.
                    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                    fixture.config.routes[0].destination =
                        format!("http://{}/v1", listener.local_addr().unwrap());
                    drop(listener);
                }
                let mut process = fixture.process().await;
                let conn = rusqlite::Connection::open(
                    fixture
                        .config
                        .state_dir
                        .join(symbiotic_ai_runtime::QUEUE_DATABASE),
                )
                .unwrap();
                conn.execute_batch(
                    "CREATE TRIGGER reject_completion BEFORE UPDATE OF finished ON egress_permits
                     BEGIN SELECT RAISE(ABORT, 'synthetic completion failure'); END;",
                )
                .unwrap();
                let result = if reject_answer {
                    validated_request_budget_call(&fixture, &process, "unfinished", |_| false).await
                } else {
                    request_budget_call(&fixture, &process, "unfinished", false, false).await
                };
                assert!(!result.receipt_persisted);
                assert_eq!(result.receipt.spend_state, SpendState::Unknown);
                assert_eq!(
                    result.output.is_some(),
                    status_code == 200 && !reject_answer
                );
                if reject_answer {
                    assert_eq!(result.error, Some(EgressError::Provider { status: None }));
                }
                conn.execute_batch("DROP TRIGGER reject_completion")
                    .unwrap();
                if restart {
                    drop(process);
                    process = fixture.process().await;
                }
                assert_reconciliation_required(
                    &request_budget_call(&fixture, &process, "different-invocation", false, false)
                        .await,
                );
                assert_eq!(
                    fixture.calls.load(Ordering::SeqCst),
                    usize::from(!pre_send_failure)
                );
            }
        }
    })
    .await
    .expect("bounded completion-failure budget regression");
}

#[tokio::test]
async fn regression_request_budget_durable_admission_write_failure_refuses_before_http() {
    tokio::time::timeout(Duration::from_secs(5), async {
        for existing_budget in [false, true] {
            let mut fixture = Fixture::new(400, "rejected".into(), Duration::ZERO).await;
            configure_request_budget(&mut fixture, 2, None);
            let process = fixture.process().await;
            if existing_budget {
                let result = request_budget_call(&fixture, &process, "first", false, false).await;
                assert!(matches!(
                    result.error,
                    Some(EgressError::Provider { status: Some(400) })
                ));
            }
            let conn = rusqlite::Connection::open(
                fixture
                    .config
                    .state_dir
                    .join(symbiotic_ai_runtime::QUEUE_DATABASE),
            )
            .unwrap();
            let operation = if existing_budget { "UPDATE" } else { "INSERT" };
            conn.execute_batch(&format!(
                "CREATE TRIGGER reject_admission BEFORE {operation} ON egress_request_failures
                 BEGIN SELECT RAISE(ABORT, 'synthetic admission failure'); END;",
            ))
            .unwrap();
            let result = request_budget_call(&fixture, &process, "refused", false, false).await;
            assert_eq!(result.error, Some(EgressError::StateUnavailable));
            assert_eq!(result.receipt.spend_state, SpendState::Released);
            assert!(result.receipt_persisted);
            assert_eq!(
                fixture.calls.load(Ordering::SeqCst),
                usize::from(existing_budget)
            );
        }
    })
    .await
    .expect("bounded admission-write budget regression");
}

#[tokio::test]
async fn regression_request_budget_durable_crash_after_send_blocks_different_invocation() {
    tokio::time::timeout(Duration::from_secs(60), async {
        let response_gate = Arc::new(tokio::sync::Semaphore::new(0));
        let mut fixture = Fixture::with_response_gate(
            400,
            "rejected".into(),
            Duration::ZERO,
            "null",
            false,
            false,
            Some(response_gate.clone()),
        )
        .await;
        configure_request_budget(&mut fixture, 3, None);
        fixture.config.routes[0].timeout_seconds = 5;
        let config = fixture.dir.path().join("config.json");
        std::fs::write(&config, serde_json::to_vec(&fixture.config).unwrap()).unwrap();
        std::fs::set_permissions(&config, std::fs::Permissions::from_mode(0o600)).unwrap();
        let mut child = Child(
            std::process::Command::new(credential_process())
                .arg(config)
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()
                .unwrap(),
        );
        let client = socket::UnixEgressClient {
            path: fixture.config.socket_path.clone(),
            max_frame_bytes: fixture.config.max_frame_bytes,
            timeout: Duration::from_secs(2),
        };
        assert!(matches!(
            wait_for_child_startup(&mut child, || exchange_client(&client, publish_revision(1)))
                .await,
            Reply::GrantRevisionPublished
        ));
        let (admission, payload) = fixture.attempt("crashed", 1, 1);
        let Reply::Permit(grant) =
            exchange_client(&client, Operation::IssuePermit(admission.clone().into()))
                .await
                .unwrap()
        else {
            panic!("permit")
        };
        let peer =
            send_without_reading(&client, inject(admission.clone(), payload, grant.permit)).await;
        tokio::time::timeout(OBSERVATION_HANG_GUARD, async {
            while fixture.calls.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("provider must observe the send before the crash");
        child.0.kill().unwrap();
        child.0.wait().unwrap();
        drop(peer);
        response_gate.add_permits(1);
        let process = fixture.process().await;
        let AttemptStatus::Dispatched { receipt } = status(&process, &admission).await else {
            panic!("crashed attempt must remain uncertain")
        };
        assert_eq!(receipt.spend_state, SpendState::Unknown);
        assert_eq!(receipt.attempt_id, admission.attempt.attempt_id());
        let recovered = permit(&process, &admission).await;
        assert_eq!(
            recovered.attempt_digest,
            digest(&admission.attempt).unwrap()
        );
        assert_reconciliation_required(
            &request_budget_call(&fixture, &process, "after-crash", false, false).await,
        );
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
        let conn = rusqlite::Connection::open(
            fixture
                .config
                .state_dir
                .join(symbiotic_ai_runtime::QUEUE_DATABASE),
        )
        .unwrap();
        let failures: u32 = conn
            .query_row(
                "SELECT failed_sends FROM egress_request_failures",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(failures, 1, "two attempts remain unused");
        let AttemptStatus::Dispatched { receipt: original } = status(&process, &admission).await
        else {
            panic!("original attempt must remain recoverable")
        };
        assert_eq!(original.reference, receipt.reference);
        assert_eq!(original.spend_state, SpendState::Unknown);
    })
    .await
    .expect("bounded crash/restart budget regression");
}

#[tokio::test]
async fn regression_request_budget_failed_sends_survive_restart_without_content() {
    tokio::time::timeout(Duration::from_secs(15), async {
        for restart in [false, true] {
            let mut fixture =
                Fixture::new(400, ANSWER_RECOVERY_MARKER.into(), Duration::ZERO).await;
            fixture.config.routes[0].provider = RouteProvider::JevClassifier {
                operator: "test".into(),
            };
            fixture.config.routes[0].answer_recovery =
                symbiotic_credential_process::AnswerRecovery::Off;
            configure_request_budget(&mut fixture, 3, None);
            let mut process = fixture.process().await;
            for call in 0..6 {
                if restart && call == 3 {
                    drop(process);
                    process = fixture.process().await;
                }
                let result =
                    request_budget_call(&fixture, &process, &format!("budget-{call}"), true, false)
                        .await;
                if call < 3 {
                    assert!(matches!(
                        result.error,
                        Some(EgressError::Provider { status: Some(400) })
                    ));
                    assert_eq!(result.receipt.spend_state, SpendState::Unknown);
                } else {
                    assert_request_budget_refused(&result);
                }
            }
            assert_eq!(fixture.calls.load(Ordering::SeqCst), 3);
            assert!(!state_has_bytes(&fixture.config.state_dir, "hello"));
            assert!(!state_has_bytes(
                &fixture.config.state_dir,
                "Does m000 continue"
            ));
            assert!(!state_has_bytes(
                &fixture.config.state_dir,
                ANSWER_RECOVERY_MARKER
            ));
            assert!(!state_has_bytes(&fixture.config.state_dir, SECRET));
        }
    })
    .await
    .expect("bounded request budget restart regression");
}

#[tokio::test]
async fn regression_request_budget_zero_renewal_and_omission_send_every_call() {
    tokio::time::timeout(Duration::from_secs(15), async {
        for configured in [false, true] {
            let mut fixture = Fixture::new(400, "rejected".into(), Duration::ZERO).await;
            if configured {
                configure_request_budget(&mut fixture, 3, Some(0));
            }
            let process = fixture.process().await;
            for call in 0..6 {
                let result =
                    request_budget_call(&fixture, &process, &format!("renew-{call}"), false, false)
                        .await;
                assert!(matches!(
                    result.error,
                    Some(EgressError::Provider { status: Some(400) })
                ));
            }
            assert_eq!(fixture.calls.load(Ordering::SeqCst), 6);
        }
    })
    .await
    .expect("bounded per-call budget regression");
}

#[tokio::test]
async fn regression_request_budget_input_and_rotated_credentials_have_fresh_budgets() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let mut fixture = Fixture::new(401, "revoked".into(), Duration::ZERO).await;
        configure_request_budget(&mut fixture, 2, None);
        let process = fixture.process().await;
        for call in 0..3 {
            let result =
                request_budget_call(&fixture, &process, &format!("revoked-{call}"), false, false)
                    .await;
            if call < 2 {
                assert!(matches!(
                    result.error,
                    Some(EgressError::Provider { status: Some(401) })
                ));
                assert_eq!(result.receipt.spend_state, SpendState::Unknown);
            } else {
                assert_request_budget_refused(&result);
            }
        }
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 2);
        for call in 0..3 {
            let result = request_budget_call(
                &fixture,
                &process,
                &format!("different-{call}"),
                false,
                true,
            )
            .await;
            if call == 2 {
                assert_request_budget_refused(&result);
            }
        }
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 4);
        std::fs::write(
            fixture.dir.path().join("provider"),
            format!("{SECRET}-rotated"),
        )
        .unwrap();
        for call in 0..3 {
            let result =
                request_budget_call(&fixture, &process, &format!("rotated-{call}"), false, false)
                    .await;
            if call == 2 {
                assert_request_budget_refused(&result);
            }
        }
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 6);
    })
    .await
    .expect("bounded request budget key regression");
}

#[tokio::test]
async fn regression_request_budget_unrelated_routes_complete_while_provider_is_blocked() {
    tokio::time::timeout(Duration::from_secs(5), async {
        for shared_account in [false, true] {
            let gate = Arc::new(tokio::sync::Semaphore::new(0));
            let mut a = Fixture::with_response_gate(
                200,
                "answer A".into(),
                Duration::ZERO,
                "null",
                false,
                false,
                Some(gate.clone()),
            )
            .await;
            let mut b = Fixture::new(200, "answer B".into(), Duration::ZERO).await;
            configure_request_budget(&mut a, 3, None);
            configure_request_budget(&mut b, 3, None);
            a.config.routes[0].timeout_seconds = 30;
            b.config.routes[0].timeout_seconds = 30;
            b.config.routes[0].route = "independent".into();
            if !shared_account {
                b.config.routes[0].account = "independent".into();
            }
            a.config.routes.push(b.config.routes[0].clone());
            let process = a.process().await;
            let (admission, payload) = a.attempt("blocked-provider", 1, 1);
            let granted = permit(&process, &admission).await;
            let first = tokio::spawn({
                let process = process.clone();
                async move {
                    dispatched(
                        exchange_wire(&process, inject(admission, payload, granted))
                            .await
                            .unwrap(),
                    )
                }
            });
            a.arrivals.acquire().await.unwrap().forget();

            let (signed, payload) = b.attempt("independent-provider", 1, 1);
            let mut attempt = signed.attempt;
            attempt.route = b.config.routes[0].route.clone();
            let admission = AdmissionKey::new(KEY.to_vec())
                .unwrap()
                .sign_attempt(attempt)
                .unwrap();
            let granted = permit(&process, &admission).await;
            let result = dispatched(
                exchange_wire(&process, inject(admission, payload, granted))
                    .await
                    .unwrap(),
            );
            assert_eq!(result.error, None);
            assert!(result.receipt_persisted);
            assert_eq!(b.calls.load(Ordering::SeqCst), 1);
            assert!(
                !first.is_finished(),
                "A must still be waiting on its response barrier"
            );
            gate.add_permits(1);
            let result = first.await.unwrap();
            assert_eq!(result.error, None);
            assert!(result.receipt_persisted);
        }
    })
    .await
    .expect("bounded unrelated request budget concurrency regression");
}

#[tokio::test]
async fn regression_request_budget_concurrent_dispatches_cannot_overspend() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let mut fixture = Fixture::with_response_gate(
            400,
            "rejected".into(),
            Duration::ZERO,
            "null",
            false,
            false,
            Some(gate.clone()),
        )
        .await;
        configure_request_budget(&mut fixture, 3, None);
        fixture.config.routes[0].timeout_seconds = 30;
        let process = fixture.process().await;
        let calls = async {
            tokio::join!(
                request_budget_call(&fixture, &process, "parallel-a", false, false),
                request_budget_call(&fixture, &process, "parallel-b", false, false),
                request_budget_call(&fixture, &process, "parallel-c", false, false),
                request_budget_call(&fixture, &process, "parallel-d", false, false),
                request_budget_call(&fixture, &process, "parallel-e", false, false),
                request_budget_call(&fixture, &process, "parallel-f", false, false),
            )
        };
        let release = async {
            for admitted in 1..=3 {
                fixture.arrivals.acquire().await.unwrap().forget();
                assert_eq!(fixture.calls.load(Ordering::SeqCst), admitted);
                let db = rusqlite::Connection::open(
                    fixture
                        .config
                        .state_dir
                        .join(symbiotic_ai_runtime::QUEUE_DATABASE),
                )
                .unwrap();
                let debits: usize = db
                    .query_row(
                        "SELECT failed_sends FROM egress_request_failures",
                        [],
                        |row| row.get(0),
                    )
                    .unwrap();
                assert_eq!(
                    debits, admitted,
                    "same-key admission must wait for completion"
                );
                gate.add_permits(1);
            }
        };
        let ((a, b, c, d, e, f), ()) = tokio::join!(calls, release);
        let results = [a, b, c, d, e, f];
        assert_eq!(
            results
                .iter()
                .filter(|r| r.receipt.spend_state == SpendState::Unknown)
                .count(),
            3
        );
        assert_eq!(
            results
                .iter()
                .filter(|r| r.error == Some(EgressError::RequestBudgetExhausted))
                .count(),
            3
        );
        for result in results
            .iter()
            .filter(|r| r.error == Some(EgressError::RequestBudgetExhausted))
        {
            assert_request_budget_refused(result);
        }
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 3);
    })
    .await
    .expect("bounded concurrent request budget regression");
}

#[tokio::test]
async fn regression_request_budget_positive_renewal_and_success_clear() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let mut fixture = Fixture::new(400, "rejected".into(), Duration::ZERO).await;
        configure_request_budget(&mut fixture, 2, Some(60));
        let process = fixture.process().await;
        for call in 0..2 {
            request_budget_call(&fixture, &process, &format!("initial-{call}"), false, false).await;
        }
        let database = fixture
            .config
            .state_dir
            .join(symbiotic_ai_runtime::QUEUE_DATABASE);
        let conn = rusqlite::Connection::open(&database).unwrap();
        // Simulate time, including rollback, without waiting for production renewal.
        conn.execute(
            "UPDATE egress_request_failures SET last_failure=?1",
            [unix_seconds() + 3600],
        )
        .unwrap();
        assert_request_budget_refused(
            &request_budget_call(&fixture, &process, "rollback", false, false).await,
        );
        conn.execute(
            "UPDATE egress_request_failures SET last_failure=?1",
            [unix_seconds() - 60],
        )
        .unwrap();
        let renewed = request_budget_call(&fixture, &process, "renewed", false, false).await;
        assert!(matches!(
            renewed.error,
            Some(EgressError::Provider { status: Some(400) })
        ));
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 3);
        let count: u32 = conn
            .query_row(
                "SELECT failed_sends FROM egress_request_failures",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
        drop(process);
        // Change the configured destination of the same route to a successful mock.
        // Its failure key is still the route+credential+input, independent of revision.
        let success = Fixture::new(200, ANSWER_RECOVERY_MARKER.into(), Duration::ZERO).await;
        fixture.config.routes[0].destination = success.config.routes[0].destination.clone();
        fixture.config.routes[0].answer_recovery =
            symbiotic_credential_process::AnswerRecovery::Off;
        let process = fixture.process().await;
        let result = request_budget_call(&fixture, &process, "success", false, false).await;
        assert!(result.error.is_none());
        assert!(result.output.is_some());
        assert_eq!(success.calls.load(Ordering::SeqCst), 1);
        let rows: u32 = conn
            .query_row("SELECT count(*) FROM egress_request_failures", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(rows, 0);
        assert!(!state_has_bytes(
            &fixture.config.state_dir,
            ANSWER_RECOVERY_MARKER
        ));
        assert!(!state_has_bytes(
            &fixture.config.state_dir,
            "private test input"
        ));
    })
    .await
    .expect("bounded renewal and success regression");
}

#[tokio::test]
async fn regression_request_budget_config_omission_and_zero_refusal() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut fixture = Fixture::new(200, "ok".into(), Duration::ZERO).await;
        let original = serde_json::to_value(&fixture.config.routes[0]).unwrap();
        assert!(original.get("request_budget").is_none());
        let mut explicit_none = original.clone();
        explicit_none["request_budget"] = serde_json::Value::Null;
        let route: RouteConfig = serde_json::from_value(explicit_none).unwrap();
        assert_eq!(serde_json::to_value(&route).unwrap(), original);
        configure_request_budget(&mut fixture, 0, None);
        assert!(matches!(
            CredentialProcess::open(fixture.config.clone()),
            Err(EgressError::InvalidRequest)
        ));
        assert!(!fixture.config.state_dir.exists());
    })
    .await
    .expect("bounded request budget configuration regression");
}

#[tokio::test]
async fn regression_request_budget_pre_send_failures_leave_allowance_unused() {
    tokio::time::timeout(Duration::from_secs(15), async {
        for credential_failure in [false, true] {
            let mut fixture = Fixture::new(400, "rejected".into(), Duration::ZERO).await;
            configure_request_budget(&mut fixture, 1, None);
            let destination = fixture.config.routes[0].destination.clone();
            if credential_failure {
                std::fs::remove_file(fixture.dir.path().join("provider")).unwrap();
            } else {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                fixture.config.routes[0].destination =
                    format!("http://{}/v1", listener.local_addr().unwrap());
                drop(listener);
            }
            let process = fixture.process().await;
            for call in 0..if credential_failure { 2 } else { 1 } {
                let result = request_budget_call(
                    &fixture,
                    &process,
                    &format!("unsent-{call}"),
                    false,
                    false,
                )
                .await;
                assert!(result.error.is_some());
                assert_ne!(result.error, Some(EgressError::RequestBudgetExhausted));
                assert_eq!(result.receipt.spend_state, SpendState::Released);
                assert!(result.receipt_persisted);
            }
            assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
            let conn = rusqlite::Connection::open(
                fixture
                    .config
                    .state_dir
                    .join(symbiotic_ai_runtime::QUEUE_DATABASE),
            )
            .unwrap();
            let rows: u32 = conn
                .query_row("SELECT count(*) FROM egress_request_failures", [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(rows, 0);
            // A connection failure starts the existing provider cooldown. Its
            // empty budget table proves the allowance was not consumed without
            // waiting for, bypassing or changing that independent runtime policy.
            if !credential_failure {
                continue;
            }
            drop(process);
            fixture.config.routes[0].destination = destination;
            if credential_failure {
                let path = fixture.dir.path().join("provider");
                std::fs::write(&path, SECRET).unwrap();
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
            }
            let process = fixture.process().await;
            let sent = request_budget_call(&fixture, &process, "first-send", false, false).await;
            assert!(matches!(
                sent.error,
                Some(EgressError::Provider { status: Some(400) })
            ));
            assert_request_budget_refused(
                &request_budget_call(&fixture, &process, "exhausted", false, false).await,
            );
            assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
        }
    })
    .await
    .expect("bounded pre-send budget regression");
}

#[tokio::test]
async fn regression_request_budget_lookup_failure_refuses_before_http() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut fixture = Fixture::new(400, "rejected".into(), Duration::ZERO).await;
        configure_request_budget(&mut fixture, 2, None);
        let process = fixture.process().await;
        let database = fixture
            .config
            .state_dir
            .join(symbiotic_ai_runtime::QUEUE_DATABASE);
        let conn = rusqlite::Connection::open(database).unwrap();
        conn.execute("DROP TABLE egress_request_failures", [])
            .unwrap();
        let result = request_budget_call(&fixture, &process, "unavailable", false, false).await;
        assert_eq!(result.error, Some(EgressError::StateUnavailable));
        assert_eq!(result.receipt.spend_state, SpendState::Released);
        assert!(result.receipt_persisted);
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
        drop(process);
        assert!(matches!(
            CredentialProcess::open(fixture.config.clone()),
            Err(EgressError::StateUnavailable)
        ));
        let exists: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='egress_request_failures')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(
            !exists,
            "restart must not recreate lost canonical budget state"
        );
    })
    .await
    .expect("bounded budget storage-failure regression");
}

#[tokio::test]
async fn logging_startup_prefers_environment_directives_and_refuses_invalid_ones() {
    use std::io::Read;
    let dir = tempfile::tempdir().unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    for (name, value) in [
        ("admission", KEY),
        ("provider", b"synthetic-passthrough-secret".as_slice()),
    ] {
        let path = dir.path().join(name);
        std::fs::write(&path, value).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    let mut config = process_config(dir.path());
    let passthrough_path = dir.path().join("logging.sock");
    // An unmatched passthrough request emits completion without a provider call.
    config.passthrough = Some(
        serde_json::from_value(serde_json::json!({
            "socket_path": passthrough_path, "max_connections": 1,
            "routes": [{"name": "logging", "tenant": "tenant", "account": "logging",
                "account_sharing_key": null, "method": "post", "path": "/v1/responses",
                "destination": "https://provider.invalid/answer",
                "secret": {"backend": "owner_only_file", "path": dir.path().join("provider")},
                "stream": "openai_responses", "forward_headers": [],
                "max_request_bytes": 1024, "max_response_bytes": 1024,
                "max_in_flight": 1, "requests_per_minute": null, "idle_timeout_seconds": 1}]
        }))
        .unwrap(),
    );
    let start = |configured: Option<&str>, environment: Option<&str>| {
        let mut config = config.clone();
        config.log = configured.map(str::to_owned);
        let path = dir.path().join("logging-config.json");
        std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        let mut command = std::process::Command::new(credential_process());
        command.env_remove(symbiotic_core::logging::LOG_ENV);
        if let Some(value) = environment {
            command.env(symbiotic_core::logging::LOG_ENV, value);
        }
        Child(
            command
                .arg(&path)
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .unwrap(),
        )
    };
    let refused = |mut child: Child| async move {
        let status = tokio::time::timeout(OBSERVATION_HANG_GUARD, async {
            loop {
                if let Some(status) = child.0.try_wait().unwrap() {
                    break status;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("invalid directives must refuse startup");
        let mut stderr = String::new();
        child
            .0
            .stderr
            .take()
            .unwrap()
            .read_to_string(&mut stderr)
            .unwrap();
        assert!(!status.success());
        assert!(stderr.contains("invalid logging directives"), "{stderr}");
    };
    refused(start(Some("invalid["), None)).await;
    refused(start(Some("info"), Some("invalid["))).await;
    refused(start(Some("invalid["), Some(""))).await;
    for (configured, environment, info_expected) in [
        (Some("invalid["), Some("warn"), false),
        (Some("info"), Some("warn"), false),
        (Some("warn"), None, false),
        (None, None, true),
    ] {
        let mut child = start(configured, environment);
        let mut caller = wait_for_child_startup(&mut child, || async {
            tokio::net::UnixStream::connect(&passthrough_path)
                .await
                .map_err(|_| EgressError::Transport)
        })
        .await;
        caller.write_all(b"POST /unknown HTTP/1.1\r\nHost: ignored.invalid\r\nContent-Length: 0\r\nx-symbiotic-request-id: synthetic-logging\r\n\r\n").await.unwrap();
        caller.shutdown().await.unwrap();
        let mut response = Vec::new();
        tokio::time::timeout(OBSERVATION_HANG_GUARD, caller.read_to_end(&mut response))
            .await
            .unwrap()
            .unwrap();
        assert!(response.starts_with(b"HTTP/1.1 404"));
        child.0.kill().unwrap();
        child.0.wait().unwrap();
        let mut stderr = String::new();
        child
            .0
            .stderr
            .take()
            .unwrap()
            .read_to_string(&mut stderr)
            .unwrap();
        let lines: Vec<serde_json::Value> = stderr
            .lines()
            .map(|line| serde_json::from_str(line).expect("production diagnostics are JSON"))
            .collect();
        let completed: Vec<_> = lines
            .iter()
            .filter(|line| line["message"] == "request completed")
            .collect();
        if info_expected {
            assert_eq!(completed.len(), 1, "{stderr}");
            assert_eq!(
                (
                    &completed[0]["level"],
                    &completed[0]["outcome"],
                    &completed[0]["spans"][0]["request_id"]
                ),
                (
                    &serde_json::json!("INFO"),
                    &serde_json::json!("route_not_found"),
                    &serde_json::json!("synthetic-logging")
                ),
            );
        } else {
            assert!(completed.is_empty(), "{stderr}");
            assert!(lines.iter().all(|line| line["level"] != "INFO"), "{stderr}");
        }
    }
}
