#![cfg(unix)]
//! Public deployment checks and real executable health/shutdown. Provider fixture
//! tests live in the library's passthrough_tests module, keeping its wire override test-only.
use serde_json::{Value, json};
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};
use symbiotic_gateway::{Gateway, GatewayConfig};

fn config(dir: &std::path::Path) -> GatewayConfig {
    serde_json::from_value(json!({"state_dir":dir.join("state"),"core_socket_path":dir.join("core.sock"),"max_secret_bytes":4096,"max_header_bytes":8388608,"io_timeout_seconds":2,"passthrough":{"socket_path":dir.join("pass.sock"),"health_path":dir.join("health.json"),"max_connections":8,"routes":[{"name":"responses","tenant":"tenant","account":"account","account_sharing_key":null,"method":"post","path":"/v1/responses","destination":"https://provider.invalid/answer","secret":{"backend":"owner_only_file","path":dir.join("key")},"stream":"openai_responses","forward_headers":["content-type"],"return_headers":["x-codex-turn-state"],"max_request_bytes":4096,"max_response_bytes":4096,"max_in_flight":2,"requests_per_minute":null,"idle_timeout_seconds":1}]}})).unwrap()
}
#[test]
fn passthrough_contract_validation_refuses_ambiguous_routes_destinations_and_limits_without_io() {
    let dir = tempfile::tempdir().unwrap();
    let good = config(dir.path());
    let route = &good.passthrough.routes[0];
    assert_eq!(
        (
            route.rate_burst_seconds,
            route.max_held_event_bytes,
            good.validate_routes()
        ),
        (0, 67_108_864, Ok(())),
    );
    let value = serde_json::to_value(&good).unwrap();
    for (field, bad) in [
        ("destination", json!("http://127.0.0.1/answer")),
        ("destination", json!("https://user@provider.invalid/answer")),
        (
            "destination",
            json!("https://provider.invalid/answer?query"),
        ),
        (
            "destination",
            json!("https://provider.invalid/answer#fragment"),
        ),
        ("path", json!("/v1/responses?query")),
        ("max_in_flight", json!(0)),
        ("max_in_flight", json!(usize::MAX)),
        ("max_request_bytes", json!(0)),
        ("max_held_event_bytes", json!(0)),
        ("idle_timeout_seconds", json!(0)),
        ("requests_per_minute", json!(0)),
        ("forward_headers", json!(["authorization"])),
        ("forward_headers", json!(["cookie"])),
        ("forward_headers", json!(["X-Forwarded-For"])),
        ("return_headers", json!(["set-cookie"])),
        ("return_headers", json!(["location"])),
    ] {
        let mut bad_config = value.clone();
        bad_config["passthrough"]["routes"][0][field] = bad;
        let bad_config: GatewayConfig = serde_json::from_value(bad_config).unwrap();
        assert!(bad_config.validate_routes().is_err(), "accepted {field}");
        assert!(Gateway::open(bad_config).is_err());
    }
    let mut invalid = good.clone();
    invalid.passthrough.max_connections = usize::MAX;
    assert_eq!(
        invalid.validate_routes(),
        Err(symbiotic_gateway::GatewayError::InvalidRequest)
    );
    for (field, names) in [
        (
            "forward_headers",
            &[
                "authorization",
                "cookie",
                "host",
                "content-length",
                "transfer-encoding",
                "connection",
                "te",
                "upgrade",
                "proxy-authorization",
            ][..],
        ),
        (
            "return_headers",
            &[
                "set-cookie",
                "cookie",
                "authorization",
                "proxy-authenticate",
                "www-authenticate",
                "location",
                "content-length",
                "transfer-encoding",
                "connection",
                "keep-alive",
                "te",
                "trailer",
                "upgrade",
                "content-encoding",
                "host",
            ][..],
        ),
    ] {
        for name in names {
            let mut invalid = value.clone();
            invalid["passthrough"]["routes"][0][field] = json!([name]);
            assert_eq!(
                serde_json::from_value::<GatewayConfig>(invalid)
                    .unwrap()
                    .validate_routes(),
                Err(symbiotic_gateway::GatewayError::InvalidRequest),
                "accepted {field}: {name}"
            );
        }
    }
    for field in ["name", "path", "max_in_flight", "requests_per_minute"] {
        let mut bad = good.clone();
        let routes = &mut bad.passthrough.routes;
        let mut second = routes[0].clone();
        second.name = "other".into();
        second.path = "/other".into();
        match field {
            "name" => second.name = routes[0].name.clone(),
            "path" => second.path = routes[0].path.clone(),
            "max_in_flight" => second.max_in_flight = 3,
            _ => second.requests_per_minute = Some(1),
        }
        routes.push(second);
        assert!(
            bad.validate_routes().is_err(),
            "accepted conflicting {field}"
        );
    }
    assert_eq!(
        fs::read_dir(dir.path()).unwrap().count(),
        0,
        "validation touched state"
    );
}
fn write(path: &std::path::Path, bytes: &[u8]) {
    fs::write(path, bytes).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
}
struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
#[tokio::test]
async fn executable_publishes_health_and_sigterm_publishes_stopping_before_exit() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let dir = tempfile::tempdir().unwrap();
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let config = config(dir.path());
    write(&dir.path().join("key"), b"synthetic-executable-static-key");
    let config_path = dir.path().join("config.json");
    write(&config_path, &serde_json::to_vec(&config).unwrap());
    let binary = std::env::var_os("CARGO_BIN_EXE_symbiotic-gateway").unwrap_or_else(|| {
        std::env::current_exe()
            .unwrap()
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("symbiotic-gateway")
            .into_os_string()
    });
    let mut child = ChildGuard(
        Command::new(binary)
            .arg(&config_path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(3);
    let health_path = dir.path().join("health.json");
    let running = loop {
        if let Ok(bytes) = fs::read(&health_path) {
            break serde_json::from_slice::<Value>(&bytes).unwrap();
        }
        assert!(Instant::now() < deadline, "health startup deadline");
        assert!(
            child.0.try_wait().unwrap().is_none(),
            "child exited before health"
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    };
    assert_eq!(running["version"], 1);
    assert_eq!(running["state"], "running");
    assert_eq!(running["routes"][0]["login"], "static");
    assert_eq!(
        fs::metadata(&health_path).unwrap().permissions().mode() & 0o777,
        0o640
    );
    let mut socket = tokio::net::UnixStream::connect(dir.path().join("pass.sock"))
        .await
        .unwrap();
    socket
        .write_all(b"GET /unknown HTTP/1.1\r\nHost: caller\r\nContent-Length: 0\r\n\r\n")
        .await
        .unwrap();
    let mut reply = Vec::new();
    socket.read_to_end(&mut reply).await.unwrap();
    assert!(reply.starts_with(b"HTTP/1.1 404 "));
    assert!(
        String::from_utf8(reply)
            .unwrap()
            .contains("route_not_found")
    );
    // SAFETY: this is the exact child started by this test, never a shared process.
    assert_eq!(unsafe { libc::kill(child.0.id() as i32, libc::SIGTERM) }, 0);
    loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            assert!(status.success());
            break;
        }
        assert!(Instant::now() < deadline, "shutdown deadline");
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    let bytes = fs::read(&health_path).unwrap();
    let health: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(health["state"], "stopping");
    assert!(!String::from_utf8(bytes).unwrap().contains("synthetic"));
    assert_eq!(
        fs::metadata(&health_path).unwrap().permissions().mode() & 0o777,
        0o640
    );
}

fn account(
    policy: symbiotic_ai_runtime::ModelQueueConfig,
) -> symbiotic_ai_runtime::AccountAdmission {
    use symbiotic_ai_runtime::{BindingIdentity, Runtime, RuntimeConfig};
    Runtime::open(RuntimeConfig::default())
        .unwrap()
        .account_admission(
            &BindingIdentity::new("tenant", "responses", "revision", "account"),
            None,
            policy,
        )
        .unwrap()
}

#[tokio::test]
async fn passthrough_contract_rate_gate_orders_cooldown_without_serializing_responses() {
    use symbiotic_ai_runtime::{ModelQueueConfig, model::ModelRequestAdmission};
    let account = account(ModelQueueConfig::default());
    let ModelRequestAdmission::Ready(grant) = account.request_admission().await.unwrap() else {
        panic!("unpaced account must permit its first send");
    };
    let until =
        chrono::DateTime::<chrono::Utc>::from_timestamp(chrono::Utc::now().timestamp() + 10, 0)
            .unwrap();
    let mut changes = account.subscribe_cooldowns();
    let recording = account.note_cooldown(until);
    tokio::pin!(recording);
    assert!(
        tokio::time::timeout(Duration::from_millis(200), recording.as_mut())
            .await
            .is_err(),
        "cooldown passed a granted send before dispatch"
    );
    let (started, dispatched) = tokio::sync::oneshot::channel();
    let (reply, response) = tokio::sync::oneshot::channel();
    let request = tokio::spawn(grant.start(async move {
        started.send(()).unwrap();
        response.await.unwrap()
    }));
    tokio::time::timeout(Duration::from_millis(200), dispatched)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_millis(200), recording)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_millis(200), changes.changed())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(account.cooldown_until().await.unwrap(), Some(until));
    assert!(matches!(
        account.request_admission().await.unwrap(),
        ModelRequestAdmission::Cooldown
    ));
    reply.send("synthetic response").unwrap();
    assert_eq!(request.await.unwrap().unwrap(), "synthetic response");
}

#[tokio::test]
async fn passthrough_contract_request_pacing_charges_dispatch_and_not_cancelled_admission() {
    use symbiotic_ai_runtime::{ModelQueueConfig, model::ModelRequestAdmission};
    let account = account(ModelQueueConfig {
        requests_per_minute: Some(60),
        ..Default::default()
    });
    let ModelRequestAdmission::Ready(cancelled) = account.request_admission().await.unwrap() else {
        panic!("first admission must be ready");
    };
    drop(cancelled);
    let ModelRequestAdmission::Ready(deferred) = account.request_admission().await.unwrap() else {
        panic!("unspent admission must be ready");
    };
    let deferred = deferred
        .start_checked(|| Ok(None::<()>), async {
            panic!("deferred transport must not be polled")
        })
        .await
        .unwrap();
    assert_eq!(deferred, None::<()>);
    let ModelRequestAdmission::Ready(sent) = account.request_admission().await.unwrap() else {
        panic!("cancelled admission charged request pacing");
    };
    assert_eq!(
        sent.start(async { "synthetic response" }).await.unwrap(),
        "synthetic response"
    );
    let ModelRequestAdmission::Wait(wait) = account.request_admission().await.unwrap() else {
        panic!("dispatch did not charge request pacing");
    };
    assert!(wait > Duration::ZERO && wait <= Duration::from_secs(1));
}

#[test]
fn passthrough_contract_new_fields_default_and_local_headers_cannot_be_forwarded() {
    let dir = tempfile::tempdir().unwrap();
    let settings = config(dir.path());
    let pass = &settings.passthrough;
    let route = &pass.routes[0];
    assert_eq!(route.max_held_event_bytes, 67_108_864);
    assert_eq!(route.rate_burst_seconds, 0);
    assert!(
        pass.retry_policies.is_empty()
            && route.retry_policy.is_empty()
            && route.error_classes.is_empty()
            && route.fallback_routes.is_empty()
            && route.new_session.is_none()
    );
    for header in [
        "x-symbiotic-retry-policy",
        "x-symbiotic-request-id",
        "x-symbiotic-queue-waited-ms",
        "authorization",
        "set-cookie",
        "proxy-authorization",
        "chatgpt-account-id",
        "content-encoding",
    ] {
        for response in [false, true] {
            let mut bad = settings.clone();
            let route = &mut bad.passthrough.routes[0];
            if response {
                route.return_headers.push(header.into());
            } else {
                route.forward_headers.push(header.into());
            }
            assert!(bad.validate_routes().is_err(), "accepted {header}");
        }
    }
}

#[test]
fn chat_completions_and_get_model_listing_routes_validate_without_io() {
    let dir = tempfile::tempdir().unwrap();
    let mut settings = config(dir.path());
    let routes = &mut settings.passthrough.routes;
    routes[0].stream = symbiotic_gateway::stream::StreamDialect::OpenaiChatCompletions;
    routes[0].path = "/v1/chat/completions".into();
    let mut models = routes[0].clone();
    models.name = "models".into();
    models.method = symbiotic_gateway::PassthroughMethod::Get;
    models.path = "/v1/models".into();
    models.destination = "https://provider.invalid/v1/models".into();
    routes.push(models);
    assert_eq!(settings.validate_routes(), Ok(()));
    settings.passthrough.routes[0].fallback_routes = vec!["models".into()];
    assert!(settings.validate_routes().is_err());
    settings.passthrough.routes[1].method = symbiotic_gateway::PassthroughMethod::Post;
    assert_eq!(settings.validate_routes(), Ok(()));
    settings.passthrough.routes[1].stream =
        symbiotic_gateway::stream::StreamDialect::OpenaiResponses;
    assert!(settings.validate_routes().is_err());
    assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 0);
}

#[tokio::test]
async fn startup_refuses_version_17_queue_with_rebuild_diagnostic() {
    let dir = tempfile::tempdir().unwrap();
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let settings = config(dir.path());
    fs::create_dir(&settings.state_dir).unwrap();
    fs::set_permissions(&settings.state_dir, fs::Permissions::from_mode(0o700)).unwrap();
    let path = settings
        .state_dir
        .join(symbiotic_ai_runtime::QUEUE_DATABASE);
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection.pragma_update(None, "user_version", 17).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    let config_path = dir.path().join("config.json");
    write(&config_path, &serde_json::to_vec(&settings).unwrap());
    let error = match Gateway::open(settings) {
        Ok(_) => panic!("accepted version 17 queue"),
        Err(error) => error,
    };
    assert_eq!(
        error.to_string(),
        "unsupported_queue_schema: unsupported queue schema; rebuild the queue database"
    );
    assert_eq!(
        connection
            .pragma_query_value(None, "user_version", |row| row.get::<_, u32>(0))
            .unwrap(),
        17
    );
    let binary = std::env::var_os("CARGO_BIN_EXE_symbiotic-gateway").unwrap_or_else(|| {
        std::env::current_exe()
            .unwrap()
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("symbiotic-gateway")
            .into_os_string()
    });
    let output = Command::new(binary).arg(config_path).output().unwrap();
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(
        String::from_utf8(output.stderr).unwrap(),
        "unsupported_queue_schema: unsupported queue schema; rebuild the queue database\n"
    );
}

fn gateway_binary() -> std::path::PathBuf {
    std::env::var_os("CARGO_BIN_EXE_symbiotic-gateway")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            std::env::current_exe()
                .unwrap()
                .parent()
                .unwrap()
                .parent()
                .unwrap()
                .join("symbiotic-gateway")
        })
}
use symbiotic_gateway::GatewayError;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
const OBSERVATION_HANG_GUARD: Duration = Duration::from_secs(60);

async fn wait_for_startup<T, F>(mut check_alive: impl FnMut(), mut probe: impl FnMut() -> F) -> T
where
    F: std::future::Future<Output = Result<T, GatewayError>>,
{
    // The socket path can appear before the server accepts requests.
    tokio::time::timeout(OBSERVATION_HANG_GUARD, async {
        loop {
            check_alive();
            match probe().await {
                Ok(reply) => break reply,
                Err(GatewayError::Transport) => tokio::time::sleep(Duration::from_millis(10)).await,
                Err(error) => panic!("child startup failed: {error:?}"),
            }
        }
    })
    .await
    .expect("child did not accept requests within the startup hang guard")
}

async fn wait_for_child_startup<T, F>(child: &mut ChildGuard, probe: impl FnMut() -> F) -> T
where
    F: std::future::Future<Output = Result<T, GatewayError>>,
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

#[tokio::test]
async fn logging_startup_prefers_environment_directives_and_refuses_invalid_ones() {
    use std::io::Read;
    let dir = tempfile::tempdir().unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    write(&dir.path().join("key"), b"synthetic-passthrough-secret");
    let config = config(dir.path());
    let passthrough_path = config.passthrough.socket_path.clone();
    let start = |configured: Option<&str>, environment: Option<&str>| {
        let mut config = config.clone();
        config.log = configured.map(str::to_owned);
        let path = dir.path().join("logging-config.json");
        std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        let mut command = std::process::Command::new(gateway_binary());
        command.env_remove(symbiotic_core::logging::LOG_ENV);
        if let Some(value) = environment {
            command.env(symbiotic_core::logging::LOG_ENV, value);
        }
        ChildGuard(
            command
                .arg(&path)
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .unwrap(),
        )
    };
    let refused = |mut child: ChildGuard| async move {
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
                .map_err(|_| GatewayError::Transport)
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
