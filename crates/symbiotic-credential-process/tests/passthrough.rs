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
use symbiotic_credential_process::{CredentialProcess, ProcessConfig};

fn config(dir: &std::path::Path) -> ProcessConfig {
    serde_json::from_value(json!({"version":4,"state_dir":dir.join("state"),"socket_path":dir.join("typed.sock"),"admission_key":{"backend":"owner_only_file","path":dir.join("admission")},"max_secret_bytes":4096,"max_frame_bytes":8388608,"max_connections":8,"io_timeout_seconds":2,"routes":[],"passthrough":{"socket_path":dir.join("pass.sock"),"health_path":dir.join("health.json"),"max_connections":8,"routes":[{"name":"responses","tenant":"tenant","account":"account","account_sharing_key":null,"method":"post","path":"/v1/responses","destination":"https://provider.invalid/answer","secret":{"backend":"owner_only_file","path":dir.join("key")},"stream":"openai_responses","forward_headers":["content-type"],"return_headers":["x-codex-turn-state"],"max_request_bytes":4096,"max_response_bytes":4096,"max_in_flight":2,"requests_per_minute":null,"idle_timeout_seconds":1}]}})).unwrap()
}
#[test]
fn passthrough_contract_validation_refuses_ambiguous_routes_destinations_and_limits_without_io() {
    let dir = tempfile::tempdir().unwrap();
    let good = config(dir.path());
    let route = &good.passthrough.as_ref().unwrap().routes[0];
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
        let bad_config: ProcessConfig = serde_json::from_value(bad_config).unwrap();
        assert!(bad_config.validate_routes().is_err(), "accepted {field}");
        assert!(CredentialProcess::open(bad_config).is_err());
    }
    let mut invalid = good.clone();
    invalid.passthrough.as_mut().unwrap().max_connections = usize::MAX;
    assert_eq!(
        invalid.validate_routes(),
        Err(symbiotic_egress::EgressError::InvalidRequest)
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
                serde_json::from_value::<ProcessConfig>(invalid)
                    .unwrap()
                    .validate_routes(),
                Err(symbiotic_egress::EgressError::InvalidRequest),
                "accepted {field}: {name}"
            );
        }
    }
    for field in ["name", "path", "max_in_flight", "requests_per_minute"] {
        let mut bad = good.clone();
        let routes = &mut bad.passthrough.as_mut().unwrap().routes;
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
    write(&dir.path().join("admission"), &[b'a'; 32]);
    write(&dir.path().join("key"), b"synthetic-executable-static-key");
    let config_path = dir.path().join("config.json");
    write(&config_path, &serde_json::to_vec(&config).unwrap());
    let binary =
        std::env::var_os("CARGO_BIN_EXE_symbiotic-credential-process").unwrap_or_else(|| {
            std::env::current_exe()
                .unwrap()
                .parent()
                .unwrap()
                .parent()
                .unwrap()
                .join("symbiotic-credential-process")
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

#[test]
fn passthrough_contract_shared_policy_refuses_unsupported_typed_limits_and_mismatched_bursts() {
    let dir = tempfile::tempdir().unwrap();
    for shared in [false, true] {
        let mut good = config(dir.path());
        let mut typed = json!({
            "tenant":"tenant", "account":"account", "account_sharing_key":null,
            "max_attempts":3, "route":"chat", "secret_ref":"", "secret":{"backend":"none"},
            "destination":"https://provider.invalid/v1", "model":"synthetic-model",
            "provider":{"kind":"open_ai_chat","operator":"synthetic"},
            "allow_loopback_http":false, "max_field_bytes":1024, "max_output_tokens":100,
            "max_in_flight":2, "requests_per_minute":null, "input_units_per_minute":null,
            "timeout_seconds":1
        });
        if shared {
            typed["tenant"] = json!("another_tenant");
            typed["account"] = json!("another_account");
            typed["account_sharing_key"] = json!("synthetic_pool");
            good.passthrough.as_mut().unwrap().routes[0].account_sharing_key =
                Some(serde_json::from_value(json!("synthetic_pool")).unwrap());
        }
        good.routes = vec![serde_json::from_value(typed.clone()).unwrap()];
        assert_eq!(good.validate_routes(), Ok(()));
        for (field, value) in [
            ("max_in_flight", json!(3)),
            ("requests_per_minute", json!(1)),
            ("input_units_per_minute", json!(1)),
            ("provider_request_limit", json!(1)),
        ] {
            let mut candidate = good.clone();
            let mut bad = typed.clone();
            bad[field] = value;
            candidate.routes = vec![serde_json::from_value(bad).unwrap()];
            assert!(
                candidate.validate_routes().is_err(),
                "accepted shared {field}"
            );
        }
        let mut candidate = good;
        candidate.passthrough.as_mut().unwrap().routes[0].rate_burst_seconds = 60;
        assert!(
            candidate.validate_routes().is_err(),
            "accepted mismatched burst"
        );
    }
    assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 0);
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
    let pass = settings.passthrough.as_ref().unwrap();
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
            let route = &mut bad.passthrough.as_mut().unwrap().routes[0];
            if response {
                route.return_headers.push(header.into());
            } else {
                route.forward_headers.push(header.into());
            }
            assert!(bad.validate_routes().is_err(), "accepted {header}");
        }
    }
}
