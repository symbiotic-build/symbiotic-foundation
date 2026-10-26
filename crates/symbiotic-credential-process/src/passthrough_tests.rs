//! Real Unix sockets and loopback providers. Only URL/TLS mapping is test injected.
use super::*;
use serde_json::{Value, json};
use std::{fs, os::unix::fs::PermissionsExt};
use tokio::sync::{mpsc, oneshot};

const KEY: &str = "synthetic-passthrough-secret";
struct Reply {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}
impl Reply {
    fn json() -> Self {
        Self {
            status: 200,
            headers: vec![("content-type".into(), "application/json".into())],
            body: br#"{"result":"ok"}"#.to_vec(),
        }
    }
    fn status(status: u16) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body: b"discarded upstream body".to_vec(),
        }
    }
    fn rate(seconds: u64) -> Self {
        Self {
            status: 429,
            headers: vec![
                ("retry-after".into(), seconds.to_string()),
                ("x-ratelimit-remaining".into(), "0".into()),
            ],
            body: Vec::new(),
        }
    }
}
struct Upstream {
    head: String,
    body: Vec<u8>,
    reply: oneshot::Sender<Reply>,
}
struct Fixture {
    base: String,
    incoming: mpsc::Receiver<Upstream>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Fixture {
    fn without_provider() -> Self {
        let (send, incoming) = mpsc::channel(1);
        Self {
            base: "http://127.0.0.1:1".into(),
            incoming,
            task: tokio::spawn(async move {
                let _send = send;
                std::future::pending::<()>().await;
            }),
        }
    }
    async fn start() -> Self {
        Self::at("127.0.0.1:0").await
    }
    async fn at(address: &str) -> Self {
        let listener = tokio::net::TcpListener::bind(address).await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let (send, incoming) = mpsc::channel(32);
        let task = tokio::spawn(async move {
            let mut tasks = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (mut stream, _) = accepted.unwrap();
                        let send = send.clone();
                        tasks.spawn(async move {
                            let mut bytes = Vec::new();
                            while !bytes.ends_with(b"\r\n\r\n") { bytes.push(stream.read_u8().await.unwrap()); }
                            let head = String::from_utf8(bytes).unwrap();
                            let length = head.lines().find_map(|line| line.strip_prefix("content-length: ")).unwrap_or("0").parse().unwrap();
                            let mut body = vec![0; length];
                            stream.read_exact(&mut body).await.unwrap();
                            let (reply, receive) = oneshot::channel();
                            send.send(Upstream { head, body, reply }).await.unwrap();
                            let Ok(reply) = receive.await else { return; };
                            let mut head = format!("HTTP/1.1 {} Fixture\r\nContent-Length: {}\r\nConnection: close\r\n", reply.status, reply.body.len());
                            for (name, value) in reply.headers { head.push_str(&format!("{name}: {value}\r\n")); }
                            head.push_str("\r\n");
                            let _ = stream.write_all(head.as_bytes()).await;
                            let _ = stream.write_all(&reply.body).await;
                        });
                    }
                    _ = tasks.join_next(), if !tasks.is_empty() => {},
                }
            }
        });
        Self {
            base,
            incoming,
            task,
        }
    }
    async fn next(&mut self) -> Upstream {
        tokio::time::timeout(Duration::from_secs(3), self.incoming.recv())
            .await
            .unwrap()
            .unwrap()
    }
}
struct Harness {
    process: CredentialProcess,
    task: tokio::task::JoinHandle<Result<()>>,
    stop: Option<oneshot::Sender<()>>,
    _dir: tempfile::TempDir,
}
impl Drop for Harness {
    fn drop(&mut self) {
        self.task.abort();
    }
}
fn write_private(path: &std::path::Path, bytes: &[u8]) {
    fs::write(path, bytes).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
}
pub(super) fn config(dir: &std::path::Path) -> ProcessConfig {
    fs::set_permissions(dir, fs::Permissions::from_mode(0o700)).unwrap();
    write_private(&dir.join("key"), KEY.as_bytes());
    write_private(&dir.join("admission"), &[b'a'; 32]);
    serde_json::from_value(json!({
        "version":4,"state_dir":dir.join("state"),"socket_path":dir.join("typed.sock"),
        "admission_key":{"backend":"owner_only_file","path":dir.join("admission")},
        "max_secret_bytes":65536,"max_frame_bytes":8388608,"max_connections":32,"io_timeout_seconds":2,"routes":[],
        "passthrough":{"socket_path":dir.join("pass.sock"),"health_path":dir.join("health.json"),"max_connections":16,
        "routes":[{"name":"responses","tenant":"tenant","account":"account","account_sharing_key":null,
            "method":"post","path":"/v1/responses","destination":"https://provider.invalid/answer",
            "secret":{"backend":"owner_only_file","path":dir.join("key")},"stream":"openai_responses",
            "forward_headers":["content-type","session-id","x-codex-turn-state","x-codex-window-id","x-test"],
            "return_headers":["x-codex-turn-state","x-request-id"],"max_request_bytes":65536,"max_response_bytes":65536,
            "max_in_flight":2,"requests_per_minute":null,"idle_timeout_seconds":1}]}
    })).unwrap()
}
impl Harness {
    async fn start(fixture: &Fixture, change: impl FnOnce(&mut ProcessConfig)) -> Self {
        Self::with_hook(fixture, change, None).await
    }
    async fn with_hook(
        fixture: &Fixture,
        change: impl FnOnce(&mut ProcessConfig),
        hook: Option<RetryHook>,
    ) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let mut config = config(dir.path());
        change(&mut config);
        let mut process = CredentialProcess::open(config).unwrap();
        let pass = Arc::get_mut(&mut process.inner)
            .unwrap()
            .passthrough
            .as_mut()
            .unwrap();
        pass.wire_base = Some(fixture.base.clone());
        pass.retry_hook = hook;
        for route in &mut pass.routes {
            route.client = reqwest::Client::builder()
                .no_proxy()
                .no_gzip()
                .no_brotli()
                .no_deflate()
                .no_zstd()
                .http1_only()
                .redirect(reqwest::redirect::Policy::none())
                .retry(reqwest::retry::never())
                .read_timeout(Duration::from_secs(route.config.idle_timeout_seconds))
                .connect_timeout(Duration::from_secs(route.config.idle_timeout_seconds))
                .build()
                .unwrap();
            if let Credential::Login(login) = &mut route.credential {
                Arc::get_mut(login).unwrap().test_transport(&fixture.base);
            }
        }
        let listener = bind(&process).unwrap();
        let (stop, stopped) = oneshot::channel();
        let owner = process.clone();
        let task = tokio::spawn(serve_until(
            owner,
            listener,
            async {
                let _ = stopped.await;
                Ok(())
            },
            current_uid(),
        ));
        let result = Self {
            process,
            task,
            stop: Some(stop),
            _dir: dir,
        };
        result
            .health_where(|value| value["state"] == "running")
            .await;
        result
    }
    fn path(&self) -> &std::path::Path {
        &self
            .process
            .config()
            .passthrough
            .as_ref()
            .unwrap()
            .socket_path
    }
    fn account(&self) -> AccountAdmission {
        self.process.inner.passthrough.as_ref().unwrap().routes[0]
            .account
            .clone()
    }
    async fn connect(&self, path: &str, body: &[u8], extra: &str) -> UnixStream {
        let mut socket = UnixStream::connect(self.path()).await.unwrap();
        let head = format!(
            "POST {path} HTTP/1.1\r\nHost: ignored.invalid\r\nContent-Length: {}\r\n{extra}\r\n",
            body.len()
        );
        socket.write_all(head.as_bytes()).await.unwrap();
        socket.write_all(body).await.unwrap();
        socket
    }
    async fn call(&self) -> tokio::task::JoinHandle<Vec<u8>> {
        let socket = self.connect("/v1/responses", b"{}", "").await;
        tokio::spawn(read(socket))
    }
    async fn health_where(&self, predicate: impl Fn(&Value) -> bool) -> Value {
        let path = self
            .process
            .config()
            .passthrough
            .as_ref()
            .unwrap()
            .health_path
            .as_ref()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Ok(bytes) = fs::read(path) {
                    let health: Value =
                        serde_json::from_slice(&bytes).expect("atomic complete JSON");
                    if predicate(&health) {
                        return health;
                    }
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap()
    }
}
async fn read(mut socket: UnixStream) -> Vec<u8> {
    let mut bytes = Vec::new();
    tokio::time::timeout(Duration::from_secs(4), socket.read_to_end(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    bytes
}
fn decoded(bytes: &[u8]) -> (u16, String, Vec<u8>) {
    let split = bytes.windows(4).position(|v| v == b"\r\n\r\n").unwrap() + 4;
    let head = String::from_utf8(bytes[..split].to_vec()).unwrap();
    let status = head.split(' ').nth(1).unwrap().parse().unwrap();
    let mut rest = &bytes[split..];
    let mut body = Vec::new();
    loop {
        let end = rest
            .windows(2)
            .position(|v| v == b"\r\n")
            .expect("complete chunk");
        let size = usize::from_str_radix(std::str::from_utf8(&rest[..end]).unwrap(), 16).unwrap();
        rest = &rest[end + 2..];
        if size == 0 {
            assert_eq!(rest, b"\r\n");
            break;
        }
        body.extend_from_slice(&rest[..size]);
        assert_eq!(&rest[size..size + 2], b"\r\n");
        rest = &rest[size + 2..];
    }
    assert!(head.contains("Connection: close\r\n"));
    (status, head.to_ascii_lowercase(), body)
}

#[tokio::test]
async fn forwarding_preserves_body_and_only_allowed_headers_and_scrubs_return_headers() {
    let mut fixture = Fixture::start().await;
    let harness = Harness::start(&fixture, |config| {
        config.passthrough.as_mut().unwrap().routes[0]
            .forward_headers
            .extend(["x-long", "x-empty", "x-nonascii"].map(str::to_owned));
    })
    .await;
    let body = b"{ \"input\" : \"unchanged\" }\n";
    // This valid block exceeds the removed draft64KiB limit but fits the configured frame.
    let padding = (0..9)
        .map(|i| format!("X-Padding-{i}: {}\r\n", "v".repeat(8192)))
        .collect::<String>();
    let extra = format!(
        "Authorization: Bearer attacker\r\nCookie: attacker\r\nX-Forwarded-For: attacker\r\nSession-Id: session\r\nX-Codex-Turn-State: turn\r\nX-Codex-Window-Id: window\r\nX-Test: \x0bdrop\r\nX-Unlisted: drop\r\nX-Long: {}\r\nX-Empty:\r\nX-Nonascii: é\r\n{padding}",
        "v".repeat(8193)
    );
    let socket = harness.connect("/v1/responses", body, &extra).await;
    let reply = tokio::spawn(read(socket));
    let upstream = fixture.next().await;
    assert_eq!(upstream.body, body);
    assert!(
        upstream
            .head
            .contains(&format!("authorization: Bearer {KEY}\r\n"))
    );
    for line in [
        "session-id: session\r\n",
        "x-codex-turn-state: turn\r\n",
        "x-codex-window-id: window\r\n",
        "accept-encoding: identity\r\n",
    ] {
        assert!(upstream.head.contains(line), "{line}");
    }
    for value in [
        "attacker",
        "x-unlisted",
        "x-test:",
        "x-long:",
        "x-empty:",
        "x-nonascii:",
        "x-padding-",
    ] {
        assert!(!upstream.head.contains(value));
    }
    let mut answer = Reply::json();
    answer.headers.extend([
        ("x-codex-turn-state".into(), "next-turn".into()),
        ("x-request-id".into(), KEY.into()),
        ("set-cookie".into(), "discard".into()),
    ]);
    upstream.reply.send(answer).ok().unwrap();
    let (status, head, body) = decoded(&reply.await.unwrap());
    assert_eq!(status, 200);
    assert!(head.contains("x-codex-turn-state: next-turn\r\n"));
    assert!(!head.contains(KEY));
    assert!(!head.contains("set-cookie"));
    assert!(!head.contains("x-request-id"));
    assert_eq!(body, br#"{"result":"ok"}"#);
}

#[tokio::test]
async fn unmatched_and_ambiguous_requests_never_reach_provider() {
    let mut fixture = Fixture::start().await;
    let harness = Harness::start(&fixture, |_| {}).await;
    for path in [
        "/unknown",
        "/v1/responses?query",
        "/v1/%72esponses",
        "//v1/responses",
        "/v1/responses#fragment",
        "/v1/\\responses",
    ] {
        let (status, _, body) = decoded(&read(harness.connect(path, b"{}", "").await).await);
        assert_eq!(status, 404);
        assert_eq!(body, br#"{"error":{"code":"route_not_found"}}"#);
    }
    for extra in [
        "Content-Length: 2\r\n",
        "Transfer-Encoding: chunked\r\n",
        "Expect: 100-continue\r\n",
        "Session-Id: one\r\nsession-id: two\r\n",
    ] {
        assert_eq!(
            decoded(&read(harness.connect("/v1/responses", b"{}", extra).await).await).0,
            400
        );
    }
    assert!(fixture.incoming.try_recv().is_err());
}

#[tokio::test]
async fn redirects_and_provider_failures_return_fixed_bodies_and_safe_headers() {
    let mut fixture = Fixture::start().await;
    let harness = Harness::start(&fixture, |c| {
        c.passthrough.as_mut().unwrap().routes[0]
            .retry_policy
            .insert(
                ErrorClass::Server,
                RetryRule {
                    action: RetryAction::Fail,
                    max_attempts: None,
                    backoff: Default::default(),
                },
            );
    })
    .await;
    for status in [302, 401, 403, 500, 503] {
        let call = harness.call().await;
        let upstream = fixture.next().await;
        let mut answer = Reply::status(status);
        answer.headers = vec![
            ("location".into(), format!("{}/stolen", fixture.base)),
            ("set-cookie".into(), KEY.into()),
            ("x-codex-turn-state".into(), "state".into()),
            ("x-ratelimit-remaining".into(), "0".into()),
        ];
        upstream.reply.send(answer).ok().unwrap();
        let (actual, head, body) = decoded(&call.await.unwrap());
        assert_eq!(actual, status);
        assert_eq!(body, br#"{"error":{"code":"upstream_status"}}"#);
        assert_eq!(head.contains("x-codex-turn-state: state"), status != 401);
        assert!(!head.contains("location:"));
        assert!(!head.contains("set-cookie"));
        assert_eq!(head.contains("x-ratelimit-remaining:"), status == 503);
        assert!(fixture.incoming.try_recv().is_err());
    }
}

#[tokio::test]
async fn rate_limit_waits_and_resends_identical_body_without_exposing_429() {
    let mut fixture = Fixture::start().await;
    let harness = Harness::start(&fixture, |_| {}).await;
    let call = harness.call().await;
    let first = fixture.next().await;
    let body = first.body.clone();
    let before = Instant::now();
    first.reply.send(Reply::rate(1)).ok().unwrap();
    harness
        .health_where(|h| !h["routes"][0]["cooldown_until"].is_null())
        .await;
    let second = harness.call().await;
    assert!(
        tokio::time::timeout(Duration::from_millis(40), fixture.incoming.recv())
            .await
            .is_err()
    );
    let resend = fixture.next().await;
    assert!(before.elapsed() >= Duration::from_millis(900));
    assert_eq!(resend.body, body);
    resend.reply.send(Reply::json()).ok().unwrap();
    fixture.next().await.reply.send(Reply::json()).ok().unwrap();
    assert_eq!(decoded(&call.await.unwrap()).0, 200);
    assert_eq!(decoded(&second.await.unwrap()).0, 200);
}

#[tokio::test]
async fn concurrent_429s_both_wait_and_resend_under_the_shared_deadline() {
    let mut fixture = Fixture::start().await;
    let harness = Harness::start(&fixture, |_| {}).await;
    let a = harness.call().await;
    let b = harness.call().await;
    let first = fixture.next().await;
    let second = fixture.next().await;
    first.reply.send(Reply::rate(1)).ok().unwrap();
    second.reply.send(Reply::rate(1)).ok().unwrap();
    harness
        .health_where(|h| !h["routes"][0]["cooldown_until"].is_null())
        .await;
    assert!(
        tokio::time::timeout(Duration::from_millis(40), fixture.incoming.recv())
            .await
            .is_err()
    );
    for _ in 0..2 {
        fixture.next().await.reply.send(Reply::json()).ok().unwrap();
    }
    assert_eq!(decoded(&a.await.unwrap()).0, 200);
    assert_eq!(decoded(&b.await.unwrap()).0, 200);
}

#[tokio::test]
async fn five_callers_share_two_slots_and_slot_wait_never_uses_cooldown_budget() {
    let mut fixture = Fixture::start().await;
    let harness = Harness::start(&fixture, |c| {
        c.passthrough.as_mut().unwrap().routes[0].max_queue_wait_seconds = Some(0)
    })
    .await;
    let mut calls = Vec::new();
    for _ in 0..5 {
        calls.push(harness.call().await);
    }
    let first = fixture.next().await;
    let second = fixture.next().await;
    assert!(
        tokio::time::timeout(Duration::from_millis(40), fixture.incoming.recv())
            .await
            .is_err()
    );
    first.reply.send(Reply::json()).ok().unwrap();
    for _ in 0..3 {
        fixture.next().await.reply.send(Reply::json()).ok().unwrap();
    }
    second.reply.send(Reply::json()).ok().unwrap();
    for call in calls {
        assert_eq!(decoded(&call.await.unwrap()).0, 200);
    }
}

#[tokio::test]
async fn a_cooldown_recorded_during_slot_wait_is_checked_before_dispatch() {
    let mut fixture = Fixture::start().await;
    let harness = Harness::start(&fixture, |c| {
        c.passthrough.as_mut().unwrap().routes[0].max_in_flight = 1
    })
    .await;
    let first = harness.call().await;
    let upstream = fixture.next().await;
    let second = harness.call().await;
    upstream.reply.send(Reply::rate(1)).ok().unwrap();
    harness
        .health_where(|h| !h["routes"][0]["cooldown_until"].is_null())
        .await;
    assert!(
        tokio::time::timeout(Duration::from_millis(40), fixture.incoming.recv())
            .await
            .is_err()
    );
    fixture.next().await.reply.send(Reply::json()).ok().unwrap();
    fixture.next().await.reply.send(Reply::json()).ok().unwrap();
    assert_eq!(decoded(&first.await.unwrap()).0, 200);
    assert_eq!(decoded(&second.await.unwrap()).0, 200);
}

#[tokio::test]
async fn disconnect_during_slot_or_cooldown_wait_cancels_without_sending() {
    let mut fixture = Fixture::start().await;
    let harness = Harness::start(&fixture, |c| {
        c.passthrough.as_mut().unwrap().routes[0].max_in_flight = 1
    })
    .await;
    let slot = harness.account().acquire().await.unwrap();
    let pending = harness.connect("/v1/responses", b"{}", "").await;
    drop(pending);
    drop(slot);
    harness
        .account()
        .note_cooldown(Utc::now() + chrono::Duration::milliseconds(100))
        .await
        .unwrap();
    let pending = harness.connect("/v1/responses", b"{}", "").await;
    drop(pending);
    assert!(
        tokio::time::timeout(Duration::from_millis(160), fixture.incoming.recv())
            .await
            .is_err()
    );
    let call = harness.call().await;
    fixture.next().await.reply.send(Reply::json()).ok().unwrap();
    assert_eq!(decoded(&call.await.unwrap()).0, 200);
}

#[tokio::test]
async fn maximum_cooldown_wait_returns_fixed_429_and_rounded_remaining_retry_after() {
    let mut fixture = Fixture::start().await;
    let harness = Harness::start(&fixture, |c| {
        c.passthrough.as_mut().unwrap().routes[0].max_queue_wait_seconds = Some(1)
    })
    .await;
    let call = harness.call().await;
    let upstream = fixture.next().await;
    let mut answer = Reply::rate(10);
    answer.headers.push(("set-cookie".into(), KEY.into()));
    answer
        .headers
        .push(("x-codex-turn-state".into(), "held-answer".into()));
    upstream.reply.send(answer).ok().unwrap();
    let (status, head, body) = decoded(&call.await.unwrap());
    assert_eq!(status, 429);
    assert_eq!(body, br#"{"error":{"code":"rate_limited"}}"#);
    assert!(head.contains("retry-after: 9\r\n"));
    assert!(!head.contains("x-ratelimit-remaining:"));
    assert!(!head.contains("x-codex-turn-state"));
    assert!(!head.contains("set-cookie"));
    assert!(fixture.incoming.try_recv().is_err());
}

#[tokio::test]
async fn passthrough_contract_retry_after_rounds_fractional_seconds_up() {
    let directory = tempfile::tempdir().unwrap();
    let mut settings = config(directory.path());
    settings.passthrough.as_mut().unwrap().routes[0].max_queue_wait_seconds = Some(0);
    let mut process = CredentialProcess::open(settings).unwrap();
    let frozen_at = Utc::now() + chrono::Duration::seconds(60);
    let pass = Arc::get_mut(&mut process.inner)
        .unwrap()
        .passthrough
        .as_mut()
        .unwrap();
    pass.frozen_at = Some(frozen_at);
    let account = pass.routes[0].account.clone();
    for (nanoseconds, seconds) in [(1, 1), (1_000_000_000, 1), (1_000_000_001, 2)] {
        account
            .note_cooldown(frozen_at + chrono::Duration::nanoseconds(nanoseconds))
            .await
            .unwrap();
        let (mut caller, server) = UnixStream::pair().unwrap();
        caller
            .write_all(b"POST /v1/responses HTTP/1.1\r\nHost: local\r\nContent-Length: 0\r\n\r\n")
            .await
            .unwrap();
        let owner = process.clone();
        let handler = tokio::spawn(async move { connection(&owner, server).await });
        let (status, headers, body) = decoded(&read(caller).await);
        assert_eq!(status, 429);
        assert!(
            headers.contains(&format!("retry-after: {seconds}\r\n")),
            "{headers}"
        );
        assert_eq!(body, br#"{"error":{"code":"rate_limited"}}"#);
        handler.await.unwrap().unwrap();
    }
}

#[tokio::test]
async fn split_sse_deltas_are_scrubbed_through_the_socket() {
    let mut fixture = Fixture::start().await;
    let harness = Harness::start(&fixture, |_| {}).await;
    let call = harness.call().await;
    let upstream = fixture.next().await;
    let text = format!(
        "data: {{\"type\":\"response.output_text.delta\",\"item_id\":\"item\",\"output_index\":0,\"content_index\":0,\"delta\":\"{}\"}}\n\ndata: {{\"type\":\"response.output_text.delta\",\"item_id\":\"item\",\"output_index\":0,\"content_index\":0,\"delta\":\"{}\"}}\n\ndata: {{\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{{\"id\":\"item\"}}}}\n\ndata: {{\"type\":\"response.completed\",\"response\":{{}}}}\n\n",
        &KEY[..12],
        &KEY[12..]
    );
    upstream
        .reply
        .send(Reply {
            status: 200,
            headers: vec![("content-type".into(), "text/event-stream".into())],
            body: text.into_bytes(),
        })
        .ok()
        .unwrap();
    let (status, _, body) = decoded(&call.await.unwrap());
    assert_eq!(status, 200);
    let body = String::from_utf8(body).unwrap();
    assert!(!body.contains(KEY));
    let text: String = body
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter_map(|event| {
            event
                .get("delta")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .collect();
    assert_eq!(text, "[redacted]");
}

#[tokio::test]
async fn invalid_content_encoding_and_type_and_response_overflow_end_without_final_chunk() {
    let mut fixture = Fixture::start().await;
    let harness = Harness::start(&fixture, |c| {
        c.passthrough.as_mut().unwrap().routes[0].max_response_bytes = 8
    })
    .await;
    for headers in [
        vec![("content-type".into(), "text/plain".into())],
        vec![
            ("content-type".into(), "application/json".into()),
            ("content-encoding".into(), "gzip".into()),
        ],
        vec![("content-type".into(), "application/json".into())],
        vec![],
    ] {
        let call = harness.call().await;
        let upstream = fixture.next().await;
        let mut reply = Reply::json();
        reply.headers = headers;
        upstream.reply.send(reply).ok().unwrap();
        let response = call.await.unwrap();
        assert_eq!(decoded(&response).0, 502);
    }
}

#[tokio::test]
async fn upstream_idle_timeout_with_fail_policy_releases_the_shared_slot() {
    let mut fixture = Fixture::start().await;
    let harness = Harness::start(&fixture, |c| {
        c.passthrough.as_mut().unwrap().routes[0].max_in_flight = 1;
        c.passthrough.as_mut().unwrap().routes[0]
            .retry_policy
            .insert(
                ErrorClass::Network,
                RetryRule {
                    action: RetryAction::Fail,
                    max_attempts: None,
                    backoff: Default::default(),
                },
            );
    })
    .await;
    let call = harness.call().await;
    let held = fixture.next().await;
    assert_eq!(decoded(&call.await.unwrap()).0, 502);
    drop(held);
    let next = harness.call().await;
    fixture.next().await.reply.send(Reply::json()).ok().unwrap();
    assert_eq!(decoded(&next.await.unwrap()).0, 200);
}

#[tokio::test]
async fn cooldown_is_durable_across_runtime_restart() {
    let mut fixture = Fixture::start().await;
    let mut harness = Harness::start(&fixture, |_| {}).await;
    let config = harness.process.config().clone();
    let until = Utc::now() + chrono::Duration::seconds(1);
    harness.account().note_cooldown(until).await.unwrap();
    harness.task.abort();
    let _ = (&mut harness.task).await;
    let retained = std::mem::replace(&mut harness._dir, tempfile::tempdir().unwrap());
    drop(harness);
    let mut process = CredentialProcess::open(config).unwrap();
    let pass = Arc::get_mut(&mut process.inner)
        .unwrap()
        .passthrough
        .as_mut()
        .unwrap();
    pass.wire_base = Some(fixture.base.clone());
    for route in &mut pass.routes {
        route.client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .read_timeout(Duration::from_secs(1))
            .build()
            .unwrap();
    }
    let listener = bind(&process).unwrap();
    let owner = process.clone();
    let task = tokio::spawn(serve_until(
        owner,
        listener,
        std::future::pending(),
        current_uid(),
    ));
    let restarted = Harness {
        process,
        task,
        stop: None,
        _dir: retained,
    };
    assert_eq!(
        restarted
            .account()
            .cooldown_until()
            .await
            .unwrap()
            .unwrap()
            .timestamp(),
        until.timestamp()
    );
    let call = restarted.call().await;
    assert!(
        tokio::time::timeout(Duration::from_millis(40), fixture.incoming.recv())
            .await
            .is_err()
    );
    let upstream = fixture.next().await;
    assert!(
        Utc::now() >= until,
        "restarted process dispatched inside its durable cooldown"
    );
    upstream.reply.send(Reply::json()).ok().unwrap();
    assert_eq!(decoded(&call.await.unwrap()).0, 200);
}

#[tokio::test(start_paused = true)]
async fn health_heartbeat_replaces_complete_sanitized_json_and_stopping_precedes_close() {
    let fixture = Fixture::without_provider();
    let mut harness = Harness::start(&fixture, |_| {}).await;
    let config = harness.process.config().passthrough.as_ref().unwrap();
    let path = config.health_path.as_ref().unwrap();
    let initial = harness.health_where(|h| h["state"] == "running").await;
    assert_eq!(
        initial,
        json!({"version":1,"state":"running","updated_at":initial["updated_at"],"routes":[{"name":"responses","login":"static","expires_at":null,"cooldown_until":null,"retried_events":{},"waiting":[],"rate_limits":{},"rate_limits_seen_at":null,"new_session_hold_until":null}]})
    );
    assert_eq!(
        fs::metadata(path).unwrap().permissions().mode() & 0o777,
        0o640
    );
    fs::write(path, b"{\"updated_at\":0}").unwrap();
    tokio::time::advance(Duration::from_secs(10)).await;
    harness.health_where(|h| h["updated_at"] != 0).await;
    harness.stop.take().unwrap().send(()).unwrap();
    let stopping = harness.health_where(|h| h["state"] == "stopping").await;
    assert!(!stopping.to_string().contains(KEY));
    assert_eq!((&mut harness.task).await.unwrap(), Ok(()));
}

fn login_config(config: &mut ProcessConfig) {
    let path = config.state_dir.parent().unwrap().join("login.json");
    let now = Utc::now().timestamp() as u64;
    write_private(&path,serde_json::to_string(&json!({"version":1,"family":"chatgpt_codex","issuer":"https://auth.openai.com","client_id":"app_EMoamEEZ73f0CkXaXp7hrann","access_token":"synthetic-old-access","refresh_token":"synthetic-old-refresh","id_token":"synthetic-old-id","account_id":"synthetic-account","principal_type":null,"principal_id":null,"expires_at":now+3600})).unwrap().as_bytes());
    config.passthrough.as_mut().unwrap().routes[0].secret = SecretSource::LoginFile {
        path,
        family: LoginFamily::ChatgptCodex,
    };
}
fn renewal() -> Reply {
    let mut reply = Reply::json();
    reply.body=serde_json::to_vec(&json!({"access_token":"synthetic-new-access","refresh_token":"synthetic-new-refresh","expires_in":3600})).unwrap();
    reply
}
#[tokio::test]
async fn unauthorized_renews_and_resends_once_and_codex_accepts_missing_stream_type() {
    let mut fixture = Fixture::start().await;
    let harness = Harness::start(&fixture, login_config).await;
    let call = harness.call().await;
    let first = fixture.next().await;
    assert!(
        first
            .head
            .contains("authorization: Bearer synthetic-old-access")
    );
    assert!(first.head.contains("chatgpt-account-id: synthetic-account"));
    assert!(first.head.contains("originator: codex_cli_rs"));
    first.reply.send(Reply::status(401)).ok().unwrap();
    let exchange = fixture.next().await;
    assert!(exchange.head.starts_with("POST /oauth/token HTTP/1.1"));
    assert_eq!(
        serde_json::from_slice::<Value>(&exchange.body).unwrap()["refresh_token"],
        "synthetic-old-refresh"
    );
    exchange.reply.send(renewal()).ok().unwrap();
    let resend = fixture.next().await;
    assert!(
        resend
            .head
            .contains("authorization: Bearer synthetic-new-access")
    );
    assert_eq!(resend.body, b"{}");
    resend
        .reply
        .send(Reply {
            status: 200,
            headers: vec![],
            body: b"data: {\"type\":\"response.completed\",\"response\":{}}\n\n".to_vec(),
        })
        .ok()
        .unwrap();
    let (status, head, _) = decoded(&call.await.unwrap());
    assert_eq!(status, 200);
    assert!(head.contains("content-type: text/event-stream"));
    let health = harness
        .health_where(|h| h["routes"][0]["login"] == "expiring")
        .await;
    assert!(!health.to_string().contains("synthetic"));
}

#[tokio::test]
async fn second_unauthorized_marks_login_failed_and_future_call_is_unavailable() {
    let mut fixture = Fixture::start().await;
    let mut harness = Harness::start(&fixture, login_config).await;
    let call = harness.call().await;
    fixture
        .next()
        .await
        .reply
        .send(Reply::status(401))
        .ok()
        .unwrap();
    fixture.next().await.reply.send(renewal()).ok().unwrap();
    fixture
        .next()
        .await
        .reply
        .send(Reply::status(401))
        .ok()
        .unwrap();
    let (status, _, body) = decoded(&call.await.unwrap());
    assert_eq!(status, 401);
    assert_eq!(body, br#"{"error":{"code":"upstream_status"}}"#);
    harness
        .health_where(|h| h["routes"][0]["login"] == "renewal_failed")
        .await;
    let (status, _, body) = decoded(&harness.call().await.await.unwrap());
    assert_eq!(status, 503);
    assert_eq!(body, br#"{"error":{"code":"credential_unavailable"}}"#);
    assert!(fixture.incoming.try_recv().is_err());
    let store = match &harness
        .process
        .config()
        .passthrough
        .as_ref()
        .unwrap()
        .routes[0]
        .secret
    {
        SecretSource::LoginFile { path, .. } => path.clone(),
        _ => unreachable!(),
    };
    harness.task.abort();
    let _ = (&mut harness.task).await;
    let retained = std::mem::replace(&mut harness._dir, tempfile::tempdir().unwrap());
    drop(harness);
    let reopened = Login::open(&store, LoginFamily::ChatgptCodex, 65536).unwrap();
    assert_eq!(
        reopened.health().await.unwrap().0,
        LoginHealth::LoginRequired
    );
    assert!(reopened.request().await.is_err());
    assert!(fixture.incoming.try_recv().is_err());
    drop(reopened);
    drop(retained);
}

#[tokio::test]
async fn fenced_login_never_sends_a_request() {
    let mut fixture = Fixture::start().await;
    let harness = Harness::start(&fixture, |c| {
        login_config(c);
        let SecretSource::LoginFile { path, .. } =
            &c.passthrough.as_ref().unwrap().routes[0].secret
        else {
            panic!()
        };
        let mut fence = path.as_os_str().to_owned();
        fence.push(".renewal");
        write_private(std::path::Path::new(&fence), b"pending");
    })
    .await;
    assert_eq!(decoded(&harness.call().await.await.unwrap()).0, 503);
    assert!(fixture.incoming.try_recv().is_err());
    harness
        .health_where(|h| h["routes"][0]["login"] == "login_required")
        .await;
}

#[tokio::test]
async fn typed_provider_and_passthrough_share_one_durable_cooldown() {
    use symbiotic_ai_runtime::{
        ModelBinding, ResponseCacheMode,
        model::{ChatMessage, ChatRequest, OpenAiCompatibleChatProvider},
    };
    let mut fixture = Fixture::start().await;
    let harness = Harness::start(&fixture, |_| {}).await;
    let route = &harness.process.inner.passthrough.as_ref().unwrap().routes[0];
    let typed = harness
        .process
        .inner
        .runtime
        .chat(
            ModelBinding::new(OpenAiCompatibleChatProvider::new(
                "synthetic",
                "model",
                &fixture.base,
                "",
            ))
            .with_identity(BindingIdentity::new(
                &route.config.tenant,
                "typed_chat",
                "typed-v1",
                &route.config.account,
            ))
            .with_policy(route.config.policy())
            .with_answer_recovery(symbiotic_ai_runtime::AnswerRecovery::Off)
            .with_response_cache(ResponseCacheMode::Off),
        )
        .unwrap();
    let passthrough = harness.call().await;
    fixture
        .next()
        .await
        .reply
        .send(Reply::rate(1))
        .ok()
        .unwrap();
    harness
        .health_where(|h| !h["routes"][0]["cooldown_until"].is_null())
        .await;
    let typed = tokio::spawn(async move {
        typed
            .chat(ChatRequest {
                messages: vec![ChatMessage {
                    role: "user".into(),
                    content: "synthetic input".into(),
                }],
                max_output_tokens: Some(8),
                temperature: None,
                response_format: None,
                role_binding: None,
                source: None,
                metadata: Value::Null,
            })
            .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(40), fixture.incoming.recv())
            .await
            .is_err()
    );
    for _ in 0..2 {
        let upstream = fixture.next().await;
        if upstream.head.starts_with("POST /answer ") {
            upstream.reply.send(Reply::json()).ok().unwrap();
        } else {
            assert!(upstream.head.starts_with("POST /chat/completions "));
            let mut reply = Reply::json();
            reply.body=br#"{"choices":[{"message":{"content":"typed answer"},"finish_reason":"stop"}],"usage":{"prompt_tokens":2,"completion_tokens":2}}"#.to_vec();
            upstream.reply.send(reply).ok().unwrap();
        }
    }
    assert_eq!(decoded(&passthrough.await.unwrap()).0, 200);
    assert_eq!(typed.await.unwrap().unwrap().text, "typed answer");
}

#[tokio::test]
async fn peer_uid_check_refuses_a_foreign_uid_before_parsing() {
    let dir = tempfile::tempdir().unwrap();
    let process = CredentialProcess::open(config(dir.path())).unwrap();
    let listener = bind(&process).unwrap();
    let socket_path = process
        .config()
        .passthrough
        .as_ref()
        .unwrap()
        .socket_path
        .clone();
    let task = tokio::spawn(serve_until(
        process,
        listener,
        std::future::pending(),
        current_uid().wrapping_add(1),
    ));
    let mut socket = UnixStream::connect(socket_path).await.unwrap();
    // No request bytes are necessary: credential refusal happens before parsing.
    let mut byte = [0];
    let closed = tokio::time::timeout(Duration::from_secs(1), socket.read(&mut byte))
        .await
        .unwrap();
    assert!(matches!(closed, Ok(0)) || closed.is_err());
    task.abort();
}

#[tokio::test]
async fn disconnect_during_an_upstream_call_frees_the_slot_immediately() {
    let mut fixture = Fixture::start().await;
    let harness = Harness::start(&fixture, |c| {
        c.passthrough.as_mut().unwrap().routes[0].max_in_flight = 1
    })
    .await;
    let socket = harness.connect("/v1/responses", b"{}", "").await;
    let held = fixture.next().await;
    drop(socket);
    let next = harness.call().await;
    let upstream = tokio::time::timeout(Duration::from_millis(300), fixture.next())
        .await
        .expect("disconnect frees the slot without waiting for the upstream timeout");
    upstream.reply.send(Reply::json()).ok().unwrap();
    assert_eq!(decoded(&next.await.unwrap()).0, 200);
    drop(held);
}

#[tokio::test]
async fn missing_retry_after_uses_account_backoff_and_resends() {
    let mut fixture = Fixture::start().await;
    let harness = Harness::start(&fixture, |_| {}).await;
    let call = harness.call().await;
    let upstream = fixture.next().await;
    let started = Instant::now();
    upstream.reply.send(Reply::status(429)).ok().unwrap();
    harness
        .health_where(|h| !h["routes"][0]["cooldown_until"].is_null())
        .await;
    let retry = fixture.next().await;
    assert!(started.elapsed() >= Duration::from_millis(900));
    retry.reply.send(Reply::json()).ok().unwrap();
    assert_eq!(decoded(&call.await.unwrap()).0, 200);
}

#[tokio::test]
async fn failed_forced_renewal_returns_unavailable_and_retains_the_fence() {
    let mut fixture = Fixture::start().await;
    let harness = Harness::start(&fixture, login_config).await;
    let call = harness.call().await;
    fixture
        .next()
        .await
        .reply
        .send(Reply::status(401))
        .ok()
        .unwrap();
    let exchange = fixture.next().await;
    assert!(exchange.head.starts_with("POST /oauth/token "));
    exchange.reply.send(Reply::status(400)).ok().unwrap();
    let (status, _, body) = decoded(&call.await.unwrap());
    assert_eq!(status, 503);
    assert_eq!(body, br#"{"error":{"code":"credential_unavailable"}}"#);
    harness
        .health_where(|h| h["routes"][0]["login"] == "login_required")
        .await;
    assert_eq!(decoded(&harness.call().await.await.unwrap()).0, 503);
    assert!(fixture.incoming.try_recv().is_err());
}

#[tokio::test]
async fn renewed_and_original_tokens_are_both_scrubbed_from_the_final_body() {
    let mut fixture = Fixture::start().await;
    let harness = Harness::start(&fixture, login_config).await;
    let call = harness.call().await;
    fixture
        .next()
        .await
        .reply
        .send(Reply::status(401))
        .ok()
        .unwrap();
    fixture.next().await.reply.send(renewal()).ok().unwrap();
    let upstream = fixture.next().await;
    let mut reply = Reply::json();
    reply.body=br#"{"text":"synthetic-old-access synthetic-new-access synthetic-old-refresh synthetic-new-refresh synthetic-account"}"#.to_vec();
    upstream.reply.send(reply).ok().unwrap();
    let (status, _, body) = decoded(&call.await.unwrap());
    assert_eq!(status, 200);
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap(),
        json!({"text":"[redacted] [redacted] [redacted] [redacted] [redacted]"})
    );
}

#[tokio::test]
async fn stream_progress_can_exceed_the_idle_timeout_without_a_total_deadline() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let (send, incoming) = mpsc::channel::<Upstream>(1);
    let task = tokio::spawn(async move {
        let _send = send;
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut head = Vec::new();
        while !head.ends_with(b"\r\n\r\n") {
            head.push(stream.read_u8().await.unwrap());
        }
        let mut body = [0; 2];
        stream.read_exact(&mut body).await.unwrap();
        let response = br#"{"text":"slow answer"}"#;
        stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",response.len()).as_bytes()).await.unwrap();
        for part in response.chunks(8) {
            stream.write_all(part).await.unwrap();
            if part.len() == 8 {
                tokio::time::sleep(Duration::from_millis(550)).await;
            }
        }
    });
    let fixture = Fixture {
        base,
        incoming,
        task,
    };
    let harness = Harness::start(&fixture, |_| {}).await;
    let (status, _, body) = decoded(&harness.call().await.await.unwrap());
    assert_eq!(status, 200);
    assert_eq!(body, br#"{"text":"slow answer"}"#);
}

#[tokio::test]
async fn account_slot_stays_held_after_response_headers_until_the_stream_finishes() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let (send, incoming) = mpsc::channel::<Upstream>(1);
    let (ready, ready_receive) = oneshot::channel();
    let task = tokio::spawn(async move {
        let _send = send;
        let (mut first, _) = listener.accept().await.unwrap();
        let mut head = Vec::new();
        while !head.ends_with(b"\r\n\r\n") {
            head.push(first.read_u8().await.unwrap());
        }
        let mut body = [0; 2];
        first.read_exact(&mut body).await.unwrap();
        first.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 15\r\nConnection: close\r\n\r\n{\"result\":").await.unwrap();
        ready.send(()).unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(60), listener.accept())
                .await
                .is_err(),
            "another request reached the provider before the first response body ended"
        );
        first.write_all(b"\"ok\"}").await.unwrap();
        drop(first);
        let (mut second, _) = listener.accept().await.unwrap();
        let mut head = Vec::new();
        while !head.ends_with(b"\r\n\r\n") {
            head.push(second.read_u8().await.unwrap());
        }
        second.read_exact(&mut body).await.unwrap();
        second.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 15\r\nConnection: close\r\n\r\n{\"result\":\"ok\"}").await.unwrap();
    });
    let mut fixture = Fixture {
        base,
        incoming,
        task,
    };
    let harness = Harness::start(&fixture, |c| {
        c.passthrough.as_mut().unwrap().routes[0].max_in_flight = 1
    })
    .await;
    let first = harness.call().await;
    ready_receive.await.unwrap();
    let second = harness.call().await;
    assert_eq!(decoded(&first.await.unwrap()).0, 200);
    assert_eq!(decoded(&second.await.unwrap()).0, 200);
    (&mut fixture.task).await.unwrap();
}

#[tokio::test]
async fn typed_provider_and_passthrough_spend_the_same_request_pacing_bucket() {
    use symbiotic_ai_runtime::{
        ModelBinding, ResponseCacheMode,
        model::{ChatMessage, ChatRequest, OpenAiCompatibleChatProvider},
    };
    let mut fixture = Fixture::start().await;
    let harness = Harness::start(&fixture, |c| {
        c.passthrough.as_mut().unwrap().routes[0].requests_per_minute = Some(60)
    })
    .await;
    let route = &harness.process.inner.passthrough.as_ref().unwrap().routes[0];
    let typed = harness
        .process
        .inner
        .runtime
        .chat(
            ModelBinding::new(OpenAiCompatibleChatProvider::new(
                "synthetic",
                "model",
                &fixture.base,
                "",
            ))
            .with_identity(BindingIdentity::new(
                &route.config.tenant,
                "typed_chat",
                "typed-v1",
                &route.config.account,
            ))
            .with_policy(route.config.policy())
            .with_answer_recovery(symbiotic_ai_runtime::AnswerRecovery::Off)
            .with_response_cache(ResponseCacheMode::Off),
        )
        .unwrap();
    let typed = tokio::spawn(async move {
        typed
            .chat(ChatRequest {
                messages: vec![ChatMessage {
                    role: "user".into(),
                    content: "synthetic input".into(),
                }],
                max_output_tokens: Some(8),
                temperature: None,
                response_format: None,
                role_binding: None,
                source: None,
                metadata: Value::Null,
            })
            .await
    });
    let first = fixture.next().await;
    let started = Instant::now();
    let mut reply = Reply::json();
    reply.body=br#"{"choices":[{"message":{"content":"typed answer"},"finish_reason":"stop"}],"usage":{"prompt_tokens":2,"completion_tokens":2}}"#.to_vec();
    first.reply.send(reply).ok().unwrap();
    assert_eq!(typed.await.unwrap().unwrap().text, "typed answer");
    let raw = harness.call().await;
    assert!(
        tokio::time::timeout(Duration::from_millis(40), fixture.incoming.recv())
            .await
            .is_err()
    );
    let next = fixture.next().await;
    assert!(started.elapsed() >= Duration::from_millis(900));
    next.reply.send(Reply::json()).ok().unwrap();
    assert_eq!(decoded(&raw.await.unwrap()).0, 200);
}

#[tokio::test]
async fn route_event_hold_limit_returns_a_visible_error_for_oversized_items() {
    let mut fixture = Fixture::start().await;
    for (limit, earlier_item_released) in [(1, false), (256, false), (256, true)] {
        let harness = Harness::start(&fixture, |config| {
            config.passthrough.as_mut().unwrap().routes[0].max_held_event_bytes = limit;
        })
        .await;
        let complete = b"data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"id\":\"complete\",\"type\":\"message\",\"content\":[]}}\n\n";
        let mut body = Vec::new();
        if earlier_item_released {
            body.extend_from_slice(complete);
        }
        // At 256 bytes, each event fits; together they exceed the item's bound.
        let delta = format!(
            "data: {{\"type\":\"response.output_text.delta\",\"item_id\":\"partial\",\"output_index\":1,\"content_index\":0,\"delta\":\"{KEY}\"}}\n\n"
        );
        for _ in 0..3 {
            body.extend_from_slice(delta.as_bytes());
        }
        body.extend_from_slice(b"data: {\"type\":\"response.completed\",\"response\":{}}\n\n");
        let call = harness.call().await;
        fixture
            .next()
            .await
            .reply
            .send(Reply {
                status: 200,
                headers: vec![("content-type".into(), "text/event-stream".into())],
                body,
            })
            .ok()
            .unwrap();
        let (status, headers, body) = decoded(&call.await.unwrap());
        if earlier_item_released {
            assert_eq!(status, 200);
            assert!(headers.contains("content-type: text/event-stream\r\n"));
            let mut expected = complete.to_vec();
            expected.extend_from_slice(
                b"event: error\ndata: {\"type\":\"error\",\"code\":\"max_held_event_bytes\"}\n\n",
            );
            assert_eq!(body, expected);
        } else {
            assert_eq!(status, 502);
            assert!(headers.contains("content-type: application/json\r\n"));
            assert_eq!(body, br#"{"error":{"code":"max_held_event_bytes"}}"#);
        }
    }
}

#[tokio::test]
async fn token_exchange_timeout_after_request_bytes_keeps_the_durable_fence() {
    let mut fixture = Fixture::start().await;
    let harness = Harness::start(&fixture, login_config).await;
    let call = harness.call().await;
    fixture
        .next()
        .await
        .reply
        .send(Reply::status(401))
        .ok()
        .unwrap();
    let exchange = fixture.next().await;
    assert!(exchange.head.starts_with("POST /oauth/token HTTP/1.1\r\n"));
    let (status, _, body) = decoded(&call.await.unwrap());
    assert_eq!(status, 503);
    assert_eq!(body, br#"{"error":{"code":"credential_unavailable"}}"#);
    harness
        .health_where(|h| h["routes"][0]["login"] == "login_required")
        .await;
    assert!(
        !fs::read(harness._dir.path().join("login.json.renewal"))
            .unwrap()
            .is_empty()
    );
    assert!(fixture.incoming.try_recv().is_err());
    drop(exchange);
}

#[tokio::test]
async fn discovery_timeout_before_exchange_leaves_an_empty_retryable_fence() {
    let mut fixture = Fixture::start().await;
    let harness = Harness::start(&fixture, |config| {
        login_config(config);
        let path = config.state_dir.parent().unwrap().join("login.json");
        let mut store: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        store["family"] = json!("xai_grok");
        store["issuer"] = json!("https://auth.x.ai");
        store["client_id"] = json!("b1a00492-073a-47ea-816f-4c329264a828");
        store["expires_at"] = json!(Utc::now().timestamp() + 60);
        write_private(&path, &serde_json::to_vec(&store).unwrap());
        config.passthrough.as_mut().unwrap().routes[0].secret = SecretSource::LoginFile {
            path,
            family: LoginFamily::XaiGrok,
        };
    })
    .await;
    let call = harness.call().await;
    let discovery = fixture.next().await;
    assert!(
        discovery
            .head
            .starts_with("GET /.well-known/openid-configuration HTTP/1.1\r\n")
    );
    let (status, _, body) = decoded(&call.await.unwrap());
    assert_eq!(status, 503);
    assert_eq!(body, br#"{"error":{"code":"credential_unavailable"}}"#);
    harness
        .health_where(|h| h["routes"][0]["login"] == "renewal_failed")
        .await;
    assert!(
        fs::read(harness._dir.path().join("login.json.renewal"))
            .unwrap()
            .is_empty()
    );
    assert!(fixture.incoming.try_recv().is_err());
    drop(discovery);
}

#[tokio::test]
async fn rate_limit_and_unauthorized_resends_charge_pacing_while_keeping_the_slot() {
    for status in [429, 401] {
        let mut fixture = Fixture::start().await;
        let harness = Harness::start(&fixture, |config| {
            if status == 401 {
                login_config(config);
            }
            let route = &mut config.passthrough.as_mut().unwrap().routes[0];
            route.requests_per_minute = Some(60);
            route.max_in_flight = 1;
        })
        .await;
        let call = harness.call().await;
        let first = fixture.next().await;
        let body = first.body.clone();
        let started = Instant::now();
        first
            .reply
            .send(if status == 429 {
                Reply::rate(0)
            } else {
                Reply::status(401)
            })
            .ok()
            .unwrap();
        if status == 401 {
            let exchange = fixture.next().await;
            assert!(exchange.head.starts_with("POST /oauth/token HTTP/1.1\r\n"));
            exchange.reply.send(renewal()).ok().unwrap();
        }
        let account = harness.account();
        assert!(
            tokio::time::timeout(Duration::from_millis(40), account.acquire())
                .await
                .is_err()
        );
        let resend = fixture.next().await;
        assert!(started.elapsed() >= Duration::from_millis(900));
        assert_eq!(resend.body, body);
        resend.reply.send(Reply::json()).ok().unwrap();
        assert_eq!(decoded(&call.await.unwrap()).0, 200);
        let available = tokio::time::timeout(Duration::from_millis(200), account.acquire())
            .await
            .unwrap()
            .unwrap();
        drop(available);
        assert!(fixture.incoming.try_recv().is_err());
    }
}

#[tokio::test]
async fn unusable_login_after_slot_cooldown_pacing_or_gate_wait_never_dispatches() {
    for state in ["login_required", "renewal_failed"] {
        for waiting in ["slot", "cooldown", "pacing", "gate"] {
            let mut fixture = Fixture::start().await;
            let harness = Harness::start(&fixture, |config| {
                login_config(config);
                let route = &mut config.passthrough.as_mut().unwrap().routes[0];
                route.max_in_flight = if waiting == "slot" { 1 } else { 2 };
                if waiting == "pacing" {
                    route.requests_per_minute = Some(60);
                }
            })
            .await;
            let first = harness.call().await;
            fixture
                .next()
                .await
                .reply
                .send(Reply::status(401))
                .ok()
                .unwrap();
            fixture.next().await.reply.send(renewal()).ok().unwrap();
            let rejected = fixture.next().await;
            if waiting == "cooldown" {
                harness
                    .account()
                    .note_cooldown(Utc::now() + chrono::Duration::milliseconds(200))
                    .await
                    .unwrap();
            }
            let held_gate = if waiting == "gate" {
                let symbiotic_ai_runtime::model::ModelRequestAdmission::Ready(grant) =
                    harness.account().request_admission().await.unwrap()
                else {
                    panic!("unpaced healthy account must grant dispatch");
                };
                Some(grant)
            } else {
                None
            };
            let waiting_call = harness.call().await;
            assert!(
                tokio::time::timeout(Duration::from_millis(40), fixture.incoming.recv())
                    .await
                    .is_err()
            );
            if state == "login_required" {
                write_private(
                    &harness._dir.path().join("login.json.renewal"),
                    b"uncertain_rotation",
                );
                rejected.reply.send(Reply::json()).ok().unwrap();
            } else {
                rejected.reply.send(Reply::status(401)).ok().unwrap();
            }
            assert_eq!(
                decoded(&first.await.unwrap()).0,
                if state == "login_required" { 200 } else { 401 }
            );
            drop(held_gate);
            let (status, _, body) = decoded(&waiting_call.await.unwrap());
            assert_eq!(status, 503);
            assert_eq!(body, br#"{"error":{"code":"credential_unavailable"}}"#);
            assert!(fixture.incoming.try_recv().is_err());
        }
    }
}

#[tokio::test]
async fn concurrent_expiring_callers_share_a_failed_renewal_and_send_no_provider_request() {
    let mut fixture = Fixture::start().await;
    let harness = Harness::start(&fixture, |config| {
        login_config(config);
        let path = config.state_dir.parent().unwrap().join("login.json");
        let mut store: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        store["expires_at"] = json!(Utc::now().timestamp() + 60);
        write_private(&path, &serde_json::to_vec(&store).unwrap());
    })
    .await;
    let first = harness.call().await;
    let exchange = fixture.next().await;
    assert!(exchange.head.starts_with("POST /oauth/token HTTP/1.1\r\n"));
    let second = harness.call().await;
    exchange.reply.send(Reply::status(400)).ok().unwrap();
    for call in [first, second] {
        let (status, _, body) = decoded(&call.await.unwrap());
        assert_eq!(status, 503);
        assert_eq!(body, br#"{"error":{"code":"credential_unavailable"}}"#);
    }
    assert!(fixture.incoming.try_recv().is_err());
}

#[tokio::test]
async fn passthrough_contract_parser_refuses_bare_newlines_before_or_within_the_request() {
    let directory = tempfile::tempdir().unwrap();
    let process = CredentialProcess::open(config(directory.path())).unwrap();
    // Exercise the HTTP connection boundary without a listening socket or provider.
    // A valid unknown path returns 404. Malformed framing must return 400 instead.
    for (bytes, status) in [
        (
            b"POST /unknown HTTP/1.1\r\nHost: local\r\nContent-Length: 0\r\n\r\n".as_slice(),
            404,
        ),
        (
            b"\nPOST /unknown HTTP/1.1\r\nHost: local\r\nContent-Length: 0\r\n\r\n",
            400,
        ),
        (
            b"\r\nPOST /unknown HTTP/1.1\r\nHost: local\r\nContent-Length: 0\r\n\r\n",
            400,
        ),
        (
            b"POST /unknown HTTP/1.1\nHost: local\r\nContent-Length: 0\r\n\r\n",
            400,
        ),
        (
            b"POST /unknown HTTP/1.1\r\n\nHost: local\r\nContent-Length: 0\r\n\r\n",
            400,
        ),
    ] {
        let (mut caller, server) = UnixStream::pair().unwrap();
        caller.write_all(bytes).await.unwrap();
        let owner = process.clone();
        let handler = tokio::spawn(async move { connection(&owner, server).await });
        let (actual, _, body) = decoded(&read(caller).await);
        assert_eq!(actual, status, "framing: {bytes:?}");
        assert_eq!(
            body,
            if status == 404 {
                br#"{"error":{"code":"route_not_found"}}"#.as_slice()
            } else {
                br#"{"error":{"code":"invalid_request"}}"#
            }
        );
        handler.await.unwrap().unwrap();
    }
}

#[tokio::test]
async fn retry_policy_header_selects_named_table_and_unknown_name_is_local_400() {
    let mut fixture = Fixture::start().await;
    let harness = Harness::start(&fixture, |c| {
        c.passthrough.as_mut().unwrap().retry_policies.insert(
            "stop".into(),
            HashMap::from([(
                ErrorClass::Throttle,
                RetryRule {
                    action: RetryAction::Fail,
                    max_attempts: None,
                    backoff: Default::default(),
                },
            )]),
        );
    })
    .await;
    let caller = harness
        .connect(
            "/v1/responses",
            b"{}",
            "x-symbiotic-retry-policy: unknown\r\n",
        )
        .await;
    assert_eq!(decoded(&read(caller).await).0, 400);
    assert!(fixture.incoming.try_recv().is_err());
    let caller = harness
        .connect("/v1/responses", b"{}", "x-symbiotic-retry-policy: stop\r\n")
        .await;
    let first = fixture.next().await;
    assert!(!first.head.contains("x-symbiotic-retry-policy"));
    first.reply.send(Reply::rate(0)).ok().unwrap();
    let (status, _, body) = decoded(&read(caller).await);
    assert_eq!(
        (status, body),
        (429, br#"{"error":{"code":"upstream_status"}}"#.to_vec())
    );
    assert!(fixture.incoming.try_recv().is_err());
}

#[tokio::test]
async fn throttle_503_and_429_resend_before_first_byte_and_health_counts_codes() {
    for (status, code) in [(429, "rate_limit_exceeded"), (503, "server_is_overloaded")] {
        let mut fixture = Fixture::start().await;
        let harness = Harness::start(&fixture, |_| {}).await;
        let caller = harness
            .connect(
                "/v1/responses",
                b"{}",
                "x-symbiotic-request-id: synthetic-request\r\n",
            )
            .await;
        let first = fixture.next().await;
        assert!(!first.head.contains("x-symbiotic-request-id"));
        let mut reply = Reply::status(status);
        reply.headers.push(("retry-after".into(), "1".into()));
        reply.body = serde_json::to_vec(&json!({"error":{"code":code}})).unwrap();
        first.reply.send(reply).ok().unwrap();
        let health = harness
            .health_where(|h| {
                h["routes"][0]["waiting"]
                    .as_array()
                    .is_some_and(|a| !a.is_empty())
            })
            .await;
        assert_eq!(health["routes"][0]["retried_events"][code], 1);
        assert_eq!(
            health["routes"][0]["waiting"][0]["request_id"],
            "synthetic-request"
        );
        assert_eq!(health["routes"][0]["waiting"][0]["reason"], "cooldown");
        assert!(
            health["routes"][0]["waiting"][0]["since"].as_i64().unwrap() <= Utc::now().timestamp()
        );
        fixture.next().await.reply.send(Reply::json()).ok().unwrap();
        let (status, headers, body) = decoded(&read(caller).await);
        assert_eq!((status, body), (200, br#"{"result":"ok"}"#.to_vec()));
        let waited: u64 = headers
            .lines()
            .find_map(|l| l.strip_prefix("x-symbiotic-queue-waited-ms: "))
            .unwrap()
            .parse()
            .unwrap();
        assert!(waited >= 800);
        harness
            .health_where(|h| h["routes"][0]["waiting"] == json!([]))
            .await;
    }
}

#[tokio::test]
async fn server_stops_on_third_attempt_and_switch_without_fallback_fails() {
    for switch in [false, true] {
        let mut fixture = Fixture::start().await;
        let harness = Harness::start(&fixture, |c| {
            let route = &mut c.passthrough.as_mut().unwrap().routes[0];
            route.retry_policy.insert(
                ErrorClass::Server,
                RetryRule {
                    action: if switch {
                        RetryAction::Switch
                    } else {
                        RetryAction::RetrySame
                    },
                    max_attempts: Some(3),
                    backoff: RetryBackoff {
                        base_delay_ms: Some(1),
                    },
                },
            );
        })
        .await;
        let call = harness.call().await;
        for _ in 0..if switch { 1 } else { 3 } {
            fixture
                .next()
                .await
                .reply
                .send(Reply::status(500))
                .ok()
                .unwrap();
        }
        let (status, _, body) = decoded(&call.await.unwrap());
        assert_eq!(
            (status, body),
            (500, br#"{"error":{"code":"upstream_status"}}"#.to_vec())
        );
        assert!(fixture.incoming.try_recv().is_err());
    }
}

#[tokio::test]
async fn provider_rate_headers_on_success_reach_health_and_caller_without_promotion() {
    let mut fixture = Fixture::start().await;
    let harness = Harness::start(&fixture, |_| {}).await;
    let call = harness.call().await;
    let first = fixture.next().await;
    let mut reply = Reply::json();
    reply.headers.extend([
        ("x-server-family-primary-used-percent".into(), "17".into()),
        ("x-codex-promo-message".into(), "promotion".into()),
    ]);
    first.reply.send(reply).ok().unwrap();
    let (status, head, _) = decoded(&call.await.unwrap());
    assert_eq!(status, 200);
    assert!(head.contains("x-server-family-primary-used-percent: 17\r\n"));
    assert!(!head.contains("promo"));
    let health = harness
        .health_where(|h| !h["routes"][0]["rate_limits_seen_at"].is_null())
        .await;
    assert_eq!(
        health["routes"][0]["rate_limits"],
        json!({"x-server-family-primary-used-percent":"17"})
    );
}

#[tokio::test]
async fn new_sessions_flow_freely_then_hold_and_probe_without_blocking_seen_sessions() {
    let mut fixture = Fixture::start().await;
    let harness = Harness::start(&fixture, |c| {
        let route = &mut c.passthrough.as_mut().unwrap().routes[0];
        route.max_in_flight = 8;
        route.new_session = Some(NewSessionConfig {
            session_headers: vec!["session-id".into(), "thread-id".into()],
        });
    })
    .await;
    let seen = harness
        .connect("/v1/responses", b"{}", "session-id: seen\r\n")
        .await;
    fixture.next().await.reply.send(Reply::json()).ok().unwrap();
    assert_eq!(decoded(&read(seen).await).0, 200);
    let mut calls = Vec::new();
    for i in 0..3 {
        calls.push(
            harness
                .connect(
                    "/v1/responses",
                    b"{}",
                    &format!("session-id: freely-{i}\r\n"),
                )
                .await,
        );
    }
    let mut sent = Vec::new();
    for _ in 0..3 {
        sent.push(fixture.next().await);
    }
    for sent in sent {
        sent.reply.send(Reply::json()).ok().unwrap();
    }
    for call in calls {
        assert_eq!(decoded(&read(call).await).0, 200);
    }
    let first = harness
        .connect(
            "/v1/responses",
            b"{}",
            "session-id: refused\r\nx-symbiotic-request-id: refused\r\n",
        )
        .await;
    fixture
        .next()
        .await
        .reply
        .send(Reply::rate(1))
        .ok()
        .unwrap();
    let h = harness
        .health_where(|h| !h["routes"][0]["new_session_hold_until"].is_null())
        .await;
    assert!(h["routes"][0]["cooldown_until"].is_null());
    let second = harness
        .connect(
            "/v1/responses",
            b"{}",
            "session-id: pending\r\nx-symbiotic-request-id: pending\r\n",
        )
        .await;
    let seen = harness
        .connect("/v1/responses", b"{}", "session-id: seen\r\n")
        .await;
    let fast = fixture.next().await;
    assert!(fast.head.contains("session-id: seen\r\n"));
    fast.reply.send(Reply::json()).ok().unwrap();
    assert_eq!(decoded(&read(seen).await).0, 200);
    let probe = fixture.next().await;
    assert!(
        tokio::time::timeout(Duration::from_millis(30), fixture.incoming.recv())
            .await
            .is_err()
    );
    probe.reply.send(Reply::rate(1)).ok().unwrap();
    harness
        .health_where(|h| h["routes"][0]["retried_events"]["429"] == 2)
        .await;
    assert!(
        tokio::time::timeout(Duration::from_millis(30), fixture.incoming.recv())
            .await
            .is_err()
    );
    let probe = fixture.next().await;
    assert!(
        tokio::time::timeout(Duration::from_millis(30), fixture.incoming.recv())
            .await
            .is_err()
    );
    probe.reply.send(Reply::json()).ok().unwrap();
    fixture.next().await.reply.send(Reply::json()).ok().unwrap();
    assert_eq!(decoded(&read(first).await).0, 200);
    assert_eq!(decoded(&read(second).await).0, 200);
}

#[tokio::test]
async fn mid_answer_failure_and_disconnect_continue_with_exact_items_and_one_joined_answer() {
    for disconnect in [false, true] {
        let mut fixture = Fixture::start().await;
        let harness = Harness::start(&fixture, |c| {
            let route = &mut c.passthrough.as_mut().unwrap().routes[0];
            for class in [ErrorClass::Network, ErrorClass::Throttle] {
                route.retry_policy.insert(
                    class,
                    RetryRule {
                        action: RetryAction::RetrySame,
                        max_attempts: Some(3),
                        backoff: RetryBackoff {
                            base_delay_ms: Some(1),
                        },
                    },
                );
            }
        })
        .await;
        let original = json!({"input":[{"role":"user","content":"question"}],"store":false,"include":["reasoning.encrypted_content"]});
        let caller = harness
            .connect("/v1/responses", &serde_json::to_vec(&original).unwrap(), "")
            .await;
        let first = fixture.next().await;
        let reasoning = json!({"id":"reason","type":"reasoning","encrypted_content":"opaque-encrypted-reasoning","summary":[],"extra":17});
        let values = vec![
            json!({"type":"response.created","sequence_number":0,"response":{"id":"first"}}),
            json!({"type":"response.output_item.done","sequence_number":1,"output_index":0,"item":reasoning}),
            json!({"type":"response.output_text.delta","sequence_number":2,"output_index":1,"item_id":"partial","delta":"must not escape"}),
        ];
        let mut stream: String = values
            .into_iter()
            .map(|v| format!("data: {v}\n\n"))
            .collect();
        if !disconnect {
            stream.push_str(&format!("data: {}\n\n",json!({"type":"response.failed","sequence_number":3,"response":{"id":"first","error":{"code":"slow_down"},"usage":{"input_tokens":5,"output_tokens":2}}})));
        }
        first
            .reply
            .send(Reply {
                status: 200,
                headers: vec![("content-type".into(), "text/event-stream".into())],
                body: stream.into_bytes(),
            })
            .ok()
            .unwrap();
        let continuation = fixture.next().await;
        let mut expected = original.clone();
        expected["input"]
            .as_array_mut()
            .unwrap()
            .push(reasoning.clone());
        assert_eq!(
            serde_json::from_slice::<Value>(&continuation.body).unwrap(),
            expected
        );
        let answer = json!({"id":"answer","type":"message","content":[{"type":"output_text","text":"complete"}]});
        let values = vec![
            json!({"type":"response.created","sequence_number":0,"response":{"id":"second"}}),
            json!({"type":"response.in_progress","sequence_number":1,"response":{"id":"second"}}),
            json!({"type":"response.output_item.done","sequence_number":2,"output_index":0,"item":answer}),
            json!({"type":"response.completed","sequence_number":3,"response":{"id":"second","output":[answer],"usage":{"input_tokens":7,"output_tokens":3}}}),
        ];
        continuation
            .reply
            .send(Reply {
                status: 200,
                headers: vec![("content-type".into(), "text/event-stream".into())],
                body: values
                    .into_iter()
                    .flat_map(|v| format!("data: {v}\n\n").into_bytes())
                    .collect(),
            })
            .ok()
            .unwrap();
        let (status, _, body) = decoded(&read(caller).await);
        assert_eq!(status, 200);
        let events = ResponsesAdapter::default().feed(&body, 100_000).unwrap();
        let values: Vec<_> = events.iter().map(|e| e.record().value.clone()).collect();
        assert_eq!(
            values
                .iter()
                .map(|v| v["type"].as_str().unwrap())
                .collect::<Vec<_>>(),
            [
                "response.created",
                "response.output_item.done",
                "response.output_item.done",
                "response.completed"
            ]
        );
        assert_eq!(
            values
                .iter()
                .map(|v| v["sequence_number"].as_u64().unwrap())
                .collect::<Vec<_>>(),
            [0, 1, 2, 3]
        );
        assert_eq!(values[2]["output_index"], 1);
        assert_eq!(values[3]["response"]["id"], "first");
        assert_eq!(values[3]["response"]["output"], json!([reasoning, answer]));
        assert_eq!(
            values[3]["response"]["usage"],
            if disconnect {
                json!({"input_tokens":7,"output_tokens":3,"total_tokens":10})
            } else {
                json!({"input_tokens":12,"output_tokens":5,"total_tokens":17})
            }
        );
        let code = if disconnect {
            "upstream_disconnect"
        } else {
            "slow_down"
        };
        harness
            .health_where(|h| h["routes"][0]["retried_events"][code] == 1)
            .await;
    }
}

#[tokio::test]
async fn unreachable_provider_recovers_after_several_backoffs_with_one_late_answer() {
    let reserved = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = reserved.local_addr().unwrap();
    drop(reserved);
    let mut absent = Fixture::without_provider();
    absent.base = format!("http://{address}");
    let (send, mut failures) = mpsc::unbounded_channel();
    let hook: RetryHook = Arc::new(move |c| {
        if c.class == ErrorClass::Network {
            let _ = send.send(c.attempt);
        }
        None
    });
    let harness = Harness::with_hook(
        &absent,
        |c| {
            c.passthrough.as_mut().unwrap().routes[0]
                .retry_policy
                .insert(
                    ErrorClass::Network,
                    RetryRule {
                        action: RetryAction::RetrySame,
                        max_attempts: Some(8),
                        backoff: RetryBackoff {
                            base_delay_ms: Some(10),
                        },
                    },
                );
        },
        Some(hook),
    )
    .await;
    let call = harness.call().await;
    for attempt in 1..=3 {
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), failures.recv())
                .await
                .unwrap(),
            Some(attempt)
        );
    }
    let mut restored = Fixture::at(&address.to_string()).await;
    restored
        .next()
        .await
        .reply
        .send(Reply::json())
        .ok()
        .unwrap();
    assert_eq!(decoded(&call.await.unwrap()).2, br#"{"result":"ok"}"#);
}

#[tokio::test]
async fn slot_wait_publishes_request_identity_and_wait_header_then_removes_waiter() {
    let dir = tempfile::tempdir().unwrap();
    let mut settings = config(dir.path());
    login_config(&mut settings);
    settings.passthrough.as_mut().unwrap().routes[0].max_in_flight = 1;
    let process = CredentialProcess::open(settings).unwrap();
    let pass = process.inner.passthrough.as_ref().unwrap();
    let permit = pass.routes[0].account.acquire().await.unwrap();
    let (mut caller, server) = UnixStream::pair().unwrap();
    caller.write_all(b"POST /v1/responses HTTP/1.1\r\nHost: local\r\nContent-Length: 2\r\nx-symbiotic-request-id: synthetic-job\r\n\r\n{}").await.unwrap();
    let owner = process.clone();
    let handler = tokio::spawn(async move { connection(&owner, server).await });
    tokio::time::timeout(Duration::from_secs(2), pass.changed.notified())
        .await
        .unwrap();
    let settings = process.config().passthrough.as_ref().unwrap();
    pass.publish_health(settings, "running").await.unwrap();
    let health: Value =
        serde_json::from_slice(&fs::read(settings.health_path.as_ref().unwrap()).unwrap()).unwrap();
    let waiting = &health["routes"][0]["waiting"][0];
    let since = waiting["since"].as_i64().unwrap();
    assert!(since <= Utc::now().timestamp());
    assert_eq!(
        waiting,
        &json!({"request_id":"synthetic-job","reason":"slot","since":since})
    );
    write_private(
        &dir.path().join("login.json.renewal"),
        b"uncertain_rotation",
    );
    drop(permit);
    let (status, headers, body) = decoded(&read(caller).await);
    assert_eq!(
        (status, body),
        (
            503,
            br#"{"error":{"code":"credential_unavailable"}}"#.to_vec()
        )
    );
    let waited = headers
        .lines()
        .find_map(|l| l.strip_prefix("x-symbiotic-queue-waited-ms: "))
        .unwrap();
    assert!(waited.parse::<u64>().is_ok());
    handler.await.unwrap().unwrap();
    pass.publish_health(settings, "running").await.unwrap();
    let health: Value =
        serde_json::from_slice(&fs::read(settings.health_path.as_ref().unwrap()).unwrap()).unwrap();
    assert_eq!(health["routes"][0]["waiting"], json!([]));
}
