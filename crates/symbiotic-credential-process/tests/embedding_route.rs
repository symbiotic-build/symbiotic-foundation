#![cfg(unix)]

use std::{
    os::unix::fs::PermissionsExt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use symbiotic_ai_runtime::model::{DEFAULT_MAX_RESPONSE_BYTES, ModelAdapter};
use symbiotic_credential_process::{
    CredentialProcess, InProcessEgressClient, ProcessConfig, RequestBudget, RouteConfig,
    RouteProvider, secrets::SecretSource, validate_routes,
};
use symbiotic_egress::*;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    task::JoinHandle,
};

const INPUTS: usize = 250;
const DIMENSIONS: usize = 1024;
const RESPONSE_BYTES: usize = 8 * 1024 * 1024;
const FRAME_BYTES: u32 = 36 * 1024 * 1024;
const DEADLINE: Duration = Duration::from_secs(5);

fn batch() -> ProviderPayload {
    let inputs: Vec<_> = (0..INPUTS)
        .map(|index| format!("{index:03}{}", "x".repeat(125)))
        .collect();
    assert_eq!(inputs.iter().map(String::len).sum::<usize>(), 32_000);
    ProviderPayload::Embedding(EmbeddingRequest {
        inputs,
        dimensions: None, // Exercise the route's default, rather than overriding it.
        task: None,
        role_binding: None,
        source: None,
        metadata: serde_json::Value::Null,
    })
}

fn component(input: usize, dimension: usize) -> f32 {
    (input * DIMENSIONS + dimension + 1) as f32 / 262_144.0
}

fn shuffled_response() -> String {
    let data: Vec<_> = (0..INPUTS)
        // Multiplication by 73 permutes all 250 indexes; no vector is in input order.
        .map(|position| {
            let index = (position * 73 + 17) % INPUTS;
            let embedding: Vec<_> = (0..DIMENSIONS)
                .map(|dimension| component(index, dimension))
                .collect();
            serde_json::json!({"index": index, "embedding": embedding})
        })
        .collect();
    serde_json::json!({"data": data, "usage": {"prompt_tokens": 32_000}}).to_string()
}

struct Fixture {
    _dir: tempfile::TempDir,
    config: ProcessConfig,
    key: AdmissionKey,
    calls: Arc<AtomicUsize>,
    requests: Arc<Mutex<Vec<serde_json::Value>>>,
    server: JoinHandle<()>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl Fixture {
    async fn new(status: u16, body: String) -> Self {
        let dir = tempfile::tempdir().unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        // Generate the test admission key at run time; no credential is embedded or logged.
        let key_bytes = uuid::Uuid::new_v4().to_string().into_bytes();
        let key_path = dir.path().join("admission");
        std::fs::write(&key_path, &key_bytes).unwrap();
        std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600)).unwrap();
        let key = AdmissionKey::new(key_bytes).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let count = calls.clone();
        let captured = requests.clone();
        let server = tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                tokio::time::timeout(DEADLINE, async {
                    let mut data = Vec::new();
                    loop {
                        let mut chunk = [0; 4096];
                        let read = stream.read(&mut chunk).await.unwrap();
                        assert_ne!(read, 0, "request ended before its body");
                        data.extend_from_slice(&chunk[..read]);
                        assert!(data.len() <= 1024 * 1024, "fixture request bound");
                        if let Some(end) = data.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                            let headers = String::from_utf8_lossy(&data[..end]);
                            let length = headers
                                .lines()
                                .find_map(|line| {
                                    line.to_ascii_lowercase()
                                        .strip_prefix("content-length: ")
                                        .map(|length| length.parse::<usize>().unwrap())
                                })
                                .expect("request content-length");
                            if data.len() >= end + 4 + length {
                                assert!(headers.starts_with("POST /v1/embeddings HTTP/1.1"));
                                captured.lock().unwrap().push(
                                    serde_json::from_slice(&data[end + 4..end + 4 + length]).unwrap(),
                                );
                                count.fetch_add(1, Ordering::SeqCst);
                                break;
                            }
                        }
                    }
                    let headers = format!(
                        "HTTP/1.1 {status} fixture\r\nContent-Type: application/json\r\nRetry-After: 7\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    stream.write_all(headers.as_bytes()).await.unwrap();
                    // Size refusal may close the connection immediately after the headers.
                    let _ = stream.write_all(body.as_bytes()).await;
                })
                .await
                .expect("loopback request must finish within five seconds");
            }
        });
        // The true model width is unverified by repository evidence. Use the smallest
        // full-vector ceiling accepted above 1024; this fixture only returns 1024.
        let provider = RouteProvider::CompatibleEmbedding {
            adapter: ModelAdapter::OpenAiEmbedding,
            operator: "openrouter".into(),
            dimensions: DIMENSIONS,
            embedding_full_dimensions: DIMENSIONS + 1,
            embedding_input_tokens: 32_000,
        };
        // Omit max_response_bytes to exercise the public deserialization default.
        let route: RouteConfig = serde_json::from_value(serde_json::json!({
            "tenant": "tenant", "account": "account", "account_sharing_key": null,
            "max_attempts": 3, "route": "embedding", "secret_ref": "",
            "secret": {"backend": "none"}, "destination": format!("http://{address}/v1"),
            "model": "qwen/qwen3-embedding-8b",
            "provider": provider,
            "allow_loopback_http": true, "max_field_bytes": 1024, "max_output_tokens": 1,
            "max_in_flight": 1, "requests_per_minute": null, "input_units_per_minute": null,
            "timeout_seconds": 2
        }))
        .unwrap();
        let config = ProcessConfig {
            log: None,
            version: PROTOCOL_VERSION,
            state_dir: dir.path().join("state"),
            socket_path: dir.path().join("egress.sock"),
            admission_key: SecretSource::OwnerOnlyFile { path: key_path },
            max_secret_bytes: 4096,
            max_frame_bytes: 8 * 1024 * 1024,
            max_connections: 1,
            io_timeout_seconds: 2,
            clock_rollback_warning_tolerance_seconds: 5,
            jobs: Default::default(),
            job_runner: Default::default(),
            passthrough: None,
            routes: vec![route],
        };
        Self {
            _dir: dir,
            config,
            key,
            calls,
            requests,
            server,
        }
    }

    async fn client(&self) -> InProcessEgressClient {
        assert_eq!(
            validate_routes(&self.config.routes, self.config.max_frame_bytes),
            Ok(())
        );
        let client =
            InProcessEgressClient::new(CredentialProcess::open(self.config.clone()).unwrap());
        let grant = self
            .key
            .sign_grant_revision(GrantRevision {
                tenant: "tenant".into(),
                incarnation: "incarnation".into(),
                revision: 1,
            })
            .unwrap();
        assert!(matches!(
            exchange(&client, Operation::PublishGrantRevision(grant)).await,
            Reply::GrantRevisionPublished
        ));
        client
    }

    async fn send(&self, client: &InProcessEgressClient, invocation: &str) -> DispatchResult {
        let payload = batch();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let route = &self.config.routes[0];
        let admission = self
            .key
            .sign_attempt(DurableAttempt {
                tenant: route.tenant.clone(),
                incarnation: "incarnation".into(),
                invocation_id: invocation.into(),
                job_queue: None,
                attempt_ordinal: 1,
                record_sequence: 1,
                recorded_at: now,
                expires_at: now + 60,
                recovery_expires_at: now + 120,
                caller_binding: "caller".into(),
                route: route.route.clone(),
                destination: route.destination.clone(),
                model: route.model.clone(),
                method: "POST".into(),
                secret_ref: route.secret_ref.clone(),
                manifest_ref: "manifest".into(),
                input_manifest_digest: "a".repeat(64),
                input_digest: payload.digest().unwrap(),
                grant_revision: 1,
            })
            .unwrap();
        let Reply::Permit(grant) =
            exchange(client, Operation::IssuePermit(Box::new(admission.clone()))).await
        else {
            panic!("expected permit");
        };
        let Reply::Dispatched(result) = exchange(
            client,
            Operation::InjectProviderCredential(Box::new(InjectProviderCredential {
                operation_version: PROTOCOL_VERSION,
                admission,
                permit: grant.permit,
                payload,
            })),
        )
        .await
        else {
            panic!("expected dispatch result");
        };
        result
    }

    fn assert_sends(&self, expected: usize) {
        assert!(!self.server.is_finished(), "loopback server failed");
        assert_eq!(self.calls.load(Ordering::SeqCst), expected);
        let requests = self.requests.lock().unwrap();
        assert_eq!(requests.len(), expected);
        let ProviderPayload::Embedding(batch) = batch() else {
            unreachable!()
        };
        for request in requests.iter() {
            assert_eq!(request["model"], "qwen/qwen3-embedding-8b");
            assert_eq!(request["dimensions"], DIMENSIONS);
            assert_eq!(request["input"], serde_json::json!(batch.inputs));
            assert!(request.get("input_type").is_none());
        }
    }
}

async fn exchange(client: &InProcessEgressClient, operation: Operation) -> Reply {
    client
        .exchange(Request {
            version: PROTOCOL_VERSION,
            operation,
        })
        .await
        .unwrap()
        .result
        .unwrap()
}

#[tokio::test]
async fn full_batch_requires_larger_response_and_frame_bounds_and_preserves_input_order() {
    let started = Instant::now();
    tokio::time::timeout(DEADLINE, async {
        let body = shuffled_response();
        let measured = body.len();
        assert!(measured > DEFAULT_MAX_RESPONSE_BYTES);
        assert!(measured <= RESPONSE_BYTES);
        let fixture = Fixture::new(200, body.clone()).await;
        assert_eq!(fixture.config.routes[0].max_response_bytes, DEFAULT_MAX_RESPONSE_BYTES);
        let client = fixture.client().await;
        let refused = fixture.send(&client, "default-bound").await;
        assert_eq!(refused.error, Some(EgressError::Provider { status: None }));
        assert_eq!(serde_json::to_value(&refused.diagnostics).unwrap(),
            serde_json::json!(["max_response_bytes_exceeded"]));
        assert!(refused.output.is_none());
        assert_eq!(refused.receipt.spend_state, SpendState::Unknown);
        assert!(refused.receipt_persisted);
        fixture.assert_sends(1);

        let mut fixture = Fixture::new(200, body).await;
        fixture.config.routes[0].max_response_bytes = RESPONSE_BYTES;
        assert_eq!(validate_routes(&fixture.config.routes, fixture.config.max_frame_bytes),
            Err(EgressError::InvalidFrameConfiguration));
        fixture.config.max_frame_bytes = FRAME_BYTES;
        let client = fixture.client().await;
        let accepted = fixture.send(&client, "raised-bound").await;
        assert_eq!(accepted.error, None);
        assert!(accepted.receipt_persisted);
        let Some(ProviderOutput::Embedding { vectors, dimensions }) = accepted.output else {
            panic!("expected embedding output");
        };
        assert_eq!(dimensions, DIMENSIONS);
        assert_eq!(vectors.len(), INPUTS);
        for (input, vector) in vectors.iter().enumerate() {
            assert_eq!(vector.len(), DIMENSIONS);
            for (dimension, value) in vector.iter().enumerate() {
                assert_eq!(*value, component(input, dimension));
            }
        }
        fixture.assert_sends(1);
        eprintln!("full batch: response_bytes={measured}, default=max_response_bytes_exceeded, raised=accepted, sends=1+1, elapsed={:?}", started.elapsed());
    }).await.expect("batch size and ordering regression must finish within five seconds");
}

#[tokio::test]
async fn rate_limited_batch_preserves_retry_after_without_a_second_send() {
    let started = Instant::now();
    tokio::time::timeout(DEADLINE, async {
        let fixture = Fixture::new(429, "{}".into()).await;
        let client = fixture.client().await;
        let result = fixture.send(&client, "rate-limited").await;
        assert_eq!(
            result.error,
            Some(EgressError::RateLimited {
                retry_after_seconds: Some(7)
            })
        );
        assert!(result.output.is_none());
        assert_eq!(result.receipt.spend_state, SpendState::Unknown);
        assert!(result.receipt_persisted);
        fixture.assert_sends(1);
        eprintln!(
            "429 batch: retry_after_seconds=7, sends=1, elapsed={:?}",
            started.elapsed()
        );
    })
    .await
    .expect("rate-limit regression must finish within five seconds");
}

#[tokio::test]
async fn identical_batch_budget_refuses_after_three_failed_sends() {
    let started = Instant::now();
    tokio::time::timeout(DEADLINE, async {
        // Both a provider rejection and a response-size refusal debit exactly once.
        for (status, body, diagnostics) in [
            (400, "{}".into(), serde_json::json!([])),
            (200, shuffled_response(), serde_json::json!(["max_response_bytes_exceeded"])),
        ] {
            let mut fixture = Fixture::new(status, body).await;
            fixture.config.routes[0].request_budget = Some(RequestBudget {
                attempts: 3,
                renewal_seconds: None,
            });
            let validations = Arc::new(AtomicUsize::new(0));
            let observed = validations.clone();
            let client = fixture.client().await.with_answer_validation(move |_| {
                observed.fetch_add(1, Ordering::SeqCst);
                false
            });
            for send in 1..=3 {
                let result = fixture.send(&client, &format!("failed-{send}")).await;
                assert_eq!(result.error, Some(EgressError::Provider {
                    status: (status != 200).then_some(status),
                }));
                assert_eq!(serde_json::to_value(&result.diagnostics).unwrap(), diagnostics);
                assert!(result.output.is_none());
                assert_eq!(result.receipt.spend_state, SpendState::Unknown);
                assert_eq!(result.receipt.status, DispatchStatus::ProviderFailed);
                assert!(result.receipt_persisted);
                fixture.assert_sends(send);
            }
            let result = fixture.send(&client, "budget-refused").await;
            assert_eq!(result.error, Some(EgressError::RequestBudgetExhausted));
            assert!(result.output.is_none());
            assert_eq!(result.receipt.spend_state, SpendState::Released);
            assert!(result.receipt_persisted);
            assert!(result.diagnostics.is_empty());
            assert_eq!(validations.load(Ordering::SeqCst), 0);
            fixture.assert_sends(3);
            eprintln!("identical batch: status={status}, failed_sends=3, fourth=RequestBudgetExhausted, validations=0, sends=3, elapsed={:?}", started.elapsed());
        }
    })
    .await
    .expect("request-budget regression must finish within five seconds");
}
