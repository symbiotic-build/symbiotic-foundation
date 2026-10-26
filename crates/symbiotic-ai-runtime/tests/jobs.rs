//! PR 3 acceptance cases against the real ledger/job transactions.
#[path = "../../test-support/clock.rs"]
mod test_clock;
use async_trait::async_trait;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use symbiotic_ai_runtime::{
    jobs::{ModelJobs, model_job_payload},
    model::{ChatMessage, ModelCapability, ProviderAuthMode, ProviderClass},
    *,
};
use symbiotic_core::{ModelIdentity, QueueItemId, TraceId};
use symbiotic_queue::{
    jobs::*,
    runner::{JobRunner, RunnerConfig, RunnerError},
};
use symbiotic_trace::{InvocationOutcome, ModelInvocationTrace, UsageTrace};
use tokio::sync::Notify;

#[derive(Clone)]
struct Provider {
    descriptor: ProviderDescriptor,
    calls: Arc<AtomicUsize>,
    started: Arc<Notify>,
    finish: Option<Arc<Notify>>,
    failures: usize,
    known_zero: bool,
    panic: bool,
    usage: UsageTrace,
    answer_marker: Option<&'static str>,
    finish_reason: Option<String>,
}
impl Provider {
    fn new() -> Self {
        Self {
            descriptor: ProviderDescriptor {
                identity: ModelIdentity::new("chat", "test", "jobs"),
                provider_class: ProviderClass::Cloud,
                capabilities: vec![ModelCapability::Chat],
                auth_mode: ProviderAuthMode::None,
                metadata: serde_json::json!({}),
            },
            calls: Arc::new(AtomicUsize::new(0)),
            started: Arc::new(Notify::new()),
            finish: None,
            failures: 0,
            known_zero: false,
            panic: false,
            usage: UsageTrace {
                input_tokens: Some(3),
                ..Default::default()
            },
            answer_marker: None,
            finish_reason: Some("stop".into()),
        }
    }
}
impl ModelProvider for Provider {
    fn descriptor(&self) -> &ProviderDescriptor {
        &self.descriptor
    }
    fn failure_charge(&self, _: &ModelError) -> model::FailureCharge {
        if self.known_zero {
            model::FailureCharge::KnownZero
        } else {
            model::FailureCharge::Unknown
        }
    }
}
#[async_trait]
impl ChatProvider for Provider {
    async fn chat(&self, _: ChatRequest) -> Result<ChatResponse, ModelError> {
        let ordinal = self.calls.fetch_add(1, Ordering::SeqCst);
        self.started.notify_one();
        if let Ok(path) = std::env::var("FDN_Q3_CRASH_MARKER") {
            std::fs::write(path, b"dispatched").unwrap();
        }
        if let Some(finish) = &self.finish {
            finish.notified().await;
        }
        assert!(!self.panic, "synthetic provider panic");
        if ordinal < self.failures {
            return Err(ModelError::Unavailable(
                symbiotic_core::DiagnosticCode::HttpUnavailable,
            ));
        }
        let mut response = ChatResponse {
            text: "paid answer".into(),
            finish_reason: self.finish_reason.clone(),
            raw_provider_response: None,
            trace: ModelInvocationTrace {
                trace_id: TraceId::new(),
                queue_item_id: None,
                model: self.descriptor.identity.clone(),
                role_binding: None,
                source: None,
                request_hash: String::new(),
                response_hash: None,
                cache: Default::default(),
                usage: self.usage.clone(),
                timing: Default::default(),
                outcome: InvocationOutcome::Succeeded,
                error_class: None,
                audit_refs: Vec::new(),
                metadata: serde_json::json!({}),
                timestamp: chrono::Utc::now(),
            },
        };
        if let Some(marker) = self.answer_marker {
            response.text = marker.into();
            response.trace.trace_id = TraceId(marker.into());
            response.trace.model = ModelIdentity::new(marker, marker, marker);
            response.trace.queue_item_id = Some(QueueItemId(marker.into()));
        }
        Ok(response)
    }
}
fn binding(p: Provider) -> ModelBinding<Provider> {
    ModelBinding::new(p)
        .with_identity(BindingIdentity::new("tenant", "provider", "1", "account"))
        .with_policy(ModelQueueConfig {
            max_in_flight: 1,
            retry_base_delay_ms: 1,
            retry_jitter_seconds: 0,
            ..Default::default()
        })
}
fn scoped_invocation(key: &str) -> Result<String, JobError> {
    serde_json::to_string(&(
        JobScope {
            tenant: "tenant".into(),
            incarnation: "1".into(),
            queue: "model".into(),
        },
        key,
    ))
    .map_err(|_| JobError::Storage)
}
fn request() -> ChatRequest {
    ChatRequest {
        messages: vec![ChatMessage {
            role: "user".into(),
            content: "input".into(),
        }],
        max_output_tokens: Some(2),
        temperature: None,
        response_format: None,
        role_binding: None,
        source: None,
        metadata: serde_json::json!({}),
    }
}
fn runtime(path: &std::path::Path) -> Runtime {
    runtime_with_config(path, RuntimeConfig::default())
}
fn runtime_with_config(path: &std::path::Path, config: RuntimeConfig) -> Runtime {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    Runtime::open(RuntimeConfig {
        state_dir: Some(path.into()),
        ..config
    })
    .unwrap()
}
fn jobs(r: &Runtime, config: JobConfig) -> ModelJobs {
    r.model_jobs(
        JobScope {
            tenant: "tenant".into(),
            incarnation: "1".into(),
            queue: "model".into(),
        },
        config,
    )
    .unwrap()
}
fn spec(key: &str, p: &Provider) -> JobSpec {
    JobSpec {
        key: key.into(),
        group: Some("batch".into()),
        owners: vec!["owner-a".into(), "owner-b".into()],
        kind: "chat".into(),
        execution: Execution::Model,
        payload: model_job_payload(&binding(p.clone()), &request()).unwrap(),
        admission: None,
        limits: JobLimits { max_attempts: 1 },
        recovery_until: None,
    }
}
async fn enqueue(j: &ModelJobs, s: JobSpec) -> JobId {
    let JobResponse::Enqueued(rows) = j.request(JobRequest::Enqueue(vec![s])).await.unwrap() else {
        panic!()
    };
    let Enqueued::Inserted(id) = &rows[0] else {
        panic!()
    };
    id.clone()
}
async fn row(j: &ModelJobs, id: &JobId) -> JobRecord {
    let JobResponse::Job(Some(row)) = j.request(JobRequest::Get(id.clone())).await.unwrap() else {
        panic!()
    };
    *row
}
async fn wait_state(j: &ModelJobs, id: &JobId, expected: JobState) -> JobRecord {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let row = row(j, id).await;
            if row.state == expected {
                return row;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap()
}
async fn start(j: &ModelJobs, p: Provider) -> JobRunner {
    j.start_chat(
        binding(p),
        RunnerConfig {
            heartbeat_interval_ms: Some(10),
            poll_interval_ms: 5,
            ..Default::default()
        },
        "chat".into(),
    )
    .await
    .unwrap()
}
fn sql(dir: &std::path::Path) -> rusqlite::Connection {
    rusqlite::Connection::open(dir.join(QUEUE_DATABASE)).unwrap()
}
fn copies(dir: &std::path::Path) -> usize {
    sql(dir)
        .query_row(
            "SELECT count(*) FROM spend_receipts WHERE recovery IS NOT NULL",
            [],
            |r| r.get(0),
        )
        .unwrap()
}

const ANSWER_ID_MARKER: &str = "ProviderAnswerIdentifierMarker97";

fn finish_reason_binding<P>(provider: P) -> ModelBinding<P> {
    ModelBinding::new(provider)
        .with_identity(BindingIdentity::new("tenant", "provider", "1", "account"))
        .with_policy(ModelQueueConfig::default())
        .with_answer_recovery(AnswerRecovery::Retain)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FinishReasonFixture {
    OpenAi,
    Anthropic,
    Host,
}

async fn assert_finish_reason_paths<P: ChatProvider + Clone + 'static>(
    make_provider: impl Fn(String, Option<&str>) -> P,
    fixture: FinishReasonFixture,
    reasons: &[Option<&str>],
    echo_identity: bool,
) {
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
    let started = std::time::Instant::now();
    for &reason in reasons {
        for path in ["execute", "job", "queued", "transport"] {
            if fixture == FinishReasonFixture::Host && path == "transport" {
                continue; // Raw host calls are outside the runtime boundary.
            }
            tokio::time::timeout(Duration::from_secs(5), async {
                let listener = if fixture == FinishReasonFixture::Host { None } else {
                    Some(tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap())
                };
                let endpoint = listener.as_ref().map(|listener|
                    format!("http://{}", listener.local_addr().unwrap())).unwrap_or_default();
                let mut body = if fixture == FinishReasonFixture::Anthropic {
                    serde_json::json!({"id":"fixture-id","model":"served-model",
                        "content":[{"type":"text","text":"OK"},
                            {"type":"thinking","thinking":"PRIVATE_REASONING","signature":"sig"}],
                        "stop_reason":reason,"usage":{"input_tokens":3,"output_tokens":1}})
                } else {
                    serde_json::json!({"id":"fixture-id","model":"served-model",
                        "choices":[{"message":{"content":"OK","reasoning_content":"PRIVATE_REASONING"},
                            "finish_reason":reason}],"usage":{"prompt_tokens":3,"completion_tokens":1}})
                };
                if echo_identity {
                    // The private marker appears only in the finish label and identities.
                    body["id"] = serde_json::json!(reason);
                    body["model"] = serde_json::json!(reason);
                    if fixture == FinishReasonFixture::Anthropic {
                        body["content"] = serde_json::json!([{"type":"text","text":"OK"}]);
                    } else {
                        body["choices"][0]["message"] = serde_json::json!({"content":"OK"});
                    }
                }
                let server = tokio::spawn(async move {
                    let Some(listener) = listener else { return; };
                    tokio::time::timeout(Duration::from_secs(2), async {
                        let (stream, _) = listener.accept().await.unwrap();
                        let mut stream = BufReader::new(stream);
                        let mut length = None;
                        loop {
                            let mut line = String::new();
                            assert!(stream.read_line(&mut line).await.unwrap() > 0);
                            if line == "\r\n" { break; }
                            if let Some((key, value)) = line.split_once(':')
                                && key.eq_ignore_ascii_case("content-length") {
                                length = Some(value.trim().parse::<usize>().unwrap());
                            }
                        }
                        let mut request = vec![0; length.unwrap()];
                        stream.read_exact(&mut request).await.unwrap();
                        let payload = body.to_string();
                        stream.get_mut().write_all(format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                            payload.len(), payload
                        ).as_bytes()).await.unwrap();
                    }).await.expect("finish-reason HTTP fixture must finish promptly");
                });
                let p = make_provider(endpoint, reason);
                let configured = finish_reason_binding(p.clone());
                let dir = tempfile::tempdir().unwrap();
                let r = runtime(dir.path());
                let output = match path {
                    "transport" => serde_json::to_value(p.chat(request()).await.unwrap()).unwrap(),
                    "queued" => {
                        let chat = r.chat(configured.clone()).unwrap();
                        let response = chat.chat(request()).await.unwrap();
                        serde_json::to_value(response).unwrap()
                    }
                    "execute" => serde_json::to_value(r.execute_chat(
                        configured.clone(), "finish-reason", request()
                    ).await.unwrap().output).unwrap(),
                    "job" => {
                        let j = jobs(&r, JobConfig::default());
                        let id = enqueue(&j, JobSpec {
                            key: "finish-reason".into(), group: None, owners: vec!["owner".into()],
                            kind: "chat".into(), execution: Execution::Model,
                            payload: model_job_payload(&configured, &request()).unwrap(),
                            admission: None, limits: JobLimits { max_attempts: 1 }, recovery_until: None,
                        }).await;
                        let runner = j.start_chat(configured.clone(), RunnerConfig {
                            worker_count: 1, poll_interval_ms: 5, ..Default::default()
                        }, "chat".into()).await.unwrap();
                        wait_state(&j, &id, JobState::Succeeded).await;
                        runner.shutdown().await.unwrap();
                        drop(j);
                        let recovery: String = sql(dir.path()).query_row(
                            "SELECT recovery FROM spend_receipts WHERE recovery IS NOT NULL", [], |r| r.get(0)
                        ).unwrap();
                        serde_json::from_str(&recovery).unwrap()
                    }
                    _ => unreachable!(),
                };
                server.await.unwrap();
                let invalid = reason == Some("PRIVATE_REASONING");
                let expected = if invalid { Some("other") } else { reason };
                let check = |output: &serde_json::Value| {
                    let text = if fixture == FinishReasonFixture::Host { "paid answer" } else { "OK" };
                    assert_eq!(output["text"], text, "{path}");
                    assert_eq!(output["finish_reason"], serde_json::json!(expected), "{path}");
                    assert!(!output.to_string().contains("PRIVATE_REASONING"), "{path}");
                    let diagnostics = &output["trace"]["metadata"][model::RUNTIME_DIAGNOSTICS];
                    if invalid {
                        let diagnostics = diagnostics.as_array().unwrap();
                        assert!(diagnostics.iter().any(|d| d["kind"] == "invalid_finish_reason"
                            && d["error"] == "invalid_response"), "{path}: {diagnostics:?}");
                        if echo_identity {
                            assert!(output["trace"]["usage"]["response_id"].is_null(), "{path}");
                            assert!(output["trace"]["usage"]["served_model"].is_null(), "{path}");
                            assert!(output["trace"]["metadata"]["provider"]["response_id"].is_null(), "{path}");
                            assert!(output["trace"]["metadata"]["provider"]["served_model"].is_null(), "{path}");
                            assert!(diagnostics.iter().any(|d| d["kind"] == "invalid_usage_identity"
                                && d["error"] == "invalid_response"), "{path}: {diagnostics:?}");
                        }
                    } else {
                        assert!(diagnostics.is_null(), "{path}: {diagnostics}");
                    }
                };
                if path != "transport" {
                    let recovery = if path == "queued" { None } else {
                        Some(sql(dir.path()).query_row::<String, _, _>(
                            "SELECT recovery FROM spend_receipts WHERE recovery IS NOT NULL", [], |r| r.get(0)
                        ).unwrap())
                    };
                    drop(r);
                    let reopened = runtime(dir.path());
                    let recovered = match path {
                        "queued" => serde_json::to_value(reopened.chat(configured).unwrap()
                            .chat(request()).await.unwrap()).unwrap(),
                        "execute" => serde_json::to_value(reopened.execute_chat(configured,
                            "finish-reason", request()).await.unwrap().output).unwrap(),
                        "job" => jobs(&reopened, JobConfig::default()).completions(1, 10000)
                            .await.unwrap().remove(0).output.unwrap(),
                        _ => unreachable!(),
                    };
                    check(&recovered);
                    if let Some(recovery) = recovery {
                        check(&serde_json::from_str(&recovery).unwrap());
                    }
                }
                check(&output);
            }).await.expect("finish-reason path must finish within five seconds");
        }
    }
    eprintln!(
        "finish-reason paths: fixture={fixture:?}, echo_identity={echo_identity}, reasons={}, elapsed={:?}",
        reasons.len(),
        started.elapsed()
    );
}

fn finish_reason_openai(url: String, _: Option<&str>) -> model::OpenAiCompatibleChatProvider {
    model::OpenAiCompatibleChatProvider::new("fixture", "fixture-model", url, "synthetic-key")
        .with_timeout(1)
        .unwrap()
        .with_request_limit(65536)
        .with_response_limit(65536)
}

fn finish_reason_anthropic(url: String, _: Option<&str>) -> model::AnthropicChatProvider {
    model::AnthropicChatProvider::new("fixture", "fixture-model", url, "synthetic-key")
        .with_timeout(1)
        .unwrap()
        .with_request_limit(65536)
        .with_response_limit(65536)
}

#[tokio::test]
async fn regression_finish_reason_openai_reasoning_stays_out_of_restart_recovery() {
    assert_finish_reason_paths(
        finish_reason_openai,
        FinishReasonFixture::OpenAi,
        &[Some("PRIVATE_REASONING")],
        false,
    )
    .await;
}

#[tokio::test]
async fn regression_finish_reason_anthropic_reasoning_stays_out_of_restart_recovery() {
    assert_finish_reason_paths(
        finish_reason_anthropic,
        FinishReasonFixture::Anthropic,
        &[Some("PRIVATE_REASONING")],
        false,
    )
    .await;
}

#[tokio::test]
async fn regression_finish_reason_openai_known_labels_and_absence_are_unchanged() {
    assert_finish_reason_paths(
        finish_reason_openai,
        FinishReasonFixture::OpenAi,
        &[
            Some("stop"),
            Some("length"),
            Some("end_turn"),
            Some("stop_sequence"),
            Some("max_tokens"),
            Some("model_context_window_exceeded"),
            Some("other"),
            None,
        ],
        false,
    )
    .await;
}

#[tokio::test]
async fn regression_finish_reason_anthropic_known_labels_are_unchanged() {
    assert_finish_reason_paths(
        finish_reason_anthropic,
        FinishReasonFixture::Anthropic,
        &[
            Some("end_turn"),
            Some("stop_sequence"),
            Some("max_tokens"),
            Some("model_context_window_exceeded"),
        ],
        false,
    )
    .await;
}

#[tokio::test]
async fn regression_finish_reason_openai_identity_echoes_stay_out_of_restart_recovery() {
    assert_finish_reason_paths(
        finish_reason_openai,
        FinishReasonFixture::OpenAi,
        &[Some("PRIVATE_REASONING")],
        true,
    )
    .await;
}

#[tokio::test]
async fn regression_finish_reason_anthropic_identity_echoes_stay_out_of_restart_recovery() {
    assert_finish_reason_paths(
        finish_reason_anthropic,
        FinishReasonFixture::Anthropic,
        &[Some("PRIVATE_REASONING")],
        true,
    )
    .await;
}

#[tokio::test]
async fn regression_finish_reason_host_text_stays_out_of_restart_recovery() {
    assert_finish_reason_paths(
        |_, reason| {
            let mut p = Provider::new();
            p.finish_reason = reason.map(str::to_owned);
            p
        },
        FinishReasonFixture::Host,
        &[Some("PRIVATE_REASONING"), Some("stop"), None],
        false,
    )
    .await;
}

fn answer_id_files_contain_marker(dir: &std::path::Path, off: bool) -> bool {
    let mut found = false;
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            found |= answer_id_files_contain_marker(&path, off);
        } else {
            let contains = std::fs::read(&path)
                .unwrap()
                .windows(ANSWER_ID_MARKER.len())
                .any(|bytes| bytes == ANSWER_ID_MARKER.as_bytes());
            if off {
                assert!(
                    !contains,
                    "answer identifier persisted in {}",
                    path.display()
                );
            }
            found |= contains;
        }
    }
    found
}

async fn assert_answer_recovery_trace_identifiers(job: bool) {
    for mode in [AnswerRecovery::Off, AnswerRecovery::Retain] {
        let off = mode == AnswerRecovery::Off;
        let dir = tempfile::tempdir().unwrap();
        let trace_path = dir.path().join("traces.jsonl");
        let r = Runtime::open(RuntimeConfig {
            state_dir: Some(dir.path().join("state")),
            trace_sink: Some(Arc::new(
                symbiotic_trace::JsonlTraceSink::open(&trace_path).unwrap(),
            )),
            ..Default::default()
        })
        .unwrap();
        let mut p = Provider::new();
        p.answer_marker = Some(ANSWER_ID_MARKER);
        let configured = binding(p.clone()).with_answer_recovery(mode);
        let expected_item;
        if job {
            let j = jobs(&r, JobConfig::default());
            let id = enqueue(&j, spec("answer-identifiers", &p)).await;
            let runner = j
                .start_chat(
                    configured,
                    RunnerConfig {
                        poll_interval_ms: 5,
                        ..Default::default()
                    },
                    "chat".into(),
                )
                .await
                .unwrap();
            let completed = wait_state(&j, &id, JobState::Succeeded).await;
            runner.shutdown().await.unwrap();
            expected_item = QueueItemId(completed.receipt.unwrap());
            assert_eq!(completed.result_expired, off);
            let delivery = j.completions(1, 10000).await.unwrap().remove(0);
            if off {
                assert!(delivery.output.is_none());
            } else {
                assert_eq!(delivery.output.unwrap()["text"], ANSWER_ID_MARKER);
            }
        } else {
            let executed = r
                .execute_chat(configured, "answer-identifiers", request())
                .await
                .unwrap();
            assert_eq!(executed.output.text, ANSWER_ID_MARKER);
            expected_item = executed.output.trace.queue_item_id.unwrap();
        }
        assert_eq!(p.calls.load(Ordering::SeqCst), 1);
        let traces = symbiotic_trace::JsonlTraceSink::read(&trace_path).unwrap();
        assert_eq!(traces.len(), 1);
        let stored = &traces[0];
        if off {
            assert_ne!(stored.trace_id.0, ANSWER_ID_MARKER);
            assert_eq!(stored.model, p.descriptor.identity);
            assert_eq!(stored.queue_item_id.as_ref(), Some(&expected_item));
            assert_eq!(stored.usage.input_tokens, Some(3));
        } else {
            assert_eq!(stored.trace_id.0, ANSWER_ID_MARKER);
            assert_eq!(
                stored.model,
                ModelIdentity::new(ANSWER_ID_MARKER, ANSWER_ID_MARKER, ANSWER_ID_MARKER)
            );
            assert_eq!(
                stored.queue_item_id,
                Some(if job {
                    QueueItemId(ANSWER_ID_MARKER.into())
                } else {
                    expected_item
                })
            );
        }
        // Scan every file while SQLite databases and their WAL files are open.
        assert!(
            dir.path()
                .join("state")
                .join(format!("{QUEUE_DATABASE}-wal"))
                .is_file()
        );
        assert_eq!(answer_id_files_contain_marker(dir.path(), off), !off);
        drop(r);
        let reopened = runtime(&dir.path().join("state"));
        assert_eq!(answer_id_files_contain_marker(dir.path(), off), !off);
        drop(reopened);
    }
}

#[tokio::test]
async fn regression_answer_recovery_direct_trace_identifiers() {
    assert_answer_recovery_trace_identifiers(false).await;
}

#[tokio::test]
async fn regression_answer_recovery_job_trace_identifiers() {
    assert_answer_recovery_trace_identifiers(true).await;
}

#[tokio::test]
async fn case_1_paid_commit_and_separate_direct_answer_recover_together() {
    let dir = tempfile::tempdir().unwrap();
    let p = Provider::new();
    {
        let r = runtime(dir.path());
        r.execute_chat(
            binding(p.clone()),
            &scoped_invocation("direct").unwrap(),
            request(),
        )
        .await
        .unwrap();
    }
    let r = runtime(dir.path());
    let j = jobs(&r, JobConfig::default());
    let id = enqueue(&j, spec("direct", &p)).await;
    let runner = start(&j, p.clone()).await;
    wait_state(&j, &id, JobState::Succeeded).await;
    assert_eq!(p.calls.load(Ordering::SeqCst), 1);
    runner.shutdown().await.unwrap();
    assert_eq!(
        j.completions(1, 10000).await.unwrap()[0]
            .output
            .as_ref()
            .unwrap()["text"],
        "paid answer"
    );
    drop(j);
    drop(r);
    let j = jobs(&runtime(dir.path()), JobConfig::default());
    assert_eq!(row(&j, &id).await.state, JobState::Succeeded);
    assert_eq!(copies(dir.path()), 1);
}

#[tokio::test]
async fn regression_off_job_delivers_previously_retained_direct_answer() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let dir = tempfile::tempdir().unwrap();
        let r = runtime(dir.path());
        let j = jobs(&r, JobConfig::default());
        let p = Provider::new();
        r.execute_chat(
            binding(p.clone()),
            &j.invocation_key("retained").unwrap(),
            request(),
        )
        .await
        .unwrap();
        let id = enqueue(&j, spec("retained", &p)).await;
        let runner = j
            .start_chat(
                binding(p.clone()).with_answer_recovery(AnswerRecovery::Off),
                RunnerConfig {
                    poll_interval_ms: 5,
                    ..Default::default()
                },
                "chat".into(),
            )
            .await
            .unwrap();
        wait_state(&j, &id, JobState::Succeeded).await;
        runner.shutdown().await.unwrap();
        let delivery = j.completions(1, 10000).await.unwrap().remove(0);
        assert_eq!(delivery.output.unwrap()["text"], "paid answer");
        assert!(!delivery.delivery.completion.result_expired);
        assert_eq!(p.calls.load(Ordering::SeqCst), 1);
    })
    .await
    .expect("bounded previously retained job delivery");
}

#[tokio::test]
async fn regression_off_job_reports_invalid_cost_without_persisting_or_retrying_it() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let dir = tempfile::tempdir().unwrap();
        let r = runtime(dir.path());
        let j = jobs(&r, JobConfig::default());
        let mut p = Provider::new();
        p.usage = UsageTrace {
            reported_cost_usd: Some("paid answer".into()),
            ..Default::default()
        };
        let id = enqueue(&j, spec("invalid-cost", &p)).await;
        let runner = j
            .start_chat(
                binding(p.clone()).with_answer_recovery(AnswerRecovery::Off),
                RunnerConfig {
                    poll_interval_ms: 5,
                    ..Default::default()
                },
                "chat".into(),
            )
            .await
            .unwrap();
        let error = runner.wait().await.unwrap_err();
        let RunnerError::Workers(errors) = error else {
            panic!("unexpected runner error: {error:?}")
        };
        assert!(errors.iter().any(|error| matches!(
            error,
            RunnerError::Store(JobError::Execution(
                symbiotic_core::DiagnosticCode::InvalidResponse
            ))
        )));
        let completed = row(&j, &id).await;
        assert_eq!(completed.state, JobState::Succeeded);
        let receipt = r
            .spend_receipt(&SpendReceiptRef::new(completed.receipt.unwrap()).unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(receipt.state, SpendState::Unknown);
        assert!(receipt.usage.is_none());
        assert!(receipt.recovery.is_none());
        assert_eq!(
            receipt.output,
            Some(serde_json::json!({"output_received": true}))
        );
        assert_eq!(p.calls.load(Ordering::SeqCst), 1);
    })
    .await
    .expect("bounded invalid job cost regression");
}

#[tokio::test]
async fn case_2_account_concurrency_one_completes_without_second_slot() {
    let dir = tempfile::tempdir().unwrap();
    let r = runtime(dir.path());
    let j = jobs(&r, JobConfig::default());
    let p = Provider::new();
    let id = enqueue(&j, spec("single-slot", &p)).await;
    let runner = start(&j, p.clone()).await;
    let result = wait_state(&j, &id, JobState::Succeeded).await;
    assert!(result.output.is_none());
    assert_eq!(p.calls.load(Ordering::SeqCst), 1);
    assert_eq!(copies(dir.path()), 1);
    let delivery = j.completions(1, 10000).await.unwrap().remove(0);
    assert!(delivery.output.is_some());
    j.request(JobRequest::Ack(vec![(
        delivery.delivery.token,
        Disposition::Accepted,
    )]))
    .await
    .unwrap();
    assert_eq!(copies(dir.path()), 0);
    runner.shutdown().await.unwrap();
}

#[tokio::test]
async fn case_6_restart_backlog_pages_respect_population_and_completion_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let r = runtime(dir.path());
    let config = JobConfig {
        max_live_jobs: 2,
        max_page: 1,
        maintenance_batch: 1,
        ..Default::default()
    };
    let j = jobs(&r, config.clone());
    let p = Provider::new();
    let first = enqueue(&j, spec("one", &p)).await;
    let second = enqueue(&j, spec("two", &p)).await;
    assert!(matches!(
        j.request(JobRequest::Enqueue(vec![spec("three", &p)]))
            .await,
        Err(JobError::QueueFull)
    ));
    drop(j);
    drop(r);
    let j = jobs(&runtime(dir.path()), config);
    let runner = start(&j, p.clone()).await;
    wait_state(&j, &first, JobState::Succeeded).await;
    wait_state(&j, &second, JobState::Succeeded).await;
    assert!(matches!(
        j.completions(1, 100).await,
        Err(JobError::CompletionTooLarge { .. })
    ));
    assert_eq!(row(&j, &first).await.delivery_generation, 0);
    let page = j.completions(1, 10000).await.unwrap();
    assert!(serde_json::to_vec(&page).unwrap().len() <= 10000);
    j.request(JobRequest::Ack(vec![(
        page[0].delivery.token.clone(),
        Disposition::Discarded,
    )]))
    .await
    .unwrap();
    let third = enqueue(&j, spec("three", &p)).await;
    wait_state(&j, &third, JobState::Succeeded).await;
    assert_eq!(p.calls.load(Ordering::SeqCst), 3);
    runner.shutdown().await.unwrap();
}

#[tokio::test]
async fn case_8_pending_past_recovery_until_runs_without_saved_output() {
    let dir = tempfile::tempdir().unwrap();
    let j = jobs(&runtime(dir.path()), JobConfig::default());
    let p = Provider::new();
    let mut s = spec("expired", &p);
    s.recovery_until = Some(chrono::Utc::now() - chrono::Duration::seconds(1));
    let id = enqueue(&j, s).await;
    j.request(JobRequest::Maintain).await.unwrap();
    assert!(row(&j, &id).await.payload.is_some());
    let runner = start(&j, p).await;
    let final_row = wait_state(&j, &id, JobState::Succeeded).await;
    assert!(final_row.result_expired);
    assert_eq!(copies(dir.path()), 0);
    assert!(j.completions(1, 10000).await.unwrap()[0].output.is_none());
    let reference = SpendReceiptRef::new(final_row.receipt.unwrap()).unwrap();
    assert_eq!(
        j_runtime_receipt(dir.path(), &reference).state,
        SpendState::Settled
    );
    runner.shutdown().await.unwrap();
}
fn j_runtime_receipt(dir: &std::path::Path, reference: &SpendReceiptRef) -> SpendReceipt {
    spend::SqliteSpendLedger::open(&dir.join(QUEUE_DATABASE))
        .unwrap()
        .receipt(reference)
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn case_12_purge_any_owner_in_flight_settles_without_output_or_copy() {
    let dir = tempfile::tempdir().unwrap();
    let j = jobs(&runtime(dir.path()), JobConfig::default());
    let mut p = Provider::new();
    p.finish = Some(Arc::new(Notify::new()));
    let id = enqueue(&j, spec("purged", &p)).await;
    let runner = start(&j, p.clone()).await;
    p.started.notified().await;
    j.request(JobRequest::PurgeOwner("owner-b".into()))
        .await
        .unwrap();
    p.finish.as_ref().unwrap().notify_one();
    let result = wait_state(&j, &id, JobState::Purged).await;
    assert!(result.payload.is_none() && result.output.is_none());
    assert_eq!(copies(dir.path()), 0);
    assert_eq!(
        j_runtime_receipt(
            dir.path(),
            &SpendReceiptRef::new(result.receipt.unwrap()).unwrap()
        )
        .state,
        SpendState::Settled
    );
    runner.shutdown().await.unwrap();
}

async fn killed_app(test: &str) {
    if let Ok(path) = std::env::var("FDN_Q3_CRASH_DIR") {
        let j = jobs(&runtime(std::path::Path::new(&path)), JobConfig::default());
        let mut p = Provider::new();
        p.finish = Some(Arc::new(Notify::new()));
        enqueue(&j, spec("crash", &p)).await;
        let _runner = start(&j, p).await;
        std::future::pending::<()>().await;
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    // A completed job and the interrupted call coexist across whole-app death.
    let committed = {
        let j = jobs(&runtime(dir.path()), JobConfig::default());
        let p = Provider::new();
        let id = enqueue(&j, spec("committed", &p)).await;
        let runner = start(&j, p).await;
        wait_state(&j, &id, JobState::Succeeded).await;
        runner.shutdown().await.unwrap();
        id
    };
    let marker = dir.path().join("dispatch");
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", test, "--nocapture"])
        .env("FDN_Q3_CRASH_DIR", dir.path())
        .env("FDN_Q3_CRASH_MARKER", &marker)
        .spawn()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while !marker.exists() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    child.kill().unwrap();
    assert!(!child.wait().unwrap().success());
    sql(dir.path())
        .execute(
            "UPDATE jobs SET lease_until=0, recovery_until=0 WHERE key='crash'",
            [],
        )
        .unwrap();
    let j = jobs(&runtime(dir.path()), JobConfig::default());
    let p = Provider::new();
    let runner = start(&j, p.clone()).await;
    let JobResponse::Diagnostics(page) = j
        .request(JobRequest::Diagnostics {
            group: "batch".into(),
            after: None,
            limit: 10,
        })
        .await
        .unwrap()
    else {
        panic!()
    };
    // The recovery loop converts the expired claim; inspect its stable ID via SQL.
    let id: String = sql(dir.path())
        .query_row("SELECT id FROM jobs WHERE key='crash'", [], |r| r.get(0))
        .unwrap();
    let id = JobId {
        scope: JobScope {
            tenant: "tenant".into(),
            incarnation: "1".into(),
            queue: "model".into(),
        },
        id,
    };
    let result = wait_state(&j, &id, JobState::Uncertain).await;
    assert!(result.payload.is_some());
    assert_eq!(p.calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        j_runtime_receipt(
            dir.path(),
            &SpendReceiptRef::new(result.receipt.unwrap()).unwrap()
        )
        .state,
        SpendState::Unknown
    );
    assert!(page.items.len() <= 10);
    assert_eq!(row(&j, &committed).await.state, JobState::Succeeded);
    assert!(j.completions(1, 10000).await.unwrap()[0].output.is_some());
    j.request(JobRequest::PurgeOwner("owner-a".into()))
        .await
        .unwrap();
    let purged = row(&j, &id).await;
    assert_eq!(purged.state, JobState::Uncertain);
    assert!(purged.payload.is_none());
    runner.shutdown().await.unwrap();
}
#[tokio::test]
// Acceptance cases 13 and 26 share this single whole-process crash scenario.
async fn case_13_killed_after_dispatch_before_commit_is_uncertain_never_retried() {
    killed_app("case_13_killed_after_dispatch_before_commit_is_uncertain_never_retried").await;
}

// Case 22 covers queue schema refusal only. On-disk egress registry stamp
// refusal is not covered here; the model registry test validates JSON config.
// Memory state belongs to the Memory runtime, which Foundation never opens.
fn database_snapshot(
    conn: &rusqlite::Connection,
) -> Vec<(String, Vec<Vec<rusqlite::types::Value>>)> {
    let mut schema = conn.prepare("SELECT name FROM sqlite_master WHERE type='table' UNION SELECT 'sqlite_master' ORDER BY name").unwrap();
    let tables = schema
        .query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    tables
        .into_iter()
        .map(|name| {
            let mut stmt = conn
                .prepare(&format!(
                    "SELECT * FROM \"{}\" ORDER BY rowid",
                    name.replace('"', "\"\"")
                ))
                .unwrap();
            let columns = stmt.column_count();
            let rows = stmt
                .query_map([], |r| {
                    (0..columns)
                        .map(|i| r.get(i))
                        .collect::<Result<Vec<rusqlite::types::Value>, _>>()
                })
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            (name, rows)
        })
        .collect()
}

#[tokio::test]
async fn case_22_incompatible_queue_schema_refuses_open_without_changes() {
    let dir = tempfile::tempdir().unwrap();
    let r = runtime(dir.path());
    let j = jobs(&r, JobConfig::default());
    let p = Provider::new();
    let id = enqueue(&j, spec("retained-before-refusal", &p)).await;
    let runner = start(&j, p).await;
    wait_state(&j, &id, JobState::Succeeded).await;
    runner.shutdown().await.unwrap();
    drop(j);
    drop(r);
    let conn = sql(dir.path());
    let before = database_snapshot(&conn);
    conn.pragma_update(None, "user_version", 999).unwrap();
    assert!(
        Runtime::open(RuntimeConfig {
            state_dir: Some(dir.path().into()),
            ..Default::default()
        })
        .is_err()
    );
    assert_eq!(
        conn.pragma_query_value(None, "user_version", |r| r.get::<_, u32>(0))
            .unwrap(),
        999
    );
    assert_eq!(database_snapshot(&conn), before);
}

#[tokio::test]
async fn d11_cancel_sent_call_keeps_answer_and_never_retries() {
    let dir = tempfile::tempdir().unwrap();
    let j = jobs(&runtime(dir.path()), JobConfig::default());
    let mut p = Provider::new();
    p.finish = Some(Arc::new(Notify::new()));
    let id = enqueue(&j, spec("cancel", &p)).await;
    let runner = start(&j, p.clone()).await;
    p.started.notified().await;
    j.request(JobRequest::Cancel(Selector::Ids(vec![id.clone()])))
        .await
        .unwrap();
    p.finish.as_ref().unwrap().notify_one();
    wait_state(&j, &id, JobState::Cancelled).await;
    assert_eq!(p.calls.load(Ordering::SeqCst), 1);
    assert!(j.completions(1, 10000).await.unwrap()[0].output.is_some());
    runner.shutdown().await.unwrap();
}

#[tokio::test]
async fn paid_result_and_job_completion_roll_back_together() {
    let dir = tempfile::tempdir().unwrap();
    let j = jobs(&runtime(dir.path()), JobConfig::default());
    let p = Provider::new();
    let id = enqueue(&j, spec("rollback", &p)).await;
    sql(dir.path()).execute_batch("CREATE TRIGGER refuse_job_result BEFORE UPDATE OF state ON jobs WHEN NEW.state='\"Succeeded\"' BEGIN SELECT RAISE(ABORT,'synthetic'); END;").unwrap();
    let runner = start(&j, p.clone()).await;
    assert!(runner.wait().await.is_err());
    assert_eq!(row(&j, &id).await.state, JobState::Running);
    assert_eq!(copies(dir.path()), 0);
    let receipt = j_runtime_receipt(
        dir.path(),
        &SpendReceiptRef::new(row(&j, &id).await.receipt.unwrap()).unwrap(),
    );
    assert!(receipt.output.is_none());
    assert_eq!(receipt.state, SpendState::Unknown);
    assert_eq!(p.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn claim_and_reservation_roll_back_together() {
    let dir = tempfile::tempdir().unwrap();
    let j = jobs(&runtime(dir.path()), JobConfig::default());
    let p = Provider::new();
    let id = enqueue(&j, spec("reserve-fails", &p)).await;
    sql(dir.path()).execute_batch("CREATE TRIGGER refuse_reservation BEFORE INSERT ON spend_receipts BEGIN SELECT RAISE(ABORT,'synthetic'); END;").unwrap();
    let runner = start(&j, p.clone()).await;
    assert!(runner.wait().await.is_err());
    let row = row(&j, &id).await;
    assert_eq!((row.state, row.generation), (JobState::Pending, 0));
    assert!(row.receipt.is_none());
    assert_eq!(p.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn purge_pending_job_discards_separately_committed_direct_answer() {
    let dir = tempfile::tempdir().unwrap();
    let r = runtime(dir.path());
    let p = Provider::new();
    let j = jobs(&r, JobConfig::default());
    r.execute_chat(
        binding(p.clone()),
        &scoped_invocation("direct-purge").unwrap(),
        request(),
    )
    .await
    .unwrap();
    let id = enqueue(&j, spec("direct-purge", &p)).await;
    j.request(JobRequest::PurgeOwner("owner-a".into()))
        .await
        .unwrap();
    assert_eq!(row(&j, &id).await.state, JobState::Purged);
    assert_eq!(copies(dir.path()), 0);
}

#[tokio::test]
async fn paid_delivery_byte_preflight_defers_last_without_leasing_or_loading_it() {
    let dir = tempfile::tempdir().unwrap();
    let j = jobs(&runtime(dir.path()), JobConfig::default());
    let p = Provider::new();
    let first = enqueue(&j, spec("first", &p)).await;
    let second = enqueue(&j, spec("second", &p)).await;
    let runner = start(&j, p).await;
    wait_state(&j, &first, JobState::Succeeded).await;
    wait_state(&j, &second, JobState::Succeeded).await;
    let one = j.completions(1, 10000).await.unwrap();
    let max_bytes = serde_json::to_vec(&one).unwrap().len() + 20;
    sql(dir.path())
        .execute(
            "UPDATE jobs SET delivery_until=NULL,delivery_generation=0",
            [],
        )
        .unwrap();
    let page = j.completions(2, max_bytes).await.unwrap();
    assert_eq!(page.len(), 1);
    assert!(serde_json::to_vec(&page).unwrap().len() <= max_bytes);
    let remaining = if page[0].delivery.completion.id == first {
        second
    } else {
        first
    };
    assert_eq!(row(&j, &remaining).await.delivery_generation, 0);
    runner.shutdown().await.unwrap();
}

#[tokio::test]
async fn known_zero_retry_uses_frozen_job_ceiling_and_settles_each_attempt() {
    let dir = tempfile::tempdir().unwrap();
    let j = jobs(&runtime(dir.path()), JobConfig::default());
    let mut p = Provider::new();
    p.failures = 1;
    p.known_zero = true;
    let mut s = spec("retry", &p);
    s.limits.max_attempts = 2;
    let id = enqueue(&j, s).await;
    let runner = start(&j, p.clone()).await;
    wait_state(&j, &id, JobState::Succeeded).await;
    assert_eq!(p.calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        sql(dir.path())
            .query_row(
                "SELECT count(*) FROM spend_receipts WHERE state='released'",
                [],
                |r| r.get::<_, usize>(0)
            )
            .unwrap(),
        1
    );
    runner.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancelled_sent_failure_keeps_unknown_accounting_and_never_retries() {
    let dir = tempfile::tempdir().unwrap();
    let j = jobs(&runtime(dir.path()), JobConfig::default());
    let mut p = Provider::new();
    p.failures = 10;
    p.finish = Some(Arc::new(Notify::new()));
    let mut s = spec("cancel-failure", &p);
    s.limits.max_attempts = 3;
    let id = enqueue(&j, s).await;
    let runner = start(&j, p.clone()).await;
    p.started.notified().await;
    j.request(JobRequest::Cancel(Selector::Ids(vec![id.clone()])))
        .await
        .unwrap();
    p.finish.as_ref().unwrap().notify_one();
    let cancelled = wait_state(&j, &id, JobState::Cancelled).await;
    assert_eq!(p.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        j_runtime_receipt(
            dir.path(),
            &SpendReceiptRef::new(cancelled.receipt.unwrap()).unwrap()
        )
        .state,
        SpendState::Unknown
    );
    runner.shutdown().await.unwrap();
}

#[tokio::test]
async fn exhausted_account_refuses_job_visibly_without_dispatch_or_reservation() {
    let dir = tempfile::tempdir().unwrap();
    let j = jobs(&runtime(dir.path()), JobConfig::default());
    let p = Provider::new();
    let id = enqueue(&j, spec("no-allowance", &p)).await;
    let mut b = binding(p.clone());
    b.policy.as_mut().unwrap().provider_request_limit = Some(0);
    let runner = j
        .start_chat(b, RunnerConfig::default(), "chat".into())
        .await
        .unwrap();
    let refused = wait_state(&j, &id, JobState::Refused).await;
    assert_eq!(
        refused.diagnostic,
        Some(symbiotic_core::DiagnosticCode::SpendBudgetExhausted)
    );
    assert_eq!(p.calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        sql(dir.path())
            .query_row("SELECT count(*) FROM spend_receipts", [], |r| r
                .get::<_, usize>(0))
            .unwrap(),
        0
    );
    runner.shutdown().await.unwrap();
}

#[tokio::test]
async fn ledger_first_committed_answer_recovers_before_new_rate_admission() {
    let dir = tempfile::tempdir().unwrap();
    let p = Provider::new();
    {
        let r = runtime(dir.path());
        r.execute_chat(
            binding(p.clone()),
            &scoped_invocation("rate-recovery").unwrap(),
            request(),
        )
        .await
        .unwrap();
    }
    let j = jobs(&runtime(dir.path()), JobConfig::default());
    let id = enqueue(&j, spec("rate-recovery", &p)).await;
    let mut b = binding(p.clone());
    b.policy.as_mut().unwrap().input_units_per_minute = Some(1);
    let runner = j
        .start_chat(b, RunnerConfig::default(), "chat".into())
        .await
        .unwrap();
    wait_state(&j, &id, JobState::Succeeded).await;
    assert_eq!(p.calls.load(Ordering::SeqCst), 1);
    runner.shutdown().await.unwrap();
}

#[tokio::test]
async fn consumer_cannot_write_model_completion_or_supplant_ledger_receipt() {
    let dir = tempfile::tempdir().unwrap();
    let j = jobs(&runtime(dir.path()), JobConfig::default());
    let p = Provider::new();
    let id = enqueue(&j, spec("owned", &p)).await;
    assert!(matches!(
        j.request(JobRequest::Complete {
            job: id.clone(),
            generation: 1,
            state: JobState::Succeeded,
            origin: ResultOrigin::Paid,
            output: None,
            receipt: Some("foreign-receipt".into()),
            diagnostic: None
        })
        .await,
        Err(JobError::InvalidRequest)
    ));
    assert_eq!(row(&j, &id).await.state, JobState::Pending);
}

#[tokio::test]
async fn direct_discard_leaves_accounting_and_reports_unavailable_job_answer() {
    let dir = tempfile::tempdir().unwrap();
    let r = runtime(dir.path());
    let j = jobs(&r, JobConfig::default());
    let p = Provider::new();
    let id = enqueue(&j, spec("discard-direct", &p)).await;
    let runner = start(&j, p.clone()).await;
    wait_state(&j, &id, JobState::Succeeded).await;
    r.discard_invocation_output(
        binding(p).identity.as_ref().unwrap(),
        None,
        &scoped_invocation("discard-direct").unwrap(),
    )
    .unwrap();
    let page = j.completions(1, 10000).await.unwrap();
    assert!(page[0].output.is_none());
    assert!(page[0].delivery.completion.result_expired);
    runner.shutdown().await.unwrap();
}

#[tokio::test]
async fn job_limit_cannot_expand_the_execution_owners_provider_ceiling() {
    let dir = tempfile::tempdir().unwrap();
    let j = jobs(&runtime(dir.path()), JobConfig::default());
    let mut p = Provider::new();
    p.failures = 10;
    p.known_zero = true;
    let mut s = spec("frozen-policy", &p);
    s.limits.max_attempts = 3;
    let id = enqueue(&j, s).await;
    let mut b = binding(p.clone());
    b.policy.as_mut().unwrap().logical_retry_attempts = 1;
    let runner = j
        .start_chat(b, RunnerConfig::default(), "chat".into())
        .await
        .unwrap();
    wait_state(&j, &id, JobState::Refused).await;
    assert_eq!(p.calls.load(Ordering::SeqCst), 1);
    runner.shutdown().await.unwrap();
}

#[tokio::test]
async fn model_job_failure_reaches_the_existing_trace_sink() {
    let dir = tempfile::tempdir().unwrap();
    let sink = Arc::new(symbiotic_trace::InMemoryTraceSink::default());
    drop(runtime(dir.path()));
    let r = Runtime::open(RuntimeConfig {
        state_dir: Some(dir.path().into()),
        trace_sink: Some(sink.clone()),
        ..Default::default()
    })
    .unwrap();
    let j = jobs(&r, JobConfig::default());
    let mut p = Provider::new();
    p.failures = 1;
    let id = enqueue(&j, spec("traced-failure", &p)).await;
    let runner = start(&j, p).await;
    wait_state(&j, &id, JobState::Uncertain).await;
    runner.shutdown().await.unwrap();
    assert!(
        sink.records()
            .iter()
            .any(|trace| trace.outcome == InvocationOutcome::Failed)
    );
}

#[tokio::test]
async fn pre_dispatch_heartbeat_failure_releases_accounting_without_transport() {
    let dir = tempfile::tempdir().unwrap();
    let j = jobs(&runtime(dir.path()), JobConfig::default());
    let p = Provider::new();
    let id = enqueue(&j, spec("heartbeat-refused", &p)).await;
    sql(dir.path()).execute_batch("CREATE TRIGGER refuse_dispatch_heartbeat BEFORE UPDATE OF lease_until ON jobs WHEN NEW.state='\"Running\"' AND OLD.receipt=NEW.receipt AND EXISTS(SELECT 1 FROM spend_receipts WHERE reference=NEW.receipt AND state='unknown') BEGIN SELECT RAISE(ABORT,'synthetic'); END;").unwrap();
    let runner = start(&j, p.clone()).await;
    assert!(runner.wait().await.is_err());
    let released = row(&j, &id).await;
    assert_eq!(released.state, JobState::Refused);
    let receipt = j_runtime_receipt(
        dir.path(),
        &SpendReceiptRef::new(released.receipt.unwrap()).unwrap(),
    );
    assert_eq!(receipt.state, SpendState::Released);
    assert!(receipt.pre_dispatch_released);
    assert_eq!(p.calls.load(Ordering::SeqCst), 0);
}

struct DelayedReceipt {
    status: ReceiptStatus,
    entered: Notify,
    resume: Notify,
}
#[async_trait]
impl QueueReceiptSink for DelayedReceipt {
    async fn record_receipt(&self, receipt: QueueReceipt) {
        if receipt.status == self.status {
            self.entered.notify_one();
            self.resume.notified().await;
        }
    }
}

#[tokio::test(start_paused = true)]
async fn slow_receipt_sink_renews_lease_and_respects_cancellation() {
    for status in [ReceiptStatus::Queued, ReceiptStatus::Running] {
        for (cancel, expire) in [(false, false), (true, false), (false, true)] {
            let dir = tempfile::tempdir().unwrap();
            let sink = Arc::new(DelayedReceipt {
                status,
                entered: Notify::new(),
                resume: Notify::new(),
            });
            let clock = test_clock::clock();
            let r = runtime_with_config(
                dir.path(),
                RuntimeConfig {
                    clock: clock.clone(),
                    receipt_sink: Some(sink.clone()),
                    ..Default::default()
                },
            );
            let j = jobs(
                &r,
                JobConfig {
                    claim_lease_seconds: 1,
                    ..Default::default()
                },
            );
            let p = Provider::new();
            let id = enqueue(&j, spec("slow-receipt", &p)).await;
            let runner = j
                .start_chat(
                    binding(p.clone()),
                    RunnerConfig {
                        worker_count: 1,
                        heartbeat_interval_ms: Some(250),
                        poll_interval_ms: 5,
                        ..Default::default()
                    },
                    "chat".into(),
                )
                .await
                .unwrap();
            tokio::time::timeout(Duration::from_secs(5), sink.entered.notified())
                .await
                .unwrap();
            // Receipt emission remains blocked longer than the entire lease.
            tokio::time::sleep(Duration::from_millis(1200)).await;
            let leased = row(&j, &id).await;
            assert_eq!(leased.state, JobState::Running);
            assert!(leased.lease_until.unwrap() > clock());
            assert_eq!(p.calls.load(Ordering::SeqCst), 0);
            if cancel {
                j.request(JobRequest::Cancel(Selector::Ids(vec![id.clone()])))
                    .await
                    .unwrap();
            }
            if expire {
                // Keep the original forced-fence case as well as real renewal.
                sql(dir.path())
                    .execute("UPDATE jobs SET lease_until=0", [])
                    .unwrap();
            }
            sink.resume.notify_one();
            if expire {
                let result = tokio::time::timeout(Duration::from_secs(5), runner.wait())
                    .await
                    .unwrap();
                assert!(result.is_err());
            } else {
                wait_state(
                    &j,
                    &id,
                    if cancel {
                        JobState::Cancelled
                    } else {
                        JobState::Succeeded
                    },
                )
                .await;
                runner.shutdown().await.unwrap();
            }
            let final_row = row(&j, &id).await;
            assert_eq!(
                p.calls.load(Ordering::SeqCst),
                usize::from(!cancel && !expire)
            );
            let receipt = j_runtime_receipt(
                dir.path(),
                &SpendReceiptRef::new(final_row.receipt.unwrap()).unwrap(),
            );
            assert_eq!(
                receipt.state,
                if cancel || expire {
                    SpendState::Released
                } else {
                    SpendState::Settled
                }
            );
            assert_eq!(receipt.pre_dispatch_released, cancel || expire);
        }
    }
}

#[tokio::test]
async fn review_12_incompatible_binding_is_rejected_before_consumption() {
    let dir = tempfile::tempdir().unwrap();
    let j = jobs(&runtime(dir.path()), JobConfig::default());
    let p = Provider::new();
    let id = enqueue(&j, spec("binding-a", &p)).await;
    let mut other = binding(p.clone());
    other.identity.as_mut().unwrap().revision.0 = "2".into();
    assert!(
        j.start_chat(other.clone(), RunnerConfig::default(), "chat".into())
            .await
            .is_err()
    );
    let mut incompatible = spec("binding-b", &p);
    incompatible.payload = model_job_payload(&other, &request()).unwrap();
    assert_eq!(
        j.request(JobRequest::Enqueue(vec![incompatible]))
            .await
            .unwrap_err(),
        JobError::KeyConflict
    );
    assert_eq!(
        (row(&j, &id).await.state, p.calls.load(Ordering::SeqCst)),
        (JobState::Pending, 0)
    );
    let runner = start(&j, p).await;
    wait_state(&j, &id, JobState::Succeeded).await;
    runner.shutdown().await.unwrap();
    // Confirmation must not reopen the kind for another binding, even after restart.
    let page = j.completions(1, 10000).await.unwrap();
    j.request(JobRequest::Ack(vec![(
        page[0].delivery.token.clone(),
        Disposition::Accepted,
    )]))
    .await
    .unwrap();
    let reopened = jobs(&runtime(dir.path()), JobConfig::default());
    assert!(
        reopened
            .start_chat(other, RunnerConfig::default(), "chat".into())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn review_19_model_api_rejects_handler_enqueue_atomically() {
    let dir = tempfile::tempdir().unwrap();
    let j = jobs(&runtime(dir.path()), JobConfig::default());
    let p = Provider::new();
    let mut handler = spec("handler", &p);
    handler.execution = Execution::Handler;
    assert_eq!(
        j.request(JobRequest::Enqueue(vec![spec("model", &p), handler]))
            .await
            .unwrap_err(),
        JobError::InvalidRequest
    );
    assert_eq!(
        sql(dir.path())
            .query_row("SELECT count(*) FROM jobs", [], |r| r.get::<_, usize>(0))
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn review_11_scopes_keep_independent_ledger_answers() {
    let dir = tempfile::tempdir().unwrap();
    let r = runtime(dir.path());
    let a = jobs(&r, JobConfig::default());
    let b = r
        .model_jobs(
            JobScope {
                tenant: "tenant".into(),
                incarnation: "2".into(),
                queue: "model".into(),
            },
            JobConfig::default(),
        )
        .unwrap();
    let p = Provider::new();
    let first = enqueue(&a, spec("shared-key", &p)).await;
    let second = enqueue(&b, spec("shared-key", &p)).await;
    let ra = start(&a, p.clone()).await;
    wait_state(&a, &first, JobState::Succeeded).await;
    let rb = start(&b, p.clone()).await;
    wait_state(&b, &second, JobState::Succeeded).await;
    let delivered = a.completions(1, 10000).await.unwrap();
    a.request(JobRequest::Ack(vec![(
        delivered[0].delivery.token.clone(),
        Disposition::Accepted,
    )]))
    .await
    .unwrap();
    let other = b.completions(1, 10000).await.unwrap();
    assert!(other[0].output.is_some());
    assert_ne!(
        row(&a, &first).await.receipt,
        row(&b, &second).await.receipt
    );
    assert_eq!(copies(dir.path()), 1);
    assert_eq!(p.calls.load(Ordering::SeqCst), 2);
    ra.shutdown().await.unwrap();
    rb.shutdown().await.unwrap();
}

#[tokio::test]
async fn review_4_direct_settlement_respects_purge_even_after_confirmation() {
    let dir = tempfile::tempdir().unwrap();
    let r = runtime(dir.path());
    let j = jobs(&r, JobConfig::default());
    let mut p = Provider::new();
    p.finish = Some(Arc::new(Notify::new()));
    let key = scoped_invocation("direct-race").unwrap();
    let call = tokio::spawn({
        let r = r.clone();
        let p = p.clone();
        async move { r.execute_chat(binding(p), &key, request()).await }
    });
    p.started.notified().await;
    let id = enqueue(&j, spec("direct-race", &p)).await;
    j.request(JobRequest::PurgeOwner("owner-a".into()))
        .await
        .unwrap();
    let page = j.completions(1, 10000).await.unwrap();
    j.request(JobRequest::Ack(vec![(
        page[0].delivery.token.clone(),
        Disposition::Discarded,
    )]))
    .await
    .unwrap();
    p.finish.as_ref().unwrap().notify_one();
    assert!(call.await.unwrap().is_ok());
    assert_eq!(copies(dir.path()), 0);
    assert_eq!(row(&j, &id).await.final_state, Some(JobState::Purged));
    assert!(row(&j, &id).await.receipt.is_some());
    assert_eq!(
        sql(dir.path())
            .query_row(
                "SELECT count(*) FROM spend_receipts WHERE state='settled'",
                [],
                |r| r.get::<_, usize>(0)
            )
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn purge_before_reservation_prevents_later_recovery_even_after_confirmation() {
    for confirmed in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let r = runtime(dir.path());
        let j = jobs(&r, JobConfig::default());
        let p = Provider::new();
        let id = enqueue(&j, spec("purge-before-reservation", &p)).await;
        let key = j.invocation_key("purge-before-reservation").unwrap();
        j.request(JobRequest::PurgeOwner("owner-a".into()))
            .await
            .unwrap();
        assert!(row(&j, &id).await.receipt.is_none());
        assert_eq!(
            sql(dir.path())
                .query_row("SELECT count(*) FROM spend_receipts", [], |r| r
                    .get::<_, usize>(0))
                .unwrap(),
            0
        );
        if confirmed {
            let page = j.completions(1, 10000).await.unwrap();
            j.request(JobRequest::Ack(vec![(
                page[0].delivery.token.clone(),
                Disposition::Discarded,
            )]))
            .await
            .unwrap();
            assert_eq!(row(&j, &id).await.final_state, Some(JobState::Purged));
        }
        let b = binding(p.clone());
        let result = r.execute_chat(b.clone(), &key, request()).await.unwrap();
        assert_eq!(result.output.text, "paid answer");
        let status = r
            .invocation_status(b.identity.as_ref().unwrap(), None, &key)
            .unwrap()
            .unwrap();
        assert_eq!(status.state, SpendState::Settled);
        assert!(!status.output_available);
        assert_eq!(copies(dir.path()), 0);
        drop(j);
        drop(r);
        assert!(matches!(
            runtime(dir.path()).execute_chat(b, &key, request()).await,
            Err(ExecutionError {
                source: ModelError::Queue(symbiotic_core::DiagnosticCode::InvocationCompleted),
                ..
            })
        ));
        assert_eq!(p.calls.load(Ordering::SeqCst), 1);
    }
}

async fn binding_purge_isolation(same_key: bool) {
    for confirmed in [false, true] {
        for answer_before_purge in [false, true] {
            for paid_job in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let r = runtime(dir.path());
                let j = jobs(&r, JobConfig::default());
                let p = Provider::new();
                let id = enqueue(&j, spec("binding-purge", &p)).await;
                let key = j
                    .invocation_key(if same_key {
                        "binding-purge"
                    } else {
                        "other-kind"
                    })
                    .unwrap();
                if paid_job {
                    let runner = start(&j, p.clone()).await;
                    wait_state(&j, &id, JobState::Succeeded).await;
                    runner.shutdown().await.unwrap();
                }
                let mut other = binding(p.clone());
                other.identity.as_mut().unwrap().revision.0 = "2".into();
                // Register the second binding under a different kind in this same scope.
                let mut second = spec("other-kind", &p);
                second.kind = "other-chat".into();
                second.owners = vec!["other-owner".into()];
                second.payload = model_job_payload(&other, &request()).unwrap();
                enqueue(&j, second).await;
                if answer_before_purge {
                    r.execute_chat(other.clone(), &key, request())
                        .await
                        .unwrap();
                }
                j.request(JobRequest::PurgeOwner("owner-a".into()))
                    .await
                    .unwrap();
                assert_eq!(row(&j, &id).await.state, JobState::Purged);
                if confirmed {
                    let page = j.completions(1, 10000).await.unwrap();
                    j.request(JobRequest::Ack(vec![(
                        page[0].delivery.token.clone(),
                        Disposition::Discarded,
                    )]))
                    .await
                    .unwrap();
                    assert_eq!(row(&j, &id).await.final_state, Some(JobState::Purged));
                }
                if !answer_before_purge {
                    r.execute_chat(other.clone(), &key, request())
                        .await
                        .unwrap();
                }
                let status = r
                    .invocation_status(other.identity.as_ref().unwrap(), None, &key)
                    .unwrap()
                    .unwrap();
                assert_eq!(status.state, SpendState::Settled);
                assert!(status.output_available);
                assert_eq!(copies(dir.path()), 1);
                drop(j);
                drop(r);
                let reopened = runtime(dir.path());
                let recovered = reopened.execute_chat(other, &key, request()).await.unwrap();
                assert_eq!(recovered.output.text, "paid answer");
                assert_eq!(recovered.attempt.unwrap().unwrap(), status);
                assert_eq!(p.calls.load(Ordering::SeqCst), 1 + usize::from(paid_job));
            }
        }
    }
}

#[tokio::test]
async fn review_6_reconciled_paid_without_answer_finalizes() {
    for disposition in ["failed", "cancelled", "purged"] {
        let dir = tempfile::tempdir().unwrap();
        let r = runtime(dir.path());
        let j = jobs(&r, JobConfig::default());
        let mut p = Provider::new();
        p.failures = 1;
        let id = enqueue(&j, spec("reconcile", &p)).await;
        let runner = start(&j, p.clone()).await;
        let uncertain = wait_state(&j, &id, JobState::Uncertain).await;
        runner.shutdown().await.unwrap();
        if disposition == "cancelled" {
            j.request(JobRequest::Cancel(Selector::Ids(vec![id.clone()])))
                .await
                .unwrap();
        }
        if disposition == "purged" {
            j.request(JobRequest::PurgeOwner("owner-a".into()))
                .await
                .unwrap();
        }
        r.reconcile_spend(
            &SpendReceiptRef::new(uncertain.receipt.unwrap()).unwrap(),
            SpendState::Settled,
            Some(UsageTrace {
                input_tokens: Some(1),
                ..Default::default()
            }),
        )
        .unwrap();
        let runner = start(&j, p.clone()).await;
        wait_state(
            &j,
            &id,
            match disposition {
                "cancelled" => JobState::Cancelled,
                "purged" => JobState::Purged,
                _ => JobState::Failed,
            },
        )
        .await;
        assert_eq!(p.calls.load(Ordering::SeqCst), 1);
        assert!(j.completions(1, 10000).await.unwrap()[0].output.is_none());
        runner.shutdown().await.unwrap();
    }
}

async fn review_7_paid_answer_size_is_enforced_on_settlement_and_adoption(adopt: bool) {
    let dir = tempfile::tempdir().unwrap();
    let r = runtime(dir.path());
    let j = jobs(
        &r,
        JobConfig {
            max_result_bytes: 16,
            ..Default::default()
        },
    );
    let p = Provider::new();
    if adopt {
        r.execute_chat(
            binding(p.clone()),
            &scoped_invocation("oversize").unwrap(),
            request(),
        )
        .await
        .unwrap();
    }
    let id = enqueue(&j, spec("oversize", &p)).await;
    let runner = start(&j, p.clone()).await;
    let failed = wait_state(&j, &id, JobState::Failed).await;
    assert_eq!(
        failed.diagnostic,
        Some(symbiotic_core::DiagnosticCode::QueueResultTooLarge)
    );
    assert_eq!(copies(dir.path()), 0);
    assert_eq!(
        j_runtime_receipt(
            dir.path(),
            &SpendReceiptRef::new(failed.receipt.unwrap()).unwrap()
        )
        .state,
        SpendState::Settled
    );
    assert!(j.completions(1, 10000).await.unwrap()[0].output.is_none());
    assert_eq!(p.calls.load(Ordering::SeqCst), 1);
    runner.shutdown().await.unwrap();
}
#[tokio::test]
async fn review_7_settlement_enforces_paid_answer_size() {
    review_7_paid_answer_size_is_enforced_on_settlement_and_adoption(false).await;
}

#[tokio::test]
async fn review_7_adoption_enforces_paid_answer_size() {
    review_7_paid_answer_size_is_enforced_on_settlement_and_adoption(true).await;
}

#[tokio::test]
async fn review_8_maintenance_budget_counts_ledger_answers() {
    let dir = tempfile::tempdir().unwrap();
    let j = jobs(
        &runtime(dir.path()),
        JobConfig {
            maintenance_bytes_per_pass: 1,
            ..Default::default()
        },
    );
    let p = Provider::new();
    let first = enqueue(&j, spec("expire-one", &p)).await;
    let second = enqueue(&j, spec("expire-two", &p)).await;
    let runner = start(&j, p).await;
    wait_state(&j, &first, JobState::Succeeded).await;
    wait_state(&j, &second, JobState::Succeeded).await;
    runner.shutdown().await.unwrap();
    // No job-copy bytes remain: the entire admission cost is canonical ledger output.
    sql(dir.path())
        .execute("UPDATE jobs SET recovery_until=0,payload=NULL", [])
        .unwrap();
    assert!(matches!(
        j.request(JobRequest::Maintain).await.unwrap(),
        JobResponse::Changed(1)
    ));
    assert_eq!(copies(dir.path()), 1);
    assert!(matches!(
        j.request(JobRequest::Maintain).await.unwrap(),
        JobResponse::Changed(1)
    ));
    assert_eq!(copies(dir.path()), 0);
}

struct BrokenTrace;
#[async_trait]
impl symbiotic_trace::TraceSink for BrokenTrace {
    async fn record_model_invocation(
        &self,
        _: ModelInvocationTrace,
    ) -> Result<(), symbiotic_trace::TraceError> {
        Err(symbiotic_trace::TraceError::Sink(
            symbiotic_core::DiagnosticCode::StorageFailure,
        ))
    }
}
async fn review_9_10_jobs_emit_attempt_receipts_and_report_trace_failure(trace_failure: bool) {
    let dir = tempfile::tempdir().unwrap();
    drop(runtime(dir.path()));
    let receipts = Arc::new(model::InMemoryReceiptSink::default());
    let r = Runtime::open(RuntimeConfig {
        state_dir: Some(dir.path().into()),
        receipt_sink: Some(receipts.clone()),
        trace_sink: trace_failure
            .then(|| Arc::new(BrokenTrace) as Arc<dyn symbiotic_trace::TraceSink>),
        ..Default::default()
    })
    .unwrap();
    let j = jobs(&r, JobConfig::default());
    let p = Provider::new();
    let id = enqueue(&j, spec("receipted", &p)).await;
    let runner = start(&j, p).await;
    let completed = wait_state(&j, &id, JobState::Succeeded).await;
    let result = runner.shutdown().await;
    assert_eq!(result.is_err(), trace_failure);
    let events = receipts.receipts();
    assert_eq!(
        events.iter().map(|e| e.status).collect::<Vec<_>>(),
        vec![
            model::ReceiptStatus::Queued,
            model::ReceiptStatus::Running,
            model::ReceiptStatus::Succeeded
        ]
    );
    assert_eq!(
        events[1].spend_receipt.as_ref().unwrap().as_str(),
        completed.receipt.as_ref().unwrap()
    );
    assert_eq!(events[2].spend_receipt, events[1].spend_receipt);
    assert_eq!(events[2].attempt, 1);
    assert_eq!(events[2].usage.as_ref().unwrap().input_tokens, Some(3));
    if trace_failure {
        assert!(events[2].metadata.get(model::RUNTIME_DIAGNOSTICS).is_some());
    }
    assert!(j.completions(1, 10000).await.unwrap()[0].output.is_some());
}
#[tokio::test]
async fn review_9_jobs_emit_attempt_receipts() {
    review_9_10_jobs_emit_attempt_receipts_and_report_trace_failure(false).await;
}

#[tokio::test]
async fn review_10_trace_failure_reaches_runner_with_paid_answer_retained() {
    review_9_10_jobs_emit_attempt_receipts_and_report_trace_failure(true).await;
}

#[tokio::test]
async fn review_14_exhausted_attempt_skips_backoff() {
    let dir = tempfile::tempdir().unwrap();
    let j = jobs(&runtime(dir.path()), JobConfig::default());
    let mut p = Provider::new();
    p.failures = 1;
    p.known_zero = true;
    let id = enqueue(&j, spec("exhausted", &p)).await;
    let mut b = binding(p.clone());
    b.policy.as_mut().unwrap().retry_base_delay_ms = 60000;
    let runner = j
        .start_chat(b, RunnerConfig::default(), "chat".into())
        .await
        .unwrap();
    tokio::time::timeout(
        Duration::from_secs(1),
        wait_state(&j, &id, JobState::Refused),
    )
    .await
    .unwrap();
    assert_eq!(p.calls.load(Ordering::SeqCst), 1);
    runner.shutdown().await.unwrap();
}

#[tokio::test]
async fn review_15_provider_panic_persists_uncertainty() {
    let dir = tempfile::tempdir().unwrap();
    let j = jobs(&runtime(dir.path()), JobConfig::default());
    let mut p = Provider::new();
    p.panic = true;
    p.known_zero = true;
    let id = enqueue(&j, spec("panic", &p)).await;
    let runner = start(&j, p.clone()).await;
    assert!(runner.wait().await.is_err());
    let uncertain = row(&j, &id).await;
    assert_eq!(uncertain.state, JobState::Uncertain);
    assert_eq!(
        j_runtime_receipt(
            dir.path(),
            &SpendReceiptRef::new(uncertain.receipt.unwrap()).unwrap()
        )
        .state,
        SpendState::Unknown
    );
    assert_eq!(p.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn review_3_purge_resolves_current_answer_after_released_predecessor() {
    let dir = tempfile::tempdir().unwrap();
    let r = runtime(dir.path());
    let j = jobs(&r, JobConfig::default());
    let p = Provider::new();
    let mut s = spec("predecessor", &p);
    s.limits.max_attempts = 2;
    let id = enqueue(&j, s).await;
    // A prior known-zero claim has returned the job to Pending.
    let key = scoped_invocation("predecessor").unwrap();
    let mut zero = p.clone();
    zero.failures = 1;
    zero.known_zero = true;
    let mut b = binding(zero);
    b.policy.as_mut().unwrap().logical_retry_attempts = 1;
    assert!(r.execute_chat(b, &key, request()).await.is_err());
    let predecessor: String = sql(dir.path())
        .query_row(
            "SELECT reference FROM spend_receipts ORDER BY rowid LIMIT 1",
            [],
            |r| r.get(0),
        )
        .unwrap();
    // Permit the second explicit attempt under the original frozen ceiling.
    sql(dir.path())
        .execute("UPDATE spend_receipts SET attempt_limit=2", [])
        .unwrap();
    sql(dir.path())
        .execute("UPDATE jobs SET receipt=?1,generation=1", [&predecessor])
        .unwrap();
    r.execute_chat(binding(p.clone()), &key, request())
        .await
        .unwrap();
    assert_eq!(copies(dir.path()), 1);
    j.request(JobRequest::PurgeOwner("owner-a".into()))
        .await
        .unwrap();
    assert_eq!(row(&j, &id).await.state, JobState::Purged);
    assert_eq!(copies(dir.path()), 0);
}

#[tokio::test]
async fn review_1_recovery_uses_recorded_attempt_not_latest_invocation() {
    let dir = tempfile::tempdir().unwrap();
    let r = runtime(dir.path());
    let j = jobs(&r, JobConfig::default());
    let mut p = Provider::new();
    p.failures = 1;
    let mut s = spec("same-attempt", &p);
    s.limits.max_attempts = 2;
    let id = enqueue(&j, s).await;
    let runner = start(&j, p.clone()).await;
    let uncertain = wait_state(&j, &id, JobState::Uncertain).await;
    runner.shutdown().await.unwrap();
    let reference = SpendReceiptRef::new(uncertain.receipt.unwrap()).unwrap();
    r.reconcile_spend(&reference, SpendState::Released, None)
        .unwrap();
    let ledger = spend::SqliteSpendLedger::open(&dir.path().join(QUEUE_DATABASE)).unwrap();
    let mut later = ledger.receipt(&reference).unwrap().unwrap().reservation;
    later.reference = SpendReceiptRef::new("later-paid-attempt").unwrap();
    assert!(ledger.reserve(&later).unwrap());
    ledger
        .finish(
            &later.reference,
            SpendState::Settled,
            Some(UsageTrace {
                input_tokens: Some(1),
                ..Default::default()
            }),
            Some(serde_json::json!({"text": "later answer"})),
            Some(&scoped_invocation("same-attempt").unwrap()),
        )
        .unwrap();
    // An exhausted claim must resolve its recorded zero-charge receipt, not
    // adopt the newer paid answer. This fixture isolates recovery selection.
    sql(dir.path())
        .execute("UPDATE jobs SET max_attempts=1", [])
        .unwrap();
    let runner = start(&j, p.clone()).await;
    let refused = wait_state(&j, &id, JobState::Refused).await;
    assert_eq!(refused.receipt.as_deref(), Some(reference.as_str()));
    assert_eq!(
        refused.diagnostic,
        Some(symbiotic_core::DiagnosticCode::AttemptBudgetExhausted)
    );
    assert_eq!(p.calls.load(Ordering::SeqCst), 1);
    runner.shutdown().await.unwrap();
}

#[tokio::test]
async fn review_5_invalid_classifier_is_refused_and_valid_work_continues() {
    let dir = tempfile::tempdir().unwrap();
    let r = runtime(dir.path());
    let j = jobs(&r, JobConfig::default());
    let b = ModelBinding::new(model::StaticClassifierProvider::new([
        model::ClassifierAnswer::noul("q", 0.5),
    ]))
    .with_identity(BindingIdentity::new("tenant", "classifier", "1", "account"))
    .with_policy(ModelQueueConfig::default());
    let bad = model::ClassifyRequest::new(Default::default(), vec![]);
    let good = model::ClassifyRequest::new(
        Default::default(),
        vec![model::ClassifierQuestion::noul("q", "question", None, None)],
    );
    let p = Provider::new();
    let mut s = spec("bad-classifier", &p);
    s.kind = "classify".into();
    s.payload = model_job_payload(&b, &bad).unwrap();
    let bad_id = enqueue(&j, s.clone()).await;
    s.key = "good-classifier".into();
    s.payload = model_job_payload(&b, &good).unwrap();
    let good_id = enqueue(&j, s).await;
    let runner = j
        .start_classifier(b, RunnerConfig::default(), "classify".into())
        .await
        .unwrap();
    assert_eq!(
        wait_state(&j, &bad_id, JobState::Refused).await.generation,
        0
    );
    wait_state(&j, &good_id, JobState::Succeeded).await;
    runner.shutdown().await.unwrap();
}

#[tokio::test]
async fn review_13_unsent_candidate_observes_cancel_and_shutdown_during_admission() {
    for cancel in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let r = runtime(dir.path());
        let j = jobs(&r, JobConfig::default());
        let p = Provider::new();
        let id = enqueue(&j, spec("waiting", &p)).await;
        // A future cooldown forces an unsent wait without relying on scheduler speed.
        let account = account_scope(binding(p.clone()).identity.as_ref().unwrap(), None).unwrap();
        sql(dir.path())
            .execute(
                "INSERT INTO queue_cooldowns(queue_id,cooldown_until,updated_at) VALUES (?1,?2,?3)",
                rusqlite::params![
                    account,
                    (chrono::Utc::now() + chrono::Duration::minutes(5)).to_rfc3339(),
                    chrono::Utc::now().to_rfc3339()
                ],
            )
            .unwrap();
        let runner = start(&j, p.clone()).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        if cancel {
            j.request(JobRequest::Cancel(Selector::Ids(vec![id.clone()])))
                .await
                .unwrap();
        }
        tokio::time::timeout(Duration::from_millis(500), runner.shutdown())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(p.calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            row(&j, &id).await.state,
            if cancel {
                JobState::Cancelled
            } else {
                JobState::Pending
            }
        );
        assert!(row(&j, &id).await.receipt.is_none());
    }
}

#[tokio::test]
async fn purging_one_binding_preserves_another_bindings_recovery_answer() {
    binding_purge_isolation(false).await;
}

#[tokio::test]
async fn audit_51_purging_one_binding_preserves_same_scoped_key_on_other_binding() {
    binding_purge_isolation(true).await;
}

#[tokio::test]
async fn audit_50_lowered_live_bound_purges_every_paid_copy() {
    let dir = tempfile::tempdir().unwrap();
    let r = runtime(dir.path());
    let j = jobs(&r, JobConfig::default());
    let p = Provider::new();
    let first = enqueue(&j, spec("first", &p)).await;
    let second = enqueue(&j, spec("second", &p)).await;
    let runner = start(&j, p).await;
    wait_state(&j, &first, JobState::Succeeded).await;
    wait_state(&j, &second, JobState::Succeeded).await;
    runner.shutdown().await.unwrap();
    drop(j);
    drop(r);
    let reopened = runtime(dir.path());
    let lowered = jobs(
        &reopened,
        JobConfig {
            max_live_jobs: 1,
            ..Default::default()
        },
    );
    assert!(matches!(
        lowered
            .request(JobRequest::PurgeOwner("owner-a".into()))
            .await
            .unwrap(),
        JobResponse::Changed(2)
    ));
    assert_eq!(row(&lowered, &first).await.state, JobState::Purged);
    assert_eq!(row(&lowered, &second).await.state, JobState::Purged);
    assert_eq!(copies(dir.path()), 0);
}

#[tokio::test]
async fn audit_52_pending_adopts_settled_receipt_without_output() {
    let dir = tempfile::tempdir().unwrap();
    let r = runtime(dir.path());
    let j = jobs(&r, JobConfig::default());
    let mut p = Provider::new();
    p.failures = 1;
    let key = j.invocation_key("settled-empty").unwrap();
    let b = binding(p.clone());
    assert!(r.execute_chat(b.clone(), &key, request()).await.is_err());
    let status = r
        .invocation_status(b.identity.as_ref().unwrap(), None, &key)
        .unwrap()
        .unwrap();
    r.reconcile_spend(
        &status.reference,
        SpendState::Settled,
        Some(UsageTrace {
            input_tokens: Some(1),
            ..Default::default()
        }),
    )
    .unwrap();
    let id = enqueue(&j, spec("settled-empty", &p)).await;
    let runner = start(&j, p.clone()).await;
    let failed = wait_state(&j, &id, JobState::Failed).await;
    assert_eq!(failed.receipt.as_deref(), Some(status.reference.as_str()));
    assert_eq!(
        failed.diagnostic,
        Some(symbiotic_core::DiagnosticCode::InvocationCompleted)
    );
    assert_eq!(p.calls.load(Ordering::SeqCst), 1);
    assert_eq!(copies(dir.path()), 0);
    runner.shutdown().await.unwrap();
}

#[tokio::test]
async fn audit_53_direct_settlement_obeys_matching_job_deadline() {
    for expired in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let r = runtime(dir.path());
        let j = jobs(&r, JobConfig::default());
        let p = Provider::new();
        let mut s = spec("direct-deadline", &p);
        let until = chrono::Utc::now() + chrono::Duration::seconds(if expired { -60 } else { 60 });
        s.recovery_until = Some(until);
        let id = enqueue(&j, s).await;
        let key = j.invocation_key("direct-deadline").unwrap();
        let b = binding(p.clone());
        r.execute_chat(b.clone(), &key, request()).await.unwrap();
        assert_eq!(copies(dir.path()), usize::from(!expired));
        assert_eq!(row(&j, &id).await.state, JobState::Pending);
        if !expired {
            let deadline: String = sql(dir.path())
                .query_row("SELECT recovery_expires_at FROM spend_receipts", [], |r| {
                    r.get(0)
                })
                .unwrap();
            assert!(chrono::DateTime::parse_from_rfc3339(&deadline).unwrap() <= until);
        }
        let mut other = b;
        other.identity.as_mut().unwrap().revision.0 = "2".into();
        r.execute_chat(other.clone(), &key, request())
            .await
            .unwrap();
        assert!(
            r.invocation_status(other.identity.as_ref().unwrap(), None, &key)
                .unwrap()
                .unwrap()
                .output_available
        );
    }
}

#[tokio::test]
async fn audit_54_failed_attempt_trace_error_reaches_shutdown() {
    let dir = tempfile::tempdir().unwrap();
    drop(runtime(dir.path()));
    let r = Runtime::open(RuntimeConfig {
        state_dir: Some(dir.path().into()),
        trace_sink: Some(Arc::new(BrokenTrace)),
        ..Default::default()
    })
    .unwrap();
    let j = jobs(&r, JobConfig::default());
    let mut p = Provider::new();
    p.failures = 1;
    let id = enqueue(&j, spec("failed-trace", &p)).await;
    let runner = start(&j, p.clone()).await;
    wait_state(&j, &id, JobState::Uncertain).await;
    let result = runner.shutdown().await;
    fn codes(error: symbiotic_queue::runner::RunnerError) -> Vec<symbiotic_core::DiagnosticCode> {
        match error {
            symbiotic_queue::runner::RunnerError::Workers(errors) => {
                errors.into_iter().flat_map(codes).collect()
            }
            symbiotic_queue::runner::RunnerError::Store(JobError::Execution(code)) => vec![code],
            other => panic!("unexpected runner failure: {other:?}"),
        }
    }
    assert_eq!(
        codes(result.unwrap_err()),
        [
            symbiotic_core::DiagnosticCode::HttpUnavailable,
            symbiotic_core::DiagnosticCode::StorageFailure
        ]
    );
    assert_eq!(p.calls.load(Ordering::SeqCst), 1);
    assert_eq!(copies(dir.path()), 0);
}

#[tokio::test]
async fn trial_purge_erases_cancelled_receiptless_direct_answer() {
    let dir = tempfile::tempdir().unwrap();
    let r = runtime(dir.path());
    let j = jobs(&r, JobConfig::default());
    let p = Provider::new();
    let id = enqueue(&j, spec("cancelled-direct", &p)).await;
    let key = j.invocation_key("cancelled-direct").unwrap();
    r.execute_chat(binding(p.clone()), &key, request())
        .await
        .unwrap();
    j.request(JobRequest::Cancel(Selector::Ids(vec![id.clone()])))
        .await
        .unwrap();
    let cancelled = row(&j, &id).await;
    assert_eq!(cancelled.state, JobState::Cancelled);
    assert!(cancelled.payload.is_none());
    assert!(cancelled.receipt.is_none());
    assert_eq!(copies(dir.path()), 1);
    j.request(JobRequest::PurgeOwner("owner-a".into()))
        .await
        .unwrap();
    assert_eq!(copies(dir.path()), 0);
    assert!(row(&j, &id).await.receipt.is_none());
    assert!(
        r.execute_chat(binding(p.clone()), &key, request())
            .await
            .is_err()
    );
    assert_eq!(p.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn trial_enqueue_clamps_preexisting_answer_and_adoption_preserves_deadline() {
    for adopt in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let clock = test_clock::clock();
        let r = runtime_with_config(
            dir.path(),
            RuntimeConfig {
                clock: clock.clone(),
                ..Default::default()
            },
        );
        let j = jobs(&r, JobConfig::default());
        let p = Provider::new();
        let key = j.invocation_key("earlier-deadline").unwrap();
        r.execute_chat(binding(p.clone()), &key, request())
            .await
            .unwrap();
        let mut s = spec("earlier-deadline", &p);
        let until = clock() + chrono::Duration::milliseconds(500);
        s.recovery_until = Some(until);
        let id = enqueue(&j, s).await;
        if adopt {
            let runner = start(&j, p.clone()).await;
            wait_state(&j, &id, JobState::Succeeded).await;
            runner.shutdown().await.unwrap();
        }
        let deadline: Option<String> = sql(dir.path())
            .query_row("SELECT recovery_expires_at FROM spend_receipts", [], |r| {
                r.get(0)
            })
            .unwrap();
        if let Some(deadline) = deadline {
            assert!(chrono::DateTime::parse_from_rfc3339(&deadline).unwrap() <= until);
        } else {
            assert_eq!(copies(dir.path()), 0);
        }
        tokio::time::sleep(
            (until - clock()).to_std().unwrap_or_default() + Duration::from_millis(5),
        )
        .await;
        assert!(
            r.execute_chat(binding(p.clone()), &key, request())
                .await
                .is_err()
        );
        assert_eq!(p.calls.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn trial_confirmed_cancel_prohibits_later_direct_recovery() {
    for disposition in [Disposition::Accepted, Disposition::Discarded] {
        let dir = tempfile::tempdir().unwrap();
        let r = runtime(dir.path());
        let j = jobs(&r, JobConfig::default());
        let mut p = Provider::new();
        let finish = Arc::new(Notify::new());
        p.finish = Some(finish.clone());
        let id = enqueue(&j, spec("confirmed-direct", &p)).await;
        let key = j.invocation_key("confirmed-direct").unwrap();
        let call = tokio::spawn({
            let r = r.clone();
            let p = p.clone();
            let key = key.clone();
            async move { r.execute_chat(binding(p), &key, request()).await }
        });
        p.started.notified().await;
        j.request(JobRequest::Cancel(Selector::Ids(vec![id.clone()])))
            .await
            .unwrap();
        let delivery = j.completions(1, 10000).await.unwrap().remove(0);
        j.request(JobRequest::Ack(vec![(
            delivery.delivery.token,
            disposition,
        )]))
        .await
        .unwrap();
        finish.notify_one();
        call.await.unwrap().unwrap();
        assert_eq!(copies(dir.path()), 0);
        let b = binding(p.clone());
        let status = r
            .invocation_status(b.identity.as_ref().unwrap(), None, &key)
            .unwrap()
            .unwrap();
        assert_eq!(status.state, SpendState::Settled);
        assert!(!status.output_available);
        assert!(r.execute_chat(b, &key, request()).await.is_err());
        assert_eq!(p.calls.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn close_confirmation_erases_receiptless_paid_answer() {
    for disposition in [Disposition::Accepted, Disposition::Discarded] {
        let dir = tempfile::tempdir().unwrap();
        let r = runtime(dir.path());
        let j = jobs(&r, JobConfig::default());
        let p = Provider::new();
        let id = enqueue(&j, spec("receiptless-confirmation", &p)).await;
        let key = j.invocation_key("receiptless-confirmation").unwrap();
        r.execute_chat(binding(p.clone()), &key, request())
            .await
            .unwrap();
        j.request(JobRequest::Cancel(Selector::Ids(vec![id.clone()])))
            .await
            .unwrap();
        let cancelled = row(&j, &id).await;
        assert_eq!(cancelled.state, JobState::Cancelled);
        assert!(cancelled.receipt.is_none());
        assert_eq!(copies(dir.path()), 1);
        let delivery = j.completions(1, 10000).await.unwrap().remove(0);
        j.request(JobRequest::Ack(vec![(
            delivery.delivery.token,
            disposition,
        )]))
        .await
        .unwrap();
        let retained: (bool, bool) = sql(dir.path())
            .query_row(
                "SELECT recovery IS NOT NULL, recovery_expires_at IS NOT NULL FROM spend_receipts",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(retained, (false, false));
        assert!(row(&j, &id).await.owners.is_empty());
        assert!(
            r.execute_chat(binding(p.clone()), &key, request())
                .await
                .is_err()
        );
        assert_eq!(p.calls.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn close_enqueue_narrows_deadline_without_decoding_saved_answer() {
    let dir = tempfile::tempdir().unwrap();
    let r = runtime(dir.path());
    let j = jobs(&r, JobConfig::default());
    let p = Provider::new();
    let key = j.invocation_key("metadata-only").unwrap();
    r.execute_chat(binding(p.clone()), &key, request())
        .await
        .unwrap();
    let mut s = spec("metadata-only", &p);
    let until = chrono::Utc::now() + chrono::Duration::hours(1);
    s.recovery_until = Some(until);
    // Invalid content detects any attempted answer deserialization. The
    // metadata update must neither read nor rewrite that content.
    sql(dir.path())
        .execute("UPDATE spend_receipts SET recovery='invalid JSON'", [])
        .unwrap();
    enqueue(&j, s.clone()).await;
    for replay in [false, true] {
        if replay {
            sql(dir.path())
                .execute(
                    "UPDATE spend_receipts SET recovery_expires_at=?1",
                    [(until + chrono::Duration::hours(1)).to_rfc3339()],
                )
                .unwrap();
            j.request(JobRequest::Enqueue(vec![s.clone()]))
                .await
                .unwrap();
        }
        let (content, deadline): (String, String) = sql(dir.path())
            .query_row(
                "SELECT recovery, recovery_expires_at FROM spend_receipts",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(content, "invalid JSON");
        assert!(chrono::DateTime::parse_from_rfc3339(&deadline).unwrap() <= until);
    }
    assert_eq!(p.calls.load(Ordering::SeqCst), 1);
}
