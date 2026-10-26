//! Configured HTTP boundaries, with synthetic credentials and loopback servers.
use async_trait::async_trait;
use serde_json::{Value, json};
use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::Arc,
};
use symbiotic_ai_runtime::{
    ChatProvider, ChatRequest, ConfiguredProvider, InMemoryReceiptSink, ModelError,
    ModelQueueConfig, Runtime, RuntimeConfig, model,
};
use symbiotic_core::{DiagnosticCode, ProviderPrincipalId, TenantId};
use symbiotic_trace::InMemoryTraceSink;

const KEY: &str = "synthetic-credential-echo-9271";

struct Credentials<'a>(&'a str);
#[async_trait]
impl model::CredentialResolver for Credentials<'_> {
    async fn resolve_auth(
        &self,
        _: &model::ProviderAuthMode,
    ) -> Result<model::ResolvedAuth, ModelError> {
        Ok(model::ResolvedAuth::ApiKey(self.0.into()))
    }
}

fn fixture(
    status: u16,
    body: String,
    location: Option<String>,
) -> (String, std::thread::JoinHandle<()>) {
    fixture_bytes(status, body.into_bytes(), location)
}

fn fixture_bytes(
    status: u16,
    body: Vec<u8>,
    location: Option<String>,
) -> (String, std::thread::JoinHandle<()>) {
    fixture_at(
        TcpListener::bind("127.0.0.1:0").unwrap(),
        status,
        body,
        location,
    )
}

fn fixture_at(
    listener: TcpListener,
    status: u16,
    body: Vec<u8>,
    location: Option<String>,
) -> (String, std::thread::JoinHandle<()>) {
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream.set_nodelay(true).unwrap();
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();
        let mut bytes = Vec::new();
        loop {
            let mut chunk = [0; 4096];
            let n = stream.read(&mut chunk).unwrap();
            assert!(n > 0);
            bytes.extend_from_slice(&chunk[..n]);
            if let Some(offset) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&bytes[..offset]);
                let length = headers
                    .lines()
                    .find_map(|line| {
                        let (key, value) = line.split_once(':')?;
                        key.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().unwrap())
                    })
                    .unwrap();
                if bytes.len() >= offset + 4 + length {
                    break;
                }
            }
        }
        let location = location.map_or(String::new(), |url| format!("Location: {url}\r\n"));
        write!(stream, "HTTP/1.1 {status} Fixture\r\nContent-Type: application/json\r\n{location}Content-Length: {}\r\nConnection: close\r\n\r\n", body.len()).unwrap();
        stream.write_all(&body).unwrap();
    });
    (url, server)
}

async fn configured(
    endpoint: &str,
    state: &std::path::Path,
    classifier: bool,
    key: &str,
) -> (
    ConfiguredProvider,
    Arc<InMemoryReceiptSink>,
    Arc<InMemoryTraceSink>,
) {
    let mut config: Value =
        serde_json::from_slice(include_bytes!("../../../examples/model-registry.json")).unwrap();
    config["accounts"] = json!([{"id": "policy", "policy": ModelQueueConfig {
        logical_retry_attempts: 1, retry_attempts: 1, request_timeout_seconds: Some(2), ..ModelQueueConfig::default()
    }}]);
    if classifier {
        config["models"][0]["adapter"] = json!("jev_classifier");
        config["models"][0]["operations"] = json!(["classify"]);
        config["models"][0]["identity"]["operation"] = json!("classify");
    }
    config["bindings"] = json!([{
        "identity": {"tenant": "tenant", "provider": "provider", "revision": "1", "account": "account"},
        "model": "example-chat", "endpoint": endpoint, "secret_ref": "synthetic",
        "account_policy": "policy", "account_sharing_key": null,
        "limits": {"max_request_bytes": 65536, "max_response_bytes": 65536, "max_output_tokens": if classifier { None } else { Some(128) }},
        "settings": {"thinking": null, "reasoning_effort": null, "dimensions": null, "served_model": null}
    }]);
    let receipts = Arc::new(InMemoryReceiptSink::default());
    let traces = Arc::new(InMemoryTraceSink::default());
    let runtime = Runtime::open(RuntimeConfig {
        state_dir: Some(state.to_path_buf()),
        registry: Some(Arc::new(
            model::ModelRegistry::from_json(&serde_json::to_vec(&config).unwrap()).unwrap(),
        )),
        receipt_sink: Some(receipts.clone()),
        trace_sink: Some(traces.clone()),
        ..RuntimeConfig::default()
    })
    .unwrap();
    let provider = runtime
        .configured_provider(
            &TenantId("tenant".into()),
            &ProviderPrincipalId("provider".into()),
            &Credentials(key),
        )
        .await
        .unwrap();
    (provider, receipts, traces)
}

async fn call(provider: &ConfiguredProvider) -> Result<(), ModelError> {
    match provider {
        ConfiguredProvider::Chat(provider) => provider
            .chat(ChatRequest {
                messages: vec![model::ChatMessage {
                    role: "user".into(),
                    content: "synthetic prompt".into(),
                }],
                max_output_tokens: Some(32),
                temperature: None,
                response_format: None,
                role_binding: None,
                source: None,
                metadata: Value::Null,
            })
            .await
            .map(|_| ()),
        ConfiguredProvider::Classifier(provider) => {
            use model::ClassifierProvider;
            provider
                .classify(model::ClassifyRequest::new(
                    serde_json::Map::new(),
                    vec![model::ClassifierQuestion::noul(
                        "answer", "yes?", None, None,
                    )],
                ))
                .await
                .map(|_| ())
        }
        _ => panic!("unexpected adapter"),
    }
}

fn assert_no_secret_files(dir: &std::path::Path) {
    assert_no_files_containing(dir, KEY);
}

fn assert_no_files_containing(dir: &std::path::Path, key: &str) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            assert_no_files_containing(&path, key);
        } else {
            let bytes = std::fs::read(&path).unwrap();
            assert!(
                !bytes.windows(key.len()).any(|part| part == key.as_bytes()),
                "credential in {path:?}"
            );
        }
    }
}

#[tokio::test]
async fn configured_401_echo_cannot_enter_error_or_runtime_bookkeeping() {
    for classifier in [false, true] {
        let (url, server) = fixture(401, format!("unauthorized key={KEY}"), None);
        let state = tempfile::tempdir().unwrap();
        let (provider, receipts, traces) =
            configured(&url, &state.path().join("state"), classifier, KEY).await;
        let err = call(&provider).await.unwrap_err();
        server.join().unwrap();
        assert!(matches!(err, ModelError::Auth(_)), "{err:?}");
        assert!(!format!("{err:?}").contains(KEY));
        assert!(
            !serde_json::to_string(&receipts.receipts())
                .unwrap()
                .contains(KEY)
        );
        assert!(
            !serde_json::to_string(&traces.records())
                .unwrap()
                .contains(KEY)
        );
        assert_no_secret_files(state.path());
    }
}

#[tokio::test]
async fn configured_success_body_echo_is_refused_before_runtime_bookkeeping() {
    let escaped = KEY
        .chars()
        .map(|c| format!("\\u{:04x}", c as u32))
        .collect::<String>();
    for (classifier, body) in [
        (
            false,
            json!({"choices": [{"message": {"content": KEY}}]}).to_string(),
        ),
        (
            false,
            json!({"id": KEY, "choices": [{"message": {"content": "OK"}}]}).to_string(),
        ),
        (
            false,
            format!(
                "{{\"ignored\":\"{escaped}\",\"choices\":[{{\"message\":{{\"content\":\"OK\"}}}}]}}"
            ),
        ),
        (
            true,
            json!({"model": "example-model", "answers": {"answer": 0.5}, "ignored": KEY})
                .to_string(),
        ),
    ] {
        let (url, server) = fixture(200, body, None);
        let state = tempfile::tempdir().unwrap();
        let (provider, receipts, traces) =
            configured(&url, &state.path().join("state"), classifier, KEY).await;
        let result = call(&provider).await;
        server.join().unwrap();
        assert!(matches!(result, Err(ModelError::Provider(_))), "{result:?}");
        assert!(!format!("{result:?}").contains(KEY));
        assert!(
            !serde_json::to_string(&receipts.receipts())
                .unwrap()
                .contains(KEY)
        );
        assert!(
            !serde_json::to_string(&traces.records())
                .unwrap()
                .contains(KEY)
        );
        assert_no_secret_files(state.path());
    }
}

#[tokio::test]
async fn configured_redirects_are_refused_without_contacting_the_target() {
    for classifier in [false, true] {
        for status in [302, 307, 308] {
            let target = TcpListener::bind("127.0.0.1:0").unwrap();
            target.set_nonblocking(true).unwrap();
            let (url, server) = fixture(
                status,
                String::new(),
                Some(format!(
                    "http://localhost:{}/other",
                    target.local_addr().unwrap().port()
                )),
            );
            let state = tempfile::tempdir().unwrap();
            let (provider, _, _) =
                configured(&url, &state.path().join("state"), classifier, KEY).await;
            let err = call(&provider).await.unwrap_err();
            server.join().unwrap();
            assert!(matches!(err, ModelError::Provider(_)), "{err:?}");
            assert_eq!(
                target.accept().unwrap_err().kind(),
                std::io::ErrorKind::WouldBlock
            );
        }
    }
}

#[tokio::test]
async fn final_validation_and_typed_decoding_errors_cannot_reach_bookkeeping() {
    for (classifier, key, body) in [
        (
            true,
            "123400000",
            r#"{"model":"example-model","answers":{"answer":{"type":"noul","noul":123400000}}}"#,
        ),
        (
            false,
            "123400000",
            r#"{"choices":[{"message":{"content":123400000}}]}"#,
        ),
        (
            false,
            KEY,
            r#"{"choices":"private provider decoding detail"}"#,
        ),
    ] {
        let (url, server) = fixture(200, body.into(), None);
        let state = tempfile::tempdir().unwrap();
        let (provider, receipts, traces) =
            configured(&url, &state.path().join("state"), classifier, key).await;
        let error = call(&provider).await.unwrap_err();
        server.join().unwrap();
        if classifier {
            assert!(
                matches!(error, ModelError::Provider(DiagnosticCode::ProviderFailure)),
                "{error:?}"
            );
        } else {
            assert!(
                matches!(
                    error,
                    ModelError::Unavailable(DiagnosticCode::InvalidResponse)
                ),
                "{error:?}"
            );
        }
        for text in [
            error.to_string(),
            serde_json::to_string(&receipts.receipts()).unwrap(),
            serde_json::to_string(&traces.records()).unwrap(),
        ] {
            assert!(!text.contains(key), "credential escaped boundary");
            assert!(!text.contains("private provider decoding detail"));
        }
        assert_no_files_containing(state.path(), key);
    }
}

#[tokio::test]
async fn same_origin_307_never_sends_a_second_request() {
    for classifier in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let target = listener.try_clone().unwrap();
        let (url, server) = fixture_at(listener, 307, Vec::new(), Some("/redirect-target".into()));
        let state = tempfile::tempdir().unwrap();
        let (provider, _, _) = configured(&url, &state.path().join("state"), classifier, KEY).await;
        assert!(matches!(
            call(&provider).await,
            Err(ModelError::Provider(_))
        ));
        server.join().unwrap();
        target.set_nonblocking(true).unwrap();
        assert_eq!(
            target.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
    }
}

#[tokio::test]
async fn jev_success_with_invalid_utf8_is_a_provider_error() {
    let mut body =
        br#"{"model":"example-model","answers":{"answer":{"type":"noul","noul":0.5}},"ignored":""#
            .to_vec();
    body.push(0xff);
    body.extend_from_slice(br#""}"#);
    let (url, server) = fixture_bytes(200, body, None);
    let state = tempfile::tempdir().unwrap();
    let (provider, _, _) = configured(&url, &state.path().join("state"), true, KEY).await;
    assert!(matches!(
        call(&provider).await,
        Err(ModelError::Provider(_))
    ));
    server.join().unwrap();
}

#[tokio::test]
async fn successful_raw_provider_json_never_reaches_response_or_cache() {
    for classifier in [false, true] {
        let body = if classifier {
            json!({"model":"example-model","answers":{"answer":{"type":"noul","noul":0.5}},"ignored":"private raw provider detail"})
        } else {
            json!({"choices":[{"message":{"content":"OK"}}],"ignored":"private raw provider detail"})
        };
        let (url, server) = fixture(200, body.to_string(), None);
        let state = tempfile::tempdir().unwrap();
        let (provider, _, _) = configured(&url, &state.path().join("state"), classifier, KEY).await;
        match &provider {
            ConfiguredProvider::Chat(provider) => {
                let response = provider
                    .chat(ChatRequest {
                        messages: vec![model::ChatMessage {
                            role: "user".into(),
                            content: "prompt".into(),
                        }],
                        max_output_tokens: Some(32),
                        temperature: None,
                        response_format: None,
                        role_binding: None,
                        source: None,
                        metadata: Value::Null,
                    })
                    .await
                    .unwrap();
                assert!(response.raw_provider_response.is_none());
                assert!(
                    !serde_json::to_string(&response)
                        .unwrap()
                        .contains("private raw provider detail")
                );
            }
            ConfiguredProvider::Classifier(provider) => {
                use model::ClassifierProvider;
                let response = provider
                    .classify(model::ClassifyRequest::new(
                        serde_json::Map::new(),
                        vec![model::ClassifierQuestion::noul(
                            "answer", "yes?", None, None,
                        )],
                    ))
                    .await
                    .unwrap();
                assert!(response.raw_provider_response.is_none());
                assert!(
                    !serde_json::to_string(&response)
                        .unwrap()
                        .contains("private raw provider detail")
                );
            }
            _ => unreachable!(),
        }
        server.join().unwrap();
        assert_no_files_containing(state.path(), "private raw provider detail");
    }
}

// Run env-sensitive checks in a child process, without mutating the test runner's environment.
#[test]
fn proxy_environment_is_ignored() {
    let proxy = TcpListener::bind("127.0.0.1:0").unwrap();
    proxy.set_nonblocking(true).unwrap();
    let proxy_url = format!("http://{}", proxy.local_addr().unwrap());
    let mut command = std::process::Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", "proxy_environment_child", "--nocapture"])
        .env("FDN_PROXY_REGRESSION", "1");
    for name in [
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
    ] {
        command.env(name, &proxy_url);
    }
    command.env_remove("NO_PROXY").env_remove("no_proxy");
    let output = command.output().unwrap();
    assert!(
        output.status.success(),
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        proxy.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
}

#[tokio::test]
async fn proxy_environment_child() {
    if std::env::var_os("FDN_PROXY_REGRESSION").is_none() {
        return;
    }
    for classifier in [false, true] {
        let body = if classifier {
            json!({"model":"example-model","answers":{"answer":{"type":"noul","noul":0.5}}})
        } else {
            json!({"choices":[{"message":{"content":"OK"}}]})
        };
        let (url, server) = fixture(200, body.to_string(), None);
        let state = tempfile::tempdir().unwrap();
        let (provider, _, _) = configured(&url, &state.path().join("state"), classifier, KEY).await;
        call(&provider).await.unwrap();
        server.join().unwrap();
    }
}

#[tokio::test]
async fn cached_credential_echo_is_refused_before_cache_hit_bookkeeping() {
    for invalid_type in [false, true] {
        let (url, server) = fixture(
            200,
            json!({"choices":[{"message":{"content":"OK"}}]}).to_string(),
            None,
        );
        let state = tempfile::tempdir().unwrap();
        let (provider, receipts, traces) =
            configured(&url, &state.path().join("state"), false, KEY).await;
        call(&provider).await.unwrap();
        server.join().unwrap();
        let mut dirs = vec![state.path().to_path_buf()];
        let mut changed = 0;
        while let Some(dir) = dirs.pop() {
            for entry in std::fs::read_dir(dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    dirs.push(path);
                } else if path.extension().is_some_and(|ext| ext == "json") {
                    let mut value: Value =
                        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
                    value["text"] = if invalid_type {
                        json!(123400000)
                    } else {
                        json!(KEY)
                    };
                    value["raw_provider_response"] = json!({"ignored": KEY});
                    std::fs::write(path, serde_json::to_vec(&value).unwrap()).unwrap();
                    changed += 1;
                }
            }
        }
        assert_eq!(changed, 1);
        let error = call(&provider).await.unwrap_err();
        if invalid_type {
            assert!(matches!(error, ModelError::Cache(_)));
        } else {
            assert!(matches!(error, ModelError::Provider(_)));
        }
        assert!(!error.to_string().contains(KEY));
        for text in [
            serde_json::to_string(&receipts.receipts()).unwrap(),
            serde_json::to_string(&traces.records()).unwrap(),
        ] {
            assert!(!text.contains(KEY));
        }
        assert!(
            !receipts
                .receipts()
                .iter()
                .any(|receipt| receipt.status == symbiotic_ai_runtime::ReceiptStatus::CacheHit)
        );
    }
}

#[tokio::test]
async fn non_success_invalid_utf8_preserves_status_class() {
    for classifier in [false, true] {
        for status in [401, 402, 429] {
            let (url, server) = fixture_bytes(status, vec![0xff], None);
            let state = tempfile::tempdir().unwrap();
            let (provider, _, _) =
                configured(&url, &state.path().join("state"), classifier, KEY).await;
            let error = call(&provider).await.unwrap_err();
            server.join().unwrap();
            match status {
                401 => assert!(matches!(error, ModelError::Auth(_)), "{error:?}"),
                402 => assert!(matches!(error, ModelError::BudgetExhausted(_)), "{error:?}"),
                429 => assert!(matches!(error, ModelError::RateLimited(_)), "{error:?}"),
                _ => unreachable!(),
            }
            assert!(!format!("{error:?} {error}").contains(KEY));
        }
    }
}

#[tokio::test]
async fn configured_retrieval_uses_shared_receipts_and_redacts_before_persistence() {
    use model::{EmbeddingRequest, ModelAdapter, RerankRequest};
    for adapter in [
        ModelAdapter::OpenAiEmbedding,
        ModelAdapter::OllamaEmbedding,
        ModelAdapter::CohereRerank,
    ] {
        for (echo, incomplete) in [(false, false), (true, false), (false, true)] {
            if incomplete && adapter == ModelAdapter::OllamaEmbedding {
                continue;
            }
            let body = match adapter {
                ModelAdapter::CohereRerank => {
                    json!({"results":[{"index":0,"relevance_score":0.7}]})
                }
                ModelAdapter::OllamaEmbedding => json!({"embedding":[1,2]}),
                _ => json!({"data":[{"index":0,"embedding":[1,2]}],"usage":{"prompt_tokens":7}}),
            };
            let mut body = body;
            if echo {
                body["ignored"] = json!(KEY);
            }
            let (endpoint, server) = fixture(200, body.to_string(), None);
            let mut config: Value =
                serde_json::from_slice(include_bytes!("../../../examples/model-registry.json"))
                    .unwrap();
            let rerank = adapter == ModelAdapter::CohereRerank;
            config["models"][0]["adapter"] = json!(adapter);
            config["models"][0]["operations"] = json!([adapter.capability()]);
            config["models"][0]["identity"]["operation"] =
                json!(if rerank { "rerank" } else { "embedding" });
            config["accounts"] = json!([{"id":"policy","policy": ModelQueueConfig {
                retry_attempts:1, logical_retry_attempts:1, request_timeout_seconds:Some(2), ..Default::default()
            }}]);
            config["bindings"] = json!([{
                "identity":{"tenant":"tenant","provider":"provider","revision":"1","account":"account"},
                "model":"example-chat","endpoint":endpoint,"secret_ref":"synthetic","account_policy":"policy","account_sharing_key":null,
                "limits":{"max_request_bytes":4096,"max_response_bytes":4096,"max_output_tokens":null},
                "settings": if rerank {json!({"rerank_candidates":2,"rerank_input_bytes":64,"rerank_context_tokens":4093,"rerank_query_tokens":2048})} else {json!({"dimensions":2,"embedding_full_dimensions":1024,"embedding_input_tokens":8192})}
            }]);
            let state = tempfile::tempdir().unwrap();
            let receipts = Arc::new(InMemoryReceiptSink::default());
            let traces = Arc::new(InMemoryTraceSink::default());
            let runtime = Runtime::open(RuntimeConfig {
                state_dir: Some(state.path().join("state")),
                registry: Some(Arc::new(
                    model::ModelRegistry::from_json(&serde_json::to_vec(&config).unwrap()).unwrap(),
                )),
                receipt_sink: Some(receipts.clone()),
                trace_sink: Some(traces.clone()),
                ..Default::default()
            })
            .unwrap();
            let provider = runtime
                .configured_provider(
                    &TenantId("tenant".into()),
                    &ProviderPrincipalId("provider".into()),
                    &Credentials(KEY),
                )
                .await
                .unwrap();
            let result = match provider {
                ConfiguredProvider::Embedding(provider) => provider
                    .embed(EmbeddingRequest {
                        inputs: vec!["input".into(); if incomplete { 2 } else { 1 }],
                        dimensions: None,
                        task: None,
                        role_binding: None,
                        source: None,
                        metadata: Value::Null,
                    })
                    .await
                    .map(|response| {
                        assert!(response.raw_provider_response.is_none());
                        response.trace
                    }),
                ConfiguredProvider::Rerank(provider) => provider
                    .rerank(RerankRequest {
                        query: "query".into(),
                        documents: vec!["candidate".into(); if incomplete { 2 } else { 1 }],
                        top_k: None,
                        role_binding: None,
                        source: None,
                        metadata: Value::Null,
                    })
                    .await
                    .map(|response| {
                        assert!(response.raw_provider_response.is_none());
                        response.trace
                    }),
                _ => panic!("wrong adapter"),
            };
            server.join().unwrap();
            if echo || incomplete {
                assert!(matches!(result, Err(ModelError::Provider(_))));
                assert!(
                    !receipts
                        .receipts()
                        .iter()
                        .any(|r| r.status == symbiotic_ai_runtime::ReceiptStatus::Succeeded)
                );
            } else {
                let trace = result.unwrap();
                assert!(trace.queue_item_id.is_some());
                if adapter == ModelAdapter::OpenAiEmbedding {
                    assert_eq!(trace.usage.input_tokens, Some(7));
                }
                assert!(
                    receipts
                        .receipts()
                        .iter()
                        .any(|r| r.status == symbiotic_ai_runtime::ReceiptStatus::Succeeded)
                );
            }
            assert!(
                !serde_json::to_string(&receipts.receipts())
                    .unwrap()
                    .contains(KEY)
            );
            assert!(
                !serde_json::to_string(&traces.records())
                    .unwrap()
                    .contains(KEY)
            );
            assert_no_secret_files(state.path());
        }
    }
}

#[tokio::test]
async fn configured_anthropic_uses_credentials_queue_receipt_and_ledger() {
    let (endpoint, server) = fixture(
        200,
        json!({
            "content":[{"type":"text","text":"OK"}], "stop_reason":"end_turn",
            "usage":{"input_tokens":7,"output_tokens":3}
        })
        .to_string(),
        None,
    );
    let state = tempfile::tempdir().unwrap();
    let mut config: Value =
        serde_json::from_slice(include_bytes!("../../../examples/model-registry.json")).unwrap();
    config["models"][0]["adapter"] = json!("anthropic_chat");
    config["accounts"] = json!([{"id":"policy","policy":ModelQueueConfig {
        logical_retry_attempts:1, retry_attempts:1, request_timeout_seconds:Some(2), ..ModelQueueConfig::default()
    }}]);
    config["bindings"] = json!([{
        "identity":{"tenant":"tenant","provider":"provider","revision":"1","account":"account"},
        "model":"example-chat-alias","endpoint":endpoint,"secret_ref":"synthetic",
        "account_policy":"policy","account_sharing_key":null,
        "limits":{"max_request_bytes":65536,"max_response_bytes":65536,"max_output_tokens":128},
        "settings":{"thinking":"enabled"}
    }]);
    let receipts = Arc::new(InMemoryReceiptSink::default());
    let runtime = Runtime::open(RuntimeConfig {
        state_dir: Some(state.path().join("state")),
        registry: Some(Arc::new(
            model::ModelRegistry::from_json(&serde_json::to_vec(&config).unwrap()).unwrap(),
        )),
        receipt_sink: Some(receipts.clone()),
        ..RuntimeConfig::default()
    })
    .unwrap();
    let wrong =
        model::OpenAiCompatibleChatProvider::new("example", "example-model", &endpoint, KEY)
            .with_request_limit(65536)
            .with_response_limit(65536)
            .with_output_limit(128)
            .with_thinking(Some(model::ThinkingMode::Enabled));
    let wrong_binding = runtime
        .registry_binding(
            &TenantId("tenant".into()),
            &ProviderPrincipalId("provider".into()),
            wrong,
        )
        .unwrap();
    assert!(matches!(
        runtime.chat(wrong_binding),
        Err(ModelError::InvalidRequest(_))
    ));
    let configured = runtime
        .configured_provider(
            &TenantId("tenant".into()),
            &ProviderPrincipalId("provider".into()),
            &Credentials(KEY),
        )
        .await
        .unwrap();
    let ConfiguredProvider::Chat(provider) = configured else {
        panic!("chat expected")
    };
    let descriptor = provider.descriptor();
    assert_eq!(descriptor.metadata["wire"], "anthropic-messages");
    let response = provider
        .chat(ChatRequest {
            messages: vec![model::ChatMessage {
                role: "user".into(),
                content: "synthetic prompt".into(),
            }],
            max_output_tokens: Some(32),
            temperature: None,
            response_format: None,
            role_binding: None,
            source: None,
            metadata: Value::Null,
        })
        .await
        .unwrap();
    server.join().unwrap();
    assert_eq!(response.text, "OK");
    assert_eq!(response.trace.usage.input_tokens, Some(7));
    assert!(response.trace.queue_item_id.is_some());
    let recorded = receipts.receipts();
    assert_eq!(
        recorded.iter().map(|r| r.status).collect::<Vec<_>>(),
        vec![
            symbiotic_ai_runtime::ReceiptStatus::Queued,
            symbiotic_ai_runtime::ReceiptStatus::Running,
            symbiotic_ai_runtime::ReceiptStatus::Succeeded,
        ]
    );
    assert_eq!(recorded[2].usage.as_ref().unwrap().input_tokens, Some(7));
    let spend = runtime
        .spend_receipt(recorded[2].spend_receipt.as_ref().unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(spend.usage.unwrap().input_tokens, Some(7));
    assert_no_secret_files(state.path());
}
