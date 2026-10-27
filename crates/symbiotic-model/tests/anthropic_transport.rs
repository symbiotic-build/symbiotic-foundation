use serde_json::{Value, json};
use std::io::{Read, Write};
use std::net::TcpListener;
use symbiotic_core::DiagnosticCode;
use symbiotic_model::{
    AnthropicChatProvider, ChatMessage, ChatProvider, ChatRequest, ModelError, ModelProvider,
    ThinkingMode, wire,
};
use symbiotic_trace::CacheStatus;
const KEY: &str = "synthetic-anthropic-test-secret";
fn fixture(
    status: u16,
    body: &str,
    chunked: bool,
) -> (String, std::thread::JoinHandle<(String, serde_json::Value)>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let payload = body.to_owned();
    let handle = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream.set_nodelay(true).unwrap();
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(10)))
            .unwrap();
        let mut bytes = Vec::new();
        let (offset, length) = loop {
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
                break (offset + 4, length);
            }
        };
        while bytes.len() < offset + length {
            let mut chunk = [0; 4096];
            let n = stream.read(&mut chunk).unwrap();
            assert!(n > 0);
            bytes.extend_from_slice(&chunk[..n]);
        }
        let headers = String::from_utf8(bytes[..offset].to_vec()).unwrap();
        if chunked {
            let _ = write!(
                stream,
                "HTTP/1.1 {status} test\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n{:X}\r\n{payload}\r\n0\r\n\r\n",
                payload.len()
            );
        } else {
            let _ = write!(
                stream,
                "HTTP/1.1 {status} test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{payload}",
                payload.len()
            );
        }
        (
            headers,
            serde_json::from_slice(&bytes[offset..offset + length]).unwrap(),
        )
    });
    (url, handle)
}

fn request() -> ChatRequest {
    ChatRequest {
        messages: vec![
            ChatMessage {
                role: "system".into(),
                content: "system\n\"é".into(),
            },
            ChatMessage {
                role: "user".into(),
                content: "evidence".into(),
            },
        ],
        max_output_tokens: None,
        temperature: None,
        response_format: None,
        role_binding: Some("local-role".into()),
        source: Some("local-source".into()),
        metadata: json!({"private":"metadata"}),
    }
}
fn provider(url: &str) -> AnthropicChatProvider {
    AnthropicChatProvider::new("fixture", "fixture-model", url, KEY)
        .with_request_limit(65536)
        .with_response_limit(65536)
}
fn answer() -> Value {
    json!({"id":"fixture-id", "model":"served-model", "stop_reason":"end_turn",
        "content":[{"type":"thinking","thinking":"private reasoning","signature":"sig"},
        {"type":"text","text":"first"},{"type":"redacted_thinking","data":"private redacted"},{"type":"text","text":"second"}],
        "usage":{"input_tokens":7,"output_tokens":348,"output_tokens_details":{"thinking_tokens":312},"cache_read_input_tokens":5,"cache_creation_input_tokens":2,"cost_usd":"0.000000125"}})
}
#[tokio::test]
async fn messages_headers_default_tokens_without_thinking_and_usage() {
    let (url, server) = fixture(200, &answer().to_string(), false);
    let response = provider(&format!("{url}/v1/"))
        .chat(request())
        .await
        .unwrap();
    let (headers, body) = server.join().unwrap();
    assert!(headers.starts_with("POST /v1/messages HTTP/1.1"));
    assert!(headers.contains(&format!("x-api-key: {KEY}")));
    assert!(headers.contains("anthropic-version: 2023-06-01"));
    assert!(headers.contains("content-type: application/json"));
    assert!(!headers.contains("authorization:"));
    assert_eq!(
        body,
        json!({"model":"fixture-model","max_tokens":16000,"system":"system\n\"é",
        "messages":[{"role":"user","content":"evidence"}],"stream":false})
    );
    assert_eq!(response.text, "firstsecond");
    assert_eq!(response.finish_reason.as_deref(), Some("end_turn"));
    assert_eq!(response.trace.usage.input_tokens, Some(14));
    assert_eq!(response.trace.usage.output_tokens, Some(348));
    assert_eq!(response.trace.usage.reasoning_tokens, Some(312));
    assert_eq!(
        response.trace.usage.reported_cost_usd.as_deref(),
        Some("0.000000125")
    );
    assert_eq!(response.trace.cache.cached_input_tokens, Some(5));
    assert_eq!(response.trace.cache.prompt_cache, CacheStatus::PartialHit);
    assert_eq!(response.trace.metadata["cache_miss_tokens"], 9);
    assert_eq!(
        response.trace.metadata["provider"]["served_model"],
        "served-model"
    );
    assert_eq!(response.trace.role_binding.as_deref(), Some("local-role"));
    assert!(
        !serde_json::to_string(&response)
            .unwrap()
            .contains("private reasoning")
    );
    assert!(response.raw_provider_response.is_none());
}
#[tokio::test]
async fn disabled_thinking_and_explicit_output_limit_preserve_conversation_order() {
    for thinking in [None, Some(ThinkingMode::Disabled)] {
        let (url, server) = fixture(200, &answer().to_string(), false);
        let mut req = request();
        req.messages.extend([
            ChatMessage {
                role: "assistant".into(),
                content: "previous".into(),
            },
            ChatMessage {
                role: "user".into(),
                content: "next".into(),
            },
        ]);
        req.max_output_tokens = Some(128);
        req.temperature = Some(0.0);
        provider(&url)
            .with_output_limit(256)
            .with_thinking(thinking)
            .chat(req.clone())
            .await
            .unwrap();
        let (_, body) = server.join().unwrap();
        match thinking {
            None => assert!(body.get("thinking").is_none()),
            Some(ThinkingMode::Disabled) => {
                assert_eq!(body["thinking"], json!({"type":"disabled"}))
            }
            Some(ThinkingMode::Enabled) => unreachable!(),
        }
        assert_eq!(body["max_tokens"], 128);
        assert_eq!(body["temperature"], 0.0);
        assert_eq!(
            body["messages"],
            serde_json::to_value(&req.messages[1..]).unwrap()
        );
    }
}
#[tokio::test]
async fn request_limits_and_unsupported_options_refuse_before_connecting() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    for case in 0..8 {
        let mut req = request();
        let mut p = provider(&url);
        match case {
            0 => p = p.with_request_limit(1),
            1 => req.max_output_tokens = Some(16001),
            2 => req.max_output_tokens = Some(0),
            3 => p = p.with_output_limit(0),
            4 => req.response_format = Some("json_object".into()),
            5 => req.messages[1].role = "tool".into(),
            6 => req.messages.push(ChatMessage {
                role: "system".into(),
                content: "late".into(),
            }),
            _ => req.messages.truncate(1),
        }
        assert!(
            matches!(p.chat(req).await, Err(ModelError::InvalidRequest(_))),
            "case {case}"
        );
    }
    assert_eq!(
        listener.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
}
#[tokio::test]
async fn zero_byte_limits_and_endpoint_credentials_are_refused() {
    for p in [
        provider("http://127.0.0.1:9").with_request_limit(0),
        provider("http://127.0.0.1:9").with_response_limit(0),
        provider("http://user:password@127.0.0.1:9"),
    ] {
        assert!(p.validate_configuration().is_err());
        assert!(p.chat(request()).await.is_err());
    }
}
#[tokio::test]
async fn exact_encoded_request_boundary_includes_system_escaping_and_thinking() {
    let mut req = request();
    req.max_output_tokens = Some(16000);
    let bytes = wire::anthropic_chat_body("fixture-model", &req, Some(ThinkingMode::Enabled), None)
        .unwrap();
    let (url, server) = fixture(200, &answer().to_string(), false);
    provider(&url)
        .with_request_limit(bytes.len())
        .with_thinking(Some(ThinkingMode::Enabled))
        .chat(req.clone())
        .await
        .unwrap();
    assert_eq!(
        server.join().unwrap().1,
        serde_json::from_slice::<Value>(&bytes).unwrap()
    );
    assert!(
        wire::anthropic_chat_body(
            "fixture-model",
            &req,
            Some(ThinkingMode::Enabled),
            Some(bytes.len() - 1)
        )
        .is_err()
    );
}
#[tokio::test]
async fn success_response_limit_covers_content_length_and_chunked_bodies() {
    for chunked in [false, true] {
        let (url, server) = fixture(200, &"x".repeat(4096), chunked);
        assert!(matches!(
            provider(&url)
                .with_response_limit(1024)
                .chat(request())
                .await,
            Err(ModelError::Provider(
                DiagnosticCode::ProviderResponseLimitExceeded
            ))
        ));
        server.join().unwrap();
    }
}
#[tokio::test]
async fn status_mapping_discards_error_bodies_and_refuses_redirects() {
    for (status, expected) in [
        (401, DiagnosticCode::AuthenticationRejected),
        (403, DiagnosticCode::AuthenticationRejected),
        (402, DiagnosticCode::HttpBudgetExhausted),
        (408, DiagnosticCode::HttpTimeout),
        (504, DiagnosticCode::HttpTimeout),
        (429, DiagnosticCode::HttpRateLimited),
        (500, DiagnosticCode::HttpUnavailable),
        (529, DiagnosticCode::HttpUnavailable),
        (400, DiagnosticCode::HttpFailure),
        (302, DiagnosticCode::ProviderRedirectRefused),
    ] {
        let (url, server) = fixture(status, &format!("invalid JSON echo {KEY}"), false);
        let error = provider(&url).chat(request()).await.unwrap_err();
        server.join().unwrap();
        assert_eq!(error.code(), expected);
        assert!(!format!("{error:?} {error}").contains(KEY));
    }
}
#[tokio::test]
async fn malformed_refusal_and_unsupported_blocks_fail_visibly() {
    let mut cases = vec![
        (json!({}), true),
        (json!({"content":[],"stop_reason":"refusal"}), false),
        (
            json!({"content":[{"type":"text","text":"partial"}],"stop_reason":"tool_use"}),
            false,
        ),
        (
            json!({"content":[{"type":"text","text":"partial"}],"stop_reason":"pause_turn"}),
            false,
        ),
        (
            json!({"content":[{"type":"text"}],"stop_reason":"end_turn"}),
            true,
        ),
        (
            json!({"content":[{"type":"tool_use","id":"x"}],"stop_reason":"tool_use"}),
            true,
        ),
    ];
    let mut overflow = answer();
    overflow["usage"]["input_tokens"] = json!(u64::MAX);
    cases.push((overflow, false));
    for (body, wrong_shape) in cases {
        let (url, server) = fixture(200, &body.to_string(), false);
        let error = provider(&url)
            .with_timeout(1)
            .unwrap()
            .chat(request())
            .await
            .unwrap_err();
        server.join().unwrap();
        if wrong_shape {
            assert!(matches!(
                error,
                ModelError::Unavailable(DiagnosticCode::InvalidResponse)
            ));
        } else {
            assert!(matches!(error, ModelError::Provider(_)));
        }
    }
    let (url, server) = fixture(200, "invalid JSON", false);
    assert!(matches!(
        provider(&url).chat(request()).await,
        Err(ModelError::Unavailable(DiagnosticCode::InvalidResponse))
    ));
    server.join().unwrap();
}
#[tokio::test]
async fn credential_echo_in_text_reasoning_or_metadata_never_returns() {
    for path in ["/content/1/text", "/content/0/thinking", "/id"] {
        let mut body = answer();
        *body.pointer_mut(path).unwrap() = json!(KEY);
        let (url, server) = fixture(200, &body.to_string(), false);
        let error = provider(&url).chat(request()).await.unwrap_err();
        server.join().unwrap();
        assert!(!format!("{error:?} {error}").contains(KEY));
    }
}
#[tokio::test]
async fn max_tokens_finish_reason_and_missing_usage_remain_visible() {
    let (url, server) = fixture(
        200,
        &json!({"content":[],"stop_reason":"max_tokens"}).to_string(),
        false,
    );
    let response = provider(&url).chat(request()).await.unwrap();
    server.join().unwrap();
    assert_eq!(response.text, "");
    assert_eq!(response.finish_reason.as_deref(), Some("max_tokens"));
    assert_eq!(response.trace.usage.input_tokens, None);
}

#[tokio::test]
async fn context_window_exhaustion_preserves_answer_finish_reason_and_measured_usage() {
    let mut body = answer();
    body["stop_reason"] = json!("model_context_window_exceeded");
    let (url, server) = fixture(200, &body.to_string(), false);
    let response = provider(&url).chat(request()).await;
    server.join().unwrap();
    let response = response.unwrap();
    assert_eq!(response.text, "firstsecond");
    assert_eq!(
        response.finish_reason.as_deref(),
        Some("model_context_window_exceeded")
    );
    assert_eq!(response.trace.usage.input_tokens, Some(14));
    assert_eq!(response.trace.usage.output_tokens, Some(348));
    assert_eq!(response.trace.usage.reasoning_tokens, Some(312));
}

#[tokio::test]
async fn explicit_enabled_thinking_preserves_default_temperature() {
    for temperature in [None, Some(1.0)] {
        let (url, server) = fixture(200, &answer().to_string(), false);
        let mut req = request();
        req.temperature = temperature;
        provider(&url)
            .with_thinking(Some(ThinkingMode::Enabled))
            .chat(req)
            .await
            .unwrap();
        let (_, body) = server.join().unwrap();
        assert_eq!(body["thinking"], json!({"type":"adaptive"}));
        assert_eq!(
            body.get("temperature"),
            temperature.map(|v| json!(v)).as_ref()
        );
    }
}

#[tokio::test]
async fn enabled_thinking_refuses_incompatible_temperature_before_connecting() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    for model in ["claude-opus-4-6", "claude-sonnet-4-6"] {
        let mut req = request();
        req.temperature = Some(0.0);
        let original = serde_json::to_value(&req).unwrap();
        assert!(matches!(
            wire::anthropic_chat_body(model, &req, Some(ThinkingMode::Enabled), None),
            Err(ModelError::InvalidRequest(_))
        ));
        assert_eq!(serde_json::to_value(&req).unwrap(), original);
        let p = AnthropicChatProvider::new("fixture", model, &url, KEY)
            .with_request_limit(65536)
            .with_response_limit(65536)
            .with_thinking(Some(ThinkingMode::Enabled));
        assert!(matches!(
            p.chat(req).await,
            Err(ModelError::InvalidRequest(_))
        ));
    }
    assert_eq!(
        listener.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
}

#[tokio::test]
async fn invalid_temperatures_are_refused_before_connecting() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    for thinking in [
        None,
        Some(ThinkingMode::Disabled),
        Some(ThinkingMode::Enabled),
    ] {
        for temperature in [-0.1, 1.5, f32::NAN, f32::NEG_INFINITY, f32::INFINITY] {
            let mut req = request();
            req.max_output_tokens = Some(128);
            req.temperature = Some(temperature);
            assert!(matches!(
                wire::anthropic_chat_body("fixture-model", &req, thinking, None),
                Err(ModelError::InvalidRequest(_))
            ));
            assert!(matches!(
                provider(&url).with_thinking(thinking).chat(req).await,
                Err(ModelError::InvalidRequest(_))
            ));
        }
    }
    assert_eq!(
        listener.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
    for thinking in [None, Some(ThinkingMode::Disabled)] {
        for temperature in [0.0, 0.5, 1.0] {
            let mut req = request();
            req.temperature = Some(temperature);
            let bytes = wire::anthropic_chat_body("fixture-model", &req, thinking, None).unwrap();
            let body: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(body["temperature"], json!(temperature));
        }
    }
}

#[tokio::test]
async fn assistant_prefill_is_refused_before_connecting() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    for thinking in [
        None,
        Some(ThinkingMode::Enabled),
        Some(ThinkingMode::Disabled),
    ] {
        let mut req = request();
        req.messages.push(ChatMessage {
            role: "assistant".into(),
            content: "Answer:".into(),
        });
        let original = serde_json::to_value(&req).unwrap();
        assert!(matches!(
            wire::anthropic_chat_body("claude-opus-4-6", &req, thinking, None),
            Err(ModelError::InvalidRequest(_))
        ));
        assert_eq!(serde_json::to_value(&req).unwrap(), original);
        assert!(matches!(
            provider(&url).with_thinking(thinking).chat(req).await,
            Err(ModelError::InvalidRequest(_))
        ));
    }
    assert_eq!(
        listener.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
}

#[tokio::test]
async fn regression_reasoning_echoes_are_not_usage_identities() {
    tokio::time::timeout(std::time::Duration::from_secs(60), async {
        for reasoning_field in ["thinking", "signature", "data"] {
            for identity_field in ["id", "model"] {
                for (scoped, echoed_identity) in [
                    (false, None),
                    (true, None),
                    (false, Some("PRIVATE_REASONING")),
                    (true, Some("PRIVATE_REASONING")),
                    (false, Some("chatcmpl-PRIVATE_REASONING")),
                    (true, Some("chatcmpl-PRIVATE_REASONING")),
                    (false, Some("PRIVATE_REASONING-suffix")),
                    (true, Some("PRIVATE_REASONING-suffix")),
                ] {
                    let echo = echoed_identity.is_some();
                    let mut body = answer();
                    body["content"][1]["text"] = json!("OK");
                    body["content"][3]["text"] = json!("");
                    let block = if reasoning_field == "data" { 2 } else { 0 };
                    body["content"][block][reasoning_field] = json!(format!(
                        "prefix {} suffix",
                        echoed_identity.unwrap_or("PRIVATE_REASONING")
                    ));
                    if let Some(identity) = echoed_identity {
                        body[identity_field] = json!(identity);
                    }
                    let (url, server) = fixture(200, &body.to_string(), false);
                    let provider = AnthropicChatProvider::new("fixture", "fixture-model", &url, "")
                        .with_timeout(1)
                        .unwrap();
                    let call = provider.chat(request());
                    let response = if scoped {
                        symbiotic_model::with_egress_http_observations(call).await
                    } else {
                        call.await
                    }
                    .expect("identity screening must preserve the paid answer");
                    server.join().unwrap();
                    assert_eq!(response.text, "OK");
                    assert!(response.raw_provider_response.is_some());
                    assert!(
                        !serde_json::to_string(&response.trace)
                            .unwrap()
                            .contains("PRIVATE_REASONING")
                    );
                    let identity = if identity_field == "id" {
                        &response.trace.usage.response_id
                    } else {
                        &response.trace.usage.served_model
                    };
                    assert_eq!(
                        identity.is_none(),
                        echo,
                        "{reasoning_field}/{identity_field}"
                    );
                    if !echo {
                        assert_eq!(
                            identity.as_deref(),
                            Some(if identity_field == "id" {
                                "fixture-id"
                            } else {
                                "served-model"
                            })
                        );
                    }
                    assert!(
                        !response
                            .trace
                            .metadata
                            .to_string()
                            .contains("PRIVATE_REASONING")
                    );
                    assert_eq!(
                        response.trace.metadata.get("runtime_diagnostics").is_some(),
                        echo
                    );
                }
            }
        }
    })
    .await
    .expect("reasoning identity fixtures must finish within the 60-second hang guard");
}

#[tokio::test]
async fn regression_anthropic_misses_remain_known_without_cache_reads() {
    let mut body = answer();
    body["usage"] = json!({"input_tokens":20,"cache_creation_input_tokens":10});
    let (url, server) = fixture(200, &body.to_string(), false);
    let response = symbiotic_model::with_egress_http_observations(
        provider(&url).with_timeout(1).unwrap().chat(request()),
    )
    .await
    .unwrap();
    server.join().unwrap();
    assert_eq!(response.trace.usage.input_tokens, Some(30));
    assert_eq!(response.trace.usage.cache_hit_tokens, None);
    assert_eq!(response.trace.usage.cache_miss_tokens, Some(30));
}

#[tokio::test]
async fn regression_request_echoes_are_not_usage_identities() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        for role in ["system", "user", "assistant"] {
            for scoped in [false, true] {
                for (identity, content, echo) in [
                    ("PRIVATE_PROMPT", "PRIVATE_PROMPT", true),
                    ("PROMPT", "prefix PRIVATE_PROMPT suffix", true),
                    ("fixture-id", "PRIVATE_PROMPT", false),
                    (role, "PRIVATE_PROMPT", false),
                    ("chatcmpl-4abc", "4", false),
                ] {
                    let mut req = request();
                    req.messages = vec![ChatMessage {
                        role: role.into(),
                        content: content.into(),
                    }];
                    // Anthropic requires a final user turn after system/assistant text.
                    if role != "user" {
                        req.messages.push(ChatMessage {
                            role: "user".into(),
                            content: "next".into(),
                        });
                    }
                    // Local metadata is not sent and must not screen valid identities.
                    req.metadata = serde_json::json!({"local": identity});
                    let body = serde_json::json!({"id":identity,"model":identity,"stop_reason":"end_turn","content":[{"type":"text","text":"OK"},{"type":"thinking","thinking":"unrelated reasoning","signature":"sig"}],"usage":{"input_tokens":7,"output_tokens":2}});
                    let (url, server) = fixture(200, &body.to_string(), false);
                    let provider = AnthropicChatProvider::new("fixture", "requested-model", url, "")
                        .with_timeout(1)
                        .unwrap();
                    let call = provider.chat(req);
                    let response = if scoped {
                        symbiotic_model::with_egress_http_observations(call).await
                    } else {
                        call.await
                    }
                    .expect("request screening must preserve the paid answer");
                    let (_, wire) = server.join().unwrap();
                    assert!(wire.to_string().contains(content));
                    assert_eq!(response.text, "OK");
                    assert_eq!(response.trace.usage.input_tokens, Some(7));
                    assert_eq!(response.trace.usage.output_tokens, Some(2));
                    assert_eq!(
                        response.trace.usage.response_id.as_deref(),
                        (!echo).then_some(identity),
                        "{role}/{identity}"
                    );
                    assert_eq!(
                        response.trace.usage.served_model.as_deref(),
                        (!echo).then_some(identity),
                        "{role}/{identity}"
                    );
                    for field in ["response_id", "served_model"] {
                        assert_eq!(
                            response.trace.metadata["provider"]
                                .get(field)
                                .and_then(serde_json::Value::as_str),
                            (!echo).then_some(identity)
                        );
                    }
                    assert_eq!(
                        response.trace.metadata.get("runtime_diagnostics").is_some(),
                        echo
                    );
                    assert!(
                        !serde_json::to_string(&response.trace)
                            .unwrap()
                            .contains("PRIVATE_PROMPT")
                    );
                }
            }
        }
    })
    .await
    .expect("request identity fixtures must finish within five seconds");
}
