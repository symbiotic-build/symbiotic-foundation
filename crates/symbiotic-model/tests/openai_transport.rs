use std::io::{Read, Write};
use std::net::TcpListener;
use symbiotic_model::{
    ChatMessage, ChatProvider, ChatRequest, OpenAiCompatibleChatProvider, ThinkingMode,
    prompt_cache_counts,
};
use symbiotic_trace::CacheStatus;

fn fixture(body: serde_json::Value) -> (String, std::thread::JoinHandle<serde_json::Value>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
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
        let payload = body.to_string();
        write!(stream, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", payload.len(), payload).unwrap();
        serde_json::from_slice(&bytes[offset..offset + length]).unwrap()
    });
    (url, handle)
}

fn request() -> ChatRequest {
    ChatRequest {
        messages: vec![ChatMessage {
            role: "user".into(),
            content: "synthetic evidence".into(),
        }],
        max_output_tokens: Some(128),
        temperature: Some(0.0),
        response_format: None,
        role_binding: None,
        source: None,
        metadata: serde_json::json!({}),
    }
}

#[tokio::test]
async fn missing_cache_counters_remain_unknown() {
    let (url, server) = fixture(serde_json::json!({
        "choices":[{"message":{"content":"OK"},"finish_reason":"stop"}],
        "usage":{"prompt_tokens":10,"completion_tokens":2}
    }));
    let response = OpenAiCompatibleChatProvider::new("fixture", "fixture", url, "synthetic-key")
        .with_request_limit(65536)
        .with_response_limit(65536)
        .chat(request())
        .await
        .unwrap();
    server.join().unwrap();
    assert_eq!(
        response.trace.cache.prompt_cache,
        CacheStatus::NotApplicable
    );
    assert_eq!(response.trace.cache.cached_input_tokens, None);
}

#[tokio::test]
async fn low_thinking_and_metadata_without_reasoning_text() {
    let (url, server) = fixture(serde_json::json!({
        "id":"fixture-id", "model":"served-model", "created":123,
        "choices":[{"message":{"content":"OK","reasoning_content":"private hidden reasoning"},"finish_reason":"stop"}],
        "usage":{"prompt_tokens":10,"completion_tokens":5,
            "prompt_tokens_details":{"cached_tokens":4},
            "completion_tokens_details":{"reasoning_tokens":3}, "cost":"0.00001234"}
    }));
    let response =
        OpenAiCompatibleChatProvider::new("fixture", "requested-model", url, "synthetic-key")
            .with_request_limit(65536)
            .with_response_limit(65536)
            .with_thinking(Some(ThinkingMode::Enabled))
            .with_reasoning_effort("low")
            .chat(request())
            .await
            .unwrap();
    let wire = server.join().unwrap();
    assert_eq!(wire["thinking"]["type"], "enabled");
    assert_eq!(wire["reasoning_effort"], "low");
    assert_eq!(wire["max_tokens"], 128);
    assert_eq!(response.trace.model.model.0, "requested-model");
    assert_eq!(
        response.trace.metadata["provider"]["served_model"],
        "served-model"
    );
    assert_eq!(
        response.trace.metadata["provider"]["response_id"],
        "fixture-id"
    );
    assert_eq!(response.trace.metadata["provider"]["created"], 123);
    assert_eq!(
        response.trace.metadata["provider"]["reported_cost_usd"],
        "0.00001234"
    );
    assert_eq!(response.trace.usage.reasoning_tokens, Some(3));
    assert_eq!(response.trace.usage.cost_micro_usd, None);
    assert_eq!(
        response.trace.usage.reported_cost_usd.as_deref(),
        Some("0.00001234")
    );
    assert_eq!(response.trace.cache.prompt_cache, CacheStatus::PartialHit);
    assert_eq!(response.trace.metadata["cache_miss_tokens"], 6);
    assert!(
        !serde_json::to_string(&response.trace)
            .unwrap()
            .contains("private hidden reasoning")
    );
}

#[tokio::test]
async fn disabled_thinking_and_nullable_content_keep_identity() {
    let (url, server) = fixture(serde_json::json!({
        "id":"no-usage", "model":"served-model",
        "choices":[{"message":{"content":null},"finish_reason":"length"}]
    }));
    let response = OpenAiCompatibleChatProvider::new("fixture", "fixture", url, "synthetic-key")
        .with_request_limit(65536)
        .with_response_limit(65536)
        .with_thinking(Some(ThinkingMode::Disabled))
        .chat(request())
        .await
        .unwrap();
    let wire = server.join().unwrap();
    assert_eq!(wire["thinking"]["type"], "disabled");
    assert!(wire.get("reasoning_effort").is_none());
    assert_eq!(response.text, "");
    assert_eq!(response.finish_reason.as_deref(), Some("length"));
    assert_eq!(
        response.trace.metadata["provider"]["response_id"],
        "no-usage"
    );
    assert_eq!(response.trace.usage.input_tokens, None);
    assert_eq!(response.trace.usage.reported_cost_usd, None);
}

#[test]
fn cache_counts_return_unknown_for_conflicts_and_derive_only_numeric_evidence() {
    for (total, hit, miss, nested) in [
        (Some(10), Some(4), Some(6), Some(5)),
        (Some(10), Some(4), Some(7), None),
        (Some(10), None, Some(11), None),
        (Some(10), Some(11), None, None),
        (Some(u64::MAX), Some(u64::MAX), Some(1), None),
    ] {
        assert_eq!(prompt_cache_counts(total, hit, miss, nested), (None, None));
    }
    for (hit, miss, expected) in [
        (None, Some(6), (Some(4), Some(6))),
        (None, None, (None, None)),
        (Some(0), None, (Some(0), Some(10))),
    ] {
        assert_eq!(prompt_cache_counts(Some(10), hit, miss, None), expected);
    }
}

#[tokio::test]
async fn provider_cost_usd_is_preserved_in_typed_usage() {
    for cost in ["0.01", "0.0000001234567890123456789"] {
        let (url, server) = fixture(serde_json::json!({
            "choices":[{"message":{"content":"OK"}}],
            "usage":{"cost_usd":cost}
        }));
        let response =
            OpenAiCompatibleChatProvider::new("fixture", "fixture", url, "synthetic-key")
                .with_request_limit(65536)
                .with_response_limit(65536)
                .chat(request())
                .await
                .unwrap();
        server.join().unwrap();
        assert_eq!(
            response.trace.usage.reported_cost_usd.as_deref(),
            Some(cost)
        );
        assert_eq!(response.trace.usage.cost_micro_usd, None);
    }
}

#[tokio::test]
async fn invalid_reported_costs_remain_unknown() {
    for cost in [
        serde_json::json!(-0.1),
        serde_json::json!("NaN"),
        serde_json::json!("inf"),
        serde_json::json!("invalid"),
    ] {
        let (url, server) = fixture(serde_json::json!({
            "choices":[{"message":{"content":"OK"}}],
            "usage":{"cost":cost}
        }));
        let response =
            OpenAiCompatibleChatProvider::new("fixture", "fixture", url, "synthetic-key")
                .with_request_limit(65536)
                .with_response_limit(65536)
                .chat(request())
                .await
                .unwrap();
        server.join().unwrap();
        assert!(response.trace.metadata["provider"]["reported_cost_usd"].is_null());
        assert_eq!(response.trace.usage.reported_cost_usd, None);
    }
}

#[tokio::test]
async fn configured_response_limit_refuses_oversized_body() {
    let (url, server) = fixture(serde_json::json!({
        "choices": [{"message": {"content": "x".repeat(4096)}}]
    }));
    let result = OpenAiCompatibleChatProvider::new("fixture", "fixture", url, "synthetic-key")
        .with_request_limit(65536)
        .with_response_limit(65536)
        .with_response_limit(1024)
        .chat(request())
        .await;
    server.join().unwrap();
    assert!(matches!(
        result,
        Err(symbiotic_model::ModelError::Provider(
            symbiotic_core::DiagnosticCode::ProviderResponseLimitExceeded
        ))
    ));
}

#[tokio::test]
async fn configured_request_limit_refuses_wire_body_before_connecting() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let result = OpenAiCompatibleChatProvider::new("fixture", "fixture", url, "synthetic-key")
        .with_request_limit(65536)
        .with_response_limit(65536)
        .with_request_limit(1)
        .chat(request())
        .await;
    assert!(matches!(
        result,
        Err(symbiotic_model::ModelError::InvalidRequest(
            symbiotic_core::DiagnosticCode::ProviderRequestLimitExceeded
        ))
    ));
    assert_eq!(
        listener.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
}

#[tokio::test]
async fn configured_request_limit_accepts_exact_encoded_body_size() {
    let mut request = request();
    request.messages[0].content = "quotes: \" newline: \n unicode: é".into();
    let expected = serde_json::json!({
        "model": "fixture",
        "messages": [{"role": "user", "content": request.messages[0].content}],
        "max_tokens": 128,
        "temperature": 0.0,
        "stream": false
    });
    let (url, server) = fixture(serde_json::json!({"choices":[{"message":{"content":"OK"}}]}));
    OpenAiCompatibleChatProvider::new("fixture", "fixture", url, "synthetic-key")
        .with_request_limit(65536)
        .with_response_limit(65536)
        .with_request_limit(serde_json::to_vec(&expected).unwrap().len())
        .chat(request)
        .await
        .unwrap();
    assert_eq!(server.join().unwrap(), expected);
}

#[tokio::test]
async fn chunked_response_is_capped_without_content_length() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream.set_nodelay(true).unwrap();
        let mut request = [0u8; 4096];
        let _ = stream.read(&mut request).unwrap();
        let body = "x".repeat(4096);
        let wire = format!(
            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n{:X}\r\n{}\r\n0\r\n\r\n",
            body.len(),
            body
        );
        let _ = stream.write_all(wire.as_bytes());
    });
    let result = OpenAiCompatibleChatProvider::new("fixture", "fixture", url, "synthetic-key")
        .with_request_limit(65536)
        .with_response_limit(1024)
        .chat(request())
        .await;
    server.join().unwrap();
    assert!(matches!(
        result,
        Err(symbiotic_model::ModelError::Provider(
            symbiotic_core::DiagnosticCode::ProviderResponseLimitExceeded
        ))
    ));
}
#[tokio::test]
async fn output_token_limits_refuse_oversized_or_unbounded_requests_before_connecting() {
    let provider = OpenAiCompatibleChatProvider::new(
        "fixture",
        "fixture",
        "http://127.0.0.1:9",
        "synthetic-key",
    )
    .with_request_limit(65536)
    .with_response_limit(65536);
    let mut unbounded = request();
    unbounded.max_output_tokens = None;
    assert!(matches!(
        provider.chat(unbounded).await,
        Err(symbiotic_model::ModelError::InvalidRequest(_))
    ));
    assert!(matches!(
        provider.with_output_limit(32).chat(request()).await,
        Err(symbiotic_model::ModelError::InvalidRequest(_))
    ));
}

#[tokio::test]
async fn disabled_thinking_with_effort_is_refused_before_transport() {
    use symbiotic_model::{ModelError, ModelProvider};
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let provider = OpenAiCompatibleChatProvider::new(
        "fixture",
        "fixture",
        format!("http://{}", listener.local_addr().unwrap()),
        "synthetic-key",
    )
    .with_request_limit(65536)
    .with_response_limit(65536)
    .with_thinking(Some(ThinkingMode::Disabled))
    .with_reasoning_effort("low");
    assert!(matches!(
        provider.validate_configuration(),
        Err(ModelError::InvalidRequest(_))
    ));
    assert!(matches!(
        provider.chat(request()).await,
        Err(ModelError::InvalidRequest(_))
    ));
    assert_eq!(
        listener.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
}

#[test]
fn local_hash_embeddings_have_no_http_destination() {
    use symbiotic_model::{HashEmbeddingProvider, ModelProvider, ProviderClass};
    let provider = HashEmbeddingProvider::new(3);
    assert_eq!(provider.descriptor().provider_class, ProviderClass::Local);
    assert!(provider.descriptor().metadata.get("endpoint").is_none());
}

async fn assert_invalid_id_preserves_answer(id: String) {
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        let (url, server) = fixture(serde_json::json!({
            "id":id, "model":"served-model", "created":0,
            "choices":[{"message":{"content":"OK"}}]
        }));
        let provider = OpenAiCompatibleChatProvider::new("fixture", "fixture", url, "")
            .with_timeout(1)
            .unwrap();
        let response = symbiotic_model::with_egress_http_observations(provider.chat(request()))
            .await
            .expect("invalid identity must preserve the paid answer");
        server.join().unwrap();
        assert_eq!(response.text, "OK");
        assert_eq!(response.trace.usage.response_id, None);
        assert_eq!(
            response.trace.usage.served_model.as_deref(),
            Some("served-model")
        );
        assert_eq!(
            response.trace.metadata["runtime_diagnostics"][0]["kind"],
            "invalid_usage_identity"
        );
    })
    .await
    .expect("identity fixture must finish within three seconds");
}

#[tokio::test]
async fn regression_reasoning_echoes_are_not_usage_identities() {
    tokio::time::timeout(std::time::Duration::from_secs(60), async {
        for reasoning_field in [
            "reasoning_content",
            "reasoning",
            "reasoning_details",
            "thinking",
        ] {
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
                    let mut body = serde_json::json!({
                        "id":"fixture-id", "model":"served-model",
                        "choices":[{"message":{"content":"OK"}}]
                    });
                    body["choices"][0]["message"][reasoning_field] = if reasoning_field
                        == "reasoning_details"
                    {
                        serde_json::json!([{"type":"reasoning.text","text":format!("prefix {} suffix", echoed_identity.unwrap_or("PRIVATE_REASONING"))}])
                    } else {
                        serde_json::json!(format!("prefix {} suffix", echoed_identity.unwrap_or("PRIVATE_REASONING")))
                    };
                    if let Some(identity) = echoed_identity {
                        body[identity_field] = serde_json::json!(identity);
                    }
                    let (url, server) = fixture(body);
                    let provider = OpenAiCompatibleChatProvider::new("fixture", "fixture", url, "")
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
async fn regression_request_phrase_id_is_dropped_without_rejecting_answer() {
    assert_invalid_id_preserves_answer("synthetic evidence".into()).await;
}

#[tokio::test]
async fn regression_129_byte_id_is_dropped_without_rejecting_answer() {
    assert_invalid_id_preserves_answer("a".repeat(129)).await;
}

#[tokio::test]
async fn regression_strict_usage_refusal_is_scoped_to_egress() {
    for malformed in [
        serde_json::json!({"id":null}),
        serde_json::json!({"usage":{"prompt_tokens":10,"prompt_cache_hit_tokens":4,"prompt_cache_miss_tokens":7}}),
    ] {
        for scoped in [false, true] {
            let mut body = malformed.clone();
            body["choices"] = serde_json::json!([{"message":{"content":"OK"}}]);
            let (url, server) = fixture(body);
            let provider = OpenAiCompatibleChatProvider::new("fixture", "fixture", url, "")
                .with_timeout(1)
                .unwrap();
            let call = provider.chat(request());
            let result = if scoped {
                symbiotic_model::with_egress_http_observations(call).await
            } else {
                call.await
            };
            server.join().unwrap();
            if scoped && malformed.get("usage").is_some() {
                assert!(matches!(
                    result,
                    Err(symbiotic_model::ModelError::Provider(
                        symbiotic_core::DiagnosticCode::InvalidResponse
                    ))
                ));
            } else {
                let response = result.expect("direct calls retain successful answers");
                assert_eq!(response.text, "OK");
                assert_eq!(response.trace.usage.response_id, None);
                assert_eq!(response.trace.usage.cache_hit_tokens, None);
                assert_eq!(response.trace.usage.cache_miss_tokens, None);
            }
        }
    }
}

#[tokio::test]
async fn regression_short_reasoning_preserves_ordinary_usage_identities() {
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        for scoped in [false, true] {
            let (url, server) = fixture(serde_json::json!({
                "id":"chatcmpl-4abc", "model":"gpt-4.1",
                "choices":[{"message":{"content":"OK","reasoning_content":"4"}}]
            }));
            let provider = OpenAiCompatibleChatProvider::new("fixture", "fixture", url, "")
                .with_timeout(1)
                .unwrap();
            let call = provider.chat(request());
            let response = if scoped {
                symbiotic_model::with_egress_http_observations(call).await
            } else {
                call.await
            }
            .unwrap();
            server.join().unwrap();
            assert_eq!(response.text, "OK");
            assert_eq!(
                response.trace.usage.response_id.as_deref(),
                Some("chatcmpl-4abc")
            );
            assert_eq!(
                response.trace.usage.served_model.as_deref(),
                Some("gpt-4.1")
            );
            assert_eq!(
                response.trace.metadata["provider"]["response_id"],
                "chatcmpl-4abc"
            );
            assert_eq!(
                response.trace.metadata["provider"]["served_model"],
                "gpt-4.1"
            );
            assert!(response.trace.metadata.get("runtime_diagnostics").is_none());
        }
    })
    .await
    .expect("short reasoning fixtures must finish within three seconds");
}

#[tokio::test]
async fn regression_keyless_direct_adapter_returns_raw_reasoning() {
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        for composed in [false, true] {
            let body = serde_json::json!({
                "id":"fixture-id", "model":"served-model",
                "choices":[{"message":{"content":"OK","reasoning_content":"PRIVATE_REASONING"}}]
            });
            let (url, server) = fixture(body.clone());
            let provider = OpenAiCompatibleChatProvider::new("fixture", "fixture", url, "")
                .with_timeout(1)
                .unwrap();
            let response = if composed {
                let provider: std::sync::Arc<dyn ChatProvider> = std::sync::Arc::new(provider);
                provider.chat(request()).await
            } else {
                provider.chat(request()).await
            }
            .unwrap();
            server.join().unwrap();
            assert_eq!(response.text, "OK");
            assert_eq!(response.raw_provider_response, Some(body));
            assert!(
                !serde_json::to_string(&response.trace)
                    .unwrap()
                    .contains("PRIVATE_REASONING")
            );
        }
    })
    .await
    .expect("keyless direct fixtures must finish within three seconds");
}

#[tokio::test]
async fn regression_request_echoes_are_not_usage_identities() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        for role in ["system", "developer", "user", "assistant", "tool"] {
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
                    // Local metadata is not sent and must not screen valid identities.
                    req.metadata = serde_json::json!({"local": identity});
                    let body = serde_json::json!({"id":identity,"model":identity,"choices":[{"message":{"content":"OK","reasoning_content":"unrelated reasoning"},"finish_reason":"stop"}],"usage":{"prompt_tokens":7,"completion_tokens":2}});
                    let (url, server) = fixture(body);
                    let provider = OpenAiCompatibleChatProvider::new("fixture", "requested-model", url, "")
                        .with_timeout(1)
                        .unwrap();
                    let call = provider.chat(req);
                    let response = if scoped {
                        symbiotic_model::with_egress_http_observations(call).await
                    } else {
                        call.await
                    }
                    .expect("request screening must preserve the paid answer");
                    let wire = server.join().unwrap();
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
