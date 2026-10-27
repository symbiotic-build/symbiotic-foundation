use std::sync::Arc;
use symbiotic_ai_runtime::model::{DEFAULT_MAX_REQUEST_BYTES, DEFAULT_MAX_RESPONSE_BYTES};
use symbiotic_credential_process::{
    CredentialProcess, ProcessConfig, RouteConfig, secrets::SecretSource, validate_routes,
};
use symbiotic_egress::{EgressError, PROTOCOL_VERSION};

const FRAME_BYTES: u32 = 8 * DEFAULT_MAX_RESPONSE_BYTES as u32;

fn route() -> RouteConfig {
    serde_json::from_value(route_json()).unwrap()
}

fn route_json() -> serde_json::Value {
    serde_json::json!({
        "tenant": "tenant", "account": "account", "account_sharing_key": null,
        "max_attempts": 3, "route": "chat", "secret_ref": "", "secret": {"backend": "none"},
        "destination": "https://example.com/v1", "model": "test-model",
        "provider": {"kind": "open_ai_chat", "operator": "test"},
        "allow_loopback_http": false, "max_field_bytes": 1024, "max_output_tokens": 100,
        "max_in_flight": 4, "requests_per_minute": null, "input_units_per_minute": null,
        "timeout_seconds": 1
    })
}

fn config(dir: &std::path::Path, routes: Vec<RouteConfig>, frame: u32) -> ProcessConfig {
    ProcessConfig {
        log: None,
        version: PROTOCOL_VERSION,
        state_dir: dir.join("state"),
        socket_path: dir.join("egress.sock"),
        admission_key: SecretSource::Resolver {
            name: "admission".into(),
            resolve: Arc::new(|_| panic!("validation must not resolve credentials")),
        },
        max_secret_bytes: 4096,
        max_frame_bytes: frame,
        max_connections: 8,
        io_timeout_seconds: 2,
        clock_rollback_warning_tolerance_seconds: 5,
        jobs: Default::default(),
        job_runner: Default::default(),
        passthrough: None,
        routes,
    }
}

fn refused(routes: Vec<RouteConfig>, frame: u32, expected: EgressError) {
    let dir = tempfile::tempdir().unwrap();
    assert_eq!(validate_routes(&routes, frame), Err(expected));
    assert_eq!(
        CredentialProcess::open(config(dir.path(), routes, frame)).err(),
        Some(expected)
    );
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
}

#[test]
fn route_validation_reports_startup_field_and_destination_refusals() {
    for field in [
        "max_attempts",
        "max_field_bytes",
        "max_input_bytes",
        "max_response_bytes",
        "max_output_tokens",
        "max_in_flight",
        "requests_per_minute",
        "input_units_per_minute",
        "timeout_seconds",
    ] {
        let mut json = route_json();
        json[field] = serde_json::json!(0);
        refused(
            vec![serde_json::from_value(json).unwrap()],
            FRAME_BYTES,
            EgressError::InvalidRequest,
        );
    }
    for field in ["tenant", "account", "route", "model"] {
        for value in ["", " "] {
            let mut json = route_json();
            json[field] = serde_json::json!(value);
            refused(
                vec![serde_json::from_value(json).unwrap()],
                FRAME_BYTES,
                EgressError::InvalidRequest,
            );
        }
    }
    for destination in [
        "not a URL",
        "file:///tmp/provider",
        "https:///",
        "https://user@example.com/v1",
        "https://user:password@example.com/v1",
        "https://example.com/v1?setting=1",
        "https://example.com/v1#fragment",
        "http://example.com/v1",
        "http://127.0.0.1/v1",
        "http://localhost/v1",
    ] {
        let mut candidate = route();
        candidate.destination = destination.into();
        refused(vec![candidate], FRAME_BYTES, EgressError::InvalidRequest);
    }
    let mut remote_http = route();
    remote_http.destination = "http://example.com/v1".into();
    remote_http.allow_loopback_http = true;
    refused(vec![remote_http], FRAME_BYTES, EgressError::InvalidRequest);
    let mut secret_mismatch = route();
    secret_mismatch.secret_ref = "reference".into();
    refused(
        vec![secret_mismatch],
        FRAME_BYTES,
        EgressError::InvalidRequest,
    );
    let mut secret_mismatch = route();
    secret_mismatch.secret = SecretSource::OwnerOnlyFile {
        path: "missing".into(),
    };
    refused(
        vec![secret_mismatch],
        FRAME_BYTES,
        EgressError::InvalidRequest,
    );
    for limit in [i64::MAX as u64 + 1, u64::MAX] {
        let mut candidate = route();
        candidate.provider_request_limit = Some(limit);
        refused(vec![candidate], FRAME_BYTES, EgressError::InvalidRequest);
    }
    for (field, value) in [
        ("max_in_flight", usize::MAX as u64),
        ("timeout_seconds", i64::MAX as u64 / 1000 + 1),
    ] {
        let mut json = route_json();
        json[field] = serde_json::json!(value);
        refused(
            vec![serde_json::from_value(json).unwrap()],
            FRAME_BYTES,
            EgressError::InvalidRequest,
        );
    }
}

#[test]
fn route_validation_reports_startup_frame_and_route_set_refusals() {
    let error = EgressError::InvalidFrameConfiguration.to_string();
    for supported in [
        "4124",
        "4096 + 24 * max_field_bytes + 4 * max_response_bytes",
    ] {
        assert!(
            error.contains(supported),
            "missing supported bound: {error}"
        );
    }
    refused(vec![], FRAME_BYTES, EgressError::InvalidRequest);
    refused(
        vec![route(), route()],
        FRAME_BYTES,
        EgressError::InvalidRequest,
    );
    for frame in [0, 4096, 4123] {
        refused(vec![route()], frame, EgressError::InvalidFrameConfiguration);
    }
    for field in ["max_field_bytes", "max_response_bytes"] {
        let mut json = route_json();
        json[field] = serde_json::json!(usize::MAX);
        refused(
            vec![serde_json::from_value(json).unwrap()],
            FRAME_BYTES,
            EgressError::InvalidFrameConfiguration,
        );
    }
    let mut candidate = route();
    candidate.max_input_bytes = FRAME_BYTES as usize / 2 + 1;
    refused(vec![candidate], FRAME_BYTES, EgressError::InvalidRequest);
    for shared in [false, true] {
        for field in [
            "max_in_flight",
            "requests_per_minute",
            "input_units_per_minute",
            "provider_request_limit",
            "timeout_seconds",
        ] {
            let mut first = route_json();
            if shared {
                first["account_sharing_key"] = serde_json::json!("pool");
            }
            let mut second = first.clone();
            second["route"] = serde_json::json!("second");
            if shared {
                second["tenant"] = serde_json::json!("other-tenant");
                second["account"] = serde_json::json!("other-account");
            }
            second[field] = serde_json::json!(10);
            let routes: Vec<RouteConfig> = vec![
                serde_json::from_value(first).unwrap(),
                serde_json::from_value(second).unwrap(),
            ];
            for routes in [routes.clone(), routes.into_iter().rev().collect()] {
                refused(routes, FRAME_BYTES, EgressError::InvalidRequest);
            }
        }
    }
}

#[test]
fn route_validation_reports_runtime_account_scope_refusals() {
    for key in ["", " "] {
        let mut json = route_json();
        json["account_sharing_key"] = serde_json::json!(key);
        refused(
            vec![serde_json::from_value(json).unwrap()],
            FRAME_BYTES,
            EgressError::InvalidRequest,
        );
    }
}

#[test]
fn route_validation_reports_startup_provider_setting_refusals() {
    let providers = [
        serde_json::json!({"kind":"open_ai_chat", "operator":""}),
        serde_json::json!({"kind":"open_ai_chat", "operator":"test", "thinking":"disabled", "reasoning_effort":"low"}),
        serde_json::json!({"kind":"anthropic_chat", "operator":"", "thinking":null}),
        serde_json::json!({"kind":"jev_classifier", "operator":""}),
        serde_json::json!({"kind":"gemini_embedding", "dimensions":0}),
        serde_json::json!({"kind":"gemini_embedding", "dimensions":16}),
    ];
    for provider in providers {
        let mut json = route_json();
        json["provider"] = provider;
        refused(
            vec![serde_json::from_value(json).unwrap()],
            FRAME_BYTES,
            EgressError::InvalidRequest,
        );
    }
    for kind in ["compatible_embedding", "cohere_rerank"] {
        let provider = if kind == "compatible_embedding" {
            serde_json::json!({"kind":kind,"adapter":"open_ai_embedding","operator":"test","dimensions":16,"embedding_full_dimensions":32,"embedding_input_tokens":100})
        } else {
            serde_json::json!({"kind":kind,"operator":"test","rerank_input_bytes":100,"rerank_candidates":10,"rerank_context_tokens":100,"rerank_query_tokens":50})
        };
        for field in provider
            .as_object()
            .unwrap()
            .keys()
            .filter(|key| !matches!(key.as_str(), "kind" | "adapter"))
        {
            let mut json = route_json();
            json["provider"] = provider.clone();
            json["provider"][field] = if field == "operator" {
                serde_json::json!("")
            } else {
                serde_json::json!(0)
            };
            refused(
                vec![serde_json::from_value(json).unwrap()],
                FRAME_BYTES,
                EgressError::InvalidRequest,
            );
        }
        let mut json = route_json();
        json["provider"] = provider;
        let field = if kind == "compatible_embedding" {
            "dimensions"
        } else {
            "rerank_query_tokens"
        };
        json["provider"][field] = serde_json::json!(1000);
        refused(
            vec![serde_json::from_value(json).unwrap()],
            FRAME_BYTES,
            EgressError::InvalidRequest,
        );
    }
    for adapter in [
        "open_ai_chat",
        "anthropic_chat",
        "gemini_embedding",
        "cohere_rerank",
        "jev_classifier",
    ] {
        let mut json = route_json();
        json["provider"] = serde_json::json!({"kind":"compatible_embedding","adapter":adapter,"operator":"test","dimensions":16,"embedding_full_dimensions":32,"embedding_input_tokens":100});
        refused(
            vec![serde_json::from_value(json).unwrap()],
            FRAME_BYTES,
            EgressError::InvalidRequest,
        );
    }
    let mut gemini = route_json();
    gemini["destination"] = serde_json::json!("https://generativelanguage.googleapis.com/v1beta");
    gemini["model"] = serde_json::json!("invalid/model");
    gemini["provider"] = serde_json::json!({"kind":"gemini_embedding","dimensions":16});
    refused(
        vec![serde_json::from_value(gemini).unwrap()],
        FRAME_BYTES,
        EgressError::InvalidRequest,
    );
}

#[test]
fn route_validation_is_pure_with_defaults_overrides_and_secret_backends() {
    let dir = tempfile::tempdir().unwrap();
    let mut candidate = route();
    assert_eq!(candidate.max_input_bytes, DEFAULT_MAX_REQUEST_BYTES);
    assert_eq!(candidate.max_response_bytes, DEFAULT_MAX_RESPONSE_BYTES);
    for source in [
        SecretSource::None,
        SecretSource::OwnerOnlyFile {
            path: dir.path().join("missing-provider"),
        },
        SecretSource::Resolver {
            name: "provider".into(),
            resolve: Arc::new(|_| panic!("validation must not resolve credentials")),
        },
    ] {
        candidate.secret_ref = if matches!(source, SecretSource::None) {
            ""
        } else {
            "reference"
        }
        .into();
        candidate.secret = source;
        for recovery in [
            symbiotic_credential_process::AnswerRecovery::Retain,
            symbiotic_credential_process::AnswerRecovery::Off,
        ] {
            candidate.answer_recovery = recovery;
            for budget in [None, Some(0), Some(1), Some(i64::MAX as u64)] {
                candidate.provider_request_limit = budget;
                assert_eq!(validate_routes(&[candidate.clone()], FRAME_BYTES), Ok(()));
            }
        }
    }
    for destination in ["http://127.0.0.1/v1", "http://localhost/v1"] {
        candidate.destination = destination.into();
        candidate.allow_loopback_http = true;
        candidate.max_input_bytes = 1;
        candidate.max_response_bytes = 1;
        candidate.max_field_bytes = 1;
        assert_eq!(validate_routes(&[candidate.clone()], 4124), Ok(()));
    }
    for provider in [
        serde_json::json!({"kind":"anthropic_chat","operator":"test","thinking":null}),
        serde_json::json!({"kind":"jev_classifier","operator":"test"}),
        serde_json::json!({"kind":"gemini_embedding","dimensions":16}),
        serde_json::json!({"kind":"compatible_embedding","adapter":"open_ai_embedding","operator":"test","dimensions":16,"embedding_full_dimensions":32,"embedding_input_tokens":100}),
        serde_json::json!({"kind":"compatible_embedding","adapter":"ollama_embedding","operator":"test","dimensions":16,"embedding_full_dimensions":32,"embedding_input_tokens":100}),
        serde_json::json!({"kind":"cohere_rerank","operator":"test","rerank_input_bytes":100,"rerank_candidates":10,"rerank_context_tokens":100,"rerank_query_tokens":50}),
    ] {
        let mut json = route_json();
        if provider["kind"] == "gemini_embedding" {
            json["destination"] =
                serde_json::json!("https://generativelanguage.googleapis.com/v1beta");
        }
        json["provider"] = provider;
        assert_eq!(
            validate_routes(&[serde_json::from_value(json).unwrap()], FRAME_BYTES),
            Ok(())
        );
    }
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
}

#[test]
fn route_validation_unsupported_typed_settings_report_supported_values() {
    for (field, supported) in [
        ("answer_recovery", vec!["retain", "off"]),
        ("thinking", vec!["enabled", "disabled"]),
        ("reasoning_effort", vec!["low", "medium", "high"]),
    ] {
        let mut json = route_json();
        if field == "answer_recovery" {
            json[field] = serde_json::json!("unsupported");
        } else {
            json["provider"][field] = serde_json::json!("unsupported");
        }
        let error = serde_json::from_value::<RouteConfig>(json)
            .err()
            .unwrap()
            .to_string();
        for value in supported {
            assert!(
                error.contains(value),
                "{field}: missing supported value {value}: {error}"
            );
        }
    }
}
