use symbiotic_egress::*;

#[test]
fn response_size_diagnostic_round_trips_without_response_bytes() {
    let started = std::time::Instant::now();
    let json = r#"["max_response_bytes_exceeded"]"#;
    let diagnostics: Vec<DispatchDiagnostic> = serde_json::from_str(json).unwrap();
    assert_eq!(serde_json::to_string(&diagnostics).unwrap(), json);
    eprintln!(
        "response size diagnostic: static wire round trip, elapsed={:?}",
        started.elapsed()
    );
}

#[test]
fn invalid_provider_json_round_trips_as_a_distinct_v4_wire_error() {
    let json = r#"{"version":4,"result":{"Err":"invalid_provider_json"}}"#;
    let response: Response = serde_json::from_str(json).unwrap();
    assert_eq!(response.version, PROTOCOL_VERSION);
    assert_eq!(serde_json::to_string(&response).unwrap(), json);
    assert_eq!(
        serde_json::to_value(response.result.err().unwrap()).unwrap(),
        serde_json::json!("invalid_provider_json")
    );
}

#[test]
fn existing_error_codes_keep_their_wire_bytes() {
    for (error, json) in [
        (EgressError::Version, r#""version""#),
        (EgressError::InvalidRequest, r#""invalid_request""#),
        (
            EgressError::InvalidFrameConfiguration,
            r#""invalid_frame_configuration""#,
        ),
        (
            EgressError::ResolverRequiresThreadMode,
            r#""resolver_requires_thread_mode""#,
        ),
        (EgressError::Unauthorized, r#""unauthorized""#),
        (EgressError::RouteRefused, r#""route_refused""#),
        (EgressError::PermitRefused, r#""permit_refused""#),
        (EgressError::AuthorityExpired, r#""authority_expired""#),
        (
            EgressError::ReconciliationRequired,
            r#""reconciliation_required""#,
        ),
        (EgressError::InvocationComplete, r#""invocation_complete""#),
        (EgressError::BudgetRefused, r#""budget_refused""#),
        (
            EgressError::RequestBudgetExhausted,
            r#""request_budget_exhausted""#,
        ),
        (
            EgressError::CredentialUnavailable,
            r#""credential_unavailable""#,
        ),
        (EgressError::StateUnavailable, r#""state_unavailable""#),
        (EgressError::LimitExceeded, r#""limit_exceeded""#),
        (
            EgressError::RateLimited {
                retry_after_seconds: None,
            },
            r#"{"rate_limited":{"retry_after_seconds":null}}"#,
        ),
        (
            EgressError::RateLimited {
                retry_after_seconds: Some(7),
            },
            r#"{"rate_limited":{"retry_after_seconds":7}}"#,
        ),
        (EgressError::Timeout, r#""timeout""#),
        (
            EgressError::Provider { status: None },
            r#"{"provider":{"status":null}}"#,
        ),
        (
            EgressError::Provider { status: Some(500) },
            r#"{"provider":{"status":500}}"#,
        ),
        (EgressError::Transport, r#""transport""#),
    ] {
        assert_eq!(serde_json::to_string(&error).unwrap(), json);
        assert_eq!(serde_json::from_str::<EgressError>(json).unwrap(), error);
    }
}

#[test]
fn v4_refusal_has_the_documented_wire_shape() {
    let response = Response {
        version: PROTOCOL_VERSION,
        result: Err(EgressError::PermitRefused),
    };
    assert_eq!(
        serde_json::to_string(&response).unwrap(),
        r#"{"version":4,"result":{"Err":"permit_refused"}}"#
    );
    let decoded: Response =
        serde_json::from_str(r#"{"version":4,"result":{"Err":"permit_refused"}}"#).unwrap();
    assert!(matches!(decoded.result, Err(EgressError::PermitRefused)));
}

#[test]
fn invalidated_status_has_a_distinct_wire_state() {
    let json = serde_json::to_string(&AttemptStatus::Invalidated).unwrap();
    assert_eq!(json, r#"{"state":"invalidated"}"#);
    assert!(matches!(
        serde_json::from_str::<AttemptStatus>(&json).unwrap(),
        AttemptStatus::Invalidated
    ));
}

#[test]
fn authority_expired_refusal_has_a_typed_v4_wire_error() {
    let response = Response {
        version: PROTOCOL_VERSION,
        result: Err(EgressError::AuthorityExpired),
    };
    let json = serde_json::to_string(&response).unwrap();
    assert_eq!(
        json,
        r#"{"version":4,"result":{"Err":"authority_expired"}}"#
    );
    assert!(matches!(
        serde_json::from_str::<Response>(&json).unwrap().result,
        Err(EgressError::AuthorityExpired)
    ));
}

#[test]
fn authority_deadline_is_required_signed_and_digested_in_v4() {
    let attempt = protocol_attempt();
    let key = AdmissionKey::new(vec![42; 32]).unwrap();
    let signed = key.sign_attempt(attempt.clone()).unwrap();
    let request = Request {
        version: PROTOCOL_VERSION,
        operation: Operation::IssuePermit(signed.clone().into()),
    };
    let decoded: Request = serde_json::from_slice(&serde_json::to_vec(&request).unwrap()).unwrap();
    let Operation::IssuePermit(decoded) = decoded.operation else {
        panic!("missing signed attempt");
    };
    assert_eq!(decoded.attempt.expires_at, 200);
    key.verify_attempt(&decoded).unwrap();
    let mut tampered = signed;
    tampered.attempt.expires_at += 1;
    assert_ne!(
        digest(&attempt).unwrap(),
        digest(&tampered.attempt).unwrap()
    );
    assert_eq!(
        key.verify_attempt(&tampered),
        Err(EgressError::Unauthorized)
    );
    let mut missing = serde_json::to_value(attempt).unwrap();
    missing.as_object_mut().unwrap().remove("expires_at");
    assert!(serde_json::from_value::<DurableAttempt>(missing).is_err());
}

#[tokio::test]
async fn memory_can_use_a_trait_object_test_double_without_the_credential_process() {
    struct Double;
    #[async_trait::async_trait]
    impl EgressClient for Double {
        async fn exchange(&self, _: Request) -> Result<Response, EgressError> {
            Err(EgressError::Transport)
        }
    }
    let client: Box<dyn EgressClient> = Box::new(Double);
    let signed = AdmissionKey::new(vec![42; 32])
        .unwrap()
        .sign_grant_revision(GrantRevision {
            tenant: "tenant".into(),
            incarnation: "incarnation".into(),
            revision: 11,
        })
        .unwrap();
    assert!(matches!(
        client
            .exchange(Request {
                version: PROTOCOL_VERSION,
                operation: Operation::PublishGrantRevision(signed)
            })
            .await,
        Err(EgressError::Transport)
    ));
}

#[cfg(unix)]
#[tokio::test]
async fn frame_limit_is_checked_without_waiting_for_or_allocating_the_declared_body() {
    use tokio::io::AsyncWriteExt;
    let (mut client, mut server) = tokio::io::duplex(4);
    client.write_u32(u32::MAX).await.unwrap();
    let response = socket::read_frame::<Response>(&mut server, 4096).await;
    assert!(matches!(response, Err(EgressError::LimitExceeded)));
}

#[tokio::test]
async fn recovery_status_method_uses_v4_and_rejects_old_replies() {
    struct Double(u16);
    #[async_trait::async_trait]
    impl EgressClient for Double {
        async fn exchange(&self, request: Request) -> Result<Response, EgressError> {
            assert_eq!(request.version, PROTOCOL_VERSION);
            let Operation::AttemptStatus(signed) = request.operation else {
                panic!("wrong operation");
            };
            AdmissionKey::new(vec![42; 32])
                .unwrap()
                .verify_attempt_id(&signed)
                .unwrap();
            Ok(Response {
                version: self.0,
                result: Ok(Reply::AttemptStatus(AttemptStatus::NotIssued)),
            })
        }
    }
    let signed = AdmissionKey::new(vec![42; 32])
        .unwrap()
        .sign_attempt_id(AttemptId {
            tenant: "tenant".into(),
            incarnation: "incarnation".into(),
            invocation_id: "invocation".into(),
            job_queue: None,
            attempt_ordinal: 1,
        })
        .unwrap();
    let client: Box<dyn EgressClient> = Box::new(Double(PROTOCOL_VERSION));
    assert!(matches!(
        client.attempt_status(signed.clone()).await.unwrap(),
        AttemptStatus::NotIssued
    ));
    assert!(matches!(
        Double(PROTOCOL_VERSION - 1).attempt_status(signed).await,
        Err(EgressError::Version)
    ));
}

#[test]
fn retrieval_output_decimals_survive_tagged_json_round_trips() {
    for json in [
        r#"{"kind":"embedding","vectors":[[0.125,-0.5]],"dimensions":2}"#,
        r#"{"kind":"rerank","hits":[{"index":0,"score":0.75}]}"#,
    ] {
        let output: ProviderOutput = serde_json::from_str(json).unwrap();
        match &output {
            ProviderOutput::Embedding {
                vectors,
                dimensions,
            } => {
                assert_eq!(vectors, &vec![vec![0.125, -0.5]]);
                assert_eq!(*dimensions, 2);
            }
            ProviderOutput::Rerank { hits } => {
                assert_eq!(hits.len(), 1);
                assert_eq!(hits[0].index, 0);
                assert_eq!(hits[0].score, 0.75);
            }
            _ => panic!("wrong output"),
        }
        let encoded = serde_json::to_string(&output).unwrap();
        let recovered: ProviderOutput = serde_json::from_str(&encoded).unwrap();
        assert_eq!(
            serde_json::to_value(output).unwrap(),
            serde_json::to_value(recovered).unwrap()
        );
    }
}

#[test]
fn accepted_receipt_identity_and_reference_round_trip_without_consumer_spend_fields() {
    let receipt = DispatchReceipt {
        attempt_digest: "a".repeat(64),
        attempt_id: AttemptId {
            tenant: "tenant".into(),
            incarnation: "incarnation".into(),
            invocation_id: "invocation".into(),
            job_queue: None,
            attempt_ordinal: 1,
        },
        reference: SpendReceiptRef::new("egress:accepted-attempt").unwrap(),
        status: DispatchStatus::ProviderFailed,
        usage: Default::default(),
        spend_state: SpendState::Unknown,
    };
    let response = Response {
        version: PROTOCOL_VERSION,
        result: Ok(Reply::AttemptStatus(AttemptStatus::Dispatched { receipt })),
    };
    let bytes = serde_json::to_vec(&response).unwrap();
    let decoded: Response = serde_json::from_slice(&bytes).unwrap();
    let Reply::AttemptStatus(AttemptStatus::Dispatched { receipt }) = decoded.result.unwrap()
    else {
        panic!("missing accepted receipt");
    };
    assert_eq!(receipt.attempt_id.invocation_id, "invocation");
    assert_eq!(
        receipt.reference,
        SpendReceiptRef::new("egress:accepted-attempt").unwrap()
    );
    assert_eq!(receipt.spend_state, SpendState::Unknown);
    let value = serde_json::to_value(&receipt).unwrap();
    assert_eq!(value.as_object().unwrap().len(), 6);
    let status = AttemptStatus::FinishedWithoutAnswer { receipt };
    let encoded = serde_json::to_value(&status).unwrap();
    assert_eq!(encoded["state"], "finished_without_answer");
    assert!(encoded.get("result").is_none());
    let AttemptStatus::FinishedWithoutAnswer { receipt } = serde_json::from_value(encoded).unwrap()
    else {
        panic!("missing content-free completion")
    };
    assert_eq!(receipt.spend_state, SpendState::Unknown);
}

fn protocol_attempt() -> DurableAttempt {
    serde_json::from_value(serde_json::json!({
        "tenant": "tenant", "incarnation": "incarnation", "invocation_id": "invocation",
        "attempt_ordinal": 1, "record_sequence": 1, "recorded_at": 100, "expires_at": 200,
        "recovery_expires_at": 300, "caller_binding": "caller", "route": "provider",
        "destination": "https://example.test", "model": "model", "method": "POST",
        "secret_ref": "secret", "manifest_ref": "manifest",
        "input_manifest_digest": "a".repeat(64), "input_digest": "b".repeat(64),
        "grant_revision": 10,
    }))
    .unwrap()
}

#[test]
fn admissions_reject_removed_consumer_spend_and_marking_fields() {
    let attempt = serde_json::to_value(protocol_attempt()).unwrap();
    for (field, value) in [
        (
            "reserved_budget",
            serde_json::json!({"unit": "provider_requests", "amount": 1, "invocation_limit": 1}),
        ),
        ("markings", serde_json::json!([])),
        ("max_attempts", serde_json::json!(3)),
    ] {
        let mut obsolete = attempt.clone();
        obsolete[field] = value;
        assert!(
            serde_json::from_value::<DurableAttempt>(obsolete).is_err(),
            "accepted {field}"
        );
    }
}

#[cfg(unix)]
mod clients {
    use super::*;
    use std::{os::unix::fs::PermissionsExt, time::Duration};
    use symbiotic_credential_process::{
        CredentialProcess, InProcessEgressClient, ProcessConfig, RouteConfig, RouteProvider,
        secrets::SecretSource, server,
    };

    async fn protocol_checks(client: &dyn EgressClient) {
        let key = AdmissionKey::new(vec![42; 32]).unwrap();
        let grant = GrantRevision {
            tenant: "tenant".into(),
            incarnation: "incarnation".into(),
            revision: 10,
        };
        let publish = |revision| {
            Operation::PublishGrantRevision(
                key.sign_grant_revision(GrantRevision {
                    revision,
                    ..grant.clone()
                })
                .unwrap(),
            )
        };
        let send = |operation| Request {
            version: PROTOCOL_VERSION,
            operation,
        };
        let mut bad_grant = key.sign_grant_revision(grant.clone()).unwrap();
        bad_grant.grant.revision += 1;
        assert!(matches!(
            client
                .exchange(send(Operation::PublishGrantRevision(bad_grant)))
                .await
                .unwrap()
                .result,
            Err(EgressError::Unauthorized)
        ));
        assert!(matches!(
            client.exchange(send(publish(10))).await.unwrap().result,
            Ok(Reply::GrantRevisionPublished)
        ));
        let mut attempt = protocol_attempt();
        attempt.expires_at = 4_000_000_000;
        attempt.recovery_expires_at = 4_000_000_001;
        attempt.secret_ref.clear();
        let admission = key.sign_attempt(attempt).unwrap();
        let id = key.sign_attempt_id(admission.attempt.attempt_id()).unwrap();
        assert!(matches!(
            client.attempt_status(id.clone()).await.unwrap(),
            AttemptStatus::NotIssued
        ));
        let mut bad_attempt = admission.clone();
        bad_attempt.attempt.input_digest = "c".repeat(64);
        assert!(matches!(
            client
                .exchange(send(Operation::IssuePermit(bad_attempt.into())))
                .await
                .unwrap()
                .result,
            Err(EgressError::Unauthorized)
        ));
        let response = client
            .exchange(send(Operation::IssuePermit(admission.clone().into())))
            .await
            .unwrap();
        assert_eq!(response.version, PROTOCOL_VERSION);
        let Reply::Permit(first) = response.result.unwrap() else {
            panic!("missing permit")
        };
        assert!(matches!(first.status, AttemptStatus::Permitted));
        let Reply::Permit(replay) = client
            .exchange(send(Operation::IssuePermit(admission.into())))
            .await
            .unwrap()
            .result
            .unwrap()
        else {
            panic!("missing replay")
        };
        assert_eq!(first.permit.token, replay.permit.token);
        assert!(matches!(
            client.attempt_status(id.clone()).await.unwrap(),
            AttemptStatus::Permitted
        ));
        let mut bad_id = id.clone();
        bad_id.attempt_id.invocation_id = "foreign".into();
        assert!(matches!(
            client.attempt_status(bad_id).await,
            Err(EgressError::Unauthorized)
        ));
        assert!(matches!(
            client.exchange(send(publish(11))).await.unwrap().result,
            Ok(Reply::GrantRevisionPublished)
        ));
        assert!(matches!(
            client.attempt_status(id.clone()).await.unwrap(),
            AttemptStatus::Invalidated
        ));
        assert!(matches!(
            client.exchange(send(publish(10))).await.unwrap().result,
            Err(EgressError::RouteRefused)
        ));
        assert!(matches!(
            client
                .exchange(send(Operation::Receipt(id.clone())))
                .await
                .unwrap()
                .result,
            Ok(Reply::Receipt(None))
        ));
        assert!(matches!(
            client
                .exchange(Request {
                    version: PROTOCOL_VERSION - 1,
                    operation: Operation::AttemptStatus(id)
                })
                .await
                .unwrap()
                .result,
            Err(EgressError::Version)
        ));
    }

    async fn run(in_process: bool) {
        let dir = tempfile::tempdir().unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let admission = dir.path().join("admission");
        std::fs::write(&admission, vec![42; 32]).unwrap();
        std::fs::set_permissions(&admission, std::fs::Permissions::from_mode(0o600)).unwrap();
        let config = ProcessConfig {
            version: PROTOCOL_VERSION,
            state_dir: dir.path().join("state"),
            socket_path: dir.path().join(if in_process {
                "absent/egress.sock"
            } else {
                "egress.sock"
            }),
            admission_key: SecretSource::OwnerOnlyFile { path: admission },
            max_secret_bytes: 4096,
            max_frame_bytes: 262144,
            max_connections: 8,
            io_timeout_seconds: 2,
            clock_rollback_warning_tolerance_seconds: 5,
            jobs: Default::default(),
            job_runner: Default::default(),
            passthrough: None,
            routes: vec![RouteConfig {
                answer_recovery: Default::default(),
                tenant: "tenant".into(),
                account: "account".into(),
                account_sharing_key: None,
                provider_request_limit: None,
                max_attempts: 3,
                request_budget: None,
                route: "provider".into(),
                secret_ref: String::new(),
                secret: SecretSource::None,
                destination: "https://example.test".into(),
                model: "model".into(),
                provider: RouteProvider::OpenAiChat {
                    operator: "test".into(),
                    thinking: None,
                    reasoning_effort: None,
                },
                allow_loopback_http: false,
                max_input_bytes: 32768,
                max_response_bytes: 32768,
                max_field_bytes: 1024,
                max_output_tokens: 100,
                max_in_flight: 4,
                requests_per_minute: None,
                input_units_per_minute: None,
                timeout_seconds: 1,
            }],
        };
        let process = CredentialProcess::open(config.clone()).unwrap();
        if in_process {
            // Thread mode needs no socket, including no socket parent directory.
            let client = InProcessEgressClient::new(process);
            protocol_checks(&client).await;
            assert!(!config.socket_path.exists());
        } else {
            let listener = server::bind(&process).unwrap();
            let task = tokio::spawn(server::serve(process, listener));
            let client = socket::UnixEgressClient {
                path: config.socket_path,
                max_frame_bytes: config.max_frame_bytes,
                timeout: Duration::from_secs(3),
            };
            protocol_checks(&client).await;
            task.abort();
            let _ = task.await;
        }
    }

    #[tokio::test]
    async fn in_process_client_preserves_the_egress_protocol() {
        run(true).await;
    }

    #[tokio::test]
    async fn socket_client_preserves_the_egress_protocol() {
        run(false).await;
    }
}

#[test]
fn rabbithole_outputs_round_trip_finish_reasons_and_classification_numbers() {
    for output in [
        serde_json::json!({"kind":"chat", "text":"partial", "finish_reason":"length"}),
        serde_json::json!({"kind":"chat", "text":"answer", "finish_reason":"stop"}),
        serde_json::json!({"kind":"chat", "text":"answer", "finish_reason":"other"}),
        serde_json::json!({"kind":"chat", "text":"answer", "finish_reason":null}),
        serde_json::json!({"kind":"classify", "answers":[
            {"question_id":"n", "value":{"noul":{"probability":0.12}}},
            {"question_id":"c", "value":{"choice":{"chosen":"a", "probabilities":[{"id":"a","probability":0.75},{"id":"b","probability":0.25}],"confidence":0.6}}},
            {"question_id":"s", "value":{"score":{"value":0.75,"probabilities":[0.25,0.75],"confidence":null}}}
        ]}),
    ] {
        let decoded: ProviderOutput =
            serde_json::from_slice(&serde_json::to_vec(&output).unwrap()).unwrap();
        assert_eq!(serde_json::to_value(decoded).unwrap(), output);
    }
}

#[test]
fn rabbithole_classification_payload_preserves_numeric_state_and_digest() {
    let state = serde_json::from_str(r#"{"count":12,"fraction":0.1200,"nested":[{"value":0.25}]}"#)
        .unwrap();
    let payload = ProviderPayload::Classify(ClassifyRequest::new(
        state,
        vec![ClassifierQuestion::noul(
            "continue",
            "Does this continue an exchange?",
            None,
            None,
        )],
    ));
    let bytes = serde_json::to_vec(&payload).unwrap();
    let decoded: ProviderPayload = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(decoded.digest().unwrap(), payload.digest().unwrap());
    assert_eq!(serde_json::to_vec(&decoded).unwrap(), bytes);
}
