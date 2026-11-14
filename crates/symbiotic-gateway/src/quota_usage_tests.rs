//! Closed-gate usage reads against a local provider fixture.
use super::*;

fn usage_config(config: &mut GatewayConfig) {
    login_config(config);
    config.passthrough.routes[0].destination =
        "https://chatgpt.com/backend-api/codex/responses".into();
}
fn usage_reply(used: u32, reset: i64, seconds: u64) -> Reply {
    Reply {
        body: serde_json::to_vec(&json!({"rate_limit":{
            "primary_window":{"used_percent":10,"limit_window_seconds":18000,"reset_at":reset},
            "secondary_window":{"used_percent":used,"limit_window_seconds":seconds,"reset_at":reset}
        }}))
        .unwrap(),
        ..Reply::json()
    }
}
async fn exhaust(harness: &Harness, provider: &mut Fixture) -> i64 {
    let reset = (harness.process.inner.passthrough.now() + chrono::Duration::days(1)).timestamp();
    let caller = harness.connect("/v1/responses", b"{}", "").await;
    provider
        .next()
        .await
        .reply
        .send(responses_reply(100, reset))
        .ok()
        .unwrap();
    assert_eq!(decoded(&read(caller).await).0, 200);
    reset
}
async fn saved(harness: &Harness) -> Value {
    let records = &harness.process.inner.passthrough.records;
    records.outbox.flush().await.unwrap();
    let gates = records.outbox.load_gates().unwrap();
    serde_json::from_str(
        &gates
            .into_iter()
            .find(|(name, _)| name == "tenant/account")
            .unwrap()
            .1,
    )
    .unwrap()
}
async fn assert_quota_refusal(caller: UnixStream) {
    let (status, _, body) = decoded(&read(caller).await);
    assert_eq!(
        (status, body),
        (402, br#"{"error":{"code":"quota_exhausted"}}"#.to_vec())
    );
}
fn assert_no_provider_request(provider: &mut Fixture) {
    if let Ok(request) = provider.incoming.try_recv() {
        panic!(
            "unexpected provider request: {}",
            request.head.lines().next().unwrap_or("<empty request>")
        );
    }
}
fn assert_usage(request: &Upstream) {
    assert!(
        request
            .head
            .starts_with("GET /backend-api/wham/usage HTTP/1.1\r\n"),
        "{}",
        request.head
    );
    assert!(
        request
            .head
            .contains("authorization: Bearer synthetic-old-access\r\n")
    );
    assert!(
        request
            .head
            .contains("chatgpt-account-id: synthetic-account\r\n")
    );
    assert!(request.head.contains("originator: codex_cli_rs\r\n"));
    assert!(request.body.is_empty());
    assert!(!request.head.contains("model"));
}

#[tokio::test]
async fn closed_usage_below_limit_releases_waiting_requests_and_logs_one_transition() {
    let (capture, _subscriber) = LogCapture::install("info");
    let mut provider = Fixture::start().await;
    let harness = Harness::start(&provider, usage_config).await;
    let reset = exhaust(&harness, &mut provider).await;
    let first = harness.connect("/v1/responses", b"{}", "").await;
    let usage = provider.next().await;
    assert_usage(&usage);
    let second = harness.connect("/v1/responses", b"{}", "").await;
    tokio::time::timeout(Duration::from_secs(2), async {
        while capture
            .lines()
            .iter()
            .filter(|line| line["message"] == "request admitted")
            .count()
            < 3
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    usage
        .reply
        .send(usage_reply(20, reset, 604800))
        .ok()
        .unwrap();
    for _ in 0..2 {
        let model = provider.next().await;
        assert!(
            model
                .head
                .starts_with("POST /backend-api/codex/responses HTTP/1.1")
        );
        model.reply.send(Reply::json()).ok().unwrap();
    }
    assert_eq!(
        (
            decoded(&read(first).await).0,
            decoded(&read(second).await).0
        ),
        (200, 200)
    );
    let transitions: Vec<_> = capture
        .lines()
        .into_iter()
        .filter(|line| line["message"] == "quota gate changed" && line["reason"] == "below_limit")
        .map(|line| json!([line["reading"], line["closed"]]))
        .collect();
    assert_eq!(transitions, vec![json!(["codex.secondary", false])]);
    let gate = saved(&harness).await;
    assert_eq!(gate["codex.secondary"]["local"]["reading"]["used"], 20.0);
    assert_no_provider_request(&mut provider);
}

#[tokio::test]
async fn queued_before_exhaustion_waits_for_usage_and_obeys_the_result_without_an_extra_read() {
    for used in [20, 100] {
        let mut provider = Fixture::start().await;
        let harness = Harness::start(&provider, usage_config).await;
        let reset =
            (harness.process.inner.passthrough.now() + chrono::Duration::days(1)).timestamp();
        let first = harness.connect("/v1/responses", b"{}", "").await;
        let response = provider.next().await;
        let slot = harness.account().acquire().await.unwrap();
        let model::ModelRequestAdmission::Ready(grant) =
            harness.account().request_admission().await.unwrap()
        else {
            panic!("an open, unpaced account must admit a request");
        };
        let queued = harness
            .connect(
                "/v1/responses",
                b"{}",
                "x-symbiotic-request-id: synthetic-queued\r\n",
            )
            .await;
        waiting_ids(&harness, &["synthetic-queued"]).await;
        response
            .reply
            .send(responses_reply(100, reset))
            .ok()
            .unwrap();
        assert_eq!(decoded(&read(first).await).0, 200);

        let refreshing = harness.connect("/v1/responses", b"{}", "").await;
        let usage = provider.next().await;
        assert_usage(&usage);
        // Release the queued caller while the usage read still holds the refresh lock.
        drop(slot);
        drop(grant);
        assert!(
            tokio::time::timeout(Duration::from_millis(40), queued.readable())
                .await
                .is_err(),
            "queued caller answered before the usage read completed (used={used})"
        );
        assert_no_provider_request(&mut provider);
        usage
            .reply
            .send(usage_reply(used, reset, 604800))
            .ok()
            .unwrap();
        if used < 100 {
            for _ in 0..2 {
                let model = provider.next().await;
                assert!(
                    model
                        .head
                        .starts_with("POST /backend-api/codex/responses HTTP/1.1")
                );
                model.reply.send(Reply::json()).ok().unwrap();
            }
            assert_eq!(
                (
                    decoded(&read(queued).await).0,
                    decoded(&read(refreshing).await).0
                ),
                (200, 200)
            );
        } else {
            assert_quota_refusal(queued).await;
            assert_quota_refusal(refreshing).await;
        }
        assert_no_provider_request(&mut provider);

        advance_clock(Duration::from_secs(200)).await;
        let next = harness.connect("/v1/responses", b"{}", "").await;
        if used < 100 {
            let model = provider.next().await;
            assert!(
                model
                    .head
                    .starts_with("POST /backend-api/codex/responses HTTP/1.1")
            );
            model.reply.send(Reply::json()).ok().unwrap();
            assert_eq!(decoded(&read(next).await).0, 200);
        } else {
            assert_quota_refusal(next).await;
        }
        assert_no_provider_request(&mut provider);
    }
}

#[tokio::test]
async fn closed_usage_exhausted_updates_reset_and_waits_300_seconds_between_reads() {
    let mut provider = Fixture::start().await;
    let harness = Harness::start(&provider, usage_config).await;
    let reset = exhaust(&harness, &mut provider).await + 3600;
    let first = harness.connect("/v1/responses", b"{}", "").await;
    let usage = provider.next().await;
    assert_usage(&usage);
    usage
        .reply
        .send(usage_reply(100, reset, 604800))
        .ok()
        .unwrap();
    assert_quota_refusal(first).await;
    let gate = saved(&harness).await;
    assert_eq!(
        gate["codex.secondary"]["local"]["reading"]["resets_at"],
        json!(DateTime::from_timestamp(reset, 0).unwrap())
    );
    advance_clock(Duration::from_secs(200)).await;
    let second = harness.connect("/v1/responses", b"{}", "").await;
    assert_quota_refusal(second).await;
    assert_no_provider_request(&mut provider);
    advance_clock(Duration::from_secs(161)).await;
    let third = harness.connect("/v1/responses", b"{}", "").await;
    let usage = provider.next().await;
    assert_usage(&usage);
    usage
        .reply
        .send(usage_reply(100, reset, 604800))
        .ok()
        .unwrap();
    assert_quota_refusal(third).await;
    assert_no_provider_request(&mut provider);
}

#[tokio::test]
async fn closed_usage_failure_preserves_observations_and_debounces_the_next_request() {
    for (failure, code) in [
        ("status", "usage_upstream_status"),
        ("timeout", "usage_timeout"),
        ("body", "usage_invalid_response"),
        ("oversized", "usage_response_too_large"),
        ("transport", "usage_transport"),
    ] {
        let (capture, _subscriber) = LogCapture::install("info");
        let mut provider = Fixture::start().await;
        provider.idle_timeout = Some(Duration::from_secs(30));
        let harness = Harness::start(&provider, usage_config).await;
        exhaust(&harness, &mut provider).await;
        let before = saved(&harness).await;
        let caller = harness.connect("/v1/responses", b"{}", "").await;
        let usage = provider.next().await;
        assert_usage(&usage);
        match failure {
            "timeout" => {
                advance_clock(Duration::from_secs(15)).await;
                drop(usage);
            }
            "transport" => drop(usage),
            _ => {
                let reply = match failure {
                    "status" => Reply::status(500),
                    "oversized" => Reply {
                        body: vec![b' '; 65537],
                        ..Reply::json()
                    },
                    _ => Reply {
                        body: b"unreadable provider text".to_vec(),
                        ..Reply::json()
                    },
                };
                usage.reply.send(reply).ok().unwrap();
            }
        }
        assert_quota_refusal(caller).await;
        let second = harness.connect("/v1/responses", b"{}", "").await;
        assert_quota_refusal(second).await;
        assert_no_provider_request(&mut provider);
        assert_eq!(saved(&harness).await, before, "{failure}");
        let failures: Vec<_> = capture
            .lines()
            .into_iter()
            .filter(|line| line["message"] == "provider usage read failed")
            .map(|line| line["code"].clone())
            .collect();
        assert_eq!(failures, vec![json!(code)], "{failure}");
    }
}

#[tokio::test]
async fn open_gate_forwards_without_a_usage_read() {
    let mut provider = Fixture::start().await;
    let harness = Harness::start(&provider, usage_config).await;
    for _ in 0..2 {
        let caller = harness.connect("/v1/responses", b"{}", "").await;
        let upstream = provider.next().await;
        assert!(
            upstream
                .head
                .starts_with("POST /backend-api/codex/responses HTTP/1.1")
        );
        upstream.reply.send(Reply::json()).ok().unwrap();
        assert_eq!(decoded(&read(caller).await).0, 200);
    }
    assert_no_provider_request(&mut provider);
}

#[tokio::test]
async fn usage_observation_survives_reopening_the_records_store() {
    let mut provider = Fixture::start().await;
    let harness = Harness::start(&provider, usage_config).await;
    let reset = exhaust(&harness, &mut provider).await;
    let caller = harness.connect("/v1/responses", b"{}", "").await;
    let usage = provider.next().await;
    assert_usage(&usage);
    usage
        .reply
        .send(usage_reply(25, reset, 604800))
        .ok()
        .unwrap();
    provider
        .next()
        .await
        .reply
        .send(Reply::json())
        .ok()
        .unwrap();
    assert_eq!(decoded(&read(caller).await).0, 200);
    let before = saved(&harness).await;
    let reopened =
        crate::records::Records::open(harness.process.config(), test_clock::clock()).unwrap();
    assert!(!reopened.is_closed("tenant/account").unwrap());
    let restored: Value =
        serde_json::from_str(&reopened.outbox.load_gates().unwrap()[0].1).unwrap();
    assert_eq!(restored, before);
    assert_eq!(
        restored["codex.secondary"]["local"]["reading"]["used"],
        25.0
    );
}

#[tokio::test]
async fn mismatched_usage_window_preserves_the_stored_exhaustion() {
    let mut provider = Fixture::start().await;
    let harness = Harness::start(&provider, usage_config).await;
    let reset = exhaust(&harness, &mut provider).await;
    let before = saved(&harness).await;
    let caller = harness.connect("/v1/responses", b"{}", "").await;
    let usage = provider.next().await;
    assert_usage(&usage);
    usage
        .reply
        .send(usage_reply(0, reset, 604801))
        .ok()
        .unwrap();
    assert_quota_refusal(caller).await;
    assert_eq!(saved(&harness).await, before);
    assert_no_provider_request(&mut provider);
}

#[tokio::test]
async fn usage_destination_refuses_other_origins_and_ports_without_sending_credentials() {
    for destination in [
        "https://other.invalid/responses",
        "https://chatgpt.com:444/responses",
        "https://chatgpt.com.other.invalid/responses",
    ] {
        let (capture, _subscriber) = LogCapture::install("info");
        let mut provider = Fixture::start().await;
        let harness = Harness::start(&provider, |c| {
            usage_config(c);
            c.passthrough.routes[0].destination = destination.into();
        })
        .await;
        exhaust(&harness, &mut provider).await;
        let before = saved(&harness).await;
        let caller = harness.connect("/v1/responses", b"{}", "").await;
        assert_quota_refusal(caller).await;
        assert_no_provider_request(&mut provider);
        assert_eq!(saved(&harness).await, before);
        let failures: Vec<_> = capture
            .lines()
            .into_iter()
            .filter(|line| line["message"] == "provider usage read failed")
            .map(|line| line["code"].clone())
            .collect();
        assert_eq!(failures, vec![json!("usage_destination_invalid")]);
    }
}

#[tokio::test]
async fn usage_windows_replace_only_matching_stored_windows_independently() {
    for primary_seconds in [18000, 604800] {
        let mut provider = Fixture::start().await;
        let harness = Harness::start(&provider, usage_config).await;
        let reset =
            (harness.process.inner.passthrough.now() + chrono::Duration::days(1)).timestamp();
        let caller = harness.connect("/v1/responses", b"{}", "").await;
        let mut reply = responses_reply(100, reset);
        for (name, value) in &mut reply.headers {
            if name == "x-codex-primary-used-percent" {
                *value = "100".into();
            }
            if name == "x-codex-primary-window-minutes" {
                *value = "10080".into();
            }
        }
        provider.next().await.reply.send(reply).ok().unwrap();
        assert_eq!(decoded(&read(caller).await).0, 200);
        let caller = harness.connect("/v1/responses", b"{}", "").await;
        let usage = provider.next().await;
        assert_usage(&usage);
        let mut reply = usage_reply(20, reset + 60, 604800);
        let mut body: Value = serde_json::from_slice(&reply.body).unwrap();
        body["rate_limit"]["primary_window"]["limit_window_seconds"] = json!(primary_seconds);
        reply.body = serde_json::to_vec(&body).unwrap();
        usage.reply.send(reply).ok().unwrap();
        let matches = primary_seconds == 604800;
        if matches {
            provider
                .next()
                .await
                .reply
                .send(Reply::json())
                .ok()
                .unwrap();
        }
        assert_eq!(
            decoded(&read(caller).await).0,
            if matches { 200 } else { 402 }
        );
        let gate = saved(&harness).await;
        assert_eq!(
            gate["codex.primary"]["local"]["reading"]["used"],
            if matches { 10.0 } else { 100.0 }
        );
        assert_eq!(gate["codex.secondary"]["local"]["reading"]["used"], 20.0);
        assert_eq!(gate["codex.secondary"]["local"]["reading"]["limit"], 100.0);
        assert_eq!(
            gate["codex.secondary"]["local"]["reading"]["window_minutes"],
            10080
        );
        assert_eq!(
            gate["codex.secondary"]["local"]["reading"]["resets_at"],
            json!(DateTime::from_timestamp(reset + 60, 0).unwrap())
        );
    }
}
