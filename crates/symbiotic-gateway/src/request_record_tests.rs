//! Public HTTP behaviour against real provider and core sockets.
use super::*;
use tokio::io::{AsyncBufReadExt, BufReader};

struct FakeCore {
    records: mpsc::Receiver<Value>,
    budgets: Arc<Mutex<Vec<Value>>>,
    gets: Arc<std::sync::atomic::AtomicU32>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for FakeCore {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl FakeCore {
    async fn start(path: &std::path::Path, conflict: bool) -> Self {
        let listener = UnixListener::bind(path).unwrap();
        let (send, records) = mpsc::channel(32);
        let budgets = Arc::new(Mutex::new(Vec::<Value>::new()));
        let shared = budgets.clone();
        let gets = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let counts = gets.clone();
        let task = tokio::spawn(async move {
            let mut handlers = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (stream, _) = accepted.unwrap();
                        let send = send.clone();
                        let budgets = shared.clone();
                        let gets = counts.clone();
                        handlers.spawn(async move {
                            let mut stream = BufReader::new(stream);
                            loop {
                                let mut line = String::new();
                                if stream.read_line(&mut line).await.unwrap() == 0 { break; }
                                let call: Value = serde_json::from_str(&line).unwrap();
                                let record = call["call"] == "request.record";
                                let reply = if record && conflict { json!({"error":{"reason":"request_id_conflict","message":"synthetic conflict"}}) }
                                else { json!({"ok":{"duplicate":false,"seq":1,"budgets":budgets.lock().unwrap().clone()}}) };
                                if !record { assert_eq!(call["call"], "budget.get"); gets.fetch_add(1, Ordering::SeqCst); }
                                if stream.get_mut().write_all(format!("{reply}\n").as_bytes()).await.is_err() { break; }
                                if record { send.send(call["args"].clone()).await.unwrap(); }
                            }
                        });
                    }
                    _ = handlers.join_next(), if !handlers.is_empty() => {}
                }
            }
        });
        Self {
            records,
            budgets,
            gets,
            task,
        }
    }
    async fn next(&mut self) -> Value {
        tokio::time::timeout(Duration::from_secs(3), self.records.recv())
            .await
            .unwrap()
            .unwrap()
    }
}
fn responses_reply(used: u32, resets_at: i64) -> Reply {
    let usage = json!({"input_tokens":41,"input_tokens_details":{"cached_tokens":30},"output_tokens":8,"output_tokens_details":{"reasoning_tokens":3},"future_tokens":7});
    Reply { status:200, headers: vec![
        ("content-type".into(), "text/event-stream".into()),
        ("x-codex-secondary-used-percent".into(), used.to_string()),
        ("x-codex-secondary-window-minutes".into(), "10080".into()),
        ("x-codex-secondary-reset-at".into(), resets_at.to_string()),
        ("x-codex-primary-used-percent".into(), "12".into()),
        ("x-codex-primary-window-minutes".into(), "300".into()),
        ("x-other-primary-used-percent".into(), "100".into()),
        ("x-other-primary-window-minutes".into(), "10080".into()),
        ("x-codex-credits-balance".into(), "9809.40".into()),
    ], body: format!("data: {}\n\n", json!({"type":"response.completed","response":{"id":"r","model":"served-model","output":[],"usage":usage}})).into_bytes() }
}
fn budget(allowed: bool, at: DateTime<Utc>, reset: DateTime<Utc>) -> Value {
    json!({"id":"account/codex.secondary","account":"account","name":"codex.secondary","unit":"percent","limit":100.0,"spent":100.0,"warn_at":0.9,"state":"exhausted","over_limit_allowed":allowed,"open_decision_ids":[],"reading":{"name":"codex.secondary","unit":"percent","used":100.0,"limit":100.0,"window_minutes":10080,"read_at":at,"resets_at":reset},"updated":at})
}
async fn empty_outbox(process: &Gateway) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while process
            .inner
            .passthrough
            .records
            .outbox
            .first()
            .await
            .unwrap()
            .is_some()
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn request_records_responses_stream_keeps_both_usage_parts_and_weekly_meter() {
    let dir = tempfile::tempdir().unwrap();
    let mut core = FakeCore::start(&dir.path().join("core.sock"), false).await;
    let mut provider = Fixture::start().await;
    let harness = Harness::start(&provider, |c| {
        c.core_socket_path = dir.path().join("core.sock")
    })
    .await;
    let caller = harness
        .connect(
            "/v1/responses",
            br#"{"model":"requested-model"}"#,
            "x-symbiotic-request-id: audit-day-7\r\n",
        )
        .await;
    let reset = Utc::now() + chrono::Duration::days(1);
    let reply = responses_reply(91, reset.timestamp());
    let raw_headers: HashMap<_, _> = reply
        .headers
        .iter()
        .filter(|(k, _)| ResponsesAdapter::rate_header(k))
        .cloned()
        .collect();
    provider.next().await.reply.send(reply).ok().unwrap();
    assert_eq!(decoded(&read(caller).await).0, 200);
    let record = core.next().await;
    assert_eq!(
        (
            &record["request_id"],
            &record["job_id"],
            &record["account"],
            &record["route"],
            &record["model"],
            &record["status"]
        ),
        (
            &json!("audit-day-7"),
            &json!("audit-day"),
            &json!("account"),
            &json!("responses"),
            &json!("served-model"),
            &json!(200)
        )
    );
    assert_eq!(
        record["usage"]["normalized"],
        json!({"input_tokens":41,"cached_input_tokens":30,"output_tokens":8,"reasoning_tokens":3})
    );
    assert_eq!(
        record["usage"]["provider"],
        json!({"input_tokens":41,"input_tokens_details":{"cached_tokens":30},"output_tokens":8,"output_tokens_details":{"reasoning_tokens":3},"future_tokens":7})
    );
    assert_eq!(
        record["meter"]["provider"],
        serde_json::to_value(raw_headers).unwrap()
    );
    assert_eq!(
        record["meter"]["readings"],
        json!([
            {"name":"codex.primary","unit":"percent","used":12.0,"window_minutes":300},
            {"name":"other.primary","unit":"percent","used":100.0,"window_minutes":10080},
            {"name":"codex.secondary","unit":"percent","used":91.0,"window_minutes":10080,"limit":100.0,"resets_at":DateTime::from_timestamp(reset.timestamp(),0).unwrap()},
            {"name":"codex.credits","unit":"credits","remaining":9809.4}
        ])
    );
    assert!(record.get("refused").is_none());
    assert!(DateTime::parse_from_rfc3339(record["time"].as_str().unwrap()).is_ok());
    empty_outbox(&harness.process).await;
    assert!(core.records.try_recv().is_err());
}

#[tokio::test]
async fn request_records_chat_stream_and_json_keep_unknown_usage_and_absent_fields() {
    for stream in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let mut core = FakeCore::start(&dir.path().join("core.sock"), false).await;
        let mut provider = Fixture::start().await;
        let harness = Harness::start(&provider, |c| {
            chat_config(c);
            c.core_socket_path = dir.path().join("core.sock");
        })
        .await;
        let caller = harness
            .connect(
                "/v1/chat/completions",
                br#"{"model":"requested"}"#,
                "x-symbiotic-request-id: single\r\n",
            )
            .await;
        let raw = json!({"prompt_tokens":7,"completion_tokens":3,"prompt_cache_hit_tokens":2,"future_tokens":7});
        let body = json!({"model":"served-chat","choices":[],"usage":raw});
        provider
            .next()
            .await
            .reply
            .send(Reply {
                status: 200,
                headers: vec![(
                    "content-type".into(),
                    if stream {
                        "text/event-stream"
                    } else {
                        "application/json"
                    }
                    .into(),
                )],
                body: if stream {
                    format!("data: {body}\n\ndata: [DONE]\n\n").into_bytes()
                } else {
                    serde_json::to_vec(&body).unwrap()
                },
            })
            .ok()
            .unwrap();
        assert_eq!(decoded(&read(caller).await).0, 200);
        let record = core.next().await;
        assert_eq!(
            record["usage"],
            json!({"normalized":{"input_tokens":7,"cached_input_tokens":2,"output_tokens":3},"provider":raw})
        );
        assert_eq!(record["model"], "served-chat");
        assert_eq!(record["job_id"], "single");
        empty_outbox(&harness.process).await;
        assert!(core.records.try_recv().is_err());
    }
}

#[tokio::test]
async fn request_records_responses_json_uses_request_model_and_preserves_missing_usage_fields() {
    let mut provider = Fixture::start().await;
    let harness = Harness::start(&provider, |_| {}).await;
    let caller = harness
        .connect("/v1/responses", br#"{"model":"requested"}"#, "")
        .await;
    provider
        .next()
        .await
        .reply
        .send(Reply {
            body: br#"{"usage":{"input_tokens":4,"future_tokens":7}}"#.to_vec(),
            ..Reply::json()
        })
        .ok()
        .unwrap();
    assert_eq!(decoded(&read(caller).await).0, 200);
    let records = request_records(&harness).await;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["model"], "requested");
    assert_eq!(
        records[0]["usage"],
        json!({"normalized":{"input_tokens":4},"provider":{"input_tokens":4,"future_tokens":7}})
    );
    assert!(
        records[0]["request_id"]
            .as_str()
            .unwrap()
            .starts_with("local-")
    );
}

#[tokio::test]
async fn request_records_weekly_exhaustion_refuses_without_upstream_until_core_allows_credits() {
    let dir = tempfile::tempdir().unwrap();
    let mut core = FakeCore::start(&dir.path().join("core.sock"), false).await;
    let mut provider = Fixture::start().await;
    let harness = Harness::start(&provider, |c| {
        c.core_socket_path = dir.path().join("core.sock")
    })
    .await;
    let caller = harness
        .connect("/v1/responses", b"{}", "x-symbiotic-request-id: job-1\r\n")
        .await;
    let reset = Utc::now() + chrono::Duration::days(1);
    provider
        .next()
        .await
        .reply
        .send(responses_reply(100, reset.timestamp()))
        .ok()
        .unwrap();
    assert_eq!(decoded(&read(caller).await).0, 200);
    core.next().await;
    let caller = harness
        .connect("/v1/responses", b"{}", "x-symbiotic-request-id: job-2\r\n")
        .await;
    let (status, _, body) = decoded(&read(caller).await);
    assert_eq!(
        (status, body),
        (402, br#"{"error":{"code":"quota_exhausted"}}"#.to_vec())
    );
    assert!(provider.incoming.try_recv().is_err());
    let refused = core.next().await;
    assert_eq!(
        (
            refused["status"].clone(),
            refused["refused"].clone(),
            refused["usage"].clone()
        ),
        (json!(402), json!("quota_exhausted"), Value::Null)
    );
    *core.budgets.lock().unwrap() = vec![budget(true, Utc::now(), reset)];
    let previous_gets = core.gets.load(Ordering::SeqCst);
    advance_clock(Duration::from_secs(11)).await;
    let caller = harness
        .connect("/v1/responses", b"{}", "x-symbiotic-request-id: job-3\r\n")
        .await;
    provider
        .next()
        .await
        .reply
        .send(responses_reply(100, reset.timestamp()))
        .ok()
        .unwrap();
    assert_eq!(decoded(&read(caller).await).0, 200);
    assert_eq!(core.gets.load(Ordering::SeqCst), previous_gets + 1);
    assert_eq!(core.next().await["request_id"], "job-3");
}

#[tokio::test]
async fn request_records_passed_weekly_reset_opens_the_gate_without_core() {
    let mut provider = Fixture::start().await;
    let harness = Harness::start(&provider, |_| {}).await;
    let now = harness.process.inner.passthrough.now();
    let caller = harness.connect("/v1/responses", b"{}", "").await;
    provider
        .next()
        .await
        .reply
        .send(responses_reply(
            100,
            (now + chrono::Duration::seconds(5)).timestamp(),
        ))
        .ok()
        .unwrap();
    assert_eq!(decoded(&read(caller).await).0, 200);
    advance_clock(Duration::from_secs(6)).await;
    let caller = harness.connect("/v1/responses", b"{}", "").await;
    provider
        .next()
        .await
        .reply
        .send(Reply::json())
        .ok()
        .unwrap();
    assert_eq!(decoded(&read(caller).await).0, 200);
}

#[tokio::test]
async fn request_records_conflict_is_deleted_without_blocking_the_next_row() {
    let dir = tempfile::tempdir().unwrap();
    let mut core = FakeCore::start(&dir.path().join("core.sock"), true).await;
    let mut provider = Fixture::start().await;
    let harness = Harness::start(&provider, |c| {
        c.core_socket_path = dir.path().join("core.sock")
    })
    .await;
    for id in ["job-1", "job-2"] {
        let caller = harness
            .connect(
                "/v1/responses",
                b"{}",
                &format!("x-symbiotic-request-id: {id}\r\n"),
            )
            .await;
        provider
            .next()
            .await
            .reply
            .send(Reply::json())
            .ok()
            .unwrap();
        assert_eq!(decoded(&read(caller).await).0, 200);
        assert_eq!(core.next().await["request_id"], id);
    }
    empty_outbox(&harness.process).await;
}

#[tokio::test]
async fn request_records_deepseek_successful_balance_keeps_allowance_and_poll_time() {
    let mut provider = Fixture::start().await;
    let harness = Harness::start(&provider, |c| {
        balance_config(c);
        c.passthrough.routes[0].balance.as_mut().unwrap().allowance = DecimalAmount::parse("20.50");
    })
    .await;
    provider
        .next()
        .await
        .reply
        .send(balance_reply("12.25", true))
        .ok()
        .unwrap();
    let health = harness
        .health_where(|h| h["routes"][0]["balance"]["total_nano_units"] == 12_250_000_000_u64)
        .await;
    let caller = harness.connect("/v1/chat/completions", b"{}", "").await;
    provider
        .next()
        .await
        .reply
        .send(chat_reply(false, true))
        .ok()
        .unwrap();
    assert_eq!(decoded(&read(caller).await).0, 200);
    let records = request_records(&harness).await;
    assert_eq!(records.len(), 1);
    assert_eq!(
        records[0]["meter"]["readings"],
        json!([{"name":"deepseek.balance","unit":"USD","remaining":12.25,"limit":20.5,"read_at":DateTime::from_timestamp(health["routes"][0]["balance"]["read_at"].as_i64().unwrap(),0).unwrap()}])
    );
}

#[tokio::test]
async fn request_records_core_outage_survives_restart_and_drains_in_order() {
    let mut provider = Fixture::start().await;
    let mut harness = Harness::start(&provider, |_| {}).await;
    for id in ["job-1", "job-2"] {
        let caller = harness
            .connect(
                "/v1/responses",
                b"{}",
                &format!("x-symbiotic-request-id: {id}\r\n"),
            )
            .await;
        provider
            .next()
            .await
            .reply
            .send(Reply::json())
            .ok()
            .unwrap();
        assert_eq!(decoded(&read(caller).await).0, 200);
    }
    assert_eq!(
        request_records(&harness)
            .await
            .iter()
            .map(|r| r["request_id"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["job-1", "job-2"]
    );
    let settings = harness.process.config().clone();
    harness.stop.take().unwrap().send(()).unwrap();
    (&mut harness.task).await.unwrap().unwrap();
    // Drop the old gateway while retaining its directory for a real reopen.
    let other = tempfile::tempdir().unwrap();
    let old = std::mem::replace(&mut harness.process, open(config(other.path())).unwrap());
    drop(old);
    let restarted = open(settings.clone()).unwrap();
    let listener = bind(&restarted).unwrap();
    let (stop, stopped) = oneshot::channel();
    let owner = restarted.clone();
    let task = tokio::spawn(serve_until(
        owner,
        listener,
        async {
            let _ = stopped.await;
            Ok(())
        },
        current_uid(),
    ));
    let mut core = FakeCore::start(&settings.core_socket_path, false).await;
    assert_eq!(core.next().await["request_id"], "job-1");
    assert_eq!(core.next().await["request_id"], "job-2");
    empty_outbox(&restarted).await;
    let conn = rusqlite::Connection::open(settings.state_dir.join("outbox.sqlite")).unwrap();
    assert_eq!(
        conn.pragma_query_value(None, "user_version", |r| r.get::<_, u32>(0))
            .unwrap(),
        1
    );
    assert_eq!(
        conn.pragma_query_value(None, "journal_mode", |r| r.get::<_, String>(0))
            .unwrap(),
        "wal"
    );
    stop.send(()).unwrap();
    task.await.unwrap().unwrap();
}

#[test]
fn request_records_state_configuration_requires_core_path_and_exact_decimal_allowance() {
    let directory = tempfile::tempdir().unwrap();
    let settings = config(directory.path());
    let mut missing = serde_json::to_value(&settings).unwrap();
    missing.as_object_mut().unwrap().remove("core_socket_path");
    assert!(serde_json::from_value::<GatewayConfig>(missing).is_err());
    let mut relative = settings.clone();
    relative.core_socket_path = "relative.sock".into();
    assert_eq!(
        relative.validate_routes(),
        Err(GatewayError::InvalidRequest)
    );
    for retired in ["price", "spend_limit"] {
        let mut value = serde_json::to_value(&settings).unwrap();
        value["passthrough"]["routes"][0][retired] = json!({});
        assert!(serde_json::from_value::<GatewayConfig>(value).is_err());
    }
    let mut settings = settings;
    balance_config(&mut settings);
    let value = serde_json::to_value(&settings).unwrap();
    for amount in [
        json!("-1"),
        json!("1e-3"),
        json!("NaN"),
        json!("1.0000000001"),
        json!("1."),
        json!(".5"),
        json!("9223372037"),
        json!(0.6),
    ] {
        let mut invalid = value.clone();
        invalid["passthrough"]["routes"][0]["balance"]["allowance"] = amount.clone();
        assert!(
            serde_json::from_value::<GatewayConfig>(invalid).is_err(),
            "accepted {amount}"
        );
    }
    let mut valid = value;
    valid["passthrough"]["routes"][0]["balance"]["allowance"] = json!("20.50");
    let parsed: GatewayConfig = serde_json::from_value(valid).unwrap();
    assert_eq!(parsed.validate_routes(), Ok(()));
    assert_eq!(
        parsed.passthrough.routes[0]
            .balance
            .as_ref()
            .unwrap()
            .allowance
            .unwrap()
            .nano_units(),
        20_500_000_000
    );
}

#[tokio::test]
async fn request_records_weekly_gate_rechecks_retries_and_records_one_failed_request() {
    let mut provider = Fixture::start().await;
    let harness = Harness::start(&provider, |_| {}).await;
    let caller = harness
        .connect(
            "/v1/responses",
            b"{}",
            "x-symbiotic-request-id: retry-job-1\r\n",
        )
        .await;
    let mut reply = responses_reply(100, (Utc::now() + chrono::Duration::days(1)).timestamp());
    reply.status = 503;
    reply.body = b"{}".to_vec();
    provider.next().await.reply.send(reply).ok().unwrap();
    let (status, _, body) = decoded(&read(caller).await);
    assert_eq!(
        (status, body),
        (402, br#"{"error":{"code":"quota_exhausted"}}"#.to_vec())
    );
    assert!(provider.incoming.try_recv().is_err());
    let records = request_records(&harness).await;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["request_id"], "retry-job-1");
    assert_eq!(records[0]["status"], 402);
    assert_eq!(records[0]["usage"], Value::Null);
    assert!(records[0].get("refused").is_none());
}
