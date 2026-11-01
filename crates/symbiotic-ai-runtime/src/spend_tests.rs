use crate::spend::SqliteSpendLedger;
use rusqlite::params;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use symbiotic_model::{SpendLedger, SpendReceiptRef, SpendReservation, SpendState};
use symbiotic_trace::UsageTrace;

fn reservation(id: &str) -> SpendReservation {
    SpendReservation {
        reference: SpendReceiptRef::new(id).unwrap(),
        account: "account".into(),
        invocation: "invocation".into(),
        binding: "binding".into(),
        request_limit: None,
    }
}

#[test]
fn zero_retention_is_swept_but_expired_job_deadlines_never_save_answers() {
    let now = chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap();
    for (deadline, saved) in [
        (None, true),
        (Some(now - chrono::Duration::seconds(1)), false),
        (Some(now), false),
        (Some(now + chrono::Duration::seconds(1)), true),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("queue.sqlite");
        symbiotic_queue_sqlite::SqliteQueue::open(&path).unwrap();
        let ledger = SqliteSpendLedger::open(&path)
            .unwrap()
            .with_retention(std::time::Duration::ZERO);
        let r = reservation("zero-retention");
        ledger.reserve_explicit(&r, 1).unwrap();
        let output = Some(serde_json::json!({"answer": "private"}));
        {
            let mut conn = ledger.0.lock().unwrap();
            let tx = conn.transaction().unwrap();
            SqliteSpendLedger::save_recovery_in(
                &tx,
                &r.reference,
                &output,
                ledger.retention(),
                deadline,
                None,
                now,
            )
            .unwrap();
            SqliteSpendLedger::finish_in(&tx, &r.reference, SpendState::Unknown, None, output)
                .unwrap();
            let stored: (Option<String>, Option<String>) = tx
                .query_row(
                    "SELECT recovery,recovery_expires_at FROM spend_receipts WHERE reference=?1",
                    [r.reference.as_str()],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            assert_eq!(stored.0.is_some(), saved, "deadline={deadline:?}");
            assert_eq!(stored.1, saved.then(|| now.to_rfc3339()));
            tx.commit().unwrap();
        }
        let before = ledger.receipt(&r.reference).unwrap().unwrap();
        assert!(before.recovery.is_none());
        assert_eq!(ledger.expire_recovery().unwrap(), usize::from(saved));
        assert_eq!(
            serde_json::to_value(ledger.receipt(&r.reference).unwrap().unwrap()).unwrap(),
            serde_json::to_value(before).unwrap()
        );
    }
}

fn steps(history: usize, explicit: bool, retention: std::time::Duration) -> Vec<usize> {
    let ledger = SqliteSpendLedger::in_memory().with_retention(retention);
    {
        let mut conn = ledger.0.lock().unwrap();
        let tx = conn.transaction().unwrap();
        tx.execute(
            "INSERT INTO spend_accounts(account,used) VALUES ('account',?1)",
            [if explicit { 0 } else { history }],
        )
        .unwrap();
        {
            let mut insert = tx.prepare("INSERT INTO spend_receipts(reference, account, invocation, binding, reservation, state, output, pre_dispatch_released, attempt_limit, attempts_used) VALUES (?1,'account','invocation','binding',?2,?3,?4,?5,?6,0)").unwrap();
            for n in 0..history {
                let r = reservation(&format!("runtime:old-item-{n}:1"));
                insert
                    .execute(params![
                        r.reference.as_str(),
                        serde_json::to_string(&r).unwrap(),
                        if explicit { "released" } else { "settled" },
                        (!explicit).then_some("{\"output_received\":true}"),
                        explicit,
                        explicit.then_some(3)
                    ])
                    .unwrap();
            }
        }
        tx.commit().unwrap();
    }
    let count = Arc::new(AtomicUsize::new(0));
    let c = count.clone();
    ledger.0.lock().unwrap().progress_handler(
        1,
        Some(move || {
            c.fetch_add(1, Ordering::Relaxed);
            false
        }),
    );
    let mut result = Vec::new();
    let r = reservation("runtime:current:1");
    if explicit {
        ledger.reserve_explicit(&r, 3).unwrap();
    } else {
        ledger.reserve(&r).unwrap();
    }
    result.push(count.swap(0, Ordering::Relaxed));
    ledger
        .finish(
            &r.reference,
            SpendState::Settled,
            Some(UsageTrace {
                input_tokens: Some(7),
                ..Default::default()
            }),
            Some(serde_json::json!({"answer":"private"})),
            None,
        )
        .unwrap();
    result.push(count.swap(0, Ordering::Relaxed));
    ledger.invocation("account", "invocation").unwrap();
    result.push(count.swap(0, Ordering::Relaxed));
    ledger.receipt(&r.reference).unwrap();
    result.push(count.swap(0, Ordering::Relaxed));
    // Implicit counting uses only the current item's bounded claim keys.
    for claim in 1..=3 {
        ledger
            .receipt(&SpendReceiptRef::new(format!("runtime:current:{claim}")).unwrap())
            .unwrap();
    }
    result.push(count.swap(0, Ordering::Relaxed));
    let receipt = ledger.receipt(&r.reference).unwrap().unwrap();
    assert!(receipt.output.is_some());
    let available = explicit && !retention.is_zero();
    assert_eq!(receipt.recovery.is_some(), available);
    if available {
        // Advance expiry deterministically without sleeping or changing the
        // production clock; the sweep must actually remove a retained answer.
        ledger.0.lock().unwrap().execute(
            "UPDATE spend_receipts SET recovery_expires_at='2000-01-01T00:00:00+00:00' WHERE reference=?1",
            [r.reference.as_str()],
        ).unwrap();
    }
    count.store(0, Ordering::Relaxed);
    // Zero retention is unreadable immediately, but still leaves an expired row to sweep.
    assert_eq!(ledger.expire_recovery().unwrap(), usize::from(explicit));
    result.push(count.swap(0, Ordering::Relaxed));
    ledger.discard_recovery("account", "invocation").unwrap();
    result.push(count.swap(0, Ordering::Relaxed));
    result
}

#[test]
fn request_and_expiry_work_is_independent_of_retained_history() {
    for retention in [
        std::time::Duration::ZERO,
        std::time::Duration::from_secs(60),
    ] {
        for explicit in [false, true] {
            let small = steps(10, explicit, retention);
            let large = steps(10_000, explicit, retention);
            eprintln!(
                "retention={retention:?}, explicit={explicit}: VM steps at 10 receipts={small:?}, at 10,000={large:?}"
            );
            for (operation, (small, large)) in small.iter().zip(&large).enumerate() {
                assert!(
                    *large <= small + 20,
                    "retention={retention:?}, explicit={explicit}, operation={operation}: {small} -> {large}"
                );
            }
        }
    }
}

#[test]
fn settlement_work_does_not_grow_with_confirmed_purge_history() {
    let mut steps = Vec::new();
    for history in [0, 2048] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("queue.sqlite");
        symbiotic_queue_sqlite::SqliteQueue::open(&path).unwrap();
        let ledger = SqliteSpendLedger::open(&path).unwrap();
        let mut conn = ledger.0.lock().unwrap();
        let tx = conn.transaction().unwrap();
        let job_scope = symbiotic_queue::jobs::JobScope {
            tenant: "tenant".into(),
            incarnation: "1".into(),
            queue: "model".into(),
        };
        let scope = serde_json::to_string(&job_scope).unwrap();
        let identity = symbiotic_core::BindingIdentity::new("tenant", "provider", "1", "account");
        let invocation = crate::spend::job_invocation_key(&job_scope, "unrelated").unwrap();
        {
            let mut insert = tx.prepare("INSERT INTO jobs(scope,id,key,digest,state,final_state,delivery_generation) VALUES (?1,?2,?2,'digest','\"Discarded\"','\"Purged\"',0)").unwrap();
            for n in 0..history {
                insert.execute(params![scope, n.to_string()]).unwrap();
            }
        }
        tx.commit().unwrap();
        drop(conn);
        let reservation = SpendReservation {
            reference: SpendReceiptRef::new("direct").unwrap(),
            account: "account".into(),
            invocation: symbiotic_model::execution_invocation_identity(&identity, &invocation)
                .unwrap(),
            binding: "binding".into(),
            request_limit: None,
        };
        ledger.reserve_explicit(&reservation, 1).unwrap();
        let counter = Arc::new(AtomicUsize::new(0));
        ledger.0.lock().unwrap().progress_handler(
            1,
            Some({
                let counter = counter.clone();
                move || {
                    counter.fetch_add(1, Ordering::Relaxed);
                    false
                }
            }),
        );
        ledger
            .finish(
                &reservation.reference,
                SpendState::Unknown,
                None,
                Some(serde_json::json!({"answer": "retained"})),
                Some(&invocation),
            )
            .unwrap();
        steps.push(counter.load(Ordering::Relaxed));
    }
    assert!(steps[1] <= steps[0] + 100, "settlement VM steps: {steps:?}");
}

mod passthrough_money {
    use crate::passthrough_spend::*;
    use crate::{
        SpendLedger, SpendReceiptRef, SpendReservation, SpendState, spend::SqliteSpendLedger,
    };
    use chrono::{DateTime, Utc};
    use symbiotic_trace::UsageTrace;
    fn rates() -> TokenRates {
        serde_json::from_value(serde_json::json!({"input_cache_miss":"0.15","input_cache_hit":"0.003","output":"0.6","reasoning":"1.2"})).unwrap()
    }
    fn price() -> PassthroughPrice {
        PassthroughPrice {
            rates: rates(),
            peak: vec![],
        }
    }
    fn at(ledger: SqliteSpendLedger, millis: i64) -> SqliteSpendLedger {
        ledger.with_clock(std::sync::Arc::new(move || {
            DateTime::from_timestamp_millis(millis).unwrap()
        }))
    }
    fn reservation(id: &str) -> SpendReservation {
        SpendReservation {
            reference: SpendReceiptRef::new(id).unwrap(),
            account: "account".into(),
            invocation: id.into(),
            binding: "binding".into(),
            request_limit: None,
        }
    }
    fn usage() -> UsageTrace {
        UsageTrace {
            input_tokens: Some(1000),
            cache_hit_tokens: Some(800),
            output_tokens: Some(300),
            reasoning_tokens: Some(100),
            ..Default::default()
        }
    }
    #[test]
    fn passthrough_money_cost_uses_cache_split_and_reasoning_inside_output_and_rounds_up() {
        // 200*.15 + 800*.003 + 200*.6 + 100*1.2 = 272.4 USD per million.
        let expected = 272_400;
        let mut u = usage();
        for split in [0, 1, 2, 3] {
            u.cache_hit_tokens = (split == 0 || split == 2).then_some(800);
            u.cache_miss_tokens = (split == 1 || split == 2).then_some(200);
            assert_eq!(
                rates().cost_nano_usd(&u),
                Some(if split == 3 { 390_000 } else { expected })
            );
        }
        assert_eq!(
            rates().cost_nano_usd(&UsageTrace {
                input_tokens: Some(1),
                output_tokens: Some(0),
                ..Default::default()
            }),
            Some(150)
        );
        // Saturating subtraction must not also discard reported reasoning tokens.
        let mut inconsistent = usage();
        inconsistent.reasoning_tokens = Some(400);
        assert_eq!(rates().cost_nano_usd(&inconsistent), Some(512_400));
        let tiny: TokenRates = serde_json::from_value(serde_json::json!({"input_cache_miss":"0.000000001","input_cache_hit":"0","output":"0"})).unwrap();
        assert_eq!(
            tiny.cost_nano_usd(&UsageTrace {
                input_tokens: Some(1),
                output_tokens: Some(0),
                ..Default::default()
            }),
            Some(1)
        );
        assert_eq!(
            rates().cost_nano_usd(&UsageTrace {
                input_tokens: Some(u64::MAX),
                output_tokens: Some(u64::MAX),
                ..Default::default()
            }),
            Some(i64::MAX as u64)
        );
    }
    #[test]
    fn passthrough_money_peak_rates_use_utc_weekday_and_exclusive_end() {
        let price: PassthroughPrice = serde_json::from_value(serde_json::json!({"rates":{"input_cache_miss":"0.15","input_cache_hit":"0.003","output":"0.6"},"peak":[{"weekdays":[1,2,3,4,5],"start":"01:00","end":"04:00","rates":{"input_cache_miss":"0.3","input_cache_hit":"0.006","output":"1.2"}}]})).unwrap();
        assert!(price.valid());
        for (at, expected) in [
            ("2026-11-02T00:59:59Z", 212_400),
            ("2026-11-02T01:00:00Z", 424_800),
            ("2026-11-02T03:59:59Z", 424_800),
            ("2026-11-02T04:00:00Z", 212_400),
            ("2026-11-01T02:00:00Z", 212_400),
        ] {
            assert_eq!(
                price.rates_at(at.parse().unwrap()).cost_nano_usd(&usage()),
                Some(expected),
                "{at}"
            );
        }
        let mut overlapping = price.clone();
        overlapping.peak.push(overlapping.peak[0].clone());
        assert!(!overlapping.valid());
        for bad in [
            "-1",
            "1e-3",
            "NaN",
            "1.0000000001",
            "1.",
            ".5",
            "9223372037",
        ] {
            assert!(DecimalAmount::parse(bad).is_none(), "{bad}");
        }
    }
    #[test]
    fn passthrough_money_receipt_cost_and_query_keep_unknown_and_unpriced_separate() {
        let mut ledger = SqliteSpendLedger::in_memory();
        for (id, time, request_id, priced, measured) in [
            ("one", 100, "job-1", true, true),
            ("two", 200, "job-2", true, false),
            ("three", 300, "other", false, true),
            ("four", 400, "job-%x", false, false),
        ] {
            let r = reservation(id);
            ledger = at(ledger, time);
            ledger
                .reserve_passthrough(&r, Some(request_id), priced.then_some(&price()), None)
                .unwrap();
            if measured {
                ledger
                    .finish(&r.reference, SpendState::Settled, Some(usage()), None, None)
                    .unwrap();
            }
        }
        let receipt = ledger
            .receipt(&reservation("one").reference)
            .unwrap()
            .unwrap();
        assert_eq!(
            (
                receipt.cost_nano_usd,
                receipt.sent_at_ms,
                receipt.caller_request_id
            ),
            (Some(272_400), Some(100), Some("job-1".into()))
        );
        assert_eq!(
            ledger.cost("account", 0, 500, None, None).unwrap(),
            SpendCost {
                settled_nano_usd: 272_400,
                unknown_usage_count: 2,
                unpriced_count: 2
            }
        );
        assert_eq!(
            ledger.cost("account", 0, 300, None, Some("job-")).unwrap(),
            SpendCost {
                settled_nano_usd: 272_400,
                unknown_usage_count: 1,
                unpriced_count: 0
            }
        );
        assert_eq!(
            ledger.cost("account", 0, 500, Some("three"), None).unwrap(),
            SpendCost {
                settled_nano_usd: 0,
                unknown_usage_count: 0,
                unpriced_count: 1
            }
        );
        assert_eq!(
            ledger.cost("account", 0, 500, None, Some("job-%")).unwrap(),
            SpendCost {
                settled_nano_usd: 0,
                unknown_usage_count: 1,
                unpriced_count: 1
            }
        );
        assert_eq!(
            ledger.cost("foreign", 0, 500, None, None).unwrap(),
            SpendCost::default()
        );
        assert_eq!(
            ledger
                .cost("account", 100, 200, None, None)
                .unwrap()
                .settled_nano_usd,
            272_400
        );
    }
    #[test]
    fn passthrough_money_unknown_holds_are_atomic_across_connections_and_expire() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("queue.sqlite");
        symbiotic_queue_sqlite::SqliteQueue::open(&path).unwrap();
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(8));
        let workers: Vec<_> = (0..8)
            .map(|n| {
                let path = path.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    let ledger = at(SqliteSpendLedger::open(&path).unwrap(), 1000);
                    let limit: SpendLimit = serde_json::from_value(
                        serde_json::json!({"usd":"1","window_seconds":1,"max_request_usd":"1"}),
                    )
                    .unwrap();
                    barrier.wait();
                    ledger
                        .reserve_passthrough(
                            &reservation(&format!("r{n}")),
                            None,
                            Some(&price()),
                            Some(&limit),
                        )
                        .map(|_| true)
                        .unwrap_or_else(|e| {
                            assert_eq!(
                                e.code(),
                                symbiotic_core::DiagnosticCode::SpendBudgetExhausted
                            );
                            false
                        })
                })
            })
            .collect();
        assert_eq!(
            workers
                .into_iter()
                .map(|w| usize::from(w.join().unwrap()))
                .sum::<usize>(),
            1
        );
        let ledger = at(SqliteSpendLedger::open(&path).unwrap(), 2000);
        let limit: SpendLimit = serde_json::from_value(
            serde_json::json!({"usd":"1","window_seconds":1,"max_request_usd":"1"}),
        )
        .unwrap();
        assert!(
            ledger
                .reserve_passthrough(&reservation("expired"), None, Some(&price()), Some(&limit))
                .unwrap()
        );
    }
    #[test]
    fn passthrough_money_settlement_replaces_unknown_hold_with_actual_cost() {
        let ledger = SqliteSpendLedger::in_memory();
        let limit: SpendLimit = serde_json::from_value(
            serde_json::json!({"usd":"0.0011","window_seconds":60,"max_request_usd":"0.0005"}),
        )
        .unwrap();
        for id in ["one", "two"] {
            ledger
                .reserve_passthrough(&reservation(id), None, Some(&price()), Some(&limit))
                .unwrap();
        }
        assert_eq!(
            ledger
                .reserve_passthrough(&reservation("three"), None, Some(&price()), Some(&limit))
                .unwrap_err()
                .code(),
            symbiotic_core::DiagnosticCode::SpendBudgetExhausted
        );
        for id in ["one", "two"] {
            ledger
                .finish(
                    &reservation(id).reference,
                    SpendState::Settled,
                    Some(usage()),
                    None,
                    None,
                )
                .unwrap();
        }
        // Two measured costs (272400 each) plus a 500000 hold fit 1100000.
        assert!(
            ledger
                .reserve_passthrough(&reservation("three"), None, Some(&price()), Some(&limit))
                .unwrap()
        );
    }
    #[test]
    fn passthrough_money_send_price_survives_restart_and_later_off_peak_settlement() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("queue.sqlite");
        symbiotic_queue_sqlite::SqliteQueue::open(&path).unwrap();
        let sent: DateTime<Utc> = "2026-11-02T02:00:00Z".parse().unwrap();
        let ledger = at(
            SqliteSpendLedger::open(&path).unwrap(),
            sent.timestamp_millis(),
        );
        let schedule: PassthroughPrice = serde_json::from_value(serde_json::json!({"rates":{"input_cache_miss":"0.15","input_cache_hit":"0.003","output":"0.6"},"peak":[{"weekdays":[1,2,3,4,5],"start":"01:00","end":"04:00","rates":{"input_cache_miss":"0.3","input_cache_hit":"0.006","output":"1.2"}}]})).unwrap();
        let r = reservation("peak");
        ledger
            .reserve_passthrough(&r, Some("job-1"), Some(&schedule), None)
            .unwrap();
        drop(ledger);
        let later: DateTime<Utc> = "2026-11-02T05:00:00Z".parse().unwrap();
        let ledger = at(
            SqliteSpendLedger::open(&path).unwrap(),
            later.timestamp_millis(),
        );
        ledger
            .finish(&r.reference, SpendState::Settled, Some(usage()), None, None)
            .unwrap();
        let receipt = ledger.receipt(&r.reference).unwrap().unwrap();
        assert_eq!(
            (receipt.cost_nano_usd, receipt.sent_at_ms),
            (Some(424800), Some(sent.timestamp_millis()))
        );
    }
    #[test]
    fn passthrough_money_previous_schema_is_refused_without_migration() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("queue.sqlite");
        symbiotic_queue_sqlite::SqliteQueue::open(&path).unwrap();
        let conn = rusqlite::Connection::open(&path).unwrap();
        let version = symbiotic_queue_sqlite::QUEUE_SCHEMA_VERSION - 1;
        conn.pragma_update(None, "user_version", version).unwrap();
        let error = match SqliteSpendLedger::open(&path) {
            Ok(_) => panic!("accepted previous queue schema"),
            Err(error) => error,
        };
        assert_eq!(
            error.code(),
            symbiotic_core::DiagnosticCode::UnsupportedQueueSchema
        );
        assert_eq!(
            conn.pragma_query_value(None, "user_version", |row| row.get::<_, u32>(0))
                .unwrap(),
            version
        );
    }
}
