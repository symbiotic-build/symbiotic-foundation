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
