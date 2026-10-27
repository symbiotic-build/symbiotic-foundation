//! Behaviour every [`QueueBackend`] must share, as reusable async checks.
//!
//! Enable the `conformance` feature in a backend crate's dev-dependencies and
//! expand [`queue_backend_conformance!`](crate::queue_backend_conformance)
//! with a constructor for a fresh, empty backend. Each check becomes one test.
//! The checks use only the trait, so a backend passes by matching the
//! documented semantics, not an implementation.

use crate::{
    ClaimRequest, EnqueueDisposition, EnqueueRequest, FailOutcome, Failure, QueueBackend,
    QueueError, QueueStatus,
};
use chrono::Duration as ChronoDuration;
use std::collections::HashSet;
use std::sync::Arc;
use symbiotic_core::{QueueId, QueueItemId};

/// Expand one `#[tokio::test]` per conformance check.
/// `pause_time = true` pauses only checks that wait for a deadline; the factory
/// must supply a queue clock synchronized with Tokio time.
///
/// ```ignore
/// symbiotic_queue::queue_backend_conformance!(|| std::sync::Arc::new(MyQueue::new()));
/// ```
#[macro_export]
macro_rules! queue_backend_conformance {
    ($make:expr) => {
        $crate::queue_backend_conformance!(@checks $make; []);
    };
    ($make:expr, pause_time = true) => {
        $crate::queue_backend_conformance!(@checks $make; [start_paused = true]);
    };
    (@checks $make:expr; $time_options:tt) => {
        $crate::queue_backend_conformance!(@tests $make;
            enqueue_deduplicates_active_and_terminal_items [],
            enqueue_rejects_empty_kind [],
            enqueue_applies_attempt_and_schedule_defaults [],
            idempotency_is_scoped_to_the_queue [],
            claim_orders_ready_items_and_skips_future_items [],
            claim_respects_max_in_flight [],
            claim_item_claims_only_the_requested_item [],
            claim_item_respects_max_in_flight [],
            claim_item_of_an_unknown_item_is_not_found [],
            claim_rejects_an_empty_worker [],
            concurrent_claims_never_double_lease [],
            lease_owner_and_running_state_are_enforced [],
            heartbeat_extends_the_lease [],
            fail_retries_until_dead_and_complete_clears_the_error [],
            fail_schedules_the_retry_delay [],
            expired_lease_cannot_complete_and_is_reclaimed $time_options,
            an_expired_final_attempt_is_dead_not_claimable $time_options,
            fail_with_records_the_class_and_the_exact_deadline $time_options,
            failure_without_retry_deadline_stops_with_attempts_remaining [],
            enqueue_replacing_supersedes_only_the_current_item [],
            cooldown_only_moves_forward [],
            regression_claims_observe_extended_cooldowns [],
            regression_unrepresentable_leases_are_refused [],
            unknown_item_is_absent [],
        );
    };
    (@tests $make:expr; $($check:ident $options:tt),+ $(,)?) => {
        $(
            $crate::queue_backend_conformance!(@test $make; $options; $check);
        )+
    };
    (@test $make:expr; [$($options:tt)*]; $check:ident) => {
        #[tokio::test($($options)*)]
        async fn $check() {
            let make: fn() -> ::std::sync::Arc<dyn $crate::QueueBackend> = $make;
            $crate::conformance::$check(make()).await;
        }
    };
}

const QUEUE: &str = "chat:conformance:model";

fn queue_id() -> QueueId {
    QueueId::new(QUEUE)
}

fn request(key: &str) -> EnqueueRequest {
    EnqueueRequest {
        queue_id: queue_id(),
        kind: "chat".to_string(),
        payload: serde_json::json!({ "key": key }),
        idempotency_key: Some(key.to_string()),
        run_after: None,
        max_attempts: Some(2),
        force: false,
    }
}

fn claim(worker: &str, limit: usize, max_in_flight: Option<usize>) -> ClaimRequest {
    ClaimRequest {
        queue_id: queue_id(),
        worker_id: worker.to_string(),
        limit,
        lease_seconds: 60,
        max_in_flight,
    }
}

async fn claim_one(queue: &dyn QueueBackend, item_id: &QueueItemId, lease: u64) {
    queue
        .claim_item(item_id, "worker", lease, None)
        .await
        .unwrap()
        .expect("item is claimable");
}

async fn status(queue: &dyn QueueBackend, item_id: &QueueItemId) -> QueueStatus {
    queue.get_item(item_id).await.unwrap().unwrap().status
}

pub async fn enqueue_deduplicates_active_and_terminal_items(queue: Arc<dyn QueueBackend>) {
    let first = queue.enqueue(request("same")).await.unwrap();
    assert_eq!(first.disposition, EnqueueDisposition::Inserted);
    let duplicate = queue.enqueue(request("same")).await.unwrap();
    assert_eq!(duplicate.disposition, EnqueueDisposition::ActiveDuplicate);
    assert_eq!(duplicate.item.item_id, first.item.item_id);

    claim_one(queue.as_ref(), &first.item.item_id, 60).await;
    let running = queue.enqueue(request("same")).await.unwrap();
    assert_eq!(running.disposition, EnqueueDisposition::ActiveDuplicate);
    queue.complete(&first.item.item_id, "worker").await.unwrap();

    let terminal = queue.enqueue(request("same")).await.unwrap();
    assert_eq!(terminal.disposition, EnqueueDisposition::TerminalDuplicate);
    assert_eq!(terminal.item.status, QueueStatus::Succeeded);

    let mut forced = request("same");
    forced.force = true;
    let inserted = queue.enqueue(forced).await.unwrap();
    assert_eq!(inserted.disposition, EnqueueDisposition::Inserted);
    assert_ne!(inserted.item.item_id, first.item.item_id);
    // The forced item is now the one duplicates resolve to.
    let after = queue.enqueue(request("same")).await.unwrap();
    assert_eq!(after.disposition, EnqueueDisposition::ActiveDuplicate);
    assert_eq!(after.item.item_id, inserted.item.item_id);
}

pub async fn enqueue_rejects_empty_kind(queue: Arc<dyn QueueBackend>) {
    let mut empty = request("empty-kind");
    empty.kind = "  ".to_string();
    let err = queue.enqueue(empty).await.unwrap_err();
    assert!(matches!(err, QueueError::InvalidRequest(_)), "{err:?}");
}

pub async fn enqueue_applies_attempt_and_schedule_defaults(queue: Arc<dyn QueueBackend>) {
    let before = queue.now();
    let mut defaulted = request("defaults");
    defaulted.max_attempts = None;
    let item = queue.enqueue(defaulted).await.unwrap().item;
    assert_eq!(item.max_attempts, 3);
    assert_eq!(item.attempt, 0);
    assert_eq!(item.status, QueueStatus::Pending);
    assert!(item.run_after >= before - ChronoDuration::seconds(1));
    assert!(item.run_after <= queue.now());

    let mut zero = request("zero-attempts");
    zero.max_attempts = Some(0);
    assert_eq!(queue.enqueue(zero).await.unwrap().item.max_attempts, 1);
}

pub async fn idempotency_is_scoped_to_the_queue(queue: Arc<dyn QueueBackend>) {
    queue.enqueue(request("shared-key")).await.unwrap();
    let mut other = request("shared-key");
    other.queue_id = QueueId::new("chat:conformance:other");
    let outcome = queue.enqueue(other).await.unwrap();
    assert_eq!(outcome.disposition, EnqueueDisposition::Inserted);
}

pub async fn claim_orders_ready_items_and_skips_future_items(queue: Arc<dyn QueueBackend>) {
    let mut later = request("later");
    later.run_after = Some(queue.now() - ChronoDuration::seconds(5));
    let mut earlier = request("earlier");
    earlier.run_after = Some(queue.now() - ChronoDuration::seconds(10));
    let mut future = request("future");
    future.run_after = Some(queue.now() + ChronoDuration::hours(1));
    let later = queue.enqueue(later).await.unwrap().item;
    let earlier = queue.enqueue(earlier).await.unwrap().item;
    let future = queue.enqueue(future).await.unwrap().item;

    let claimed = queue.claim(claim("worker", 10, None)).await.unwrap();
    let ids: Vec<_> = claimed.iter().map(|item| item.item_id.clone()).collect();
    assert_eq!(ids, vec![earlier.item_id, later.item_id]);
    assert!(claimed.iter().all(|item| {
        item.status == QueueStatus::Running
            && item.attempt == 1
            && item.lease_owner.as_deref() == Some("worker")
            && item.lease_until.is_some()
    }));
    assert_eq!(
        status(queue.as_ref(), &future.item_id).await,
        QueueStatus::Pending
    );
    assert!(
        queue
            .claim_item(&future.item_id, "worker", 60, None)
            .await
            .unwrap()
            .is_none()
    );
}

pub async fn claim_respects_max_in_flight(queue: Arc<dyn QueueBackend>) {
    for idx in 0..3 {
        queue
            .enqueue(request(&format!("capped-{idx}")))
            .await
            .unwrap();
    }
    let first = queue.claim(claim("worker-a", 3, Some(2))).await.unwrap();
    assert_eq!(first.len(), 2);
    let second = queue.claim(claim("worker-b", 1, Some(2))).await.unwrap();
    assert!(second.is_empty());
    queue.complete(&first[0].item_id, "worker-a").await.unwrap();
    let third = queue.claim(claim("worker-b", 3, Some(2))).await.unwrap();
    assert_eq!(third.len(), 1);
}

pub async fn claim_item_claims_only_the_requested_item(queue: Arc<dyn QueueBackend>) {
    let first = queue.enqueue(request("target-a")).await.unwrap().item;
    let second = queue.enqueue(request("target-b")).await.unwrap().item;
    let claimed = queue
        .claim_item(&second.item_id, "worker", 60, Some(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claimed.item_id, second.item_id);
    assert_eq!(claimed.attempt, 1);
    assert_eq!(
        status(queue.as_ref(), &first.item_id).await,
        QueueStatus::Pending
    );
    // A running item is not claimable again.
    assert!(
        queue
            .claim_item(&second.item_id, "other", 60, None)
            .await
            .unwrap()
            .is_none()
    );
}

pub async fn claim_item_respects_max_in_flight(queue: Arc<dyn QueueBackend>) {
    let first = queue.enqueue(request("cap-a")).await.unwrap().item;
    let second = queue.enqueue(request("cap-b")).await.unwrap().item;
    claim_one(queue.as_ref(), &first.item_id, 60).await;
    assert!(
        queue
            .claim_item(&second.item_id, "worker", 60, Some(1))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        queue
            .claim_item(&second.item_id, "worker", 60, Some(2))
            .await
            .unwrap()
            .is_some()
    );
}

pub async fn claim_item_of_an_unknown_item_is_not_found(queue: Arc<dyn QueueBackend>) {
    let err = queue
        .claim_item(&QueueItemId::new(), "worker", 60, None)
        .await
        .unwrap_err();
    assert!(matches!(err, QueueError::NotFound(_)), "{err:?}");
}

pub async fn claim_rejects_an_empty_worker(queue: Arc<dyn QueueBackend>) {
    let item = queue.enqueue(request("no-worker")).await.unwrap().item;
    let err = queue.claim(claim(" ", 1, None)).await.unwrap_err();
    assert!(matches!(err, QueueError::InvalidRequest(_)), "{err:?}");
    let err = queue
        .claim_item(&item.item_id, "", 60, None)
        .await
        .unwrap_err();
    assert!(matches!(err, QueueError::InvalidRequest(_)), "{err:?}");
}

pub async fn concurrent_claims_never_double_lease(queue: Arc<dyn QueueBackend>) {
    for idx in 0..25 {
        queue
            .enqueue(request(&format!("race-{idx}")))
            .await
            .unwrap();
    }
    let handles: Vec<_> = (0..10)
        .map(|idx| {
            let queue = queue.clone();
            tokio::spawn(async move {
                queue
                    .claim(claim(&format!("worker-{idx}"), 3, None))
                    .await
                    .unwrap()
            })
        })
        .collect();
    let mut ids = HashSet::new();
    let mut total = 0;
    for handle in handles {
        for item in handle.await.unwrap() {
            total += 1;
            ids.insert(item.item_id);
        }
    }
    assert_eq!(total, ids.len(), "an item was leased twice");
    assert_eq!(ids.len(), 25);
}

pub async fn lease_owner_and_running_state_are_enforced(queue: Arc<dyn QueueBackend>) {
    let item = queue.enqueue(request("lease")).await.unwrap().item;
    let err = queue.complete(&item.item_id, "worker").await.unwrap_err();
    assert!(matches!(err, QueueError::NotRunning(_)), "{err:?}");

    claim_one(queue.as_ref(), &item.item_id, 60).await;
    let err = queue.complete(&item.item_id, "other").await.unwrap_err();
    assert!(matches!(err, QueueError::LeaseMismatch(_)), "{err:?}");
    let err = queue
        .fail(
            &item.item_id,
            "other",
            symbiotic_core::DiagnosticCode::QueueFailure,
            Some(0),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, QueueError::LeaseMismatch(_)), "{err:?}");
    let err = queue
        .heartbeat(&item.item_id, "other", 60)
        .await
        .unwrap_err();
    assert!(matches!(err, QueueError::LeaseMismatch(_)), "{err:?}");

    let err = queue
        .complete(&QueueItemId::new(), "worker")
        .await
        .unwrap_err();
    assert!(matches!(err, QueueError::NotFound(_)), "{err:?}");
}

pub async fn heartbeat_extends_the_lease(queue: Arc<dyn QueueBackend>) {
    let item = queue.enqueue(request("heartbeat")).await.unwrap().item;
    claim_one(queue.as_ref(), &item.item_id, 5).await;
    let before = queue
        .get_item(&item.item_id)
        .await
        .unwrap()
        .unwrap()
        .lease_until
        .unwrap();
    queue.heartbeat(&item.item_id, "worker", 600).await.unwrap();
    let after = queue.get_item(&item.item_id).await.unwrap().unwrap();
    assert_eq!(after.status, QueueStatus::Running);
    assert_eq!(after.attempt, 1);
    assert!(after.lease_until.unwrap() > before + ChronoDuration::seconds(60));
}

pub async fn fail_retries_until_dead_and_complete_clears_the_error(queue: Arc<dyn QueueBackend>) {
    let item = queue.enqueue(request("retry")).await.unwrap().item;
    claim_one(queue.as_ref(), &item.item_id, 60).await;
    let outcome = queue
        .fail(
            &item.item_id,
            "worker",
            symbiotic_core::DiagnosticCode::QueueFailure,
            Some(0),
        )
        .await
        .unwrap();
    assert_eq!(outcome, FailOutcome::RetryScheduled);
    let failed = queue.get_item(&item.item_id).await.unwrap().unwrap();
    assert_eq!(failed.status, QueueStatus::Failed);
    assert_eq!(
        failed.last_error,
        Some(symbiotic_core::DiagnosticCode::QueueFailure)
    );
    assert!(failed.lease_owner.is_none() && failed.lease_until.is_none());
    // A failed item is still active for deduplication.
    let duplicate = queue.enqueue(request("retry")).await.unwrap();
    assert_eq!(duplicate.disposition, EnqueueDisposition::ActiveDuplicate);

    claim_one(queue.as_ref(), &item.item_id, 60).await;
    let outcome = queue
        .fail(
            &item.item_id,
            "worker",
            symbiotic_core::DiagnosticCode::QueueFailure,
            Some(0),
        )
        .await
        .unwrap();
    assert_eq!(outcome, FailOutcome::MovedToDead);
    let dead = queue.get_item(&item.item_id).await.unwrap().unwrap();
    assert_eq!(dead.status, QueueStatus::Dead);
    assert_eq!(dead.attempt, 2);
    assert_eq!(
        dead.last_error,
        Some(symbiotic_core::DiagnosticCode::QueueFailure)
    );
    let terminal = queue.enqueue(request("retry")).await.unwrap();
    assert_eq!(terminal.disposition, EnqueueDisposition::TerminalDuplicate);

    let recovered = queue.enqueue(request("recovered")).await.unwrap().item;
    claim_one(queue.as_ref(), &recovered.item_id, 60).await;
    queue
        .fail(
            &recovered.item_id,
            "worker",
            symbiotic_core::DiagnosticCode::QueueFailure,
            Some(0),
        )
        .await
        .unwrap();
    claim_one(queue.as_ref(), &recovered.item_id, 60).await;
    queue.complete(&recovered.item_id, "worker").await.unwrap();
    let done = queue.get_item(&recovered.item_id).await.unwrap().unwrap();
    assert_eq!(done.status, QueueStatus::Succeeded);
    assert_eq!(done.last_error, None);
    assert!(done.lease_owner.is_none() && done.lease_until.is_none());
}

pub async fn fail_schedules_the_retry_delay(queue: Arc<dyn QueueBackend>) {
    let item = queue.enqueue(request("delayed")).await.unwrap().item;
    claim_one(queue.as_ref(), &item.item_id, 60).await;
    queue
        .fail(
            &item.item_id,
            "worker",
            symbiotic_core::DiagnosticCode::QueueFailure,
            Some(3_600),
        )
        .await
        .unwrap();
    let failed = queue.get_item(&item.item_id).await.unwrap().unwrap();
    assert!(failed.run_after > queue.now() + ChronoDuration::minutes(59));
    assert!(
        queue
            .claim_item(&item.item_id, "worker", 60, None)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        queue
            .claim(claim("worker", 5, None))
            .await
            .unwrap()
            .is_empty()
    );
}

/// Sleeps just over one second: the shortest lease a backend grants.
pub async fn expired_lease_cannot_complete_and_is_reclaimed(queue: Arc<dyn QueueBackend>) {
    let item = queue.enqueue(request("expired")).await.unwrap().item;
    claim_one(queue.as_ref(), &item.item_id, 1).await;
    tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;

    let err = queue.complete(&item.item_id, "worker").await.unwrap_err();
    assert!(matches!(err, QueueError::LeaseMismatch(_)), "{err:?}");
    let err = queue
        .heartbeat(&item.item_id, "worker", 60)
        .await
        .unwrap_err();
    assert!(matches!(err, QueueError::LeaseMismatch(_)), "{err:?}");

    assert_eq!(queue.reclaim_expired_leases(&queue_id()).await.unwrap(), 1);
    assert_eq!(queue.reclaim_expired_leases(&queue_id()).await.unwrap(), 0);
    let reclaimed = queue.get_item(&item.item_id).await.unwrap().unwrap();
    assert_eq!(reclaimed.status, QueueStatus::Failed);
    assert_eq!(
        reclaimed.last_error,
        Some(symbiotic_core::DiagnosticCode::LeaseExpired)
    );
    assert!(reclaimed.lease_owner.is_none() && reclaimed.lease_until.is_none());

    // Another worker can take it over; the attempt counter keeps counting.
    let retaken = queue
        .claim_item(&item.item_id, "other", 60, Some(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retaken.attempt, 2);
    assert_eq!(retaken.lease_owner.as_deref(), Some("other"));
}

pub async fn cooldown_only_moves_forward(queue: Arc<dyn QueueBackend>) {
    assert_eq!(queue.cooldown_until(&queue_id()).await.unwrap(), None);
    let later = queue.now() + ChronoDuration::seconds(30);
    let earlier = queue.now() + ChronoDuration::seconds(10);
    queue.note_cooldown(&queue_id(), later).await.unwrap();
    queue.note_cooldown(&queue_id(), earlier).await.unwrap();
    let until = queue.cooldown_until(&queue_id()).await.unwrap().unwrap();
    assert!(
        (until - later).num_milliseconds().abs() < 1_000,
        "{until} vs {later}"
    );
    let other = QueueId::new("chat:conformance:other");
    assert_eq!(queue.cooldown_until(&other).await.unwrap(), None);
}

pub async fn unknown_item_is_absent(queue: Arc<dyn QueueBackend>) {
    assert!(queue.get_item(&QueueItemId::new()).await.unwrap().is_none());
}

/// A crash during the last allowed attempt must not buy another attempt:
/// once that lease expires the item is dead, whether a sweep or a claim
/// finds it first. Sleeps just over one second.
pub async fn an_expired_final_attempt_is_dead_not_claimable(queue: Arc<dyn QueueBackend>) {
    let mut swept = request("final-swept");
    swept.max_attempts = Some(1);
    let mut claimed = request("final-claimed");
    claimed.max_attempts = Some(1);
    let swept = queue.enqueue(swept).await.unwrap().item;
    let claimed = queue.enqueue(claimed).await.unwrap().item;
    claim_one(queue.as_ref(), &swept.item_id, 1).await;
    claim_one(queue.as_ref(), &claimed.item_id, 1).await;
    tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;

    // A claim of the expired item reclaims every expired lease of the queue.
    assert!(
        queue
            .claim_item(&claimed.item_id, "restarted", 60, None)
            .await
            .unwrap()
            .is_none()
    );
    for item_id in [&swept.item_id, &claimed.item_id] {
        let item = queue.get_item(item_id).await.unwrap().unwrap();
        assert_eq!(item.status, QueueStatus::Dead, "{item:?}");
        assert_eq!(item.attempt, 1);
        assert!(item.lease_owner.is_none());
    }
    assert!(
        queue
            .claim(claim("restarted", 5, None))
            .await
            .unwrap()
            .is_empty()
    );
    let mut again = request("final-swept");
    again.max_attempts = Some(1);
    let duplicate = queue.enqueue(again).await.unwrap();
    assert_eq!(duplicate.disposition, EnqueueDisposition::TerminalDuplicate);
}

pub async fn fail_with_records_the_class_and_the_exact_deadline(queue: Arc<dyn QueueBackend>) {
    let item = queue.enqueue(request("classified")).await.unwrap().item;
    claim_one(queue.as_ref(), &item.item_id, 60).await;
    let deadline = queue.now() + ChronoDuration::milliseconds(400);
    let outcome = queue
        .fail_with(
            &item.item_id,
            "worker",
            Failure {
                error: symbiotic_core::DiagnosticCode::QueueFailure,
                error_class: Some(symbiotic_core::FailureClass::RateLimited),
                run_after: Some(deadline),
            },
        )
        .await
        .unwrap();
    assert_eq!(outcome, FailOutcome::RetryScheduled);
    let failed = queue.get_item(&item.item_id).await.unwrap().unwrap();
    assert_eq!(
        failed.last_error,
        Some(symbiotic_core::DiagnosticCode::QueueFailure)
    );
    assert_eq!(
        failed.last_error_class,
        Some(symbiotic_core::FailureClass::RateLimited)
    );
    assert!(
        (failed.run_after - deadline).num_milliseconds().abs() < 5,
        "{} vs {deadline}",
        failed.run_after
    );
    assert!(
        queue
            .claim_item(&item.item_id, "worker", 60, None)
            .await
            .unwrap()
            .is_none(),
        "not before the deadline"
    );
    tokio::time::sleep(std::time::Duration::from_millis(450)).await;
    claim_one(queue.as_ref(), &item.item_id, 60).await;
    queue.complete(&item.item_id, "worker").await.unwrap();
    let done = queue.get_item(&item.item_id).await.unwrap().unwrap();
    assert_eq!(done.last_error_class, None);
}

async fn finish(queue: &dyn QueueBackend, item_id: &QueueItemId) {
    claim_one(queue, item_id, 60).await;
    queue.complete(item_id, "worker").await.unwrap();
}

pub async fn enqueue_replacing_supersedes_only_the_current_item(queue: Arc<dyn QueueBackend>) {
    let first = queue.enqueue(request("replace")).await.unwrap().item;
    finish(queue.as_ref(), &first.item_id).await;

    let second = queue
        .enqueue_replacing(request("replace"), &first.item_id)
        .await
        .unwrap();
    assert_eq!(second.disposition, EnqueueDisposition::Inserted);
    let second = second.item;
    assert_ne!(second.item_id, first.item_id);

    // A caller still holding the first item cannot replace the second.
    let stale = queue
        .enqueue_replacing(request("replace"), &first.item_id)
        .await
        .unwrap();
    assert_eq!(stale.disposition, EnqueueDisposition::ActiveDuplicate);
    assert_eq!(stale.item.item_id, second.item_id);
    finish(queue.as_ref(), &second.item_id).await;
    let stale = queue
        .enqueue_replacing(request("replace"), &first.item_id)
        .await
        .unwrap();
    assert_eq!(stale.disposition, EnqueueDisposition::TerminalDuplicate);
    assert_eq!(stale.item.item_id, second.item_id);

    // Two callers holding the current item: exactly one replaces it.
    let racers: Vec<_> = (0..2)
        .map(|_| {
            let queue = queue.clone();
            let current = second.item_id.clone();
            tokio::spawn(async move {
                queue
                    .enqueue_replacing(request("replace"), &current)
                    .await
                    .unwrap()
            })
        })
        .collect();
    let mut inserted = 0;
    let mut ids = HashSet::new();
    for racer in racers {
        let outcome = racer.await.unwrap();
        if outcome.disposition == EnqueueDisposition::Inserted {
            inserted += 1;
        }
        ids.insert(outcome.item.item_id);
    }
    assert_eq!(inserted, 1);
    assert_eq!(ids.len(), 1, "both callers end on the same replacement");
}

/// A terminal refusal must never reenter the claimable work set.
pub async fn failure_without_retry_deadline_stops_with_attempts_remaining(
    queue: Arc<dyn QueueBackend>,
) {
    let item = queue.enqueue(request("stopped")).await.unwrap().item;
    claim_one(queue.as_ref(), &item.item_id, 60).await;
    let outcome = queue
        .fail_with(
            &item.item_id,
            "worker",
            Failure {
                error: symbiotic_core::DiagnosticCode::QueueFailure,
                error_class: Some(symbiotic_core::FailureClass::Queue),
                run_after: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(outcome, FailOutcome::Stopped);
    let stopped = queue.get_item(&item.item_id).await.unwrap().unwrap();
    assert_eq!(stopped.status, QueueStatus::Stopped);
    assert!(stopped.attempt < stopped.max_attempts);
    assert!(
        queue
            .claim_item(&item.item_id, "other", 60, None)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        queue
            .claim(claim("other", 1, None))
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(queue.reclaim_expired_leases(&queue_id()).await.unwrap(), 0);
    let duplicate = queue.enqueue(request("stopped")).await.unwrap();
    assert_eq!(duplicate.disposition, EnqueueDisposition::TerminalDuplicate);
    assert_eq!(duplicate.item.item_id, item.item_id);
    assert_eq!(
        duplicate.item.last_error_class,
        Some(symbiotic_core::FailureClass::Queue)
    );
}

/// A cooldown extension accepted before a claim must keep both claim APIs pending.
pub async fn regression_claims_observe_extended_cooldowns(queue: Arc<dyn QueueBackend>) {
    let item = queue.enqueue(request("cooldown-claim")).await.unwrap().item;
    let now = queue.now();
    queue
        .note_cooldown(&queue_id(), now - ChronoDuration::seconds(1))
        .await
        .unwrap();
    assert!(queue.cooldown_until(&queue_id()).await.unwrap().unwrap() < queue.now());
    // Simulate the extension between a caller's final read and claim acceptance.
    queue
        .note_cooldown(&queue_id(), now + ChronoDuration::seconds(60))
        .await
        .unwrap();
    assert!(
        queue
            .claim_item(&item.item_id, "worker", 60, None)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        queue
            .claim(claim("worker", 1, None))
            .await
            .unwrap()
            .is_empty()
    );
    let item = queue.get_item(&item.item_id).await.unwrap().unwrap();
    assert_eq!(item.status, QueueStatus::Pending);
    assert_eq!(item.attempt, 0);
}

/// Invalid leases must fail without spending an attempt or poisoning the backend.
pub async fn regression_unrepresentable_leases_are_refused(queue: Arc<dyn QueueBackend>) {
    let item = queue.enqueue(request("invalid-lease")).await.unwrap().item;
    for seconds in [u64::MAX, i64::MAX as u64 / 1000] {
        assert!(matches!(
            queue
                .claim_item(&item.item_id, "worker", seconds, None)
                .await,
            Err(QueueError::InvalidRequest(_))
        ));
        let mut request = claim("worker", 1, None);
        request.lease_seconds = seconds;
        assert!(matches!(
            queue.claim(request).await,
            Err(QueueError::InvalidRequest(_))
        ));
    }
    assert_eq!(
        queue
            .get_item(&item.item_id)
            .await
            .unwrap()
            .unwrap()
            .attempt,
        0
    );
    claim_one(queue.as_ref(), &item.item_id, 60).await;
    assert!(matches!(
        queue.heartbeat(&item.item_id, "worker", u64::MAX).await,
        Err(QueueError::InvalidRequest(_))
    ));
    queue.heartbeat(&item.item_id, "worker", 60).await.unwrap();
}
