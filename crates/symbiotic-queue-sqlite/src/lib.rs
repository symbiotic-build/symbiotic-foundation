//! Local SQLite backend for the `symbiotic-queue` contracts.
//!
//! A separate crate, so a graph that names only the queue contracts (traces,
//! queue-bound model providers, hosts with their own queue) never contains
//! SQLite, whatever features other crates in the same build enable.

pub mod jobs;

use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use rusqlite::{Connection, OptionalExtension, params};
use std::path::Path;
use std::sync::{Arc, Mutex};
use symbiotic_core::{QueueId, QueueItemId};
use symbiotic_queue::{
    ClaimRequest, EnqueueDisposition, EnqueueOutcome, EnqueueRequest, FailOutcome, Failure,
    QueueBackend, QueueError, QueueEvent, QueueEventSink, QueueItem, QueueStatus,
};

/// Status as stored in the SQLite `status` column.
fn status_str(status: QueueStatus) -> &'static str {
    match status {
        QueueStatus::Pending => "pending",
        QueueStatus::Running => "running",
        QueueStatus::Succeeded => "succeeded",
        QueueStatus::Failed => "failed",
        QueueStatus::Dead => "dead",
        QueueStatus::Stopped => "stopped",
    }
}

fn parse_status(value: &str) -> Result<QueueStatus, QueueError> {
    match value {
        "pending" => Ok(QueueStatus::Pending),
        "running" => Ok(QueueStatus::Running),
        "succeeded" => Ok(QueueStatus::Succeeded),
        "failed" => Ok(QueueStatus::Failed),
        "dead" => Ok(QueueStatus::Dead),
        "stopped" => Ok(QueueStatus::Stopped),
        _other => Err(QueueError::Storage(
            symbiotic_core::DiagnosticCode::StorageFailure,
        )),
    }
}

#[derive(Clone)]
pub struct SqliteQueue {
    conn: Arc<Mutex<Connection>>,
    event_sink: Option<Arc<dyn QueueEventSink>>,
    clock: symbiotic_queue::jobs::JobClock,
}

impl SqliteQueue {
    // Lock acquisition and all SQLite work stay on the blocking pool. Callers
    // await the committed result before delivering events on the async runtime.
    async fn with_connection<T: Send + 'static>(
        &self,
        operation: impl FnOnce(
            &mut Connection,
            &symbiotic_queue::jobs::JobClock,
        ) -> Result<T, QueueError>
        + Send
        + 'static,
    ) -> Result<T, QueueError> {
        let conn = self.conn.clone();
        let clock = self.clock.clone();
        tokio::task::spawn_blocking(move || {
            let mut conn = conn.lock().map_err(lock_error)?;
            operation(&mut conn, &clock)
        })
        .await
        .map_err(storage_error)?
    }

    /// Initialize the current queue/job format on a caller-owned ledger connection.
    /// Unknown formats are refused; pre-release state has no migrations.
    pub fn initialize_connection(conn: &mut Connection) -> Result<(), QueueError> {
        configure(conn)
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self, QueueError> {
        if let Some(parent) = path.as_ref().parent() {
            std::fs::create_dir_all(parent).map_err(storage_error)?;
        }
        let mut conn = Connection::open(path).map_err(storage_error)?;
        configure(&mut conn)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            event_sink: None,
            clock: Arc::new(Utc::now),
        })
    }

    pub fn in_memory() -> Result<Self, QueueError> {
        let mut conn = Connection::open_in_memory().map_err(storage_error)?;
        configure(&mut conn)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            event_sink: None,
            clock: Arc::new(Utc::now),
        })
    }

    /// Use a trusted host clock for queue deadlines and events.
    pub fn with_clock(mut self, clock: symbiotic_queue::jobs::JobClock) -> Self {
        self.clock = clock;
        self
    }

    pub fn with_event_sink(mut self, sink: Arc<dyn QueueEventSink>) -> Self {
        self.event_sink = Some(sink);
        self
    }

    pub fn get(&self, item_id: &QueueItemId) -> Result<Option<QueueItem>, QueueError> {
        let conn = self.conn.lock().map_err(lock_error)?;
        let mut stmt = conn
            .prepare(
                "select item_id, queue_id, kind, payload_json, status, attempt, max_attempts,
                        run_after, lease_owner, lease_until, idempotency_key, last_error,
                        created_at, updated_at, last_error_class
                 from queue_items where item_id = ?1",
            )
            .map_err(storage_error)?;
        stmt.query_row(params![item_id.0], row_to_item)
            .optional()
            .map_err(storage_error)
    }

    pub async fn mark_stale_active_dead(
        &self,
        queue_id: &QueueId,
        stale_before: DateTime<Utc>,
        reason: symbiotic_core::DiagnosticCode,
    ) -> Result<usize, QueueError> {
        self.mark_stale_active_dead_inner(Some(queue_id), stale_before, reason)
            .await
    }

    pub async fn mark_all_stale_active_dead(
        &self,
        stale_before: DateTime<Utc>,
        reason: symbiotic_core::DiagnosticCode,
    ) -> Result<usize, QueueError> {
        self.mark_stale_active_dead_inner(None, stale_before, reason)
            .await
    }

    async fn mark_stale_active_dead_inner(
        &self,
        queue_id: Option<&QueueId>,
        stale_before: DateTime<Utc>,
        reason: symbiotic_core::DiagnosticCode,
    ) -> Result<usize, QueueError> {
        let now = self.now();
        let queue_id = queue_id.cloned();
        let (updated, items) = self
            .with_connection(move |conn, _clock| {
                let tx = conn
                    .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                    .map_err(storage_error)?;
                let items = stale_active_items_in_tx(&tx, queue_id.as_ref(), stale_before, now)?;
                let updated = match queue_id.as_ref() {
                    Some(queue_id) => tx
                        .execute(
                            "update queue_items
                         set status = 'dead',
                             lease_owner = null,
                             lease_until = null,
                             last_error = ?3,
                             updated_at = ?4
                         where queue_id = ?1
                           and status in ('pending', 'failed', 'running')
                           and updated_at < ?2
                           and (status != 'running' or lease_until is null or lease_until < ?4)",
                            params![queue_id.0, ts(stale_before), reason.code(), ts(now)],
                        )
                        .map_err(storage_error)?,
                    None => tx
                        .execute(
                            "update queue_items
                         set status = 'dead',
                             lease_owner = null,
                             lease_until = null,
                             last_error = ?2,
                             updated_at = ?3
                         where status in ('pending', 'failed', 'running')
                           and updated_at < ?1
                           and (status != 'running' or lease_until is null or lease_until < ?3)",
                            params![ts(stale_before), reason.code(), ts(now)],
                        )
                        .map_err(storage_error)?,
                };
                tx.commit().map_err(storage_error)?;
                Ok((updated, items))
            })
            .await?;
        if let Some(sink) = &self.event_sink {
            for mut item in items {
                item.status = QueueStatus::Dead;
                item.lease_owner = None;
                item.lease_until = None;
                item.last_error = Some(reason);
                item.updated_at = now;
                sink.record_queue_event(QueueEvent {
                    item_id: item.item_id,
                    queue_id: item.queue_id,
                    kind: item.kind,
                    status: item.status,
                    attempt: item.attempt,
                    timestamp: self.now(),
                    error: Some(reason),
                })
                .await;
            }
        }
        Ok(updated)
    }

    /// Mark active items of every queue that have not changed since
    /// `stale_before` (and hold no live lease) dead with `reason`, without
    /// notifying the event sink. For startup and maintenance sweeps outside
    /// an async context. Returns the number of items marked.
    pub fn retire_stale_active(
        &self,
        stale_before: DateTime<Utc>,
        reason: symbiotic_core::DiagnosticCode,
    ) -> Result<usize, QueueError> {
        let now = self.now();
        let mut conn = self.conn.lock().map_err(lock_error)?;
        let tx = conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        let updated = tx
            .execute(
                "update queue_items
                 set status = 'dead',
                     lease_owner = null,
                     lease_until = null,
                     last_error = ?2,
                     updated_at = ?3
                 where status in ('pending', 'failed', 'running')
                   and updated_at < ?1
                   and (status != 'running' or lease_until is null or lease_until < ?3)",
                params![ts(stale_before), reason.code(), ts(now)],
            )
            .map_err(storage_error)?;
        tx.commit().map_err(storage_error)?;
        Ok(updated)
    }

    /// Delete terminal items (succeeded, dead or stopped) last updated before
    /// `before`. Active items are kept
    /// whatever their age. Returns the number of items deleted.
    ///
    /// A deleted terminal item no longer deduplicates its idempotency key:
    /// the next request with that key is inserted afresh.
    pub fn prune_terminal_before(&self, before: DateTime<Utc>) -> Result<usize, QueueError> {
        let mut conn = self.conn.lock().map_err(lock_error)?;
        let tx = conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        let deleted = tx
            .execute(
                "delete from queue_items
                 where status in ('succeeded', 'dead', 'stopped') and updated_at < ?1",
                params![ts(before)],
            )
            .map_err(storage_error)?;
        tx.commit().map_err(storage_error)?;
        Ok(deleted)
    }

    /// Enqueue; with `replacing`, a force enqueue happens only while that
    /// item is still the newest for the key. That check takes the write lock
    /// first (an immediate transaction), so it holds across connections.
    async fn enqueue_inner(
        &self,
        request: EnqueueRequest,
        replacing: Option<&QueueItemId>,
    ) -> Result<EnqueueOutcome, QueueError> {
        if request.kind.trim().is_empty() {
            return Err(QueueError::InvalidRequest(
                symbiotic_core::DiagnosticCode::QueueItemKindMustNotBeEmpty,
            ));
        }
        let now = self.now();
        let run_after = request.run_after.unwrap_or(now);
        let max_attempts = request.max_attempts.unwrap_or(3).max(1);
        let payload = serde_json::to_string(&request.payload).map_err(storage_error)?;

        let replacing = replacing.cloned();
        let outcome = self.with_connection(move |conn, _clock| {
            // Acquire the writer lock before reading deduplication state;
            // a concurrent ledger writer must not cause a read-to-write upgrade.
            let tx = conn
                .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                .map_err(storage_error)?;
            if let Some(key) = &request.idempotency_key {
                let existing = find_by_idempotency(&tx, &request.queue_id, key)?;
                if let Some(existing) = existing {
                    let active = matches!(
                        existing.status,
                        QueueStatus::Pending | QueueStatus::Running | QueueStatus::Failed
                    );
                    let terminal = matches!(
                        existing.status,
                        QueueStatus::Succeeded | QueueStatus::Dead | QueueStatus::Stopped
                    );
                    let superseded = replacing.as_ref().is_some_and(|current| existing.item_id != *current);
                    let reclaimed = replacing.is_some()
                        && existing.status == QueueStatus::Failed
                        && existing.last_error
                            == Some(symbiotic_core::DiagnosticCode::LeaseExpired);
                    if (active && !reclaimed) || (terminal && !request.force) || superseded {
                        tx.commit().map_err(storage_error)?;
                        let disposition = if active {
                            EnqueueDisposition::ActiveDuplicate
                        } else {
                            EnqueueDisposition::TerminalDuplicate
                        };
                        return Ok(EnqueueOutcome {
                            item: existing,
                            disposition,
                        });
                    }
                }
            }

            if let Some(current) = &replacing {
                tx.execute("UPDATE queue_items SET status='stopped', last_error_class='queue' WHERE item_id=?1 AND status='failed'", [&current.0]).map_err(storage_error)?;
            }
            let item = QueueItem {
                item_id: QueueItemId::new(),
                queue_id: request.queue_id,
                kind: request.kind,
                payload: request.payload,
                status: QueueStatus::Pending,
                attempt: 0,
                max_attempts,
                run_after,
                lease_owner: None,
                lease_until: None,
                idempotency_key: request.idempotency_key,
                last_error: None,
                last_error_class: None,
                created_at: now,
                updated_at: now,
            };
            if let Err(err) = tx.execute(
                "insert into queue_items
                 (item_id, queue_id, kind, payload_json, status, attempt, max_attempts,
                  run_after, lease_owner, lease_until, idempotency_key, last_error, created_at, updated_at)
                 values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, null, null, ?9, null, ?10, ?11)",
                params![
                    item.item_id.0,
                    item.queue_id.0,
                    item.kind,
                    payload,
                    status_str(item.status),
                    item.attempt,
                    item.max_attempts,
                    ts(item.run_after),
                    item.idempotency_key,
                    ts(item.created_at),
                    ts(item.updated_at),
                ],
            ) {
                if let Some(key) = &item.idempotency_key
                    && is_unique_constraint(&err)
                    && let Some(existing) = find_by_idempotency(&tx, &item.queue_id, key)?
                {
                    tx.commit().map_err(storage_error)?;
                    return Ok(EnqueueOutcome {
                        item: existing,
                        disposition: EnqueueDisposition::ActiveDuplicate,
                    });
                }
                return Err(storage_error(err));
            }
            tx.commit().map_err(storage_error)?;
            Ok(EnqueueOutcome { item, disposition: EnqueueDisposition::Inserted })
        }).await?;

        if outcome.disposition == EnqueueDisposition::Inserted {
            self.emit(outcome.item.clone(), None).await;
        }
        Ok(outcome)
    }

    async fn emit(&self, item: QueueItem, error: Option<symbiotic_core::DiagnosticCode>) {
        if let Some(sink) = &self.event_sink {
            sink.record_queue_event(QueueEvent {
                item_id: item.item_id,
                queue_id: item.queue_id,
                kind: item.kind,
                status: item.status,
                attempt: item.attempt,
                timestamp: self.now(),
                error,
            })
            .await;
        }
    }
}

#[async_trait]
impl QueueBackend for SqliteQueue {
    fn now(&self) -> DateTime<Utc> {
        (self.clock)()
    }

    async fn jobs(
        &self,
        scope: &symbiotic_queue::jobs::JobScope,
        config: &symbiotic_queue::jobs::JobConfig,
        clock: symbiotic_queue::jobs::JobClock,
        request: symbiotic_queue::jobs::JobRequest,
    ) -> Result<symbiotic_queue::jobs::JobResponse, symbiotic_queue::jobs::JobError> {
        let (backend, scope, config) = (self.clone(), scope.clone(), config.clone());
        tokio::task::spawn_blocking(move || {
            backend.job_operation(&scope, &config, || clock(), request)
        })
        .await
        .map_err(|_| symbiotic_queue::jobs::JobError::Storage)?
    }
    async fn enqueue(&self, request: EnqueueRequest) -> Result<EnqueueOutcome, QueueError> {
        self.enqueue_inner(request, None).await
    }

    async fn enqueue_replacing(
        &self,
        mut request: EnqueueRequest,
        current: &QueueItemId,
    ) -> Result<EnqueueOutcome, QueueError> {
        request.force = true;
        self.enqueue_inner(request, Some(current)).await
    }

    async fn claim(&self, request: ClaimRequest) -> Result<Vec<QueueItem>, QueueError> {
        if request.worker_id.trim().is_empty() {
            return Err(QueueError::InvalidRequest(
                symbiotic_core::DiagnosticCode::WorkerIdMustNotBeEmpty,
            ));
        }
        let now = self.now();
        let lease_until = lease_deadline(now, request.lease_seconds)?;
        let mut limit = request.limit.max(1);
        let claimed = self
            .with_connection(move |conn, _clock| {
                let tx = conn
                    .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                    .map_err(storage_error)?;
                if cooldown_active(&tx, &request.queue_id, _clock)? {
                    return Ok(Vec::new());
                }
                reclaim_expired_in_tx(&tx, &request.queue_id, now)?;
                if let Some(max_in_flight) = request.max_in_flight {
                    let running: i64 = tx
                        .query_row(
                            "select count(*) from queue_items
                         where queue_id = ?1 and status = 'running'",
                            params![request.queue_id.0],
                            |row| row.get(0),
                        )
                        .map_err(storage_error)?;
                    if running >= max_in_flight as i64 {
                        tx.commit().map_err(storage_error)?;
                        return Ok(Vec::new());
                    }
                    limit = limit.min(max_in_flight.saturating_sub(running as usize));
                }
                let ids = {
                    let mut stmt = tx
                        .prepare(
                            "select item_id from queue_items
                         where queue_id = ?1
                           and status in ('pending', 'failed')
                           and attempt < max_attempts
                           and run_after <= ?2
                         order by run_after asc, created_at asc
                         limit ?3",
                        )
                        .map_err(storage_error)?;
                    stmt.query_map(params![request.queue_id.0, ts(now), limit as i64], |row| {
                        row.get::<_, String>(0)
                    })
                    .map_err(storage_error)?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(storage_error)?
                };
                let mut out = Vec::new();
                for id in ids {
                    let updated = tx
                        .execute(
                            "update queue_items
                     set status = 'running',
                         attempt = attempt + 1,
                         lease_owner = ?2,
                         lease_until = ?3,
                         updated_at = ?4
                    where item_id = ?1
                       and status in ('pending', 'failed')
                       and attempt < max_attempts",
                            params![id, request.worker_id, ts(lease_until), ts(now)],
                        )
                        .map_err(storage_error)?;
                    if updated == 1
                        && let Some(item) = get_in_tx(&tx, &QueueItemId(id))?
                    {
                        out.push(item);
                    }
                }
                tx.commit().map_err(storage_error)?;
                Ok(out)
            })
            .await?;
        for item in claimed.clone() {
            self.emit(item, None).await;
        }
        Ok(claimed)
    }

    async fn claim_item(
        &self,
        item_id: &QueueItemId,
        worker_id: &str,
        lease_seconds: u64,
        max_in_flight: Option<usize>,
    ) -> Result<Option<QueueItem>, QueueError> {
        if worker_id.trim().is_empty() {
            return Err(QueueError::InvalidRequest(
                symbiotic_core::DiagnosticCode::WorkerIdMustNotBeEmpty,
            ));
        }
        let now = self.now();
        let lease_until = lease_deadline(now, lease_seconds)?;
        let item_id = item_id.clone();
        let worker_id = worker_id.to_owned();
        let claimed = self
            .with_connection(move |conn, _clock| {
                let (item_id, worker_id) = (&item_id, worker_id.as_str());
                let tx = conn
                    .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                    .map_err(storage_error)?;
                let item = get_required(&tx, item_id)?;
                if cooldown_active(&tx, &item.queue_id, _clock)? {
                    return Ok(None);
                }
                reclaim_expired_in_tx(&tx, &item.queue_id, now)?;
                if let Some(max_in_flight) = max_in_flight {
                    let running: i64 = tx
                        .query_row(
                            "select count(*) from queue_items
                         where queue_id = ?1 and status = 'running'",
                            params![item.queue_id.0],
                            |row| row.get(0),
                        )
                        .map_err(storage_error)?;
                    if running >= max_in_flight as i64 {
                        tx.commit().map_err(storage_error)?;
                        return Ok(None);
                    }
                }
                let refreshed = get_required(&tx, item_id)?;
                if matches!(refreshed.status, QueueStatus::Pending | QueueStatus::Failed)
                    && refreshed.attempt >= refreshed.max_attempts
                {
                    // Every allowed attempt is used: the item is dead, not
                    // claimable (items reclaimed before this rule existed).
                    tx.execute(
                        "update queue_items
                     set status = 'dead',
                         last_error = coalesce(last_error, 'attempt_budget_exhausted'),
                         updated_at = ?2
                     where item_id = ?1",
                        params![item_id.0, ts(now)],
                    )
                    .map_err(storage_error)?;
                    tx.commit().map_err(storage_error)?;
                    return Ok(None);
                }
                if !matches!(refreshed.status, QueueStatus::Pending | QueueStatus::Failed)
                    || refreshed.run_after > now
                {
                    tx.commit().map_err(storage_error)?;
                    return Ok(None);
                }
                let updated = tx
                    .execute(
                        "update queue_items
                 set status = 'running',
                     attempt = attempt + 1,
                     lease_owner = ?2,
                     lease_until = ?3,
                     updated_at = ?4
                where item_id = ?1
                   and status in ('pending', 'failed')",
                        params![item_id.0, worker_id, ts(lease_until), ts(now)],
                    )
                    .map_err(storage_error)?;
                if updated != 1 {
                    tx.commit().map_err(storage_error)?;
                    return Ok(None);
                }
                let claimed = get_required(&tx, item_id)?;
                tx.commit().map_err(storage_error)?;
                Ok(Some(claimed))
            })
            .await?;
        if let Some(item) = claimed.clone() {
            self.emit(item, None).await;
        }
        Ok(claimed)
    }

    async fn get_item(&self, item_id: &QueueItemId) -> Result<Option<QueueItem>, QueueError> {
        let item_id = item_id.clone();
        self.with_connection(move |conn, _clock| get_in_tx(conn, &item_id))
            .await
    }

    async fn heartbeat(
        &self,
        item_id: &QueueItemId,
        worker_id: &str,
        lease_seconds: u64,
    ) -> Result<(), QueueError> {
        // Reject invalid durations before waiting for a connection.
        lease_deadline(self.now(), lease_seconds)?;
        let item_id = item_id.clone();
        let worker_id = worker_id.to_owned();
        let item = self
            .with_connection(move |conn, _clock| {
                let (item_id, worker_id) = (&item_id, worker_id.as_str());
                update_running_item(conn, item_id, worker_id, _clock, |conn| {
                    // Renewal deadlines follow write-transaction order, not
                    // the order in which async callers reached the blocking pool.
                    let now = _clock();
                    let lease_until = lease_deadline(now, lease_seconds)?;
                    conn.execute(
                "update queue_items set lease_until = ?2, updated_at = ?3 where item_id = ?1",
                params![item_id.0, ts(lease_until), ts(now)],
            )
            .map_err(storage_error)?;
                    get_required(conn, item_id)
                })
            })
            .await?;
        self.emit(item, None).await;
        Ok(())
    }

    async fn complete(&self, item_id: &QueueItemId, worker_id: &str) -> Result<(), QueueError> {
        let now = self.now();
        let item_id = item_id.clone();
        let worker_id = worker_id.to_owned();
        let item = self
            .with_connection(move |conn, _clock| {
                let (item_id, worker_id) = (&item_id, worker_id.as_str());
                update_running_item(conn, item_id, worker_id, _clock, |conn| {
                    conn.execute(
                        "update queue_items
                 set status = 'succeeded',
                     lease_owner = null,
                     lease_until = null,
                     last_error = null,
                     last_error_class = null,
                     updated_at = ?2
                 where item_id = ?1",
                        params![item_id.0, ts(now)],
                    )
                    .map_err(storage_error)?;
                    get_required(conn, item_id)
                })
            })
            .await?;
        self.emit(item, None).await;
        Ok(())
    }

    async fn fail(
        &self,
        item_id: &QueueItemId,
        worker_id: &str,
        error: symbiotic_core::DiagnosticCode,
        retry_after_seconds: Option<u64>,
    ) -> Result<FailOutcome, QueueError> {
        let run_after =
            self.now() + ChronoDuration::seconds(retry_after_seconds.unwrap_or(1) as i64);
        self.fail_with(
            item_id,
            worker_id,
            Failure {
                error,
                error_class: None,
                run_after: Some(run_after),
            },
        )
        .await
    }

    async fn fail_with(
        &self,
        item_id: &QueueItemId,
        worker_id: &str,
        failure: Failure,
    ) -> Result<FailOutcome, QueueError> {
        let now = self.now();
        let run_after = failure
            .run_after
            .unwrap_or_else(|| now + ChronoDuration::seconds(1));
        let item_id = item_id.clone();
        let worker_id = worker_id.to_owned();
        let (item, outcome) = self
            .with_connection(move |conn, _clock| {
                let (item_id, worker_id) = (&item_id, worker_id.as_str());
                update_running_item(conn, item_id, worker_id, _clock, |conn| {
                    let item = get_required(conn, item_id)?;
                    let exhausted = item.attempt >= item.max_attempts;
                    let status = if failure.run_after.is_none() {
                        QueueStatus::Stopped
                    } else if exhausted {
                        QueueStatus::Dead
                    } else {
                        QueueStatus::Failed
                    };
                    conn.execute(
                        "update queue_items
                 set status = ?2,
                     run_after = ?3,
                     lease_owner = null,
                     lease_until = null,
                     last_error = ?4,
                     last_error_class = ?5,
                     updated_at = ?6
                 where item_id = ?1",
                        params![
                            item_id.0,
                            status_str(status),
                            ts(run_after),
                            failure.error.code(),
                            failure.error_class.map(|class| class.as_str()),
                            ts(now)
                        ],
                    )
                    .map_err(storage_error)?;
                    let updated = get_required(conn, item_id)?;
                    let outcome = if failure.run_after.is_none() {
                        FailOutcome::Stopped
                    } else if exhausted {
                        FailOutcome::MovedToDead
                    } else {
                        FailOutcome::RetryScheduled
                    };
                    Ok((updated, outcome))
                })
            })
            .await?;
        self.emit(item, Some(failure.error)).await;
        Ok(outcome)
    }

    async fn reclaim_expired_leases(&self, queue_id: &QueueId) -> Result<usize, QueueError> {
        let now = self.now();
        let queue_id = queue_id.clone();
        let (reclaimed, events) = self
            .with_connection(move |conn, _clock| {
                let tx = conn
                    .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                    .map_err(storage_error)?;
                let expired = expired_running_items_in_tx(&tx, &queue_id, now)?;
                let reclaimed = reclaim_expired_in_tx(&tx, &queue_id, now)?;
                let events = expired
                    .into_iter()
                    .map(|mut item| {
                        item.status = if item.attempt >= item.max_attempts {
                            QueueStatus::Dead
                        } else {
                            QueueStatus::Failed
                        };
                        item.lease_owner = None;
                        item.lease_until = None;
                        item.last_error
                            .get_or_insert(symbiotic_core::DiagnosticCode::LeaseExpired);
                        item.updated_at = now;
                        item
                    })
                    .collect::<Vec<_>>();
                tx.commit().map_err(storage_error)?;
                Ok((reclaimed, events))
            })
            .await?;
        if let Some(sink) = &self.event_sink {
            for item in events {
                sink.record_queue_event(QueueEvent {
                    item_id: item.item_id,
                    queue_id: item.queue_id,
                    kind: item.kind,
                    status: item.status,
                    attempt: item.attempt,
                    timestamp: self.now(),
                    error: Some(symbiotic_core::DiagnosticCode::LeaseExpired),
                })
                .await;
            }
        }
        Ok(reclaimed)
    }

    async fn cooldown_until(
        &self,
        queue_id: &QueueId,
    ) -> Result<Option<DateTime<Utc>>, QueueError> {
        let queue_id = queue_id.clone();
        self.with_connection(move |conn, _clock| {
            let raw: Option<String> = conn
                .query_row(
                    "select cooldown_until from queue_cooldowns where queue_id = ?1",
                    params![queue_id.0],
                    |row| row.get(0),
                )
                .optional()
                .map_err(storage_error)?;
            raw.map(|value| {
                parse_ts(value).map_err(|_err| {
                    QueueError::Storage(symbiotic_core::DiagnosticCode::StorageFailure)
                })
            })
            .transpose()
        })
        .await
    }

    async fn note_cooldown(
        &self,
        queue_id: &QueueId,
        until: DateTime<Utc>,
    ) -> Result<(), QueueError> {
        let now = self.now();
        let queue_id = queue_id.clone();
        self.with_connection(move |conn, _clock| {
            conn.execute(
                "insert into queue_cooldowns(queue_id, cooldown_until, updated_at)
             values (?1, ?2, ?3)
             on conflict(queue_id) do update set
               cooldown_until = case
                 when queue_cooldowns.cooldown_until < excluded.cooldown_until
                 then excluded.cooldown_until
                 else queue_cooldowns.cooldown_until
               end,
               updated_at = excluded.updated_at",
                params![queue_id.0, ts(until), ts(now)],
            )
            .map_err(storage_error)?;
            Ok(())
        })
        .await
    }
}

fn lease_deadline(now: DateTime<Utc>, seconds: u64) -> Result<DateTime<Utc>, QueueError> {
    i64::try_from(seconds.max(1))
        .ok()
        .and_then(ChronoDuration::try_seconds)
        .and_then(|duration| now.checked_add_signed(duration))
        .ok_or(QueueError::InvalidRequest(
            symbiotic_core::DiagnosticCode::InvalidConfiguration,
        ))
}

fn cooldown_active(
    conn: &Connection,
    queue_id: &QueueId,
    clock: &symbiotic_queue::jobs::JobClock,
) -> Result<bool, QueueError> {
    let raw: Option<String> = conn
        .query_row(
            "select cooldown_until from queue_cooldowns where queue_id = ?1",
            params![queue_id.0],
            |row| row.get(0),
        )
        .optional()
        .map_err(storage_error)?;
    raw.map(|value| {
        parse_ts(value)
            .map(|until| until > clock())
            .map_err(storage_error)
    })
    .transpose()
    .map(|active| active.unwrap_or(false))
}

/// Atomic current operational format: queue and spend tables, with no migrations.
pub const QUEUE_SCHEMA_VERSION: u32 = 17;

fn configure(conn: &mut Connection) -> Result<(), QueueError> {
    conn.busy_timeout(std::time::Duration::from_millis(sqlite_busy_timeout_ms()))
        .map_err(storage_error)?;
    conn.pragma_update(None, "synchronous", "FULL")
        .map_err(storage_error)?;
    conn.pragma_update(None, "fullfsync", true)
        .map_err(storage_error)?;
    conn.pragma_update(None, "journal_mode", "WAL")
        .map_err(storage_error)?;
    let tx = conn
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .map_err(storage_error)?;
    let schema_version: i64 = tx
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .map_err(storage_error)?;
    // Under the no-migrations rule, initialization atomically stamps the current queue schema and other versions or unversioned queue layouts are refused.
    if schema_version == i64::from(QUEUE_SCHEMA_VERSION) {
        return Ok(());
    }
    // Before release, only an empty, unversioned queue can be initialized.
    let existing_queue = tx
        .prepare("select 1 from sqlite_master where type = 'table' and name collate nocase in ('queue_items', 'queue_cooldowns', 'spend_accounts', 'spend_receipts', 'jobs', 'job_owners', 'model_job_bindings')")
        .and_then(|mut stmt| stmt.exists([]))
        .map_err(storage_error)?;
    if schema_version != 0 || existing_queue {
        return Err(QueueError::Storage(
            symbiotic_core::DiagnosticCode::UnsupportedQueueSchema,
        ));
    }
    tx.execute_batch(
        "
        create table spend_accounts (
            account text primary key, used integer not null default 0
        );
        create table spend_receipts (
            reference text primary key, account text not null, invocation text not null,
            binding text not null, reservation text not null, state text not null,
            usage text, output text, handoff_input text, dispatch_owner text,
            pre_dispatch_released integer not null default 0,
            attempt_limit integer, attempts_used integer not null default 0,
            recovery text, recovery_expires_at text
        );
        create unique index spend_active_invocation on spend_receipts(account, invocation)
            where state = 'unknown';
        create index spend_invocation_lookup on spend_receipts(account, invocation);
        create index spend_recovery_expiry on spend_receipts(recovery_expires_at)
            where recovery is not null;
        create table queue_items (
            item_id text primary key,
            queue_id text not null,
            kind text not null,
            payload_json text not null,
            status text not null,
            attempt integer not null,
            max_attempts integer not null,
            run_after text not null,
            lease_owner text,
            lease_until text,
            idempotency_key text,
            last_error text,
            last_error_class text,
            created_at text not null,
            updated_at text not null
        );
        create index idx_queue_claim
            on queue_items(queue_id, status, run_after, created_at);
        create index idx_queue_idempotency
            on queue_items(queue_id, idempotency_key);
        create unique index idx_queue_active_idempotency
            on queue_items(queue_id, idempotency_key)
            where idempotency_key is not null
              and status in ('pending', 'running', 'failed');
        create table queue_cooldowns (
            queue_id text primary key,
            cooldown_until text not null,
            updated_at text not null
        );
        ",
    )
    .map_err(storage_error)?;
    jobs::initialize(&tx).map_err(storage_error)?;
    tx.pragma_update(None, "user_version", QUEUE_SCHEMA_VERSION)
        .map_err(storage_error)?;
    tx.commit().map_err(storage_error)
}

fn invalid_stored_code() -> rusqlite::Error {
    queue_to_sql(QueueError::Storage(
        symbiotic_core::DiagnosticCode::UnsupportedQueueSchema,
    ))
}

fn read_code(
    row: &rusqlite::Row<'_>,
    column: usize,
) -> rusqlite::Result<Option<symbiotic_core::DiagnosticCode>> {
    row.get::<_, Option<String>>(column)?
        .map(|code| symbiotic_core::DiagnosticCode::parse(&code).ok_or_else(invalid_stored_code))
        .transpose()
}

fn sqlite_busy_timeout_ms() -> u64 {
    std::env::var("SYMBIOTIC_QUEUE_SQLITE_BUSY_TIMEOUT_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(60_000)
}

fn find_by_idempotency(
    conn: &Connection,
    queue_id: &QueueId,
    key: &str,
) -> Result<Option<QueueItem>, QueueError> {
    let mut stmt = conn
        .prepare(
            "select item_id, queue_id, kind, payload_json, status, attempt, max_attempts,
                    run_after, lease_owner, lease_until, idempotency_key, last_error,
                    created_at, updated_at, last_error_class
             from queue_items
             where queue_id = ?1 and idempotency_key = ?2
             order by created_at desc
             limit 1",
        )
        .map_err(storage_error)?;
    stmt.query_row(params![queue_id.0, key], row_to_item)
        .optional()
        .map_err(storage_error)
}

fn get_in_tx(conn: &Connection, item_id: &QueueItemId) -> Result<Option<QueueItem>, QueueError> {
    let mut stmt = conn
        .prepare(
            "select item_id, queue_id, kind, payload_json, status, attempt, max_attempts,
                    run_after, lease_owner, lease_until, idempotency_key, last_error,
                    created_at, updated_at, last_error_class
             from queue_items where item_id = ?1",
        )
        .map_err(storage_error)?;
    stmt.query_row(params![item_id.0], row_to_item)
        .optional()
        .map_err(storage_error)
}

fn get_required(conn: &Connection, item_id: &QueueItemId) -> Result<QueueItem, QueueError> {
    get_in_tx(conn, item_id)?.ok_or(QueueError::NotFound(
        symbiotic_core::DiagnosticCode::InvalidConfiguration,
    ))
}

fn update_running_item<T>(
    conn: &mut Connection,
    item_id: &QueueItemId,
    worker_id: &str,
    clock: &symbiotic_queue::jobs::JobClock,
    update: impl FnOnce(&Connection) -> Result<T, QueueError>,
) -> Result<T, QueueError> {
    let tx = conn
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .map_err(storage_error)?;
    let current = get_required(&tx, item_id)?;
    if current.status != QueueStatus::Running {
        return Err(QueueError::NotRunning(
            symbiotic_core::DiagnosticCode::InvalidConfiguration,
        ));
    }
    if current.lease_owner.as_deref() != Some(worker_id) {
        return Err(QueueError::LeaseMismatch(
            symbiotic_core::DiagnosticCode::InvalidConfiguration,
        ));
    }
    if current
        .lease_until
        .is_none_or(|lease_until| lease_until < clock())
    {
        return Err(QueueError::LeaseMismatch(
            symbiotic_core::DiagnosticCode::InvalidConfiguration,
        ));
    }
    let value = update(&tx)?;
    tx.commit().map_err(storage_error)?;
    Ok(value)
}

fn expired_running_items_in_tx(
    conn: &Connection,
    queue_id: &QueueId,
    now: DateTime<Utc>,
) -> Result<Vec<QueueItem>, QueueError> {
    let mut stmt = conn
        .prepare(
            "select item_id, queue_id, kind, payload_json, status, attempt, max_attempts,
                    run_after, lease_owner, lease_until, idempotency_key, last_error,
                    created_at, updated_at, last_error_class
             from queue_items
             where queue_id = ?1
               and status = 'running'
               and lease_until is not null
               and lease_until < ?2
             order by updated_at asc",
        )
        .map_err(storage_error)?;
    stmt.query_map(params![queue_id.0, ts(now)], row_to_item)
        .map_err(storage_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(storage_error)
}

fn stale_active_items_in_tx(
    conn: &Connection,
    queue_id: Option<&QueueId>,
    stale_before: DateTime<Utc>,
    now: DateTime<Utc>,
) -> Result<Vec<QueueItem>, QueueError> {
    match queue_id {
        Some(queue_id) => {
            let mut stmt = conn
                .prepare(
                    "select item_id, queue_id, kind, payload_json, status, attempt, max_attempts,
                            run_after, lease_owner, lease_until, idempotency_key, last_error,
                            created_at, updated_at, last_error_class
                     from queue_items
                     where queue_id = ?1
                       and status in ('pending', 'failed', 'running')
                       and updated_at < ?2
                       and (status != 'running' or lease_until is null or lease_until < ?3)
                     order by updated_at asc",
                )
                .map_err(storage_error)?;
            stmt.query_map(params![queue_id.0, ts(stale_before), ts(now)], row_to_item)
                .map_err(storage_error)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(storage_error)
        }
        None => {
            let mut stmt = conn
                .prepare(
                    "select item_id, queue_id, kind, payload_json, status, attempt, max_attempts,
                            run_after, lease_owner, lease_until, idempotency_key, last_error,
                            created_at, updated_at, last_error_class
                     from queue_items
                     where status in ('pending', 'failed', 'running')
                       and updated_at < ?1
                       and (status != 'running' or lease_until is null or lease_until < ?2)
                     order by updated_at asc",
                )
                .map_err(storage_error)?;
            stmt.query_map(params![ts(stale_before), ts(now)], row_to_item)
                .map_err(storage_error)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(storage_error)
        }
    }
}

fn reclaim_expired_in_tx(
    conn: &Connection,
    queue_id: &QueueId,
    now: DateTime<Utc>,
) -> Result<usize, QueueError> {
    // An expired lease on the last allowed attempt ends the item: another
    // claim would be an attempt beyond its budget.
    conn.execute(
        "update queue_items
         set status = case when attempt >= max_attempts then 'dead' else 'failed' end,
             lease_owner = null,
             lease_until = null,
             updated_at = ?2,
             last_error = 'lease_expired',
             last_error_class = 'queue'
         where queue_id = ?1
           and status = 'running'
           and lease_until is not null
           and lease_until < ?2",
        params![queue_id.0, ts(now)],
    )
    .map_err(storage_error)
}

fn row_to_item(row: &rusqlite::Row<'_>) -> rusqlite::Result<QueueItem> {
    let payload_json: String = row.get(3)?;
    let status: String = row.get(4)?;
    Ok(QueueItem {
        item_id: QueueItemId(row.get(0)?),
        queue_id: QueueId(row.get(1)?),
        kind: row.get(2)?,
        payload: serde_json::from_str(&payload_json).map_err(json_to_sql)?,
        status: parse_status(&status).map_err(queue_to_sql)?,
        attempt: row.get(5)?,
        max_attempts: row.get(6)?,
        run_after: parse_ts(row.get::<_, String>(7)?).map_err(queue_to_sql)?,
        lease_owner: row.get(8)?,
        lease_until: row
            .get::<_, Option<String>>(9)?
            .map(parse_ts)
            .transpose()
            .map_err(queue_to_sql)?,
        idempotency_key: row.get(10)?,
        last_error: read_code(row, 11)?,
        last_error_class: row
            .get::<_, Option<String>>(14)?
            .map(|class| {
                symbiotic_core::FailureClass::parse(&class).ok_or_else(invalid_stored_code)
            })
            .transpose()?,
        created_at: parse_ts(row.get::<_, String>(12)?).map_err(queue_to_sql)?,
        updated_at: parse_ts(row.get::<_, String>(13)?).map_err(queue_to_sql)?,
    })
}

fn ts(value: DateTime<Utc>) -> String {
    value.to_rfc3339()
}

fn parse_ts(value: String) -> Result<DateTime<Utc>, QueueError> {
    DateTime::parse_from_rfc3339(&value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|_err| QueueError::Storage(symbiotic_core::DiagnosticCode::StorageFailure))
}

fn storage_error(_error: impl std::fmt::Display) -> QueueError {
    QueueError::Storage(symbiotic_core::DiagnosticCode::StorageFailure)
}

fn lock_error(_: std::sync::PoisonError<std::sync::MutexGuard<'_, Connection>>) -> QueueError {
    QueueError::Unavailable(symbiotic_core::DiagnosticCode::SqliteQueueLockPoisoned)
}

fn json_to_sql(error: serde_json::Error) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(3, rusqlite::types::Type::Text, Box::new(error))
}

fn queue_to_sql(error: QueueError) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(4, rusqlite::types::Type::Text, Box::new(error))
}

fn is_unique_constraint(err: &rusqlite::Error) -> bool {
    matches!(
        err,
        rusqlite::Error::SqliteFailure(code, _)
            if code.code == rusqlite::ErrorCode::ConstraintViolation
    )
}

#[cfg(test)]
#[path = "../../test-support/clock.rs"]
mod test_clock;

#[cfg(test)]
mod tests {
    #[test]
    fn schema_keeps_only_canonical_spend_and_queue_state() {
        let queue = SqliteQueue::in_memory().unwrap();
        let conn = queue.conn.lock().unwrap();
        for table in ["queue_events", "spend_invocation_bindings"] {
            assert!(
                !conn
                    .query_row(
                        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name=?1)",
                        [table],
                        |r| r.get::<_, bool>(0)
                    )
                    .unwrap()
            );
        }
        for column in [
            "attempt_limit",
            "attempts_used",
            "recovery",
            "recovery_expires_at",
        ] {
            assert_eq!(
                conn.query_row(
                    "SELECT count(*) FROM pragma_table_info('spend_receipts') WHERE name=?1",
                    [column],
                    |r| r.get::<_, u32>(0)
                )
                .unwrap(),
                1
            );
        }
        assert_eq!(
            conn.query_row(
                "SELECT count(*) FROM sqlite_master WHERE name='spend_invocations'",
                [],
                |r| r.get::<_, u32>(0)
            )
            .unwrap(),
            0
        );
        let active: String = conn
            .query_row(
                "SELECT sql FROM sqlite_master WHERE name='spend_active_invocation'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(active.contains("where state = 'unknown'"));
        assert_eq!(conn.query_row("SELECT count(*) FROM pragma_table_info('spend_accounts') WHERE name='request_limit'", [], |r| r.get::<_, u32>(0)).unwrap(), 0);
    }

    #[test]
    fn queue_schema_initializes_and_reopens_at_the_current_version() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("queue.sqlite");
        drop(SqliteQueue::open(&path).unwrap());
        let conn = Connection::open(&path).unwrap();
        assert_eq!(
            conn.pragma_query_value(None, "user_version", |row| row.get::<_, u32>(0))
                .unwrap(),
            QUEUE_SCHEMA_VERSION
        );
        assert!(SqliteQueue::open(&path).is_ok());
    }

    #[test]
    fn partial_queue_schema_is_refused_without_schema_writes() {
        for table in [
            "QUEUE_ITEMS",
            "queue_cooldowns",
            "spend_accounts",
            "spend_receipts",
            "jobs",
            "job_owners",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("queue.sqlite");
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(&format!("create table {table} (foreign_column text)"))
                .unwrap();
            let error = match SqliteQueue::open(&path) {
                Ok(_) => panic!("accepted partial schema with {table}"),
                Err(error) => error,
            };
            assert!(matches!(
                error,
                QueueError::Storage(symbiotic_core::DiagnosticCode::UnsupportedQueueSchema)
            ));

            assert_eq!(
                conn.pragma_query_value(None, "user_version", |row| row.get::<_, u32>(0))
                    .unwrap(),
                0
            );
            assert_eq!(
                conn.query_row(
                    "select count(*) from sqlite_master where type = 'table'",
                    [],
                    |row| row.get::<_, u32>(0)
                )
                .unwrap(),
                1
            );
        }
    }

    #[test]
    fn queue_schema_creation_and_version_stamp_roll_back_together() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("queue.sqlite");
        let conn = Connection::open(&path).unwrap();
        // Fail late in initialization, after queue_items is created.
        conn.execute_batch("create view queue_cooldowns as select 1")
            .unwrap();
        assert!(SqliteQueue::open(&path).is_err());
        assert_eq!(
            conn.pragma_query_value(None, "user_version", |row| row.get::<_, u32>(0))
                .unwrap(),
            0
        );
        assert_eq!(conn.query_row("select count(*) from sqlite_master where type = 'table' and name in ('queue_items', 'queue_cooldowns', 'spend_accounts', 'spend_receipts', 'jobs', 'job_owners', 'model_job_bindings')", [], |row| row.get::<_, u32>(0)).unwrap(), 0);
        conn.execute_batch("drop view queue_cooldowns").unwrap();
        assert!(SqliteQueue::open(&path).is_ok());
    }

    #[test]
    fn trial_version_15_confirmed_purge_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("queue.sqlite");
        drop(SqliteQueue::open(&path).unwrap());
        let conn = Connection::open(&path).unwrap();
        conn.execute("INSERT INTO jobs(scope,id,key,digest,state,final_state,kind,delivery_generation) VALUES ('scope','id','key','digest','\"Accepted\"','\"Purged\"',NULL,1)", []).unwrap();
        conn.pragma_update(None, "user_version", 15).unwrap();
        assert!(matches!(
            SqliteQueue::open(&path),
            Err(QueueError::Storage(
                symbiotic_core::DiagnosticCode::UnsupportedQueueSchema
            ))
        ));
        assert_eq!(
            conn.pragma_query_value(None, "user_version", |r| r.get::<_, u32>(0))
                .unwrap(),
            15
        );
        assert!(
            conn.query_row("SELECT kind IS NULL FROM jobs", [], |r| r.get::<_, bool>(0))
                .unwrap()
        );
    }

    #[test]
    fn unversioned_existing_queue_or_wrong_version_is_refused_without_migration() {
        for version in [
            -1,
            0,
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
            i64::from(QUEUE_SCHEMA_VERSION) - 1,
            i64::from(QUEUE_SCHEMA_VERSION) + 1,
        ] {
            for existing in [false, true] {
                if version == 0 && !existing {
                    continue;
                }
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().join("queue.sqlite");
                if existing {
                    drop(SqliteQueue::open(&path).unwrap());
                }
                let conn = Connection::open(&path).unwrap();
                conn.pragma_update(None, "user_version", version).unwrap();
                let error = match SqliteQueue::open(&path) {
                    Ok(_) => panic!("accepted schema version {version}"),
                    Err(error) => error,
                };
                assert!(matches!(
                    error,
                    QueueError::Storage(symbiotic_core::DiagnosticCode::UnsupportedQueueSchema)
                ));

                assert_eq!(
                    conn.pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))
                        .unwrap(),
                    version
                );
            }
        }
    }

    use super::*;
    use futures::future::join_all;

    fn request(key: &str) -> EnqueueRequest {
        EnqueueRequest {
            queue_id: QueueId::new("chat:deepseek:pro"),
            kind: "chat".to_string(),
            payload: serde_json::json!({"hello": "world"}),
            idempotency_key: Some(key.to_string()),
            run_after: None,
            max_attempts: Some(2),
            force: false,
        }
    }

    #[test]
    fn sqlite_busy_timeout_defaults_to_longer_lock_wait() {
        unsafe {
            std::env::remove_var("SYMBIOTIC_QUEUE_SQLITE_BUSY_TIMEOUT_MS");
        }
        assert_eq!(sqlite_busy_timeout_ms(), 60_000);

        unsafe {
            std::env::set_var("SYMBIOTIC_QUEUE_SQLITE_BUSY_TIMEOUT_MS", "1500");
        }
        assert_eq!(sqlite_busy_timeout_ms(), 1500);

        unsafe {
            std::env::set_var("SYMBIOTIC_QUEUE_SQLITE_BUSY_TIMEOUT_MS", "0");
        }
        assert_eq!(sqlite_busy_timeout_ms(), 60_000);

        unsafe {
            std::env::remove_var("SYMBIOTIC_QUEUE_SQLITE_BUSY_TIMEOUT_MS");
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn overlapping_heartbeats_renew_in_write_transaction_order() {
        use std::time::Duration;

        let dir = tempfile::tempdir_in(std::env::var_os("CARGO_MANIFEST_DIR").unwrap()).unwrap();
        let path = dir.path().join("queue.sqlite");
        let older_queue = SqliteQueue::open(&path).unwrap();
        let newer_queue = SqliteQueue::open(&path).unwrap();
        let item = older_queue.enqueue(request("overlap")).await.unwrap().item;
        older_queue
            .claim_item(&item.item_id, "worker", 3, None)
            .await
            .unwrap()
            .unwrap();

        let conn = older_queue.conn.clone();
        let (locked_tx, locked_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let holder = std::thread::spawn(move || {
            let _guard = conn.lock().unwrap();
            locked_tx.send(()).unwrap();
            release_rx.recv_timeout(Duration::from_secs(2)).is_ok()
        });
        locked_rx.await.unwrap();
        assert!(matches!(
            tokio::time::timeout(
                Duration::from_millis(100),
                older_queue.heartbeat(&item.item_id, "worker", u64::MAX),
            )
            .await
            .expect("invalid duration waited for the connection"),
            Err(QueueError::InvalidRequest(_))
        ));
        let older = older_queue.heartbeat(&item.item_id, "worker", 3);
        tokio::pin!(older);
        assert!(futures::poll!(&mut older).is_pending());
        // Separate the captured times while the older call waits for its connection.
        let after_older_poll = Utc::now();
        tokio::time::timeout(Duration::from_secs(1), async {
            while Utc::now() <= after_older_poll {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("clock did not advance while the older heartbeat waited");
        tokio::time::timeout(
            Duration::from_secs(1),
            newer_queue.heartbeat(&item.item_id, "worker", 3),
        )
        .await
        .unwrap()
        .unwrap();
        let newer = newer_queue.get_item(&item.item_id).await.unwrap().unwrap();
        release_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), &mut older)
            .await
            .unwrap()
            .unwrap();
        assert!(
            holder.join().unwrap(),
            "lock was released only by the guard"
        );
        let final_item = older_queue.get_item(&item.item_id).await.unwrap().unwrap();
        assert!(
            final_item.lease_until >= newer.lease_until,
            "an older heartbeat shortened the newer lease"
        );
        assert!(final_item.updated_at >= newer.updated_at);
        assert_eq!(final_item.attempt, newer.attempt);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn contended_heartbeat_leaves_async_worker_free() {
        use std::time::Duration;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("queue.sqlite");
        let queue = SqliteQueue::open(&path).unwrap();
        let item = queue.enqueue(request("heartbeat")).await.unwrap().item;
        let claimed = queue
            .claim_item(&item.item_id, "worker", 60, None)
            .await
            .unwrap()
            .unwrap();
        let (locked_tx, locked_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let holder = std::thread::spawn(move || {
            let mut conn = Connection::open(path).unwrap();
            let tx = conn
                .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                .unwrap();
            locked_tx.send(()).unwrap();
            // An independent thread bounds failure even if the runtime is blocked.
            let released_by_task = release_rx.recv_timeout(Duration::from_secs(2)).is_ok();
            tx.rollback().unwrap();
            released_by_task
        });
        locked_rx.await.unwrap();

        let renewal = queue.heartbeat(&item.item_id, "worker", 120);
        tokio::pin!(renewal);
        let pending = futures::poll!(&mut renewal).is_pending();
        if pending {
            let conn = queue.conn.clone();
            let progress = tokio::spawn(async move {
                tokio::time::timeout(Duration::from_secs(2), async {
                    // Wait for the database worker to acquire the queue connection.
                    while conn.try_lock().is_ok() {
                        tokio::task::yield_now().await;
                    }
                    for _ in 0..16 {
                        tokio::task::yield_now().await;
                    }
                    release_tx.send(()).unwrap();
                })
                .await
                .expect("async task did not progress while heartbeat waited");
            });
            tokio::time::timeout(Duration::from_secs(5), &mut renewal)
                .await
                .expect("heartbeat did not finish after lock release")
                .unwrap();
            progress.await.unwrap();
        }
        let released_by_task = holder.join().unwrap();
        assert!(pending, "heartbeat blocked the current-thread runtime");
        assert!(released_by_task, "lock was released only by the hang guard");
        let renewed = queue.get_item(&item.item_id).await.unwrap().unwrap();
        assert!(renewed.lease_until > claimed.lease_until);
        assert!(renewed.lease_until.unwrap() > Utc::now());
        assert_eq!(renewed.attempt, claimed.attempt);
        queue.complete(&item.item_id, "worker").await.unwrap();
    }

    async fn connection_wait_yields<T>(
        queue: &SqliteQueue,
        operation: impl std::future::Future<Output = Result<T, QueueError>>,
    ) -> T {
        use std::time::Duration;

        let conn = queue.conn.clone();
        let (locked_tx, locked_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let holder = std::thread::spawn(move || {
            let _guard = conn.lock().unwrap();
            locked_tx.send(()).unwrap();
            release_rx.recv_timeout(Duration::from_secs(2)).is_ok()
        });
        locked_rx.await.unwrap();
        tokio::pin!(operation);
        let pending = futures::poll!(&mut operation).is_pending();
        let result = if pending {
            // Only a task on the same current-thread runtime can release the lock.
            let progress = tokio::spawn(async move { release_tx.send(()).unwrap() });
            let result = tokio::time::timeout(Duration::from_secs(5), &mut operation).await;
            progress.await.unwrap();
            Some(result)
        } else {
            None
        };
        let released_by_task = holder.join().unwrap();
        assert!(pending, "connection wait blocked the async worker");
        assert!(released_by_task, "lock was released only by the hang guard");
        result.unwrap().expect("queue call did not finish").unwrap()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn backend_calls_leave_async_worker_free_during_connection_wait() {
        let queue = SqliteQueue::in_memory().unwrap();
        let item = connection_wait_yields(&queue, queue.enqueue(request("connection")))
            .await
            .item;
        let duplicate = connection_wait_yields(
            &queue,
            queue.enqueue_replacing(request("connection"), &item.item_id),
        )
        .await;
        assert_eq!(duplicate.disposition, EnqueueDisposition::ActiveDuplicate);
        connection_wait_yields(
            &queue,
            queue.claim(ClaimRequest {
                queue_id: item.queue_id.clone(),
                worker_id: "worker".into(),
                limit: 1,
                lease_seconds: 60,
                max_in_flight: None,
            }),
        )
        .await;
        connection_wait_yields(&queue, queue.get_item(&item.item_id)).await;
        connection_wait_yields(&queue, queue.heartbeat(&item.item_id, "worker", 60)).await;
        connection_wait_yields(&queue, queue.reclaim_expired_leases(&item.queue_id)).await;
        connection_wait_yields(&queue, queue.cooldown_until(&item.queue_id)).await;
        connection_wait_yields(
            &queue,
            queue.note_cooldown(&item.queue_id, Utc::now() - ChronoDuration::seconds(1)),
        )
        .await;
        let reason = symbiotic_core::DiagnosticCode::LeaseExpired;
        connection_wait_yields(
            &queue,
            queue.mark_stale_active_dead(&item.queue_id, Utc::now(), reason),
        )
        .await;
        connection_wait_yields(&queue, queue.mark_all_stale_active_dead(Utc::now(), reason)).await;
        connection_wait_yields(&queue, queue.complete(&item.item_id, "worker")).await;

        for key in ["fail", "fail_with"] {
            let item = queue.enqueue(request(key)).await.unwrap().item;
            connection_wait_yields(&queue, queue.claim_item(&item.item_id, "worker", 60, None))
                .await;
            if key == "fail" {
                connection_wait_yields(&queue, queue.fail(&item.item_id, "worker", reason, None))
                    .await;
            } else {
                connection_wait_yields(
                    &queue,
                    queue.fail_with(
                        &item.item_id,
                        "worker",
                        Failure {
                            error: reason,
                            error_class: None,
                            run_after: None,
                        },
                    ),
                )
                .await;
            }
        }
    }

    #[tokio::test]
    async fn multi_connection_enqueue_deduplicates_active_idempotency_key() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("queue.sqlite");
        let first = SqliteQueue::open(&path).unwrap();
        let second = SqliteQueue::open(&path).unwrap();

        let outcomes = join_all(vec![
            first.enqueue(request("cross-process")),
            second.enqueue(request("cross-process")),
        ])
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

        let inserted = outcomes
            .iter()
            .filter(|outcome| outcome.disposition == EnqueueDisposition::Inserted)
            .count();
        let duplicates = outcomes
            .iter()
            .filter(|outcome| outcome.disposition == EnqueueDisposition::ActiveDuplicate)
            .count();
        assert_eq!(inserted, 1);
        assert_eq!(duplicates, 1);
        assert_eq!(outcomes[0].item.item_id.0, outcomes[1].item.item_id.0);
    }

    #[tokio::test]
    async fn multi_connection_claim_does_not_return_stale_loser() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("queue.sqlite");
        let writer = SqliteQueue::open(&path).unwrap();
        for idx in 0..20 {
            writer
                .enqueue(request(&format!("multi-claim-{idx}")))
                .await
                .unwrap();
        }
        let first = SqliteQueue::open(&path).unwrap();
        let second = SqliteQueue::open(&path).unwrap();

        let claimed = join_all(vec![
            first.claim(ClaimRequest {
                queue_id: QueueId::new("chat:deepseek:pro"),
                worker_id: "worker-a".to_string(),
                limit: 20,
                lease_seconds: 60,
                max_in_flight: None,
            }),
            second.claim(ClaimRequest {
                queue_id: QueueId::new("chat:deepseek:pro"),
                worker_id: "worker-b".to_string(),
                limit: 20,
                lease_seconds: 60,
                max_in_flight: None,
            }),
        ])
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

        let mut ids = claimed
            .iter()
            .flat_map(|items| items.iter().map(|item| item.item_id.0.clone()))
            .collect::<Vec<_>>();
        ids.sort();
        ids.dedup();
        let claimed_count = claimed.iter().map(Vec::len).sum::<usize>();
        assert_eq!(ids.len(), claimed_count);
        assert_eq!(claimed_count, 20);
    }

    #[tokio::test]
    async fn mark_stale_active_dead_preserves_fresh_and_live_running_items() {
        let queue = SqliteQueue::in_memory().unwrap();
        let queue_id = QueueId::new("chat:deepseek:pro");
        let stale_pending = queue.enqueue(request("stale-pending")).await.unwrap().item;
        let fresh_pending = queue.enqueue(request("fresh-pending")).await.unwrap().item;
        let stale_failed = queue.enqueue(request("stale-failed")).await.unwrap().item;
        let stale_running = queue.enqueue(request("stale-running")).await.unwrap().item;
        let live_running = queue.enqueue(request("live-running")).await.unwrap().item;

        {
            let conn = queue.conn.lock().unwrap();
            let old = ts(Utc::now() - ChronoDuration::hours(2));
            conn.execute(
                "update queue_items set updated_at = ?2 where item_id = ?1",
                params![stale_pending.item_id.0, old],
            )
            .unwrap();
            conn.execute(
                "update queue_items
                 set status = 'failed', attempt = 1, updated_at = ?2
                 where item_id = ?1",
                params![stale_failed.item_id.0, old],
            )
            .unwrap();
            conn.execute(
                "update queue_items
                 set status = 'running',
                     attempt = 1,
                     lease_owner = 'old-worker',
                     lease_until = ?2,
                     updated_at = ?3
                 where item_id = ?1",
                params![
                    stale_running.item_id.0,
                    ts(Utc::now() - ChronoDuration::hours(1)),
                    old
                ],
            )
            .unwrap();
            conn.execute(
                "update queue_items
                 set status = 'running',
                     attempt = 1,
                     lease_owner = 'live-worker',
                     lease_until = ?2,
                     updated_at = ?3
                 where item_id = ?1",
                params![
                    live_running.item_id.0,
                    ts(Utc::now() + ChronoDuration::hours(1)),
                    old
                ],
            )
            .unwrap();
        }

        let updated = queue
            .mark_stale_active_dead(
                &queue_id,
                Utc::now() - ChronoDuration::hours(1),
                symbiotic_core::DiagnosticCode::StaleQueueItem,
            )
            .await
            .unwrap();
        assert_eq!(updated, 3);

        assert_eq!(
            queue.get(&stale_pending.item_id).unwrap().unwrap().status,
            QueueStatus::Dead
        );
        assert_eq!(
            queue.get(&stale_failed.item_id).unwrap().unwrap().status,
            QueueStatus::Dead
        );
        assert_eq!(
            queue.get(&stale_running.item_id).unwrap().unwrap().status,
            QueueStatus::Dead
        );
        assert_eq!(
            queue.get(&fresh_pending.item_id).unwrap().unwrap().status,
            QueueStatus::Pending
        );
        assert_eq!(
            queue.get(&live_running.item_id).unwrap().unwrap().status,
            QueueStatus::Running
        );
    }

    #[tokio::test]
    async fn fail_retries_until_dead_and_reopens_from_disk() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("queue.sqlite");
        let queue = SqliteQueue::open(&path).unwrap();
        queue.enqueue(request("retry")).await.unwrap();
        let claimed = queue
            .claim(ClaimRequest {
                queue_id: QueueId::new("chat:deepseek:pro"),
                worker_id: "worker".to_string(),
                limit: 1,
                lease_seconds: 60,
                max_in_flight: None,
            })
            .await
            .unwrap();
        assert_eq!(
            queue
                .fail(
                    &claimed[0].item_id,
                    "worker",
                    symbiotic_core::DiagnosticCode::QueueFailure,
                    Some(0)
                )
                .await
                .unwrap(),
            FailOutcome::RetryScheduled
        );
        let reopened = SqliteQueue::open(&path).unwrap();
        let claimed_again = reopened
            .claim(ClaimRequest {
                queue_id: QueueId::new("chat:deepseek:pro"),
                worker_id: "worker".to_string(),
                limit: 1,
                lease_seconds: 60,
                max_in_flight: None,
            })
            .await
            .unwrap();
        assert_eq!(
            reopened
                .fail(
                    &claimed_again[0].item_id,
                    "worker",
                    symbiotic_core::DiagnosticCode::QueueFailure,
                    Some(0)
                )
                .await
                .unwrap(),
            FailOutcome::MovedToDead
        );
        assert_eq!(
            reopened.get(&claimed[0].item_id).unwrap().unwrap().status,
            QueueStatus::Dead
        );
    }

    #[tokio::test]
    async fn stopped_failure_survives_reopen_with_attempts_remaining() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("queue.sqlite");
        let queue = SqliteQueue::open(&path).unwrap();
        let item = queue.enqueue(request("stopped")).await.unwrap().item;
        queue
            .claim_item(&item.item_id, "worker", 60, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            queue
                .fail_with(
                    &item.item_id,
                    "worker",
                    Failure {
                        error: symbiotic_core::DiagnosticCode::QueueFailure,
                        error_class: Some(symbiotic_core::FailureClass::Queue),
                        run_after: None,
                    }
                )
                .await
                .unwrap(),
            FailOutcome::Stopped
        );
        drop(queue);
        let reopened = SqliteQueue::open(&path).unwrap();
        let duplicate = reopened.enqueue(request("stopped")).await.unwrap();
        assert_eq!(duplicate.disposition, EnqueueDisposition::TerminalDuplicate);
        assert_eq!(duplicate.item.status, QueueStatus::Stopped);
        assert_eq!(
            duplicate.item.last_error_class,
            Some(symbiotic_core::FailureClass::Queue)
        );
        assert!(duplicate.item.attempt < duplicate.item.max_attempts);
        assert!(
            reopened
                .claim_item(&item.item_id, "later", 60, None)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn prune_removes_old_terminal_items_only() {
        let queue = SqliteQueue::in_memory().unwrap();
        let done = queue.enqueue(request("done")).await.unwrap().item;
        let active = queue.enqueue(request("active")).await.unwrap().item;
        queue
            .claim_item(&done.item_id, "worker", 60, None)
            .await
            .unwrap()
            .unwrap();
        queue.complete(&done.item_id, "worker").await.unwrap();
        {
            let conn = queue.conn.lock().unwrap();
            let old = ts(Utc::now() - ChronoDuration::days(30));
            conn.execute("update queue_items set updated_at = ?1", params![old])
                .unwrap();
        }

        let deleted = queue
            .prune_terminal_before(Utc::now() - ChronoDuration::days(7))
            .unwrap();
        assert_eq!(deleted, 1);
        assert!(queue.get(&done.item_id).unwrap().is_none());
        assert!(queue.get(&active.item_id).unwrap().is_some());
        let again = queue.enqueue(request("done")).await.unwrap();
        assert_eq!(again.disposition, EnqueueDisposition::Inserted);
    }

    #[tokio::test(start_paused = true)]
    async fn reopened_queue_does_not_retry_a_crashed_final_attempt() {
        let clock = test_clock::clock();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("queue.sqlite");
        let item = {
            let queue = SqliteQueue::open(&path).unwrap().with_clock(clock.clone());
            let mut final_only = request("crashed");
            final_only.max_attempts = Some(1);
            let item = queue.enqueue(final_only).await.unwrap().item;
            queue
                .claim_item(&item.item_id, "crashed-worker", 1, None)
                .await
                .unwrap()
                .unwrap();
            item
            // The process "crashes" here: the lease is never completed.
        };
        tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;

        let reopened = SqliteQueue::open(&path).unwrap().with_clock(clock.clone());
        assert!(
            reopened
                .claim_item(&item.item_id, "restarted", 60, None)
                .await
                .unwrap()
                .is_none()
        );
        let dead = reopened.get(&item.item_id).unwrap().unwrap();
        assert_eq!(dead.status, QueueStatus::Dead);
        assert_eq!(dead.attempt, 1);
    }

    struct RecordingSink(Mutex<Vec<QueueEvent>>);

    #[async_trait]
    impl QueueEventSink for RecordingSink {
        async fn record_queue_event(&self, event: QueueEvent) {
            self.0.lock().unwrap().push(event);
        }
    }

    #[tokio::test]
    async fn event_sink_receives_transitions() {
        let sink = Arc::new(RecordingSink(Mutex::new(Vec::new())));
        let queue = SqliteQueue::in_memory()
            .unwrap()
            .with_event_sink(sink.clone());
        queue.enqueue(request("event")).await.unwrap();
        let claimed = queue
            .claim(ClaimRequest {
                queue_id: QueueId::new("chat:deepseek:pro"),
                worker_id: "worker".to_string(),
                limit: 1,
                lease_seconds: 60,
                max_in_flight: None,
            })
            .await
            .unwrap();
        queue.complete(&claimed[0].item_id, "worker").await.unwrap();
        let events = sink.0.lock().unwrap();
        let transitions: Vec<_> = events
            .iter()
            .map(|event| {
                (
                    event.item_id.clone(),
                    event.queue_id.clone(),
                    event.kind.as_str(),
                    event.status,
                    event.attempt,
                    event.error,
                )
            })
            .collect();
        assert_eq!(
            transitions,
            [
                QueueStatus::Pending,
                QueueStatus::Running,
                QueueStatus::Succeeded
            ]
            .into_iter()
            .enumerate()
            .map(|(index, status)| (
                claimed[0].item_id.clone(),
                QueueId::new("chat:deepseek:pro"),
                "chat",
                status,
                u32::from(index > 0),
                None,
            ))
            .collect::<Vec<_>>()
        );
    }
}
