//! SQLite accounting in Foundation's operational database, never a Memory store.
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use std::{path::Path, sync::Mutex};
use symbiotic_core::DiagnosticCode;
use symbiotic_model::{
    AcceptedSpendHandoff, ModelError, SpendLedger, SpendReceipt, SpendReceiptRef, SpendReservation,
    SpendState,
};
use symbiotic_trace::UsageTrace;

fn storage(_: impl std::fmt::Debug) -> ModelError {
    ModelError::Queue(DiagnosticCode::SpendLedgerUnavailable)
}
fn conflict() -> ModelError {
    ModelError::Queue(DiagnosticCode::SpendReconciliationRequired)
}
fn state_name(state: SpendState) -> &'static str {
    match state {
        SpendState::Unknown => "unknown",
        SpendState::Released => "released",
        SpendState::Settled => "settled",
    }
}

pub(crate) fn job_invocation_key(
    scope: &symbiotic_queue::jobs::JobScope,
    key: &str,
) -> Result<String, ModelError> {
    serde_json::to_string(&(scope, key)).map_err(storage)
}

// Look up the canonical job through scope/key, then check its frozen binding.
// The model kind survives confirmation and addresses the existing binding registry.
pub(crate) fn job_recovery_policy(
    tx: &rusqlite::Transaction<'_>,
    invocation: Option<&str>,
    bound_invocation: &str,
) -> Result<(bool, Option<chrono::DateTime<chrono::Utc>>), ModelError> {
    let Some(invocation) = invocation else {
        return Ok((false, None));
    };
    let Ok((scope, key)) =
        serde_json::from_str::<(symbiotic_queue::jobs::JobScope, String)>(invocation)
    else {
        return Ok((false, None));
    };
    let row: Option<(Option<String>, bool, Option<i64>)> = tx.query_row(
        "SELECT b.binding, coalesce(j.purged=1 OR j.final_state='\"Purged\"' OR j.state IN ('\"Accepted\"','\"Discarded\"'),0), j.recovery_until
         FROM jobs j LEFT JOIN model_job_bindings b ON b.scope=j.scope AND b.kind=j.kind
         WHERE j.scope=?1 AND j.key=?2 AND j.kind IS NOT NULL
         AND (j.execution='\"Model\"' OR j.execution IS NULL)",
        params![serde_json::to_string(&scope).map_err(storage)?, key],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    ).optional().map_err(storage)?;
    let Some((binding, erased, deadline)) = row else {
        return Ok((false, None));
    };
    let (identity, _): (
        symbiotic_core::BindingIdentity,
        Option<symbiotic_core::AccountSharingKey>,
    ) = serde_json::from_str(&binding.ok_or_else(|| storage(()))?).map_err(storage)?;
    if symbiotic_model::execution_invocation_identity(&identity, invocation)? != bound_invocation {
        return Ok((false, None));
    }
    let deadline = deadline
        .map(|millis| chrono::DateTime::from_timestamp_millis(millis).ok_or_else(|| storage(())))
        .transpose()?;
    Ok((erased, deadline))
}

// Enqueue and adoption narrow the sole saved answer without extending its lifetime.
pub(crate) fn constrain_job_recovery_in(
    tx: &rusqlite::Transaction<'_>,
    reference: &SpendReceiptRef,
    invocation: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<(bool, Option<chrono::DateTime<chrono::Utc>>), ModelError> {
    let (bound_invocation, deadline): (String, Option<String>) = tx
        .query_row(
            "SELECT invocation, recovery_expires_at FROM spend_receipts WHERE reference=?1",
            [reference.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(storage)?;
    let (prohibited, job_deadline) = job_recovery_policy(tx, Some(invocation), &bound_invocation)?;
    let deadline = deadline
        .map(|s| chrono::DateTime::parse_from_rfc3339(&s).map(|d| d.with_timezone(&chrono::Utc)))
        .transpose()
        .map_err(storage)?;
    let deadline = deadline.map(|until| job_deadline.map_or(until, |job| job.min(until)));
    if prohibited || deadline.is_some_and(|until| until <= now) {
        tx.execute(
            "UPDATE spend_receipts SET recovery=NULL,recovery_expires_at=NULL WHERE reference=?1",
            [reference.as_str()],
        )
        .map_err(storage)?;
    } else if let Some(until) = deadline {
        tx.execute(
            "UPDATE spend_receipts SET recovery_expires_at=?2 WHERE reference=?1",
            params![reference.as_str(), until.to_rfc3339()],
        )
        .map_err(storage)?;
    }
    Ok((prohibited, deadline))
}

/// Ledger handle for the versioned queue database. Opens only current queue state.
pub struct SqliteSpendLedger(
    pub(super) Mutex<Connection>,
    std::time::Duration,
    symbiotic_queue::jobs::JobClock,
);
impl SqliteSpendLedger {
    /// Open an already initialized Foundation queue database.
    pub fn open(path: &Path) -> Result<Self, ModelError> {
        let conn = Connection::open(path).map_err(storage)?;
        conn.busy_timeout(std::time::Duration::from_secs(5))
            .map_err(storage)?;
        conn.pragma_update(None, "synchronous", "FULL")
            .map_err(storage)?;
        conn.pragma_update(None, "fullfsync", true)
            .map_err(storage)?;
        // Validate the atomic queue format stamp without creating or migrating tables.
        let version: u32 = conn
            .pragma_query_value(None, "user_version", |r| r.get(0))
            .map_err(storage)?;
        if version != symbiotic_queue_sqlite::QUEUE_SCHEMA_VERSION {
            return Err(storage(()));
        }
        Ok(Self(
            Mutex::new(conn),
            symbiotic_model::DEFAULT_RETENTION,
            std::sync::Arc::new(chrono::Utc::now),
        ))
    }

    #[cfg(test)]
    pub(crate) fn in_memory() -> Self {
        let mut conn = Connection::open_in_memory().unwrap();
        symbiotic_queue_sqlite::SqliteQueue::initialize_connection(&mut conn).unwrap();
        Self(
            Mutex::new(conn),
            symbiotic_model::DEFAULT_RETENTION,
            std::sync::Arc::new(chrono::Utc::now),
        )
    }

    /// Use the runtime's existing retention window for saved answers.
    pub fn with_retention(mut self, retention: std::time::Duration) -> Self {
        self.1 = retention;
        self
    }

    /// Use a trusted host clock for spend and recovery deadlines.
    pub fn with_clock(mut self, clock: symbiotic_queue::jobs::JobClock) -> Self {
        self.2 = clock;
        self
    }

    pub(crate) fn now(&self) -> chrono::DateTime<chrono::Utc> {
        (self.2)()
    }

    pub(crate) fn retention(&self) -> std::time::Duration {
        self.1
    }

    pub(crate) fn release_in(
        tx: &rusqlite::Transaction<'_>,
        reference: &SpendReceiptRef,
    ) -> Result<(), ModelError> {
        let old = receipt_in(tx, reference, chrono::Utc::now())?.ok_or_else(conflict)?;
        if old.state != SpendState::Unknown && !old.pre_dispatch_released {
            return Err(conflict());
        }
        Self::finish_in(tx, reference, SpendState::Released, None, None)?;
        tx.execute("UPDATE spend_receipts SET pre_dispatch_released=1,
            attempts_used=CASE WHEN attempt_limit IS NOT NULL AND pre_dispatch_released=0 THEN attempts_used-1 ELSE attempts_used END WHERE reference=?1", [reference.as_str()]).map_err(storage)?;
        Ok(())
    }

    /// Clear all expired answers in indexed batches of at most 64 rows.
    pub fn expire_recovery(&self) -> Result<usize, ModelError> {
        let conn = self.0.lock().map_err(storage)?;
        let now = self.now().to_rfc3339();
        let mut removed = 0;
        loop {
            let batch = conn.execute(
                "UPDATE spend_receipts SET recovery=NULL, recovery_expires_at=NULL WHERE rowid IN (
                SELECT rowid FROM spend_receipts WHERE recovery IS NOT NULL AND recovery_expires_at<=?1
                ORDER BY recovery_expires_at LIMIT 64)",
                [&now],
            ).map_err(storage)?;
            removed += batch;
            if batch == 0 {
                return Ok(removed);
            }
        }
    }

    /// Reserve inside the caller's immediate transaction (permit/replay acceptance).
    /// Returns false for authenticated reattachment to exactly the same reservation.
    pub fn reserve_in(
        conn: &rusqlite::Transaction<'_>,
        r: &SpendReservation,
    ) -> Result<bool, ModelError> {
        Self::reserve_with_limit_in(conn, r, None, false)
    }

    pub(crate) fn reserve_with_limit_in(
        conn: &rusqlite::Transaction<'_>,
        r: &SpendReservation,
        attempt_limit: Option<u32>,
        handoff: bool,
    ) -> Result<bool, ModelError> {
        // The rowid order is acceptance order: each predecessor must be released
        // before another attempt is accepted. Carry the original ceiling and count.
        let previous = invocation_in(conn, &r.account, &r.invocation, chrono::Utc::now())?;
        if previous
            .as_ref()
            .is_some_and(|p| p.reservation.binding != r.binding)
        {
            return Err(conflict());
        }
        if let Some(old) = receipt_in(conn, &r.reference, chrono::Utc::now())? {
            return if old.reservation == *r {
                Ok(false)
            } else {
                Err(conflict())
            };
        }
        let explicit =
            attempt_limit.is_some() || previous.as_ref().is_some_and(|p| p.attempt_limit.is_some());
        if (explicit || handoff)
            && let Some(previous) = &previous
        {
            if previous.output.is_some() {
                return if explicit {
                    Err(ModelError::Queue(DiagnosticCode::InvocationCompleted))
                } else {
                    Err(conflict())
                };
            }
            if previous.state == SpendState::Settled {
                return Err(conflict());
            }
        }
        // Exactly matches the unique partial index; settled implicit history is irrelevant.
        let active: bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM spend_receipts WHERE account=?1 AND invocation=?2 AND state='unknown')", params![r.account, r.invocation], |row| row.get(0)).map_err(storage)?;
        if active {
            return Err(conflict());
        }
        let frozen = previous
            .as_ref()
            .and_then(|p| p.attempt_limit)
            .or(attempt_limit);
        let used = previous.as_ref().map_or(0, |p| p.attempts_used);
        if frozen.is_some_and(|limit| used >= limit) {
            return Err(ModelError::BudgetExhausted(
                DiagnosticCode::AttemptBudgetExhausted,
            ));
        }
        let used = if frozen.is_some() {
            used.checked_add(1).ok_or_else(|| storage(()))?
        } else {
            0
        };
        let limit = r
            .request_limit
            .map(i64::try_from)
            .transpose()
            .map_err(storage)?;
        conn.execute(
            "INSERT OR IGNORE INTO spend_accounts(account) VALUES (?1)",
            [&r.account],
        )
        .map_err(storage)?;
        let changed = conn.execute("UPDATE spend_accounts SET used=used+1 WHERE account=?1 AND used<9223372036854775807 AND (?2 IS NULL OR used<?2)", params![r.account, limit]).map_err(storage)?;
        if changed != 1 {
            return Err(ModelError::BudgetExhausted(
                DiagnosticCode::SpendBudgetExhausted,
            ));
        }
        let json = serde_json::to_string(r).map_err(storage)?;
        conn.execute("INSERT INTO spend_receipts(reference, account, invocation, binding, reservation, state, attempt_limit, attempts_used) VALUES (?1, ?2, ?3, ?4, ?5, 'unknown', ?6, ?7)", params![r.reference.as_str(), r.account, r.invocation, r.binding, json, frozen, used]).map_err(storage)?;
        Ok(true)
    }

    /// Reserve a trusted handoff and its exact queued input in the permit transaction.
    pub fn reserve_handoff_in(
        conn: &rusqlite::Transaction<'_>,
        handoff: &AcceptedSpendHandoff,
    ) -> Result<bool, ModelError> {
        if handoff.input_identity.is_empty() {
            return Err(conflict());
        }
        if !Self::reserve_with_limit_in(conn, &handoff.reservation, None, true)? {
            let input: Option<String> = conn
                .query_row(
                    "SELECT handoff_input FROM spend_receipts WHERE reference=?1",
                    [handoff.reservation.reference.as_str()],
                    |row| row.get(0),
                )
                .map_err(storage)?;
            return if input.as_deref() == Some(handoff.input_identity.as_str()) {
                Ok(false)
            } else {
                Err(conflict())
            };
        }
        conn.execute(
            "UPDATE spend_receipts SET handoff_input=?2 WHERE reference=?1",
            params![
                handoff.reservation.reference.as_str(),
                handoff.input_identity
            ],
        )
        .map_err(storage)?;
        Ok(true)
    }

    /// Sole saved-answer writer, shared by direct invocations and durable jobs.
    pub(crate) fn save_recovery_in(
        tx: &rusqlite::Transaction<'_>,
        r: &SpendReceiptRef,
        output: &Option<serde_json::Value>,
        retention: std::time::Duration,
        deadline: Option<chrono::DateTime<chrono::Utc>>,
        invocation: Option<&str>,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), ModelError> {
        // Content-free completion evidence is never a recoverable provider response.
        if output.as_ref() == Some(&serde_json::json!({"output_received": true})) {
            return Ok(());
        }
        let old = receipt_in(tx, r, chrono::Utc::now())?.ok_or_else(conflict)?;
        let (erased, job_deadline) =
            job_recovery_policy(tx, invocation, &old.reservation.invocation)?;
        let deadline = match (deadline, job_deadline) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        let keep = !erased;
        if old.attempt_limit.is_some()
            && old.output.is_some()
            && let Some(value) = &output
            && old.recovery.as_ref() != Some(value)
        {
            return Err(conflict());
        }
        if old.attempt_limit.is_some()
            && old.output.is_none()
            && let Some(value) = &output
        {
            let expires = chrono::Duration::from_std(retention)
                .ok()
                .and_then(|d| now.checked_add_signed(d))
                .ok_or_else(|| storage(()))?;
            let expires = deadline.map_or(expires, |until| until.min(expires));
            if !keep || deadline.is_some_and(|until| until <= now) {
                return Ok(());
            }
            tx.execute(
                "UPDATE spend_receipts SET recovery=?2, recovery_expires_at=?3 WHERE reference=?1",
                params![
                    r.as_str(),
                    serde_json::to_string(value).map_err(storage)?,
                    expires.to_rfc3339()
                ],
            )
            .map_err(storage)?;
        }
        Ok(())
    }

    /// Settle or reconcile in the same transaction as execution/recovery state.
    /// Repeating an identical settlement is harmless; conflicting evidence is refused.
    pub fn finish_in(
        conn: &rusqlite::Transaction<'_>,
        reference: &SpendReceiptRef,
        state: SpendState,
        usage: Option<UsageTrace>,
        output: Option<serde_json::Value>,
    ) -> Result<(), ModelError> {
        let old = receipt_in(conn, reference, chrono::Utc::now())?.ok_or_else(conflict)?;
        if (state == SpendState::Settled
            && usage
                .as_ref()
                .is_none_or(|u| !symbiotic_model::has_measured_usage(u)))
            || (state == SpendState::Released && (old.output.is_some() || output.is_some()))
        {
            return Err(conflict());
        }
        // One owner converts all successful outputs to content-free accounting evidence.
        let output = output
            .map(|_| serde_json::json!({"output_received": true}))
            .or_else(|| old.output.clone());
        let usage = usage
            .map(|u| serde_json::to_string(&u))
            .transpose()
            .map_err(storage)?;
        let output = output
            .map(|o| serde_json::to_string(&o))
            .transpose()
            .map_err(storage)?;
        if old.state != SpendState::Unknown {
            if old.state == state
                && serde_json::to_string(&old.usage).map_err(storage)?
                    == usage.clone().unwrap_or_else(|| "null".into())
                && serde_json::to_string(&old.output).map_err(storage)?
                    == output.clone().unwrap_or_else(|| "null".into())
            {
                return Ok(());
            }
            return Err(conflict());
        }
        if state == SpendState::Released {
            let changed = conn
                .execute(
                    "UPDATE spend_accounts SET used=used-1 WHERE account=?1 AND used>0",
                    [&old.reservation.account],
                )
                .map_err(storage)?;
            if changed != 1 {
                return Err(storage(()));
            }
        }
        conn.execute(
            "UPDATE spend_receipts SET state=?2, usage=?3, output=?4 WHERE reference=?1",
            params![reference.as_str(), state_name(state), usage, output],
        )
        .map_err(storage)?;
        Ok(())
    }
}

pub(crate) fn receipt_in(
    conn: &Connection,
    reference: &SpendReceiptRef,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<Option<SpendReceipt>, ModelError> {
    let row = conn
        .query_row(
            "SELECT reservation, state, usage, output, pre_dispatch_released, attempt_limit, attempts_used, CASE WHEN recovery_expires_at>?2 THEN recovery ELSE NULL END FROM spend_receipts WHERE reference=?1",
            params![reference.as_str(), now.to_rfc3339()],
            |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, Option<String>>(2)?,
                    r.get::<_, Option<String>>(3)?,
                    r.get::<_, bool>(4)?,
                    r.get::<_, Option<u32>>(5)?,
                    r.get::<_, u32>(6)?,
                    r.get::<_, Option<String>>(7)?,
                ))
            },
        )
        .optional()
        .map_err(storage)?;
    row.map(
        |(
            r,
            state,
            usage,
            output,
            pre_dispatch_released,
            attempt_limit,
            attempts_used,
            recovery,
        )| {
            Ok(SpendReceipt {
                reservation: serde_json::from_str(&r).map_err(storage)?,
                pre_dispatch_released,
                attempt_limit,
                attempts_used,
                recovery: recovery
                    .map(|v| serde_json::from_str(&v))
                    .transpose()
                    .map_err(storage)?,
                state: match state.as_str() {
                    "unknown" => SpendState::Unknown,
                    "released" => SpendState::Released,
                    "settled" => SpendState::Settled,
                    _ => return Err(storage(())),
                },
                usage: usage
                    .map(|u| serde_json::from_str(&u))
                    .transpose()
                    .map_err(storage)?,
                output: output
                    .map(|o| serde_json::from_str(&o))
                    .transpose()
                    .map_err(storage)?,
            })
        },
    )
    .transpose()
}
/// Find the latest accepted receipt reference without loading receipt content.
pub(crate) fn invocation_reference_in(
    conn: &Connection,
    account: &str,
    invocation: &str,
) -> Result<Option<SpendReceiptRef>, ModelError> {
    let reference: Option<String> = conn.query_row("SELECT reference FROM spend_receipts WHERE account=?1 AND invocation=?2 ORDER BY rowid DESC LIMIT 1", params![account, invocation], |r| r.get(0)).optional().map_err(storage)?;
    reference
        .map(|r| SpendReceiptRef::new(r).map_err(storage))
        .transpose()
}
pub(crate) fn invocation_in(
    conn: &Connection,
    account: &str,
    invocation: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<Option<SpendReceipt>, ModelError> {
    invocation_reference_in(conn, account, invocation)?
        .map(|reference| receipt_in(conn, &reference, now))
        .transpose()
        .map(Option::flatten)
}

impl SpendLedger for SqliteSpendLedger {
    fn reserve_explicit(&self, r: &SpendReservation, limit: u32) -> Result<bool, ModelError> {
        let mut conn = self.0.lock().map_err(storage)?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage)?;
        let accepted = Self::reserve_with_limit_in(&tx, r, Some(limit), false)?;
        tx.commit().map_err(storage)?;
        Ok(accepted)
    }
    fn discard_recovery(&self, account: &str, invocation: &str) -> Result<(), ModelError> {
        let conn = self.0.lock().map_err(storage)?;
        conn.execute("UPDATE spend_receipts SET recovery=NULL, recovery_expires_at=NULL WHERE reference=(
            SELECT reference FROM spend_receipts WHERE account=?1 AND invocation=?2 ORDER BY rowid DESC LIMIT 1)", params![account, invocation]).map_err(storage)?;
        Ok(())
    }
    fn purge_recovery(
        &self,
        matches: &dyn Fn(&serde_json::Value) -> Result<bool, ModelError>,
    ) -> Result<usize, ModelError> {
        let mut conn = self.0.lock().map_err(storage)?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage)?;
        let mut references = Vec::new();
        {
            // Only live payload rows, never accounting history or implicit markers.
            let mut stmt = tx
                .prepare(
                    "SELECT reference, recovery FROM spend_receipts WHERE recovery IS NOT NULL",
                )
                .map_err(storage)?;
            let rows = stmt
                .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))
                .map_err(storage)?;
            for row in rows {
                let (reference, output) = row.map_err(storage)?;
                let value = serde_json::from_str(&output).map_err(storage)?;
                if matches(&value)? {
                    references.push(reference);
                }
            }
        }
        // Finish the indexed read before deleting its entries.
        let mut removed = 0;
        for reference in references {
            removed += tx.execute("UPDATE spend_receipts SET recovery=NULL, recovery_expires_at=NULL WHERE reference=?1", [reference]).map_err(storage)?;
        }
        tx.commit().map_err(storage)?;
        Ok(removed)
    }
    fn release_before_dispatch(&self, reference: &SpendReceiptRef) -> Result<(), ModelError> {
        let mut conn = self.0.lock().map_err(storage)?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage)?;
        Self::release_in(&tx, reference)?;
        tx.commit().map_err(storage)
    }
    fn reserve(&self, r: &SpendReservation) -> Result<bool, ModelError> {
        let mut conn = self.0.lock().map_err(storage)?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage)?;
        let accepted = Self::reserve_in(&tx, r)?;
        tx.commit().map_err(storage)?;
        Ok(accepted)
    }
    fn acquire_handoff(
        &self,
        handoff: &AcceptedSpendHandoff,
        account: &str,
        input_identity: &str,
        owner: &str,
    ) -> Result<(), ModelError> {
        let mut conn = self.0.lock().map_err(storage)?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage)?;
        let old = receipt_in(&tx, &handoff.reservation.reference, chrono::Utc::now())?
            .ok_or_else(conflict)?;
        if old.reservation != handoff.reservation
            || old.state != SpendState::Unknown
            || old.reservation.account != account
            || handoff.input_identity != input_identity
            || owner.is_empty()
        {
            return Err(conflict());
        }
        let changed = tx
            .execute(
                "UPDATE spend_receipts SET dispatch_owner=?3 WHERE reference=?1
             AND handoff_input=?2 AND dispatch_owner IS NULL AND state='unknown'",
                params![
                    handoff.reservation.reference.as_str(),
                    input_identity,
                    owner
                ],
            )
            .map_err(storage)?;
        if changed != 1 {
            return Err(conflict());
        }
        tx.commit().map_err(storage)
    }
    fn receipt(&self, reference: &SpendReceiptRef) -> Result<Option<SpendReceipt>, ModelError> {
        let conn = self.0.lock().map_err(storage)?;
        receipt_in(&conn, reference, self.now())
    }
    fn invocation(
        &self,
        account: &str,
        invocation: &str,
    ) -> Result<Option<SpendReceipt>, ModelError> {
        let conn = self.0.lock().map_err(storage)?;
        invocation_in(&conn, account, invocation, self.now())
    }
    fn finish(
        &self,
        r: &SpendReceiptRef,
        state: SpendState,
        usage: Option<UsageTrace>,
        output: Option<serde_json::Value>,
        invocation: Option<&str>,
    ) -> Result<(), ModelError> {
        let mut conn = self.0.lock().map_err(storage)?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage)?;
        Self::save_recovery_in(&tx, r, &output, self.1, None, invocation, self.now())?;
        Self::finish_in(&tx, r, state, usage, output)?;
        tx.commit().map_err(storage)
    }
}
