//! Backend-owned job SQL, also usable on the ledger's caller-supplied transaction.
use super::SqliteQueue;
use chrono::{DateTime, Duration, Utc};
use rusqlite::{Connection, OptionalExtension, Row, Transaction, params};
use serde::{Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use symbiotic_core::{DiagnosticCode, QueueItemId};
use symbiotic_queue::jobs::*;

type SqlProjection<'a, T> = (&'a str, fn(&Row<'_>) -> rusqlite::Result<T>);

const COLUMNS: &str = "scope, id, key, digest, job_group, owners, kind, execution, state, final_state, payload, max_attempts, generation, lease_until, cancel_requested, purged, output, origin, receipt, recovery_until, result_expired, created_at, finished_at, delivery_generation, delivery_until, diagnostic, output_bytes, admission";

fn storage(_: impl std::fmt::Display) -> JobError {
    JobError::Storage
}
fn json(value: &impl Serialize) -> Result<String, JobError> {
    serde_json::to_string(value).map_err(storage)
}
fn optional_json<T: Serialize>(value: &Option<T>) -> Result<Option<String>, JobError> {
    value.as_ref().map(json).transpose()
}
fn stamp(time: DateTime<Utc>) -> i64 {
    time.timestamp_millis()
}
fn read_time(row: &Row<'_>, index: usize) -> rusqlite::Result<DateTime<Utc>> {
    let raw: i64 = row.get(index)?;
    DateTime::from_timestamp_millis(raw).ok_or_else(|| {
        rusqlite::Error::FromSqlConversionFailure(
            index,
            rusqlite::types::Type::Integer,
            Box::new(JobError::InvalidRequest),
        )
    })
}
fn read_optional_time(row: &Row<'_>, index: usize) -> rusqlite::Result<Option<DateTime<Utc>>> {
    let raw: Option<i64> = row.get(index)?;
    raw.map(|_| read_time(row, index)).transpose()
}
fn read_json<T: DeserializeOwned>(row: &Row<'_>, index: usize) -> rusqlite::Result<T> {
    let raw: String = row.get(index)?;
    serde_json::from_str(&raw).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(index, rusqlite::types::Type::Text, Box::new(e))
    })
}
fn read_optional_json<T: DeserializeOwned>(
    row: &Row<'_>,
    index: usize,
) -> rusqlite::Result<Option<T>> {
    let raw: Option<String> = row.get(index)?;
    raw.map(|s| {
        serde_json::from_str(&s).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(
                index,
                rusqlite::types::Type::Text,
                Box::new(e),
            )
        })
    })
    .transpose()
}
fn read_record(row: &Row<'_>) -> rusqlite::Result<JobRecord> {
    let state: JobState = read_json(row, 8)?;
    if state.acked() {
        // Only these scoped tombstone fields survive confirmation. The shared
        // record API fills absent operational fields with neutral values.
        return Ok(JobRecord {
            id: JobId {
                scope: read_json(row, 0)?,
                id: row.get(1)?,
            },
            key: row.get(2)?,
            digest: row.get(3)?,
            state,
            final_state: read_optional_json(row, 9)?,
            receipt: row.get(18)?,
            delivery_generation: row.get(23)?,
            group: None,
            owners: Vec::new(),
            kind: row.get::<_, Option<String>>(6)?.unwrap_or_default(),
            execution: if row.get::<_, Option<String>>(6)?.is_some() {
                Execution::Model
            } else {
                Execution::Handler
            },
            payload: None,
            admission: None,
            max_attempts: 0,
            generation: 0,
            lease_until: None,
            cancel_requested: false,
            purged: false,
            output: None,
            output_bytes: 0,
            origin: None,
            recovery_until: None,
            result_expired: false,
            created_at: DateTime::UNIX_EPOCH,
            finished_at: None,
            delivery_until: None,
            diagnostic: None,
        });
    }
    Ok(JobRecord {
        id: JobId {
            scope: read_json(row, 0)?,
            id: row.get(1)?,
        },
        key: row.get(2)?,
        digest: row.get(3)?,
        group: row.get(4)?,
        owners: read_json(row, 5)?,
        kind: row.get(6)?,
        execution: read_json(row, 7)?,
        state: read_json(row, 8)?,
        final_state: read_optional_json(row, 9)?,
        payload: row.get(10)?,
        max_attempts: row.get(11)?,
        generation: row.get(12)?,
        lease_until: read_optional_time(row, 13)?,
        cancel_requested: row.get(14)?,
        purged: row.get(15)?,
        output: row.get(16)?,
        origin: read_optional_json(row, 17)?,
        receipt: row.get(18)?,
        recovery_until: read_optional_time(row, 19)?,
        result_expired: row.get(20)?,
        created_at: read_time(row, 21)?,
        finished_at: read_optional_time(row, 22)?,
        delivery_generation: row.get(23)?,
        delivery_until: read_optional_time(row, 24)?,
        diagnostic: read_optional_json(row, 25)?,
        output_bytes: row.get(26)?,
        admission: row.get(27)?,
    })
}

/// Initialize job tables as part of the queue's atomic format initialization.
pub(super) fn initialize(tx: &Transaction<'_>) -> Result<(), rusqlite::Error> {
    tx.execute_batch(
        "CREATE TABLE model_job_bindings (
        scope TEXT NOT NULL, kind TEXT NOT NULL, binding TEXT NOT NULL,
        PRIMARY KEY(scope, kind)
    );
    CREATE TABLE jobs (
        scope TEXT NOT NULL,
        id TEXT NOT NULL,
        key TEXT NOT NULL,
        digest TEXT NOT NULL,
        job_group TEXT,
        owners TEXT,
        kind TEXT,
        execution TEXT,
        state TEXT NOT NULL,
        final_state TEXT,
        payload BLOB,
        max_attempts INTEGER,
        generation INTEGER,
        lease_until INTEGER,
        cancel_requested INTEGER,
        purged INTEGER,
        output BLOB,
        origin TEXT,
        receipt TEXT,
        recovery_until INTEGER,
        result_expired INTEGER,
        created_at INTEGER,
        finished_at INTEGER,
        delivery_generation INTEGER NOT NULL,
        delivery_until INTEGER,
        diagnostic TEXT,
        output_bytes INTEGER,
        admission BLOB,
        PRIMARY KEY(scope, id), UNIQUE(scope, key)
    );
    CREATE INDEX jobs_claim ON jobs(scope, created_at, id)
        WHERE state IN ('\"Pending\"','\"AwaitingAdmission\"','\"Running\"','\"Uncertain\"');
    CREATE INDEX jobs_delivery ON jobs(scope, finished_at, id)
        WHERE state IN ('\"Succeeded\"','\"Failed\"','\"Cancelled\"','\"Refused\"','\"Purged\"');
    CREATE INDEX jobs_expiry ON jobs(scope, recovery_until, id)
        WHERE result_expired=0 AND state IN ('\"Succeeded\"','\"Failed\"','\"Cancelled\"','\"Refused\"','\"Purged\"');
    CREATE INDEX jobs_unfinished_group ON jobs(scope, job_group, id)
        WHERE state IN ('\"Pending\"','\"AwaitingAdmission\"','\"Running\"','\"Uncertain\"');
    CREATE INDEX jobs_diagnostics ON jobs(scope, job_group, id) WHERE state IN ('\"Failed\"','\"Uncertain\"','\"AwaitingAdmission\"');
    CREATE TABLE job_owners (
        scope TEXT NOT NULL, owner TEXT NOT NULL, job_id TEXT NOT NULL,
        PRIMARY KEY(scope, owner, job_id)
    );
    CREATE INDEX job_owners_job ON job_owners(scope, job_id);
",
    )
}

/// All SQL stays in this backend; dropping the caller's transaction rolls it back.
/// The caller must use an IMMEDIATE transaction to serialize checks, ledger and
/// job writes, and commit only after every participant succeeds. The connection
/// must first have been initialized with [`SqliteQueue::initialize_connection`].
/// Sample `now` after acquiring that transaction and reuse it across participants.
pub fn jobs_in_transaction(
    tx: &mut Transaction<'_>,
    scope: &JobScope,
    config: &JobConfig,
    now: DateTime<Utc>,
    request: JobRequest,
) -> Result<JobResponse, JobError> {
    let savepoint = tx.savepoint().map_err(storage)?;
    let response = apply_job_request(&mut SqlRows::new(&savepoint), scope, config, now, request)?;
    savepoint.commit().map_err(storage)?;
    Ok(response)
}

/// Derive whether a scoped kind needs execution or recovery through the unfinished
/// job index. Jobs awaiting renewed admission do not require an execution owner.
/// Reads at most one content-free match; work never visits retained final history.
pub fn jobs_need_execution_in_transaction(
    tx: &Transaction<'_>,
    scope: &JobScope,
    kind: String,
    now: DateTime<Utc>,
) -> Result<bool, JobError> {
    Ok(!SqlRows::new(tx)
        .select_with(scope, JobQuery::Execution(kind), now, 1, ("1", |_| Ok(())))?
        .is_empty())
}

/// Receipt owners affected by deletion, selected before the transition removes
/// owner membership or tombstones metadata. Work is bounded by the live population.
/// SQL for job selection remains exclusively in this backend.
pub fn paid_copies_for_request(
    tx: &Transaction<'_>,
    scope: &JobScope,
    config: &JobConfig,
    now: DateTime<Utc>,
    request: &JobRequest,
) -> Result<Vec<JobRecord>, JobError> {
    let mut rows = SqlRows::new(tx);
    let ids = match request {
        JobRequest::Ack(acks) => {
            if acks.len() > config.max_batch {
                return Err(JobError::InvalidRequest);
            }
            acks.iter()
                .map(|(token, _)| {
                    scoped(scope, &token.job)?;
                    Ok(token.job.clone())
                })
                .collect::<Result<Vec<_>, JobError>>()?
        }
        JobRequest::PurgeOwner(owner) => {
            let live = rows.live_count(scope)?;
            rows.inspect(scope, JobQuery::Owner(owner.clone()), now, live)?
                .into_iter()
                .map(|r| r.id)
                .collect()
        }
        JobRequest::Maintain => rows
            .inspect(scope, JobQuery::Expired, now, config.maintenance_batch)?
            .into_iter()
            .map(|r| r.id)
            .collect(),
        _ => Vec::new(),
    };
    ids.into_iter().map(|id| rows.metadata(&id)).collect()
}

struct SqlRows<'a> {
    conn: &'a Connection,
    #[cfg(test)]
    metrics: Option<&'a QueryMetrics>,
}
#[cfg(test)]
#[derive(Default)]
struct QueryMetrics {
    steps: std::cell::Cell<i32>,
    plans: std::cell::RefCell<Vec<String>>,
}
impl<'a> SqlRows<'a> {
    fn new(conn: &'a Connection) -> Self {
        Self {
            conn,
            #[cfg(test)]
            metrics: None,
        }
    }
}
impl SqlRows<'_> {
    fn get(&mut self, id: &JobId) -> Result<Option<JobRecord>, JobError> {
        self.conn
            .query_row(
                &format!("SELECT {COLUMNS} FROM jobs WHERE scope=?1 AND id=?2"),
                params![json(&id.scope)?, id.id],
                read_record,
            )
            .optional()
            .map_err(storage)
    }
    fn by_key(&mut self, scope: &JobScope, key: &str) -> Result<Option<JobRecord>, JobError> {
        self.conn
            .query_row(
                &format!("SELECT {COLUMNS} FROM jobs WHERE scope=?1 AND key=?2"),
                params![json(scope)?, key],
                read_record,
            )
            .optional()
            .map_err(storage)
    }
    fn select(
        &mut self,
        scope: &JobScope,
        query: JobQuery,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<JobRecord>, JobError> {
        self.select_with(scope, query, now, limit, (COLUMNS, read_record))
    }
    fn inspect(
        &mut self,
        scope: &JobScope,
        query: JobQuery,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<JobInfo>, JobError> {
        self.select_with(
            scope,
            query,
            now,
            limit,
            ("scope, id, state, diagnostic, output_bytes", |r| {
                Ok(JobInfo {
                    id: JobId {
                        scope: read_json(r, 0)?,
                        id: r.get(1)?,
                    },
                    state: read_json(r, 2)?,
                    diagnostic: read_optional_json(r, 3)?,
                    output_bytes: r.get(4)?,
                })
            }),
        )
    }
    fn metadata(&mut self, id: &JobId) -> Result<JobRecord, JobError> {
        let columns = COLUMNS
            .split(", ")
            .map(|c| match c {
                "payload" | "output" | "admission" => "NULL",
                _ => c,
            })
            .collect::<Vec<_>>()
            .join(", ");
        self.conn
            .query_row(
                &format!("SELECT {columns} FROM jobs WHERE scope=?1 AND id=?2"),
                params![json(&id.scope)?, id.id],
                read_record,
            )
            .optional()
            .map_err(storage)?
            .ok_or(JobError::NotFound)
    }
    fn output(&mut self, id: &JobId) -> Result<Option<Vec<u8>>, JobError> {
        self.conn
            .query_row(
                "SELECT output FROM jobs WHERE scope=?1 AND id=?2",
                params![json(&id.scope)?, id.id],
                |r| r.get(0),
            )
            .map_err(storage)
    }
    fn recovery_bytes(&mut self, id: &JobId) -> Result<usize, JobError> {
        self.conn
            .query_row(
                "SELECT coalesce(length(CAST(payload AS BLOB)),0) + coalesce(length(CAST(admission AS BLOB)),0) + coalesce(length(CAST(output AS BLOB)),0) + coalesce((SELECT length(CAST(recovery AS BLOB)) FROM spend_receipts WHERE reference=jobs.receipt),0) FROM jobs WHERE scope=?1 AND id=?2",
                params![json(&id.scope)?, id.id],
                |r| r.get(0),
            )
            .map_err(storage)
    }
    fn deliver(&mut self, row: &JobRecord, expired: bool) -> Result<(), JobError> {
        if expired {
            self.expire(&row.id)?;
        }
        self.conn.execute("UPDATE jobs SET delivery_generation=?3, delivery_until=?4 WHERE scope=?1 AND id=?2", params![json(&row.id.scope)?, row.id.id, i64::try_from(row.delivery_generation).map_err(storage)?, row.delivery_until.map(stamp)]).map_err(storage)?;
        Ok(())
    }
    fn expire(&mut self, id: &JobId) -> Result<(), JobError> {
        self.conn.execute("UPDATE jobs SET result_expired=1, payload=NULL, admission=NULL, output=NULL, output_bytes=0 WHERE scope=?1 AND id=?2", params![json(&id.scope)?, id.id]).map_err(storage)?;
        Ok(())
    }
    fn save(&mut self, row: JobRecord) -> Result<(), JobError> {
        if row.state.acked() {
            // One persistence owner narrows every confirmed write, including
            // future callers of save. Model kind retains the frozen binding for sticky erasure.
            self.conn.execute("INSERT INTO jobs (scope,id,key,digest,state,final_state,receipt,delivery_generation,kind) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9) ON CONFLICT(scope,id) DO UPDATE SET key=excluded.key,digest=excluded.digest,state=excluded.state,final_state=excluded.final_state,receipt=excluded.receipt,delivery_generation=excluded.delivery_generation,job_group=NULL,owners=NULL,kind=excluded.kind,execution=NULL,payload=NULL,admission=NULL,max_attempts=NULL,generation=NULL,lease_until=NULL,cancel_requested=NULL,purged=NULL,output=NULL,origin=NULL,recovery_until=NULL,result_expired=NULL,created_at=NULL,finished_at=NULL,delivery_until=NULL,diagnostic=NULL,output_bytes=NULL",
                params![json(&row.id.scope)?, row.id.id, row.key, row.digest, json(&row.state)?, optional_json(&row.final_state)?, row.receipt, i64::try_from(row.delivery_generation).map_err(storage)?, (row.execution == Execution::Model).then_some(&row.kind)]).map_err(storage)?;
            self.conn
                .execute(
                    "DELETE FROM job_owners WHERE scope=?1 AND job_id=?2",
                    params![json(&row.id.scope)?, row.id.id],
                )
                .map_err(storage)?;
            return Ok(());
        }
        // Validate before touching SQL, also when used directly by backend tests.
        millisecond_time(row.created_at)?;
        for time in [
            row.lease_until,
            row.recovery_until,
            row.finished_at,
            row.delivery_until,
        ]
        .into_iter()
        .flatten()
        {
            millisecond_time(time)?;
        }
        let old = self.get(&row.id)?;
        self.conn.execute("INSERT INTO jobs (scope, id, key, digest, job_group, owners, kind, execution, state, final_state, payload, max_attempts, generation, lease_until, cancel_requested, purged, output, origin, receipt, recovery_until, result_expired, created_at, finished_at, delivery_generation, delivery_until, diagnostic, output_bytes, admission) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28) ON CONFLICT(scope,id) DO UPDATE SET key=excluded.key, digest=excluded.digest, job_group=excluded.job_group, owners=excluded.owners, kind=excluded.kind, execution=excluded.execution, state=excluded.state, final_state=excluded.final_state, payload=excluded.payload, max_attempts=excluded.max_attempts, generation=excluded.generation, lease_until=excluded.lease_until, cancel_requested=excluded.cancel_requested, purged=excluded.purged, output=excluded.output, origin=excluded.origin, receipt=excluded.receipt, recovery_until=excluded.recovery_until, result_expired=excluded.result_expired, created_at=excluded.created_at, finished_at=excluded.finished_at, delivery_generation=excluded.delivery_generation, delivery_until=excluded.delivery_until, diagnostic=excluded.diagnostic, output_bytes=excluded.output_bytes, admission=excluded.admission",
            params![
                json(&row.id.scope)?,
                &row.id.id,
                &row.key,
                &row.digest,
                &row.group,
                json(&row.owners)?,
                &row.kind,
                json(&row.execution)?,
                json(&row.state)?,
                optional_json(&row.final_state)?,
                &row.payload,
                i64::from(row.max_attempts),
                i64::try_from(row.generation).map_err(storage)?,
                row.lease_until.map(stamp),
                &row.cancel_requested,
                &row.purged,
                &row.output,
                optional_json(&row.origin)?,
                &row.receipt,
                row.recovery_until.map(stamp),
                &row.result_expired,
                stamp(row.created_at),
                row.finished_at.map(stamp),
                i64::try_from(row.delivery_generation).map_err(storage)?,
                row.delivery_until.map(stamp),
                optional_json(&row.diagnostic)?,
                i64::try_from(stored_output_bytes(&row)?).map_err(storage)?,
                &row.admission,
            ]).map_err(storage)?;
        if old.as_ref().is_none_or(|old| old.owners != row.owners) {
            self.conn
                .execute(
                    "DELETE FROM job_owners WHERE scope=?1 AND job_id=?2",
                    params![json(&row.id.scope)?, row.id.id],
                )
                .map_err(storage)?;
            for owner in &row.owners {
                self.conn
                    .execute(
                        "INSERT INTO job_owners(scope,owner,job_id) VALUES (?1,?2,?3) ON CONFLICT(scope,owner,job_id) DO NOTHING",
                        params![json(&row.id.scope)?, owner, row.id.id],
                    )
                    .map_err(storage)?;
            }
        }
        Ok(())
    }
    fn usage(&mut self, scope: &JobScope) -> Result<PendingUsage, JobError> {
        // The JSON tuple has exactly the same canonical fields as job_input_bytes.
        // CAST to BLOB counts UTF-8 bytes, rather than SQLite's character count.
        self.conn.query_row("SELECT count(*), coalesce(sum(length(CAST(json_array(json(scope),key,job_group,json(owners),kind,json(execution),max_attempts) AS BLOB))+coalesce(length(payload),0)+coalesce(length(admission),0)),0) FROM jobs INDEXED BY jobs_claim WHERE scope=?1 AND state IN ('\"Pending\"','\"AwaitingAdmission\"','\"Running\"','\"Uncertain\"')",
            [json(scope)?], |r| Ok(PendingUsage { items: r.get(0)?, bytes: r.get(1)? })).map_err(storage)
    }
    fn live_count(&self, scope: &JobScope) -> Result<usize, JobError> {
        self.conn.query_row("SELECT (SELECT count(*) FROM jobs INDEXED BY jobs_claim WHERE scope=?1 AND state IN ('\"Pending\"','\"AwaitingAdmission\"','\"Running\"','\"Uncertain\"')) + (SELECT count(*) FROM jobs INDEXED BY jobs_delivery WHERE scope=?1 AND state IN ('\"Succeeded\"','\"Failed\"','\"Cancelled\"','\"Refused\"','\"Purged\"'))", [json(scope)?], |r| r.get(0)).map_err(storage)
    }
    fn select_with<T>(
        &mut self,
        scope: &JobScope,
        query: JobQuery,
        now: DateTime<Utc>,
        limit: usize,
        projection: SqlProjection<'_, T>,
    ) -> Result<Vec<T>, JobError> {
        let (columns, decode) = projection;
        let bound = i64::try_from(limit).map_err(|_| JobError::InvalidRequest)?;
        if let JobQuery::Owner(owner) = query {
            let columns = columns
                .split(", ")
                .map(|c| format!("jobs.{c}"))
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "SELECT {columns} FROM job_owners JOIN jobs ON jobs.scope=job_owners.scope AND jobs.id=job_owners.job_id WHERE job_owners.scope=?1 AND job_owners.owner=?2 ORDER BY job_owners.job_id LIMIT ?3"
            );
            return self.read(
                &sql,
                vec![json(scope)?.into(), owner.into(), bound.into()],
                decode,
            );
        }
        let mut args = vec![rusqlite::types::Value::Text(json(scope)?)];
        let (filter, order, index) = match query {
            JobQuery::Admission { after } => {
                args.push(after.unwrap_or_default().into());
                ("state IN ('\"Pending\"','\"AwaitingAdmission\"','\"Running\"','\"Uncertain\"') AND state='\"AwaitingAdmission\"' AND id>?2".to_string(), "id", "jobs_claim")
            }
            JobQuery::Execution(kind) => {
                args.push(kind.into());
                ("state IN ('\"Pending\"','\"AwaitingAdmission\"','\"Running\"','\"Uncertain\"') AND state!='\"AwaitingAdmission\"' AND kind=?2".to_string(), "created_at, id", "jobs_claim")
            }
            JobQuery::Group(group) => {
                args.push(group.into());
                (
                    "job_group=?2 AND state IN ('\"Pending\"','\"AwaitingAdmission\"','\"Running\"','\"Uncertain\"')"
                        .to_string(),
                    "id",
                    "jobs_unfinished_group",
                )
            }
            JobQuery::Pending { kinds } => {
                args.push(json(&kinds)?.into());
                args.push(stamp(now).into());
                let filter = "state IN ('\"Pending\"','\"AwaitingAdmission\"','\"Running\"','\"Uncertain\"') AND (state='\"Pending\"' OR (state='\"Running\"' AND execution='\"Handler\"' AND lease_until<=?3)) AND kind IN (SELECT value FROM json_each(?2))".to_string();
                (filter, "created_at, id", "jobs_claim")
            }
            JobQuery::Recovery { kinds, after } => {
                args.push(json(&kinds)?.into());
                args.push(stamp(now).into());
                args.push(after.unwrap_or_default().into());
                ("state IN ('\"Pending\"','\"AwaitingAdmission\"','\"Running\"','\"Uncertain\"') AND execution='\"Model\"' AND (state='\"Uncertain\"' OR (state='\"Running\"' AND lease_until<=?3)) AND kind IN (SELECT value FROM json_each(?2)) AND id>?4".to_string(), "id", "jobs_claim")
            }
            JobQuery::Final => {
                args.push(stamp(now).into());
                ("state IN ('\"Succeeded\"','\"Failed\"','\"Cancelled\"','\"Refused\"','\"Purged\"') AND (delivery_until IS NULL OR delivery_until<=?2)".to_string(), "finished_at, id", "jobs_delivery")
            }
            JobQuery::Expired => {
                args.push(stamp(now).into());
                ("state IN ('\"Succeeded\"','\"Failed\"','\"Cancelled\"','\"Refused\"','\"Purged\"') AND result_expired=0 AND recovery_until<=?2".to_string(), "recovery_until, id", "jobs_expiry")
            }
            JobQuery::Diagnostics { group, after } => {
                args.push(group.into());
                args.push(after.unwrap_or_default().into());
                (
                    "job_group=?2 AND state IN ('\"Failed\"','\"Uncertain\"','\"AwaitingAdmission\"') AND id>?3"
                        .to_string(),
                    "id",
                    "jobs_diagnostics",
                )
            }
            JobQuery::Owner(_) => return Err(JobError::InvalidRequest),
        };
        args.push(bound.into());
        let sql = format!(
            "SELECT {columns} FROM jobs INDEXED BY {index} WHERE scope=?1 AND {filter} ORDER BY {order} LIMIT ?{}",
            args.len()
        );
        self.read(&sql, args, decode)
    }
    fn read<T>(
        &self,
        sql: &str,
        args: Vec<rusqlite::types::Value>,
        decode: fn(&Row<'_>) -> rusqlite::Result<T>,
    ) -> Result<Vec<T>, JobError> {
        let mut stmt = self.conn.prepare(sql).map_err(storage)?;
        let records = stmt
            .query_map(rusqlite::params_from_iter(&args), decode)
            .map_err(storage)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(storage)?;
        #[cfg(test)]
        if let Some(metrics) = self.metrics {
            metrics
                .steps
                .set(metrics.steps.get() + stmt.get_status(rusqlite::StatementStatus::VmStep));
            let mut plan = self
                .conn
                .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
                .map_err(storage)?;
            let details = plan
                .query_map(rusqlite::params_from_iter(&args), |r| r.get::<_, String>(3))
                .map_err(storage)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(storage)?;
            metrics.plans.borrow_mut().extend(details);
        }
        Ok(records)
    }
}

impl SqliteQueue {
    pub(super) fn job_operation(
        &self,
        scope: &JobScope,
        config: &JobConfig,
        clock: impl FnOnce() -> DateTime<Utc>,
        request: JobRequest,
    ) -> Result<JobResponse, JobError> {
        let mut conn = self.conn.lock().map_err(storage)?;
        let mut tx = conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(storage)?;
        let response = jobs_in_transaction(&mut tx, scope, config, clock(), request)?;
        tx.commit().map_err(storage)?;
        Ok(response)
    }
}

/// Backend row selection; all operational pages are bounded at the storage read.
#[derive(Clone, Debug)]
enum JobQuery {
    Admission {
        after: Option<String>,
    },
    /// Unfinished work requiring execution or recovery, excluding admission waits.
    Execution(String),
    /// Unfinished scoped group scan for atomic group cancellation.
    Group(String),
    /// Indexed owner membership, chunked in ascending ID order.
    Owner(String),
    /// Waiting claims within handler kinds, FIFO.
    Pending {
        kinds: Vec<String>,
    },
    /// Expired model claims and unresolved paid attempts, ascending ID.
    Recovery {
        kinds: Vec<String>,
        after: Option<String>,
    },
    /// Unleased final deliveries, oldest first.
    Final,
    /// Final recovery copies past deadline.
    Expired,
    /// Static diagnostic states, ascending ID.
    Diagnostics {
        group: String,
        after: Option<String>,
    },
}

/// Content-free operational projection from canonical rows.
#[derive(Clone, Debug)]
struct JobInfo {
    /// Scoped identity.
    pub id: JobId,
    /// Lifecycle.
    pub state: JobState,
    /// Static diagnostic.
    pub diagnostic: Option<DiagnosticCode>,
    /// Encoded saved-answer length.
    pub output_bytes: usize,
}

// Waiting work and expired unpaid claims can stop without ledger reconciliation.
// Cancel and erasure use the same rule so neither strands an expired handler.
fn claimable(row: &JobRecord, now: DateTime<Utc>) -> bool {
    row.state == JobState::Pending
        || row.state == JobState::Running
            && row.execution == Execution::Handler
            && row.lease_until.is_some_and(|until| until <= now)
}

// Cancellation and reclaim share the sticky erasure precedence.
fn stopped_state(row: &JobRecord) -> JobState {
    if row.purged {
        JobState::Purged
    } else {
        JobState::Cancelled
    }
}

/// Ordered store timestamps use Unix milliseconds within chrono's supported range.
/// Ordinary submillisecond clock precision is quantized; leap seconds are refused.
fn millisecond_time(time: DateTime<Utc>) -> Result<DateTime<Utc>, JobError> {
    if time.timestamp_subsec_nanos() >= 1_000_000_000 {
        return Err(JobError::InvalidRequest);
    }
    DateTime::from_timestamp_millis(time.timestamp_millis()).ok_or(JobError::InvalidRequest)
}

fn deadline(now: DateTime<Utc>, seconds: u64) -> Result<DateTime<Utc>, JobError> {
    i64::try_from(seconds)
        .ok()
        .and_then(Duration::try_seconds)
        .and_then(|d| now.checked_add_signed(d))
        .ok_or(JobError::InvalidRequest)
}

fn scoped(scope: &JobScope, job: &JobId) -> Result<(), JobError> {
    if &job.scope == scope {
        Ok(())
    } else {
        Err(JobError::Scope)
    }
}

fn get(rows: &mut SqlRows<'_>, scope: &JobScope, id: &JobId) -> Result<JobRecord, JobError> {
    scoped(scope, id)?;
    rows.get(id)?.ok_or(JobError::NotFound)
}

fn live(row: &JobRecord, generation: u64, now: DateTime<Utc>) -> Result<(), JobError> {
    if row.state == JobState::Running
        && row.generation == generation
        && row.lease_until.is_some_and(|until| until > now)
    {
        Ok(())
    } else {
        Err(JobError::StaleClaim)
    }
}

fn delete_copies(row: &mut JobRecord) {
    row.payload = None;
    row.admission = None;
    row.output = None;
    row.output_bytes = 0;
}

fn finish(
    row: &mut JobRecord,
    state: JobState,
    now: DateTime<Utc>,
    config: &JobConfig,
) -> Result<(), JobError> {
    row.state = state;
    row.finished_at = Some(now);
    row.lease_until = None;
    if row.recovery_until.is_none() {
        row.recovery_until = Some(deadline(now, config.retention_seconds)?);
    }
    if row.recovery_until.is_some_and(|until| until <= now) {
        row.result_expired = true;
        delete_copies(row);
    }
    Ok(())
}

fn page(config: &JobConfig, limit: usize) -> Result<(), JobError> {
    if limit == 0 || limit > config.max_page {
        Err(JobError::InvalidRequest)
    } else {
        Ok(())
    }
}

// One byte-bound owner for all JSON-array pages: reject an individually oversized
// row, and defer a row that only exceeds the remaining aggregate page budget.
fn page_bytes(
    item: &impl Serialize,
    used: usize,
    max_bytes: usize,
    comma: bool,
    oversized: impl FnOnce(usize) -> JobError,
) -> Result<Option<usize>, JobError> {
    admit_page_bytes(encoded_bytes(item)?, used, max_bytes, comma, oversized)
}

fn admit_page_bytes(
    size: usize,
    used: usize,
    max_bytes: usize,
    comma: bool,
    oversized: impl FnOnce(usize) -> JobError,
) -> Result<Option<usize>, JobError> {
    let individual = size.checked_add(2).ok_or(JobError::InvalidRequest)?;
    if individual > max_bytes {
        return Err(oversized(individual));
    }
    let required = used
        .checked_add(size)
        .and_then(|b| b.checked_add(usize::from(comma)))
        .ok_or(JobError::InvalidRequest)?;
    Ok((required <= max_bytes).then_some(required))
}

fn candidate_page_bytes(
    row: &JobRecord,
    used: usize,
    max_bytes: usize,
    comma: bool,
) -> Result<Option<usize>, JobError> {
    page_bytes(row, used, max_bytes, comma, |bytes| {
        JobError::CandidateTooLarge {
            job: row.id.clone(),
            bytes,
        }
    })
}

// An encoded output is never empty (`[]`), so zero means "not yet counted".
fn stored_output_bytes(row: &JobRecord) -> Result<usize, JobError> {
    match &row.output {
        Some(_) if row.output_bytes > 0 => Ok(row.output_bytes),
        Some(output) => encoded_bytes(output),
        None => Ok(0),
    }
}

// Encoded delivery size from a content-free snapshot; a present output adds
// `,"output":` plus its encoded value.
fn delivery_bytes(delivery: &Delivery, output_bytes: Option<usize>) -> Result<usize, JobError> {
    let output = match output_bytes {
        Some(bytes) => bytes.checked_add(10).ok_or(JobError::Storage)?,
        None => 0,
    };
    encoded_bytes(delivery)?
        .checked_add(output)
        .ok_or(JobError::Storage)
}

// The one-item page length of a retained handler result that no legal completion
// page can carry. Until delivery, only the delivery ordinal and lease grow:
// measure their widest encodings.
fn undeliverable(row: &mut JobRecord, config: &JobConfig) -> Result<Option<usize>, JobError> {
    if row.output.is_none() {
        return Ok(None);
    }
    let content = (row.payload.take(), row.admission.take(), row.output.take());
    let snapshot = Delivery {
        completion: JobRecord {
            delivery_generation: u64::MAX,
            delivery_until: Some(DateTime::<Utc>::MAX_UTC),
            ..row.clone()
        },
        token: DeliveryToken {
            job: row.id.clone(),
            generation: u64::MAX,
        },
    };
    (row.payload, row.admission, row.output) = content;
    let size = delivery_bytes(&snapshot, Some(stored_output_bytes(row)?))?;
    let page = size.checked_add(2).ok_or(JobError::Storage)?;
    Ok((page > config.max_page_bytes).then_some(page))
}

// Enqueue is the sole creator of live rows and retained input. Both utilization
// checks derive from canonical rows under the same IMMEDIATE transaction.
fn insert_job(rows: &mut SqlRows<'_>, config: &JobConfig, row: JobRecord) -> Result<(), JobError> {
    // Model workers read candidate pages; handler runners claim rows directly.
    if row.execution == Execution::Model {
        candidate_page_bytes(&row, 2, config.max_page_bytes, false)?;
    }
    if rows.live_count(&row.id.scope)? >= config.max_live_jobs
        || rows
            .usage(&row.id.scope)?
            .bytes
            .checked_add(job_input_bytes(&row)?)
            .ok_or(JobError::Storage)?
            > config.max_pending_bytes
    {
        return Err(JobError::QueueFull);
    }
    rows.save(row)
}

fn claim_job(
    rows: &mut SqlRows<'_>,
    config: &JobConfig,
    now: DateTime<Utc>,
    mut row: JobRecord,
) -> Result<Option<JobRecord>, JobError> {
    if !claimable(&row, now) {
        return Ok(None);
    }
    if row.cancel_requested {
        let state = stopped_state(&row);
        finish(&mut row, state, now, config)?;
        delete_copies(&mut row);
        rows.save(row)?;
        return Ok(None);
    }
    if row.generation >= u64::from(row.max_attempts) {
        finish(&mut row, JobState::Refused, now, config)?;
        rows.save(row)?;
        return Ok(None);
    }
    row.state = JobState::Running;
    row.generation = row
        .generation
        .checked_add(1)
        .ok_or(JobError::InvalidRequest)?;
    row.lease_until = Some(deadline(now, config.claim_lease_seconds)?);
    rows.save(row.clone())?;
    Ok(Some(row))
}

/// Apply one SQLite operation with scoped lifecycle, bounds and fences.
/// Errors require rollback, including delivery leases.
fn apply_job_request(
    rows: &mut SqlRows<'_>,
    scope: &JobScope,
    config: &JobConfig,
    now: DateTime<Utc>,
    request: JobRequest,
) -> Result<JobResponse, JobError> {
    let now = millisecond_time(now)?;
    if config.version != 1
        || config.max_batch == 0
        || config.max_page == 0
        || config.max_live_jobs == 0
        || config.max_page_bytes < 2
        || config.max_result_bytes == 0
        || config.maintenance_batch == 0
        || config.maintenance_bytes_per_pass == 0
        || config.claim_lease_seconds == 0
        || config.delivery_lease_seconds == 0
        || [&scope.tenant, &scope.incarnation, &scope.queue]
            .iter()
            .any(|s| s.is_empty())
    {
        return Err(JobError::InvalidRequest);
    }
    // Reject overflowing time policies before any writes or attempt consumption.
    deadline(now, config.claim_lease_seconds)?;
    deadline(now, config.delivery_lease_seconds)?;
    deadline(now, config.retention_seconds)?;
    match request {
        JobRequest::AwaitAdmission(job) => {
            let mut row = get(rows, scope, &job)?;
            if row.state != JobState::Pending || row.execution != Execution::Model {
                return Err(JobError::InvalidRequest);
            }
            row.state = JobState::AwaitingAdmission;
            rows.save(row)?;
            Ok(JobResponse::Done)
        }
        JobRequest::Admit { job, admission } => {
            let mut row = get(rows, scope, &job)?;
            if !matches!(row.state, JobState::Pending | JobState::AwaitingAdmission)
                || row.execution != Execution::Model
                || admission.is_empty()
            {
                return Err(JobError::InvalidRequest);
            }
            if row.admission.as_deref() == Some(admission.as_slice()) {
                return Ok(JobResponse::Done);
            }
            row.admission = Some(admission);
            row.state = JobState::Pending;
            candidate_page_bytes(&row, 2, config.max_page_bytes, false)?;
            rows.save(row)?;
            if rows.usage(scope)?.bytes > config.max_pending_bytes {
                return Err(JobError::QueueFull);
            }
            Ok(JobResponse::Done)
        }
        JobRequest::AdmissionNotices { after, limit } => {
            page(config, limit)?;
            let notices = rows.select_with(
                scope,
                JobQuery::Admission { after },
                now,
                limit,
                ("scope, id, state, diagnostic", |r| {
                    Ok(JobDiagnostic {
                        id: JobId {
                            scope: read_json(r, 0)?,
                            id: r.get(1)?,
                        },
                        state: read_json(r, 2)?,
                        code: read_optional_json(r, 3)?,
                    })
                }),
            )?;
            Ok(JobResponse::Diagnostics(DiagnosticPage {
                after: notices.last().map(|n| n.id.id.clone()),
                items: notices,
            }))
        }
        JobRequest::Enqueue(specs) => {
            if specs.len() > config.max_batch {
                return Err(JobError::InvalidRequest);
            }
            let mut outcomes = Vec::with_capacity(specs.len());
            for mut spec in specs {
                if spec.key.is_empty() || spec.kind.is_empty() || spec.limits.max_attempts == 0 {
                    return Err(JobError::InvalidRequest);
                }
                spec.recovery_until = spec.recovery_until.map(millisecond_time).transpose()?;
                let digest = hex::encode(Sha256::digest(&spec.payload));
                if let Some(row) = rows.by_key(scope, &spec.key)? {
                    if row.digest != digest {
                        return Err(JobError::KeyConflict);
                    }
                    outcomes.push(if row.state.acked() {
                        Enqueued::AlreadyDone(Box::new(row))
                    } else {
                        Enqueued::Joined(row.id)
                    });
                    continue;
                }
                let id = JobId {
                    scope: scope.clone(),
                    id: QueueItemId::new().0,
                };
                insert_job(
                    rows,
                    config,
                    JobRecord {
                        id: id.clone(),
                        key: spec.key,
                        digest,
                        group: spec.group,
                        owners: spec.owners,
                        kind: spec.kind,
                        execution: spec.execution,
                        state: JobState::Pending,
                        final_state: None,
                        payload: Some(spec.payload),
                        admission: spec.admission,
                        max_attempts: spec.limits.max_attempts,
                        generation: 0,
                        lease_until: None,
                        cancel_requested: false,
                        purged: false,
                        output: None,
                        output_bytes: 0,
                        origin: None,
                        receipt: None,
                        recovery_until: spec.recovery_until,
                        result_expired: false,
                        created_at: now,
                        finished_at: None,
                        delivery_generation: 0,
                        delivery_until: None,
                        diagnostic: None,
                    },
                )?;
                outcomes.push(Enqueued::Inserted(id));
            }
            Ok(JobResponse::Enqueued(outcomes))
        }
        JobRequest::Claim {
            kinds,
            slots_available,
        } => {
            if kinds.is_empty() {
                return Err(JobError::InvalidRequest);
            }
            if slots_available == 0 {
                return Ok(JobResponse::Job(None));
            }
            let candidates =
                rows.select(scope, JobQuery::Pending { kinds }, now, config.max_page)?;
            for row in candidates {
                if let Some(row) = claim_job(rows, config, now, row)? {
                    return Ok(JobResponse::Job(Some(Box::new(row))));
                }
            }
            Ok(JobResponse::Job(None))
        }
        JobRequest::Candidates {
            kinds,
            limit,
            max_bytes,
        } => {
            page(config, limit)?;
            if kinds.is_empty() {
                return Err(JobError::InvalidRequest);
            }
            if max_bytes < 2 || max_bytes > config.max_page_bytes {
                return Err(JobError::InvalidRequest);
            }
            let selected = rows.select(scope, JobQuery::Pending { kinds }, now, limit)?;
            let mut page = Vec::new();
            let mut bytes: usize = 2;
            for row in selected {
                let Some(required) =
                    candidate_page_bytes(&row, bytes, max_bytes, !page.is_empty())?
                else {
                    break;
                };
                bytes = required;
                page.push(row);
            }
            Ok(JobResponse::Candidates(page))
        }
        JobRequest::RecoveryCandidates {
            kinds,
            after,
            limit,
            max_bytes,
        } => {
            page(config, limit)?;
            if kinds.is_empty() || max_bytes < 2 || max_bytes > config.max_page_bytes {
                return Err(JobError::InvalidRequest);
            }
            let selected = rows.select(scope, JobQuery::Recovery { kinds, after }, now, limit)?;
            let mut page = Vec::new();
            let mut bytes = 2;
            for row in selected {
                let Some(required) =
                    candidate_page_bytes(&row, bytes, max_bytes, !page.is_empty())?
                else {
                    break;
                };
                bytes = required;
                page.push(row);
            }
            Ok(JobResponse::Candidates(page))
        }
        JobRequest::RefusePending { job, diagnostic } => {
            let mut row = get(rows, scope, &job)?;
            if row.execution != Execution::Model {
                return Err(JobError::InvalidRequest);
            }
            if row.state == JobState::Pending {
                row.diagnostic = Some(diagnostic);
                finish(&mut row, JobState::Refused, now, config)?;
                rows.save(row)?;
            }
            Ok(JobResponse::Done)
        }
        JobRequest::ClaimPaid { job, receipt } => {
            let row = get(rows, scope, &job)?;
            if row.execution != Execution::Model || receipt.is_empty() {
                return Err(JobError::InvalidRequest);
            }
            let claimed = claim_job(rows, config, now, row)?;
            if let Some(mut row) = claimed {
                row.receipt = Some(receipt);
                row.origin = Some(ResultOrigin::Paid);
                rows.save(row.clone())?;
                Ok(JobResponse::Job(Some(Box::new(row))))
            } else {
                Ok(JobResponse::Job(None))
            }
        }
        JobRequest::ClaimJob(job) => {
            let row = get(rows, scope, &job)?;
            Ok(JobResponse::Job(
                claim_job(rows, config, now, row)?.map(Box::new),
            ))
        }
        JobRequest::Heartbeat { job, generation } => {
            scoped(scope, &job)?;
            let row = rows.metadata(&job)?;
            live(&row, generation, now)?;
            let until = stamp(deadline(now, config.claim_lease_seconds)?);
            let sql = "UPDATE jobs SET lease_until=?3 WHERE scope=?1 AND id=?2";
            rows.conn
                .execute(sql, params![json(scope)?, job.id, until])
                .map_err(storage)?;
            Ok(JobResponse::Heartbeat(row.cancel_requested || row.purged))
        }
        JobRequest::Complete {
            job,
            generation,
            state,
            origin,
            output,
            receipt,
            diagnostic,
        } => {
            let mut row = get(rows, scope, &job)?;
            if let Some(output) = &output {
                let bytes = output.len();
                if bytes > config.max_result_bytes {
                    return Err(JobError::ResultTooLarge {
                        job: row.id.clone(),
                        bytes,
                    });
                }
            }
            live(&row, generation, now)?;
            if origin == ResultOrigin::Paid && row.execution != Execution::Model
                || origin == ResultOrigin::Handler && row.execution != Execution::Handler
                || state == JobState::Uncertain && row.execution != Execution::Model
            {
                return Err(JobError::InvalidRequest);
            }
            if !matches!(
                state,
                JobState::Succeeded
                    | JobState::Failed
                    | JobState::Cancelled
                    | JobState::Refused
                    | JobState::Uncertain
            ) || origin == ResultOrigin::Paid && output.is_some()
                || origin == ResultOrigin::Paid && receipt.is_none()
                || origin != ResultOrigin::Paid && receipt.is_some()
                || state == JobState::Succeeded
                    && origin != ResultOrigin::Paid
                    && output.is_none()
                    && !row.purged
                    && row.recovery_until.is_none_or(|until| until > now)
            {
                return Err(JobError::InvalidRequest);
            }
            row.origin = Some(origin);
            row.receipt = receipt;
            row.diagnostic = diagnostic;
            if state == JobState::Uncertain {
                row.state = state;
                row.lease_until = None;
                if row.purged {
                    delete_copies(&mut row);
                }
            } else if row.purged {
                finish(&mut row, JobState::Purged, now, config)?;
                delete_copies(&mut row);
            } else {
                row.output = output;
                let state = if row.cancel_requested {
                    JobState::Cancelled
                } else {
                    state
                };
                finish(&mut row, state, now, config)?;
                // Count a kept output once: the page check and the stored length reuse it.
                row.output_bytes = row
                    .output
                    .as_ref()
                    .map(encoded_bytes)
                    .transpose()?
                    .unwrap_or(0);
                // Fail in this same write, so the claim cannot lapse between refusal and failure.
                if let Some(bytes) = undeliverable(&mut row, config)? {
                    row.output = None;
                    row.output_bytes = 0;
                    if row.state != JobState::Cancelled {
                        row.state = JobState::Failed;
                        row.diagnostic = Some(DiagnosticCode::QueueFailure);
                    }
                    rows.save(row)?;
                    return Ok(JobResponse::ResultRefused { bytes });
                }
            }
            rows.save(row)?;
            Ok(JobResponse::Done)
        }
        JobRequest::Resolve {
            job,
            generation,
            resolution,
        } => {
            let mut row = get(rows, scope, &job)?;
            if row.execution != Execution::Model
                || !row.state.unfinished()
                || row.generation != generation
            {
                return Err(JobError::StaleClaim);
            }
            row.origin = Some(ResultOrigin::Paid);
            match resolution {
                JobResolution::Uncertain { receipt } => {
                    if row.state != JobState::Running && row.state != JobState::Uncertain {
                        return Err(JobError::InvalidRequest);
                    }
                    row.receipt = Some(receipt);
                    row.state = JobState::Uncertain;
                    row.lease_until = None;
                    row.diagnostic = Some(DiagnosticCode::SpendReconciliationRequired);
                }
                JobResolution::KnownZeroCharge { receipt } => {
                    if row.state != JobState::Running && row.state != JobState::Uncertain {
                        return Err(JobError::InvalidRequest);
                    }
                    row.receipt = Some(receipt);
                    row.lease_until = None;
                    row.diagnostic = None;
                    if row.purged {
                        finish(&mut row, JobState::Purged, now, config)?;
                    } else if row.cancel_requested {
                        finish(&mut row, JobState::Cancelled, now, config)?;
                        delete_copies(&mut row);
                    } else if row.generation >= u64::from(row.max_attempts) {
                        row.diagnostic = Some(DiagnosticCode::AttemptBudgetExhausted);
                        finish(&mut row, JobState::Refused, now, config)?;
                    } else if row.admission.is_some() {
                        row.state = JobState::AwaitingAdmission;
                    } else {
                        row.state = JobState::Pending;
                    }
                }
                JobResolution::PaidResult {
                    receipt,
                    recovery_until,
                } => {
                    row.receipt = Some(receipt);
                    row.diagnostic = None;
                    if let Some(until) = recovery_until {
                        let until = millisecond_time(until)?;
                        row.recovery_until = Some(
                            row.recovery_until
                                .map_or(until, |current| current.min(until)),
                        );
                    }
                    let state = if row.purged {
                        JobState::Purged
                    } else if row.cancel_requested {
                        JobState::Cancelled
                    } else {
                        JobState::Succeeded
                    };
                    finish(&mut row, state, now, config)?;
                }
                JobResolution::Failed {
                    receipt,
                    diagnostic,
                } => {
                    row.receipt = Some(receipt);
                    row.diagnostic = Some(diagnostic);
                    let state = if row.purged {
                        JobState::Purged
                    } else if row.cancel_requested {
                        JobState::Cancelled
                    } else {
                        JobState::Failed
                    };
                    finish(&mut row, state, now, config)?;
                }
            }
            if row.purged {
                delete_copies(&mut row);
            }
            rows.save(row)?;
            Ok(JobResponse::Done)
        }
        JobRequest::Completions { limit, max_bytes } => {
            page(config, limit)?;
            if max_bytes < 2 || max_bytes > config.max_page_bytes {
                return Err(JobError::InvalidRequest);
            }
            let candidates = rows.inspect(scope, JobQuery::Final, now, limit)?;
            let mut admitted = Vec::new();
            let mut bytes: usize = 2; // JSON array brackets, plus commas between deliveries.
            for info in candidates {
                let mut row = rows.metadata(&info.id)?;
                if row.recovery_until.is_some_and(|until| until <= now) {
                    row.result_expired = true;
                    delete_copies(&mut row);
                }
                row.delivery_generation = row
                    .delivery_generation
                    .checked_add(1)
                    .ok_or(JobError::InvalidRequest)?;
                row.delivery_until = Some(deadline(now, config.delivery_lease_seconds)?);
                let token = DeliveryToken {
                    job: row.id.clone(),
                    generation: row.delivery_generation,
                };
                let delivery = Delivery {
                    completion: row.clone(),
                    token,
                };
                let has_output = info.output_bytes > 0 && !row.result_expired;
                let size = delivery_bytes(&delivery, has_output.then_some(info.output_bytes))?;
                let Some(required) =
                    admit_page_bytes(size, bytes, max_bytes, !admitted.is_empty(), |bytes| {
                        JobError::CompletionTooLarge {
                            job: row.id.clone(),
                            bytes,
                        }
                    })?
                else {
                    break;
                };
                bytes = required;
                admitted.push((row, delivery, has_output));
            }
            let mut deliveries = Vec::with_capacity(admitted.len());
            for (row, mut delivery, has_output) in admitted {
                if has_output {
                    delivery.completion.output = rows.output(&row.id)?;
                    if delivery.completion.output.is_none() {
                        return Err(JobError::Storage);
                    }
                }
                rows.deliver(&row, row.result_expired)?;
                deliveries.push(delivery);
            }
            Ok(JobResponse::Deliveries(CompletionPage {
                items: deliveries,
            }))
        }
        JobRequest::Ack(acks) => {
            if acks.len() > config.max_batch
                || serde_json::to_vec(&acks)
                    .map_err(|_| JobError::Storage)?
                    .len()
                    > config.max_page_bytes
            {
                return Err(JobError::InvalidRequest);
            }
            // Scope checks for the entire request precede every lookup/write.
            for (token, _) in &acks {
                scoped(scope, &token.job)?;
            }
            let mut results = Vec::with_capacity(acks.len());
            for (token, disposition) in acks {
                let mut row = get(rows, scope, &token.job)?;
                if row.state.unfinished() {
                    return Err(JobError::NotFinal);
                }
                if token.generation == 0 || token.generation > row.delivery_generation {
                    return Err(JobError::InvalidRequest);
                }
                if row.state.acked() {
                    results.push(AckResult::AlreadyAcked(
                        if row.state == JobState::Accepted {
                            Disposition::Accepted
                        } else {
                            Disposition::Discarded
                        },
                    ));
                } else {
                    row.final_state = Some(row.state);
                    row.state = match disposition {
                        Disposition::Accepted => JobState::Accepted,
                        Disposition::Discarded => JobState::Discarded,
                    };
                    rows.save(row)?;
                    results.push(AckResult::Acked(disposition));
                }
            }
            Ok(JobResponse::Acks(results))
        }
        JobRequest::Cancel(selector) => {
            let selected = match selector {
                Selector::Ids(ids) => {
                    if ids.len() > config.max_batch {
                        return Err(JobError::InvalidRequest);
                    }
                    for id in &ids {
                        scoped(scope, id)?;
                    }
                    let unique: BTreeSet<_> = ids.into_iter().map(|id| id.id).collect();
                    unique
                        .into_iter()
                        .map(|id| {
                            get(
                                rows,
                                scope,
                                &JobId {
                                    scope: scope.clone(),
                                    id,
                                },
                            )
                        })
                        .collect::<Result<Vec<_>, _>>()?
                }
                Selector::Group(group) => {
                    let live = rows.live_count(scope)?;
                    rows.select(scope, JobQuery::Group(group), now, live)?
                }
            };
            let mut changed = 0;
            for mut row in selected {
                if !row.state.unfinished() {
                    continue;
                }
                if claimable(&row, now) || row.state == JobState::AwaitingAdmission {
                    let state = stopped_state(&row);
                    finish(&mut row, state, now, config)?;
                    delete_copies(&mut row);
                } else {
                    row.cancel_requested = true;
                }
                rows.save(row)?;
                changed += 1;
            }
            Ok(JobResponse::Changed(changed))
        }
        JobRequest::PurgeOwner(owner) => {
            let live = rows.live_count(scope)?;
            let selected = rows.select(scope, JobQuery::Owner(owner), now, live)?;
            let count = selected.len();
            for mut row in selected {
                row.purged = true;
                row.owners.clear();
                row.cancel_requested = true;
                delete_copies(&mut row);
                if claimable(&row, now)
                    || row.state != JobState::Running
                        && row.state != JobState::Uncertain
                        && !row.state.acked()
                {
                    finish(&mut row, JobState::Purged, now, config)?;
                }
                rows.save(row)?;
            }
            Ok(JobResponse::Changed(count))
        }
        JobRequest::Diagnostics {
            group,
            after,
            limit,
        } => {
            page(config, limit)?;
            let selected =
                rows.inspect(scope, JobQuery::Diagnostics { group, after }, now, limit)?;
            let after = selected.last().map(|r| r.id.id.clone());
            let items = selected
                .into_iter()
                .map(|r| JobDiagnostic {
                    id: r.id,
                    state: r.state,
                    code: r.diagnostic,
                })
                .collect();
            Ok(JobResponse::Diagnostics(DiagnosticPage { items, after }))
        }
        JobRequest::Maintain => {
            let selected = rows.inspect(scope, JobQuery::Expired, now, config.maintenance_batch)?;
            let mut count = 0;
            let mut bytes: usize = 0;
            for row in selected {
                let required = bytes
                    .checked_add(rows.recovery_bytes(&row.id)?)
                    .ok_or(JobError::Storage)?;
                // Always admit the first job, including one larger than the budget.
                if count > 0 && required > config.maintenance_bytes_per_pass {
                    break;
                }
                rows.expire(&row.id)?;
                count += 1;
                bytes = required;
                if bytes >= config.maintenance_bytes_per_pass {
                    break;
                }
            }
            Ok(JobResponse::Changed(count))
        }
        JobRequest::Status(id) => {
            scoped(scope, &id)?;
            Ok(JobResponse::Job(Some(Box::new(rows.metadata(&id)?))))
        }
        JobRequest::Get(id) => Ok(JobResponse::Job(Some(Box::new(get(rows, scope, &id)?)))),
        JobRequest::PendingUsage => Ok(JobResponse::Usage(rows.usage(scope)?)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use symbiotic_queue::QueueBackend;

    fn scope() -> JobScope {
        JobScope {
            tenant: "tenant".into(),
            incarnation: "restore".into(),
            queue: "queue".into(),
        }
    }
    fn spec(key: &str) -> JobSpec {
        JobSpec {
            key: key.into(),
            group: Some("group".into()),
            owners: vec!["owner".into()],
            kind: "handler".into(),
            execution: Execution::Handler,
            payload: key.as_bytes().to_vec(),
            admission: None,
            limits: JobLimits { max_attempts: 3 },
            recovery_until: None,
        }
    }
    fn count(conn: &Connection, table: &str) -> usize {
        conn.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))
            .unwrap()
    }

    #[test]
    fn jobs_heartbeat_never_decodes_or_rewrites_content() {
        let queue = SqliteQueue::in_memory().unwrap();
        let scope = scope();
        let config = JobConfig::default();
        let now = Utc::now();
        queue
            .job_operation(
                &scope,
                &config,
                || now,
                JobRequest::Enqueue(vec![spec("heartbeat")]),
            )
            .unwrap();
        let row = match queue
            .job_operation(
                &scope,
                &config,
                || now,
                JobRequest::Claim {
                    kinds: vec!["handler".into()],
                    slots_available: 1,
                },
            )
            .unwrap()
        {
            JobResponse::Job(Some(row)) => row,
            other => panic!("{other:?}"),
        };
        // Invalid content types make any attempt to decode the blobs fail visibly.
        // Heartbeats need only metadata and must preserve even unreadable content.
        queue
            .conn
            .lock()
            .unwrap()
            .execute(
                "UPDATE jobs SET payload='unreadable input', output='unreadable output'",
                [],
            )
            .unwrap();
        for (cancel, purged) in [(false, false), (true, false), (false, true)] {
            queue
                .conn
                .lock()
                .unwrap()
                .execute(
                    "UPDATE jobs SET cancel_requested=?1, purged=?2",
                    params![cancel, purged],
                )
                .unwrap();
            assert!(matches!(queue.job_operation(
                &scope, &config, || now + Duration::seconds(1),
                JobRequest::Heartbeat { job: row.id.clone(), generation: row.generation }
            ).unwrap(), JobResponse::Heartbeat(requested) if requested == (cancel || purged)));
        }
        let conn = queue.conn.lock().unwrap();
        assert!(conn.query_row(
            "SELECT payload='unreadable input' AND output='unreadable output' AND lease_until=?1 FROM jobs",
            [stamp(now + Duration::seconds(1) + Duration::seconds(config.claim_lease_seconds as i64))],
            |r| r.get::<_, bool>(0)
        ).unwrap());
    }

    #[test]
    fn jobs_confirmation_retains_only_authorized_tombstone_fields() {
        for (execution, disposition) in [
            (Execution::Model, Disposition::Accepted),
            (Execution::Model, Disposition::Discarded),
            (Execution::Handler, Disposition::Accepted),
            (Execution::Handler, Disposition::Discarded),
        ] {
            let queue = SqliteQueue::in_memory().unwrap();
            let scope = scope();
            let config = JobConfig::default();
            let now = Utc::now();
            let mut spec = spec("tombstone");
            spec.execution = execution;
            queue
                .job_operation(
                    &scope,
                    &config,
                    || now,
                    JobRequest::Enqueue(vec![spec.clone()]),
                )
                .unwrap();
            let mut row = SqlRows::new(&queue.conn.lock().unwrap())
                .by_key(&scope, &spec.key)
                .unwrap()
                .unwrap();
            // Populate every operational field so incomplete narrowing is visible.
            row.state = JobState::Failed;
            row.final_state = Some(JobState::Failed);
            row.generation = 3;
            row.lease_until = Some(now);
            row.cancel_requested = true;
            row.purged = true;
            row.output = Some(b"result".to_vec());
            row.origin = Some(ResultOrigin::Paid);
            row.receipt = Some("receipt-reference".into());
            row.recovery_until = Some(now + Duration::seconds(60));
            row.result_expired = true;
            row.finished_at = Some(now);
            row.delivery_generation = 2;
            row.delivery_until = Some(now);
            row.diagnostic = Some(DiagnosticCode::AttemptBudgetExhausted);
            SqlRows::new(&queue.conn.lock().unwrap())
                .save(row.clone())
                .unwrap();
            let token = DeliveryToken {
                job: row.id.clone(),
                generation: 1,
            };
            assert!(
                matches!(queue.job_operation(&scope, &config, || now, JobRequest::Ack(vec![(token.clone(), disposition)])).unwrap(), JobResponse::Acks(results) if results == vec![AckResult::Acked(disposition)])
            );
            {
                let conn = queue.conn.lock().unwrap();
                let cleared = COLUMNS
                    .split(", ")
                    .filter(|column| {
                        ![
                            "scope",
                            "id",
                            "key",
                            "digest",
                            "state",
                            "final_state",
                            "receipt",
                            "delivery_generation",
                            "kind",
                        ]
                        .contains(column)
                            || *column == "kind" && execution == Execution::Handler
                    })
                    .map(|column| format!("{column} IS NULL"))
                    .collect::<Vec<_>>()
                    .join(" AND ");
                assert!(
                    conn.query_row(
                        &format!("SELECT {cleared} FROM jobs WHERE scope=?1 AND id=?2"),
                        params![json(&scope).unwrap(), row.id.id],
                        |r| r.get::<_, bool>(0)
                    )
                    .unwrap(),
                    "operational metadata survived confirmation"
                );
                assert_eq!(count(&conn, "job_owners"), 0);
            }
            let tombstone = match queue
                .job_operation(
                    &scope,
                    &config,
                    || now,
                    JobRequest::Enqueue(vec![spec.clone()]),
                )
                .unwrap()
            {
                JobResponse::Enqueued(mut results) => match results.remove(0) {
                    Enqueued::AlreadyDone(row) => row,
                    other => panic!("{other:?}"),
                },
                other => panic!("{other:?}"),
            };
            assert_eq!(tombstone.id, row.id);
            assert_eq!(tombstone.digest, row.digest);
            assert_eq!(tombstone.final_state, Some(JobState::Failed));
            assert_eq!(tombstone.receipt, row.receipt);
            assert_eq!(tombstone.delivery_generation, 2);
            assert_eq!(tombstone.execution, execution);
            assert!(
                tombstone.group.is_none()
                    && tombstone.kind
                        == if execution == Execution::Model {
                            spec.kind.clone()
                        } else {
                            String::new()
                        }
                    && tombstone.owners.is_empty()
            );
            assert!(
                matches!(queue.job_operation(&scope, &config, || now, JobRequest::Ack(vec![(token, disposition)])).unwrap(), JobResponse::Acks(results) if results == vec![AckResult::AlreadyAcked(disposition)])
            );
            for generation in [0, 3] {
                assert!(matches!(
                    queue.job_operation(
                        &scope,
                        &config,
                        || now,
                        JobRequest::Ack(vec![(
                            DeliveryToken {
                                job: row.id.clone(),
                                generation
                            },
                            disposition
                        )])
                    ),
                    Err(JobError::InvalidRequest)
                ));
            }
            spec.payload.push(0);
            assert!(matches!(
                queue.job_operation(&scope, &config, || now, JobRequest::Enqueue(vec![spec])),
                Err(JobError::KeyConflict)
            ));
        }
    }

    #[test]
    fn jobs_caller_transaction_commit_and_rollback() {
        let mut conn = Connection::open_in_memory().unwrap();
        SqliteQueue::initialize_connection(&mut conn).unwrap();
        conn.execute_batch("CREATE TABLE caller_records (id INTEGER PRIMARY KEY)")
            .unwrap();
        let config = JobConfig::default();
        let now = Utc::now();
        {
            let mut tx = conn
                .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                .unwrap();
            tx.execute("INSERT INTO caller_records VALUES (1)", [])
                .unwrap();
            jobs_in_transaction(
                &mut tx,
                &scope(),
                &config,
                now,
                JobRequest::Enqueue(vec![spec("rollback")]),
            )
            .unwrap();
            assert_eq!(count(&tx, "jobs"), 1);
            // Simulated crash/abort: caller record and job roll back together.
        }
        assert_eq!(count(&conn, "caller_records"), 0);
        assert_eq!(count(&conn, "jobs"), 0);
        {
            let mut tx = conn
                .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                .unwrap();
            tx.execute("INSERT INTO caller_records VALUES (1)", [])
                .unwrap();
            jobs_in_transaction(
                &mut tx,
                &scope(),
                &config,
                now,
                JobRequest::Enqueue(vec![spec("commit")]),
            )
            .unwrap();
            tx.commit().unwrap();
        }
        assert_eq!(count(&conn, "caller_records"), 1);
        assert_eq!(count(&conn, "jobs"), 1);
        assert_eq!(
            conn.query_row("SELECT typeof(payload) FROM jobs", [], |r| r
                .get::<_, String>(0))
                .unwrap(),
            "blob"
        );
        {
            let mut tx = conn
                .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                .unwrap();
            let mut conflict = spec("commit");
            conflict.payload = b"conflict".to_vec();
            assert!(matches!(
                jobs_in_transaction(
                    &mut tx,
                    &scope(),
                    &config,
                    now,
                    JobRequest::Enqueue(vec![spec("new"), conflict])
                ),
                Err(JobError::KeyConflict)
            ));
            // Even a caller that commits after handling the error cannot retain half a batch.
            tx.commit().unwrap();
        }
        assert_eq!(count(&conn, "jobs"), 1);
    }

    #[tokio::test]
    async fn jobs_reopen_preserves_results_and_content_free_tombstones() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("queue.sqlite");
        let scope = scope();
        let config = JobConfig::default();
        let now = Utc::now();
        let queue = SqliteQueue::open(&path).unwrap();
        let id = match queue
            .jobs(
                &scope,
                &config,
                std::sync::Arc::new(move || now),
                JobRequest::Enqueue(vec![spec("durable")]),
            )
            .await
            .unwrap()
        {
            JobResponse::Enqueued(mut v) => match v.remove(0) {
                Enqueued::Inserted(id) => id,
                other => panic!("{other:?}"),
            },
            other => panic!("{other:?}"),
        };
        let row = match queue
            .jobs(
                &scope,
                &config,
                std::sync::Arc::new(move || now),
                JobRequest::Claim {
                    kinds: vec!["handler".into()],
                    slots_available: 1,
                },
            )
            .await
            .unwrap()
        {
            JobResponse::Job(Some(r)) => r,
            other => panic!("{other:?}"),
        };
        queue
            .jobs(
                &scope,
                &config,
                std::sync::Arc::new(move || now),
                JobRequest::Complete {
                    job: id.clone(),
                    generation: row.generation,
                    state: JobState::Succeeded,
                    origin: ResultOrigin::Handler,
                    output: Some((0..=255).collect::<Vec<u8>>()),
                    receipt: None,
                    diagnostic: None,
                },
            )
            .await
            .unwrap();
        drop(queue);
        let queue = SqliteQueue::open(&path).unwrap();
        let delivery = match queue
            .jobs(
                &scope,
                &config,
                std::sync::Arc::new(move || now),
                JobRequest::Completions {
                    limit: 1,
                    max_bytes: 100_000,
                },
            )
            .await
            .unwrap()
        {
            JobResponse::Deliveries(mut v) => v.items.remove(0),
            other => panic!("{other:?}"),
        };
        assert_eq!(
            delivery.completion.output,
            Some((0..=255).collect::<Vec<u8>>())
        );
        queue
            .jobs(
                &scope,
                &config,
                std::sync::Arc::new(move || now),
                JobRequest::Ack(vec![(delivery.token, Disposition::Accepted)]),
            )
            .await
            .unwrap();
        drop(queue);
        let queue = SqliteQueue::open(&path).unwrap();
        let result = queue
            .jobs(
                &scope,
                &config,
                std::sync::Arc::new(move || now),
                JobRequest::Enqueue(vec![spec("durable")]),
            )
            .await
            .unwrap();
        match result {
            JobResponse::Enqueued(mut v) => match v.remove(0) {
                Enqueued::AlreadyDone(row) => {
                    assert_eq!(row.id, id);
                    assert_eq!(row.final_state, Some(JobState::Succeeded));
                    assert!(row.output.is_none() && row.payload.is_none());
                }
                other => panic!("{other:?}"),
            },
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn jobs_independent_connections_serialize_bounds_and_confirm() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("queue.sqlite");
        let a = SqliteQueue::open(&path).unwrap();
        let b = SqliteQueue::open(&path).unwrap();
        let config = JobConfig {
            max_live_jobs: 1,
            ..JobConfig::default()
        };
        let scope = scope();
        let now = Utc::now();
        // Both connections can read the initial state; capacity is checked only
        // after acquiring the writer lock, so the second operation cannot exceed it.
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let handles: Vec<_> = [a.clone(), b.clone()]
            .into_iter()
            .enumerate()
            .map(|(i, queue)| {
                let barrier = barrier.clone();
                let scope = scope.clone();
                let config = config.clone();
                tokio::task::spawn_blocking(move || {
                    barrier.wait();
                    queue.job_operation(
                        &scope,
                        &config,
                        || now,
                        JobRequest::Enqueue(vec![spec(&format!("concurrent-{i}"))]),
                    )
                })
            })
            .collect();
        let mut inserted = 0;
        let mut full = 0;
        for handle in handles {
            match handle.await.unwrap() {
                Ok(JobResponse::Enqueued(_)) => inserted += 1,
                Err(JobError::QueueFull) => full += 1,
                other => panic!("{other:?}"),
            }
        }
        assert_eq!((inserted, full), (1, 1));
        let row = match b
            .jobs(
                &scope,
                &config,
                std::sync::Arc::new(move || now),
                JobRequest::Claim {
                    kinds: vec!["handler".into()],
                    slots_available: 1,
                },
            )
            .await
            .unwrap()
        {
            JobResponse::Job(Some(r)) => r,
            other => panic!("{other:?}"),
        };
        a.jobs(
            &scope,
            &config,
            std::sync::Arc::new(move || now),
            JobRequest::Complete {
                job: row.id.clone(),
                generation: row.generation,
                state: JobState::Succeeded,
                origin: ResultOrigin::Handler,
                output: Some(b"answer".to_vec()),
                receipt: None,
                diagnostic: None,
            },
        )
        .await
        .unwrap();
        let token = match a
            .jobs(
                &scope,
                &config,
                std::sync::Arc::new(move || now),
                JobRequest::Completions {
                    limit: 1,
                    max_bytes: 100_000,
                },
            )
            .await
            .unwrap()
        {
            JobResponse::Deliveries(mut v) => v.items.remove(0).token,
            other => panic!("{other:?}"),
        };
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let handles: Vec<_> = [
            (a.clone(), Disposition::Accepted),
            (b.clone(), Disposition::Discarded),
        ]
        .into_iter()
        .map(|(queue, disposition)| {
            let barrier = barrier.clone();
            let scope = scope.clone();
            let config = config.clone();
            let token = token.clone();
            tokio::task::spawn_blocking(move || {
                barrier.wait();
                queue.job_operation(
                    &scope,
                    &config,
                    || now,
                    JobRequest::Ack(vec![(token, disposition)]),
                )
            })
        })
        .collect();
        let mut results = Vec::new();
        for handle in handles {
            match handle.await.unwrap().unwrap() {
                JobResponse::Acks(mut v) => results.push(v.remove(0)),
                other => panic!("{other:?}"),
            }
        }
        let winner = results
            .iter()
            .find_map(|r| {
                if let AckResult::Acked(d) = r {
                    Some(*d)
                } else {
                    None
                }
            })
            .unwrap();
        assert_eq!(
            results
                .iter()
                .filter(|r| **r == AckResult::Acked(winner))
                .count(),
            1
        );
        assert_eq!(
            results
                .iter()
                .filter(|r| **r == AckResult::AlreadyAcked(winner))
                .count(),
            1
        );
        // With one live slot, admission depends on which writer wins: enqueue
        // before confirmation must refuse, and enqueue after it must succeed.
        let cross_id = match a
            .job_operation(
                &scope,
                &config,
                || now,
                JobRequest::Enqueue(vec![spec("cross-final")]),
            )
            .unwrap()
        {
            JobResponse::Enqueued(mut v) => match v.remove(0) {
                Enqueued::Inserted(id) => id,
                other => panic!("{other:?}"),
            },
            other => panic!("{other:?}"),
        };
        a.job_operation(
            &scope,
            &config,
            || now,
            JobRequest::Cancel(Selector::Ids(vec![cross_id.clone()])),
        )
        .unwrap();
        let cross_token = match a
            .job_operation(
                &scope,
                &config,
                || now,
                JobRequest::Completions {
                    limit: 1,
                    max_bytes: 100_000,
                },
            )
            .unwrap()
        {
            JobResponse::Deliveries(mut page) => page.items.remove(0).token,
            other => panic!("{other:?}"),
        };
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let handles: Vec<_> = [
            (
                a.clone(),
                JobRequest::Ack(vec![(cross_token, Disposition::Accepted)]),
            ),
            (b.clone(), JobRequest::Enqueue(vec![spec("cross-admitted")])),
        ]
        .into_iter()
        .map(|(queue, request)| {
            let barrier = barrier.clone();
            let scope = scope.clone();
            let config = config.clone();
            tokio::task::spawn_blocking(move || {
                barrier.wait();
                queue.job_operation(&scope, &config, || now, request)
            })
        })
        .collect();
        let mut handles = handles.into_iter();
        let confirmed = handles.next().unwrap().await.unwrap().unwrap();
        assert!(
            matches!(confirmed, JobResponse::Acks(ref results) if results == &[AckResult::Acked(Disposition::Accepted)])
        );
        let admitted = match handles.next().unwrap().await.unwrap() {
            Ok(JobResponse::Enqueued(results)) => {
                assert!(matches!(results.as_slice(), [Enqueued::Inserted(_)]));
                true
            }
            Err(JobError::QueueFull) => false,
            other => panic!("{other:?}"),
        };
        // Confirmation has now committed. A replay joins an admitted job; the
        // previously refused key inserts once, proving refusal left no row.
        let result = b
            .job_operation(
                &scope,
                &config,
                || now,
                JobRequest::Enqueue(vec![spec("cross-admitted")]),
            )
            .unwrap();
        assert!(
            matches!(result, JobResponse::Enqueued(ref results) if matches!(results.as_slice(), [Enqueued::Joined(_)]) == admitted && matches!(results.as_slice(), [Enqueued::Inserted(_)]) != admitted)
        );
        assert_eq!(
            SqlRows::new(&b.conn.lock().unwrap())
                .live_count(&scope)
                .unwrap(),
            1
        );
    }
    #[test]
    fn jobs_indexed_pages_have_flat_work_and_cursor_plans() {
        let mut measurements = Vec::new();
        for retained in [10, 10_000] {
            let mut conn = Connection::open_in_memory().unwrap();
            SqliteQueue::initialize_connection(&mut conn).unwrap();
            let mut tx = conn.transaction().unwrap();
            let now = DateTime::from_timestamp_millis(1_000_000).unwrap();
            jobs_in_transaction(
                &mut tx,
                &scope(),
                &JobConfig::default(),
                now,
                JobRequest::Enqueue(vec![spec("template")]),
            )
            .unwrap();
            let mut store = SqlRows::new(&tx);
            let template = store.by_key(&scope(), "template").unwrap().unwrap();
            for i in 0..retained + 8 {
                let mut row = template.clone();
                row.id.id = if i < retained && i % 2 == 1 {
                    format!("zz-tail-{i:08}")
                } else {
                    format!("{i:08}")
                };
                row.key = row.id.id.clone();
                row.state = if i < retained {
                    if i % 2 == 0 {
                        JobState::Accepted
                    } else {
                        JobState::Failed
                    }
                } else {
                    JobState::Succeeded
                };
                row.payload = None;
                row.admission = None;
                row.finished_at = Some(now);
                row.delivery_generation = 1;
                row.delivery_until = (i >= retained).then_some(now + chrono::Duration::seconds(30));
                store.save(row).unwrap();
            }
            let mut eligible = template.clone();
            eligible.id.id = "zz-eligible".into();
            eligible.key = "eligible".into();
            eligible.state = JobState::Failed;
            eligible.finished_at = Some(now);
            eligible.owners = vec!["target".into()];
            store.save(eligible.clone()).unwrap();
            let metrics = QueryMetrics::default();
            store.metrics = Some(&metrics);
            let finals = store.select(&scope(), JobQuery::Final, now, 1).unwrap();
            assert_eq!(finals[0].id, eligible.id);
            let delivery_steps = metrics.steps.replace(0);
            let diagnostics = store
                .select(
                    &scope(),
                    JobQuery::Diagnostics {
                        group: "group".into(),
                        after: None,
                    },
                    now,
                    1,
                )
                .unwrap();
            assert_eq!(diagnostics[0].id, eligible.id);
            let diagnostic_steps = metrics.steps.replace(0);
            let owner = store
                .select(&scope(), JobQuery::Owner("target".into()), now, 1)
                .unwrap();
            assert_eq!(owner[0].id, eligible.id);
            let owner_steps = metrics.steps.replace(0);
            let unfinished = store
                .select(&scope(), JobQuery::Group("group".into()), now, 1)
                .unwrap();
            assert_eq!(unfinished[0].id, template.id);
            let group_steps = metrics.steps.get();
            // Actual EXPLAIN QUERY PLAN for the executed selectors:
            // SEARCH jobs USING INDEX jobs_delivery (scope=?)
            // SEARCH jobs USING INDEX jobs_diagnostics (scope=? AND job_group=? AND id>?)
            let plans = metrics.plans.borrow();
            for index in [
                "jobs_delivery",
                "jobs_diagnostics",
                "sqlite_autoindex_job_owners_1",
                "jobs_unfinished_group",
            ] {
                assert!(plans.iter().any(|p| p.contains(index)), "{plans:?}");
            }
            assert!(
                plans
                    .iter()
                    .all(|p| !p.contains("SCAN") && !p.contains("TEMP B-TREE")),
                "{plans:?}"
            );
            assert!(
                delivery_steps < 300 && diagnostic_steps < 150,
                "{delivery_steps}, {diagnostic_steps}"
            );
            measurements.push((delivery_steps, diagnostic_steps, owner_steps, group_steps));
        }
        assert_eq!(
            measurements[0], measurements[1],
            "retained history must not affect selector work"
        );
        eprintln!(
            "delivery/diagnostic/owner/group VM steps at 10 vs 10,000 retained rows: {measurements:?}"
        );
    }

    #[test]
    fn jobs_projections_preflight_and_erase_without_loading_content() {
        let mut conn = Connection::open_in_memory().unwrap();
        SqliteQueue::initialize_connection(&mut conn).unwrap();
        let mut tx = conn.transaction().unwrap();
        let now = DateTime::from_timestamp_millis(1_000_000).unwrap();
        let config = JobConfig::default();
        jobs_in_transaction(
            &mut tx,
            &scope(),
            &config,
            now,
            JobRequest::Enqueue(vec![spec("projection")]),
        )
        .unwrap();
        let mut store = SqlRows::new(&tx);
        let mut row = store.by_key(&scope(), "projection").unwrap().unwrap();
        row.state = JobState::Failed;
        row.finished_at = Some(now);
        row.recovery_until = Some(now + chrono::Duration::seconds(1));
        row.output = Some(vec![b'x'; 1000]);
        store.save(row.clone()).unwrap();
        // Invalid content is a read probe: any materialization produces Storage.
        tx.execute("UPDATE jobs SET output=42, payload=42", [])
            .unwrap();
        assert!(
            matches!(jobs_in_transaction(&mut tx, &scope(), &config, now, JobRequest::Completions { limit: 1, max_bytes: 100 }), Err(JobError::CompletionTooLarge { job, .. }) if job == row.id)
        );
        assert_eq!(
            tx.query_row("SELECT delivery_generation FROM jobs", [], |r| r
                .get::<_, u64>(0))
                .unwrap(),
            0
        );
        match jobs_in_transaction(
            &mut tx,
            &scope(),
            &config,
            now,
            JobRequest::Diagnostics {
                group: "group".into(),
                after: None,
                limit: 1,
            },
        )
        .unwrap()
        {
            JobResponse::Diagnostics(page) => assert_eq!(page.items[0].id, row.id),
            other => panic!("{other:?}"),
        }
        assert!(matches!(
            jobs_in_transaction(
                &mut tx,
                &scope(),
                &config,
                now + chrono::Duration::seconds(2),
                JobRequest::Maintain
            )
            .unwrap(),
            JobResponse::Changed(1)
        ));
        let stored = SqlRows::new(&tx).get(&row.id).unwrap().unwrap();
        assert!(stored.result_expired && stored.output.is_none() && stored.payload.is_none());
        assert_eq!(stored.output_bytes, 0);
    }

    #[test]
    fn jobs_numeric_timestamp_readers_reject_unsupported_integers() {
        let mut conn = Connection::open_in_memory().unwrap();
        SqliteQueue::initialize_connection(&mut conn).unwrap();
        let mut tx = conn.transaction().unwrap();
        jobs_in_transaction(
            &mut tx,
            &scope(),
            &JobConfig::default(),
            Utc::now(),
            JobRequest::Enqueue(vec![spec("time")]),
        )
        .unwrap();
        assert_eq!(
            tx.query_row("SELECT typeof(created_at) FROM jobs", [], |r| r
                .get::<_, String>(0))
                .unwrap(),
            "integer"
        );
        tx.execute("UPDATE jobs SET created_at=?1", [i64::MAX])
            .unwrap();
        assert!(matches!(
            SqlRows::new(&tx).by_key(&scope(), "time"),
            Err(JobError::Storage)
        ));
    }
}
