//! Durable core records and the line-oriented local service protocol.
mod budgets;
mod calls;
mod reconcile;
pub mod server;

use chrono::{DateTime, SecondsFormat, Utc};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params, types::ValueRef};
use serde_json::{Value, json};
use std::{
    fs,
    os::unix::fs::{DirBuilderExt, PermissionsExt},
    path::Path,
    time::Duration,
};
use symbiotic_core::{
    Timestamp,
    event::{EventId, EventKind},
};

/// Service error, with a stable wire reason and a caller-readable explanation.
#[derive(Debug, thiserror::Error, serde::Serialize)]
#[error("{message}")]
pub struct Error {
    /// Lower snake case wire reason.
    pub reason: String,
    /// Explanation naming the failed field or operation.
    pub message: String,
}
impl Error {
    pub(crate) fn new(reason: &str, message: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
            message: message.into(),
        }
    }
}
impl From<rusqlite::Error> for Error {
    fn from(e: rusqlite::Error) -> Self {
        Self::new("store_error", e.to_string())
    }
}
impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Self::new("args_invalid", format!("args: {e}"))
    }
}
impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::new("io_error", e.to_string())
    }
}
type Result<T> = std::result::Result<T, Error>;

/// One SQLite writer. Calls are serialized by the server's mutex.
pub struct Store {
    pub(crate) conn: Connection,
}
impl Store {
    /// Create the private state folder and open its current database.
    pub fn open(state: &Path) -> Result<Self> {
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(state)?;
        fs::set_permissions(state, fs::Permissions::from_mode(0o700))?;
        Self::open_version(&state.join("core.sqlite"), 3)
    }
    /// Open a database and migrate through `version` (1 through 3).
    /// This also permits constructing historical files for migration checks.
    pub fn open_version(path: &Path, version: u32) -> Result<Self> {
        let mut conn = Connection::open(path)?;
        conn.busy_timeout(Duration::from_secs(60))?;
        conn.pragma_update(None, "synchronous", "FULL")?;
        conn.pragma_update(None, "fullfsync", true)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        migrate(&mut conn, version)?;
        Ok(Self { conn })
    }
}

/// Apply numbered SQL migrations atomically, refusing unknown newer versions.
pub fn migrate(conn: &mut Connection, target: u32) -> Result<()> {
    const SQL: [&str; 3] = [
        include_str!("../migrations/0001_initial.sql"),
        include_str!("../migrations/0002_read_indexes.sql"),
        include_str!("../migrations/0003_job_heartbeat.sql"),
    ];
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let current: u32 = tx.pragma_query_value(None, "user_version", |r| r.get(0))?;
    if current > SQL.len() as u32 || current > target {
        return Err(Error::new(
            "store_version_newer",
            format!("store version {current} is newer than supported version {target}"),
        ));
    }
    if target > SQL.len() as u32 {
        return Err(invalid("version"));
    }
    for version in current..target {
        tx.execute_batch(SQL[version as usize])?;
        tx.pragma_update(None, "user_version", version + 1)?;
    }
    tx.commit()?;
    Ok(())
}

/// Dispatch one request line into one response line (without a trailing newline).
/// Every mutating call commits its records and events in one IMMEDIATE transaction.
pub fn dispatch(store: &mut Store, line: &str) -> String {
    let response = (|| -> Result<Value> {
        let request: Value = serde_json::from_str(line)?;
        fields(&request, &["call", "args"])?;
        let call = string(&request, "call")?;
        let args = request
            .get("args")
            .filter(|v| v.is_object())
            .ok_or_else(|| invalid("args"))?;
        if call == "reconcile" {
            return reconcile::run(&store.conn, args);
        }
        let tx = store
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let value = match call {
            "job.create" => calls::job_create(&tx, args),
            "job.transition" => calls::job_transition(&tx, args),
            "job.heartbeat" => calls::job_heartbeat(&tx, args),
            "job.list" => calls::job_list(&tx, args),
            "request.record" => calls::request_record(&tx, args),
            "decision.answer" => budgets::answer(&tx, args),
            "budget.get" => {
                fields(args, &["account"])?;
                Ok(json!({"budgets": budgets::get(&tx, string(args, "account")?)?}))
            }
            "events.since" => calls::events_since(&tx, args),
            _ => Err(Error::new(
                "call_unknown",
                format!("call {call} is unknown"),
            )),
        }?;
        tx.commit()?;
        Ok(value)
    })();
    match response {
        Ok(value) => json!({"ok": value}).to_string(),
        Err(e) => json!({"error": e}).to_string(),
    }
}

pub(crate) fn now() -> Timestamp {
    Timestamp(Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true))
}
pub(crate) fn date(value: &str, field: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .map(|t| t.with_timezone(&Utc))
        .map_err(|_| invalid(field))
}
pub(crate) fn invalid(field: &str) -> Error {
    Error::new(
        "args_invalid",
        format!("field {field} is missing or invalid"),
    )
}
pub(crate) fn string<'a>(v: &'a Value, field: &str) -> Result<&'a str> {
    v.get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| invalid(field))
}
pub(crate) fn fields(v: &Value, names: &[&str]) -> Result<()> {
    let object = v.as_object().ok_or_else(|| invalid("args"))?;
    for key in object.keys() {
        if !names.contains(&key.as_str()) {
            return Err(invalid(key));
        }
    }
    Ok(())
}
pub(crate) fn optional_string(v: &Value, field: &str) -> Result<()> {
    if let Some(value) = v.get(field)
        && !value.is_string()
    {
        return Err(invalid(field));
    }
    Ok(())
}
pub(crate) fn strings(v: &Value, field: &str) -> Result<Vec<String>> {
    let Some(value) = v.get(field) else {
        return Ok(vec![]);
    };
    value
        .as_array()
        .ok_or_else(|| invalid(field))?
        .iter()
        .map(|x| x.as_str().map(str::to_owned).ok_or_else(|| invalid(field)))
        .collect()
}
pub(crate) fn append(
    conn: &Connection,
    subject: &str,
    id: &str,
    kind: &str,
    payload: &Value,
) -> Result<(i64, EventId)> {
    let event_id = EventId(uuid::Uuid::new_v4().to_string());
    let kind = EventKind(kind.into());
    let time = now();
    conn.execute("INSERT INTO events(id,time,subject_kind,subject_id,kind,payload) VALUES (?1,?2,?3,?4,?5,?6)",
        params![event_id.0, time.0, subject, id, kind.0, payload.to_string()])?;
    Ok((conn.last_insert_rowid(), event_id))
}
// Table-specific JSON columns are decoded here; SQL identifiers are internal constants.
pub(crate) fn rows(
    conn: &Connection,
    sql: &str,
    args: impl rusqlite::Params,
) -> Result<Vec<Value>> {
    let mut stmt = conn.prepare(sql)?;
    let names = stmt
        .column_names()
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    let mut cursor = stmt.query(args)?;
    let mut result = Vec::new();
    while let Some(row) = cursor.next()? {
        let mut object = serde_json::Map::new();
        for (i, name) in names.iter().enumerate() {
            let value = match row.get_ref(i)? {
                ValueRef::Null => Value::Null,
                ValueRef::Integer(x) => json!(x),
                ValueRef::Real(x) => json!(x),
                ValueRef::Text(x) => {
                    let s = std::str::from_utf8(x)
                        .map_err(|e| Error::new("store_error", e.to_string()))?;
                    if [
                        "inputs",
                        "waiting_for",
                        "envelope",
                        "answer",
                        "answered_by",
                        "meter",
                        "usage",
                        "payload",
                    ]
                    .contains(&name.as_str())
                    {
                        serde_json::from_str(s)
                            .map_err(|e| Error::new("store_error", e.to_string()))?
                    } else {
                        json!(s)
                    }
                }
                ValueRef::Blob(_) => return Err(Error::new("store_error", "unexpected blob")),
            };
            object.insert(name.clone(), value);
        }
        result.push(Value::Object(object));
    }
    Ok(result)
}
pub(crate) fn one(conn: &Connection, table: &str, id: &str) -> Result<Option<Value>> {
    Ok(rows(conn, &format!("SELECT * FROM {table} WHERE id=?1"), [id])?.pop())
}
pub(crate) fn event_for(
    conn: &Connection,
    subject: &str,
    kind: &str,
    id: &str,
) -> Result<Option<(i64, Value)>> {
    let row: Option<(i64, String)> = conn
        .query_row(
            "SELECT seq,payload FROM events WHERE subject_kind=?1 AND subject_id=?2 AND kind=?3 ORDER BY seq LIMIT 1",
            params![subject, id, kind],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    row.map(|(seq, s)| Ok((seq, serde_json::from_str(&s)?)))
        .transpose()
}
