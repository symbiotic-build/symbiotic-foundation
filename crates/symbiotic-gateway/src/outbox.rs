//! One SQLite owner keeps disk work off the request handler. Commands retain FIFO order.
use crate::GatewayError;
use rusqlite::{Connection, OptionalExtension, params};
use std::{
    path::Path,
    sync::{Arc, mpsc},
    thread::JoinHandle,
};
use tokio::sync::{Notify, oneshot};

type Result<T> = std::result::Result<T, GatewayError>;
type Row = Option<(i64, String)>;
enum Command {
    Insert(String, String),
    First(oneshot::Sender<Result<Row>>),
    Delete(i64, oneshot::Sender<Result<()>>),
    Flush(oneshot::Sender<()>),
    Stop,
}
pub(crate) struct Outbox {
    commands: mpsc::Sender<Command>,
    worker: Option<JoinHandle<()>>,
    pub changed: Arc<Notify>,
}
impl Outbox {
    pub fn open(dir: &Path) -> Result<Self> {
        let path = dir.join("outbox.sqlite");
        symbiotic_ai_runtime::model::private_fs::ensure_private_file(&path)
            .map_err(|_| GatewayError::StateUnavailable)?;
        let mut conn = Connection::open(path).map_err(storage)?;
        conn.busy_timeout(std::time::Duration::from_secs(5))
            .map_err(storage)?;
        conn.pragma_update(None, "journal_mode", "WAL")
            .map_err(storage)?;
        conn.pragma_update(None, "synchronous", "FULL")
            .map_err(storage)?;
        conn.pragma_update(None, "fullfsync", true)
            .map_err(storage)?;
        let version: u32 = conn
            .pragma_query_value(None, "user_version", |r| r.get(0))
            .map_err(storage)?;
        match version {
            0 => {
                let tx = conn.transaction().map_err(storage)?;
                tx.execute_batch("CREATE TABLE outbox (seq INTEGER PRIMARY KEY, request_id TEXT NOT NULL UNIQUE, record TEXT NOT NULL);").map_err(storage)?;
                tx.pragma_update(None, "user_version", 1).map_err(storage)?;
                tx.commit().map_err(storage)?;
            }
            1 => {}
            _ => return Err(GatewayError::StateUnavailable),
        }
        let changed = Arc::new(Notify::new());
        let notify = changed.clone();
        let (commands, receive) = mpsc::channel();
        let worker = std::thread::Builder::new()
            .name("gateway-outbox".into())
            .spawn(move || {
                while let Ok(command) = receive.recv() {
                    match command {
                        Command::Insert(request_id, record) => {
                            if conn
                                .execute(
                                    "INSERT INTO outbox(request_id, record) VALUES (?1, ?2)",
                                    params![request_id, record],
                                )
                                .is_err()
                            {
                                tracing::error!(
                                    code = "outbox_write_failed",
                                    request_id,
                                    "request record write failed"
                                );
                            } else {
                                notify.notify_one();
                            }
                        }
                        Command::First(reply) => {
                            let result = conn
                                .query_row(
                                    "SELECT seq, record FROM outbox ORDER BY seq LIMIT 1",
                                    [],
                                    |r| Ok((r.get(0)?, r.get(1)?)),
                                )
                                .optional()
                                .map_err(storage);
                            let _ = reply.send(result);
                        }
                        Command::Delete(seq, reply) => {
                            let _ = reply.send(
                                conn.execute("DELETE FROM outbox WHERE seq=?1", [seq])
                                    .map(|_| ())
                                    .map_err(storage),
                            );
                        }
                        Command::Flush(reply) => {
                            let _ = reply.send(());
                        }
                        Command::Stop => break,
                    }
                }
            })
            .map_err(|_| GatewayError::StateUnavailable)?;
        Ok(Self {
            commands,
            worker: Some(worker),
            changed,
        })
    }
    pub fn insert(&self, request_id: String, record: String) {
        if self
            .commands
            .send(Command::Insert(request_id.clone(), record))
            .is_err()
        {
            tracing::error!(
                code = "outbox_write_failed",
                request_id,
                "request record write failed"
            );
        }
    }
    pub async fn first(&self) -> Result<Row> {
        let (send, receive) = oneshot::channel();
        self.commands.send(Command::First(send)).map_err(storage)?;
        receive.await.map_err(storage)?
    }
    pub async fn delete(&self, seq: i64) -> Result<()> {
        let (send, receive) = oneshot::channel();
        self.commands
            .send(Command::Delete(seq, send))
            .map_err(storage)?;
        receive.await.map_err(storage)?
    }
    pub async fn flush(&self) -> Result<()> {
        let (send, receive) = oneshot::channel();
        self.commands.send(Command::Flush(send)).map_err(storage)?;
        receive.await.map_err(storage)
    }
}
fn storage(_: impl std::fmt::Debug) -> GatewayError {
    GatewayError::StateUnavailable
}
impl Drop for Outbox {
    fn drop(&mut self) {
        let _ = self.commands.send(Command::Stop);
        if let Some(worker) = self.worker.take() {
            if worker.join().is_err() {
                tracing::error!(code = "outbox_worker_failed", "outbox worker failed");
            }
        }
    }
}
