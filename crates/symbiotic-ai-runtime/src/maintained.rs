//! Shared maintenance owner for an opened persistent ledger.

use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;
use symbiotic_core::DiagnosticCode;
use symbiotic_model::{DirResponseCache, ModelError};
use symbiotic_queue::QueueBackend;
use symbiotic_queue_sqlite::SqliteQueue;
use tokio::sync::watch;

const ORPHANED: DiagnosticCode = DiagnosticCode::StaleQueueItem;

/// How long and how much of the response cache the sweep keeps.
pub(crate) struct ResponseRetention {
    pub(crate) cache: DirResponseCache,
    pub(crate) max_age: Option<Duration>,
    pub(crate) max_bytes: Option<u64>,
}

/// The shared retention operation used at open and on each timer tick.
pub(crate) struct Sweep {
    queue: SqliteQueue,
    retention: chrono::Duration,
    responses: ResponseRetention,
    recovery: crate::spend::SqliteSpendLedger,
}

impl Sweep {
    /// Use the runtime retention settings and its existing operational stores.
    pub(crate) fn new(
        queue: SqliteQueue,
        retention: Duration,
        responses: ResponseRetention,
        recovery: crate::spend::SqliteSpendLedger,
    ) -> Self {
        Self {
            queue,
            retention: chrono::Duration::from_std(retention)
                .unwrap_or_else(|_| chrono::Duration::days(3650)),
            responses,
            recovery,
        }
    }

    /// Drain expired recovery independently of cache validation, prune cached
    /// responses (which validates the cache tree first), then retire old queue state.
    pub(crate) fn maintain(&self) -> Result<(), ModelError> {
        self.recovery.expire_recovery()?;
        self.responses
            .cache
            .prune(self.responses.max_age, self.responses.max_bytes)?;
        let cutoff = self.queue.now() - self.retention;
        self.queue
            .retire_stale_active(cutoff, ORPHANED)
            .and_then(|_| self.queue.prune_terminal_before(cutoff))
            .map_err(|_err| ModelError::Queue(DiagnosticCode::QueueFailure))?;
        Ok(())
    }
}

/// Shared by runtime handles, bound providers and active attempts.
/// The timer owns only a weak reference, so it cannot keep its ledger alive.
pub(crate) struct Maintenance {
    sweep: Sweep,
    stop: mpsc::Sender<()>,
    worker: Mutex<Option<JoinHandle<()>>>,
    errors: watch::Sender<Option<ModelError>>,
}

impl Maintenance {
    /// Start the timer after the open-time sweep has succeeded.
    pub(crate) fn start(sweep: Sweep, interval: Duration) -> Result<Arc<Self>, ModelError> {
        let (stop, stopped) = mpsc::channel();
        let (errors, _) = watch::channel(None);
        let owner = Arc::new(Self {
            sweep,
            stop,
            worker: Mutex::new(None),
            errors,
        });
        let weak = Arc::downgrade(&owner);
        let worker = std::thread::Builder::new()
            .name("ai-runtime-maintenance".into())
            .spawn(move || loop {
                match stopped.recv_timeout(interval) {
                    Ok(()) | Err(RecvTimeoutError::Disconnected) => break,
                    Err(RecvTimeoutError::Timeout) => {
                        let Some(owner) = weak.upgrade() else { break };
                        if let Err(error) = owner.sweep.maintain() {
                            tracing::warn!(code = %error.code(), "AI runtime maintenance failed");
                            owner.errors.send_replace(Some(error));
                        }
                    }
                }
            })
            .map_err(|_| ModelError::Queue(DiagnosticCode::SpendLedgerUnavailable))?;
        *owner
            .worker
            .lock()
            .map_err(|_| ModelError::Queue(DiagnosticCode::SpendLedgerUnavailable))? = Some(worker);
        Ok(owner)
    }

    /// Retain the latest static failure diagnostic for the runtime handle.
    pub(crate) fn last_error(&self) -> Option<ModelError> {
        self.errors.borrow().clone()
    }
}

impl Drop for Maintenance {
    fn drop(&mut self) {
        // A disconnected channel means the worker has already stopped.
        let _ = self.stop.send(());
        let worker = self
            .worker
            .get_mut()
            .unwrap_or_else(|poisoned| {
                tracing::warn!("AI runtime maintenance worker lock poisoned during shutdown");
                poisoned.into_inner()
            })
            .take();
        if let Some(worker) = worker {
            // The last holder can disappear during a sweep. In that case the
            // worker drops its temporary upgrade and exits itself; never self-join.
            if worker.thread().id() == std::thread::current().id() {
                return;
            }
            if let Err(panic) = worker.join() {
                if std::thread::panicking() {
                    tracing::warn!("AI runtime maintenance worker panicked during shutdown");
                } else {
                    std::panic::resume_unwind(panic);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use symbiotic_model::{SpendLedger, SpendReceiptRef, SpendReservation, SpendState};

    #[derive(Clone)]
    struct HeldChat {
        descriptor: symbiotic_model::ProviderDescriptor,
        started: Arc<tokio::sync::Notify>,
        finish: Arc<tokio::sync::Notify>,
    }

    impl symbiotic_model::ModelProvider for HeldChat {
        fn descriptor(&self) -> &symbiotic_model::ProviderDescriptor {
            &self.descriptor
        }
    }

    #[async_trait::async_trait]
    impl symbiotic_model::ChatProvider for HeldChat {
        async fn chat(
            &self,
            _: symbiotic_model::ChatRequest,
        ) -> Result<symbiotic_model::ChatResponse, ModelError> {
            self.started.notify_one();
            self.finish.notified().await;
            Err(ModelError::Queue(
                DiagnosticCode::SpendReconciliationRequired,
            ))
        }
    }

    #[tokio::test]
    async fn abandoned_attempt_keeps_maintenance_alive_after_runtime_and_provider_drop() {
        use symbiotic_model::{
            ChatMessage, ChatRequest, ModelCapability, ProviderAuthMode, ProviderClass,
        };
        let dir = tempfile::tempdir().unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        let runtime = crate::Runtime::open(crate::RuntimeConfig {
            state_dir: Some(dir.path().to_path_buf()),
            maintenance_interval: Duration::from_millis(10),
            ..crate::RuntimeConfig::default()
        })
        .unwrap();
        let owner = runtime.inner.maintenance.as_ref().unwrap();
        let weak = Arc::downgrade(owner);
        let worker = owner.worker.lock().unwrap().take().unwrap();
        let started = Arc::new(tokio::sync::Notify::new());
        let finish = Arc::new(tokio::sync::Notify::new());
        let provider = runtime
            .chat(
                crate::ModelBinding::new(HeldChat {
                    descriptor: symbiotic_model::ProviderDescriptor {
                        identity: symbiotic_core::ModelIdentity::new("chat", "test", "held"),
                        provider_class: ProviderClass::Cloud,
                        capabilities: vec![ModelCapability::Chat],
                        auth_mode: ProviderAuthMode::None,
                        metadata: serde_json::json!({}),
                    },
                    started: started.clone(),
                    finish: finish.clone(),
                })
                .with_identity(crate::BindingIdentity::new(
                    "tenant", "provider", "1", "account",
                ))
                .with_policy(crate::ModelQueueConfig::default()),
            )
            .unwrap();
        let caller = tokio::spawn(async move {
            provider
                .chat(ChatRequest {
                    messages: vec![ChatMessage {
                        role: "user".into(),
                        content: "held".into(),
                    }],
                    max_output_tokens: Some(1),
                    temperature: None,
                    response_format: None,
                    role_binding: None,
                    source: None,
                    metadata: serde_json::json!({}),
                })
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), started.notified())
            .await
            .unwrap();
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        drop(runtime);
        assert!(
            weak.upgrade().is_some(),
            "owned attempt lost its maintenance owner"
        );
        assert!(!worker.is_finished());
        finish.notify_one();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        while !worker.is_finished() && tokio::time::Instant::now() < deadline {
            tokio::task::yield_now().await;
        }
        assert!(worker.is_finished());
        worker.join().unwrap();
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn timer_ends_when_every_holder_drops() {
        let dir = tempfile::tempdir().unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        let runtime = crate::Runtime::open(crate::RuntimeConfig {
            state_dir: Some(dir.path().to_path_buf()),
            maintenance_interval: Duration::from_millis(10),
            ..crate::RuntimeConfig::default()
        })
        .unwrap();
        let owner = runtime.inner.maintenance.as_ref().unwrap();
        let weak = Arc::downgrade(owner);
        let worker = owner.worker.lock().unwrap().take().unwrap();
        let holder = owner.clone();
        drop(runtime);
        assert!(!worker.is_finished());
        drop(holder);
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while !worker.is_finished() && std::time::Instant::now() < deadline {
            std::thread::yield_now();
        }
        assert!(worker.is_finished(), "the weak timer kept itself alive");
        worker.join().unwrap();
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn idle_runtime_maintenance_deletes_the_entire_expired_answer_backlog() {
        let dir = tempfile::tempdir().unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        let path = dir.path().join(crate::QUEUE_DATABASE);
        let _runtime = crate::Runtime::open(crate::RuntimeConfig {
            state_dir: Some(dir.path().to_path_buf()),
            maintenance_interval: Duration::from_millis(10),
            ..crate::RuntimeConfig::default()
        })
        .unwrap();
        let ledger = crate::spend::SqliteSpendLedger::open(&path)
            .unwrap()
            .with_retention(Duration::from_secs(60));
        // Commit the 129-row setup once; maintenance still drains the real file.
        {
            let mut conn = ledger.0.lock().unwrap();
            let tx = conn.transaction().unwrap();
            for n in 0..129 {
                let r = SpendReservation {
                    reference: SpendReceiptRef::new(format!("paid-{n}")).unwrap(),
                    account: "account".into(),
                    invocation: format!("explicit-{n}"),
                    binding: "input".into(),
                    request_limit: None,
                };
                crate::spend::SqliteSpendLedger::reserve_with_limit_in(&tx, &r, Some(3), false)
                    .unwrap();
                let output = Some(serde_json::json!({"answer":"private"}));
                crate::spend::SqliteSpendLedger::save_recovery_in(
                    &tx,
                    &r.reference,
                    &output,
                    Duration::from_secs(60),
                    None,
                    None,
                    chrono::Utc::now(),
                )
                .unwrap();
                crate::spend::SqliteSpendLedger::finish_in(
                    &tx,
                    &r.reference,
                    SpendState::Unknown,
                    None,
                    output,
                )
                .unwrap();
            }
            tx.commit().unwrap();
        }
        let conn = rusqlite::Connection::open(path).unwrap();
        conn.execute(
            "UPDATE spend_receipts SET recovery_expires_at='2000-01-01T00:00:00+00:00'",
            [],
        )
        .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while conn
            .query_row(
                "SELECT count(*) FROM spend_receipts WHERE recovery IS NOT NULL",
                [],
                |r| r.get::<_, usize>(0),
            )
            .unwrap()
            != 0
            && std::time::Instant::now() < deadline
        {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(conn.query_row(
            "SELECT count(*) FROM spend_receipts WHERE recovery IS NOT NULL OR recovery_expires_at IS NOT NULL",
            [], |r| r.get::<_, usize>(0)).unwrap(), 0);
        assert_eq!(
            conn.query_row("SELECT count(*) FROM spend_receipts", [], |r| r
                .get::<_, usize>(0))
                .unwrap(),
            129
        );
    }

    #[cfg(unix)]
    #[test]
    fn refused_cache_maintenance_still_expires_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("queue.sqlite");
        let queue = SqliteQueue::open(&path).unwrap();
        let ledger = crate::spend::SqliteSpendLedger::open(&path)
            .unwrap()
            .with_retention(Duration::ZERO);
        let r = SpendReservation {
            reference: SpendReceiptRef::new("paid").unwrap(),
            account: "account".into(),
            invocation: "explicit".into(),
            binding: "input".into(),
            request_limit: None,
        };
        ledger.reserve_explicit(&r, 3).unwrap();
        ledger
            .finish(
                &r.reference,
                SpendState::Unknown,
                None,
                Some(serde_json::json!({"answer":"private"})),
                None,
            )
            .unwrap();
        let cache_root = dir.path().join("responses");
        let bystander = dir.path().join("bystander");
        std::fs::create_dir(&bystander).unwrap();
        let precious = bystander.join("precious");
        std::fs::write(&precious, "untouched").unwrap();
        std::os::unix::fs::symlink(&bystander, &cache_root).unwrap();
        let maintained = Sweep::new(
            queue,
            Duration::from_secs(60),
            ResponseRetention {
                cache: DirResponseCache::new(cache_root.clone()),
                max_age: None,
                max_bytes: None,
            },
            ledger,
        );
        assert!(matches!(
            maintained.maintain(),
            Err(ModelError::Cache(
                symbiotic_core::DiagnosticCode::CachePathRefused
            ))
        ));
        assert_eq!(
            rusqlite::Connection::open(path)
                .unwrap()
                .query_row(
                    "SELECT count(*) FROM spend_receipts WHERE recovery IS NOT NULL OR recovery_expires_at IS NOT NULL",
                    [],
                    |r| r.get::<_, usize>(0)
                )
                .unwrap(),
            0
        );
        assert_eq!(std::fs::read_link(cache_root).unwrap(), bystander);
        assert_eq!(std::fs::read_to_string(precious).unwrap(), "untouched");
    }
}
