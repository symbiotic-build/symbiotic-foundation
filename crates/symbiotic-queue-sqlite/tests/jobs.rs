//! SQLite job-store acceptance cases; clocks advance deterministically.

use chrono::{DateTime, Duration, Utc};
use serde_json::json;
use symbiotic_queue::jobs::*;
use symbiotic_queue::*;
macro_rules! data {
    ($($t:tt)*) => { serde_json::to_vec(&serde_json::json!($($t)*)).unwrap() };
}
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use symbiotic_queue::runner::*;

// Count live buffers of exactly the regression's claim size, including clones.
struct PayloadAllocator;
static LARGE_BUFFERS: AtomicUsize = AtomicUsize::new(0);
const CLAIM_BYTES: usize = 8 * 1024 * 1024;
#[global_allocator]
static ALLOCATOR: PayloadAllocator = PayloadAllocator;
unsafe impl std::alloc::GlobalAlloc for PayloadAllocator {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        let ptr = unsafe { std::alloc::System.alloc(layout) };
        if layout.size() == CLAIM_BYTES && !ptr.is_null() {
            LARGE_BUFFERS.fetch_add(1, Ordering::SeqCst);
        }
        ptr
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: std::alloc::Layout) {
        if layout.size() == CLAIM_BYTES {
            LARGE_BUFFERS.fetch_sub(1, Ordering::SeqCst);
        }
        unsafe { std::alloc::System.dealloc(ptr, layout) };
    }
}

struct Handler<F>(F);
#[async_trait::async_trait]
impl<F, Fut> JobHandler for Handler<F>
where
    F: Fn(JobContext, Vec<u8>) -> Fut + Send + Sync,
    Fut: std::future::Future<Output = Result<Vec<u8>, JobFailure>> + Send,
{
    async fn run(&self, ctx: &JobContext, payload: &[u8]) -> Result<Vec<u8>, JobFailure> {
        (self.0)(ctx.clone(), payload.to_vec()).await
    }
}

fn fixed_clock(now: DateTime<Utc>) -> JobClock {
    Arc::new(move || now)
}

type AfterJob = dyn Fn(&JobRequest, &JobResponse) -> futures::future::BoxFuture<'static, Result<(), JobError>>
    + Send
    + Sync;

struct InterceptJobs<F> {
    backend: Arc<symbiotic_queue_sqlite::SqliteQueue>,
    before: F,
    after: Option<Box<AfterJob>>,
}

#[async_trait::async_trait]
impl<F> QueueBackend for InterceptJobs<F>
where
    F: Fn(&JobRequest) -> futures::future::BoxFuture<'static, Result<(), JobError>> + Send + Sync,
{
    async fn jobs(
        &self,
        scope: &JobScope,
        config: &JobConfig,
        clock: JobClock,
        request: JobRequest,
    ) -> Result<JobResponse, JobError> {
        (self.before)(&request).await?;
        let response = self
            .backend
            .jobs(scope, config, clock, request.clone())
            .await?;
        if let Some(after) = &self.after {
            after(&request, &response).await?;
        }
        Ok(response)
    }

    async fn enqueue(&self, request: EnqueueRequest) -> Result<EnqueueOutcome, QueueError> {
        self.backend.enqueue(request).await
    }
    async fn claim(&self, request: ClaimRequest) -> Result<Vec<QueueItem>, QueueError> {
        self.backend.claim(request).await
    }
    async fn claim_item(
        &self,
        id: &symbiotic_core::QueueItemId,
        worker: &str,
        lease: u64,
        max_in_flight: Option<usize>,
    ) -> Result<Option<QueueItem>, QueueError> {
        self.backend
            .claim_item(id, worker, lease, max_in_flight)
            .await
    }
    async fn get_item(
        &self,
        id: &symbiotic_core::QueueItemId,
    ) -> Result<Option<QueueItem>, QueueError> {
        self.backend.get_item(id).await
    }
    async fn heartbeat(
        &self,
        id: &symbiotic_core::QueueItemId,
        worker: &str,
        lease: u64,
    ) -> Result<(), QueueError> {
        self.backend.heartbeat(id, worker, lease).await
    }
    async fn complete(
        &self,
        id: &symbiotic_core::QueueItemId,
        worker: &str,
    ) -> Result<(), QueueError> {
        self.backend.complete(id, worker).await
    }
    async fn fail(
        &self,
        id: &symbiotic_core::QueueItemId,
        worker: &str,
        error: symbiotic_core::DiagnosticCode,
        retry: Option<u64>,
    ) -> Result<FailOutcome, QueueError> {
        self.backend.fail(id, worker, error, retry).await
    }
    async fn fail_with(
        &self,
        id: &symbiotic_core::QueueItemId,
        worker: &str,
        failure: Failure,
    ) -> Result<FailOutcome, QueueError> {
        self.backend.fail_with(id, worker, failure).await
    }
    async fn reclaim_expired_leases(
        &self,
        queue: &symbiotic_core::QueueId,
    ) -> Result<usize, QueueError> {
        self.backend.reclaim_expired_leases(queue).await
    }
}

impl Suite {
    async fn runner<F, Fut>(&self, workers: usize, handler: F) -> JobRunner
    where
        F: Fn(JobContext, Vec<u8>) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<Vec<u8>, JobFailure>> + Send,
    {
        self.runner_with_backend(self.backend.clone(), workers, handler)
            .await
    }

    async fn runner_with_backend<F, Fut>(
        &self,
        backend: Arc<dyn QueueBackend>,
        workers: usize,
        handler: F,
    ) -> JobRunner
    where
        F: Fn(JobContext, Vec<u8>) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<Vec<u8>, JobFailure>> + Send,
    {
        self.runner_with_config(
            backend,
            RunnerConfig {
                worker_count: workers,
                poll_interval_ms: 10,
                ..RunnerConfig::default()
            },
            handler,
        )
        .await
    }

    async fn runner_with_config<F, Fut>(
        &self,
        backend: Arc<dyn QueueBackend>,
        config: RunnerConfig,
        handler: F,
    ) -> JobRunner
    where
        F: Fn(JobContext, Vec<u8>) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<Vec<u8>, JobFailure>> + Send,
    {
        JobRunner::start(
            backend,
            self.scope.clone(),
            self.config.clone(),
            config,
            "handler".into(),
            Arc::new(Handler(handler)),
        )
        .await
        .unwrap()
    }

    async fn final_row(&self, id: &JobId) -> JobRecord {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let row = self.get(id).await;
                if !row.state.unfinished() {
                    return row;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap()
    }
}

/// Case 15, v1: reopen recovers an unpaid claim; checkpoints moved by §13.
#[tokio::test]
async fn runner_case_15_product_handler_recovers_without_spend_receipt() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("jobs.sqlite");
    let mut s = Suite::new();
    s.backend = Arc::new(symbiotic_queue_sqlite::SqliteQueue::open(&path).unwrap());
    s.now = Utc::now() - Duration::seconds(31);
    let id = s.insert(s.spec("recover")).await;
    let old = s.claim().await;
    drop(s.backend);
    s.backend = Arc::new(symbiotic_queue_sqlite::SqliteQueue::open(&path).unwrap());
    s.now = Utc::now();
    let runner = s
        .runner(1, move |ctx, payload| async move {
            assert_eq!(ctx.attempt, 2);
            assert_eq!(ctx.key, "recover");
            Ok(payload)
        })
        .await;
    let recovered = s.final_row(&id).await;
    assert_eq!(recovered.state, JobState::Succeeded);
    assert_eq!(recovered.generation, old.generation + 1);
    assert_eq!(recovered.origin, Some(ResultOrigin::Handler));
    assert!(recovered.receipt.is_none());
    assert!(matches!(
        s.op(JobRequest::Complete {
            job: id,
            generation: old.generation,
            state: JobState::Succeeded,
            origin: ResultOrigin::Handler,
            output: Some(b"stale".to_vec()),
            receipt: None,
            diagnostic: None,
        })
        .await,
        Err(JobError::StaleClaim)
    ));
    assert_eq!(recovered.output, Some(s.spec("recover").payload));
    runner.shutdown().await.unwrap();
}

#[tokio::test]
async fn runner_crash_before_entry_consumes_frozen_attempt_ceiling() {
    let mut s = Suite::new();
    s.now = Utc::now() - Duration::seconds(31);
    let mut spec = s.spec("crash-before-entry");
    spec.limits.max_attempts = 1;
    let id = s.insert(spec).await;
    let claim = s.claim().await;
    assert_eq!(claim.generation, 1);
    assert_eq!(claim.max_attempts, 1);
    // The worker crashes after the handoff without ever invoking its handler.
    drop(claim);
    s.now = Utc::now();
    let calls = Arc::new(AtomicUsize::new(0));
    let runner = s
        .runner(1, {
            let calls = calls.clone();
            move |_, bytes| {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Ok(bytes) }
            }
        })
        .await;
    let row = s.final_row(&id).await;
    runner.shutdown().await.unwrap();
    assert_eq!(row.state, JobState::Refused);
    assert_eq!(row.generation, 1);
    assert_eq!(row.max_attempts, 1);
    assert!(row.output.is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn runner_worker_loop_preserves_opaque_bytes_and_handler_failures_are_final() {
    let mut s = Suite::new();
    s.now = Utc::now();
    let bytes = vec![0, 255, b'\n'];
    let mut spec = s.spec("binary");
    spec.payload = bytes.clone();
    let id = s.insert(spec).await;
    let failed = s.insert(s.spec("failure")).await;
    let runner = s
        .runner(1, |ctx, bytes| async move {
            if ctx.key == "failure" {
                Err(JobFailure {
                    code: symbiotic_core::DiagnosticCode::QueueFailure,
                })
            } else {
                Ok(bytes)
            }
        })
        .await;
    assert_eq!(s.final_row(&id).await.output, Some(bytes));
    let row = s.final_row(&failed).await;
    assert_eq!(row.state, JobState::Failed);
    assert_eq!(row.generation, 1);
    assert_eq!(
        row.diagnostic,
        Some(symbiotic_core::DiagnosticCode::QueueFailure)
    );
    runner.shutdown().await.unwrap();
}

#[tokio::test]
async fn runner_worker_count_bounds_claims_and_shutdown_drains_handlers() {
    let mut s = Suite::new();
    s.now = Utc::now();
    let ids = [
        s.insert(s.spec("one")).await,
        s.insert(s.spec("two")).await,
        s.insert(s.spec("three")).await,
    ];
    let permits = Arc::new(tokio::sync::Semaphore::new(0));
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let gate = permits.clone();
    let runner = s
        .runner(2, move |ctx, bytes| {
            let (gate, tx) = (gate.clone(), tx.clone());
            async move {
                tx.send(ctx.id).unwrap();
                gate.acquire().await.unwrap().forget();
                Ok(bytes)
            }
        })
        .await;
    let first = rx.recv().await.unwrap();
    let second = rx.recv().await.unwrap();
    assert_ne!(first, second);
    let pending = ids
        .iter()
        .find(|id| **id != first && **id != second)
        .unwrap();
    assert_eq!(s.get(pending).await.state, JobState::Pending);
    let shutdown = runner.shutdown();
    tokio::pin!(shutdown);
    tokio::select! {
        biased;
        result = &mut shutdown => panic!("shutdown finished before handlers: {result:?}"),
        _ = tokio::task::yield_now() => {},
    }
    permits.add_permits(2);
    shutdown.await.unwrap();
    assert_eq!(s.get(pending).await.generation, 0);
    assert_eq!(s.get(&first).await.state, JobState::Succeeded);
    assert_eq!(s.get(&second).await.state, JobState::Succeeded);

    // Shutdown drains an in-flight maintenance pass and stops idle workers.
    let mut idle = Suite::new();
    idle.now = Utc::now();
    let entered = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Semaphore::new(0));
    let claims = Arc::new(AtomicUsize::new(0));
    let backend = Arc::new(InterceptJobs {
        after: None,
        backend: idle.backend.clone(),
        before: {
            let (entered, resume, claims) = (entered.clone(), resume.clone(), claims.clone());
            move |request: &JobRequest| {
                let (entered, resume) = (entered.clone(), resume.clone());
                let maintenance = matches!(request, JobRequest::Maintain);
                if matches!(request, JobRequest::Claim { .. }) {
                    claims.fetch_add(1, Ordering::SeqCst);
                }
                Box::pin(async move {
                    if maintenance {
                        entered.notify_one();
                        resume.acquire().await.unwrap().forget();
                    }
                    Ok(())
                }) as futures::future::BoxFuture<'static, Result<(), JobError>>
            }
        },
    });
    let runner = idle
        .runner_with_backend(backend, 1, |_, bytes| async { Ok(bytes) })
        .await;
    entered.notified().await;
    let shutdown = runner.shutdown();
    tokio::pin!(shutdown);
    tokio::select! {
        biased;
        result = &mut shutdown => panic!("shutdown finished before maintenance: {result:?}"),
        _ = tokio::task::yield_now() => {},
    }
    let stopped_claims = claims.load(Ordering::SeqCst);
    resume.add_permits(1);
    shutdown.await.unwrap();
    assert_eq!(claims.load(Ordering::SeqCst), stopped_claims);
}

#[tokio::test(start_paused = true)]
async fn runner_first_heartbeat_storage_error_before_entry_does_not_cancel() {
    let mut s = Suite::new();
    s.now = Utc::now();
    s.config.claim_lease_seconds = 1;
    let id = s.insert(s.spec("pre-entry-storage-error")).await;
    let entered = Arc::new(AtomicBool::new(false));
    let heartbeats = Arc::new(AtomicUsize::new(0));
    let recovered = Arc::new(tokio::sync::Notify::new());
    let backend = Arc::new(InterceptJobs {
        backend: s.backend.clone(),
        before: {
            let (entered, heartbeats) = (entered.clone(), heartbeats.clone());
            move |request: &JobRequest| {
                let first = matches!(request, JobRequest::Heartbeat { .. })
                    && heartbeats.fetch_add(1, Ordering::SeqCst) == 0;
                if first {
                    assert!(!entered.load(Ordering::SeqCst));
                }
                Box::pin(async move {
                    if first {
                        Err(JobError::Storage)
                    } else {
                        Ok(())
                    }
                }) as futures::future::BoxFuture<'static, Result<(), JobError>>
            }
        },
        after: Some(Box::new({
            let recovered = recovered.clone();
            move |request, response| {
                if matches!(request, JobRequest::Heartbeat { .. }) {
                    assert!(matches!(response, JobResponse::Heartbeat(false)));
                    recovered.notify_one();
                }
                Box::pin(async { Ok(()) })
            }
        })),
    });
    let runner = s
        .runner_with_backend(backend, 1, {
            let entered = entered.clone();
            move |ctx, bytes| {
                let (entered, recovered) = (entered.clone(), recovered.clone());
                async move {
                    entered.store(true, Ordering::SeqCst);
                    assert!(!ctx.cancel.is_cancelled());
                    recovered.notified().await;
                    assert!(!ctx.cancel.is_cancelled());
                    Ok(bytes)
                }
            }
        })
        .await;
    let error = tokio::time::timeout(std::time::Duration::from_secs(2), runner.wait())
        .await
        .unwrap()
        .unwrap_err();
    assert!(
        entered.load(Ordering::SeqCst),
        "claim skipped handler entry"
    );
    assert!(heartbeats.load(Ordering::SeqCst) >= 2);
    assert!(matches!(error, RunnerError::Workers(workers)
        if matches!(workers.as_slice(), [RunnerError::Workers(errors)]
            if matches!(errors.as_slice(), [RunnerError::Monitoring { cause: JobError::Storage, count: 1 }]))));
    let row = s.get(&id).await;
    assert_eq!(row.state, JobState::Succeeded);
    assert_eq!(row.generation, 1);
    assert_eq!(row.output, Some(s.spec("pre-entry-storage-error").payload));
}

#[tokio::test]
async fn runner_monitoring_storage_error_renews_lease_until_handler_finishes() {
    let mut s = Suite::new();
    s.now = Utc::now();
    s.config.claim_lease_seconds = 1;
    let id = s.insert(s.spec("draining-lease")).await;
    let started = Arc::new(AtomicBool::new(false));
    let failed = Arc::new(AtomicBool::new(false));
    let failure_observed = Arc::new(tokio::sync::Notify::new());
    let renewals = Arc::new(AtomicUsize::new(0));
    let backend = Arc::new(InterceptJobs {
        after: None,
        backend: s.backend.clone(),
        before: {
            let (started, failed, renewals) = (started.clone(), failed.clone(), renewals.clone());
            let failure_observed = failure_observed.clone();
            move |request: &JobRequest| {
                let monitoring = matches!(request, JobRequest::Heartbeat { .. });
                if matches!(request, JobRequest::Heartbeat { .. }) && failed.load(Ordering::SeqCst)
                {
                    renewals.fetch_add(1, Ordering::SeqCst);
                }
                let error = monitoring
                    && started.load(Ordering::SeqCst)
                    && !failed.swap(true, Ordering::SeqCst);
                let failure_observed = failure_observed.clone();
                Box::pin(async move {
                    if error {
                        failure_observed.notify_one();
                        Err(JobError::Storage)
                    } else {
                        Ok(())
                    }
                }) as futures::future::BoxFuture<'static, Result<(), JobError>>
            }
        },
    });
    let finish = Arc::new(tokio::sync::Semaphore::new(0));
    let runner = s
        .runner_with_backend(backend, 1, {
            let finish = finish.clone();
            move |ctx, bytes| {
                let (started, finish) = (started.clone(), finish.clone());
                async move {
                    started.store(true, Ordering::SeqCst);
                    finish.acquire().await.unwrap().forget();
                    assert!(!ctx.cancel.is_cancelled());
                    Ok(bytes)
                }
            }
        })
        .await;
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        failure_observed.notified(),
    )
    .await
    .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
    s.now = Utc::now();
    assert!(
        matches!(
            s.op(JobRequest::ClaimJob(id.clone())).await.unwrap(),
            JobResponse::Job(None)
        ),
        "draining handler lost its lease"
    );
    assert!(renewals.load(Ordering::SeqCst) >= 2);
    finish.add_permits(1);
    let error = tokio::time::timeout(std::time::Duration::from_secs(2), runner.wait())
        .await
        .unwrap()
        .unwrap_err();
    assert!(matches!(error, RunnerError::Workers(workers)
        if matches!(workers.as_slice(), [RunnerError::Workers(errors)]
            if matches!(errors.as_slice(), [RunnerError::Monitoring { cause: JobError::Storage, count: 1 }]))));
    let row = s.get(&id).await;
    assert_eq!(row.state, JobState::Succeeded);
    assert_eq!(row.generation, 1);
    assert_eq!(row.output, Some(s.spec("draining-lease").payload));
}

#[tokio::test]
async fn runner_no_lookup_between_claim_and_completion() {
    let mut s = Suite::new();
    s.now = Utc::now();
    s.config.claim_lease_seconds = 1;
    let id = s.insert(s.spec("no-running-lookup")).await;
    let started = Arc::new(AtomicBool::new(false));
    let lookups = Arc::new(AtomicUsize::new(0));
    let renewals = Arc::new(AtomicUsize::new(0));
    let backend = Arc::new(InterceptJobs {
        after: None,
        backend: s.backend.clone(),
        before: {
            let (started, lookups, renewals) = (started.clone(), lookups.clone(), renewals.clone());
            move |request: &JobRequest| {
                let running_lookup = matches!(request, JobRequest::Get(_));
                if running_lookup {
                    lookups.fetch_add(1, Ordering::SeqCst);
                }
                if matches!(request, JobRequest::Heartbeat { .. }) && started.load(Ordering::SeqCst)
                {
                    renewals.fetch_add(1, Ordering::SeqCst);
                }
                Box::pin(async move {
                    if running_lookup {
                        return Err(JobError::InvalidRequest);
                    }
                    Ok(())
                }) as futures::future::BoxFuture<'static, Result<(), JobError>>
            }
        },
    });
    let entered = Arc::new(tokio::sync::Notify::new());
    let finish = Arc::new(tokio::sync::Semaphore::new(0));
    let runner = s
        .runner_with_backend(backend, 1, {
            let (entered, finish, started) = (entered.clone(), finish.clone(), started.clone());
            move |_, bytes| {
                let (entered, finish, started) = (entered.clone(), finish.clone(), started.clone());
                async move {
                    started.store(true, Ordering::SeqCst);
                    entered.notify_one();
                    finish.acquire().await.unwrap().forget();
                    Ok(bytes)
                }
            }
        })
        .await;
    tokio::time::timeout(std::time::Duration::from_secs(2), entered.notified())
        .await
        .expect("runner did not hand the claim directly to the handler");
    tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
    assert!(
        renewals.load(Ordering::SeqCst) >= 2,
        "lookup blocked renewal"
    );
    s.now = Utc::now();
    assert!(matches!(
        s.op(JobRequest::ClaimJob(id.clone())).await.unwrap(),
        JobResponse::Job(None)
    ));
    finish.add_permits(1);
    tokio::time::timeout(std::time::Duration::from_secs(2), runner.shutdown())
        .await
        .expect("lookup blocked completion")
        .unwrap();
    assert_eq!(s.get(&id).await.state, JobState::Succeeded);
    assert_eq!(
        lookups.load(Ordering::SeqCst),
        0,
        "running handler read content"
    );
}

#[tokio::test]
async fn runner_drains_heartbeat_before_completion() {
    for failure in [None, Some(JobError::Storage), Some(JobError::StaleClaim)] {
        let mut s = Suite::new();
        s.now = Utc::now();
        let id = s.insert(s.spec("slow-heartbeat")).await;
        let entered = Arc::new(tokio::sync::Notify::new());
        let heartbeat_finish = Arc::new(tokio::sync::Semaphore::new(0));
        let heartbeats = Arc::new(AtomicUsize::new(0));
        let completions = Arc::new(AtomicUsize::new(0));
        let completion_entered = Arc::new(tokio::sync::Notify::new());
        let commits = Arc::new(AtomicUsize::new(0));
        let committed = Arc::new(tokio::sync::Notify::new());
        let failing = failure.is_some();
        let stale = matches!(failure, Some(JobError::StaleClaim));
        let backend = Arc::new(InterceptJobs {
            after: Some(Box::new({
                let (commits, committed) = (commits.clone(), committed.clone());
                move |request, response| {
                    if matches!(request, JobRequest::Complete { .. }) {
                        assert!(matches!(response, JobResponse::Done));
                        commits.fetch_add(1, Ordering::SeqCst);
                        committed.notify_one();
                    }
                    Box::pin(async { Ok(()) })
                }
            })),
            backend: s.backend.clone(),
            before: {
                let (entered, heartbeat_finish) = (entered.clone(), heartbeat_finish.clone());
                let (heartbeats, completions, completion_entered) = (
                    heartbeats.clone(),
                    completions.clone(),
                    completion_entered.clone(),
                );
                move |request: &JobRequest| {
                    let heartbeat = matches!(request, JobRequest::Heartbeat { .. })
                        && heartbeats.fetch_add(1, Ordering::SeqCst) == 0;
                    if matches!(request, JobRequest::Complete { .. }) {
                        completions.fetch_add(1, Ordering::SeqCst);
                        completion_entered.notify_one();
                    }
                    let (entered, heartbeat_finish) = (entered.clone(), heartbeat_finish.clone());
                    Box::pin(async move {
                        if heartbeat {
                            entered.notify_one();
                            heartbeat_finish.acquire().await.unwrap().forget();
                            if failing {
                                return Err(if stale {
                                    JobError::StaleClaim
                                } else {
                                    JobError::Storage
                                });
                            }
                        }
                        Ok(())
                    })
                        as futures::future::BoxFuture<'static, Result<(), JobError>>
                }
            },
        });
        let finish = Arc::new(tokio::sync::Semaphore::new(0));
        let finished = Arc::new(tokio::sync::Notify::new());
        let runner = s
            .runner_with_config(
                backend,
                RunnerConfig {
                    worker_count: 1,
                    poll_interval_ms: 10,
                    heartbeat_interval_ms: Some(10),
                    ..RunnerConfig::default()
                },
                {
                    let (finish, finished) = (finish.clone(), finished.clone());
                    move |_, bytes| {
                        let (finish, finished) = (finish.clone(), finished.clone());
                        async move {
                            finish.acquire().await.unwrap().forget();
                            finished.notify_one();
                            Ok(bytes)
                        }
                    }
                },
            )
            .await;
        entered.notified().await;
        finish.add_permits(1);
        finished.notified().await;
        // The handler has finished, but completion must wait for the in-flight heartbeat.
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(50),
                completion_entered.notified()
            )
            .await
            .is_err(),
            "completion raced the outstanding heartbeat"
        );
        assert_eq!(completions.load(Ordering::SeqCst), 0);
        assert_eq!(heartbeats.load(Ordering::SeqCst), 1);
        heartbeat_finish.add_permits(1);
        tokio::time::timeout(std::time::Duration::from_secs(2), committed.notified())
            .await
            .expect("runner did not attempt completion after draining the heartbeat");
        assert_eq!(s.get(&id).await.state, JobState::Succeeded);
        assert_eq!(completions.load(Ordering::SeqCst), 1);
        assert_eq!(commits.load(Ordering::SeqCst), 1);
        if let Some(cause) = failure {
            let error = tokio::time::timeout(std::time::Duration::from_secs(2), runner.wait())
                .await
                .expect("runner did not report the heartbeat failure")
                .unwrap_err();
            assert!(matches!(error, RunnerError::Workers(workers)
        if matches!(workers.as_slice(), [RunnerError::Workers(errors)]
            if matches!(errors.as_slice(), [RunnerError::Monitoring { cause: actual, count: 1 }] if actual == &cause))));
        } else {
            // A successful delayed heartbeat must not fence the completed claim or stop the worker.
            let next = s.insert(s.spec("after-slow-heartbeat")).await;
            finish.add_permits(1);
            assert_eq!(s.final_row(&next).await.state, JobState::Succeeded);
            runner.shutdown().await.unwrap();
            assert_eq!(completions.load(Ordering::SeqCst), 2);
            assert_eq!(commits.load(Ordering::SeqCst), 2);
        }
    }
}

#[tokio::test]
async fn runner_maintenance_passes_continue_while_handler_runs() {
    assert_eq!(RunnerConfig::default().maintenance_interval_ms, 60_000);
    let mut s = Suite::new();
    s.now = Utc::now() - Duration::seconds(2);
    s.config.retention_seconds = 1;
    s.config.maintenance_batch = 1;
    let mut expired = Vec::new();
    for key in ["expired-one", "expired-two", "expired-three"] {
        expired.push(s.ready(key).await);
    }
    s.now = Utc::now();
    let id = s.insert(s.spec("long-handler")).await;
    let started = Arc::new(tokio::sync::Notify::new());
    let finish = Arc::new(tokio::sync::Semaphore::new(0));
    let runner = s
        .runner_with_config(
            s.backend.clone(),
            RunnerConfig {
                worker_count: 1,
                poll_interval_ms: 10,
                maintenance_interval_ms: 40,
                ..RunnerConfig::default()
            },
            {
                let (started, finish) = (started.clone(), finish.clone());
                move |_, bytes| {
                    let (started, finish) = (started.clone(), finish.clone());
                    async move {
                        started.notify_one();
                        finish.acquire().await.unwrap().forget();
                        Ok(bytes)
                    }
                }
            },
        )
        .await;
    tokio::time::timeout(std::time::Duration::from_secs(2), started.notified())
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        for expired_id in expired {
            while !s.get(&expired_id).await.result_expired {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            let row = s.get(&expired_id).await;
            assert!(row.payload.is_none() && row.output.is_none());
        }
    })
    .await
    .expect("handler delayed bounded maintenance passes");
    assert_eq!(s.get(&id).await.state, JobState::Running);
    finish.add_permits(1);
    runner.shutdown().await.unwrap();
    assert_eq!(s.get(&id).await.state, JobState::Succeeded);
}

#[derive(Clone, Copy)]
enum CompletionFence {
    Live,
    Expired,
    Superseded,
}

#[tokio::test]
async fn runner_monitoring_storage_error_still_completes_finished_handler() {
    for state in [JobState::Succeeded, JobState::Failed] {
        monitoring_error_completion_case(state, CompletionFence::Live).await;
    }
}

#[tokio::test]
async fn runner_monitoring_storage_error_completion_keeps_lease_and_generation_fences() {
    for fence in [CompletionFence::Expired, CompletionFence::Superseded] {
        monitoring_error_completion_case(JobState::Succeeded, fence).await;
    }
}

async fn monitoring_error_completion_case(state: JobState, fence: CompletionFence) {
    let mut s = Suite::new();
    s.now = Utc::now();
    s.config.claim_lease_seconds = 3;
    let id = s.insert(s.spec("storage-error")).await;
    let started = Arc::new(AtomicBool::new(false));
    let failed = Arc::new(AtomicBool::new(false));
    let failure_observed = Arc::new(tokio::sync::Notify::new());
    let completions = Arc::new(AtomicUsize::new(0));
    let backend = Arc::new(InterceptJobs {
        after: None,
        backend: s.backend.clone(),
        before: {
            let (started, failed, completions) =
                (started.clone(), failed.clone(), completions.clone());
            let failure_observed = failure_observed.clone();
            move |request: &JobRequest| {
                if matches!(request, JobRequest::Complete { .. }) {
                    completions.fetch_add(1, Ordering::SeqCst);
                }
                let monitoring = matches!(request, JobRequest::Heartbeat { .. });
                let error = monitoring
                    && started.load(Ordering::SeqCst)
                    && !failed.swap(true, Ordering::SeqCst);
                let failure_observed = failure_observed.clone();
                Box::pin(async move {
                    if error {
                        failure_observed.notify_one();
                        Err(JobError::Storage)
                    } else {
                        Ok(())
                    }
                }) as futures::future::BoxFuture<'static, Result<(), JobError>>
            }
        },
    });
    let (store, scope, config) = (s.backend.clone(), s.scope.clone(), s.config.clone());
    let runner = s
        .runner_with_backend(backend, 1, move |ctx, bytes| {
            let failure_observed = failure_observed.clone();
            let (started, store, scope, config) = (
                started.clone(),
                store.clone(),
                scope.clone(),
                config.clone(),
            );
            async move {
                started.store(true, Ordering::SeqCst);
                failure_observed.notified().await;
                assert!(!ctx.cancel.is_cancelled());
                match fence {
                    CompletionFence::Live => {}
                    CompletionFence::Expired => {
                        // An older trusted clock makes the lease expire before completion.
                        store
                            .jobs(
                                &scope,
                                &config,
                                fixed_clock(Utc::now() - Duration::seconds(4)),
                                JobRequest::Heartbeat {
                                    job: ctx.id.clone(),
                                    generation: ctx.attempt,
                                },
                            )
                            .await
                            .unwrap();
                    }
                    CompletionFence::Superseded => {
                        assert!(
                            matches!(store.jobs(&scope, &config, fixed_clock(Utc::now() + Duration::seconds(4)),
                            JobRequest::ClaimJob(ctx.id.clone())).await.unwrap(),
                            JobResponse::Job(Some(row)) if row.generation == ctx.attempt + 1)
                        );
                    }
                }
                if state == JobState::Failed {
                    Err(JobFailure {
                        code: symbiotic_core::DiagnosticCode::QueueFailure,
                    })
                } else {
                    Ok(bytes)
                }
            }
        })
        .await;
    let error = tokio::time::timeout(std::time::Duration::from_secs(15), runner.wait())
        .await
        .unwrap()
        .unwrap_err();
    assert!(matches!(error, RunnerError::Workers(workers)
    if matches!(workers.as_slice(), [RunnerError::Workers(errors)]
        if match fence {
            CompletionFence::Live => matches!(errors.as_slice(), [RunnerError::Monitoring { cause: JobError::Storage, count: 1 }]),
            _ => matches!(errors.as_slice(), [RunnerError::Monitoring { cause: JobError::Storage, count: 1 }, RunnerError::Store(JobError::StaleClaim)]),
        })));
    assert!(failed.load(Ordering::SeqCst));
    assert_eq!(completions.load(Ordering::SeqCst), 1);
    let row = s.get(&id).await;
    if !matches!(fence, CompletionFence::Live) {
        assert_eq!(row.state, JobState::Running);
        assert!(row.output.is_none());
        assert_eq!(
            row.generation,
            if matches!(fence, CompletionFence::Superseded) {
                2
            } else {
                1
            }
        );
        return;
    }
    assert_eq!(row.state, state);
    assert_eq!(row.generation, 1);
    if state == JobState::Succeeded {
        assert_eq!(row.output, Some(s.spec("storage-error").payload));
    } else {
        assert_eq!(
            row.diagnostic,
            Some(symbiotic_core::DiagnosticCode::QueueFailure)
        );
    }
}

#[tokio::test]
async fn runner_renews_lease_while_handler_runs() {
    let mut s = Suite::new();
    s.now = Utc::now();
    s.config.claim_lease_seconds = 1;
    let id = s.insert(s.spec("renew")).await;
    let permits = Arc::new(tokio::sync::Semaphore::new(0));
    let gate = permits.clone();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let runner = s
        .runner(1, move |_, bytes| {
            let (gate, tx) = (gate.clone(), tx.clone());
            async move {
                tx.send(()).unwrap();
                gate.acquire().await.unwrap().forget();
                Ok(bytes)
            }
        })
        .await;
    rx.recv().await.unwrap();
    let initial = s.get(&id).await.lease_until.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
    s.now = Utc::now();
    assert!(s.get(&id).await.lease_until.unwrap() > initial);
    assert!(matches!(
        s.op(JobRequest::ClaimJob(id.clone())).await.unwrap(),
        JobResponse::Job(None)
    ));
    permits.add_permits(1);
    assert_eq!(s.final_row(&id).await.generation, 1);
    runner.shutdown().await.unwrap();
}

#[tokio::test]
async fn runner_cancel_stops_pending_at_once_and_running_handler_decides() {
    let mut s = Suite::new();
    s.now = Utc::now();
    assert!(RunnerConfig::default().heartbeat_interval_ms.is_none());
    let heartbeat_entered = Arc::new(tokio::sync::Notify::new());
    let heartbeat_gate = Arc::new(tokio::sync::Semaphore::new(0));
    let backend = Arc::new(InterceptJobs {
        after: None,
        backend: s.backend.clone(),
        before: {
            let (entered, gate) = (heartbeat_entered.clone(), heartbeat_gate.clone());
            move |request: &JobRequest| {
                let heartbeat = matches!(request, JobRequest::Heartbeat { .. });
                let (entered, gate) = (entered.clone(), gate.clone());
                Box::pin(async move {
                    if heartbeat {
                        entered.notify_one();
                        gate.acquire().await.unwrap().forget();
                    }
                    Ok(())
                }) as futures::future::BoxFuture<'static, Result<(), JobError>>
            }
        },
    });
    let running = s.insert(s.spec("running")).await;
    let permits = Arc::new(tokio::sync::Semaphore::new(0));
    let gate = permits.clone();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let (tokens, mut token_rx) = tokio::sync::mpsc::unbounded_channel();
    let runner = s
        .runner_with_config(
            backend,
            RunnerConfig {
                worker_count: 1,
                heartbeat_interval_ms: Some(100),
                ..RunnerConfig::default()
            },
            move |ctx, _| {
                tokens.send(ctx.cancel.clone()).unwrap();
                let (gate, tx) = (gate.clone(), tx.clone());
                async move {
                    tx.send(false).unwrap();
                    ctx.cancel.cancelled().await;
                    assert!(ctx.cancel.is_cancelled());
                    ctx.cancel.cancelled().await;
                    tx.send(true).unwrap();
                    gate.acquire().await.unwrap().forget();
                    Ok(b"already sent finished".to_vec())
                }
            },
        )
        .await;
    assert!(!rx.recv().await.unwrap());
    let token = token_rx.recv().await.unwrap();
    heartbeat_entered.notified().await;
    let pending = s.insert(s.spec("pending")).await;
    assert_eq!(
        s.changed(JobRequest::Cancel(Selector::Group("group".into())))
            .await,
        2
    );
    let row = s.get(&pending).await;
    assert_eq!(row.state, JobState::Cancelled);
    assert!(row.payload.is_none());
    assert_eq!(row.generation, 0);
    // A pending heartbeat has not yet read cancel intent; no separate poll may signal it.
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    assert!(!token.is_cancelled());
    heartbeat_gate.add_permits(1);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(500), rx.recv())
            .await
            .unwrap()
            .unwrap()
    );
    assert_eq!(s.get(&running).await.state, JobState::Running);
    permits.add_permits(1);
    let row = s.final_row(&running).await;
    assert_eq!(row.state, JobState::Cancelled);
    assert_eq!(row.output, Some(b"already sent finished".to_vec()));
    assert_eq!(row.payload, Some(s.spec("running").payload));
    assert_eq!(row.generation, 1);
    runner.shutdown().await.unwrap();
}

#[tokio::test]
async fn runner_external_purge_signals_token_and_keeps_no_output() {
    let mut s = Suite::new();
    s.config.claim_lease_seconds = 1;
    s.now = Utc::now();
    let id = s.insert(s.spec("purge")).await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let runner = s
        .runner(1, move |ctx, bytes| {
            let tx = tx.clone();
            async move {
                tx.send(()).unwrap();
                ctx.cancel.cancelled().await;
                Ok(bytes)
            }
        })
        .await;
    rx.recv().await.unwrap();
    s.changed(JobRequest::PurgeOwner("owner-a".into())).await;
    let row = s.final_row(&id).await;
    assert_eq!(row.state, JobState::Purged);
    assert!(row.output.is_none() && row.payload.is_none());
    runner.shutdown().await.unwrap();
}

#[tokio::test]
async fn runner_handler_panic_is_final_and_visible() {
    let mut s = Suite::new();
    s.now = Utc::now();
    let id = s.insert(s.spec("panic")).await;
    let runner = s
        .runner(1, |_, _| async { panic!("synthetic handler panic") })
        .await;
    assert!(matches!(runner.wait().await, Err(RunnerError::Workers(_))));
    let row = s.get(&id).await;
    assert_eq!(row.state, JobState::Failed);
    assert_eq!(
        row.diagnostic,
        Some(symbiotic_core::DiagnosticCode::QueueFailure)
    );
    assert_eq!(row.generation, 1);
}

#[tokio::test]
async fn runner_rejects_heartbeat_longer_than_half_the_claim_lease() {
    let mut s = Suite::new();
    s.config.claim_lease_seconds = 1;
    let operations = Arc::new(AtomicUsize::new(0));
    let backend = Arc::new(InterceptJobs {
        backend: s.backend.clone(),
        after: None,
        before: {
            let operations = operations.clone();
            move |_: &JobRequest| {
                operations.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { Ok(()) })
                    as futures::future::BoxFuture<'static, Result<(), JobError>>
            }
        },
    });
    for interval in [Some(501), Some(1_000), Some(1_001), Some(u64::MAX), None] {
        if interval.is_none() {
            s.config.claim_lease_seconds = 0;
        }
        let result = JobRunner::start(
            backend.clone(),
            s.scope.clone(),
            s.config.clone(),
            RunnerConfig {
                heartbeat_interval_ms: interval,
                ..RunnerConfig::default()
            },
            "handler".into(),
            Arc::new(Handler(|_, _| async {
                panic!("invalid policy started a handler")
            })),
        )
        .await;
        let rejected = matches!(result, Err(RunnerError::Store(JobError::InvalidRequest)));
        if let Ok(runner) = result {
            runner.shutdown().await.unwrap();
        }
        assert!(rejected, "accepted heartbeat {interval:?}");
    }
    assert_eq!(operations.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn runner_accepts_heartbeat_at_half_the_claim_lease_and_derived_default() {
    let mut s = Suite::new();
    s.config.claim_lease_seconds = 1;
    for interval in [Some(500), None] {
        let runner = s
            .runner_with_config(
                s.backend.clone(),
                RunnerConfig {
                    heartbeat_interval_ms: interval,
                    ..RunnerConfig::default()
                },
                |_, bytes| async { Ok(bytes) },
            )
            .await;
        runner.shutdown().await.unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn runner_cancel_or_purge_after_claim_uses_handoff_payload_and_heartbeat() {
    for (purge, observe_cancel_first) in
        [(false, false), (true, false), (false, true), (true, true)]
    {
        let mut s = Suite::new();
        s.now = Utc::now();
        let spec = s.spec("claimed-handoff");
        let payload = spec.payload.clone();
        let id = s.insert(spec).await;
        let heartbeat_gate = Arc::new(tokio::sync::Semaphore::new(0));
        let backend = Arc::new(InterceptJobs {
            backend: s.backend.clone(),
            before: {
                let gate = heartbeat_gate.clone();
                move |request: &JobRequest| {
                    let heartbeat = matches!(request, JobRequest::Heartbeat { .. });
                    let gate = gate.clone();
                    Box::pin(async move {
                        if heartbeat && !observe_cancel_first {
                            gate.acquire().await.unwrap().forget();
                        }
                        Ok(())
                    })
                        as futures::future::BoxFuture<'static, Result<(), JobError>>
                }
            },
            after: Some({
                let (store, scope, config) = (s.backend.clone(), s.scope.clone(), s.config.clone());
                Box::new(move |request: &JobRequest, response: &JobResponse| {
                    let job = match (request, response) {
                        (JobRequest::Claim { .. }, JobResponse::Job(Some(row))) => {
                            assert_eq!(row.state, JobState::Running);
                            assert_eq!(row.generation, 1);
                            assert!(row.payload.is_some());
                            Some(row.id.clone())
                        }
                        _ => None,
                    };
                    let (store, scope, config) = (store.clone(), scope.clone(), config.clone());
                    Box::pin(async move {
                        if let Some(job) = job {
                            let request = if purge {
                                JobRequest::PurgeOwner("owner-a".into())
                            } else {
                                JobRequest::Cancel(Selector::Ids(vec![job]))
                            };
                            assert!(matches!(
                                store
                                    .jobs(&scope, &config, Arc::new(Utc::now), request)
                                    .await?,
                                JobResponse::Changed(1)
                            ));
                        }
                        Ok(())
                    })
                        as futures::future::BoxFuture<'static, Result<(), JobError>>
                }) as Box<AfterJob>
            }),
        });
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let runner = s
            .runner_with_backend(backend, 1, move |ctx, bytes| {
                let tx = tx.clone();
                async move {
                    if observe_cancel_first {
                        ctx.cancel.cancelled().await;
                    }
                    assert_eq!(ctx.cancel.is_cancelled(), observe_cancel_first);
                    tx.send(bytes.clone()).unwrap();
                    ctx.cancel.cancelled().await;
                    Ok(bytes)
                }
            })
            .await;
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
                .await
                .expect("claim payload did not reach the handler"),
            Some(payload.clone())
        );
        if !observe_cancel_first {
            // The mutation committed before entry, but only the heartbeat signals it.
            assert_eq!(s.get(&id).await.state, JobState::Running);
            heartbeat_gate.add_permits(1);
        }
        let row = s.final_row(&id).await;
        runner.shutdown().await.unwrap();
        assert_eq!(row.generation, 1);
        if purge {
            assert_eq!(row.state, JobState::Purged);
            assert!(row.payload.is_none());
            assert!(row.output.is_none());
            assert!(row.owners.is_empty());
            let delivery = s.deliveries(1, 100_000).await.remove(0);
            assert!(delivery.completion.output.is_none());
        } else {
            assert_eq!(row.state, JobState::Cancelled);
            assert_eq!(row.payload, Some(payload.clone()));
            assert_eq!(row.output, Some(payload));
        }
    }
}

#[tokio::test]
async fn jobs_cancel_or_purge_before_claim_is_never_claimed() {
    for purge in [false, true] {
        let s = Suite::new();
        let id = s.insert(s.spec("before-claim")).await;
        let request = if purge {
            JobRequest::PurgeOwner("owner-a".into())
        } else {
            JobRequest::Cancel(Selector::Ids(vec![id.clone()]))
        };
        assert_eq!(s.changed(request).await, 1);
        for request in [
            JobRequest::Claim {
                kinds: vec!["handler".into()],
                slots_available: 1,
            },
            JobRequest::ClaimJob(id.clone()),
        ] {
            assert!(matches!(
                s.op(request).await.unwrap(),
                JobResponse::Job(None)
            ));
        }
        let row = s.get(&id).await;
        assert_eq!(row.generation, 0);
        assert_eq!(
            row.state,
            if purge {
                JobState::Purged
            } else {
                JobState::Cancelled
            }
        );
        assert!(row.payload.is_none() && row.output.is_none());
    }
}

#[tokio::test]
async fn runner_oversized_result_is_terminal_and_typed() {
    let mut s = Suite::new();
    s.now = Utc::now();
    s.config.max_result_bytes = 4;
    let id = s.insert(s.spec("oversized-result")).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let runner = s
        .runner(1, {
            let calls = calls.clone();
            move |_, _| {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Ok(vec![0; 5]) }
            }
        })
        .await;
    let error = tokio::time::timeout(std::time::Duration::from_secs(2), runner.wait())
        .await
        .unwrap()
        .unwrap_err();
    assert!(matches!(error, RunnerError::Workers(workers)
        if matches!(workers.as_slice(), [RunnerError::Workers(errors)]
            if matches!(errors.as_slice(), [RunnerError::Store(JobError::ResultTooLarge { job, bytes: 5 })] if job == &id))));
    let row = s.get(&id).await;
    assert_eq!(row.state, JobState::Failed);
    assert_eq!(
        row.diagnostic,
        Some(symbiotic_core::DiagnosticCode::QueueFailure)
    );
    assert!(row.output.is_none());
    s.advance(60);
    assert!(matches!(
        s.op(JobRequest::ClaimJob(id.clone())).await.unwrap(),
        JobResponse::Job(None)
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

/// A raw result under `max_result_bytes` whose delivery cannot fit one legal page
/// fails visibly instead of stalling every later completion in its scope.
#[tokio::test]
async fn runner_result_larger_than_a_delivery_page_fails_and_later_completions_deliver() {
    let mut s = Suite::new();
    s.now = Utc::now();
    let oversized = s.insert(s.spec("page-oversized")).await;
    let runner = s
        .runner(1, |_, _| async { Ok(vec![255; 1024 * 1024]) })
        .await;
    s.final_row(&oversized).await;
    let stopped = runner.shutdown().await;

    s.now = Utc::now() + Duration::seconds(1);
    let later = s.insert(s.spec("page-later")).await;
    let claim = s.claim().await;
    assert_eq!(claim.id, later);
    s.complete(&claim, data!({ "answer": "later" })).await;
    let deliveries = s.deliveries(64, s.config.max_page_bytes).await;
    assert_eq!(
        deliveries
            .iter()
            .map(|d| d.completion.id.clone())
            .collect::<Vec<_>>(),
        vec![oversized.clone(), later]
    );
    assert_eq!(deliveries[0].completion.state, JobState::Failed);
    assert_eq!(
        deliveries[0].completion.diagnostic,
        Some(symbiotic_core::DiagnosticCode::QueueFailure)
    );
    assert!(deliveries[0].completion.output.is_none());
    assert_eq!(
        deliveries[1].completion.output,
        Some(data!({ "answer": "later" }))
    );
    assert!(matches!(stopped, Err(RunnerError::Workers(workers))
        if matches!(workers.as_slice(), [RunnerError::Workers(errors)]
            if matches!(errors.as_slice(), [RunnerError::Store(JobError::ResultTooLarge { job, .. })] if job == &oversized))));
}

#[tokio::test]
async fn runner_handoff_retains_only_one_payload_buffer() {
    struct BorrowedHandler(Arc<tokio::sync::Notify>);
    #[async_trait::async_trait]
    impl JobHandler for BorrowedHandler {
        async fn run(&self, _: &JobContext, payload: &[u8]) -> Result<Vec<u8>, JobFailure> {
            assert_eq!(payload.len(), CLAIM_BYTES);
            assert!(payload.iter().all(|byte| *byte == 42));
            assert_eq!(
                LARGE_BUFFERS.load(Ordering::SeqCst),
                1,
                "runner copied the claim payload"
            );
            self.0.notify_one();
            Ok(Vec::new())
        }
    }
    let mut s = Suite::new();
    s.now = Utc::now();
    let mut spec = s.spec("large-claim");
    spec.payload = vec![42; CLAIM_BYTES];
    let id = s.insert(spec).await;
    let entered = Arc::new(tokio::sync::Notify::new());
    let runner = JobRunner::start(
        s.backend.clone(),
        s.scope.clone(),
        s.config.clone(),
        RunnerConfig {
            worker_count: 1,
            ..RunnerConfig::default()
        },
        "handler".into(),
        Arc::new(BorrowedHandler(entered.clone())),
    )
    .await
    .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), entered.notified())
        .await
        .unwrap();
    let row = s.final_row(&id).await;
    runner.shutdown().await.unwrap();
    assert_eq!(row.state, JobState::Succeeded);
}

#[tokio::test]
async fn runner_repeated_monitoring_errors_are_bounded_and_completion_is_separate() {
    let mut s = Suite::new();
    s.now = Utc::now();
    s.config.claim_lease_seconds = 1;
    s.insert(s.spec("repeated-monitoring")).await;
    let started = Arc::new(AtomicBool::new(false));
    let failures = Arc::new(AtomicUsize::new(0));
    let finish = Arc::new(tokio::sync::Semaphore::new(0));
    let backend = Arc::new(InterceptJobs {
        after: None,
        backend: s.backend.clone(),
        before: {
            let (started, failures, finish) = (started.clone(), failures.clone(), finish.clone());
            move |request: &JobRequest| {
                let monitoring = matches!(request, JobRequest::Heartbeat { .. })
                    && started.load(Ordering::SeqCst);
                let completing = matches!(request, JobRequest::Complete { .. });
                let failure = monitoring.then(|| {
                    let count = failures.fetch_add(1, Ordering::SeqCst) + 1;
                    if count == 8 {
                        finish.add_permits(1);
                    }
                    if count % 2 == 1 {
                        JobError::Storage
                    } else {
                        JobError::Unavailable
                    }
                });
                Box::pin(async move {
                    if let Some(error) = failure {
                        Err(error)
                    } else if completing {
                        Err(JobError::Unavailable)
                    } else {
                        Ok(())
                    }
                }) as futures::future::BoxFuture<'static, Result<(), JobError>>
            }
        },
    });
    let runner = s
        .runner_with_backend(backend, 1, move |_, bytes| {
            let (started, finish) = (started.clone(), finish.clone());
            async move {
                started.store(true, Ordering::SeqCst);
                finish.acquire().await.unwrap().forget();
                Ok(bytes)
            }
        })
        .await;
    let error = tokio::time::timeout(std::time::Duration::from_secs(5), runner.wait())
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(failures.load(Ordering::SeqCst), 8);
    let RunnerError::Workers(workers) = error else {
        panic!("{error:?}")
    };
    let [RunnerError::Workers(errors)] = workers.as_slice() else {
        panic!("{workers:?}")
    };
    assert_eq!(
        errors.len(),
        3,
        "repeated monitoring failures must be summarized"
    );
    assert!(matches!(
        errors[2],
        RunnerError::Store(JobError::Unavailable)
    ));
    assert!(matches!(
        errors[0],
        RunnerError::Monitoring {
            cause: JobError::Storage,
            count: 4
        }
    ));
    assert!(matches!(
        errors[1],
        RunnerError::Monitoring {
            cause: JobError::Unavailable,
            count: 4
        }
    ));
}

struct Suite {
    backend: Arc<symbiotic_queue_sqlite::SqliteQueue>,
    scope: JobScope,
    config: JobConfig,
    now: DateTime<Utc>,
}
impl Suite {
    fn new() -> Self {
        let backend = Arc::new(symbiotic_queue_sqlite::SqliteQueue::in_memory().unwrap());
        Self {
            backend,
            scope: JobScope {
                tenant: "tenant".into(),
                incarnation: "restore-1".into(),
                queue: "jobs".into(),
            },
            config: JobConfig::default(),
            now: DateTime::parse_from_rfc3339("2026-10-03T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
        }
    }
    fn spec(&self, key: &str) -> JobSpec {
        JobSpec {
            key: key.into(),
            group: Some("group".into()),
            owners: vec!["owner-a".into(), "owner-b".into()],
            kind: "handler".into(),
            execution: Execution::Handler,
            payload: data!({ "input": key }),
            admission: None,
            limits: JobLimits { max_attempts: 3 },
            recovery_until: None,
        }
    }
    async fn op(&self, request: JobRequest) -> Result<JobResponse, JobError> {
        self.backend
            .jobs(&self.scope, &self.config, fixed_clock(self.now), request)
            .await
    }
    async fn enqueue(&self, specs: Vec<JobSpec>) -> Vec<Enqueued> {
        match self.op(JobRequest::Enqueue(specs)).await.unwrap() {
            JobResponse::Enqueued(v) => v,
            other => panic!("{other:?}"),
        }
    }
    async fn insert(&self, spec: JobSpec) -> JobId {
        match self.enqueue(vec![spec]).await.remove(0) {
            Enqueued::Inserted(id) => id,
            other => panic!("{other:?}"),
        }
    }
    async fn get(&self, id: &JobId) -> JobRecord {
        match self.op(JobRequest::Get(id.clone())).await.unwrap() {
            JobResponse::Job(Some(row)) => *row,
            other => panic!("{other:?}"),
        }
    }
    async fn claim(&self) -> JobRecord {
        match self
            .op(JobRequest::Claim {
                kinds: vec!["handler".into()],
                slots_available: 4,
            })
            .await
            .unwrap()
        {
            JobResponse::Job(Some(row)) => *row,
            other => panic!("{other:?}"),
        }
    }
    async fn complete(&self, row: &JobRecord, output: Vec<u8>) {
        self.op(JobRequest::Complete {
            job: row.id.clone(),
            generation: row.generation,
            state: JobState::Succeeded,
            origin: ResultOrigin::Handler,
            output: Some(output),
            receipt: None,
            diagnostic: None,
        })
        .await
        .unwrap();
    }
    async fn ready(&self, key: &str) -> JobId {
        let id = self.insert(self.spec(key)).await;
        let row = self.claim().await;
        assert_eq!(id, row.id);
        self.complete(&row, data!({ "answer": key })).await;
        id
    }
    async fn deliveries(&self, limit: usize, max_bytes: usize) -> Vec<Delivery> {
        match self
            .op(JobRequest::Completions { limit, max_bytes })
            .await
            .unwrap()
        {
            JobResponse::Deliveries(v) => v.items,
            other => panic!("{other:?}"),
        }
    }
    async fn ack(&self, token: DeliveryToken, disposition: Disposition) -> AckResult {
        match self
            .op(JobRequest::Ack(vec![(token, disposition)]))
            .await
            .unwrap()
        {
            JobResponse::Acks(mut v) => v.remove(0),
            other => panic!("{other:?}"),
        }
    }

    fn advance(&mut self, seconds: i64) {
        self.now += Duration::seconds(seconds);
    }
    async fn changed(&self, request: JobRequest) -> usize {
        match self.op(request).await.unwrap() {
            JobResponse::Changed(n) => n,
            other => panic!("{other:?}"),
        }
    }
}

/// Case 4: stale issued delivery tokens remain confirmable, first disposition wins.
#[tokio::test]
async fn jobs_case_4_delivery_lease_expires_first_confirm_wins() {
    let mut s = Suite::new();
    let id = s.ready("lease").await;
    let first = s.deliveries(1, 100_000).await.remove(0).token;
    assert!(s.deliveries(1, 100_000).await.is_empty());
    s.advance(31);
    let second = s.deliveries(1, 100_000).await.remove(0).token;
    assert!(second.generation > first.generation);
    assert_eq!(
        s.ack(first, Disposition::Accepted).await,
        AckResult::Acked(Disposition::Accepted)
    );
    assert_eq!(
        s.ack(second, Disposition::Discarded).await,
        AckResult::AlreadyAcked(Disposition::Accepted)
    );
    let row = s.get(&id).await;
    assert_eq!(row.state, JobState::Accepted);
    assert!(row.payload.is_none() && row.output.is_none() && row.owners.is_empty());
    assert!(matches!(
        s.enqueue(vec![s.spec("lease")]).await[0],
        Enqueued::AlreadyDone(_)
    ));
}

/// Case 5: accepted/discarded counts remain separate and summaries rebuild exactly.
#[tokio::test]
async fn jobs_case_5_accepted_discarded_status() {
    let mut s = Suite::new();
    let accepted = s.ready("accepted").await;
    s.advance(1);
    let discarded = s.ready("discarded").await;
    let deliveries = s.deliveries(2, 100_000).await;
    s.ack(deliveries[0].token.clone(), Disposition::Accepted)
        .await;
    s.ack(deliveries[1].token.clone(), Disposition::Discarded)
        .await;
    assert_eq!(s.get(&accepted).await.state, JobState::Accepted);
    assert_eq!(s.get(&discarded).await.state, JobState::Discarded);
}

/// Case 6: replay a full pending population after reopening, then page and drain it.
#[tokio::test]
async fn jobs_case_6_backlog_bounds_and_maintenance() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("jobs.sqlite");
    let mut s = Suite::new();
    s.backend = Arc::new(symbiotic_queue_sqlite::SqliteQueue::open(&path).unwrap());
    s.config.max_live_jobs = 2;
    s.config.max_batch = 2;
    s.config.max_page = 1;
    s.config.maintenance_batch = 1;
    s.config.retention_seconds = 1;
    for base in [0, 2, 4] {
        let specs = vec![
            s.spec(&format!("job-{base}")),
            s.spec(&format!("job-{}", base + 1)),
        ];
        let inserted = s.enqueue(specs.clone()).await;
        assert!(inserted.iter().all(|e| matches!(e, Enqueued::Inserted(_))));
        drop(s.backend);
        s.backend = Arc::new(symbiotic_queue_sqlite::SqliteQueue::open(&path).unwrap());
        let replay = s.enqueue(specs).await;
        for (original, replay) in inserted.iter().zip(replay) {
            assert!(
                matches!((original, replay), (Enqueued::Inserted(a), Enqueued::Joined(b)) if *a == b)
            );
        }
        assert!(matches!(
            s.op(JobRequest::Enqueue(vec![s.spec("overflow")])).await,
            Err(JobError::QueueFull)
        ));
        for _ in 0..2 {
            let row = s.claim().await;
            s.complete(&row, data!("result")).await;
            s.advance(1);
        }
        // Final-but-unconfirmed jobs still occupy the population bound.
        assert!(matches!(
            s.op(JobRequest::Enqueue(vec![s.spec("overflow")])).await,
            Err(JobError::QueueFull)
        ));
        for _ in 0..2 {
            let page = s.deliveries(1, 100_000).await;
            assert_eq!(page.len(), 1);
            assert!(serde_json::to_vec(&page).unwrap().len() <= 100_000);
            s.ack(page[0].token.clone(), Disposition::Accepted).await;
        }
        assert!(s.deliveries(1, 100_000).await.is_empty());
    }
    let ids = [s.ready("expire-1").await, s.ready("expire-2").await];
    s.advance(2);
    assert_eq!(s.changed(JobRequest::Maintain).await, 1);
    assert_eq!(s.changed(JobRequest::Maintain).await, 1);
    assert_eq!(s.changed(JobRequest::Maintain).await, 0);
    for _ in &ids {
        let delivery = s.deliveries(1, 100_000).await.remove(0);
        assert!(ids.contains(&delivery.completion.id));
        assert!(delivery.completion.result_expired && delivery.completion.output.is_none());
        s.ack(delivery.token, Disposition::Discarded).await;
    }
}

/// Claims, finals and expired recovery copies never free capacity; confirmation does.
#[tokio::test]
async fn jobs_live_population_requires_confirm_and_survives_cap_lowering() {
    let mut s = Suite::new();
    s.config.max_live_jobs = 2;
    let a = s.insert(s.spec("a")).await;
    let b = s.insert(s.spec("b")).await;
    let running = s.claim().await;
    s.config.max_live_jobs = 1;
    assert!(matches!(
        s.enqueue(vec![s.spec("a")]).await[0],
        Enqueued::Joined(_)
    ));
    assert!(matches!(
        s.op(JobRequest::Enqueue(vec![s.spec("overflow")])).await,
        Err(JobError::QueueFull)
    ));
    s.complete(&running, data!("answer")).await;
    let remaining = s.claim().await;
    s.complete(&remaining, data!("answer")).await;
    let deliveries = s.deliveries(2, 100_000).await;
    assert_eq!(
        deliveries.len(),
        2,
        "lowering the population cap cannot strand deliveries"
    );
    s.ack(deliveries[0].token.clone(), Disposition::Accepted)
        .await;
    assert!(matches!(
        s.op(JobRequest::Enqueue(vec![s.spec("overflow")])).await,
        Err(JobError::QueueFull)
    ));
    s.ack(deliveries[1].token.clone(), Disposition::Discarded)
        .await;
    s.insert(s.spec("room-after-confirm")).await;
    for id in [a, b] {
        let row = s.get(&id).await;
        assert!(row.owners.is_empty() && row.payload.is_none() && row.output.is_none());
    }
}

/// Case 8: recovery expiry never terminates Pending or Uncertain.
#[tokio::test]
async fn jobs_case_8_recovery_expiry_is_not_execution_expiry() {
    let s = Suite::new();
    let mut spec = s.spec("expired-pending");
    spec.recovery_until = Some(s.now - Duration::seconds(1));
    let id = s.insert(spec).await;
    assert_eq!(s.changed(JobRequest::Maintain).await, 0);
    assert_eq!(s.get(&id).await.state, JobState::Pending);
    let row = s.claim().await;
    s.complete(&row, data!("late result")).await;
    let row = s.get(&id).await;
    assert_eq!(row.state, JobState::Succeeded);
    assert!(row.result_expired && row.output.is_none() && row.payload.is_none());
    let mut spec = s.spec("uncertain");
    spec.execution = Execution::Model;
    spec.recovery_until = Some(s.now - Duration::seconds(1));
    let id = s.insert(spec).await;
    let row = s.claim().await;
    s.op(JobRequest::Complete {
        job: id.clone(),
        generation: row.generation,
        state: JobState::Uncertain,
        origin: ResultOrigin::Paid,
        output: None,
        receipt: Some("receipt".into()),
        diagnostic: None,
    })
    .await
    .unwrap();
    assert_eq!(s.changed(JobRequest::Maintain).await, 0);
    let row = s.get(&id).await;
    assert_eq!(row.state, JobState::Uncertain);
    assert!(row.payload.is_some());
    assert!(matches!(
        s.op(JobRequest::Claim {
            kinds: vec!["handler".into()],
            slots_available: 1,
        })
        .await
        .unwrap(),
        JobResponse::Job(None)
    ));
}

/// Case 9: an oversized first eligible completion errors without taking any lease.
#[tokio::test]
async fn jobs_case_9_oversized_completion_is_visible() {
    let s = Suite::new();
    let id = s.insert(s.spec("large")).await;
    let row = s.claim().await;
    s.complete(&row, data!("a".repeat(5000))).await;
    assert!(
        matches!(s.op(JobRequest::Completions { limit: 1, max_bytes: 1000 }).await,
        Err(JobError::CompletionTooLarge { job, .. }) if job == id)
    );
    let row = s.get(&id).await;
    assert_eq!(row.delivery_generation, 0);
    assert!(row.delivery_until.is_none());
    assert_eq!(s.deliveries(1, 100_000).await.len(), 1);
}

/// Case 11: a foreign tenant/incarnation cannot complete, cancel, confirm or read.
#[tokio::test]
async fn jobs_case_11_foreign_scope_refused_before_access() {
    let s = Suite::new();
    let id = s.ready("scoped").await;
    let token = s.deliveries(1, 100_000).await.remove(0).token;
    for tenant in [true, false] {
        let mut scope = s.scope.clone();
        if tenant {
            scope.tenant = "other".into();
        } else {
            scope.incarnation = "restore-2".into();
        }
        for req in [
            JobRequest::Complete {
                job: id.clone(),
                generation: 1,
                state: JobState::Succeeded,
                origin: ResultOrigin::Handler,
                output: Some(data!("foreign")),
                receipt: None,
                diagnostic: None,
            },
            JobRequest::Cancel(Selector::Ids(vec![id.clone()])),
            JobRequest::Ack(vec![(token.clone(), Disposition::Discarded)]),
            JobRequest::Get(id.clone()),
        ] {
            assert!(matches!(
                s.backend
                    .jobs(&scope, &s.config, fixed_clock(s.now), req)
                    .await,
                Err(JobError::Scope)
            ));
        }
        // Spoofing the scope on an ID cannot find another scope's row either.
        let mut spoof = id.clone();
        spoof.scope = scope.clone();
        assert!(matches!(
            s.backend
                .jobs(
                    &scope,
                    &s.config,
                    fixed_clock(s.now),
                    JobRequest::Get(spoof)
                )
                .await,
            Err(JobError::NotFound)
        ));
    }
    assert_eq!(s.get(&id).await.state, JobState::Succeeded);
    assert_eq!(
        s.ack(token, Disposition::Accepted).await,
        AckResult::Acked(Disposition::Accepted)
    );
}

/// Owner erasure fences a late running handler result.
#[tokio::test]
async fn jobs_owner_purge_fences_result_commit() {
    let s = Suite::new();
    // Running completion serializes against the same sticky flag.
    let id = s.insert(s.spec("running")).await;
    let row = s.claim().await;
    s.changed(JobRequest::PurgeOwner("owner-b".into())).await;
    s.complete(&row, data!("late handler output")).await;
    let row = s.get(&id).await;
    assert_eq!(row.state, JobState::Purged);
    assert!(row.output.is_none() && row.payload.is_none());
}

/// Case 20: superseded claims cannot heartbeat or complete.
#[tokio::test]
async fn jobs_case_20_claim_generation_fencing() {
    let mut s = Suite::new();
    let id = s.insert(s.spec("fencing")).await;
    let mut other = s.spec("other-running");
    other.kind = "other".into();
    let other_running = s.insert(other).await;
    s.op(JobRequest::ClaimJob(other_running.clone()))
        .await
        .unwrap();
    let mut other = s.spec("other-pending");
    other.kind = "other".into();
    let other_pending = s.insert(other).await;
    let first = s.claim().await;
    s.advance(31);
    let excluded = [s.get(&other_running).await, s.get(&other_pending).await];
    match s
        .op(JobRequest::Candidates {
            kinds: vec!["handler".into()],
            limit: 4,
            max_bytes: 100_000,
        })
        .await
        .unwrap()
    {
        JobResponse::Candidates(rows) => {
            assert_eq!(rows.iter().map(|r| &r.id).collect::<Vec<_>>(), vec![&id])
        }
        other => panic!("{other:?}"),
    }
    let second = s.claim().await;
    assert_eq!(second.id, id);
    for before in excluded {
        assert_eq!(
            serde_json::to_value(s.get(&before.id).await).unwrap(),
            serde_json::to_value(before).unwrap()
        );
    }
    assert!(second.generation > first.generation);
    for req in [
        JobRequest::Heartbeat {
            job: id.clone(),
            generation: first.generation,
        },
        JobRequest::Complete {
            job: id.clone(),
            generation: first.generation,
            state: JobState::Succeeded,
            origin: ResultOrigin::Handler,
            output: Some(data!("old")),
            receipt: None,
            diagnostic: None,
        },
    ] {
        assert!(matches!(s.op(req).await, Err(JobError::StaleClaim)));
    }
    s.complete(&second, data!("new")).await;
    assert_eq!(s.get(&id).await.output, Some(data!("new")));
}

/// Batch conflicts and capacity errors roll back inserted jobs.
#[tokio::test]
async fn jobs_atomic_batch_key_conflicts_and_byte_bounds() {
    let mut s = Suite::new();
    let existing = s.insert(s.spec("key")).await;
    let mut conflict = s.spec("key");
    conflict.payload = data!("different");
    assert!(matches!(
        s.op(JobRequest::Enqueue(vec![s.spec("new"), conflict]))
            .await,
        Err(JobError::KeyConflict)
    ));
    assert!(matches!(
        s.enqueue(vec![s.spec("new")]).await[0],
        Enqueued::Inserted(_)
    ));
    s.config.max_pending_bytes = 1;
    assert!(matches!(
        s.op(JobRequest::Enqueue(vec![s.spec("bytes")])).await,
        Err(JobError::QueueFull)
    ));
    assert!(matches!(
        s.enqueue(vec![s.spec("key")]).await[0],
        Enqueued::Joined(_)
    ));
    assert_eq!(s.get(&existing).await.max_attempts, 3);
    let mut duplicate = s.spec("duplicate");
    duplicate.payload = data!("same");
    let mut conflict = duplicate.clone();
    conflict.payload = data!("other");
    s.config.max_pending_bytes = 100_000;
    assert!(matches!(
        s.op(JobRequest::Enqueue(vec![duplicate.clone(), conflict]))
            .await,
        Err(JobError::KeyConflict)
    ));
    assert!(matches!(
        s.enqueue(vec![duplicate]).await[0],
        Enqueued::Inserted(_)
    ));
}

/// Pending cancellation deletes input; running cancellation keeps the completed answer.
#[tokio::test]
async fn jobs_cancel_and_result_origins() {
    let s = Suite::new();
    let waiting = s.insert(s.spec("waiting")).await;
    assert_eq!(
        s.changed(JobRequest::Cancel(Selector::Ids(vec![waiting.clone()])))
            .await,
        1
    );
    assert!(s.get(&waiting).await.payload.is_none());
    let running = s.insert(s.spec("running")).await;
    let row = s.claim().await;
    assert_eq!(
        s.changed(JobRequest::Cancel(Selector::Group("group".into())))
            .await,
        1
    );
    assert!(s.get(&running).await.cancel_requested);
    s.complete(&row, data!("answer retained")).await;
    let row = s.get(&running).await;
    assert_eq!(row.state, JobState::Cancelled);
    assert_eq!(row.output, Some(data!("answer retained")));
    assert_eq!(row.payload, Some(s.spec("running").payload));
    assert_eq!(row.origin, Some(ResultOrigin::Handler));
    assert!(row.receipt.is_none());
    let mut paid = s.spec("paid");
    paid.execution = Execution::Model;
    let id = s.insert(paid).await;
    let row = s.claim().await;
    assert!(matches!(
        s.op(JobRequest::Complete {
            job: id.clone(),
            generation: row.generation,
            state: JobState::Succeeded,
            origin: ResultOrigin::Paid,
            output: Some(data!("duplicated ledger output")),
            receipt: Some("receipt".into()),
            diagnostic: None
        })
        .await,
        Err(JobError::InvalidRequest)
    ));
    s.op(JobRequest::Complete {
        job: id.clone(),
        generation: row.generation,
        state: JobState::Succeeded,
        origin: ResultOrigin::Paid,
        output: None,
        receipt: Some("receipt".into()),
        diagnostic: None,
    })
    .await
    .unwrap();
    let row = s.get(&id).await;
    assert_eq!(row.origin, Some(ResultOrigin::Paid));
    assert_eq!(row.receipt.as_deref(), Some("receipt"));
}

#[tokio::test]
async fn jobs_running_cancel_retains_payload_until_confirm_expiry_or_purge() {
    for cleanup in ["confirm", "expiry", "purge"] {
        let mut s = Suite::new();
        let spec = s.spec("running-cancel");
        let payload = spec.payload.clone();
        let id = s.insert(spec).await;
        let claim = s.claim().await;
        s.changed(JobRequest::Cancel(Selector::Ids(vec![id.clone()])))
            .await;
        s.complete(&claim, data!("answer")).await;
        let row = s.get(&id).await;
        assert_eq!(row.state, JobState::Cancelled);
        assert_eq!(row.payload, Some(payload.clone()));
        assert_eq!(row.output, Some(data!("answer")));
        let delivery = s.deliveries(1, 100_000).await.remove(0);
        assert_eq!(s.get(&id).await.payload, Some(payload));
        match cleanup {
            "confirm" => {
                assert_eq!(
                    s.ack(delivery.token, Disposition::Accepted).await,
                    AckResult::Acked(Disposition::Accepted)
                );
            }
            "expiry" => {
                s.now = row.recovery_until.unwrap();
                assert_eq!(s.changed(JobRequest::Maintain).await, 1);
                assert!(s.get(&id).await.result_expired);
            }
            "purge" => {
                assert_eq!(s.changed(JobRequest::PurgeOwner("owner-a".into())).await, 1);
                assert!(s.get(&id).await.purged);
            }
            _ => unreachable!(),
        }
        let row = s.get(&id).await;
        assert!(row.payload.is_none() && row.output.is_none());
    }
}

/// Expired handler claims can be cancelled/erased without stranding unfinished rows.
#[tokio::test]
async fn jobs_expired_handler_cancel_and_purge() {
    let mut s = Suite::new();
    for purge in [false, true] {
        let id = s
            .insert(s.spec(if purge {
                "erase-expired"
            } else {
                "cancel-expired"
            }))
            .await;
        let claimed = s.claim().await;
        s.advance(31);
        if purge {
            s.changed(JobRequest::PurgeOwner("owner-a".into())).await;
        } else {
            s.changed(JobRequest::Cancel(Selector::Ids(vec![id.clone()])))
                .await;
        }
        let row = s.get(&id).await;
        assert_eq!(
            row.state,
            if purge {
                JobState::Purged
            } else {
                JobState::Cancelled
            }
        );
        assert!(row.payload.is_none());
        assert!(matches!(
            s.op(JobRequest::Heartbeat {
                job: id,
                generation: claimed.generation,
            })
            .await,
            Err(JobError::StaleClaim)
        ));
    }
    for purge in [false, true] {
        let id = s
            .insert(s.spec(if purge {
                "erase-before-expiry"
            } else {
                "cancel-before-expiry"
            }))
            .await;
        s.claim().await;
        if purge {
            s.changed(JobRequest::PurgeOwner("owner-a".into())).await;
        } else {
            s.changed(JobRequest::Cancel(Selector::Ids(vec![id.clone()])))
                .await;
        }
        s.advance(31);
        assert!(matches!(
            s.op(JobRequest::Claim {
                kinds: vec!["handler".into()],
                slots_available: 1,
            })
            .await
            .unwrap(),
            JobResponse::Job(None)
        ));
        assert_eq!(
            s.get(&id).await.state,
            if purge {
                JobState::Purged
            } else {
                JobState::Cancelled
            }
        );
    }
    let mut paid = s.spec("paid-expired-cancel");
    paid.execution = Execution::Model;
    let id = s.insert(paid).await;
    s.claim().await;
    s.advance(31);
    s.changed(JobRequest::Cancel(Selector::Ids(vec![id.clone()])))
        .await;
    let row = s.get(&id).await;
    assert_eq!(row.state, JobState::Running);
    assert!(row.cancel_requested && row.payload.is_some());
}

/// Handler results cannot invent receipt accounting.
#[tokio::test]
async fn jobs_result_origin_contracts() {
    let s = Suite::new();
    let id = s.insert(s.spec("origin-contract")).await;
    let row = s.claim().await;
    for (origin, generation, output, receipt) in [
        (
            ResultOrigin::Handler,
            row.generation,
            Some(data!("handler")),
            Some("fake-receipt".into()),
        ),
        (ResultOrigin::Handler, row.generation, None, None),
    ] {
        assert!(matches!(
            s.op(JobRequest::Complete {
                job: id.clone(),
                generation,
                state: JobState::Succeeded,
                origin,
                output,
                receipt,
                diagnostic: None,
            })
            .await,
            Err(JobError::InvalidRequest)
        ));
        assert_eq!(s.get(&id).await.state, JobState::Running);
    }
    s.complete(&row, b"null".to_vec()).await;
}

/// Failed and uncertain diagnostics page in ID order without omissions.
#[tokio::test]
async fn jobs_diagnostics_paginate() {
    let s = Suite::new();
    let mut ids = Vec::new();
    for i in 0..5 {
        let mut spec = s.spec(&format!("diagnostic-{i}"));
        if i % 2 == 0 {
            spec.execution = Execution::Model;
        }
        let id = s.insert(spec).await;
        let row = s.claim().await;
        s.op(JobRequest::Complete {
            job: id.clone(),
            generation: row.generation,
            state: if i % 2 == 0 {
                JobState::Uncertain
            } else {
                JobState::Failed
            },
            origin: if i % 2 == 0 {
                ResultOrigin::Paid
            } else {
                ResultOrigin::Handler
            },
            output: None,
            receipt: (i % 2 == 0).then(|| "receipt".into()),
            diagnostic: Some(symbiotic_core::DiagnosticCode::SpendReconciliationRequired),
        })
        .await
        .unwrap();
        ids.push(id.id);
    }
    ids.sort();
    let mut after = None;
    let mut actual = Vec::new();
    loop {
        match s
            .op(JobRequest::Diagnostics {
                group: "group".into(),
                after: after.clone(),
                limit: 2,
            })
            .await
            .unwrap()
        {
            JobResponse::Diagnostics(page) => {
                assert!(page.items.len() <= 2);
                if page.items.is_empty() {
                    break;
                }
                after = page.after;
                actual.extend(page.items.into_iter().map(|d| d.id.id));
            }
            other => panic!("{other:?}"),
        }
    }
    assert_eq!(actual, ids);
}

/// An oversized later item fails the whole call and rolls back earlier leases.
#[tokio::test]
async fn jobs_oversized_later_completion_rolls_back_page() {
    let mut s = Suite::new();
    let first = s.ready("small").await;
    s.advance(1);
    let second = s.insert(s.spec("large-second")).await;
    let row = s.claim().await;
    s.complete(&row, data!("x".repeat(5000))).await;
    assert!(
        matches!(s.op(JobRequest::Completions { limit: 2, max_bytes: 2500 }).await,
        Err(JobError::CompletionTooLarge { job, .. }) if job == second)
    );
    for id in [&first, &second] {
        let row = s.get(id).await;
        assert_eq!(row.delivery_generation, 0);
        assert!(row.delivery_until.is_none());
    }
}

/// Invalid policy/page/lease values fail before writes, and expired claims are fenced.
#[tokio::test]
async fn jobs_invalid_bounds_and_leases_leave_store_usable() {
    let mut s = Suite::new();
    let id = s.insert(s.spec("invalid")).await;
    s.config.maintenance_bytes_per_pass = 0;
    assert!(matches!(
        s.op(JobRequest::Maintain).await,
        Err(JobError::InvalidRequest)
    ));
    s.config.maintenance_bytes_per_pass = JobConfig::default().maintenance_bytes_per_pass;
    s.config.claim_lease_seconds = u64::MAX;
    assert!(matches!(
        s.op(JobRequest::Claim {
            kinds: vec!["handler".into()],
            slots_available: 1,
        })
        .await,
        Err(JobError::InvalidRequest)
    ));
    s.config.claim_lease_seconds = 30;
    assert_eq!(s.get(&id).await.attempt(), 0);
    for (limit, max_bytes) in [(0, 1000), (65, 1000), (1, 1), (1, usize::MAX)] {
        assert!(matches!(
            s.op(JobRequest::Completions { limit, max_bytes }).await,
            Err(JobError::InvalidRequest)
        ));
    }
    s.claim().await;
    s.advance(31);
    // Paid expired leases cannot be reclaimed just because the account has capacity.
    s.complete(&s.claim().await, data!("handler recovered"))
        .await;
    let mut paid = s.spec("paid-lease");
    paid.execution = Execution::Model;
    let id = s.insert(paid).await;
    let row = s.claim().await;
    s.advance(31);
    assert!(matches!(
        s.op(JobRequest::Claim {
            kinds: vec!["handler".into()],
            slots_available: 4,
        })
        .await
        .unwrap(),
        JobResponse::Job(None)
    ));
    assert_eq!(s.get(&id).await.generation, row.generation);
    assert!(s.get(&id).await.payload.is_some());
}

/// Inspection is bounded and read-only; handoff claims the same ID.
#[tokio::test]
async fn jobs_candidate_claim_is_bounded() {
    let s = Suite::new();
    let id = s.insert(s.spec("candidate")).await;
    match s
        .op(JobRequest::Candidates {
            kinds: vec!["handler".into()],
            limit: 1,
            max_bytes: 100_000,
        })
        .await
        .unwrap()
    {
        JobResponse::Candidates(rows) => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].id, id);
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(s.get(&id).await.attempt(), 0);
    assert!(matches!(s.op(JobRequest::Candidates {
            kinds: vec!["handler".into()], limit: 1, max_bytes: 100
        }).await, Err(JobError::CandidateTooLarge { job, .. }) if job == id));
    let row = match s.op(JobRequest::ClaimJob(id.clone())).await.unwrap() {
        JobResponse::Job(Some(row)) => row,
        other => panic!("{other:?}"),
    };
    assert_eq!(row.id, id);
    assert_eq!(row.attempt(), 1);
    assert!(matches!(
        s.op(JobRequest::ClaimJob(id)).await.unwrap(),
        JobResponse::Job(None)
    ));
}

/// Direct handler claims accept inputs whose numeric-array encoding exceeds a page.
#[tokio::test]
async fn jobs_enqueue_accepts_handler_input_larger_than_candidate_page() {
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        let s = Suite::new();
        let mut spec = s.spec("large-handler-input");
        spec.payload = vec![b'x'; 512 * 1024];
        assert!(spec.payload.len() < s.config.max_pending_bytes);
        let id = s.insert(spec.clone()).await;
        assert!(encoded_bytes(&s.get(&id).await).unwrap() + 2 > s.config.max_page_bytes);

        let row = s.claim().await;
        assert_eq!(row.id, id);
        assert_eq!(row.payload.as_ref(), Some(&spec.payload));
        s.complete(&row, Vec::new()).await;
        assert_eq!(s.get(&id).await.state, JobState::Succeeded);
    })
    .await
    .expect("handler enqueue and direct claim must finish within two seconds");
}

/// Numeric-array expansion is bounded before any member of an enqueue batch commits.
#[tokio::test]
async fn jobs_enqueue_refuses_unreadable_input() {
    let probe = Suite::new();
    let mut spec = probe.spec("chat-page-bound");
    spec.execution = Execution::Model;
    spec.payload = Vec::new();
    spec.admission = Some(b"signed authority".to_vec());
    let probe_id = probe.insert(spec.clone()).await;
    let mut expected = probe.get(&probe_id).await;
    spec.payload = vec![b'x'; JobConfig::default().max_page_bytes / 4];
    expected.payload = Some(spec.payload.clone());
    let bytes = encoded_bytes(&expected).unwrap() + 2;
    assert!(bytes > probe.config.max_page_bytes);
    assert!(spec.payload.len() < probe.config.max_page_bytes);

    let mut s = Suite::new();
    // The default bound refuses the expanded input, despite ample raw-byte capacity.
    assert!(matches!(
        s.op(JobRequest::Enqueue(vec![spec.clone()])).await,
        Err(JobError::CandidateTooLarge { bytes: actual, .. }) if actual == bytes
    ));
    s.config.max_page_bytes = bytes - 1;
    assert!(matches!(
        s.op(JobRequest::Enqueue(vec![s.spec("batch-prefix"), spec.clone()])).await,
        Err(JobError::CandidateTooLarge { bytes: actual, .. }) if actual == bytes
    ));
    let JobResponse::Usage(usage) = s.op(JobRequest::PendingUsage).await.unwrap() else {
        panic!("usage")
    };
    assert_eq!(usage.items, 0);
    assert_eq!(usage.bytes, 0);

    // The identical input fits just under the configured bound and reads back.
    s.config.max_page_bytes = bytes + 1;
    let id = s.insert(spec.clone()).await;
    let JobResponse::Candidates(rows) = s
        .op(JobRequest::Candidates {
            kinds: vec![spec.kind],
            limit: 1,
            max_bytes: s.config.max_page_bytes,
        })
        .await
        .unwrap()
    else {
        panic!("candidates")
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, id);
    assert_eq!(rows[0].payload.as_ref(), Some(&spec.payload));
    assert_eq!(encoded_bytes(&rows).unwrap(), bytes);
}

#[tokio::test]
async fn jobs_admission_notice_cursor_pages_unresolved_rows_in_id_order() {
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        let mut s = Suite::new();
        s.config.max_page = 2;
        let mut expected = Vec::new();
        for i in 0..5 {
            let mut spec = s.spec(&format!("notice-{i}"));
            spec.execution = Execution::Model;
            spec.admission = Some(b"synthetic authority".to_vec());
            let id = s.insert(spec).await;
            s.op(JobRequest::AwaitAdmission(id.clone())).await.unwrap();
            expected.push(id);
        }
        expected.sort_by(|a, b| a.id.cmp(&b.id));
        let mut after = None;
        let mut seen = Vec::new();
        for count in [2, 2, 1, 0] {
            let JobResponse::Diagnostics(page) = s
                .op(JobRequest::AdmissionNotices { after, limit: 2 })
                .await
                .unwrap()
            else {
                panic!("notice page")
            };
            assert_eq!(page.items.len(), count);
            assert_eq!(page.after, page.items.last().map(|n| n.id.id.clone()));
            seen.extend(page.items.iter().map(|n| n.id.clone()));
            after = page.after;
        }
        assert_eq!(seen, expected);
        let JobResponse::Diagnostics(first) = s
            .op(JobRequest::AdmissionNotices {
                after: None,
                limit: 2,
            })
            .await
            .unwrap()
        else {
            panic!("first page")
        };
        assert_eq!(
            first.items.iter().map(|n| n.id.clone()).collect::<Vec<_>>(),
            expected[..2]
        );
        for id in expected {
            assert_eq!(s.get(&id).await.state, JobState::AwaitingAdmission);
        }
    })
    .await
    .expect("bounded store notice pagination");
}

/// Renewed authority cannot grow a claimable record beyond its candidate page.
#[tokio::test]
async fn jobs_admission_refuses_unreadable_input() {
    let mut s = Suite::new();
    let mut spec = s.spec("renew-page-bound");
    spec.execution = Execution::Model;
    spec.admission = Some(b"old authority".to_vec());
    let id = s.insert(spec.clone()).await;
    let admission = vec![b'x'; s.config.max_page_bytes / 4];
    let mut expected = s.get(&id).await;
    expected.admission = Some(admission.clone());
    let bytes = encoded_bytes(&expected).unwrap() + 2;
    assert!(bytes > s.config.max_page_bytes);

    for state in [JobState::Pending, JobState::AwaitingAdmission] {
        if state == JobState::AwaitingAdmission {
            s.op(JobRequest::AwaitAdmission(id.clone())).await.unwrap();
        }
        s.config.max_page_bytes = bytes - 1;
        assert!(matches!(
            s.op(JobRequest::Admit { job: id.clone(), admission: admission.clone() }).await,
            Err(JobError::CandidateTooLarge { job, bytes: actual }) if job == id && actual == bytes
        ));
        let row = s.get(&id).await;
        assert_eq!(row.state, state);
        assert_eq!(row.admission, spec.admission);
        assert_eq!(row.generation, 0);
    }

    s.config.max_page_bytes = bytes + 1;
    s.op(JobRequest::Admit {
        job: id.clone(),
        admission: admission.clone(),
    })
    .await
    .unwrap();
    let JobResponse::Candidates(rows) = s
        .op(JobRequest::Candidates {
            kinds: vec![spec.kind],
            limit: 1,
            max_bytes: s.config.max_page_bytes,
        })
        .await
        .unwrap()
    else {
        panic!("candidates")
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, id);
    assert_eq!(rows[0].state, JobState::Pending);
    assert_eq!(rows[0].admission.as_ref(), Some(&admission));
    assert_eq!(encoded_bytes(&rows).unwrap(), bytes);
}

/// A mixed final/pending ack batch or foreign selector cannot partly commit.
#[tokio::test]
async fn jobs_ack_and_cancel_batches_roll_back() {
    let s = Suite::new();
    let id = s.ready("final").await;
    let token = s.deliveries(1, 100_000).await.remove(0).token;
    let pending = s.insert(s.spec("pending")).await;
    assert!(matches!(
        s.op(JobRequest::Ack(vec![
            (token.clone(), Disposition::Accepted),
            (
                DeliveryToken {
                    job: pending.clone(),
                    generation: 1
                },
                Disposition::Discarded
            )
        ]))
        .await,
        Err(JobError::NotFinal)
    ));
    assert_eq!(s.get(&id).await.state, JobState::Succeeded);
    let mut foreign = pending.clone();
    foreign.scope.incarnation = "foreign".into();
    assert!(matches!(
        s.op(JobRequest::Cancel(Selector::Ids(vec![
            pending.clone(),
            foreign
        ])))
        .await,
        Err(JobError::Scope)
    ));
    assert_eq!(s.get(&pending).await.state, JobState::Pending);
    assert_eq!(
        s.ack(token, Disposition::Accepted).await,
        AckResult::Acked(Disposition::Accepted)
    );
}

/// Paid recovery is explicit, fenced and cannot be converted to an automatic retry.
#[tokio::test]
async fn jobs_paid_reconciliation_requires_explicit_evidence() {
    let mut s = Suite::new();
    let mut spec = s.spec("paid-recovery");
    spec.execution = Execution::Model;
    spec.recovery_until = Some(s.now - Duration::seconds(1));
    let id = s.insert(spec).await;
    let first = s.claim().await;
    s.advance(31);
    s.op(JobRequest::Resolve {
        job: id.clone(),
        generation: first.generation,
        resolution: JobResolution::Uncertain {
            receipt: "receipt-1".into(),
        },
    })
    .await
    .unwrap();
    assert_eq!(s.get(&id).await.state, JobState::Uncertain);
    assert!(s.get(&id).await.payload.is_some());
    assert!(matches!(
        s.op(JobRequest::ClaimJob(id.clone())).await.unwrap(),
        JobResponse::Job(None)
    ));
    s.op(JobRequest::Resolve {
        job: id.clone(),
        generation: first.generation,
        resolution: JobResolution::KnownZeroCharge {
            receipt: "receipt-1".into(),
        },
    })
    .await
    .unwrap();
    let second = s.claim().await;
    assert_eq!(second.attempt(), 2);
    assert!(matches!(
        s.op(JobRequest::Resolve {
            job: id.clone(),
            generation: first.generation,
            resolution: JobResolution::PaidResult {
                receipt: "stale".into(),
                recovery_until: None
            }
        })
        .await,
        Err(JobError::StaleClaim)
    ));
    s.op(JobRequest::Resolve {
        job: id.clone(),
        generation: second.generation,
        resolution: JobResolution::PaidResult {
            receipt: "receipt-2".into(),
            recovery_until: None,
        },
    })
    .await
    .unwrap();
    let row = s.get(&id).await;
    assert_eq!(row.state, JobState::Succeeded);
    assert!(row.result_expired && row.payload.is_none() && row.output.is_none());
    assert_eq!(row.receipt.as_deref(), Some("receipt-2"));
    // A committed direct-call result is recovered without a new claim.
    let mut spec = s.spec("direct");
    spec.execution = Execution::Model;
    let id = s.insert(spec).await;
    s.op(JobRequest::Resolve {
        job: id.clone(),
        generation: 0,
        resolution: JobResolution::PaidResult {
            receipt: "direct-receipt".into(),
            recovery_until: Some(s.now + Duration::seconds(60)),
        },
    })
    .await
    .unwrap();
    let row = s.get(&id).await;
    assert_eq!(row.attempt(), 0);
    assert_eq!(row.state, JobState::Succeeded);
    assert_eq!(row.origin, Some(ResultOrigin::Paid));
}

/// Empty bytes remain distinct from an erased waiting copy or missing answer.
#[tokio::test]
async fn jobs_empty_payload_and_result_round_trip() {
    let s = Suite::new();
    let mut spec = s.spec("empty");
    spec.payload = Vec::new();
    let id = s.insert(spec).await;
    let row = s.get(&id).await;
    let recovered: JobRecord = serde_json::from_value(serde_json::to_value(&row).unwrap()).unwrap();
    assert_eq!(recovered.payload, Some(Vec::new()));
    let row = s.claim().await;
    s.complete(&row, Vec::new()).await;
    let delivery = s.deliveries(1, 100_000).await.remove(0);
    let recovered: Delivery =
        serde_json::from_value(serde_json::to_value(&delivery).unwrap()).unwrap();
    assert_eq!(recovered.completion.output, Some(Vec::new()));
    assert!(recovered.completion.payload.is_none());
    s.ack(recovered.token, Disposition::Accepted).await;
    let row = s.get(&id).await;
    assert!(row.payload.is_none() && row.output.is_none());
}

/// Arbitrary bytes, including invalid UTF-8 and empty content, are valid data.
#[tokio::test]
async fn jobs_binary_bytes_and_raw_size_bounds() {
    let mut s = Suite::new();
    s.config.max_result_bytes = 4;
    let mut spec = s.spec("binary");
    spec.payload = (0..=255).collect();
    let id = s.insert(spec.clone()).await;
    assert_eq!(s.get(&id).await.payload, Some(spec.payload.clone()));
    assert!(matches!(
        s.enqueue(vec![spec.clone()]).await[0],
        Enqueued::Joined(_)
    ));
    spec.payload.push(0);
    assert!(matches!(
        s.op(JobRequest::Enqueue(vec![spec])).await,
        Err(JobError::KeyConflict)
    ));
    let row = s.claim().await;
    assert!(matches!(
        s.op(JobRequest::Complete {
            job: id.clone(),
            generation: row.generation,
            state: JobState::Succeeded,
            origin: ResultOrigin::Handler,
            output: Some(vec![255; 5]),
            receipt: None,
            diagnostic: None,
        })
        .await,
        Err(JobError::ResultTooLarge { bytes: 5, .. })
    ));
    assert_eq!(s.get(&id).await.state, JobState::Running);
    s.complete(&row, vec![255; 4]).await;
    let delivery = s.deliveries(1, 100_000).await.remove(0);
    assert_eq!(delivery.completion.output, Some(vec![255; 4]));
    assert_eq!(
        delivery.completion.output_bytes,
        serde_json::to_vec(&vec![255u8; 4]).unwrap().len()
    );
    assert!(serde_json::to_vec(&vec![delivery.clone()]).unwrap().len() <= 100_000);
    s.ack(delivery.token, Disposition::Accepted).await;
    assert!(s.get(&id).await.output.is_none());
}

/// Completion accepts exactly the handler results one maximum delivery page can
/// carry, and the largest accepted result is delivered intact.
#[tokio::test]
async fn jobs_result_accepted_only_when_one_delivery_page_carries_it() {
    let mut s = Suite::new();
    s.config.max_page_bytes = 4096;
    let id = s.insert(s.spec("page-bound")).await;
    let claim = s.claim().await;
    let complete = |len: usize| JobRequest::Complete {
        job: id.clone(),
        generation: claim.generation,
        state: JobState::Succeeded,
        origin: ResultOrigin::Handler,
        // Each 255 byte encodes as `255,`: the widest JSON array element.
        output: Some(vec![255; len]),
        receipt: None,
        diagnostic: None,
    };
    let mut len = s.config.max_page_bytes / 4;
    let before = serde_json::to_value(s.get(&id).await).unwrap();
    loop {
        match s.op(complete(len)).await {
            Ok(JobResponse::Done) => break,
            Err(JobError::ResultTooLarge { job, .. }) if job == id => {
                assert_eq!(serde_json::to_value(s.get(&id).await).unwrap(), before);
                len -= 1;
            }
            other => panic!("{other:?}"),
        }
    }
    assert!(len > 0 && len < s.config.max_page_bytes / 4);
    let delivery = s.deliveries(1, s.config.max_page_bytes).await.remove(0);
    assert_eq!(delivery.completion.output, Some(vec![255; len]));
    assert!(serde_json::to_vec(&vec![delivery]).unwrap().len() <= s.config.max_page_bytes);
}

/// Pending capacity counts raw payload bytes plus canonical metadata, without parsing content.
#[tokio::test]
async fn jobs_pending_raw_bytes_and_metadata_bound() {
    let mut s = Suite::new();
    let mut spec = s.spec("metadata\n鍵");
    spec.group = Some("group\n🦀".into());
    spec.owners = vec!["owner\"é\n".into()];
    spec.payload = vec![255, 0, 128, 1];
    let id = s.insert(spec.clone()).await;
    let row = s.get(&id).await;
    let bytes = job_input_bytes(&row).unwrap();
    match s.op(JobRequest::PendingUsage).await.unwrap() {
        JobResponse::Usage(usage) => assert_eq!((usage.items, usage.bytes), (1, bytes)),
        other => panic!("{other:?}"),
    }
    spec.key = "next".into();
    let mut next = row;
    next.key = spec.key.clone();
    let additional = job_input_bytes(&next).unwrap();
    s.config.max_pending_bytes = bytes + additional - 1;
    assert!(matches!(
        s.op(JobRequest::Enqueue(vec![spec.clone()])).await,
        Err(JobError::QueueFull)
    ));
    s.config.max_pending_bytes += 1;
    s.insert(spec).await;
    match s.op(JobRequest::PendingUsage).await.unwrap() {
        JobResponse::Usage(usage) => {
            assert_eq!((usage.items, usage.bytes), (2, bytes + additional))
        }
        other => panic!("{other:?}"),
    }
}

/// Opaque payload and result representations must round-trip exactly (B1).
#[tokio::test]
async fn jobs_opaque_representation_round_trip() {
    let s = Suite::new();
    let bytes = b" {\"value\": null, \"value\": -0.0} ";
    let mut spec = s.spec("opaque");
    spec.payload = bytes.to_vec();
    let id = s.insert(spec).await;
    assert_eq!(s.get(&id).await.payload.as_deref(), Some(bytes.as_slice()));
    let row = s.claim().await;
    s.complete(&row, bytes.to_vec()).await;
    assert_eq!(
        s.deliveries(1, 100_000).await[0]
            .completion
            .output
            .as_deref(),
        Some(bytes.as_slice())
    );
}

/// Floating point representations must survive storage unchanged (B2).
#[tokio::test]
async fn jobs_opaque_float_round_trip() {
    let s = Suite::new();
    let bytes = b"1.2345678901234567890123456789e-100";
    let mut spec = s.spec("float");
    spec.payload = bytes.to_vec();
    let id = s.insert(spec).await;
    assert_eq!(s.get(&id).await.payload.as_deref(), Some(bytes.as_slice()));
    let row = s.claim().await;
    s.complete(&row, bytes.to_vec()).await;
    assert_eq!(
        s.deliveries(1, 100_000).await[0]
            .completion
            .output
            .as_deref(),
        Some(bytes.as_slice())
    );
}

/// Unknown operational metadata is rejected; arbitrary business payload fields are valid.
#[tokio::test]
async fn jobs_unknown_metadata_is_refused() {
    let s = Suite::new();
    let mut spec = serde_json::to_value(s.spec("metadata-typo")).unwrap();
    spec.as_object_mut()
        .unwrap()
        .insert("recovery_untl".into(), serde_json::to_value(s.now).unwrap());
    assert!(serde_json::from_value::<JobSpec>(spec).is_err());
    let mut config = serde_json::to_value(&s.config).unwrap();
    config
        .as_object_mut()
        .unwrap()
        .insert("max_pendng_items".into(), json!(1));
    assert!(serde_json::from_value::<JobConfig>(config).is_err());
    let mut spec = s.spec("business-data");
    spec.payload = data!({"arbitrary_business_field":null});
    s.insert(spec).await;
}

/// Erasure remains the final state when cancel follows handler lease expiry.
#[tokio::test]
async fn jobs_purged_expired_claim_cancel_is_purged() {
    let mut s = Suite::new();
    let id = s.insert(s.spec("purged-cancel")).await;
    s.claim().await;
    assert_eq!(s.changed(JobRequest::PurgeOwner("owner-a".into())).await, 1);
    s.advance(31);
    assert_eq!(
        s.changed(JobRequest::Cancel(Selector::Ids(vec![id.clone()])))
            .await,
        1
    );
    let row = s.get(&id).await;
    assert_eq!(row.state, JobState::Purged);
    assert!(row.payload.is_none() && row.output.is_none());
}

/// Duplicate running identities count one job and group cancel visits unfinished rows only.
#[tokio::test]
async fn jobs_cancel_deduplicates_unfinished_group() {
    let mut s = Suite::new();
    s.config.maintenance_batch = 1;
    let final_id = s.ready("final").await;
    let running = s.insert(s.spec("running")).await;
    s.claim().await;
    assert_eq!(
        s.changed(JobRequest::Cancel(Selector::Ids(vec![
            running.clone(),
            running.clone()
        ])))
        .await,
        1
    );
    let pending = s.insert(s.spec("pending")).await;
    assert_eq!(
        s.changed(JobRequest::Cancel(Selector::Group("group".into())))
            .await,
        2
    );
    assert_eq!(s.get(&pending).await.state, JobState::Cancelled);
    assert_eq!(s.get(&final_id).await.state, JobState::Succeeded);
    assert!(s.get(&running).await.cancel_requested);
}

/// Atomic owner erasure visits live memberships and leaves unrelated rows intact.
#[tokio::test]
async fn jobs_owner_purge_live_membership() {
    let mut s = Suite::new();
    s.config.maintenance_batch = 2;
    let mut ids = Vec::new();
    for i in 0..7 {
        ids.push(s.insert(s.spec(&format!("owner-{i}"))).await);
    }
    let mut unrelated = s.spec("other-owner");
    unrelated.owners = vec!["other".into()];
    let unrelated = s.insert(unrelated).await;
    assert_eq!(s.changed(JobRequest::PurgeOwner("owner-a".into())).await, 7);
    let mut erased = Vec::new();
    for id in ids {
        let row = s.get(&id).await;
        assert_eq!(row.state, JobState::Purged);
        assert!(row.owners.is_empty());
        erased.push(row);
    }
    s.advance(1);
    let new = s.insert(s.spec("new-owner-job")).await;
    assert_eq!(s.changed(JobRequest::PurgeOwner("owner-a".into())).await, 1);
    assert_eq!(s.get(&new).await.state, JobState::Purged);
    for before in erased {
        assert_eq!(
            serde_json::to_value(s.get(&before.id).await).unwrap(),
            serde_json::to_value(before).unwrap()
        );
    }
    assert_eq!(s.get(&unrelated).await.state, JobState::Pending);
}

/// An oversized expired job is erased alone and cannot stall later maintenance.
#[tokio::test]
async fn jobs_maintenance_oversized_job_makes_progress() {
    let mut s = Suite::new();
    s.config.retention_seconds = 1;
    s.config.maintenance_bytes_per_pass = 16;
    assert_eq!(
        JobConfig::default().maintenance_bytes_per_pass,
        16 * 1024 * 1024
    );
    let mut ids = Vec::new();
    for output in [data!(null), data!("x".repeat(32)), data!(null)] {
        let mut spec = s.spec(&format!("maintenance-{}", ids.len()));
        spec.payload = b"null".to_vec();
        let id = s.insert(spec).await;
        let claim = s.claim().await;
        s.complete(&claim, output).await;
        ids.push(id);
        s.advance(1);
    }
    // The first small job cannot share a pass with the oversized second job.
    for (pass, id) in ids.iter().enumerate() {
        assert_eq!(s.changed(JobRequest::Maintain).await, 1);
        let row = s.get(id).await;
        assert!(row.result_expired && row.payload.is_none() && row.output.is_none());
        assert_eq!(row.output_bytes, 0);
        for waiting in &ids[pass + 1..] {
            assert!(!s.get(waiting).await.result_expired);
        }
    }
    assert_eq!(s.changed(JobRequest::Maintain).await, 0);
}

/// The budget counts encoded UTF-8 input and result bytes and splits a small-job backlog.
#[tokio::test]
async fn jobs_maintenance_small_jobs_share_byte_budget() {
    let mut s = Suite::new();
    s.config.retention_seconds = 1;
    // Each retained payload encodes to 4 bytes and each result to 6 bytes.
    s.config.maintenance_bytes_per_pass = 20;
    let mut ids = Vec::new();
    for i in 0..5 {
        let mut spec = s.spec(&format!("maintenance-small-{i}"));
        spec.payload = data!("é");
        let id = s.insert(spec).await;
        let claim = s.claim().await;
        s.complete(&claim, data!("🦀")).await;
        ids.push(id);
        s.advance(1);
    }
    let mut erased = 0;
    for expected in [2, 2, 1, 0] {
        assert_eq!(s.changed(JobRequest::Maintain).await, expected);
        erased += expected;
        for (i, id) in ids.iter().enumerate() {
            let row = s.get(id).await;
            assert_eq!(row.result_expired, i < erased);
            assert_eq!(row.payload.is_none(), i < erased);
            assert_eq!(row.output.is_none(), i < erased);
        }
    }
}

/// Result limits use encoded UTF-8 bytes and reject before changing the claim.
#[tokio::test]
async fn jobs_result_byte_limit_is_atomic() {
    let mut s = Suite::new();
    s.config.max_result_bytes = 4;
    let id = s.insert(s.spec("result-bound")).await;
    let claim = s.claim().await;
    let before = s.get(&id).await;
    assert!(matches!(s.op(JobRequest::Complete {
        job: id.clone(), generation: claim.generation, state: JobState::Succeeded,
        origin: ResultOrigin::Handler, output: Some(data!("🦀")), receipt: None, diagnostic: None,
    }).await, Err(JobError::ResultTooLarge { job, bytes: 6 }) if job == id));
    assert_eq!(
        serde_json::to_value(s.get(&id).await).unwrap(),
        serde_json::to_value(before).unwrap()
    );
    s.complete(&claim, data!("é")).await;
    assert_eq!(s.get(&id).await.output, Some(data!("é")));
    assert_eq!(
        s.deliveries(1, 100_000).await[0].completion.output,
        Some(data!("é"))
    );
}

/// Erased paid uncertainty stays reconcilable until terminal ledger evidence.
#[tokio::test]
async fn jobs_purged_complete_uncertain_then_resolve() {
    let s = Suite::new();
    for resolution in [
        JobResolution::KnownZeroCharge {
            receipt: "receipt".into(),
        },
        JobResolution::PaidResult {
            receipt: "receipt".into(),
            recovery_until: None,
        },
        JobResolution::Failed {
            receipt: "receipt".into(),
            diagnostic: symbiotic_core::DiagnosticCode::AttemptBudgetExhausted,
        },
    ] {
        let mut spec = s.spec(&format!("uncertain-{resolution:?}"));
        spec.execution = Execution::Model;
        let id = s.insert(spec).await;
        let claim = s.claim().await;
        assert_eq!(claim.id, id);
        assert_eq!(s.changed(JobRequest::PurgeOwner("owner-a".into())).await, 1);
        s.op(JobRequest::Complete {
            job: id.clone(),
            generation: claim.generation,
            state: JobState::Uncertain,
            origin: ResultOrigin::Paid,
            output: None,
            receipt: Some("receipt".into()),
            diagnostic: Some(symbiotic_core::DiagnosticCode::SpendReconciliationRequired),
        })
        .await
        .unwrap();
        let row = s.get(&id).await;
        assert_eq!(row.state, JobState::Uncertain);
        assert!(row.purged && row.payload.is_none() && row.output.is_none());
        assert!(row.finished_at.is_none());
        s.op(JobRequest::Resolve {
            job: id.clone(),
            generation: claim.generation,
            resolution,
        })
        .await
        .unwrap();
        let row = s.get(&id).await;
        assert_eq!(row.state, JobState::Purged);
        assert!(row.purged && row.payload.is_none() && row.output.is_none());
    }
}

/// Numeric timestamps keep chronological order across extended years and reject leap seconds atomically.
#[tokio::test]
async fn jobs_timestamp_order_and_range() {
    use chrono::TimeZone;
    let mut s = Suite::new();
    s.now = Utc.with_ymd_and_hms(9999, 12, 31, 0, 0, 0).unwrap();
    let early = s.ready("early-year").await;
    s.now = Utc.with_ymd_and_hms(10000, 1, 1, 0, 0, 0).unwrap();
    let late = s.ready("extended-year").await;
    let page = s.deliveries(2, 100_000).await;
    assert_eq!(
        page.iter()
            .map(|d| d.completion.id.clone())
            .collect::<Vec<_>>(),
        vec![early, late]
    );
    let leap = DateTime::from_timestamp(1_483_228_799, 1_500_000_000).unwrap();
    let mut invalid = s.spec("invalid-time");
    invalid.recovery_until = Some(leap);
    assert!(matches!(
        s.op(JobRequest::Enqueue(vec![
            s.spec("rolled-back-time"),
            invalid
        ]))
        .await,
        Err(JobError::InvalidRequest)
    ));
    assert!(matches!(
        s.enqueue(vec![s.spec("rolled-back-time")]).await[0],
        Enqueued::Inserted(_)
    ));
    s.now = DateTime::<Utc>::MAX_UTC;
    assert!(matches!(
        s.op(JobRequest::Enqueue(vec![s.spec("overflow-time")]))
            .await,
        Err(JobError::InvalidRequest)
    ));
}

/// Byte-bounded completion pages defer the next row without leasing it.
#[tokio::test]
async fn jobs_completion_page_byte_bound_is_exact() {
    let mut s = Suite::new();
    let first = s.ready("first").await;
    s.advance(1);
    let second = s.ready("second").await;
    let deliveries = s.deliveries(2, 100_000).await;
    let first_size = deliveries
        .iter()
        .map(|d| serde_json::to_vec(&vec![d.clone()]).unwrap().len())
        .max()
        .unwrap();
    s.advance(31);
    let page = s.deliveries(2, first_size).await;
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].completion.id, first);
    assert!(serde_json::to_vec(&page).unwrap().len() <= first_size);
    assert_eq!(s.get(&second).await.delivery_generation, 1);
    s.ack(page[0].token.clone(), Disposition::Accepted).await;
    assert_eq!(s.deliveries(1, 100_000).await[0].completion.id, second);
}

#[tokio::test(flavor = "current_thread")]
async fn regression_runner_sqlite_lock_leaves_other_database_live() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = Suite::new();
    a.backend =
        Arc::new(symbiotic_queue_sqlite::SqliteQueue::open(dir.path().join("a.sqlite")).unwrap());
    a.now = Utc::now();
    a.config.claim_lease_seconds = 1;
    let mut spec = a.spec("independent");
    spec.limits.max_attempts = 1;
    let id = a.insert(spec).await;
    let mut b = Suite::new();
    let path_b = dir.path().join("b.sqlite");
    b.backend = Arc::new(symbiotic_queue_sqlite::SqliteQueue::open(&path_b).unwrap());
    let runner_b = b.runner(1, |_, bytes| async { Ok(bytes) }).await;
    let (entered, entry) = tokio::sync::oneshot::channel();
    let entered = Arc::new(std::sync::Mutex::new(Some(entered)));
    let runner_a = a
        .runner(1, move |_, bytes| {
            entered.lock().unwrap().take().unwrap().send(()).unwrap();
            async move {
                tokio::time::sleep(std::time::Duration::from_millis(1400)).await;
                Ok(bytes)
            }
        })
        .await;
    entry.await.unwrap();
    let initial = a.get(&id).await.lease_until.unwrap();
    let (locked, lock_ready) = std::sync::mpsc::channel();
    let lock = std::thread::spawn(move || {
        let mut conn = rusqlite::Connection::open(path_b).unwrap();
        let tx = conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .unwrap();
        locked.send(()).unwrap();
        std::thread::sleep(std::time::Duration::from_secs(3));
        tx.commit().unwrap();
    });
    lock_ready.recv().unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    let renewed = a.get(&id).await.lease_until.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;
    let row = a.get(&id).await;
    let stopped_a = runner_a.shutdown().await;
    runner_b.shutdown().await.unwrap();
    lock.join().unwrap();
    assert!(
        renewed > initial,
        "database B stalled database A's heartbeat"
    );
    assert_eq!(
        row.state,
        JobState::Succeeded,
        "database B stalled database A's completion"
    );
    assert!(row.finished_at.unwrap() < initial + Duration::seconds(1));
    stopped_a.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn regression_sqlite_completion_lock_wait_cannot_revive_expired_claim() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("expiry.sqlite");
    let mut s = Suite::new();
    s.backend = Arc::new(symbiotic_queue_sqlite::SqliteQueue::open(&path).unwrap());
    s.now = Utc::now();
    s.config.claim_lease_seconds = 1;
    let id = s.insert(s.spec("expired-completion")).await;
    let claim = s.claim().await;
    let (locked, lock_ready) = std::sync::mpsc::channel();
    let release = std::thread::spawn(move || {
        let mut conn = rusqlite::Connection::open(path).unwrap();
        let tx = conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .unwrap();
        locked.send(()).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(1200));
        tx.commit().unwrap();
    });
    lock_ready.recv().unwrap();
    let started = Utc::now();
    assert!(started < claim.lease_until.unwrap());
    let result = s
        .backend
        .jobs(
            &s.scope,
            &s.config,
            Arc::new(Utc::now),
            JobRequest::Complete {
                job: id.clone(),
                generation: claim.generation,
                state: JobState::Succeeded,
                origin: ResultOrigin::Handler,
                output: Some(b"late".to_vec()),
                receipt: None,
                diagnostic: None,
            },
        )
        .await;
    release.join().unwrap();
    assert!(Utc::now() > claim.lease_until.unwrap());
    assert!(
        matches!(result, Err(JobError::StaleClaim)),
        "late completion accepted: {result:?}"
    );
    let row = s.get(&id).await;
    assert_eq!(row.state, JobState::Running);
    assert!(row.output.is_none());
}

#[tokio::test]
async fn signed_zero_charge_recovery_requires_successor_authority() {
    let suite = Suite::new();
    let mut spec = suite.spec("signed");
    spec.execution = Execution::Model;
    spec.admission = Some(b"opaque signed authority".to_vec());
    let id = suite.insert(spec).await;
    let JobResponse::Job(Some(claim)) = suite
        .op(JobRequest::ClaimPaid {
            job: id.clone(),
            receipt: "pre-dispatch".into(),
        })
        .await
        .unwrap()
    else {
        panic!("claim")
    };
    suite
        .op(JobRequest::Resolve {
            job: id.clone(),
            generation: claim.generation,
            resolution: JobResolution::KnownZeroCharge {
                receipt: "pre-dispatch".into(),
            },
        })
        .await
        .unwrap();
    let row = suite.get(&id).await;
    assert_eq!(row.state, JobState::AwaitingAdmission);
    assert_eq!(row.generation, 1);
    assert!(row.payload.is_some());
    assert!(matches!(
        suite
            .op(JobRequest::ClaimPaid {
                job: id.clone(),
                receipt: "blind-retry".into()
            })
            .await
            .unwrap(),
        JobResponse::Job(None)
    ));
    // Replaying the old admission must not make it eligible again.
    suite
        .op(JobRequest::Admit {
            job: id.clone(),
            admission: row.admission.unwrap(),
        })
        .await
        .unwrap();
    assert_eq!(suite.get(&id).await.state, JobState::AwaitingAdmission);
    suite
        .op(JobRequest::Admit {
            job: id.clone(),
            admission: b"successor".to_vec(),
        })
        .await
        .unwrap();
    assert_eq!(suite.get(&id).await.state, JobState::Pending);
}

#[tokio::test]
async fn admission_bytes_share_pending_bound_and_failed_updates_roll_back() {
    let mut suite = Suite::new();
    let mut spec = suite.spec("signed-bounds");
    spec.execution = Execution::Model;
    spec.admission = Some(vec![1; 64]);
    let id = suite.insert(spec).await;
    let JobResponse::Usage(usage) = suite.op(JobRequest::PendingUsage).await.unwrap() else {
        panic!("usage")
    };
    suite.config.max_pending_bytes = usage.bytes;
    suite
        .op(JobRequest::AwaitAdmission(id.clone()))
        .await
        .unwrap();
    assert_eq!(
        suite
            .op(JobRequest::Admit {
                job: id.clone(),
                admission: vec![2; 65]
            })
            .await
            .unwrap_err(),
        JobError::QueueFull
    );
    let row = suite.get(&id).await;
    assert_eq!(row.state, JobState::AwaitingAdmission);
    assert_eq!(row.admission, Some(vec![1; 64]));
    assert!(matches!(
        suite
            .op(JobRequest::Cancel(Selector::Ids(vec![id.clone()])))
            .await
            .unwrap(),
        JobResponse::Changed(1)
    ));
    let row = suite.get(&id).await;
    assert_eq!(row.state, JobState::Cancelled);
    assert!(row.admission.is_none() && row.payload.is_none());
}

#[tokio::test]
async fn admission_bytes_share_maintenance_budget() {
    let mut suite = Suite::new();
    suite.config.maintenance_bytes_per_pass = 80;
    suite.config.retention_seconds = 1;
    let mut ids = Vec::new();
    for key in ["signed-maintenance-a", "signed-maintenance-b"] {
        let mut spec = suite.spec(key);
        spec.execution = Execution::Model;
        spec.payload = vec![0; 4];
        spec.admission = Some(vec![1; 64]);
        let id = suite.insert(spec).await;
        let JobResponse::Job(Some(claim)) = suite
            .op(JobRequest::ClaimPaid {
                job: id.clone(),
                receipt: key.into(),
            })
            .await
            .unwrap()
        else {
            panic!("claim")
        };
        suite
            .op(JobRequest::Resolve {
                job: id.clone(),
                generation: claim.generation,
                resolution: JobResolution::Failed {
                    receipt: key.into(),
                    diagnostic: symbiotic_core::DiagnosticCode::AuthenticationRejected,
                },
            })
            .await
            .unwrap();
        ids.push(id);
        suite.advance(1);
    }
    assert_eq!(suite.changed(JobRequest::Maintain).await, 1);
    let first = suite.get(&ids[0]).await;
    assert!(first.result_expired && first.admission.is_none());
    assert!(!suite.get(&ids[1]).await.result_expired);
    assert_eq!(suite.changed(JobRequest::Maintain).await, 1);
    let second = suite.get(&ids[1]).await;
    assert!(second.result_expired && second.admission.is_none());
}
