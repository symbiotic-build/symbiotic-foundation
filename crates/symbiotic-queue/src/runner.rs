//! Generic handler workers. SQLite owns recovery, cancellation intent and claim fences.

use crate::{QueueBackend, jobs::*};
use async_trait::async_trait;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
use symbiotic_core::DiagnosticCode;
use thiserror::Error;
use tokio::{
    sync::watch,
    task::{JoinHandle, JoinSet},
};

/// Versioned worker policy. Lease length comes solely from [`JobConfig`];
/// its existing 30-second claim default remains provisional for the runner.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunnerConfig {
    /// Supported configuration format (1).
    pub version: u32,
    /// Concurrent handlers (4; PROVISIONAL).
    pub worker_count: usize,
    /// Store polling period for new work (100 ms; PROVISIONAL).
    pub poll_interval_ms: u64,
    /// Heartbeat/cancel interval in milliseconds; None derives lease/3 (PROVISIONAL).
    /// The effective interval must be positive and at most half the claim lease.
    pub heartbeat_interval_ms: Option<u64>,
    /// Independent bounded maintenance period (60 seconds; PROVISIONAL).
    pub maintenance_interval_ms: u64,
}

impl Default for RunnerConfig {
    fn default() -> Self {
        Self {
            version: 1,
            worker_count: 4,
            poll_interval_ms: 100,
            heartbeat_interval_ms: None,
            maintenance_interval_ms: 60_000,
        }
    }
}

/// Cooperative cancellation; a handler decides how to finish work already started.
/// Store cancellation latency depends on heartbeat scheduling and successful store-call
/// time, including storage busy-timeout and connection-mutex waits; it has no numeric bound.
#[derive(Clone, Debug)]
pub struct CancelToken(watch::Receiver<bool>);

impl CancelToken {
    /// Whether cancellation was requested or the execution owner is gone.
    pub fn is_cancelled(&self) -> bool {
        *self.0.borrow() || self.0.has_changed().is_err()
    }

    /// Wait for cancellation without losing a signal sent before this call.
    pub async fn cancelled(&self) {
        let mut rx = self.0.clone();
        while !*rx.borrow_and_update() {
            if rx.changed().await.is_err() {
                return;
            }
        }
    }
}

/// Claim identity and cooperative cancellation supplied to a handler.
#[derive(Clone, Debug)]
pub struct JobContext {
    /// Scoped durable identity.
    pub id: JobId,
    /// Stable caller idempotency key within `id.scope`, preserved across claims.
    pub key: String,
    /// One-based claim generation, used only to fence queue writes.
    pub attempt: u64,
    /// Cancellation requested by the consumer or input erasure.
    pub cancel: CancelToken,
}

/// Static handler failure, safe to retain without input or error text.
#[derive(Clone, Copy, Debug, Error)]
#[error("{code}")]
pub struct JobFailure {
    /// Durable failure diagnostic.
    pub code: DiagnosticCode,
}

/// Product work with opaque, exactly round-tripping bytes and no spend receipt.
/// Handler failures are final; cancellation never causes a retry.
/// After a crash, an expired lease can be claimed again only within the job's
/// frozen attempt ceiling. A crash between claim and handler entry consumes an
/// attempt; with `max_attempts = 1`, recovery ends Refused without running the handler.
/// Handlers with side effects must deduplicate by the stable scoped job key,
/// which stays the same across claims. The claim generation ([`JobContext::attempt`])
/// is only a fencing token for queue writes.
#[async_trait]
pub trait JobHandler: Send + Sync {
    /// Execute one claim. Work already sent must finish under its own timeout;
    /// cancellation alone never drops this future.
    async fn run(&self, ctx: &JobContext, payload: &[u8]) -> Result<Vec<u8>, JobFailure>;
}

/// Visible runner failures; worker errors are collected while running handlers drain.
#[derive(Debug, Error)]
pub enum RunnerError {
    /// Store/configuration failure.
    #[error(transparent)]
    Store(#[from] JobError),
    /// One heartbeat failure cause and its occurrence count for a claim.
    #[error("job heartbeat failed {count} times; cause: {cause}")]
    Monitoring {
        /// Store/configuration cause.
        cause: JobError,
        /// Failed heartbeats with this cause, including the first.
        count: usize,
    },
    /// Handler panicked; the runner attempts a fenced final failure without retry.
    #[error("job handler panicked: {0:?}")]
    HandlerPanicked(JobId),
    /// A worker task failed outside the handler boundary.
    #[error("job worker task failed")]
    WorkerTask,
    /// All failures from a stopped runner, including failures while draining.
    #[error("job runner failed: {0:?}")]
    Workers(Vec<RunnerError>),
}

struct Shared {
    backend: Arc<dyn QueueBackend>,
    scope: JobScope,
    jobs: JobConfig,
    kind: String,
    handler: Arc<dyn JobHandler>,
    poll: Duration,
    heartbeat: Duration,
}

impl Shared {
    fn observe_heartbeat(
        response: Result<JobResponse, JobError>,
        cancel: &watch::Sender<bool>,
        errors: &mut Vec<RunnerError>,
    ) -> bool {
        let error = match response {
            Ok(JobResponse::Heartbeat(requested)) => {
                if requested {
                    cancel.send_replace(true);
                }
                return false;
            }
            Ok(_) => JobError::InvalidRequest,
            Err(error) => error,
        };
        let fenced = matches!(error, JobError::StaleClaim);
        match errors
            .iter_mut()
            .find(|entry| matches!(entry, RunnerError::Monitoring { cause, .. } if cause == &error))
        {
            Some(RunnerError::Monitoring { count, .. }) => *count += 1,
            _ => errors.push(RunnerError::Monitoring {
                cause: error,
                count: 1,
            }),
        }
        if fenced {
            cancel.send_replace(true);
        }
        fenced
    }

    async fn op(&self, request: JobRequest) -> Result<JobResponse, JobError> {
        self.backend
            .jobs(&self.scope, &self.jobs, Arc::new(Utc::now), request)
            .await
    }

    async fn execute(self: &Arc<Self>, mut claim: JobRecord) -> Result<(), RunnerError> {
        if claim.execution != Execution::Handler {
            return Err(JobError::InvalidRequest.into());
        }
        let payload = claim.payload.take().ok_or(JobError::InvalidRequest)?;
        let (cancel, rx) = watch::channel(false);
        let shared = self.clone();
        let ctx = JobContext {
            id: claim.id.clone(),
            key: claim.key.clone(),
            attempt: claim.generation,
            cancel: CancelToken(rx),
        };
        // JoinSet supplies the panic boundary and aborts the task if its owning
        // worker is itself aborted. Ordinary cancel/shutdown always drains it.
        let mut task = JoinSet::new();
        task.spawn(async move { shared.handler.run(&ctx, &payload).await });
        let mut renew = tokio::time::interval(self.heartbeat);
        let mut errors = Vec::new();
        let mut heartbeat = None;
        let outcome = loop {
            let response = tokio::select! {
                result = task.join_next() => break result,
                _ = renew.tick() => {
                    let mut pending = Box::pin(self.op(JobRequest::Heartbeat {
                        job: claim.id.clone(), generation: claim.generation,
                    }));
                    tokio::select! {
                        response = &mut pending => response,
                        result = task.join_next() => {
                            heartbeat = Some(pending);
                            break result;
                        }
                    }
                },
            };
            if Self::observe_heartbeat(response, &cancel, &mut errors) {
                break task.join_next().await;
            }
        };
        // Stop scheduling heartbeats when the handler finishes, then drain any
        // outstanding call before completion so the two store operations never race.
        if let Some(heartbeat) = heartbeat {
            Self::observe_heartbeat(heartbeat.await, &cancel, &mut errors);
        }
        let (state, output, diagnostic) = match outcome {
            Some(Ok(Ok(output))) if output.len() > self.jobs.max_result_bytes => {
                errors.push(
                    JobError::ResultTooLarge {
                        job: claim.id.clone(),
                        bytes: output.len(),
                    }
                    .into(),
                );
                (JobState::Failed, None, Some(DiagnosticCode::QueueFailure))
            }
            Some(Ok(Ok(output))) => (JobState::Succeeded, Some(output), None),
            Some(Ok(Err(failure))) => (JobState::Failed, None, Some(failure.code)),
            Some(Err(_)) => {
                errors.push(RunnerError::HandlerPanicked(claim.id.clone()));
                (JobState::Failed, None, Some(DiagnosticCode::QueueFailure))
            }
            None => {
                errors.push(RunnerError::WorkerTask);
                return Err(RunnerError::Workers(errors));
            }
        };
        // Finished work, including a panic, always reaches the store's claim fence.
        // A heartbeat failure is recorded above but never prevents this attempt.
        let completion = self
            .op(JobRequest::Complete {
                job: claim.id.clone(),
                generation: claim.generation,
                state,
                origin: ResultOrigin::Handler,
                output,
                receipt: None,
                diagnostic,
            })
            .await;
        match completion {
            Ok(JobResponse::Done) => {}
            // The store already ended the job; report why its result was not kept.
            Ok(JobResponse::ResultRefused { bytes }) => errors.push(
                JobError::ResultTooLarge {
                    job: claim.id,
                    bytes,
                }
                .into(),
            ),
            Ok(_) => errors.push(JobError::InvalidRequest.into()),
            Err(error) => errors.push(error.into()),
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(RunnerError::Workers(errors))
        }
    }

    async fn maintain(
        self: Arc<Self>,
        mut stop: watch::Receiver<bool>,
        interval: Duration,
    ) -> Result<(), RunnerError> {
        let mut timer = tokio::time::interval(interval);
        loop {
            if *stop.borrow() {
                return Ok(());
            }
            tokio::select! {
                biased;
                _ = stop.changed() => continue,
                _ = timer.tick() => {
                    if !matches!(self.op(JobRequest::Maintain).await?, JobResponse::Changed(_)) {
                        return Err(JobError::InvalidRequest.into());
                    }
                }
            }
        }
    }

    async fn worker(self: Arc<Self>, mut stop: watch::Receiver<bool>) -> Result<(), RunnerError> {
        loop {
            if *stop.borrow() {
                return Ok(());
            }
            match self
                .op(JobRequest::Claim {
                    kinds: vec![self.kind.clone()],
                    slots_available: 1,
                })
                .await?
            {
                JobResponse::Job(Some(row)) => self.execute(*row).await?,
                JobResponse::Job(None) => {
                    tokio::select! {
                        _ = tokio::time::sleep(self.poll) => {},
                        _ = stop.changed() => {},
                    }
                }
                _ => return Err(JobError::InvalidRequest.into()),
            }
        }
    }
}

/// Owns a fixed number of workers for one generic handler kind in one scope.
/// Dropping it stops new claims; running handlers finish. Use [`Self::shutdown`]
/// or [`Self::wait`] to observe errors. Model kinds belong to PR 3's ledger owner.
pub struct JobRunner {
    stop: watch::Sender<bool>,
    task: Option<JoinHandle<Result<(), RunnerError>>>,
}

impl JobRunner {
    /// Validate policy/store availability, then start workers without consumer resubmission.
    /// The kind must be reserved for [`Execution::Handler`] jobs.
    pub async fn start(
        backend: Arc<dyn QueueBackend>,
        scope: JobScope,
        jobs: JobConfig,
        config: RunnerConfig,
        kind: String,
        handler: Arc<dyn JobHandler>,
    ) -> Result<Self, RunnerError> {
        let lease = Duration::from_secs(jobs.claim_lease_seconds);
        let heartbeat = config
            .heartbeat_interval_ms
            .map(Duration::from_millis)
            .unwrap_or(lease / 3);
        if config.version != 1
            || config.worker_count == 0
            || config.poll_interval_ms == 0
            || heartbeat.is_zero()
            || heartbeat > lease / 2
            || config.maintenance_interval_ms == 0
            || kind.is_empty()
        {
            return Err(JobError::InvalidRequest.into());
        }
        let shared = Arc::new(Shared {
            backend,
            scope,
            heartbeat,
            jobs,
            kind,
            handler,
            poll: Duration::from_millis(config.poll_interval_ms),
        });
        if !matches!(
            shared.op(JobRequest::PendingUsage).await?,
            JobResponse::Usage(_)
        ) {
            return Err(JobError::InvalidRequest.into());
        }
        let workers = shared.clone();
        Ok(Self::start_owned(
            config.worker_count,
            move |rx| shared.clone().worker(rx),
            move |rx| {
                let shared = workers.clone();
                async move {
                    shared
                        .maintain(rx, Duration::from_millis(config.maintenance_interval_ms))
                        .await
                }
            },
        ))
    }

    /// Compose the same worker supervision and draining lifecycle with a
    /// Foundation execution owner (model admission and ledger transactions).
    #[doc(hidden)]
    pub fn start_owned<W, M, WF, MF>(count: usize, worker: W, maintenance: M) -> Self
    where
        W: Fn(watch::Receiver<bool>) -> WF + Send + 'static,
        M: FnOnce(watch::Receiver<bool>) -> MF + Send + 'static,
        WF: std::future::Future<Output = Result<(), RunnerError>> + Send + 'static,
        MF: std::future::Future<Output = Result<(), RunnerError>> + Send + 'static,
    {
        let (stop, rx) = watch::channel(false);
        let stopped = stop.clone();
        let task = tokio::spawn(async move {
            let mut tasks = JoinSet::new();
            tasks.spawn(maintenance(rx.clone()));
            for _ in 0..count {
                tasks.spawn(worker(rx.clone()));
            }
            let mut errors = Vec::new();
            while let Some(result) = tasks.join_next().await {
                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => errors.push(error),
                    Err(_) => errors.push(RunnerError::WorkerTask),
                }
                if !errors.is_empty() {
                    stopped.send_replace(true);
                }
            }
            if errors.is_empty() {
                Ok(())
            } else {
                Err(RunnerError::Workers(errors))
            }
        });
        Self {
            stop,
            task: Some(task),
        }
    }

    /// Whether supervision has finished; await `wait` to collect its result.
    pub fn is_finished(&self) -> bool {
        self.task.as_ref().is_none_or(JoinHandle::is_finished)
    }

    /// Stop claiming and await running handlers and all worker errors.
    pub async fn shutdown(self) -> Result<(), RunnerError> {
        self.stop.send_replace(true);
        self.wait().await
    }

    /// Await worker failure. Use [`Self::shutdown`] to stop normally.
    pub async fn wait(mut self) -> Result<(), RunnerError> {
        self.task
            .take()
            .ok_or(RunnerError::WorkerTask)?
            .await
            .map_err(|_| RunnerError::WorkerTask)?
    }
}

impl Drop for JobRunner {
    fn drop(&mut self) {
        self.stop.send_replace(true);
    }
}
