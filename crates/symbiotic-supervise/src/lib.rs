//! Unix child lifecycle only. Queue, engine and session recovery belong to callers.
#![cfg(unix)]

mod unix;

use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    process::{Command, ExitStatus},
    sync::mpsc::{self, Receiver, Sender},
    thread::JoinHandle,
    time::{Duration, Instant},
};

pub use unix::{watch_parent, watch_parent_with_diagnostic_wait};

/// Explicit versioned policy; values are supplied by the app, with no hidden defaults.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Policy {
    /// Must be 1.
    pub version: u16,
    /// Allowed restarts within the sliding crash window (0 disables restart).
    pub max_restarts: u32,
    /// Sliding crash window in milliseconds.
    pub crash_window_ms: u64,
    /// Fixed delay before each restart, in milliseconds.
    pub backoff_ms: u64,
    /// Time allowed after SIGTERM before SIGKILL, in milliseconds.
    pub stop_grace_ms: u64,
    /// Death-detection interval in milliseconds.
    pub poll_ms: u64,
}

/// Visible lifecycle failures; none authorizes replay of product work.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Unsupported version or invalid timing values.
    #[error("invalid supervision policy")]
    InvalidPolicy,
    /// OS operation failed.
    #[error("supervision OS operation failed: {0}")]
    Io(#[from] std::io::Error),
    /// Repeated exits exhausted the restart allowance.
    #[error(
        "restart limit reached ({restarts} restarts in {window_ms}ms); last exit: {last_status}"
    )]
    CrashLimit {
        /// Restarts already used in the current window.
        restarts: u32,
        /// Configured window.
        window_ms: u64,
        /// Last child's exit status.
        last_status: ExitStatus,
    },
    /// An internal thread or channel stopped unexpectedly.
    #[error("supervision thread unavailable")]
    Unavailable,
}

/// Events describe processes only; Started is not application readiness.
#[derive(Debug)]
pub enum Event {
    /// A new child was spawned. Check its application protocol separately.
    Started(u32),
    /// An unexpected exit was detected and reaped before any restart.
    Exited(ExitStatus),
}

/// A managed child. Call `stop` to observe shutdown errors; drop also stops and reaps.
/// Commands must be freshly built by the factory on each restart. Entrypoints call
/// [`watch_parent`]; unmodified tools need an app-owned wrapper that kills their group.
pub struct Supervisor {
    stop: Sender<()>,
    events: Receiver<Result<Event, Error>>,
    thread: Option<JoinHandle<Result<(), Error>>>,
}

impl Supervisor {
    /// Start supervision, spawning all children on one process-lifetime thread.
    pub fn start(
        mut command: impl FnMut() -> Command + Send + 'static,
        policy: Policy,
    ) -> Result<Self, Error> {
        if policy.version != 1
            || policy.crash_window_ms == 0
            || policy.backoff_ms == 0
            || policy.poll_ms == 0
        {
            return Err(Error::InvalidPolicy);
        }
        let (stop, stopped) = mpsc::channel();
        let (events_tx, events) = mpsc::channel();
        let thread = std::thread::Builder::new()
            .name("foundation-lifecycle".into())
            .spawn(move || {
                let result = lifecycle(&mut command, &policy, &stopped, &events_tx);
                if let Err(error) = result {
                    // Report the original failure after lifecycle cleanup.
                    events_tx.send(Err(error)).map_err(|_| Error::Unavailable)?;
                }
                Ok(())
            })?;
        Ok(Self {
            stop,
            events,
            thread: Some(thread),
        })
    }

    /// Wait for a spawn, exit or typed failure. Consume events throughout supervision.
    pub fn next_event(&self) -> Result<Event, Error> {
        self.events.recv().map_err(|_| Error::Unavailable)?
    }

    /// SIGTERM, wait the configured grace period, then SIGKILL if needed, and reap.
    pub fn stop(mut self) -> Result<(), Error> {
        self.finish()
    }

    fn finish(&mut self) -> Result<(), Error> {
        if let Some(thread) = self.thread.take() {
            // A completed worker may already have closed its receiver; joining still
            // returns its shutdown result.
            let _ = self.stop.send(());
            thread.join().map_err(|_| Error::Unavailable)??;
            for event in self.events.try_iter() {
                event?;
            }
        }
        Ok(())
    }
}

impl Drop for Supervisor {
    fn drop(&mut self) {
        // Drop cannot return failures; explicit stop is the observable shutdown API.
        if let Err(error) = self.finish() {
            unix::diagnostic(format_args!("{error}"));
        }
    }
}

fn stopping(stop: &Receiver<()>, delay: Duration) -> bool {
    !matches!(
        stop.recv_timeout(delay),
        Err(mpsc::RecvTimeoutError::Timeout)
    )
}

fn lifecycle(
    command: &mut impl FnMut() -> Command,
    policy: &Policy,
    stop: &Receiver<()>,
    events: &Sender<Result<Event, Error>>,
) -> Result<(), Error> {
    let mut crashes = VecDeque::new();
    loop {
        if stopping(stop, Duration::ZERO) {
            return Ok(());
        }
        let mut child = unix::spawn(command())?;
        if events.send(Ok(Event::Started(child.id()))).is_err() {
            child.stop(policy)?;
            return Err(Error::Unavailable);
        }
        let status = loop {
            if stopping(stop, Duration::from_millis(policy.poll_ms)) {
                return child.stop(policy);
            }
            if let Some(status) = child.try_wait()? {
                break status;
            }
        };
        events
            .send(Ok(Event::Exited(status)))
            .map_err(|_| Error::Unavailable)?;
        let now = Instant::now();
        while crashes.front().is_some_and(|time| {
            now.duration_since(*time) >= Duration::from_millis(policy.crash_window_ms)
        }) {
            crashes.pop_front();
        }
        if crashes.len() >= policy.max_restarts as usize {
            return Err(Error::CrashLimit {
                restarts: policy.max_restarts,
                window_ms: policy.crash_window_ms,
                last_status: status,
            });
        }
        crashes.push_back(now);
        if stopping(stop, Duration::from_millis(policy.backoff_ms)) {
            return Ok(());
        }
    }
}
