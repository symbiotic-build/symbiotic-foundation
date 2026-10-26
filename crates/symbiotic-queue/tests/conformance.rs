//! The in-process backend passes the shared `QueueBackend` conformance checks.
#![cfg(feature = "conformance")]

use std::sync::Arc;
use symbiotic_queue::{MemoryQueue, QueueBackend};

#[path = "../../test-support/clock.rs"]
mod test_clock;

symbiotic_queue::queue_backend_conformance!(
    || Arc::new(MemoryQueue::new().with_clock(test_clock::clock())) as Arc<dyn QueueBackend>,
    pause_time = true
);
