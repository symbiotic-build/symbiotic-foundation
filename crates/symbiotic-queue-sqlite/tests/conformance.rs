//! The SQLite backend passes the shared `QueueBackend` conformance checks.

use std::sync::Arc;
use symbiotic_queue::QueueBackend;
use symbiotic_queue_sqlite::SqliteQueue;

#[path = "../../test-support/clock.rs"]
mod test_clock;

symbiotic_queue::queue_backend_conformance!(
    || Arc::new(
        SqliteQueue::in_memory()
            .expect("open in-memory SQLite queue")
            .with_clock(test_clock::clock())
    ) as Arc<dyn QueueBackend>,
    pause_time = true
);
