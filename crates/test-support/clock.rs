//! UTC deadlines and Tokio timers advance together in tests that pause time.
use std::sync::Arc;

/// Return a UTC clock that follows this Tokio runtime's monotonic clock.
pub fn clock() -> Arc<dyn Fn() -> chrono::DateTime<chrono::Utc> + Send + Sync> {
    let runtime = tokio::runtime::Handle::current();
    let utc = chrono::Utc::now();
    let started = tokio::time::Instant::now();
    Arc::new(move || {
        // Maintenance and SQLite can sample this clock from their own threads.
        let _entered = runtime.enter();
        utc + chrono::Duration::from_std(started.elapsed()).unwrap()
    })
}
