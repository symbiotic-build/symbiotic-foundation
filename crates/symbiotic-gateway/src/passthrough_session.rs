//! Account retry and concurrency state, route session history and login-wide new-session hold.
use super::*;
use std::collections::VecDeque;

/// Headers whose tuple identifies one provider session. Absent means no session gate.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NewSessionConfig {
    /// Lowercase caller header names; missing members retain their tuple position.
    pub session_headers: Vec<String>,
}
#[derive(Default)]
pub(super) struct SeenSessions {
    entries: HashMap<Vec<Option<Vec<u8>>>, Instant>,
    order: VecDeque<(Vec<Option<Vec<u8>>>, Instant)>,
}
impl SeenSessions {
    pub(super) fn contains(&mut self, key: &Vec<Option<Vec<u8>>>) -> bool {
        while self
            .order
            .front()
            .is_some_and(|(_, t)| t.elapsed() >= Duration::from_secs(86400))
        {
            if let Some((k, t)) = self.order.pop_front()
                && self.entries.get(&k) == Some(&t)
            {
                self.entries.remove(&k);
            }
        }
        self.entries.contains_key(key)
    }
    pub(super) fn insert(&mut self, key: Vec<Option<Vec<u8>>>) {
        if self.contains(&key) {
            return;
        }
        if self.entries.len() >= 100_000
            && let Some((k, _)) = self.order.pop_front()
        {
            self.entries.remove(&k);
        }
        let now = Instant::now();
        self.entries.insert(key.clone(), now);
        self.order.push_back((key, now));
    }
}
/// Retry counts and concurrency share account scope; session admission shares the login.
pub(super) struct AccountRetries {
    pub failures: Arc<std::sync::atomic::AtomicU32>,
    pub concurrency: Arc<AccountConcurrency>,
    pub sessions: Arc<SessionAdmission>,
}
pub(super) struct SessionAdmission {
    pub state: Mutex<SessionHold>,
    pub probe: Arc<Semaphore>,
    pub changes: tokio::sync::watch::Sender<()>,
}
impl Default for SessionAdmission {
    fn default() -> Self {
        Self {
            state: Default::default(),
            probe: Arc::new(Semaphore::new(1)),
            changes: tokio::sync::watch::channel(()).0,
        }
    }
}
#[derive(Default)]
pub(super) struct SessionHold {
    pub until: Option<DateTime<Utc>>,
    pub probing: bool,
}
impl Route {
    pub(super) fn session_key(&self, request: &Request) -> Option<Vec<Option<Vec<u8>>>> {
        let config = self.config.new_session.as_ref()?;
        let key: Vec<_> = config
            .session_headers
            .iter()
            .map(|n| {
                request
                    .headers
                    .iter()
                    .find(|(k, _)| k == n)
                    .map(|(_, v)| v.clone())
            })
            .collect();
        key.iter().any(Option::is_some).then_some(key)
    }
    pub(super) fn is_new(&self, key: &Option<Vec<Option<Vec<u8>>>>) -> Result<bool> {
        match key {
            Some(key) => Ok(!self
                .seen
                .lock()
                .map_err(|_| GatewayError::StateUnavailable)?
                .contains(key)),
            None => Ok(false),
        }
    }
}

/// An account's adaptive cap. A FIFO semaphore keeps arrival order; existing holders
/// survive decreases, and permits they return pay off the part not yet removed.
pub(super) struct AccountConcurrency {
    ceiling: usize,
    permits: Arc<tokio::sync::Semaphore>,
    state: Mutex<ConcurrencyState>,
}
struct ConcurrencyState {
    limit: usize,
    // Permits still to remove after a decrease found them held.
    debt: usize,
    successes: u8,
}
pub(super) struct ConcurrencyPermit {
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
    owner: Arc<AccountConcurrency>,
}
impl AccountConcurrency {
    pub(super) fn new(ceiling: usize) -> Self {
        Self {
            ceiling,
            permits: Arc::new(tokio::sync::Semaphore::new(ceiling)),
            state: Mutex::new(ConcurrencyState {
                limit: ceiling,
                debt: 0,
                successes: 0,
            }),
        }
    }
    pub(super) async fn acquire(self: &Arc<Self>) -> Result<ConcurrencyPermit> {
        let permit = self
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| GatewayError::StateUnavailable)?;
        Ok(ConcurrencyPermit {
            permit: Some(permit),
            owner: self.clone(),
        })
    }
    pub(super) fn outcome(&self, throttle: bool, route: &PassthroughRoute) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| GatewayError::StateUnavailable)?;
        let previous = state.limit;
        if throttle {
            state.limit = (state.limit / 2).max(1);
            state.successes = 0;
            let remove = previous - state.limit;
            let removed = self.permits.forget_permits(remove);
            state.debt += remove - removed;
        } else {
            state.successes += 1;
            if state.successes == 10 {
                state.successes = 0;
                if state.limit < self.ceiling {
                    state.limit += 1;
                    if state.debt > 0 {
                        state.debt -= 1;
                    } else {
                        self.permits.add_permits(1);
                    }
                }
            }
        }
        if state.limit != previous {
            tracing::info!(
                route = route.name,
                account = route.account,
                effective_limit = state.limit,
                "account concurrency changed"
            );
        }
        Ok(())
    }
}
impl Drop for ConcurrencyPermit {
    fn drop(&mut self) {
        let mut state = self
            .owner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(permit) = self.permit.take()
            && state.debt > 0
        {
            state.debt -= 1;
            permit.forget();
        }
    }
}
