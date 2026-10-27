//! Sanitized health publication. These files are a replaceable view, never account state.
use super::*;
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    path::Path,
};

#[derive(Clone, Serialize)]
pub(super) struct Waiting {
    pub request_id: String,
    pub reason: &'static str,
    pub since: i64,
}
#[derive(Default)]
pub(super) struct RouteActivity {
    pub retried_events: HashMap<String, u64>,
    pub waiting: HashMap<uuid::Uuid, Waiting>,
    pub rate_limits: HashMap<String, String>,
    pub rate_limits_seen_at: Option<i64>,
}
/// Removes a waiting entry on completion, error and cancellation alike.
pub(super) struct WaitEntry {
    health: Arc<Mutex<RouteActivity>>,
    changed: Arc<Notify>,
    id: uuid::Uuid,
}
impl Drop for WaitEntry {
    fn drop(&mut self) {
        self.health
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .waiting
            .remove(&self.id);
        self.changed.notify_one();
    }
}
impl Passthrough {
    pub(super) fn waiting(
        &self,
        route: &Route,
        request_id: Option<&str>,
        reason: &'static str,
    ) -> Result<Option<WaitEntry>> {
        let Some(request_id) = request_id else {
            return Ok(None);
        };
        let id = uuid::Uuid::new_v4();
        route
            .health
            .lock()
            .map_err(|_| EgressError::StateUnavailable)?
            .waiting
            .insert(
                id,
                Waiting {
                    request_id: request_id.to_owned(),
                    reason,
                    since: Utc::now().timestamp(),
                },
            );
        self.changed.notify_one();
        Ok(Some(WaitEntry {
            health: route.health.clone(),
            changed: self.changed.clone(),
            id,
        }))
    }
    pub(super) async fn observed_headers(&self, route: &Route, headers: &HeaderMap) -> Result<()> {
        let secrets = route.secrets().await;
        let values: HashMap<_, _> = headers
            .iter()
            .filter_map(|(n, v)| {
                (ResponsesAdapter::rate_header(n.as_str())
                    && !contains_secret(n.as_str().as_bytes(), &secrets)
                    && safe_value(v.as_bytes(), &secrets, 256))
                .then(|| v.to_str().ok().map(|v| (n.to_string(), v.to_owned())))
                .flatten()
            })
            .collect();
        if !values.is_empty() {
            let mut h = route
                .health
                .lock()
                .map_err(|_| EgressError::StateUnavailable)?;
            h.rate_limits = values;
            h.rate_limits_seen_at = Some(Utc::now().timestamp());
            self.changed.notify_one();
        }
        Ok(())
    }
}

#[derive(Serialize)]
struct Health<'a> {
    version: u8,
    state: &'a str,
    updated_at: i64,
    routes: Vec<RouteHealth<'a>>,
}
#[derive(Serialize)]
struct RouteHealth<'a> {
    name: &'a str,
    login: String,
    expires_at: Option<u64>,
    cooldown_until: Option<i64>,
    retried_events: HashMap<String, u64>,
    waiting: Vec<Waiting>,
    rate_limits: HashMap<String, String>,
    rate_limits_seen_at: Option<i64>,
    new_session_hold_until: Option<i64>,
}
impl Passthrough {
    pub(super) async fn publish_health(
        &self,
        config: &PassthroughConfig,
        state: &str,
    ) -> Result<()> {
        let Some(path) = &config.health_path else {
            return Ok(());
        };
        let mut routes = Vec::new();
        for route in &self.routes {
            let (login, expires_at) = match &route.credential {
                Credential::Static(_) => ("static".to_owned(), None),
                Credential::Login(login) => {
                    let (health, expiry) = login.health().await?;
                    (
                        match health {
                            LoginHealth::Ok => "ok",
                            LoginHealth::Expiring => "expiring",
                            LoginHealth::RenewalFailed => "renewal_failed",
                            LoginHealth::LoginRequired => "login_required",
                        }
                        .to_owned(),
                        Some(expiry),
                    )
                }
            };
            if !safe_value(route.config.name.as_bytes(), &route.secrets().await, 128) {
                return Err(EgressError::CredentialUnavailable);
            }
            let cooldown = route
                .account
                .cooldown_until()
                .await
                .map_err(|_| EgressError::StateUnavailable)?
                .filter(|at| *at > self.now())
                .map(|at| at.timestamp());
            let secrets = route.secrets().await;
            let h = route
                .health
                .lock()
                .map_err(|_| EgressError::StateUnavailable)?;
            if h.waiting
                .values()
                .any(|w| !safe_value(w.request_id.as_bytes(), &secrets, 64))
                || h.retried_events
                    .keys()
                    .any(|code| contains_secret(code.as_bytes(), &secrets))
            {
                return Err(EgressError::CredentialUnavailable);
            }
            let hold = route
                .retries
                .sessions
                .state
                .lock()
                .map_err(|_| EgressError::StateUnavailable)?
                .until
                .filter(|t| *t > self.now())
                .map(|t| t.timestamp());
            routes.push(RouteHealth {
                name: &route.config.name,
                login,
                expires_at,
                cooldown_until: cooldown,
                retried_events: h.retried_events.clone(),
                waiting: h.waiting.values().cloned().collect(),
                rate_limits: h
                    .rate_limits
                    .iter()
                    .filter(|(k, v)| {
                        !contains_secret(k.as_bytes(), &secrets)
                            && safe_value(v.as_bytes(), &secrets, 256)
                    })
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
                rate_limits_seen_at: h.rate_limits_seen_at,
                new_session_hold_until: hold,
            });
        }
        let bytes = serde_json::to_vec(&Health {
            version: 1,
            state,
            updated_at: Utc::now().timestamp(),
            routes,
        })
        .map_err(|_| EgressError::StateUnavailable)?;
        atomic_health(path, &bytes)
    }
}
fn atomic_health(path: &Path, bytes: &[u8]) -> Result<()> {
    let directory = path.parent().ok_or(EgressError::InvalidRequest)?;
    let meta = fs::symlink_metadata(directory).map_err(|_| EgressError::StateUnavailable)?;
    // The deployment provisions this directory separately from the private socket
    // directory. Its ownership/group policy is not the socket's owner-only policy.
    if !meta.is_dir() {
        return Err(EgressError::StateUnavailable);
    }
    match fs::symlink_metadata(path) {
        Ok(meta) if !meta.is_file() => return Err(EgressError::StateUnavailable),
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => return Err(EgressError::StateUnavailable),
    }
    let temporary = directory.join(format!(".health-{}.tmp", uuid::Uuid::new_v4()));
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o640)
            .open(&temporary)
            .map_err(|_| EgressError::StateUnavailable)?;
        if file
            .metadata()
            .map_err(|_| EgressError::StateUnavailable)?
            .gid()
            != meta.gid()
        {
            return Err(EgressError::StateUnavailable);
        }
        file.set_permissions(fs::Permissions::from_mode(0o640))
            .map_err(|_| EgressError::StateUnavailable)?;
        file.write_all(bytes)
            .and_then(|()| file.sync_all())
            .map_err(|_| EgressError::StateUnavailable)?;
        fs::rename(&temporary, path).map_err(|_| EgressError::StateUnavailable)?;
        File::open(directory)
            .and_then(|dir| dir.sync_all())
            .map_err(|_| EgressError::StateUnavailable)
    })();
    if temporary.exists() {
        fs::remove_file(temporary).map_err(|_| EgressError::StateUnavailable)?;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn passthrough_contract_health_publication_is_complete_sanitized_and_inherits_directory_group()
     {
        let directory = tempfile::tempdir().unwrap();
        let config = super::super::tests::config(directory.path());
        let process = super::super::tests::open(config).unwrap();
        let pass = process.inner.passthrough.as_ref().unwrap();
        let config = process.config().passthrough.as_ref().unwrap();
        let path = config.health_path.as_ref().unwrap();
        let before = Utc::now().timestamp();
        pass.publish_health(config, "running").await.unwrap();
        let mut value: serde_json::Value =
            serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
        let updated = value["updated_at"].as_i64().unwrap();
        assert!(updated >= before && updated <= Utc::now().timestamp());
        value["updated_at"] = serde_json::json!(0);
        assert_eq!(
            value,
            serde_json::json!({"version":1,"state":"running","updated_at":0,"routes":[{"name":"responses","login":"static","expires_at":null,"cooldown_until":null,"retried_events":{},"waiting":[],"rate_limits":{},"rate_limits_seen_at":null,"new_session_hold_until":null}]})
        );
        let metadata = fs::metadata(path).unwrap();
        assert_eq!(metadata.mode() & 0o777, 0o640);
        assert_eq!(
            metadata.gid(),
            fs::metadata(directory.path()).unwrap().gid()
        );
        let reader_path = path.clone();
        let (stop, stopped) = std::sync::mpsc::channel();
        let reader = std::thread::spawn(move || {
            while stopped.try_recv().is_err() {
                let value: serde_json::Value =
                    serde_json::from_slice(&fs::read(&reader_path).unwrap()).unwrap();
                assert!(matches!(
                    value["state"].as_str(),
                    Some("running" | "stopping")
                ));
            }
        });
        for _ in 0..10 {
            pass.publish_health(config, "stopping").await.unwrap();
            pass.publish_health(config, "running").await.unwrap();
        }
        stop.send(()).unwrap();
        reader.join().unwrap();
    }
}
