//! Same-UID undecoded HTTP forwarding through the runtime's account admission.
//!
//! Ordering: authenticate peer, parse and match, check credential, acquire FIFO slot,
//! wait for shared cooldown, renew, pace, recheck cooldown, send. Hold the slot through
//! every retry and the complete filtered response. Dropping a connection drops this
//! entire future, including its admission waiter and upstream request.
use crate::{
    CredentialProcess, ProcessConfig,
    login::{Login, LoginHealth, LoginRequest},
    secrets::{LoginFamily, SecretSource},
    stream::{ItemJoin, StreamDialect},
};
use chrono::{DateTime, Utc};
use forward::RequestLog;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};
use symbiotic_ai_runtime::model;
use symbiotic_ai_runtime::{
    AccountAdmission, BindingIdentity, ModelQueueConfig, Runtime,
    model::{SecretValue, retry},
};
use symbiotic_core::logging::REQUEST_ID_HEADER;
use symbiotic_egress::EgressError;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{UnixListener, UnixStream},
    sync::{Notify, Semaphore},
    time::Instant,
};
use tracing::Instrument;

#[path = "passthrough_retry.rs"]
mod retry_policy;
pub use retry_policy::{
    ErrorClass, RetryAction, RetryBackoff, RetryContext, RetryDecision, RetryHook, RetryPolicy,
    RetryRule,
};
#[path = "passthrough_session.rs"]
mod session;
pub use session::NewSessionConfig;
use session::{AccountRetries, SeenSessions};
use symbiotic_ai_runtime::model::item_stream::{ItemEvent, ResponsesAdapter};

type Result<T> = std::result::Result<T, EgressError>;

/// Configuration for a second, same-UID HTTP/1.1 socket. Configuration version stays 4.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PassthroughConfig {
    /// Socket in a provisioned owner-only directory, distinct from the typed socket.
    pub socket_path: PathBuf,
    /// Optional sanitized, atomically replaced health file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub health_path: Option<PathBuf>,
    /// Maximum handlers, including callers waiting for slots or cooldowns.
    pub max_connections: usize,
    /// Exact caller-method/path bindings.
    pub routes: Vec<PassthroughRoute>,
    /// Named sparse retry overrides, selected by a caller-only header.
    #[serde(default)]
    pub retry_policies: HashMap<String, RetryPolicy>,
}

/// HTTP methods supported by an undecoded route.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PassthroughMethod {
    /// GET with an explicit Content-Length, including zero.
    Get,
    /// POST with a bounded Content-Length body.
    Post,
}
impl PassthroughMethod {
    fn text(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Post => "POST",
        }
    }
}

/// One exact endpoint with account-owned credentials and limits.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PassthroughRoute {
    /// Unique non-secret route name, also used in health.
    pub name: String,
    /// Tenant namespace.
    pub tenant: String,
    /// Concrete account within the tenant.
    pub account: String,
    /// Explicit quota sharing, including across tenants.
    pub account_sharing_key: Option<symbiotic_ai_runtime::AccountSharingKey>,
    /// Exact caller method.
    pub method: PassthroughMethod,
    /// Exact origin-form path without query, fragment, percent or backslash.
    pub path: String,
    /// Exact HTTPS destination; never caller supplied.
    pub destination: String,
    /// Renewable login or exact static key file.
    pub secret: SecretSource,
    /// Supported response event dialect.
    pub stream: StreamDialect,
    /// Lowercase request header allowlist, excluding authentication and hop headers.
    pub forward_headers: Vec<String>,
    /// Lowercase response header allowlist, excluding cookies and hop headers.
    #[serde(default)]
    pub return_headers: Vec<String>,
    /// Maximum buffered caller body bytes.
    pub max_request_bytes: usize,
    /// Maximum upstream body bytes; overflow ends the response.
    pub max_response_bytes: usize,
    /// Provisional retained event-envelope bound per item; defaults to 64 MiB.
    #[serde(default = "crate::stream::default_max_held_event_bytes")]
    pub max_held_event_bytes: usize,
    /// Shared account slot cap, required and positive.
    pub max_in_flight: usize,
    /// Shared request pacing, absent means unrestricted.
    pub requests_per_minute: Option<u32>,
    /// Shared burst duration; zero preserves the runtime's one-request default.
    #[serde(default)]
    pub rate_burst_seconds: u64,
    /// Maximum cumulative cooldown wait only; slot and pacing waits are excluded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_queue_wait_seconds: Option<u64>,
    /// Maximum interval between upstream bytes, including response headers.
    pub idle_timeout_seconds: u64,
    /// Adapter code classification extensions.
    #[serde(default)]
    pub error_classes: HashMap<String, ErrorClass>,
    /// Sparse overrides of the default class table.
    #[serde(default)]
    pub retry_policy: RetryPolicy,
    /// Optional route names used only by the switch action, in order.
    #[serde(default)]
    pub fallback_routes: Vec<String>,
    /// Optional new-session admission policy.
    #[serde(default)]
    pub new_session: Option<NewSessionConfig>,
}

impl PassthroughRoute {
    fn identity(&self) -> BindingIdentity {
        BindingIdentity::new(&self.tenant, &self.name, "passthrough-v1", &self.account)
    }
    fn policy(&self) -> ModelQueueConfig {
        ModelQueueConfig {
            max_in_flight: self.max_in_flight,
            requests_per_minute: self.requests_per_minute,
            rate_burst_seconds: self.rate_burst_seconds,
            ..Default::default()
        }
    }
}

fn path_valid(path: &str) -> bool {
    path.starts_with('/')
        && !path.starts_with("//")
        && path
            .bytes()
            .all(|b| (b'!'..=b'~').contains(&b) && !b"?%#\\".contains(&b))
}
fn forbidden_header(name: &str) -> bool {
    matches!(
        name,
        "authorization"
            | "proxy-authorization"
            | "proxy-authenticate"
            | "www-authenticate"
            | "cookie"
            | "cookie2"
            | "set-cookie"
            | "host"
            | "connection"
            | "keep-alive"
            | "transfer-encoding"
            | "content-length"
            | "content-encoding"
            | "te"
            | "trailer"
            | "upgrade"
            | "expect"
            | "accept-encoding"
            | "forwarded"
            | "via"
            | "originator"
            | "chatgpt-account-id"
            | "x-xai-token-auth"
            | "x-symbiotic-retry-policy"
            | symbiotic_core::logging::REQUEST_ID_HEADER
            | "x-symbiotic-queue-waited-ms"
    ) || name.starts_with("x-forwarded-")
}
fn header_list_valid(headers: &[String], response: bool) -> bool {
    let mut seen = HashSet::new();
    headers.iter().all(|name| {
        !name.is_empty()
            && name.bytes().all(|b| !b.is_ascii_uppercase())
            && HeaderName::from_bytes(name.as_bytes()).is_ok()
            && !forbidden_header(name)
            && !(response && matches!(name.as_str(), "content-type" | "location"))
            && seen.insert(name)
    })
}

pub(crate) fn validate(config: &ProcessConfig) -> Result<()> {
    let Some(pass) = &config.passthrough else {
        return Ok(());
    };
    if pass.socket_path == config.socket_path
        || !pass.socket_path.is_absolute()
        || pass.max_connections == 0
        || pass.max_connections > Semaphore::MAX_PERMITS
        || pass.routes.is_empty()
        || pass
            .retry_policies
            .iter()
            .any(|(n, p)| !printable(n.as_bytes(), 128) || !retry_policy::valid(p))
        || pass.health_path.as_ref().is_some_and(|path| {
            !path.is_absolute() || path == &pass.socket_path || path == &config.socket_path
        })
    {
        return Err(EgressError::InvalidRequest);
    }
    if let Some(health) = &pass.health_path {
        let sources = std::iter::once(&config.admission_key)
            .chain(config.routes.iter().map(|r| &r.secret))
            .chain(pass.routes.iter().map(|r| &r.secret));
        for source in sources {
            if let SecretSource::LoginFile { path, .. } | SecretSource::OwnerOnlyFile { path } =
                source
            {
                let mut fence = path.as_os_str().to_owned();
                fence.push(".renewal");
                let mut lock = path.as_os_str().to_owned();
                lock.push(".lock");
                if health == path || health.as_os_str() == fence || health.as_os_str() == lock {
                    return Err(EgressError::InvalidRequest);
                }
            }
        }
        if health.starts_with(&config.state_dir) {
            return Err(EgressError::InvalidRequest);
        }
    }
    let mut names: HashSet<&str> = config.routes.iter().map(|r| r.route.as_str()).collect();
    let mut paths = HashSet::new();
    // Same account policy comparison as typed routes, without opening state or secrets.
    let mut policies = HashMap::new();
    for route in &config.routes {
        let scope = symbiotic_ai_runtime::account_scope(
            &BindingIdentity::new(&route.tenant, &route.route, "validation", &route.account),
            route.account_sharing_key.as_ref(),
        )
        .map_err(|_| EgressError::InvalidRequest)?;
        policies.insert(
            scope,
            (
                route.max_in_flight,
                route.requests_per_minute,
                ModelQueueConfig::default().rate_burst_seconds,
                route.input_units_per_minute.is_some() || route.provider_request_limit.is_some(),
            ),
        );
    }
    for route in &pass.routes {
        if route.name.is_empty()
            || route.name.len() > 128
            || !route
                .name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
            || !names.insert(&route.name)
            || !paths.insert((route.method, &route.path))
            || route.tenant.trim().is_empty()
            || route.account.trim().is_empty()
            || !path_valid(&route.path)
            || route.max_request_bytes == 0
            || route.max_response_bytes == 0
            || route.max_held_event_bytes == 0
            || route.max_in_flight == 0
            || route.requests_per_minute == Some(0)
            || route.idle_timeout_seconds == 0
            || route.stream != StreamDialect::OpenaiResponses
            || !retry_policy::valid(&route.retry_policy)
            || route
                .fallback_routes
                .iter()
                .any(|n| !pass.routes.iter().any(|r| &r.name == n))
            || route.new_session.as_ref().is_some_and(|c| {
                c.session_headers.is_empty() || !header_list_valid(&c.session_headers, false)
            })
            || !header_list_valid(&route.forward_headers, false)
            || !header_list_valid(&route.return_headers, true)
        {
            return Err(EgressError::InvalidRequest);
        }
        route
            .policy()
            .validate()
            .map_err(|_| EgressError::InvalidRequest)?;
        let url =
            reqwest::Url::parse(&route.destination).map_err(|_| EgressError::InvalidRequest)?;
        if url.scheme() != "https"
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(EgressError::InvalidRequest);
        }
        if !matches!(&route.secret, SecretSource::LoginFile { path, .. } | SecretSource::OwnerOnlyFile { path } if path.is_absolute())
        {
            return Err(EgressError::InvalidRequest);
        }
        let scope = symbiotic_ai_runtime::account_scope(
            &route.identity(),
            route.account_sharing_key.as_ref(),
        )
        .map_err(|_| EgressError::InvalidRequest)?;
        let policy = (
            route.max_in_flight,
            route.requests_per_minute,
            route.rate_burst_seconds,
            false,
        );
        if policies
            .insert(scope, policy)
            .is_some_and(|existing| existing != policy)
        {
            return Err(EgressError::InvalidRequest);
        }
    }
    Ok(())
}

enum Credential {
    Login(Arc<Login>),
    Static(SecretValue<String>),
}
struct Route {
    config: PassthroughRoute,
    account: AccountAdmission,
    credential: Credential,
    client: reqwest::Client,
    retries: AccountRetries,
    seen: Mutex<SeenSessions>,
    health: Arc<Mutex<RouteActivity>>,
}
pub(crate) struct Passthrough {
    routes: Vec<Route>,
    changed: Arc<Notify>,
    retry_policies: HashMap<String, RetryPolicy>,
    pub(crate) retry_hook: Option<RetryHook>,
    #[cfg(test)]
    wire_base: Option<String>,
    #[cfg(test)]
    clock: Option<Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>>,
}
impl Passthrough {
    pub(crate) fn open(config: &ProcessConfig, runtime: &Runtime) -> Result<Option<Self>> {
        let Some(pass) = &config.passthrough else {
            return Ok(None);
        };
        let changed = Arc::new(Notify::new());
        let mut logins: HashMap<PathBuf, (LoginFamily, Arc<Login>)> = HashMap::new();
        let mut routes = Vec::new();
        let mut retry_accounts: HashMap<String, Arc<std::sync::atomic::AtomicU32>> = HashMap::new();
        let mut session_accounts: HashMap<String, Arc<session::SessionAdmission>> = HashMap::new();
        for config_route in &pass.routes {
            let account = runtime
                .account_admission(
                    &config_route.identity(),
                    config_route.account_sharing_key.as_ref(),
                    config_route.policy(),
                )
                .map_err(|_| EgressError::InvalidRequest)?;
            let credential = match &config_route.secret {
                SecretSource::LoginFile { path, family } => {
                    let entry = match logins.entry(path.clone()) {
                        std::collections::hash_map::Entry::Occupied(entry) => {
                            if entry.get().0 != *family {
                                return Err(EgressError::InvalidRequest);
                            }
                            entry.into_mut()
                        }
                        std::collections::hash_map::Entry::Vacant(entry) => {
                            let mut login = Login::open(path, *family, config.max_secret_bytes)?;
                            login.notifications = changed.clone();
                            entry.insert((*family, Arc::new(login)))
                        }
                    };
                    Credential::Login(entry.1.clone())
                }
                SecretSource::OwnerOnlyFile { .. } => {
                    let bytes = config_route.secret.load(config.max_secret_bytes)?;
                    let key = SecretValue::new(
                        std::str::from_utf8(&bytes)
                            .map_err(|_| EgressError::CredentialUnavailable)?
                            .to_owned(),
                    );
                    if key.is_empty() || !key.bytes().all(|b| (b'!'..=b'~').contains(&b)) {
                        return Err(EgressError::CredentialUnavailable);
                    }
                    Credential::Static(key)
                }
                _ => return Err(EgressError::InvalidRequest),
            };
            let client = reqwest::Client::builder()
                .https_only(true)
                .read_timeout(Duration::from_secs(config_route.idle_timeout_seconds))
                .connect_timeout(Duration::from_secs(config_route.idle_timeout_seconds))
                .no_gzip()
                .no_brotli()
                .no_deflate()
                .no_zstd()
                .http1_only()
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .retry(reqwest::retry::never())
                .build()
                .map_err(|_| EgressError::StateUnavailable)?;
            let scope = symbiotic_ai_runtime::account_scope(
                &config_route.identity(),
                config_route.account_sharing_key.as_ref(),
            )
            .map_err(|_| EgressError::InvalidRequest)?;
            let session_key = match &config_route.secret {
                SecretSource::LoginFile { path, .. } => format!("login:{}", path.display()),
                _ => scope.clone(),
            };
            let retries = AccountRetries {
                failures: retry_accounts.entry(scope).or_default().clone(),
                sessions: session_accounts.entry(session_key).or_default().clone(),
            };
            routes.push(Route {
                config: config_route.clone(),
                account,
                credential,
                client,
                retries,
                seen: Default::default(),
                health: Default::default(),
            });
        }
        Ok(Some(Self {
            routes,
            changed,
            retry_policies: pass.retry_policies.clone(),
            retry_hook: None,
            #[cfg(test)]
            wire_base: None,
            #[cfg(test)]
            clock: None,
        }))
    }

    fn now(&self) -> DateTime<Utc> {
        #[cfg(test)]
        if let Some(clock) = &self.clock {
            return clock();
        }
        Utc::now()
    }
}
impl Route {
    async fn secrets(&self) -> SecretValue<Vec<String>> {
        match &self.credential {
            Credential::Login(login) => login.held_secrets().await,
            Credential::Static(key) => SecretValue::new(vec![(**key).clone()]),
        }
    }
    async fn usable(&self) -> Result<()> {
        if let Credential::Login(login) = &self.credential {
            login.usable().await?;
        }
        Ok(())
    }
    async fn headers(&self) -> Result<(HeaderMap, Option<LoginRequest>)> {
        match &self.credential {
            Credential::Login(login) => {
                let request = login.request().await?;
                Ok((request.headers().clone(), Some(request)))
            }
            Credential::Static(key) => {
                let mut headers = HeaderMap::new();
                let mut value = HeaderValue::from_str(&format!("Bearer {}", **key))
                    .map_err(|_| EgressError::CredentialUnavailable)?;
                value.set_sensitive(true);
                headers.insert("authorization", value);
                Ok((headers, None))
            }
        }
    }
}

/// Bind the configured passthrough socket with the same directory and bind-lock rules.
pub fn bind(process: &CredentialProcess) -> Result<UnixListener> {
    let config = process
        .config()
        .passthrough
        .as_ref()
        .ok_or(EgressError::InvalidRequest)?;
    crate::server::bind_path(&config.socket_path)
}

/// Serve HTTP until SIGTERM/SIGINT. Publish `stopping` before closing the listener.
/// Every handler is owned here and cancelled on shutdown or server failure.
pub async fn serve(process: CredentialProcess, listener: UnixListener) -> Result<()> {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate()).map_err(|_| EgressError::StateUnavailable)?;
    let mut interrupt =
        signal(SignalKind::interrupt()).map_err(|_| EgressError::StateUnavailable)?;
    serve_until(
        process,
        listener,
        async move {
            let received = tokio::select! {
                received = term.recv() => received,
                received = interrupt.recv() => received,
            };
            received.ok_or(EgressError::StateUnavailable)
        },
        current_uid(),
    )
    .await
}

async fn serve_until(
    process: CredentialProcess,
    listener: UnixListener,
    shutdown: impl std::future::Future<Output = Result<()>>,
    owner_uid: u32,
) -> Result<()> {
    let pass = process
        .inner
        .passthrough
        .as_ref()
        .ok_or(EgressError::InvalidRequest)?;
    let config = process
        .config()
        .passthrough
        .as_ref()
        .ok_or(EgressError::InvalidRequest)?;
    let mut cooldown_changes = pass
        .routes
        .first()
        .ok_or(EgressError::InvalidRequest)?
        .account
        .subscribe_cooldowns();
    pass.publish_health(config, "running").await?;
    let connections = Arc::new(Semaphore::new(config.max_connections));
    let mut handlers = tokio::task::JoinSet::new();
    tokio::pin!(shutdown);
    let mut heartbeat = tokio::time::interval(Duration::from_secs(10));
    loop {
        tokio::select! {
            biased;
            shutdown_result = &mut shutdown => {
                // This awaited durable publication precedes listener drop.
                pass.publish_health(config, "stopping").await?;
                handlers.abort_all();
                while handlers.join_next().await.is_some() {}
                return shutdown_result;
            }
            _ = heartbeat.tick() => { pass.publish_health(config, "running").await?; }
            () = pass.changed.notified() => { pass.publish_health(config, "running").await?; }
            result = cooldown_changes.changed() => {
                result.map_err(|_| EgressError::StateUnavailable)?;
                pass.publish_health(config, "running").await?;
            }
            Some(result) = handlers.join_next(), if !handlers.is_empty() => {
                if result.is_err() { return Err(EgressError::StateUnavailable); }
            }
            accepted = async {
                let permit = connections.clone().acquire_owned().await.map_err(|_| EgressError::StateUnavailable)?;
                let (socket, _) = listener.accept().await.map_err(|_| EgressError::Transport)?;
                Ok::<_, EgressError>((socket, permit))
            } => {
                let (socket, permit) = accepted?;
                let Ok(peer) = socket.peer_cred() else { continue; };
                // SAFETY: geteuid takes no arguments and retains no memory.
                if peer.uid() != owner_uid { continue; }
                let process = process.clone();
                handlers.spawn(async move {
                    let _permit = permit;
                    // The handler logs failures inside the request span.
                    let _ = connection(&process, socket).await;
                });
            }
        }
    }
}
fn current_uid() -> u32 {
    // SAFETY: geteuid takes no arguments and retains no memory.
    unsafe { libc::geteuid() }
}

fn trim_ows(mut bytes: &[u8]) -> &[u8] {
    while matches!(bytes.first(), Some(b' ' | b'\t')) {
        bytes = &bytes[1..];
    }
    while matches!(bytes.last(), Some(b' ' | b'\t')) {
        bytes = &bytes[..bytes.len() - 1];
    }
    bytes
}

struct Request {
    method: String,
    path: String,
    headers: Vec<(String, Vec<u8>)>,
    length: usize,
}
async fn read_request(socket: &mut UnixStream, max_frame_bytes: u32) -> Result<Request> {
    let mut bytes = Vec::new();
    loop {
        if bytes.len() >= max_frame_bytes as usize {
            return Err(EgressError::LimitExceeded);
        }
        bytes.push(socket.read_u8().await.map_err(|_| EgressError::Transport)?);
        if bytes.ends_with(b"\r\n\r\n") {
            break;
        }
    }
    let lines: Vec<_> = bytes
        .split_inclusive(|b| *b == b'\n')
        .map(|line| {
            line.strip_suffix(b"\r\n")
                .ok_or(EgressError::InvalidRequest)
        })
        .collect::<Result<_>>()?;
    let first = std::str::from_utf8(lines.first().ok_or(EgressError::InvalidRequest)?)
        .map_err(|_| EgressError::InvalidRequest)?;
    let parts: Vec<_> = first.split(' ').collect();
    if parts.len() != 3 || parts[2] != "HTTP/1.1" {
        return Err(EgressError::InvalidRequest);
    }
    let (method, path) = (parts[0].to_owned(), parts[1].to_owned());
    let mut headers = Vec::new();
    let mut names = HashSet::new();
    for line in lines.iter().skip(1).take(lines.len().saturating_sub(2)) {
        let colon = line
            .iter()
            .position(|b| *b == b':')
            .ok_or(EgressError::InvalidRequest)?;
        let name = &line[..colon];
        let name = HeaderName::from_bytes(name)
            .map_err(|_| EgressError::InvalidRequest)?
            .as_str()
            .to_owned();
        if !names.insert(name.clone()) || matches!(name.as_str(), "transfer-encoding" | "expect") {
            return Err(EgressError::InvalidRequest);
        }
        let value = trim_ows(&line[colon + 1..]);
        if value.contains(&b'\r') || value.contains(&b'\n') {
            return Err(EgressError::InvalidRequest);
        }
        headers.push((name, value.to_vec()));
    }
    if !names.contains("host") {
        return Err(EgressError::InvalidRequest);
    }
    let length = headers
        .iter()
        .find(|(k, _)| k == "content-length")
        .map(|(_, v)| v)
        .filter(|v| !v.is_empty() && v.iter().all(u8::is_ascii_digit))
        .and_then(|v| std::str::from_utf8(v).ok()?.parse().ok())
        .ok_or(EgressError::InvalidRequest)?;
    Ok(Request {
        method,
        path,
        headers,
        length,
    })
}
async fn connection(process: &CredentialProcess, socket: UnixStream) -> Result<()> {
    let wait = RequestLog::new();
    let span = wait.span.clone();
    async {
        let result = connection_request(process, socket, wait).await;
        if let Err(error) = &result {
            tracing::warn!(code = %error, "passthrough connection ended");
        }
        result
    }
    .instrument(span)
    .await
}
async fn connection_request(
    process: &CredentialProcess,
    mut socket: UnixStream,
    mut wait: RequestLog,
) -> Result<()> {
    let timeout = Duration::from_secs(process.config().io_timeout_seconds);
    let request = match tokio::time::timeout(
        timeout,
        read_request(&mut socket, process.config().max_frame_bytes),
    )
    .await
    {
        Ok(Ok(request)) => request,
        Ok(Err(EgressError::Transport)) => return Err(EgressError::Transport),
        _ => return refuse(process, socket, 400, "invalid_request", wait).await,
    };
    let pass = process
        .inner
        .passthrough
        .as_ref()
        .ok_or(EgressError::InvalidRequest)?;
    let route = pass.routes.iter().find(|route| {
        path_valid(&request.path)
            && route.config.path == request.path
            && route.config.method.text() == request.method
    });
    let request_id = request
        .headers
        .iter()
        .find(|(name, _)| name == REQUEST_ID_HEADER)
        .map(|(_, value)| value);
    let mut invalid_id = request_id.is_some_and(|value| !printable(value, 64));
    if let Some(value) = request_id {
        if let Some(route) = route {
            let secrets = route.secrets().await;
            invalid_id = !safe_value(value, &secrets, 64);
        } else {
            // An unmatched request still must not log a configured credential as its ID.
            for route in &pass.routes {
                let secrets = route.secrets().await;
                invalid_id |= !safe_value(value, &secrets, 64);
            }
        }
    }
    let id = request_id
        .filter(|_| !invalid_id)
        .and_then(|value| std::str::from_utf8(value).ok());
    if let Some(id) = id {
        wait.span.record("request_id", id);
    }
    let Some(route) = route else {
        return refuse(process, socket, 404, "route_not_found", wait).await;
    };
    wait.route = Some(route.config.name.clone());
    if request.length > route.config.max_request_bytes {
        return refuse(process, socket, 413, "request_too_large", wait).await;
    }
    if invalid_id {
        return refuse(process, socket, 400, "request_id_invalid", wait).await;
    }
    let mut body = vec![0; request.length];
    tokio::time::timeout(timeout, socket.read_exact(&mut body))
        .await
        .map_err(|_| EgressError::Transport)?
        .map_err(|_| EgressError::Transport)?;
    let (mut read, mut write) = socket.into_split();
    // One request per connection. EOF or extra caller bytes cancel the entire work.
    let cancelled = async {
        let mut byte = [0];
        let _ = read.read(&mut byte).await;
    };
    let result = tokio::select! {
        biased;
        () = cancelled => Ok(()),
        result = pass.forward(route, &request, body, &mut write, id, &mut wait) => result,
    };
    if let Err(error) = &result {
        // These errors carry only static diagnostics, never provider data.
        wait.outcome = error.to_string();
    }
    result
}

fn http_error(error: reqwest::Error) -> EgressError {
    if error.is_timeout() {
        EgressError::Timeout
    } else {
        EgressError::Transport
    }
}

fn printable(value: &[u8], max: usize) -> bool {
    !value.is_empty() && value.len() <= max && value.iter().all(|b| (b' '..=b'~').contains(b))
}
fn contains_secret(value: &[u8], secrets: &[String]) -> bool {
    secrets.iter().filter(|s| !s.is_empty()).any(|s| {
        symbiotic_ai_runtime::model::credential_encodings(s).map_or(true, |variants| {
            variants
                .iter()
                .any(|s| value.windows(s.len()).any(|part| part == s.as_bytes()))
        })
    })
}
fn safe_value(value: &[u8], secrets: &[String], max: usize) -> bool {
    printable(value, max) && !contains_secret(value, secrets)
}

fn rate_header(name: &str, _route: &PassthroughRoute) -> bool {
    name == "retry-after" || name.starts_with("x-ratelimit-") || ResponsesAdapter::rate_header(name)
}

fn returned(
    route: &PassthroughRoute,
    upstream: &HeaderMap,
    secrets: &[String],
    status: u16,
) -> Vec<(String, String)> {
    upstream
        .iter()
        .filter_map(|(name, value)| {
            let name = name.as_str();
            let rate = ResponsesAdapter::rate_header(name)
                || (matches!(status, 429 | 503) && rate_header(name, route));
            if forbidden_header(name)
                || contains_secret(name.as_bytes(), secrets)
                || !(rate || route.return_headers.iter().any(|h| h == name))
                || !safe_value(value.as_bytes(), secrets, if rate { 256 } else { 8192 })
            {
                return None;
            }
            Some((name.to_owned(), value.to_str().ok()?.to_owned()))
        })
        .collect()
}
async fn head(
    write: &mut (impl tokio::io::AsyncWrite + Unpin),
    status: u16,
    content_type: &str,
    headers: &[(String, String)],
) -> Result<()> {
    let mut bytes = format!(
        "HTTP/1.1 {status} Response\r\nConnection: close\r\nTransfer-Encoding: chunked\r\nContent-Type: {content_type}\r\n"
    );
    for (name, value) in headers {
        bytes.push_str(name);
        bytes.push_str(": ");
        bytes.push_str(value);
        bytes.push_str("\r\n");
    }
    if !headers
        .iter()
        .any(|(name, _)| name == "x-symbiotic-queue-waited-ms")
    {
        bytes.push_str("x-symbiotic-queue-waited-ms: 0\r\n");
    }
    bytes.push_str("\r\n");
    write
        .write_all(bytes.as_bytes())
        .await
        .map_err(|_| EgressError::Transport)
}
async fn chunk(write: &mut (impl tokio::io::AsyncWrite + Unpin), bytes: &[u8]) -> Result<()> {
    if bytes.is_empty() {
        return Ok(());
    }
    write
        .write_all(format!("{:x}\r\n", bytes.len()).as_bytes())
        .await
        .map_err(|_| EgressError::Transport)?;
    write
        .write_all(bytes)
        .await
        .map_err(|_| EgressError::Transport)?;
    write
        .write_all(b"\r\n")
        .await
        .map_err(|_| EgressError::Transport)
}
async fn finish(write: &mut (impl tokio::io::AsyncWrite + Unpin)) -> Result<()> {
    write
        .write_all(b"0\r\n\r\n")
        .await
        .map_err(|_| EgressError::Transport)
}
/// Answer before the request body is read, then drain the caller's unread bytes.
/// Closing a Unix stream with unread input resets the peer on Linux, which can
/// discard this response before the caller reads it.
async fn refuse(
    process: &CredentialProcess,
    mut socket: UnixStream,
    status: u16,
    code: &str,
    mut wait: RequestLog,
) -> Result<()> {
    wait.outcome = code.into();
    fixed(&mut socket, status, code, &[]).await?;
    wait.status = Some(status);
    drop(wait);
    let _ = socket.shutdown().await;
    let unread = u64::from(process.config().max_frame_bytes);
    let timeout = Duration::from_secs(process.config().io_timeout_seconds);
    let _ = tokio::time::timeout(
        timeout,
        tokio::io::copy(&mut (&mut socket).take(unread), &mut tokio::io::sink()),
    )
    .await;
    Ok(())
}
async fn fixed(
    write: &mut (impl tokio::io::AsyncWrite + Unpin),
    status: u16,
    code: &str,
    headers: &[(String, String)],
) -> Result<()> {
    head(write, status, "application/json", headers).await?;
    chunk(
        write,
        format!("{{\"error\":{{\"code\":\"{code}\"}}}}").as_bytes(),
    )
    .await?;
    finish(write).await
}

#[path = "passthrough_forward.rs"]
mod forward;

#[path = "passthrough_health.rs"]
mod health;
use health::RouteActivity;

#[cfg(test)]
#[path = "passthrough_tests.rs"]
mod tests;
