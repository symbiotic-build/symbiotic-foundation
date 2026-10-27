//! One process owns a subscription store and durably fences token rotation.
//!
//! Ordering: fence sync and parent sync precede exchange; store sync, atomic
//! installation and parent sync precede fence clear. Cancellation leaves the
//! fence set. Only a successful install or explicit import clears it.
use crate::secrets::{LoginFamily, read_private_file};
use base64::Engine;
use reqwest::{
    Client, Url,
    header::{AUTHORIZATION, HeaderMap, HeaderValue},
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::{
    fs::{self, File, OpenOptions},
    io::{Seek, SeekFrom, Write},
    os::{
        fd::AsRawFd,
        unix::fs::{MetadataExt, OpenOptionsExt},
    },
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use symbiotic_ai_runtime::model::SecretValue;
use symbiotic_egress::EgressError;
use tokio::sync::Mutex;

const CODEX_CLIENT: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
const GROK_CLIENT: &str = "b1a00492-073a-47ea-816f-4c329264a828";
const KIMI_CLIENT: &str = "17e5f671-d194-4dfb-9706-5516cb48c098";
const UNAVAILABLE: EgressError = EgressError::CredentialUnavailable;
type Result<T> = std::result::Result<T, EgressError>;

impl LoginFamily {
    fn issuer(self) -> &'static str {
        match self {
            Self::ChatgptCodex => "https://auth.openai.com",
            Self::XaiGrok => "https://auth.x.ai",
            Self::KimiCode => "https://auth.kimi.com",
        }
    }
    fn client_id(self) -> &'static str {
        match self {
            Self::ChatgptCodex => CODEX_CLIENT,
            Self::XaiGrok => GROK_CLIENT,
            Self::KimiCode => KIMI_CLIENT,
        }
    }
}

// Deliberately no Debug, including on the store and handle. Serde is private
// to the durable store and the provider exchange. Every owned token zeroizes.
#[derive(Clone)]
struct Token(SecretValue<String>);
impl Serialize for Token {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}
impl<'de> Deserialize<'de> for Token {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        String::deserialize(deserializer).map(|text| Self(SecretValue::new(text)))
    }
}
impl Token {
    fn text(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Store {
    version: u32,
    family: LoginFamily,
    issuer: String,
    client_id: String,
    access_token: Token,
    refresh_token: Token,
    id_token: Option<Token>,
    account_id: Option<Token>,
    principal_type: Option<Token>,
    principal_id: Option<Token>,
    expires_at: u64,
}
impl Store {
    fn validate(&self, family: LoginFamily) -> Result<()> {
        if self.version != 1
            || self.family != family
            || self.issuer != family.issuer()
            || self.client_id != family.client_id()
            || !valid_text(self.access_token.text())
            || !valid_text(self.refresh_token.text())
            || self.expires_at == 0
            || self
                .id_token
                .as_ref()
                .is_some_and(|v| !valid_text(v.text()))
            || self
                .account_id
                .as_ref()
                .is_some_and(|v| !valid_text(v.text()))
            || (family == LoginFamily::ChatgptCodex && self.account_id.is_none())
        {
            return Err(UNAVAILABLE);
        }
        self.headers()?;
        Ok(())
    }
    fn headers(&self) -> Result<HeaderMap> {
        let mut headers = HeaderMap::new();
        let authorization = SecretValue::new(format!("Bearer {}", self.access_token.text()));
        let mut value = HeaderValue::from_str(&authorization).map_err(|_| UNAVAILABLE)?;
        value.set_sensitive(true);
        headers.insert(AUTHORIZATION, value);
        match self.family {
            LoginFamily::ChatgptCodex => {
                let account = self.account_id.as_ref().ok_or(UNAVAILABLE)?;
                let mut value = HeaderValue::from_str(account.text()).map_err(|_| UNAVAILABLE)?;
                value.set_sensitive(true);
                headers.insert("chatgpt-account-id", value);
                headers.insert("originator", HeaderValue::from_static("codex_cli_rs"));
            }
            LoginFamily::XaiGrok => {
                headers.insert("x-xai-token-auth", HeaderValue::from_static("xai-grok-cli"));
            }
            LoginFamily::KimiCode => {}
        }
        Ok(headers)
    }
    fn remember(&self, history: &mut SecretValue<Vec<String>>) {
        for token in [
            Some(&self.access_token),
            Some(&self.refresh_token),
            self.id_token.as_ref(),
            self.account_id.as_ref(),
            self.principal_id.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            if !token.text().is_empty() && !history.iter().any(|old| old == token.text()) {
                history.push(token.text().to_owned());
            }
        }
    }
}

/// Non-secret login state for a later health route.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LoginHealth {
    /// Token remains valid for more than two days.
    Ok,
    /// Token expires within two days.
    Expiring,
    /// A safe-to-retry exchange failed, or a retried request got another 401.
    RenewalFailed,
    /// A rotation may have happened; explicit login/import is required.
    LoginRequired,
}

struct State {
    store: Store,
    failure: Option<LoginHealth>,
    generation: u64,
    retry_renewal: bool,
}

/// A request's headers and its one permitted 401 retry. Does not implement Debug.
/// Keep this value until the upstream request finishes. Start a new one per request.
pub struct LoginRequest {
    headers: HeaderMap,
    generation: u64,
    retried: bool,
}
impl LoginRequest {
    /// Whether this request already used its one 401-triggered renewal.
    pub(crate) fn retried(&self) -> bool {
        self.retried
    }

    /// Credential headers to inject, with sensitive values hidden by HeaderMap Debug.
    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }
}

/// Lifetime-exclusive owner of one login store. No other process may import it.
pub struct Login {
    path: PathBuf,
    max_secret_bytes: usize,
    _lock: crate::ProcessLock,
    fence: File,
    state: Mutex<State>,
    http: Http,
    renewing: std::sync::atomic::AtomicBool,
    history_state: tokio::sync::watch::Sender<SecretValue<Vec<String>>>,
    health_state: tokio::sync::watch::Sender<(Option<LoginHealth>, u64, bool)>,
    pub(crate) notifications: std::sync::Arc<tokio::sync::Notify>,
    #[cfg(test)]
    fail_install: std::sync::atomic::AtomicBool,
}
impl Login {
    /// Open a protected, bounded store and take its nonblocking lifetime lock.
    /// A set renewal fence opens in `login_required` and never exchanges a token.
    pub fn open(path: &Path, family: LoginFamily, max_secret_bytes: usize) -> Result<Self> {
        crate::process_security::require_protection().map_err(|_| UNAVAILABLE)?;
        if max_secret_bytes == 0 {
            return Err(UNAVAILABLE);
        }
        let lock = lock(path)?;
        let bytes = read_private_file(path, max_secret_bytes)?;
        let store: Store = serde_json::from_slice(&bytes).map_err(|_| UNAVAILABLE)?;
        store.validate(family)?;
        let fence = private_file(&sibling(path, ".renewal"))?;
        let failure = if fence.metadata().map_err(|_| UNAVAILABLE)?.len() != 0 {
            Some(LoginHealth::LoginRequired)
        } else {
            None
        };
        let mut history = SecretValue::new(Vec::new());
        store.remember(&mut history);
        let health_state = tokio::sync::watch::channel((failure, store.expires_at, false)).0;
        Ok(Self {
            renewing: std::sync::atomic::AtomicBool::new(false),
            history_state: tokio::sync::watch::channel(history).0,
            health_state,
            notifications: Default::default(),
            path: path.into(),
            max_secret_bytes,
            _lock: lock,
            fence,
            state: Mutex::new(State {
                store,
                failure,
                generation: 0,
                retry_renewal: false,
            }),
            http: Http::new()?,
            #[cfg(test)]
            fail_install: std::sync::atomic::AtomicBool::new(false),
        })
    }

    #[cfg(test)]
    pub(crate) fn test_transport(&mut self, base: &str, timeout: Duration) {
        self.http.client = Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .timeout(timeout)
            .build()
            .expect("fixture client");
        self.http.wire_base = Some(base.into());
    }

    // Safe pre-send failures may renew again; uncertain/terminal failures refuse.
    pub(crate) async fn usable(&self) -> Result<()> {
        let (failure, _, retry_renewal) = *self.health_state.borrow();
        let renewing = self.renewing.load(std::sync::atomic::Ordering::Acquire);
        let fenced = self.fence.metadata().map_err(|_| UNAVAILABLE)?.len() != 0;
        if !renewing
            && (fenced
                || failure == Some(LoginHealth::LoginRequired)
                || (failure == Some(LoginHealth::RenewalFailed) && !retry_renewal))
        {
            return Err(UNAVAILABLE);
        }
        Ok(())
    }

    /// Report non-secret state and token expiry as Unix seconds, without renewal.
    pub async fn health(&self) -> Result<(LoginHealth, u64)> {
        let (failure, expires_at, _) = *self.health_state.borrow();
        let renewing = self.renewing.load(std::sync::atomic::Ordering::Acquire);
        let fenced = self.fence.metadata().map_err(|_| UNAVAILABLE)?.len() != 0;
        let failure = if renewing { None } else { failure };
        let health = failure.unwrap_or(if fenced && !renewing {
            LoginHealth::LoginRequired
        } else if expires_at <= now()?.saturating_add(2 * 24 * 60 * 60) {
            LoginHealth::Expiring
        } else {
            LoginHealth::Ok
        });
        Ok((health, expires_at))
    }

    fn publish(&self, state: &State) {
        self.health_state.send_replace((
            state.failure,
            state.store.expires_at,
            state.retry_renewal,
        ));
        self.notifications.notify_one();
    }

    /// Renew within 300 seconds of expiry and return this request's injected headers.
    pub async fn request(&self) -> Result<LoginRequest> {
        let mut state = self.state.lock().await;
        self.renew(&mut state, false).await?;
        Ok(LoginRequest {
            headers: state.store.headers()?,
            generation: state.generation,
            retried: false,
        })
    }

    /// After 401, renew once and update this request's headers. A second 401
    /// reports `renewal_failed` without exchanging again. Concurrent stale 401s
    /// adopt the in-process renewal already completed for their generation.
    pub async fn after_unauthorized(&self, request: &mut LoginRequest) -> Result<()> {
        let mut state = self.state.lock().await;
        if request.retried {
            // A terminal authentication failure requires import, including after
            // restart. Reuse the existing durable refusal fence, not another store.
            state.failure = Some(LoginHealth::RenewalFailed);
            state.retry_renewal = false;
            self.publish(&state);
            // Refuse in-process admission even if the durable fence cannot be
            // written. Persistence errors must never leave a rejected login usable.
            set_fence(&self.fence, &self.path, true)?;
            return Err(UNAVAILABLE);
        }
        request.retried = true;
        self.check_fence(&mut state)?;
        if request.generation == state.generation {
            self.renew(&mut state, true).await?;
        }
        if state.failure.is_some() {
            return Err(UNAVAILABLE);
        }
        request.headers = state.store.headers()?;
        request.generation = state.generation;
        Ok(())
    }

    /// Update a waiting request after cooldown without granting another 401 retry.
    pub(crate) async fn refresh_request(&self, request: &mut LoginRequest) -> Result<()> {
        let mut state = self.state.lock().await;
        self.renew(&mut state, false).await?;
        request.headers = state.store.headers()?;
        request.generation = state.generation;
        Ok(())
    }

    /// Every credential value held during this handle's lifetime, including rotations.
    /// The caller owns a zeroizing snapshot for response-header checks.
    pub async fn held_secrets(&self) -> SecretValue<Vec<String>> {
        self.history_state.borrow().clone()
    }

    fn check_fence(&self, state: &mut State) -> Result<()> {
        if self.fence.metadata().map_err(|_| UNAVAILABLE)?.len() != 0 {
            state.failure = Some(LoginHealth::LoginRequired);
            self.publish(state);
        }
        Ok(())
    }

    async fn renew(&self, state: &mut State, force: bool) -> Result<bool> {
        self.check_fence(state)?;
        if state.failure == Some(LoginHealth::LoginRequired)
            || (state.failure == Some(LoginHealth::RenewalFailed) && !state.retry_renewal)
        {
            return Err(UNAVAILABLE);
        }
        let at = now()?;
        if !force && !state.retry_renewal && state.store.expires_at > at.saturating_add(300) {
            if state.failure.is_some() {
                return Err(UNAVAILABLE);
            }
            return Ok(false);
        }
        let endpoint = match self
            .http
            .endpoint(state.store.family, self.max_secret_bytes)
            .await
        {
            Ok(endpoint) => endpoint,
            Err(_) => {
                state.failure = Some(LoginHealth::RenewalFailed);
                state.retry_renewal = true;
                self.publish(state);
                return Err(UNAVAILABLE);
            }
        };
        // Set the in-memory failure before the durable write: cancellation at any
        // later await cannot expose the superseded token or repeat the exchange.
        self.renewing
            .store(true, std::sync::atomic::Ordering::Release);
        let _renewing = Renewing(self);
        state.failure = Some(LoginHealth::LoginRequired);
        self.publish(state);
        set_fence(&self.fence, &self.path, true)?;
        let response = match self
            .http
            .exchange(&endpoint, &state.store, self.max_secret_bytes)
            .await
        {
            Ok(response) => response,
            Err(ExchangeError::BeforeSend) => {
                set_fence(&self.fence, &self.path, false)?;
                state.failure = Some(LoginHealth::RenewalFailed);
                state.retry_renewal = true;
                self.publish(state);
                return Err(UNAVAILABLE);
            }
            Err(ExchangeError::Uncertain) => return Err(UNAVAILABLE),
        };
        // Remember received tokens even if validation or installation fails.
        let mut updated = state.store.clone();
        updated.access_token = response.access_token;
        if let Some(token) = response.refresh_token {
            updated.refresh_token = token;
        }
        if let Some(token) = response.id_token {
            updated.id_token = Some(token);
        }
        self.history_state
            .send_modify(|history| updated.remember(history));
        updated.expires_at = response
            .expires_in
            .and_then(|seconds| at.checked_add(seconds))
            .or_else(|| jwt_exp(updated.access_token.text()))
            .filter(|expiry| *expiry > at)
            .ok_or(UNAVAILABLE)?;
        updated.validate(updated.family)?;
        state.store = updated;
        #[cfg(test)]
        if self.fail_install.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(UNAVAILABLE);
        }
        install(&self.path, &state.store, true, self.max_secret_bytes)?;
        set_fence(&self.fence, &self.path, false)?;
        state.generation = state.generation.checked_add(1).ok_or(UNAVAILABLE)?;
        state.failure = None;
        state.retry_renewal = false;
        self.publish(state);
        Ok(true)
    }
}

struct Renewing<'a>(&'a Login);
impl Drop for Renewing<'_> {
    fn drop(&mut self) {
        self.0
            .renewing
            .store(false, std::sync::atomic::Ordering::Release);
        self.0.notifications.notify_one();
    }
}

fn sibling(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(suffix);
    PathBuf::from(name)
}
fn parent(path: &Path) -> &Path {
    path.parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."))
}
fn private_file(path: &Path) -> Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC)
        .open(path)
        .map_err(|_| UNAVAILABLE)?;
    let meta = file.metadata().map_err(|_| UNAVAILABLE)?;
    // SAFETY: geteuid takes no arguments and retains no pointers.
    if !meta.is_file() || meta.uid() != unsafe { libc::geteuid() } || meta.mode() & 0o077 != 0 {
        return Err(UNAVAILABLE);
    }
    Ok(file)
}
fn lock(path: &Path) -> Result<crate::ProcessLock> {
    let file = private_file(&sibling(path, ".lock"))?;
    // SAFETY: descriptor is live; flock retains no pointers. Closing releases it.
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        return Err(UNAVAILABLE);
    }
    Ok(crate::ProcessLock(file))
}
fn set_fence(file: &File, path: &Path, pending: bool) -> Result<()> {
    if pending {
        let mut descriptor = file;
        descriptor
            .seek(SeekFrom::Start(0))
            .map_err(|_| UNAVAILABLE)?;
        descriptor.write_all(&[1]).map_err(|_| UNAVAILABLE)?;
    } else {
        file.set_len(0).map_err(|_| UNAVAILABLE)?;
    }
    file.sync_all().map_err(|_| UNAVAILABLE)?;
    if pending {
        File::open(parent(path))
            .and_then(|dir| dir.sync_all())
            .map_err(|_| UNAVAILABLE)?;
    }
    Ok(())
}
fn install(path: &Path, store: &Store, replace: bool, limit: usize) -> Result<()> {
    let bytes = SecretValue::new(serde_json::to_vec(store).map_err(|_| UNAVAILABLE)?);
    if bytes.len() > limit {
        return Err(UNAVAILABLE);
    }
    let temporary = sibling(path, &format!(".{}.tmp", uuid::Uuid::new_v4()));
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temporary)
            .map_err(|_| UNAVAILABLE)?;
        file.write_all(&bytes)
            .and_then(|()| file.sync_all())
            .map_err(|_| UNAVAILABLE)?;
        if replace {
            fs::rename(&temporary, path).map_err(|_| UNAVAILABLE)?;
        } else {
            // Atomic no-clobber installation, even if an unrelated writer ignores flock.
            fs::hard_link(&temporary, path).map_err(|_| UNAVAILABLE)?;
            fs::remove_file(&temporary).map_err(|_| UNAVAILABLE)?;
        }
        File::open(parent(path))
            .and_then(|dir| dir.sync_all())
            .map_err(|_| UNAVAILABLE)
    })();
    if temporary.exists() {
        fs::remove_file(temporary).map_err(|_| UNAVAILABLE)?;
    }
    result
}
fn valid_text(text: &str) -> bool {
    !text.is_empty() && !text.contains(['\r', '\n'])
}
fn now() -> Result<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .map_err(|_| UNAVAILABLE)
}
fn jwt_claims<T: serde::de::DeserializeOwned>(token: &str) -> Option<T> {
    let bytes = SecretValue::new(
        base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(token.split('.').nth(1)?)
            .ok()?,
    );
    serde_json::from_slice(&bytes).ok()
}
fn jwt_exp(token: &str) -> Option<u64> {
    #[derive(Deserialize)]
    struct Claims {
        exp: u64,
    }
    jwt_claims::<Claims>(token).map(|claims| claims.exp)
}

/// Import a native Codex auth.json, scoped Grok session, or Kimi Code login file.
/// Runs no provider tool or network request. Import clears an uncertain renewal.
/// Existing stores require `replace`; an active Login always refuses import.
pub fn import_native(
    family: LoginFamily,
    native: &Path,
    store: &Path,
    replace: bool,
    max_secret_bytes: usize,
) -> Result<()> {
    crate::process_security::require_protection().map_err(|_| UNAVAILABLE)?;
    if max_secret_bytes == 0 {
        return Err(UNAVAILABLE);
    }
    let _lock = lock(store)?;
    match fs::symlink_metadata(store) {
        Ok(_) if !replace => return Err(UNAVAILABLE),
        Ok(_) => {
            read_private_file(store, max_secret_bytes)?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
        Err(_) => return Err(UNAVAILABLE),
    }
    let bytes = read_private_file(native, max_secret_bytes)?;
    let credential = parse_native(family, &bytes)?;
    credential.validate(family)?;
    if credential.expires_at <= now()? {
        return Err(UNAVAILABLE);
    }
    let fence = private_file(&sibling(store, ".renewal"))?;
    set_fence(&fence, store, true)?;
    install(store, &credential, replace, max_secret_bytes)?;
    set_fence(&fence, store, false)
}

fn parse_native(family: LoginFamily, bytes: &[u8]) -> Result<Store> {
    #[derive(Deserialize)]
    struct Codex {
        tokens: CodexTokens,
        auth_mode: Option<String>,
    }
    #[derive(Deserialize)]
    struct CodexTokens {
        access_token: Token,
        refresh_token: Token,
        id_token: Token,
        account_id: Token,
    }
    #[derive(Deserialize)]
    struct Grok {
        auth_mode: String,
        oidc_issuer: String,
        oidc_client_id: String,
        key: Token,
        refresh_token: Token,
        expires_at: String,
        principal_type: Option<Token>,
        principal_id: Option<Token>,
    }
    let store = || Store {
        version: 1,
        family,
        issuer: family.issuer().into(),
        client_id: family.client_id().into(),
        access_token: Token(SecretValue::new(String::new())),
        refresh_token: Token(SecretValue::new(String::new())),
        id_token: None,
        account_id: None,
        principal_type: None,
        principal_id: None,
        expires_at: 0,
    };
    match family {
        LoginFamily::ChatgptCodex => {
            let native: Codex = serde_json::from_slice(bytes).map_err(|_| UNAVAILABLE)?;
            if native
                .auth_mode
                .as_deref()
                .is_some_and(|mode| mode != "chatgpt")
            {
                return Err(UNAVAILABLE);
            }
            #[derive(Deserialize)]
            struct Claims {
                iss: String,
                client_id: String,
                exp: u64,
            }
            let claims =
                jwt_claims::<Claims>(native.tokens.access_token.text()).ok_or(UNAVAILABLE)?;
            if claims.iss != family.issuer() || claims.client_id != family.client_id() {
                return Err(UNAVAILABLE);
            }
            let mut store = store();
            store.expires_at = claims.exp;
            store.access_token = native.tokens.access_token;
            store.refresh_token = native.tokens.refresh_token;
            store.id_token = Some(native.tokens.id_token);
            store.account_id = Some(native.tokens.account_id);
            Ok(store)
        }
        LoginFamily::KimiCode => {
            #[derive(Deserialize)]
            struct Kimi {
                access_token: Token,
                refresh_token: Token,
                expires_at: f64,
            }
            let native: Kimi = serde_json::from_slice(bytes).map_err(|_| UNAVAILABLE)?;
            // The CLI stores Unix seconds, including fractional values. Round down
            // into the shared store without accepting negative or overflowing times.
            if !native.expires_at.is_finite()
                || native.expires_at <= 0.0
                || native.expires_at >= u64::MAX as f64
            {
                return Err(UNAVAILABLE);
            }
            let mut store = store();
            store.expires_at = native.expires_at.floor() as u64;
            store.access_token = native.access_token;
            store.refresh_token = native.refresh_token;
            Ok(store)
        }
        LoginFamily::XaiGrok => {
            #[derive(Deserialize)]
            struct GrokFile {
                #[serde(rename = "https://auth.x.ai::b1a00492-073a-47ea-816f-4c329264a828")]
                session: Grok,
            }
            let token = serde_json::from_slice::<GrokFile>(bytes)
                .map_err(|_| UNAVAILABLE)?
                .session;
            if token.auth_mode != "oidc"
                || token.oidc_issuer != family.issuer()
                || token.oidc_client_id != family.client_id()
            {
                return Err(UNAVAILABLE);
            }
            let mut store = store();
            store.expires_at = u64::try_from(
                chrono::DateTime::parse_from_rfc3339(&token.expires_at)
                    .map_err(|_| UNAVAILABLE)?
                    .timestamp(),
            )
            .map_err(|_| UNAVAILABLE)?;
            store.access_token = token.key;
            store.refresh_token = token.refresh_token;
            store.principal_type = token.principal_type;
            store.principal_id = token.principal_id;
            Ok(store)
        }
    }
}

#[derive(Deserialize)]
struct Renewed {
    access_token: Token,
    refresh_token: Option<Token>,
    id_token: Option<Token>,
    expires_in: Option<u64>,
}
enum ExchangeError {
    BeforeSend,
    Uncertain,
}
struct Http {
    client: Client,
    #[cfg(test)]
    wire_base: Option<String>,
}
impl Http {
    fn new() -> Result<Self> {
        Ok(Self {
            client: Client::builder()
                .https_only(true)
                .retry(reqwest::retry::never())
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(Duration::from_secs(30))
                .build()
                .map_err(|_| UNAVAILABLE)?,
            #[cfg(test)]
            wire_base: None,
        })
    }
    fn allowed(url: &str, family: LoginFamily) -> Result<Url> {
        let url = Url::parse(url).map_err(|_| UNAVAILABLE)?;
        let issuer = Url::parse(family.issuer()).map_err(|_| UNAVAILABLE)?;
        if url.scheme() != "https"
            || url.host_str() != issuer.host_str()
            || url.port_or_known_default() != Some(443)
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
        {
            return Err(UNAVAILABLE);
        }
        Ok(url)
    }
    fn wire_url(&self, url: Url) -> Url {
        #[cfg(test)]
        if let Some(base) = &self.wire_base {
            return Url::parse(&format!("{base}{}", url.path())).expect("fixture URL");
        }
        url
    }
    async fn endpoint(&self, family: LoginFamily, limit: usize) -> Result<Url> {
        let path = match family {
            LoginFamily::ChatgptCodex => Some("/oauth/token"),
            LoginFamily::KimiCode => Some("/api/oauth/token"),
            LoginFamily::XaiGrok => None,
        };
        if let Some(path) = path {
            return Self::allowed(&format!("{}{path}", family.issuer()), family);
        }
        let discovery = Self::allowed(
            &format!("{}/.well-known/openid-configuration", family.issuer()),
            family,
        )?;
        let response = self
            .client
            .get(self.wire_url(discovery))
            .send()
            .await
            .map_err(|_| UNAVAILABLE)?;
        let bytes = body(response, limit).await?;
        #[derive(Deserialize)]
        struct Discovery {
            token_endpoint: String,
        }
        let document: Discovery = serde_json::from_slice(&bytes).map_err(|_| UNAVAILABLE)?;
        Self::allowed(&document.token_endpoint, family)
    }
    async fn exchange(
        &self,
        endpoint: &Url,
        store: &Store,
        limit: usize,
    ) -> std::result::Result<Renewed, ExchangeError> {
        #[derive(Serialize)]
        struct Grant<'a> {
            grant_type: &'static str,
            client_id: &'a str,
            refresh_token: &'a Token,
            #[serde(skip_serializing_if = "Option::is_none")]
            principal_type: &'a Option<Token>,
            #[serde(skip_serializing_if = "Option::is_none")]
            principal_id: &'a Option<Token>,
        }
        let grant = Grant {
            grant_type: "refresh_token",
            client_id: &store.client_id,
            refresh_token: &store.refresh_token,
            principal_type: &store.principal_type,
            principal_id: &store.principal_id,
        };
        let builder = self.client.post(self.wire_url(endpoint.clone()));
        let builder = if store.family == LoginFamily::KimiCode {
            builder.header("accept", "application/json")
        } else {
            builder
        };
        let builder = if store.family == LoginFamily::ChatgptCodex {
            builder
                .json(&grant)
                .header("originator", "codex_cli_rs")
                .header("accept", "*/*")
                .header(
                    "user-agent",
                    format!(
                        "codex_cli_rs/0.159.1 ({}; {}) symbiotic-credential-process",
                        std::env::consts::OS,
                        std::env::consts::ARCH
                    ),
                )
        } else {
            // URL query pairs use the same application/x-www-form-urlencoded
            // encoder. Reuse it without enabling reqwest's optional form crate.
            let mut encoded =
                Url::parse("https://form.invalid/").map_err(|_| ExchangeError::BeforeSend)?;
            {
                let mut pairs = encoded.query_pairs_mut();
                pairs
                    .append_pair("grant_type", grant.grant_type)
                    .append_pair("client_id", grant.client_id)
                    .append_pair("refresh_token", grant.refresh_token.text());
                if let Some(value) = grant.principal_type {
                    pairs.append_pair("principal_type", value.text());
                }
                if let Some(value) = grant.principal_id {
                    pairs.append_pair("principal_id", value.text());
                }
            }
            let encoded = SecretValue::new(String::from(encoded));
            let (_, form) = encoded.split_once('?').ok_or(ExchangeError::BeforeSend)?;
            builder
                .header("content-type", "application/x-www-form-urlencoded")
                .body(form.to_owned())
        };
        let response = builder.send().await.map_err(|error| {
            if error.is_connect() || error.is_builder() {
                ExchangeError::BeforeSend
            } else {
                ExchangeError::Uncertain
            }
        })?;
        let bytes = body(response, limit)
            .await
            .map_err(|_| ExchangeError::Uncertain)?;
        serde_json::from_slice(&bytes).map_err(|_| ExchangeError::Uncertain)
    }
}
async fn body(mut response: reqwest::Response, limit: usize) -> Result<SecretValue<Vec<u8>>> {
    // Never read or log provider error bodies; they may echo credentials.
    if !response.status().is_success() {
        return Err(UNAVAILABLE);
    }
    let mut bytes = SecretValue::new(Vec::new());
    while let Some(chunk) = response.chunk().await.map_err(|_| UNAVAILABLE)? {
        if chunk.len() > limit.saturating_sub(bytes.len()) {
            return Err(UNAVAILABLE);
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    const LIMIT: usize = 1024 * 1024;
    struct Fixture {
        base: String,
        requests: Arc<std::sync::Mutex<Vec<(String, Value)>>>,
        exchanges: Arc<AtomicUsize>,
        task: tokio::task::JoinHandle<()>,
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            self.task.abort();
        }
    }
    impl Fixture {
        async fn start(status: u16, discovery: &str) -> Self {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let base = format!("http://{}", listener.local_addr().unwrap());
            let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
            let exchanges = Arc::new(AtomicUsize::new(0));
            let captured = requests.clone();
            let counter = exchanges.clone();
            let discovery = discovery.to_owned();
            let task = tokio::spawn(async move {
                loop {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    let mut bytes = Vec::new();
                    let (head, length) = loop {
                        let byte = socket.read_u8().await.unwrap();
                        bytes.push(byte);
                        if bytes.ends_with(b"\r\n\r\n") {
                            let head = String::from_utf8(bytes.clone()).unwrap();
                            let length = head
                                .lines()
                                .find_map(|line| {
                                    line.to_ascii_lowercase()
                                        .strip_prefix("content-length: ")
                                        .map(|n| n.parse::<usize>().unwrap())
                                })
                                .unwrap_or(0);
                            break (head, length);
                        }
                    };
                    let mut body = vec![0; length];
                    socket.read_exact(&mut body).await.unwrap();
                    let is_discovery = head.starts_with("GET ");
                    let value = if body.is_empty() {
                        Value::Null
                    } else if head
                        .to_ascii_lowercase()
                        .contains("content-type: application/json")
                    {
                        serde_json::from_slice(&body).unwrap()
                    } else {
                        json!(String::from_utf8(body).unwrap())
                    };
                    captured.lock().unwrap().push((head, value));
                    let (code, response) = if is_discovery {
                        (200, json!({"token_endpoint":discovery}).to_string())
                    } else {
                        counter.fetch_add(1, Ordering::SeqCst);
                        (
                            status,
                            if status == 200 {
                                json!({"access_token":"new-access","refresh_token":"new-refresh","id_token":"new-id","expires_in":3600}).to_string()
                            } else {
                                "synthetic secret error body".into()
                            },
                        )
                    };
                    let reply = format!(
                        "HTTP/1.1 {code} fixture\r\nContent-Length: {}\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{response}",
                        response.len()
                    );
                    socket.write_all(reply.as_bytes()).await.unwrap();
                }
            });
            Self {
                base,
                requests,
                exchanges,
                task,
            }
        }
        fn count(&self) -> usize {
            self.exchanges.load(Ordering::SeqCst)
        }
    }
    fn token(value: &str) -> Token {
        Token(SecretValue::new(value.to_owned()))
    }
    fn stored(family: LoginFamily, expiry: u64) -> Store {
        Store {
            version: 1,
            family,
            issuer: family.issuer().into(),
            client_id: family.client_id().into(),
            access_token: token("old-access"),
            refresh_token: token(if family == LoginFamily::XaiGrok {
                "old-refresh+&= /é"
            } else {
                "old-refresh"
            }),
            id_token: Some(token("old-id")),
            account_id: (family == LoginFamily::ChatgptCodex).then(|| token("account")),
            principal_type: (family == LoginFamily::XaiGrok).then(|| token("user")),
            principal_id: (family == LoginFamily::XaiGrok).then(|| token("principal")),
            expires_at: expiry,
        }
    }
    fn login(dir: &Path, family: LoginFamily, expiry: u64, fixture: &Fixture) -> Login {
        crate::protect_process().unwrap();
        let path = dir.join("login");
        install(&path, &stored(family, expiry), false, LIMIT).unwrap();
        let mut login = Login::open(&path, family, LIMIT).unwrap();
        login.http = Http {
            client: Client::builder()
                .retry(reqwest::retry::never())
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            wire_base: Some(fixture.base.clone()),
        };
        login
    }

    #[tokio::test]
    async fn kimi_expired_login_renews_once_and_reopens_with_rotated_tokens() {
        let family: LoginFamily = serde_json::from_str("\"kimi_code\"").unwrap();
        let fixture = Fixture::start(200, "https://auth.x.ai/token").await;
        let dir = tempfile::tempdir().unwrap();
        let login = login(dir.path(), family, now().unwrap() - 1, &fixture);
        for _ in 0..2 {
            let request = login.request().await.unwrap();
            assert_eq!(request.headers()[AUTHORIZATION], "Bearer new-access");
            assert_eq!(request.headers().len(), 1);
            let diagnostic = format!("{:?}", request.headers());
            for secret in ["old-access", "old-refresh", "new-access", "new-refresh"] {
                assert!(!diagnostic.contains(secret));
            }
        }
        assert_eq!(fixture.count(), 1);
        {
            let requests = fixture.requests.lock().unwrap();
            assert_eq!(requests.len(), 1);
            let (headers, body) = &requests[0];
            assert!(headers.starts_with("POST /api/oauth/token HTTP/1.1\r\n"));
            assert!(headers.contains("content-type: application/x-www-form-urlencoded\r\n"));
            assert!(headers.contains("accept: application/json\r\n"));
            assert!(!headers.contains("authorization:"));
            assert_eq!(
                body,
                &json!(
                    "grant_type=refresh_token&client_id=17e5f671-d194-4dfb-9706-5516cb48c098&refresh_token=old-refresh"
                )
            );
        }
        let held = login.held_secrets().await;
        for secret in ["old-access", "old-refresh", "new-access", "new-refresh"] {
            assert!(held.iter().any(|value| value == secret));
        }
        drop(login);
        let reopened = Login::open(&dir.path().join("login"), family, LIMIT).unwrap();
        assert_eq!(
            reopened.request().await.unwrap().headers()[AUTHORIZATION],
            "Bearer new-access"
        );
        assert_eq!(fixture.count(), 1);
    }

    #[tokio::test]
    async fn kimi_renewal_failure_requires_login_without_secret_diagnostics() {
        let family: LoginFamily = serde_json::from_str("\"kimi_code\"").unwrap();
        for status in [400, 401, 500, 302] {
            let fixture = Fixture::start(status, "https://auth.x.ai/token").await;
            let dir = tempfile::tempdir().unwrap();
            let login = login(dir.path(), family, now().unwrap() - 1, &fixture);
            let error = login.request().await.err().unwrap();
            assert_eq!(error, EgressError::CredentialUnavailable);
            let diagnostic = format!("{error:?}: {error}");
            for secret in ["old-access", "old-refresh", "synthetic secret error body"] {
                assert!(!diagnostic.contains(secret));
            }
            assert_eq!(login.health().await.unwrap().0, LoginHealth::LoginRequired);
            assert!(login.request().await.is_err());
            assert_eq!(fixture.count(), 1);
            drop(login);
            let reopened = Login::open(&dir.path().join("login"), family, LIMIT).unwrap();
            assert_eq!(
                reopened.health().await.unwrap().0,
                LoginHealth::LoginRequired
            );
            assert!(reopened.request().await.is_err());
        }
    }

    #[tokio::test]
    async fn ten_expiring_requests_share_one_exchange() {
        let fixture = Fixture::start(200, "https://auth.x.ai/token").await;
        let dir = tempfile::tempdir().unwrap();
        let login = Arc::new(login(
            dir.path(),
            LoginFamily::ChatgptCodex,
            now().unwrap() + 60,
            &fixture,
        ));
        let barrier = Arc::new(tokio::sync::Barrier::new(10));
        let mut tasks = Vec::new();
        for _ in 0..10 {
            let login = login.clone();
            let barrier = barrier.clone();
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                login.request().await.unwrap().headers()[AUTHORIZATION].clone()
            }));
        }
        for task in tasks {
            assert_eq!(task.await.unwrap(), "Bearer new-access");
        }
        assert_eq!(fixture.count(), 1);
        assert_eq!(login.health().await.unwrap().0, LoginHealth::Expiring);
    }

    #[tokio::test]
    async fn exchanged_but_uninstalled_rotation_requires_import() {
        let fixture = Fixture::start(200, "https://auth.x.ai/token").await;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("login");
        let login = login(
            dir.path(),
            LoginFamily::ChatgptCodex,
            now().unwrap() + 60,
            &fixture,
        );
        login.fail_install.store(true, Ordering::Relaxed);
        assert!(login.request().await.is_err());
        assert_eq!(login.health().await.unwrap().0, LoginHealth::LoginRequired);
        let held = login.held_secrets().await;
        for secret in [
            "old-access",
            "old-refresh",
            "new-access",
            "new-refresh",
            "new-id",
            "account",
        ] {
            assert!(held.iter().any(|v| v == secret));
        }
        assert!(fs::metadata(sibling(&path, ".renewal")).unwrap().len() > 0);
        drop(login);
        let reopened = Login::open(&path, LoginFamily::ChatgptCodex, LIMIT).unwrap();
        assert_eq!(
            reopened.health().await.unwrap().0,
            LoginHealth::LoginRequired
        );
        assert!(reopened.request().await.is_err());
        assert_eq!(fixture.count(), 1);
        drop(reopened);
        let native = dir.path().join("auth.json");
        let access = format!(
            "synthetic.{}.sig",
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .encode(serde_json::to_vec(&json!({"exp":now().unwrap()+3600,"iss":"https://auth.openai.com","client_id":CODEX_CLIENT})).unwrap())
        );
        fs::write(&native, serde_json::to_vec(&json!({"tokens":{"access_token":access,"refresh_token":"fresh-refresh","id_token":"fresh-id","account_id":"account"}})).unwrap()).unwrap();
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&native, fs::Permissions::from_mode(0o600)).unwrap();
        import_native(LoginFamily::ChatgptCodex, &native, &path, true, LIMIT).unwrap();
        let reopened = Login::open(&path, LoginFamily::ChatgptCodex, LIMIT).unwrap();
        assert_eq!(
            reopened.request().await.unwrap().headers()[AUTHORIZATION],
            format!("Bearer {access}")
        );
        assert_eq!(fixture.count(), 1);
    }

    #[tokio::test]
    async fn rotation_is_persisted_and_all_secrets_remain_available() {
        let fixture = Fixture::start(200, "https://auth.x.ai/token").await;
        let dir = tempfile::tempdir().unwrap();
        let login = login(
            dir.path(),
            LoginFamily::ChatgptCodex,
            now().unwrap() + 60,
            &fixture,
        );
        let request = login.request().await.unwrap();
        assert_eq!(request.headers()["chatgpt-account-id"], "account");
        assert_eq!(request.headers()["originator"], "codex_cli_rs");
        let bytes = read_private_file(&login.path, LIMIT).unwrap();
        let record: Store = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(record.refresh_token.text(), "new-refresh");
        assert_eq!(record.access_token.text(), "new-access");
        assert_eq!(record.id_token.as_ref().unwrap().text(), "new-id");
        assert_eq!(
            fs::metadata(sibling(&login.path, ".renewal"))
                .unwrap()
                .len(),
            0
        );
        let mut held = (*login.held_secrets().await).clone();
        held.sort();
        assert_eq!(
            held,
            [
                "account",
                "new-access",
                "new-id",
                "new-refresh",
                "old-access",
                "old-id",
                "old-refresh"
            ]
        );
        drop(login);
        let reopened =
            Login::open(&dir.path().join("login"), LoginFamily::ChatgptCodex, LIMIT).unwrap();
        assert_eq!(
            reopened.request().await.unwrap().headers()[AUTHORIZATION],
            "Bearer new-access"
        );
    }

    #[tokio::test]
    async fn unauthorized_forces_once_and_concurrent_stale_requests_adopt() {
        let fixture = Fixture::start(200, "https://auth.x.ai/token").await;
        let dir = tempfile::tempdir().unwrap();
        let login = login(
            dir.path(),
            LoginFamily::ChatgptCodex,
            now().unwrap() + 3600,
            &fixture,
        );
        let mut first = login.request().await.unwrap();
        let mut second = login.request().await.unwrap();
        assert_eq!(fixture.count(), 0);
        login.after_unauthorized(&mut first).await.unwrap();
        login.after_unauthorized(&mut second).await.unwrap();
        assert_eq!(first.headers()[AUTHORIZATION], "Bearer new-access");
        assert_eq!(second.headers()[AUTHORIZATION], "Bearer new-access");
        assert!(login.after_unauthorized(&mut first).await.is_err());
        assert_eq!(fixture.count(), 1);
        assert_eq!(login.health().await.unwrap().0, LoginHealth::RenewalFailed);
        assert!(login.request().await.is_err());
    }

    #[tokio::test]
    async fn provider_answered_failure_fences_the_account() {
        for status in [400, 401, 500, 302] {
            let fixture = Fixture::start(status, "https://auth.x.ai/token").await;
            let dir = tempfile::tempdir().unwrap();
            let login = login(
                dir.path(),
                LoginFamily::ChatgptCodex,
                now().unwrap() + 60,
                &fixture,
            );
            let error = login.request().await.err().unwrap();
            assert_eq!(error, EgressError::CredentialUnavailable);
            assert!(!error.to_string().contains("synthetic"));
            assert_eq!(login.health().await.unwrap().0, LoginHealth::LoginRequired);
            assert!(login.request().await.is_err());
            assert_eq!(fixture.count(), 1);
        }
    }

    #[tokio::test]
    async fn connection_refusal_clears_fence_and_next_request_retries() {
        let fixture = Fixture::start(200, "https://auth.x.ai/token").await;
        let dir = tempfile::tempdir().unwrap();
        let mut login = login(
            dir.path(),
            LoginFamily::ChatgptCodex,
            now().unwrap() + 3600,
            &fixture,
        );
        let mut request = login.request().await.unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let closed = format!("http://{}", listener.local_addr().unwrap());
        drop(listener);
        login.http.wire_base = Some(closed);
        assert!(login.after_unauthorized(&mut request).await.is_err());
        assert_eq!(login.health().await.unwrap().0, LoginHealth::RenewalFailed);
        assert_eq!(
            fs::metadata(sibling(&login.path, ".renewal"))
                .unwrap()
                .len(),
            0
        );
        login.http.wire_base = Some(fixture.base.clone());
        assert_eq!(
            login.request().await.unwrap().headers()[AUTHORIZATION],
            "Bearer new-access"
        );
        assert_eq!(fixture.count(), 1);
    }

    #[tokio::test]
    async fn codex_renewal_sends_json_and_pinned_identity() {
        let fixture = Fixture::start(200, "https://auth.x.ai/token").await;
        let dir = tempfile::tempdir().unwrap();
        let login = login(
            dir.path(),
            LoginFamily::ChatgptCodex,
            now().unwrap() + 60,
            &fixture,
        );
        login.request().await.unwrap();
        let requests = fixture.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        let (headers, body) = &requests[0];
        assert!(headers.starts_with("POST /oauth/token HTTP/1.1\r\n"));
        assert!(headers.contains("content-type: application/json\r\n"));
        assert!(headers.contains("originator: codex_cli_rs\r\n"));
        assert!(headers.contains("user-agent: codex_cli_rs/0.159.1"));
        assert!(!headers.contains("authorization:"));
        assert_eq!(
            body,
            &json!({"grant_type":"refresh_token","client_id":CODEX_CLIENT,"refresh_token":"old-refresh"})
        );
    }

    #[tokio::test]
    async fn grok_discovers_exact_host_then_sends_form_and_family_headers() {
        let fixture = Fixture::start(200, "https://auth.x.ai/token").await;
        let dir = tempfile::tempdir().unwrap();
        let login = login(
            dir.path(),
            LoginFamily::XaiGrok,
            now().unwrap() + 60,
            &fixture,
        );
        assert_eq!(
            login.request().await.unwrap().headers()["x-xai-token-auth"],
            "xai-grok-cli"
        );
        let requests = fixture.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert!(
            requests[0]
                .0
                .starts_with("GET /.well-known/openid-configuration HTTP/1.1")
        );
        assert_eq!(requests[0].1, Value::Null);
        assert!(requests[1].0.starts_with("POST /token HTTP/1.1"));
        assert!(
            requests[1]
                .0
                .contains("content-type: application/x-www-form-urlencoded\r\n")
        );
        assert_eq!(
            requests[1].1,
            json!(format!(
                "grant_type=refresh_token&client_id={GROK_CLIENT}&refresh_token=old-refresh%2B%26%3D+%2F%C3%A9&principal_type=user&principal_id=principal"
            ))
        );
    }

    #[tokio::test]
    async fn discovery_refuses_untrusted_destinations_before_sending_refresh_token() {
        for endpoint in [
            "https://evil.invalid/token",
            "https://auth.x.ai.evil.invalid/token",
            "http://auth.x.ai/token",
            "https://auth.x.ai:444/token",
            "https://user@auth.x.ai/token",
            "https://auth.x.ai/token#fragment",
        ] {
            let fixture = Fixture::start(200, endpoint).await;
            let dir = tempfile::tempdir().unwrap();
            let login = login(
                dir.path(),
                LoginFamily::XaiGrok,
                now().unwrap() + 60,
                &fixture,
            );
            assert!(login.request().await.is_err(), "accepted {endpoint}");
            assert_eq!(fixture.count(), 0);
            assert_eq!(login.health().await.unwrap().0, LoginHealth::RenewalFailed);
            assert_eq!(
                fs::metadata(sibling(&login.path, ".renewal"))
                    .unwrap()
                    .len(),
                0
            );
        }
    }

    #[tokio::test]
    async fn passthrough_contract_terminal_unauthorized_refusal_requires_import_after_reopen() {
        crate::protect_process().unwrap();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("login");
        install(
            &path,
            &stored(LoginFamily::ChatgptCodex, now().unwrap() + 3600),
            false,
            LIMIT,
        )
        .unwrap();
        let mut login = Login::open(&path, LoginFamily::ChatgptCodex, LIMIT).unwrap();
        // The real connector targets an absent socket in our own directory.
        // It cannot send a token exchange, so the first failure must clear the fence.
        login.http.client = Client::builder()
            .no_proxy()
            .unix_socket(directory.path().join("absent.sock"))
            .timeout(Duration::from_millis(100))
            .retry(reqwest::retry::never())
            .build()
            .unwrap();
        login.http.wire_base = Some("http://localhost".into());
        let mut request = login.request().await.unwrap();
        assert!(login.after_unauthorized(&mut request).await.is_err());
        assert!(fs::read(sibling(&path, ".renewal")).unwrap().is_empty());
        assert!(login.after_unauthorized(&mut request).await.is_err());
        assert_eq!(login.health().await.unwrap().0, LoginHealth::RenewalFailed);
        drop(login);
        let reopened = Login::open(&path, LoginFamily::ChatgptCodex, LIMIT).unwrap();
        assert_eq!(
            reopened.health().await.unwrap().0,
            LoginHealth::LoginRequired
        );
        assert!(reopened.request().await.is_err());
    }

    #[tokio::test]
    async fn terminal_unauthorized_fence_write_failure_still_refuses_new_requests() {
        crate::protect_process().unwrap();
        for seconds in [60, 3600] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("login");
            install(
                &path,
                &stored(LoginFamily::ChatgptCodex, now().unwrap() + seconds),
                false,
                LIMIT,
            )
            .unwrap();
            let mut login = Login::open(&path, LoginFamily::ChatgptCodex, LIMIT).unwrap();
            // Start at the second 401 of an already-retried request. This test
            // targets refusal persistence, independent of the exchange fixture.
            let mut request = LoginRequest {
                headers: login.state.lock().await.store.headers().unwrap(),
                generation: 0,
                retried: true,
            };
            // Real filesystem I/O fails: the fence descriptor cannot write.
            login.fence = File::open(sibling(&path, ".renewal")).unwrap();
            assert_eq!(
                login.after_unauthorized(&mut request).await,
                Err(UNAVAILABLE)
            );
            assert_eq!(login.health().await.unwrap().0, LoginHealth::RenewalFailed);
            assert!(login.request().await.is_err());
            assert_eq!(login.health().await.unwrap().0, LoginHealth::RenewalFailed);
        }
    }

    #[tokio::test]
    async fn health_distinguishes_two_day_expiry_without_renewal() {
        crate::protect_process().unwrap();
        for (seconds, expected) in [
            (3 * 24 * 60 * 60, LoginHealth::Ok),
            (2 * 24 * 60 * 60, LoginHealth::Expiring),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("login");
            let expiry = now().unwrap() + seconds;
            install(
                &path,
                &stored(LoginFamily::ChatgptCodex, expiry),
                false,
                LIMIT,
            )
            .unwrap();
            // Health reads only the local store; it needs no provider fixture.
            let login = Login::open(&path, LoginFamily::ChatgptCodex, LIMIT).unwrap();
            let health = login.health().await.unwrap();
            assert_eq!(health, (expected, expiry));
            assert_eq!(
                serde_json::to_value(health.0).unwrap(),
                match expected {
                    LoginHealth::Ok => json!("ok"),
                    _ => json!("expiring"),
                }
            );
        }
    }
}
