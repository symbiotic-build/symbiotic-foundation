//! Request records, provider-unit meters and persisted quota observations.
use crate::{GatewayConfig, GatewayError, outbox::Outbox};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::UnixStream,
    time::Instant,
};

type Result<T> = std::result::Result<T, GatewayError>;

const USAGE_READ_INTERVAL: Duration = Duration::from_secs(300);

/// A route-owned read pairing each observation with its original window length in seconds.
/// Credentials and transport stay with the route; polling performs one read.
pub(crate) type UsageRead<'a> = Pin<
    Box<dyn Future<Output = std::result::Result<Vec<(Reading, u64)>, &'static str>> + Send + 'a>,
>;

#[derive(Clone, Default, Serialize, Deserialize)]
pub(crate) struct Reading {
    pub name: String,
    pub unit: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub used: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remaining: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub window_minutes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resets_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub read_at: Option<DateTime<Utc>>,
}
#[derive(Default, Serialize, Deserialize)]
pub(crate) struct Meter {
    pub readings: Vec<Reading>,
    pub provider: HashMap<String, String>,
}
impl Meter {
    pub fn headers(headers: &reqwest::header::HeaderMap) -> Self {
        let provider: HashMap<String, String> = headers
            .iter()
            .filter_map(|(name, value)| {
                crate::passthrough::rate_header(name.as_str())
                    .then(|| {
                        value
                            .to_str()
                            .ok()
                            .map(|v| (name.to_string(), v.to_owned()))
                    })
                    .flatten()
            })
            .collect();
        let mut readings = Vec::new();
        for window in ["primary", "secondary"] {
            let suffix = format!("-{window}-");
            let mut families = std::collections::BTreeSet::new();
            for key in provider.keys() {
                for field in ["used-percent", "window-minutes", "reset-at"] {
                    if let Some(family) = key
                        .strip_prefix("x-")
                        .and_then(|s| s.strip_suffix(&format!("{suffix}{field}")))
                    {
                        families.insert(family.to_owned());
                    }
                }
            }
            for family in families {
                let get = |field: &str| provider.get(&format!("x-{family}-{window}-{field}"));
                let window_minutes = get("window-minutes").and_then(|v| v.parse().ok());
                readings.push(Reading {
                    name: format!("{family}.{window}"),
                    unit: "percent".into(),
                    used: get("used-percent").and_then(|v| numeric(v)),
                    window_minutes,
                    resets_at: get("reset-at")
                        .and_then(|v| v.parse().ok())
                        .and_then(|s| DateTime::from_timestamp(s, 0)),
                    limit: (family == "codex" && window_minutes == Some(10080)).then_some(100.0),
                    ..Default::default()
                });
            }
        }
        if let Some(balance) = provider
            .get("x-codex-credits-balance")
            .and_then(|v| numeric(v))
        {
            readings.push(Reading {
                name: "codex.credits".into(),
                unit: "credits".into(),
                remaining: Some(balance),
                ..Default::default()
            });
        }
        Self { readings, provider }
    }
}
fn numeric(value: &str) -> Option<f64> {
    value.parse::<f64>().ok().filter(|v| v.is_finite())
}

#[derive(Serialize)]
pub(crate) struct Usage {
    normalized: HashMap<&'static str, u64>,
    provider: Value,
}
impl Usage {
    pub fn new(provider: &Value, trace: &symbiotic_trace::UsageTrace) -> Option<Self> {
        if provider.is_null() {
            return None;
        }
        let normalized = [
            ("input_tokens", trace.input_tokens),
            (
                "cached_input_tokens",
                trace.cache_hit_tokens.filter(|_| {
                    [
                        "/input_tokens_details/cached_tokens",
                        "/prompt_tokens_details/cached_tokens",
                        "/prompt_cache_hit_tokens",
                    ]
                    .iter()
                    .any(|path| provider.pointer(path).is_some_and(|v| !v.is_null()))
                }),
            ),
            ("output_tokens", trace.output_tokens),
            ("reasoning_tokens", trace.reasoning_tokens),
        ]
        .into_iter()
        .filter_map(|(key, value)| value.map(|v| (key, v)))
        .collect();
        Some(Self {
            normalized,
            provider: provider.clone(),
        })
    }
}
#[derive(Serialize)]
pub(crate) struct RequestRecord {
    pub request_id: String,
    pub job_id: String,
    pub account: String,
    pub route: String,
    pub model: String,
    pub status: u16,
    pub time: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refused: Option<&'static str>,
    pub usage: Option<Usage>,
    pub meter: Meter,
}

#[derive(Deserialize)]
struct Budget {
    account: String,
    name: String,
    limit: f64,
    spent: f64,
    state: String,
    over_limit_allowed: bool,
    reading: Reading,
    updated: DateTime<Utc>,
}
#[derive(Serialize, Deserialize)]
struct Observation {
    reading: Reading,
    time: DateTime<Utc>,
    exhausted: bool,
    allowed: bool,
}
impl Observation {
    fn at(&self) -> DateTime<Utc> {
        self.reading.read_at.unwrap_or(self.time)
    }
}
#[derive(Default, Serialize, Deserialize)]
struct Sources {
    local: Option<Observation>,
    core: Option<Observation>,
    #[serde(skip)]
    closed: bool,
}
impl Sources {
    fn newest(&self) -> Option<&Observation> {
        match (&self.local, &self.core) {
            (Some(local), Some(core)) if local.at() > core.at() => Some(local),
            (_, Some(core)) => Some(core),
            (local, _) => local.as_ref(),
        }
    }
    fn closed(&mut self, account: &str, name: &str, now: DateTime<Utc>) -> bool {
        let newest = self.newest();
        // An allowance answers one window; it lapses with that window's reset.
        let reason = if self
            .core
            .as_ref()
            .is_some_and(|s| s.allowed && s.reading.resets_at.is_none_or(|t| t > now))
        {
            "over_limit_allowed"
        } else if newest.is_some_and(|s| s.reading.resets_at.is_some_and(|t| t <= now)) {
            "window_reset"
        } else if newest.is_some_and(|s| s.exhausted) {
            "quota_exhausted"
        } else {
            "below_limit"
        };
        let closed = reason == "quota_exhausted";
        if self.closed != closed {
            tracing::info!(
                account,
                reading = name,
                reason,
                closed,
                "quota gate changed"
            );
            self.closed = closed;
        }
        closed
    }
}
#[derive(Default)]
struct AccountGate {
    readings: HashMap<String, Sources>,
    last_refresh: Option<Instant>,
    last_usage_read: Option<Instant>,
}
impl AccountGate {
    fn closed(&mut self, account: &str, now: DateTime<Utc>) -> bool {
        // Only Codex readings gate requests (operator decision 5, 2026-11-01); other budgets
        // warn through the core. Evaluate every gated reading so every transition is logged.
        self.readings
            .iter_mut()
            .filter(|(name, _)| name.starts_with("codex."))
            .fold(false, |closed, (name, sources)| {
                sources.closed(account, name, now) || closed
            })
    }
}
struct Account {
    gate: Mutex<AccountGate>,
    refresh: tokio::sync::Mutex<()>,
}
pub(crate) struct Records {
    pub outbox: Outbox,
    core_path: PathBuf,
    core_timeout: Duration,
    accounts: HashMap<String, Account>,
    pub clock: symbiotic_queue::jobs::JobClock,
}
impl Records {
    pub fn open(
        config: &GatewayConfig,
        clock: symbiotic_queue::jobs::JobClock,
    ) -> Result<Arc<Self>> {
        let outbox = Outbox::open(&config.state_dir)?;
        let saved: HashMap<String, String> = outbox.load_gates()?.into_iter().collect();
        let accounts = config
            .passthrough
            .routes
            .iter()
            .map(|route| {
                Ok((
                    route.record_account(),
                    Account {
                        gate: Mutex::new(AccountGate {
                            readings: saved
                                .get(&route.record_account())
                                .map(|body| serde_json::from_str(body))
                                .transpose()
                                .map_err(|_| GatewayError::StateUnavailable)?
                                .unwrap_or_default(),
                            last_refresh: None,
                            last_usage_read: None,
                        }),
                        refresh: tokio::sync::Mutex::new(()),
                    },
                ))
            })
            .collect::<Result<_>>()?;
        Ok(Arc::new(Self {
            outbox,
            core_path: config.core_socket_path.clone(),
            core_timeout: Duration::from_secs(config.io_timeout_seconds),
            accounts,
            clock,
        }))
    }
    pub fn observe(&self, account: &str, readings: &[Reading], time: DateTime<Utc>) {
        let Some(account_gate) = self.accounts.get(account) else {
            return;
        };
        let mut gate = account_gate
            .gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut updated = false;
        for reading in readings
            .iter()
            .filter(|r| r.limit.is_some() || r.name == "deepseek.balance")
        {
            let sources = gate.readings.entry(reading.name.clone()).or_default();
            let observation = Observation {
                reading: reading.clone(),
                time,
                exhausted: reading
                    .used
                    .zip(reading.limit)
                    .is_some_and(|(used, limit)| used >= limit),
                allowed: false,
            };
            if sources
                .local
                .as_ref()
                .is_none_or(|old| observation.at() >= old.at())
            {
                sources.local = Some(observation);
                updated = true;
            }
        }
        if updated {
            self.save_gate(account, &gate);
        }
        gate.closed(account, (self.clock)());
    }
    fn save_gate(&self, account: &str, gate: &AccountGate) {
        match serde_json::to_string(&gate.readings) {
            Ok(body) => self.outbox.save_gate(account, body),
            Err(_) => tracing::error!(
                code = "gate_write_failed",
                account,
                "quota gate encoding failed"
            ),
        }
    }
    pub fn balance(&self, account: &str) -> Option<Reading> {
        self.accounts
            .get(account)?
            .gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .readings
            .get("deepseek.balance")?
            .local
            .as_ref()
            .map(|s| s.reading.clone())
    }
    pub fn is_closed(&self, name: &str) -> Result<bool> {
        let account = self
            .accounts
            .get(name)
            .ok_or(GatewayError::StateUnavailable)?;
        let mut gate = account
            .gate
            .lock()
            .map_err(|_| GatewayError::StateUnavailable)?;
        Ok(gate.closed(name, (self.clock)()))
    }
    fn budgets(&self, value: &Value) -> Result<()> {
        let budgets: Vec<Budget> =
            serde_json::from_value(value.clone()).map_err(|_| GatewayError::InvalidProviderJson)?;
        for budget in budgets {
            let Some(account) = self.accounts.get(&budget.account) else {
                continue;
            };
            let mut gate = account
                .gate
                .lock()
                .map_err(|_| GatewayError::StateUnavailable)?;
            let sources = gate.readings.entry(budget.name.clone()).or_default();
            let mut reading = budget.reading;
            reading.limit = Some(budget.limit);
            let observation = Observation {
                reading,
                time: budget.updated,
                exhausted: budget.state == "exhausted" || budget.spent >= budget.limit,
                allowed: budget.over_limit_allowed,
            };
            // The reading time selects usage; updated breaks ties for answered decisions.
            if sources
                .core
                .as_ref()
                .is_none_or(|old| (observation.at(), observation.time) >= (old.at(), old.time))
            {
                sources.core = Some(observation);
                self.save_gate(&budget.account, &gate);
            }
            gate.closed(&budget.account, (self.clock)());
        }
        Ok(())
    }
    pub async fn closed(&self, name: &str, usage_reader: Option<UsageRead<'_>>) -> Result<bool> {
        let account = self
            .accounts
            .get(name)
            .ok_or(GatewayError::StateUnavailable)?;
        let _refresh = account.refresh.lock().await;
        let refresh = {
            let mut gate = account
                .gate
                .lock()
                .map_err(|_| GatewayError::StateUnavailable)?;
            let closed = gate.closed(name, (self.clock)());
            gate.last_refresh.is_none()
                || (closed
                    && gate
                        .last_refresh
                        .is_some_and(|at| at.elapsed() >= Duration::from_secs(10)))
        };
        if refresh {
            account
                .gate
                .lock()
                .map_err(|_| GatewayError::StateUnavailable)?
                .last_refresh = Some(Instant::now());
            let result = async {
                let mut core = Core::connect(&self.core_path).await?;
                let reply = core.call("budget.get", json!({"account": name})).await?;
                self.budgets(&reply["ok"]["budgets"])
            };
            match tokio::time::timeout(Duration::from_secs(2), result).await {
                Ok(Ok(())) => {}
                _ => tracing::warn!(
                    code = "budget_refresh_failed",
                    account = name,
                    "quota refresh failed"
                ),
            }
        }
        if let Some(reader) = usage_reader {
            let due = {
                let mut gate = account
                    .gate
                    .lock()
                    .map_err(|_| GatewayError::StateUnavailable)?;
                if gate.closed(name, (self.clock)())
                    && gate
                        .last_usage_read
                        .is_none_or(|at| at.elapsed() >= USAGE_READ_INTERVAL)
                {
                    // Stamp before the read: failures and waiting requests share the interval.
                    gate.last_usage_read = Some(Instant::now());
                    true
                } else {
                    false
                }
            };
            if due {
                match reader.await {
                    Ok(readings) => {
                        let readings = {
                            let gate = account
                                .gate
                                .lock()
                                .map_err(|_| GatewayError::StateUnavailable)?;
                            readings
                                .into_iter()
                                .filter_map(|(mut reading, window_seconds)| {
                                    let stored =
                                        &gate.readings.get(&reading.name)?.newest()?.reading;
                                    if stored.window_minutes.and_then(|m| m.checked_mul(60))
                                        != Some(window_seconds)
                                    {
                                        return None;
                                    }
                                    reading.limit = stored.limit;
                                    Some(reading)
                                })
                                .collect::<Vec<_>>()
                        };
                        self.observe(name, &readings, (self.clock)());
                    }
                    Err(code) => tracing::warn!(code, account = name, "provider usage read failed"),
                }
            }
        }
        let mut gate = account
            .gate
            .lock()
            .map_err(|_| GatewayError::StateUnavailable)?;
        Ok(gate.closed(name, (self.clock)()))
    }
    pub async fn run(&self) -> Result<()> {
        for account in self.accounts.keys() {
            self.closed(account, None).await?;
        }
        let mut core: Option<Core> = None;
        let mut delay = Duration::from_secs(1);
        loop {
            let pending = self.outbox.first().await;
            if let Ok(None) = pending {
                core = None;
                self.outbox.changed.notified().await;
                continue;
            }
            let result = async {
                let (seq, body) = pending?.ok_or(GatewayError::StateUnavailable)?;
                let record: Value =
                    serde_json::from_str(&body).map_err(|_| GatewayError::StateUnavailable)?;
                if core.is_none() {
                    core = Some(Core::connect(&self.core_path).await?);
                }
                let reply = core
                    .as_mut()
                    .ok_or(GatewayError::StateUnavailable)?
                    .call("request.record", record.clone())
                    .await?;
                if reply.get("ok").is_some() {
                    // The core stored the record; an unreadable budget list must not resend it.
                    if self.budgets(&reply["ok"]["budgets"]).is_err() {
                        tracing::error!(
                            code = "budget_reply_invalid",
                            request_id = record["request_id"].as_str().unwrap_or(""),
                            "core budget reply unreadable"
                        );
                    }
                } else if reply["error"]["reason"] == "request_id_conflict" {
                    tracing::error!(
                        code = "request_id_conflict",
                        request_id = record["request_id"].as_str().unwrap_or(""),
                        "core rejected conflicting record"
                    );
                } else {
                    return Err(GatewayError::StateUnavailable);
                }
                self.outbox.delete(seq).await
            };
            match tokio::time::timeout(self.core_timeout, result).await {
                Ok(Ok(())) => {
                    delay = Duration::from_secs(1);
                }
                _ => {
                    core = None;
                    tracing::warn!(
                        code = "outbox_drain_failed",
                        "request records remain in outbox"
                    );
                    tokio::select! { _ = tokio::time::sleep(delay) => {}, () = self.outbox.changed.notified() => {} }
                    delay = (delay * 2).min(Duration::from_secs(30));
                }
            }
        }
    }
}
struct Core {
    stream: BufReader<UnixStream>,
}
impl Core {
    async fn connect(path: &Path) -> Result<Self> {
        Ok(Self {
            stream: BufReader::new(
                UnixStream::connect(path)
                    .await
                    .map_err(|_| GatewayError::Transport)?,
            ),
        })
    }
    async fn call(&mut self, call: &str, args: Value) -> Result<Value> {
        let mut bytes = serde_json::to_vec(&json!({"call": call, "args": args}))
            .map_err(|_| GatewayError::InvalidRequest)?;
        bytes.push(b'\n');
        self.stream
            .get_mut()
            .write_all(&bytes)
            .await
            .map_err(|_| GatewayError::Transport)?;
        let mut line = String::new();
        if self
            .stream
            .read_line(&mut line)
            .await
            .map_err(|_| GatewayError::Transport)?
            == 0
            || !line.ends_with('\n')
        {
            return Err(GatewayError::Transport);
        }
        serde_json::from_str(&line).map_err(|_| GatewayError::InvalidProviderJson)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use symbiotic_ai_runtime::model::item_stream::{ChatCompletionsAdapter, ItemEvent};

    #[test]
    fn request_records_state_missing_cache_hit_stays_absent() {
        let event = ChatCompletionsAdapter::json(br#"{"choices":[],"usage":{"prompt_tokens":7,"prompt_cache_miss_tokens":5,"future_tokens":7}}"#).unwrap();
        let ItemEvent::Usage { record, usage, .. } = event else {
            panic!("expected usage");
        };
        let serialized = serde_json::to_value(Usage::new(&record.value["usage"], &usage)).unwrap();
        assert_eq!(
            serialized,
            json!({"normalized":{"input_tokens":7},"provider":{"prompt_tokens":7,"prompt_cache_miss_tokens":5,"future_tokens":7}})
        );
    }

    #[tokio::test]
    async fn request_records_state_outbox_retains_records_in_order_across_reopen() {
        let directory = tempfile::tempdir().unwrap();
        let outbox = Outbox::open(directory.path()).unwrap();
        for id in ["job-1", "job-2"] {
            outbox.insert(id.into(), json!({"request_id":id}).to_string());
        }
        outbox.flush().await.unwrap();
        drop(outbox);
        let outbox = Outbox::open(directory.path()).unwrap();
        let (seq, body) = outbox.first().await.unwrap().unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&body).unwrap(),
            json!({"request_id":"job-1"})
        );
        outbox.delete(seq).await.unwrap();
        let (seq, body) = outbox.first().await.unwrap().unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&body).unwrap(),
            json!({"request_id":"job-2"})
        );
        outbox.delete(seq).await.unwrap();
        assert_eq!(outbox.first().await.unwrap(), None);
        let connection =
            rusqlite::Connection::open(directory.path().join("outbox.sqlite")).unwrap();
        assert_eq!(
            connection
                .pragma_query_value(None, "user_version", |r| r.get::<_, u32>(0))
                .unwrap(),
            1
        );
        assert_eq!(
            connection
                .pragma_query_value(None, "journal_mode", |r| r.get::<_, String>(0))
                .unwrap(),
            "wal"
        );
    }
}
