//! Route-owned provider balance reads. Only numeric observations reach health.
use super::*;

/// Supported provider balance protocol.
#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BalanceProvider {
    /// GET https://api.deepseek.com/user/balance with the route's bearer secret.
    Deepseek,
}
/// Currencies returned by the DeepSeek balance endpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Currency {
    /// US dollars.
    USD,
    /// Chinese yuan.
    CNY,
}
/// Threshold in one explicitly selected provider currency.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LowBalance {
    /// Currency to select from balance_infos.
    pub currency: Currency,
    /// Warn below this exact amount.
    pub amount: DecimalAmount,
}
/// Optional per-route balance monitoring. It never gates requests.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BalanceConfig {
    /// Closed protocol and destination selection.
    pub provider: BalanceProvider,
    /// Positive polling interval; first poll runs at server start.
    pub interval_seconds: u64,
    /// Minimum interval between reads triggered by 402; defaults to one second.
    #[serde(default = "default_debounce")]
    pub debounce_seconds: u64,
    /// Selected currency and low-balance warning threshold.
    pub low_balance: LowBalance,
}
fn default_debounce() -> u64 {
    1
}
impl BalanceConfig {
    pub(super) fn valid(&self, destination: &str) -> bool {
        // The same route credential must already be authorized for this origin.
        let Ok(url) = reqwest::Url::parse(destination) else {
            return false;
        };
        self.interval_seconds > 0
            && self.debounce_seconds > 0
            && Instant::now()
                .checked_add(Duration::from_secs(self.interval_seconds))
                .is_some()
            && url.scheme() == "https"
            && url.host_str() == Some("api.deepseek.com")
            && url.port_or_known_default() == Some(443)
    }
}
/// Static diagnostic codes; no provider text is retained on failure.
#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum BalanceReadError {
    CredentialUnavailable,
    Transport,
    UpstreamStatus,
    ResponseTooLarge,
    InvalidResponse,
}
#[derive(Clone, Serialize)]
pub(super) struct BalanceHealth {
    currency: Currency,
    total_nano_units: Option<u64>,
    is_available: Option<bool>,
    last_read_error: Option<BalanceReadError>,
    read_at: Option<i64>,
    error_at: Option<i64>,
    low: bool,
}
#[derive(Default)]
pub(super) struct BalanceActivity {
    pub value: Option<BalanceHealth>,
}
#[derive(Deserialize)]
struct BalanceResponse {
    is_available: bool,
    balance_infos: Vec<BalanceInfo>,
}
#[derive(Deserialize)]
struct BalanceInfo {
    currency: Currency,
    total_balance: DecimalAmount,
}

impl Passthrough {
    pub(super) async fn balance_loop(&self, route: &Route) -> Result<()> {
        let config = route
            .config
            .balance
            .as_ref()
            .ok_or(EgressError::InvalidRequest)?;
        let mut interval = tokio::time::interval(Duration::from_secs(config.interval_seconds));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut last_read: Option<Instant> = None;
        loop {
            tokio::select! {
                _ = interval.tick() => {},
                () = route.balance_refresh.notified() => {
                    if last_read.is_some_and(|last| last.elapsed() < Duration::from_secs(config.debounce_seconds)) { continue; }
                }
            }
            last_read = Some(Instant::now());
            self.read_balance(route).await?;
        }
    }
    pub(super) async fn read_balance(&self, route: &Route) -> Result<()> {
        let config = route
            .config
            .balance
            .as_ref()
            .ok_or(EgressError::InvalidRequest)?;
        let result = self.fetch_balance(route, config).await;
        let mut health = route
            .health
            .lock()
            .map_err(|_| EgressError::StateUnavailable)?;
        match result {
            Ok(value) => {
                if value.low
                    && !health
                        .balance
                        .value
                        .as_ref()
                        .is_some_and(|previous| previous.low)
                {
                    tracing::warn!(route = route.config.name, currency = ?value.currency, total_nano_units = value.total_nano_units, is_available = value.is_available, "provider balance low");
                }
                health.balance.value = Some(value);
            }
            Err(code) => {
                if health
                    .balance
                    .value
                    .as_ref()
                    .and_then(|v| v.last_read_error)
                    .is_none()
                {
                    tracing::warn!(route = route.config.name, code = ?code, "provider balance read failed");
                }
                let value = health.balance.value.get_or_insert(BalanceHealth {
                    currency: config.low_balance.currency,
                    total_nano_units: None,
                    is_available: None,
                    last_read_error: None,
                    read_at: None,
                    error_at: None,
                    low: false,
                });
                value.last_read_error = Some(code);
                value.error_at = Some(self.now().timestamp());
            }
        }
        self.changed.notify_one();
        Ok(())
    }
    async fn fetch_balance(
        &self,
        route: &Route,
        config: &BalanceConfig,
    ) -> std::result::Result<BalanceHealth, BalanceReadError> {
        let (headers, _login_request) = route
            .headers()
            .await
            .map_err(|_| BalanceReadError::CredentialUnavailable)?;
        let destination = "https://api.deepseek.com/user/balance";
        #[cfg(test)]
        let wire_url = self
            .wire_base
            .as_ref()
            .map(|base| format!("{base}/user/balance"));
        #[cfg(test)]
        let destination = wire_url.as_deref().unwrap_or(destination);
        let mut response = route
            .client
            .get(destination)
            .headers(headers)
            .header("accept-encoding", "identity")
            .send()
            .await
            .map_err(|_| BalanceReadError::Transport)?;
        if response.status() != reqwest::StatusCode::OK {
            return Err(BalanceReadError::UpstreamStatus);
        }
        let max = route.config.max_response_bytes;
        if response.content_length().is_some_and(|n| n > max as u64) {
            return Err(BalanceReadError::ResponseTooLarge);
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| BalanceReadError::Transport)?
        {
            if chunk.len() > max.saturating_sub(bytes.len()) {
                return Err(BalanceReadError::ResponseTooLarge);
            }
            bytes.extend_from_slice(&chunk);
        }
        let value: BalanceResponse =
            serde_json::from_slice(&bytes).map_err(|_| BalanceReadError::InvalidResponse)?;
        let mut matches = value
            .balance_infos
            .into_iter()
            .filter(|info| info.currency == config.low_balance.currency);
        let info = matches.next().ok_or(BalanceReadError::InvalidResponse)?;
        if matches.next().is_some() {
            return Err(BalanceReadError::InvalidResponse);
        }
        Ok(BalanceHealth {
            currency: info.currency,
            total_nano_units: Some(info.total_balance.nano_units()),
            is_available: Some(value.is_available),
            last_read_error: None,
            read_at: Some(self.now().timestamp()),
            error_at: None,
            low: !value.is_available || info.total_balance < config.low_balance.amount,
        })
    }
}
