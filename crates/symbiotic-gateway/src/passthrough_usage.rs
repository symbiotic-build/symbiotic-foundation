//! Route-owned, read-only Codex quota observations.
use super::*;
use crate::records::{Reading, UsageRead};

const USAGE_TIMEOUT: Duration = Duration::from_secs(2);
const USAGE_PATH: &str = "/backend-api/wham/usage";

#[derive(Deserialize)]
struct UsageResponse {
    rate_limit: UsageWindows,
}
#[derive(Deserialize)]
struct UsageWindows {
    primary_window: Option<UsageWindow>,
    secondary_window: Option<UsageWindow>,
}
#[derive(Deserialize)]
struct UsageWindow {
    used_percent: u32,
    limit_window_seconds: u64,
    reset_at: i64,
}

impl Passthrough {
    pub(super) fn usage_reader<'a>(&'a self, route: &'a Route) -> Option<UsageRead<'a>> {
        matches!(
            (&route.credential, &route.config.secret),
            (
                Credential::Login(_),
                SecretSource::LoginFile {
                    family: LoginFamily::ChatgptCodex,
                    ..
                }
            )
        )
        .then(|| Box::pin(self.read_usage(route)) as UsageRead<'a>)
    }

    async fn read_usage(
        &self,
        route: &Route,
    ) -> std::result::Result<Vec<(Reading, u64)>, &'static str> {
        tokio::time::timeout(USAGE_TIMEOUT, self.fetch_usage(route))
            .await
            .map_err(|_| "usage_timeout")?
    }

    async fn fetch_usage(
        &self,
        route: &Route,
    ) -> std::result::Result<Vec<(Reading, u64)>, &'static str> {
        let origin = reqwest::Url::parse(&route.config.destination)
            .map_err(|_| "usage_destination_invalid")?;
        if origin.scheme() != "https"
            || origin.host_str() != Some("chatgpt.com")
            || origin.port_or_known_default() != Some(443)
            || !origin.username().is_empty()
            || origin.password().is_some()
        {
            return Err("usage_destination_invalid");
        }
        let destination = origin
            .join(USAGE_PATH)
            .map_err(|_| "usage_destination_invalid")?;
        if destination.origin() != origin.origin() {
            return Err("usage_destination_invalid");
        }
        // Only the wire transport is replaced in tests; destination checks still run.
        #[cfg(test)]
        let destination = self
            .wire_base
            .as_ref()
            .map(|base| format!("{base}{USAGE_PATH}"))
            .map(|url| reqwest::Url::parse(&url))
            .transpose()
            .map_err(|_| "usage_destination_invalid")?
            .unwrap_or(destination);
        let (headers, _login_request) = route
            .headers()
            .await
            .map_err(|_| "usage_credential_unavailable")?;
        let mut response = route
            .client
            .get(destination)
            .headers(headers)
            .header("accept-encoding", "identity")
            .send()
            .await
            .map_err(|_| "usage_transport")?;
        if response.status() != reqwest::StatusCode::OK {
            return Err("usage_upstream_status");
        }
        let max = route.config.max_response_bytes;
        if response.content_length().is_some_and(|n| n > max as u64) {
            return Err("usage_response_too_large");
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| "usage_transport")? {
            if chunk.len() > max.saturating_sub(bytes.len()) {
                return Err("usage_response_too_large");
            }
            bytes.extend_from_slice(&chunk);
        }
        let value: UsageResponse =
            serde_json::from_slice(&bytes).map_err(|_| "usage_invalid_response")?;
        let mut readings = Vec::new();
        for (name, window) in [
            ("codex.primary", value.rate_limit.primary_window),
            ("codex.secondary", value.rate_limit.secondary_window),
        ] {
            let Some(window) = window else {
                continue;
            };
            if window.limit_window_seconds == 0 {
                return Err("usage_invalid_response");
            }
            readings.push((
                Reading {
                    name: name.into(),
                    unit: "percent".into(),
                    used: Some(f64::from(window.used_percent)),
                    window_minutes: Some(window.limit_window_seconds / 60),
                    resets_at: Some(
                        DateTime::from_timestamp(window.reset_at, 0)
                            .ok_or("usage_invalid_response")?,
                    ),
                    read_at: Some(self.now()),
                    ..Default::default()
                },
                window.limit_window_seconds,
            ));
        }
        if readings.is_empty() {
            return Err("usage_invalid_response");
        }
        Ok(readings)
    }
}
