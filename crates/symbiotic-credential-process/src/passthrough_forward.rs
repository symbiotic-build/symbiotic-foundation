//! One retry engine for HTTP failures, connection failures and interrupted items.
use super::*;
use std::sync::atomic::Ordering;

struct AttemptLog<'a> {
    route: &'a str,
    attempt: u32,
    new_session: bool,
    observed: Option<u16>,
}
impl AttemptLog<'_> {
    fn warn(
        &self,
        code: &str,
        class: ErrorClass,
        bytes_reached_caller: bool,
        action: &str,
        delay_ms: Option<u128>,
    ) {
        tracing::warn!(
            route = self.route,
            status = self.observed,
            code,
            ?class,
            attempt = self.attempt,
            new_session = self.new_session,
            bytes_reached_caller,
            action,
            delay_ms,
            "upstream failure"
        );
    }
}

struct Failure {
    status: u16,
    code: String,
    class: ErrorClass,
    headers: HeaderMap,
    event: Option<Box<ItemEvent>>,
    new_session: bool,
}
impl Failure {
    fn network(code: &str, headers: Option<&HeaderMap>) -> Self {
        Self {
            status: 502,
            code: code.into(),
            class: ResponsesAdapter::classify(None, code),
            headers: headers.cloned().unwrap_or_default(),
            event: None,
            new_session: false,
        }
    }
}
enum AnswerOutcome {
    Complete,
    Terminated(&'static str),
    Failure(Failure),
}
#[derive(Default)]
struct WaitTime {
    total: Duration,
    cooldown: Duration,
}
impl Passthrough {
    async fn pause(
        &self,
        route: &Route,
        id: Option<&str>,
        reason: &'static str,
        duration: Duration,
        wait: &mut WaitTime,
    ) -> Result<()> {
        let _entry = self.waiting(route, id, reason)?;
        let start = Instant::now();
        tokio::time::sleep(duration).await;
        let elapsed = start.elapsed();
        wait.total += elapsed;
        if reason != "slot" {
            wait.cooldown += elapsed;
        }
        Ok(())
    }
    async fn session_permit(
        &self,
        route: &Route,
        key: &Option<Vec<Option<Vec<u8>>>>,
        id: Option<&str>,
        wait: &mut WaitTime,
    ) -> Result<std::result::Result<Option<tokio::sync::OwnedSemaphorePermit>, DateTime<Utc>>> {
        let mut changes = route.retries.sessions.changes.subscribe();
        loop {
            if !route.is_new(key)? {
                return Ok(Ok(None));
            }
            let (until, probing) = {
                let s = route
                    .retries
                    .sessions
                    .state
                    .lock()
                    .map_err(|_| EgressError::StateUnavailable)?;
                (s.until, s.probing)
            };
            let now = self.now();
            if let Some(until) = until.filter(|t| *t > now) {
                let remaining = (until - now)
                    .to_std()
                    .map_err(|_| EgressError::StateUnavailable)?;
                let budget = route
                    .config
                    .max_queue_wait_seconds
                    .map(|s| Duration::from_secs(s).saturating_sub(wait.cooldown));
                if budget == Some(Duration::ZERO) {
                    return Ok(Err(until));
                }
                self.pause(
                    route,
                    id,
                    "new_session",
                    budget.map_or(remaining, |b| b.min(remaining)),
                    wait,
                )
                .await?;
                route.usable().await?;
                continue;
            }
            if !probing {
                return Ok(Ok(None));
            }
            let _entry = self.waiting(route, id, "new_session")?;
            let started = Instant::now();
            let permit = tokio::select! {
                p=route.retries.sessions.probe.clone().acquire_owned()=>Some(p.map_err(|_|EgressError::StateUnavailable)?),
                r=changes.changed()=>{r.map_err(|_|EgressError::StateUnavailable)?;None},
            };
            wait.total += started.elapsed();
            wait.cooldown += started.elapsed();
            route.usable().await?;
            if let Some(permit) = permit {
                let s = route
                    .retries
                    .sessions
                    .state
                    .lock()
                    .map_err(|_| EgressError::StateUnavailable)?;
                if s.until.is_none_or(|t| t <= self.now()) {
                    return Ok(Ok(Some(permit)));
                }
            }
        }
    }
    async fn cooldown(
        &self,
        route: &Route,
        id: Option<&str>,
        wait: &mut WaitTime,
    ) -> Result<Option<DateTime<Utc>>> {
        loop {
            let until = route
                .account
                .cooldown_until()
                .await
                .map_err(|_| EgressError::StateUnavailable)?;
            let now = self.now();
            let Some(until) = until.filter(|t| *t > now) else {
                return Ok(None);
            };
            let remaining = (until - now)
                .to_std()
                .map_err(|_| EgressError::StateUnavailable)?;
            let budget = route
                .config
                .max_queue_wait_seconds
                .map(|s| Duration::from_secs(s).saturating_sub(wait.cooldown));
            if budget == Some(Duration::ZERO) {
                return Ok(Some(until));
            }
            self.pause(
                route,
                id,
                "cooldown",
                budget.map_or(remaining, |b| b.min(remaining)),
                wait,
            )
            .await?;
            route.usable().await?;
        }
    }
    async fn local_error(
        &self,
        write: &mut (impl tokio::io::AsyncWrite + Unpin),
        started: bool,
        status: u16,
        code: &'static str,
        wait: &WaitTime,
        until: Option<DateTime<Utc>>,
    ) -> Result<()> {
        if started {
            chunk(
                write,
                format!("event: error\ndata: {{\"type\":\"error\",\"code\":\"{code}\"}}\n\n")
                    .as_bytes(),
            )
            .await?;
            return finish(write).await;
        }
        let mut headers = vec![(
            "x-symbiotic-queue-waited-ms".into(),
            wait.total.as_millis().to_string(),
        )];
        if let Some(until) = until {
            headers.push((
                "retry-after".into(),
                (until - self.now())
                    .to_std()
                    .unwrap_or_default()
                    .as_nanos()
                    .div_ceil(1_000_000_000)
                    .to_string(),
            ));
        }
        fixed(write, status, code, &headers).await
    }
    async fn retry_decision(
        &self,
        route: &Route,
        failure: &Failure,
        log: &AttemptLog<'_>,
        policy: &RetryPolicy,
        started: bool,
        fallback_available: bool,
    ) -> Result<RetryDecision> {
        if contains_secret(failure.code.as_bytes(), &route.secrets().await) {
            return Err(EgressError::InvalidProviderJson);
        }
        let class = route
            .config
            .error_classes
            .get(&failure.code)
            .copied()
            .unwrap_or(failure.class);
        let context = RetryContext {
            class,
            code: failure.code.clone(),
            attempt: log.attempt,
            bytes_reached_caller: started,
            route_name: route.config.name.clone(),
        };
        let mut rule = retry_policy::decision(policy, self.retry_hook.as_ref(), &context);
        if rule.action == RetryAction::Switch && !fallback_available {
            rule.action = RetryAction::Fail;
        }
        route
            .retries
            .failures
            .try_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                Some(n.saturating_add(1))
            })
            .map_err(|_| EgressError::StateUnavailable)?;
        if rule.action != RetryAction::Fail {
            let mut health = route
                .health
                .lock()
                .map_err(|_| EgressError::StateUnavailable)?;
            let n = health
                .retried_events
                .entry(failure.code.clone())
                .or_default();
            *n = n.checked_add(1).ok_or(EgressError::StateUnavailable)?;
            self.changed.notify_one();
        }
        let delay = if rule.action == RetryAction::RetrySame {
            let count = route.retries.failures.load(Ordering::Acquire);
            let retry_after = (failure.headers.get_all("retry-after").iter().count() == 1)
                .then(|| failure.headers.get("retry-after"))
                .flatten()
                .and_then(|v| v.to_str().ok())
                .and_then(|v| retry::parse_retry_after(v, self.now().timestamp()).ok());
            Some(match retry_after {
                Some(s) => Duration::from_secs(s),
                None => Duration::from_millis(
                    retry::delay_ms(
                        count,
                        rule.backoff
                            .base_delay_ms
                            .unwrap_or(route.account.retry_base_delay_ms()),
                        0,
                        0,
                    )
                    .map_err(|_| EgressError::StateUnavailable)?,
                ),
            })
        } else {
            None
        };
        // One journal line per retry decision, before the hold or cooldown is recorded.
        log.warn(
            &failure.code,
            class,
            started,
            match rule.action {
                RetryAction::RetrySame => "retry_same",
                RetryAction::Switch => "switch",
                RetryAction::Fail => "fail",
            },
            delay.map(|d| d.as_millis()),
        );
        if let Some(delay) = delay {
            let until = self
                .now()
                .checked_add_signed(
                    chrono::Duration::from_std(delay).map_err(|_| EgressError::StateUnavailable)?,
                )
                .ok_or(EgressError::StateUnavailable)?;
            if failure.new_session {
                let _gate = route
                    .account
                    .send_gate()
                    .await
                    .map_err(|_| EgressError::StateUnavailable)?;
                let mut hold = route
                    .retries
                    .sessions
                    .state
                    .lock()
                    .map_err(|_| EgressError::StateUnavailable)?;
                hold.until = Some(hold.until.map_or(until, |t| t.max(until)));
                hold.probing = true;
                route.retries.sessions.changes.send_replace(());
                self.changed.notify_one();
            } else {
                route
                    .account
                    .note_cooldown(until)
                    .await
                    .map_err(|_| EgressError::StateUnavailable)?;
            }
        }
        Ok(rule)
    }
    pub(super) async fn forward(
        &self,
        initial: &Route,
        request: Request,
        original: Vec<u8>,
        write: &mut (impl tokio::io::AsyncWrite + Unpin),
    ) -> Result<()> {
        let named = request
            .headers
            .iter()
            .find(|(n, _)| n == "x-symbiotic-retry-policy")
            .map(|(_, v)| std::str::from_utf8(v));
        let named = match named {
            Some(Ok(n)) => match self.retry_policies.get(n) {
                Some(p) => Some(p),
                None => return fixed(write, 400, "retry_policy_unknown", &[]).await,
            },
            Some(Err(_)) => return fixed(write, 400, "retry_policy_unknown", &[]).await,
            None => None,
        };
        let request_id = request
            .headers
            .iter()
            .find(|(n, _)| n == "x-symbiotic-request-id")
            .map(|(_, v)| v);
        let secrets = initial.secrets().await;
        if request_id.is_some_and(|v| !safe_value(v, &secrets, 64)) {
            return fixed(write, 400, "request_id_invalid", &[]).await;
        }
        let id = request_id.and_then(|v| std::str::from_utf8(v).ok());
        let mut route = initial;
        let mut fallback = initial.config.fallback_routes.iter();
        let mut wait = WaitTime::default();
        let mut started = false;
        let mut join = ItemJoin::new(route.config.max_held_event_bytes);
        let mut buffered = reqwest::Request::new(
            reqwest::Method::POST,
            reqwest::Url::parse("https://provider.invalid")
                .map_err(|_| EgressError::InvalidRequest)?,
        );
        *buffered.body_mut() = Some(original.clone().into());
        let mut slot = None;
        let mut authentication: Option<(HeaderMap, Option<LoginRequest>)> = None;
        let mut attempt = 0u32;
        loop {
            if route.usable().await.is_err() {
                return self
                    .local_error(write, started, 503, "credential_unavailable", &wait, None)
                    .await;
            }
            let key = route.session_key(&request);
            if route.is_new(&key)? {
                let hold = route
                    .retries
                    .sessions
                    .state
                    .lock()
                    .map_err(|_| EgressError::StateUnavailable)?;
                if hold.probing {
                    slot = None;
                }
            }
            let mut session = match self.session_permit(route, &key, id, &mut wait).await {
                Ok(Ok(p)) => p,
                Ok(Err(until)) => {
                    return self
                        .local_error(write, started, 429, "rate_limited", &wait, Some(until))
                        .await;
                }
                Err(EgressError::CredentialUnavailable) => {
                    return self
                        .local_error(write, started, 503, "credential_unavailable", &wait, None)
                        .await;
                }
                Err(error) => return Err(error),
            };
            if slot.is_none() {
                let _entry = self.waiting(route, id, "slot")?;
                let now = Instant::now();
                slot = Some(
                    route
                        .account
                        .acquire()
                        .await
                        .map_err(|_| EgressError::StateUnavailable)?,
                );
                wait.total += now.elapsed();
            }
            let cooldown = match self.cooldown(route, id, &mut wait).await {
                Ok(c) => c,
                Err(EgressError::CredentialUnavailable) => {
                    return self
                        .local_error(write, started, 503, "credential_unavailable", &wait, None)
                        .await;
                }
                Err(error) => return Err(error),
            };
            if route.usable().await.is_err() {
                return self
                    .local_error(write, started, 503, "credential_unavailable", &wait, None)
                    .await;
            }
            if let Some(until) = cooldown {
                return self
                    .local_error(write, started, 429, "rate_limited", &wait, Some(until))
                    .await;
            }
            if authentication.is_none() {
                authentication = match route.headers().await {
                    Ok(a) => Some(a),
                    Err(_) => {
                        return self
                            .local_error(write, started, 503, "credential_unavailable", &wait, None)
                            .await;
                    }
                };
            }
            if let (Credential::Login(login), Some((injected, Some(login_request)))) =
                (&route.credential, authentication.as_mut())
            {
                if login.refresh_request(login_request).await.is_err() {
                    return self
                        .local_error(write, started, 503, "credential_unavailable", &wait, None)
                        .await;
                }
                *injected = login_request.headers().clone();
            }
            let mut headers = authentication
                .as_ref()
                .ok_or(EgressError::StateUnavailable)?
                .0
                .clone();
            let connection_headers: HashSet<_> = request
                .headers
                .iter()
                .filter(|(n, _)| n == "connection")
                .flat_map(|(_, v)| v.split(|b| *b == b','))
                .filter_map(|v| {
                    std::str::from_utf8(trim_ows(v))
                        .ok()
                        .map(str::to_ascii_lowercase)
                })
                .collect();
            for (name, value) in &request.headers {
                if !forbidden_header(name)
                    && !connection_headers.contains(name)
                    && route.config.forward_headers.contains(name)
                    && printable(value, 8192)
                {
                    headers.insert(
                        HeaderName::from_bytes(name.as_bytes())
                            .map_err(|_| EgressError::InvalidRequest)?,
                        HeaderValue::from_bytes(value).map_err(|_| EgressError::InvalidRequest)?,
                    );
                }
            }
            headers.insert("accept-encoding", HeaderValue::from_static("identity"));
            let destination = &route.config.destination;
            #[cfg(test)]
            let wire_url = self.wire_base.as_ref().map(|base| {
                format!(
                    "{base}{}",
                    reqwest::Url::parse(destination)
                        .expect("validated URL")
                        .path()
                )
            });
            #[cfg(test)]
            let destination = wire_url.as_ref().unwrap_or(destination);
            let grant = match route
                .account
                .request_admission()
                .await
                .map_err(|_| EgressError::StateUnavailable)?
            {
                model::ModelRequestAdmission::Cooldown => continue,
                model::ModelRequestAdmission::Wait(d) => {
                    self.pause(route, id, "slot", d, &mut wait).await?;
                    continue;
                }
                model::ModelRequestAdmission::Ready(g) => g,
            };
            if route.usable().await.is_err() {
                drop(grant);
                return self
                    .local_error(write, started, 503, "credential_unavailable", &wait, None)
                    .await;
            }
            // Reusable body storage is cloned only after admission succeeds.
            let transport_request = route
                .client
                .request(
                    match route.config.method {
                        PassthroughMethod::Get => reqwest::Method::GET,
                        PassthroughMethod::Post => reqwest::Method::POST,
                    },
                    destination,
                )
                .headers(headers)
                .body(
                    buffered
                        .try_clone()
                        .and_then(|mut request| request.body_mut().take())
                        .ok_or(EgressError::StateUnavailable)?,
                )
                .build()
                .map_err(|_| EgressError::InvalidRequest)?;
            // execute() starts reqwest's timers; defer it until the grant actually sends.
            let transport = async { route.client.execute(transport_request).await };
            let mut was_new = false;
            // Step 2: reserve a passthrough spend receipt immediately before sending.
            let sent = grant
                .start_checked(
                    || {
                        let hold = route.retries.sessions.state.lock().map_err(|_| {
                            model::ModelError::Queue(model::DiagnosticCode::QueueFailure)
                        })?;
                        let new = route.is_new(&key).map_err(|_| {
                            model::ModelError::Queue(model::DiagnosticCode::QueueFailure)
                        })?;
                        if new
                            && (hold.until.is_some_and(|t| t > self.now())
                                || (hold.probing && session.is_none()))
                        {
                            Ok(None)
                        } else {
                            was_new = new;
                            Ok(Some(hold))
                        }
                    },
                    transport,
                )
                .await
                .map_err(|_| EgressError::StateUnavailable)?;
            let Some(sent) = sent else {
                slot = None;
                continue;
            };
            attempt = attempt.saturating_add(1);
            let log = AttemptLog {
                route: &route.config.name,
                attempt,
                new_session: was_new,
                observed: sent.as_ref().ok().map(|r| r.status().as_u16()),
            };
            let failure = match sent {
                Err(_) => Failure::network("network", None),
                Ok(mut response) => {
                    self.observed_headers(route, response.headers()).await?;
                    let status = response.status().as_u16();
                    let response_headers = response.headers().clone();
                    if (200..300).contains(&status) {
                        if let Some(key) = &key {
                            route
                                .seen
                                .lock()
                                .map_err(|_| EgressError::StateUnavailable)?
                                .insert(key.clone());
                        }
                        if was_new {
                            let mut hold = route
                                .retries
                                .sessions
                                .state
                                .lock()
                                .map_err(|_| EgressError::StateUnavailable)?;
                            if session.is_some() && hold.until.is_none_or(|t| t <= self.now()) {
                                hold.until = None;
                                hold.probing = false;
                                route.retries.sessions.changes.send_replace(());
                                self.changed.notify_one();
                            }
                        }
                        drop(session.take());
                        match self
                            .answer(
                                route,
                                &mut response,
                                write,
                                &mut started,
                                &mut join,
                                &mut wait,
                            )
                            .await?
                        {
                            AnswerOutcome::Complete => {
                                route.retries.failures.store(0, Ordering::Release);
                                return Ok(());
                            }
                            AnswerOutcome::Terminated(code) => {
                                log.warn(code, ErrorClass::Final, started, "fail", None);
                                return self
                                    .local_error(write, started, 502, code, &wait, None)
                                    .await;
                            }
                            AnswerOutcome::Failure(f) => f,
                        }
                    } else {
                        if status == 401 && matches!(route.credential, Credential::Static(_)) {
                            log.warn("upstream_status", ErrorClass::Final, started, "fail", None);
                            return self
                                .local_error(write, false, 401, "upstream_status", &wait, None)
                                .await;
                        }
                        if status == 401
                            && let (Credential::Login(login), Some((injected, Some(login_request)))) =
                                (&route.credential, authentication.as_mut())
                        {
                            let renewed = login_request.retried();
                            if login.after_unauthorized(login_request).await.is_ok() {
                                log.warn(
                                    "upstream_status",
                                    ErrorClass::Final,
                                    started,
                                    "renew",
                                    None,
                                );
                                *injected = login_request.headers().clone();
                                continue;
                            }
                            if !renewed {
                                log.warn(
                                    "credential_unavailable",
                                    ErrorClass::Final,
                                    started,
                                    "fail",
                                    None,
                                );
                                return self
                                    .local_error(
                                        write,
                                        started,
                                        503,
                                        "credential_unavailable",
                                        &wait,
                                        None,
                                    )
                                    .await;
                            }
                        }
                        match error_body(&mut response, route.config.max_response_bytes).await {
                            Ok(bytes) => {
                                let (code, class) = ResponsesAdapter::http_failure(status, &bytes);
                                Failure {
                                    status,
                                    class,
                                    code,
                                    headers: response_headers,
                                    event: None,
                                    new_session: status == 429 && was_new,
                                }
                            }
                            Err(EgressError::LimitExceeded) => {
                                log.warn(
                                    "response_too_large",
                                    ErrorClass::Final,
                                    started,
                                    "fail",
                                    None,
                                );
                                return self
                                    .local_error(
                                        write,
                                        started,
                                        502,
                                        "response_too_large",
                                        &wait,
                                        None,
                                    )
                                    .await;
                            }
                            Err(_) => Failure::network("network", Some(&response_headers)),
                        }
                    }
                }
            };
            let mut policy = route.config.retry_policy.clone();
            if let Some(named) = named {
                policy.extend(named.clone());
            }
            let decision = self
                .retry_decision(route, &failure, &log, &policy, started, fallback.len() != 0)
                .await?;
            // Keep the single probe until its outcome has recorded the shared hold.
            drop(session);
            let complete = join
                .discard_partial()
                .map_err(|_| EgressError::InvalidProviderJson)?;
            if !complete.is_empty() {
                if !started {
                    let mut headers =
                        returned(&route.config, &failure.headers, &route.secrets().await, 200);
                    headers.push((
                        "x-symbiotic-queue-waited-ms".into(),
                        wait.total.as_millis().to_string(),
                    ));
                    head(write, 200, "text/event-stream", &headers).await?;
                    started = true;
                }
                chunk(write, &complete).await?;
            }
            match decision.action {
                RetryAction::Fail => {
                    if started && let Some(event) = failure.event {
                        let bytes = ResponsesAdapter::encode(&event)
                            .map_err(|_| EgressError::InvalidProviderJson)?;
                        chunk(write, &bytes).await?;
                        return finish(write).await;
                    }
                    if !started {
                        let secrets = route.secrets().await;
                        let mut h =
                            returned(&route.config, &failure.headers, &secrets, failure.status);
                        h.push((
                            "x-symbiotic-queue-waited-ms".into(),
                            wait.total.as_millis().to_string(),
                        ));
                        return fixed(write, failure.status, "upstream_status", &h).await;
                    }
                    return self
                        .local_error(
                            write,
                            started,
                            failure.status,
                            "upstream_disconnect",
                            &wait,
                            None,
                        )
                        .await;
                }
                RetryAction::Switch => {
                    let Some(name) = fallback.next() else {
                        return self
                            .local_error(
                                write,
                                started,
                                failure.status,
                                "upstream_status",
                                &wait,
                                None,
                            )
                            .await;
                    };
                    route = self
                        .routes
                        .iter()
                        .find(|r| &r.config.name == name)
                        .ok_or(EgressError::InvalidRequest)?;
                    slot = None;
                    authentication = None;
                }
                RetryAction::RetrySame => {}
            }
            if failure.new_session {
                slot = None;
            }
            if join.has_completed_items() {
                *buffered.body_mut() = Some(
                    join.continuation(&original)
                        .map_err(|_| EgressError::InvalidProviderJson)?
                        .into(),
                );
            }
            join.next_attempt();
        }
    }
    async fn answer(
        &self,
        route: &Route,
        response: &mut reqwest::Response,
        write: &mut (impl tokio::io::AsyncWrite + Unpin),
        started: &mut bool,
        join: &mut ItemJoin,
        wait: &mut WaitTime,
    ) -> Result<AnswerOutcome> {
        let encoding = response.headers().get("content-encoding");
        let raw = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        let kind = raw.split(';').next().unwrap_or("").trim();
        let kind = if response.headers().get("content-type").is_none()
            && matches!(
                &route.config.secret,
                SecretSource::LoginFile {
                    family: LoginFamily::ChatgptCodex,
                    ..
                }
            ) {
            "text/event-stream"
        } else {
            kind
        };
        if encoding.is_some_and(|v| !v.as_bytes().eq_ignore_ascii_case(b"identity"))
            || response.headers().get_all("content-type").iter().count() > 1
            || response
                .headers()
                .get_all("content-encoding")
                .iter()
                .count()
                > 1
            || !matches!(kind, "text/event-stream" | "application/json")
            || (*started && kind != "text/event-stream")
        {
            return Ok(AnswerOutcome::Terminated("invalid_provider_response"));
        }
        let kind = kind.to_owned();
        let mut adapter = ResponsesAdapter::default();
        let mut received = 0usize;
        let mut json = Vec::new();
        loop {
            let next = match response.chunk().await {
                Ok(c) => c,
                Err(_) => {
                    return Ok(AnswerOutcome::Failure(Failure::network(
                        "upstream_disconnect",
                        Some(response.headers()),
                    )));
                }
            };
            let Some(bytes) = next else {
                if kind == "application/json" {
                    if serde_json::from_slice::<serde_json::Value>(&json).is_err() {
                        return Ok(AnswerOutcome::Terminated("invalid_provider_json"));
                    }
                    if !*started {
                        self.answer_head(route, response, write, &kind, wait)
                            .await?;
                        *started = true;
                    }
                    chunk(write, &json).await?;
                    finish(write).await?;
                    return Ok(AnswerOutcome::Complete);
                }
                return Ok(AnswerOutcome::Failure(Failure::network(
                    "upstream_disconnect",
                    Some(response.headers()),
                )));
            };
            received = received.saturating_add(bytes.len());
            if received > route.config.max_response_bytes {
                return Ok(AnswerOutcome::Terminated("response_too_large"));
            }
            if kind == "application/json" {
                json.extend_from_slice(&bytes);
                continue;
            }
            let events = match adapter.feed(&bytes, route.config.max_held_event_bytes) {
                Ok(e) => e,
                Err(error) => {
                    let error = crate::stream::StreamError::from(error);
                    return Ok(AnswerOutcome::Terminated(error.0));
                }
            };
            for event in events {
                if let ItemEvent::Error { code, kind, .. } = &event {
                    join.add_usage(
                        &ResponsesAdapter::event_usage(&event)
                            .map_err(|_| EgressError::InvalidProviderJson)?,
                    )
                    .map_err(|_| EgressError::InvalidProviderJson)?;
                    return Ok(AnswerOutcome::Failure(Failure {
                        status: 502,
                        code: code.clone(),
                        class: *kind,
                        headers: response.headers().clone(),
                        event: Some(Box::new(event)),
                        new_session: false,
                    }));
                }
                let completed = matches!(
                    event,
                    ItemEvent::Usage {
                        completed: true,
                        ..
                    }
                );
                let clean = match join.accept(event) {
                    Ok(c) => c,
                    Err(e) => {
                        return Ok(AnswerOutcome::Terminated(e.0));
                    }
                };
                if !clean.is_empty() {
                    if !*started {
                        self.answer_head(route, response, write, &kind, wait)
                            .await?;
                        *started = true;
                    }
                    chunk(write, &clean).await?;
                }
                if completed {
                    // Step 2: settle measured usage and emit an opted-in usage trailer.
                    finish(write).await?;
                    return Ok(AnswerOutcome::Complete);
                }
            }
        }
    }
    async fn answer_head(
        &self,
        route: &Route,
        response: &reqwest::Response,
        write: &mut (impl tokio::io::AsyncWrite + Unpin),
        kind: &str,
        wait: &WaitTime,
    ) -> Result<()> {
        let mut headers = returned(
            &route.config,
            response.headers(),
            &route.secrets().await,
            response.status().as_u16(),
        );
        headers.push((
            "x-symbiotic-queue-waited-ms".into(),
            wait.total.as_millis().to_string(),
        ));
        head(write, response.status().as_u16(), kind, &headers).await
    }
}

async fn error_body(response: &mut reqwest::Response, limit: usize) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(http_error)? {
        if bytes.len().saturating_add(chunk.len()) > limit {
            return Err(EgressError::LimitExceeded);
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}
