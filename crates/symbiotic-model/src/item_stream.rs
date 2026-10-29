//! Provider-neutral complete-item events, with lossless provider envelopes.
use serde::{Deserialize, Serialize};
use serde_json::Value;
use symbiotic_trace::UsageTrace;

/// Failure classification supplied by the wire adapter, independent of retry policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorClass {
    /// Capacity or rate refusal.
    Throttle,
    /// Transport failure, including an incomplete stream.
    Network,
    /// Other provider server failure.
    Server,
    /// Authentication, invalid input, refusal or exhausted quota.
    Final,
}

/// Original provider data and neutral event identity.
#[derive(Clone, Debug)]
pub struct ItemRecord {
    /// Provider JSON, including unknown fields. Adapters rewrite only explicit edits.
    pub value: Value,
    /// Output item identity, when present.
    pub item_id: Option<String>,
    /// Attempt-local output position.
    pub output_index: Option<u64>,
    /// Original provider event sequence.
    pub provider_sequence_number: Option<u64>,
    /// Provider response identity carried by lifecycle or response events.
    pub response_id: Option<String>,
    original: Value,
    wire: Vec<u8>,
}
impl ItemRecord {
    /// Original provider bytes, before any caller-facing edits.
    pub fn wire(&self) -> &[u8] {
        &self.wire
    }
}

/// Neutral events consumed by the shared hold and join engine.
#[derive(Clone, Debug)]
pub enum ItemEvent {
    /// Initial response lifecycle event.
    ResponseStarted(ItemRecord),
    /// Response progress lifecycle event.
    ResponseInProgress(ItemRecord),
    /// Beginning of one output item.
    ItemStarted(ItemRecord),
    /// Incremental item content or function arguments.
    ItemDelta(ItemRecord),
    /// Complete opaque provider item, preserved for exact continuation replay.
    ItemDone { record: ItemRecord, item: Value },
    /// Provider usage contribution, including terminal response envelopes.
    Usage {
        /// Original envelope and neutral response identity.
        record: ItemRecord,
        /// Provider-reported token usage contribution.
        usage: UsageTrace,
        /// Whether this event completes the response.
        completed: bool,
    },
    /// Provider failure, classified by the adapter.
    Error {
        /// Original error event for a fail decision.
        record: ItemRecord,
        /// Provider error code.
        code: String,
        /// Adapter-supplied failure class.
        kind: ErrorClass,
    },
    /// Lifecycle or unknown provider event; preserved without interpretation.
    Other(ItemRecord),
}
impl ItemEvent {
    /// Access the original envelope and neutral identity.
    pub fn record(&self) -> &ItemRecord {
        match self {
            Self::ResponseStarted(r)
            | Self::ResponseInProgress(r)
            | Self::ItemStarted(r)
            | Self::ItemDelta(r)
            | Self::Other(r) => r,
            Self::ItemDone { record, .. }
            | Self::Usage { record, .. }
            | Self::Error { record, .. } => record,
        }
    }
    /// Edit only caller-facing fields, leaving the opaque replay item untouched.
    pub fn record_mut(&mut self) -> &mut ItemRecord {
        match self {
            Self::ResponseStarted(r)
            | Self::ResponseInProgress(r)
            | Self::ItemStarted(r)
            | Self::ItemDelta(r)
            | Self::Other(r) => r,
            Self::ItemDone { record, .. }
            | Self::Usage { record, .. }
            | Self::Error { record, .. } => record,
        }
    }
}

/// Static parser error; never contains provider text.
#[derive(Debug, thiserror::Error)]
pub enum ItemStreamError {
    /// Malformed provider envelope or invalid typed usage.
    #[error("invalid_provider_stream")]
    InvalidEvent,
    /// An unfinished event exceeds the configured item bound.
    #[error("max_held_event_bytes")]
    HeldEventBytesExceeded,
}

/// Explicit caller-facing join edits. Opaque replay data is separate from these edits.
pub struct JoinFields<'a> {
    /// First response identity, retained across attempts.
    pub response_id: Option<&'a str>,
    /// Next downstream sequence, when the provider supplied one.
    pub sequence_number: Option<u64>,
    /// Logical output position after accounting for prior and discarded items.
    pub output_index: Option<u64>,
    /// All complete items for a joined terminal response.
    pub completed_items: Option<&'a [Value]>,
    /// Summed provider-reported usage for a joined terminal response.
    pub usage: Option<&'a UsageTrace>,
}

/// OpenAI Responses SSE adapter. No retry or admission policy lives here.
#[derive(Default)]
pub struct ResponsesAdapter {
    records: SseRecords,
}
#[derive(Default)]
struct SseRecords {
    pending: Vec<u8>,
}
impl SseRecords {
    fn feed(&mut self, bytes: &[u8], limit: usize) -> Result<Vec<Vec<u8>>, ItemStreamError> {
        let mut out = Vec::new();
        for &byte in bytes {
            self.pending.push(byte);
            if self.pending.len() > limit {
                return Err(ItemStreamError::HeldEventBytesExceeded);
            }
            if self.pending.ends_with(b"\n\n") || self.pending.ends_with(b"\r\n\r\n") {
                out.push(std::mem::take(&mut self.pending));
            }
        }
        Ok(out)
    }
}
impl ResponsesAdapter {
    /// Decode complete SSE records, refusing an unfinished record above the item limit.
    pub fn feed(
        &mut self,
        bytes: &[u8],
        max_held_event_bytes: usize,
    ) -> Result<Vec<ItemEvent>, ItemStreamError> {
        self.records
            .feed(bytes, max_held_event_bytes)?
            .into_iter()
            .map(|wire| self.decode(wire))
            .collect()
    }
    fn decode(&mut self, wire: Vec<u8>) -> Result<ItemEvent, ItemStreamError> {
        let text = std::str::from_utf8(&wire).map_err(|_| ItemStreamError::InvalidEvent)?;
        let data: Vec<_> = text
            .lines()
            .filter_map(|line| {
                line.strip_prefix("data:")
                    .map(|s| s.strip_prefix(' ').unwrap_or(s))
            })
            .collect();
        let value: Value = if data.is_empty() {
            Value::Null
        } else {
            serde_json::from_str(&data.join("\n")).map_err(|_| ItemStreamError::InvalidEvent)?
        };
        if !data.is_empty()
            && (value["type"].as_str().is_none_or(str::is_empty)
                || value.get("delta").is_some_and(|v| !v.is_string()))
        {
            return Err(ItemStreamError::InvalidEvent);
        }
        for index in [
            "output_index",
            "sequence_number",
            "content_index",
            "summary_index",
        ] {
            if value
                .get(index)
                .is_some_and(|v| !v.is_null() && v.as_u64().is_none())
            {
                return Err(ItemStreamError::InvalidEvent);
            }
        }
        let kind = value["type"].as_str().unwrap_or("").to_owned();
        let delta = value["delta"].is_string();
        let item_id = value["item_id"]
            .as_str()
            .or_else(|| value["item"]["id"].as_str())
            .map(str::to_owned);
        let record = ItemRecord {
            item_id,
            output_index: value["output_index"].as_u64(),
            provider_sequence_number: value["sequence_number"].as_u64(),
            response_id: value
                .pointer("/response/id")
                .or_else(|| value.get("response_id"))
                .and_then(Value::as_str)
                .map(str::to_owned),
            original: value.clone(),
            value,
            wire,
        };
        Ok(match kind.as_str() {
            "response.created" => ItemEvent::ResponseStarted(record),
            "response.in_progress" => ItemEvent::ResponseInProgress(record),
            "response.output_item.added" => ItemEvent::ItemStarted(record),
            "response.output_item.done" => {
                let item = record
                    .value
                    .get("item")
                    .cloned()
                    .filter(Value::is_object)
                    .ok_or(ItemStreamError::InvalidEvent)?;
                ItemEvent::ItemDone { record, item }
            }
            "response.failed" | "response.incomplete" | "error" => {
                let code = record
                    .value
                    .pointer("/response/error/code")
                    .or_else(|| record.value.pointer("/error/code"))
                    .or_else(|| record.value.get("code"))
                    .and_then(Value::as_str)
                    .unwrap_or("upstream_failure")
                    .to_owned();
                let kind = Self::classify(None, &code);
                ItemEvent::Error { record, code, kind }
            }
            "response.completed" => {
                if !record.value["response"].is_object() {
                    return Err(ItemStreamError::InvalidEvent);
                }
                let usage = Self::usage(&record.value)?;
                ItemEvent::Usage {
                    record,
                    usage,
                    completed: true,
                }
            }
            _ if delta => ItemEvent::ItemDelta(record),
            _ => ItemEvent::Other(record),
        })
    }
    /// Re-encode an envelope byte-for-byte when its JSON has not changed.
    pub fn encode(event: &ItemEvent) -> Result<Vec<u8>, ItemStreamError> {
        let r = event.record();
        if r.value == r.original {
            return Ok(r.wire.clone());
        }
        let text = std::str::from_utf8(&r.wire).map_err(|_| ItemStreamError::InvalidEvent)?;
        let mut out = Vec::new();
        // Keep non-data SSE fields and comments. Only the JSON data is replaced.
        for line in text
            .lines()
            .filter(|l| !l.is_empty() && !l.starts_with("data:"))
        {
            out.extend_from_slice(line.as_bytes());
            out.push(b'\n');
        }
        out.extend_from_slice(b"data: ");
        out.extend(serde_json::to_vec(&r.value).map_err(|_| ItemStreamError::InvalidEvent)?);
        out.extend_from_slice(b"\n\n");
        Ok(out)
    }
    /// Apply only specified join fields, preserving every other provider JSON field.
    pub fn rewrite(event: &mut ItemEvent, fields: JoinFields<'_>) -> Result<(), ItemStreamError> {
        let v = &mut event.record_mut().value;
        if v.is_null() {
            return Ok(());
        }
        if let Some(n) = fields.sequence_number {
            v["sequence_number"] = serde_json::json!(n);
        }
        if let Some(index) = fields.output_index {
            v["output_index"] = serde_json::json!(index);
        }
        if let Some(items) = fields.completed_items {
            v["response"]["output"] = serde_json::json!(items);
        }
        if let Some(usage) = fields.usage {
            let u = &mut v["response"]["usage"];
            if !u.is_object() {
                *u = serde_json::json!({});
            }
            for (key, count) in [
                ("input_tokens", usage.input_tokens),
                ("output_tokens", usage.output_tokens),
            ] {
                if let Some(n) = count {
                    u[key] = serde_json::json!(n);
                }
            }
            if let Some(n) = usage.cache_hit_tokens {
                u["input_tokens_details"]["cached_tokens"] = serde_json::json!(n);
            }
            if let Some(n) = usage.reasoning_tokens {
                u["output_tokens_details"]["reasoning_tokens"] = serde_json::json!(n);
            }
            if let (Some(i), Some(o)) = (usage.input_tokens, usage.output_tokens) {
                u["total_tokens"] =
                    serde_json::json!(i.checked_add(o).ok_or(ItemStreamError::InvalidEvent)?);
            }
        }
        if let Some(id) = fields.response_id {
            fn ids(v: &mut Value, id: &str) {
                match v {
                    Value::Object(o) => {
                        if let Some(r) = o.get_mut("response")
                            && r.get("id").is_some()
                        {
                            r["id"] = serde_json::json!(id);
                        }
                        if o.contains_key("response_id") {
                            o.insert("response_id".into(), serde_json::json!(id));
                        }
                        for v in o.values_mut() {
                            ids(v, id);
                        }
                    }
                    Value::Array(a) => {
                        for v in a {
                            ids(v, id);
                        }
                    }
                    _ => {}
                }
            }
            ids(v, id);
        }
        Ok(())
    }
    /// Encode a continuation from the original request and opaque complete items.
    pub fn continuation_request(
        original: &[u8],
        items: &[Value],
    ) -> Result<Vec<u8>, ItemStreamError> {
        let mut body: Value =
            serde_json::from_slice(original).map_err(|_| ItemStreamError::InvalidEvent)?;
        let input = body.get_mut("input").ok_or(ItemStreamError::InvalidEvent)?;
        if let Value::String(text) = input {
            *input = serde_json::json!([{"role":"user","content":text}]);
        }
        input
            .as_array_mut()
            .ok_or(ItemStreamError::InvalidEvent)?
            .extend(items.iter().cloned());
        serde_json::to_vec(&body).map_err(|_| ItemStreamError::InvalidEvent)
    }

    /// Decode an HTTP error code before applying this adapter's classification.
    /// Non-JSON error bodies retain their HTTP status as the health code.
    pub fn http_failure(status: u16, body: &[u8]) -> (String, ErrorClass) {
        let value: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
        let code = value
            .pointer("/error/code")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .unwrap_or_else(|| status.to_string());
        let class = Self::classify(Some(status), &code);
        (code, class)
    }

    /// Classify provider errors. Route extensions are applied by the policy engine.
    pub fn classify(status: Option<u16>, code: &str) -> ErrorClass {
        match code {
            "insufficient_quota"
            | "quota_exceeded"
            | "quota_exhausted"
            | "context_length_exceeded"
            | "content_policy_violation"
            | "cyber_policy"
            | "invalid_api_key" => ErrorClass::Final,
            "server_is_overloaded" | "rate_limit_exceeded" | "slow_down" => ErrorClass::Throttle,
            "network" | "upstream_disconnect" => ErrorClass::Network,
            _ if status == Some(429) => ErrorClass::Throttle,
            _ if status.is_some_and(|s| (500..600).contains(&s)) => ErrorClass::Server,
            _ => ErrorClass::Final,
        }
    }
    /// Recognize only the pinned provider rate-limit header grammar.
    pub fn rate_header(name: &str) -> bool {
        if matches!(
            name,
            "x-codex-credits-has-credits"
                | "x-codex-credits-unlimited"
                | "x-codex-credits-balance"
                | "x-codex-rate-limit-reached-type"
        ) {
            return true;
        }
        let Some(name) = name.strip_prefix("x-") else {
            return false;
        };
        [
            "-limit-name",
            "-primary-used-percent",
            "-primary-window-minutes",
            "-primary-reset-at",
            "-secondary-used-percent",
            "-secondary-window-minutes",
            "-secondary-reset-at",
        ]
        .iter()
        .any(|suffix| {
            name.strip_suffix(suffix).is_some_and(|family| {
                !family.is_empty()
                    && family
                        .bytes()
                        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
            })
        })
    }
    /// Read a terminal attempt's usage without interpreting its error policy.
    pub fn event_usage(event: &ItemEvent) -> Result<UsageTrace, ItemStreamError> {
        Self::usage(&event.record().value)
    }
    fn usage(value: &Value) -> Result<UsageTrace, ItemStreamError> {
        let mut usage = decode_usage(
            &value["response"]["usage"],
            "input_tokens",
            "output_tokens",
            "input_tokens_details",
            "output_tokens_details",
        )?;
        usage.response_id = value["response"]["id"].as_str().map(str::to_owned);
        usage.served_model = value["response"]["model"].as_str().map(str::to_owned);
        Ok(usage)
    }
}

fn decode_usage(
    u: &Value,
    input: &str,
    output: &str,
    input_details: &str,
    output_details: &str,
) -> Result<UsageTrace, ItemStreamError> {
    if !u.is_null() && !u.is_object() {
        return Err(ItemStreamError::InvalidEvent);
    }
    for details in [input_details, output_details] {
        if u.get(details)
            .is_some_and(|v| !v.is_null() && !v.is_object())
        {
            return Err(ItemStreamError::InvalidEvent);
        }
    }
    let number = |v: &Value| {
        if v.is_null() {
            Ok(None)
        } else {
            v.as_u64().map(Some).ok_or(ItemStreamError::InvalidEvent)
        }
    };
    let input = number(&u[input])?;
    let cached = number(&u[input_details]["cached_tokens"])?;
    if cached.zip(input).is_some_and(|(c, i)| c > i) {
        return Err(ItemStreamError::InvalidEvent);
    }
    Ok(UsageTrace {
        input_tokens: input,
        output_tokens: number(&u[output])?,
        cache_hit_tokens: cached,
        cache_miss_tokens: input.zip(cached).map(|(i, c)| i - c),
        reasoning_tokens: number(&u[output_details]["reasoning_tokens"])?,
        ..Default::default()
    })
}

/// Chat Completions decoder. Deltas have no replayable item identity.
/// Consumers relay original bytes and may retry only before releasing bytes.
#[derive(Default)]
pub struct ChatCompletionsAdapter {
    records: SseRecords,
}
impl ChatCompletionsAdapter {
    /// Decode complete SSE records, preserving comments, unknown fields and `[DONE]`.
    pub fn feed(&mut self, bytes: &[u8], limit: usize) -> Result<Vec<ItemEvent>, ItemStreamError> {
        self.records
            .feed(bytes, limit)?
            .into_iter()
            .map(|wire| {
                let text = std::str::from_utf8(&wire).map_err(|_| ItemStreamError::InvalidEvent)?;
                let data: Vec<_> = text
                    .lines()
                    .filter_map(|l| {
                        l.strip_prefix("data:")
                            .map(|d| d.strip_prefix(' ').unwrap_or(d))
                    })
                    .collect();
                if data.is_empty() {
                    return Ok(ItemEvent::Other(Self::record(Value::Null, wire)));
                }
                let data = data.join("\n");
                if data == "[DONE]" {
                    return Ok(ItemEvent::Usage {
                        record: Self::record(Value::Null, wire),
                        usage: UsageTrace::default(),
                        completed: true,
                    });
                }
                let value =
                    serde_json::from_str(&data).map_err(|_| ItemStreamError::InvalidEvent)?;
                Self::decode(value, wire, false)
            })
            .collect()
    }
    /// Decode one non-streaming Chat Completions JSON response.
    pub fn json(bytes: &[u8]) -> Result<ItemEvent, ItemStreamError> {
        let value = serde_json::from_slice(bytes).map_err(|_| ItemStreamError::InvalidEvent)?;
        Self::decode(value, bytes.to_vec(), true)
    }
    fn record(value: Value, wire: Vec<u8>) -> ItemRecord {
        ItemRecord {
            response_id: value["id"].as_str().map(str::to_owned),
            original: value.clone(),
            value,
            wire,
            item_id: None,
            output_index: None,
            provider_sequence_number: None,
        }
    }
    fn decode(value: Value, wire: Vec<u8>, completed: bool) -> Result<ItemEvent, ItemStreamError> {
        if !value.is_object() {
            return Err(ItemStreamError::InvalidEvent);
        }
        if let Some(error) = value.get("error") {
            if !error.is_object() {
                return Err(ItemStreamError::InvalidEvent);
            }
            let code = error["code"]
                .as_str()
                .or_else(|| error["type"].as_str())
                .unwrap_or("upstream_failure")
                .to_owned();
            let kind = ResponsesAdapter::classify(None, &code);
            return Ok(ItemEvent::Error {
                record: Self::record(value, wire),
                code,
                kind,
            });
        }
        let choices = value["choices"]
            .as_array()
            .ok_or(ItemStreamError::InvalidEvent)?;
        if choices.iter().any(|c| {
            !c.is_object()
                || c["index"].as_u64().is_none()
                || c.get("delta").is_some_and(|v| !v.is_object())
                || c.get("message").is_some_and(|v| !v.is_object())
                || c.get("finish_reason")
                    .is_some_and(|v| !v.is_null() && !v.is_string())
        }) {
            return Err(ItemStreamError::InvalidEvent);
        }
        let usage = decode_usage(
            &value["usage"],
            "prompt_tokens",
            "completion_tokens",
            "prompt_tokens_details",
            "completion_tokens_details",
        )?;
        let record = Self::record(value, wire);
        if completed || !record.value["usage"].is_null() {
            Ok(ItemEvent::Usage {
                record,
                usage,
                completed,
            })
        } else {
            Ok(ItemEvent::ItemDelta(record))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn chat_completions_chunks_preserve_wire_and_decode_usage_at_every_split() {
        let wire = b": keep\r\ndata: {\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"part\"}}]},\"finish_reason\":null}],\"unknown\":1.2300}\r\n\r\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":7,\"completion_tokens\":3,\"prompt_tokens_details\":{\"cached_tokens\":2},\"completion_tokens_details\":{\"reasoning_tokens\":1}}}\n\ndata: [DONE]\n\n";
        for split in 0..=wire.len() {
            let mut adapter = ChatCompletionsAdapter::default();
            let mut events = adapter.feed(&wire[..split], 4096).unwrap();
            events.extend(adapter.feed(&wire[split..], 4096).unwrap());
            assert_eq!(
                events
                    .iter()
                    .flat_map(|e| e.record().wire().iter().copied())
                    .collect::<Vec<_>>(),
                wire
            );
            assert!(matches!(events[0], ItemEvent::ItemDelta(_)));
            let ItemEvent::Usage {
                usage,
                completed: false,
                ..
            } = &events[1]
            else {
                panic!("reported usage")
            };
            assert_eq!(
                (
                    usage.input_tokens,
                    usage.output_tokens,
                    usage.cache_hit_tokens,
                    usage.cache_miss_tokens,
                    usage.reasoning_tokens
                ),
                (Some(7), Some(3), Some(2), Some(5), Some(1))
            );
            assert!(matches!(
                events[2],
                ItemEvent::Usage {
                    completed: true,
                    ..
                }
            ));
        }
    }
    #[test]
    fn chat_completions_json_preserves_missing_usage_and_classifies_errors() {
        let event = ChatCompletionsAdapter::json(
            br#"{"choices":[{"index":0,"message":{"content":"answer"},"finish_reason":"stop"}]}"#,
        )
        .unwrap();
        let ItemEvent::Usage {
            usage,
            completed: true,
            ..
        } = event
        else {
            panic!("JSON response")
        };
        assert_eq!(
            serde_json::to_value(usage).unwrap(),
            serde_json::to_value(UsageTrace::default()).unwrap()
        );
        let error = ChatCompletionsAdapter::json(br#"{"error":{"code":"slow_down"}}"#).unwrap();
        assert!(matches!(
            error,
            ItemEvent::Error {
                kind: ErrorClass::Throttle,
                ..
            }
        ));
        for value in [
            serde_json::json!({"choices":{},"usage":{}}),
            serde_json::json!({"choices":[{"index":-1}]}),
            serde_json::json!({"choices":[],"usage":{"prompt_tokens":-1}}),
            serde_json::json!({"choices":[],"usage":{"completion_tokens":"3"}}),
            serde_json::json!({"choices":[],"usage":{"prompt_tokens":1,"prompt_tokens_details":{"cached_tokens":2}}}),
        ] {
            assert!(ChatCompletionsAdapter::json(&serde_json::to_vec(&value).unwrap()).is_err());
        }
        assert!(matches!(
            ChatCompletionsAdapter::default().feed(b"data: ", 5),
            Err(ItemStreamError::HeldEventBytesExceeded)
        ));
    }
    #[test]
    fn responses_sequence_round_trips_with_unknown_fields_and_metadata() {
        let wire = b": comment\r\nevent: response.created\r\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"r\"},\"extra\":1.2300}\r\n\r\ndata: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"reasoning\",\"encrypted_content\":\"opaque\"}}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":2,\"output_tokens\":3}}}\n\n";
        for split in 0..=wire.len() {
            let mut adapter = ResponsesAdapter::default();
            let mut events = adapter.feed(&wire[..split], 4096).unwrap();
            events.extend(adapter.feed(&wire[split..], 4096).unwrap());
            assert_eq!(
                events
                    .iter()
                    .flat_map(|e| ResponsesAdapter::encode(e).unwrap())
                    .collect::<Vec<_>>(),
                wire
            );
        }
    }
    #[test]
    fn function_argument_deltas_keep_call_identity_and_original_fragments() {
        let values = [
            serde_json::json!({"type":"response.output_item.added","sequence_number":0,"output_index":0,"item":{"id":"call-item","type":"function_call","call_id":"function-call","name":"lookup","arguments":""}}),
            serde_json::json!({"type":"response.function_call_arguments.delta","sequence_number":1,"output_index":0,"item_id":"call-item","call_id":"function-call","delta":"{\"x\":"}),
            serde_json::json!({"type":"response.function_call_arguments.delta","sequence_number":2,"output_index":0,"item_id":"call-item","delta":"1}"}),
        ];
        let wire: Vec<u8> = values
            .into_iter()
            .flat_map(|v| format!("data: {v}\n\n").into_bytes())
            .collect();
        let events = ResponsesAdapter::default().feed(&wire, 4096).unwrap();
        assert_eq!(
            events
                .iter()
                .flat_map(|e| ResponsesAdapter::encode(e).unwrap())
                .collect::<Vec<_>>(),
            wire
        );
    }
    #[test]
    fn malformed_responses_records_are_refused_instead_of_forwarded_as_unknown_events() {
        for value in [
            serde_json::json!({"choices":[{"delta":{"content":"text"}}]}),
            serde_json::json!([]),
            serde_json::json!({"type":""}),
            serde_json::json!({"type":"response.output_text.delta","delta":{"text":"wrong shape"}}),
        ] {
            assert!(
                ResponsesAdapter::default()
                    .feed(format!("data: {value}\n\n").as_bytes(), 4096)
                    .is_err()
            );
        }
    }
    #[test]
    fn terminal_usage_preserves_absence_and_rejects_invalid_counts() {
        for response in [
            serde_json::json!("invalid"),
            serde_json::json!({"usage":{"input_tokens_details":5}}),
            serde_json::json!({"usage":{"output_tokens_details":[]}}),
        ] {
            let value = serde_json::json!({"type":"response.completed","response":response});
            assert!(
                ResponsesAdapter::default()
                    .feed(format!("data: {value}\n\n").as_bytes(), 4096)
                    .is_err()
            );
        }
        let wire = b"data: {\"type\":\"response.completed\",\"response\":{\"usage\":{}}}\n\n";
        let event = ResponsesAdapter::default()
            .feed(wire, 4096)
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(
            serde_json::to_value(ResponsesAdapter::event_usage(&event).unwrap()).unwrap(),
            serde_json::to_value(UsageTrace::default()).unwrap()
        );
        for invalid in [
            serde_json::json!(-1),
            serde_json::json!(1.5),
            serde_json::json!("12"),
            serde_json::json!({}),
        ] {
            let v = serde_json::json!({"type":"response.completed","response":{"usage":{"input_tokens":invalid}}});
            assert!(
                ResponsesAdapter::default()
                    .feed(format!("data: {v}\n\n").as_bytes(), 4096)
                    .is_err()
            );
        }
        assert!(
            ResponsesAdapter::default()
                .feed(
                    b"data: {\"type\":\"response.completed\",\"response\":{\"usage\":[]}}\n\n",
                    4096
                )
                .is_err()
        );
    }
    #[test]
    fn cyber_policy_refusal_is_final_regardless_of_http_status() {
        for status in [None, Some(429), Some(500), Some(503)] {
            assert_eq!(
                ResponsesAdapter::classify(status, "cyber_policy"),
                ErrorClass::Final
            );
        }
    }

    #[test]
    fn provider_failure_and_rate_header_classification_is_exact() {
        assert_eq!(
            ResponsesAdapter::http_failure(503, br#"{"error":{"code":"server_is_overloaded"}}"#),
            ("server_is_overloaded".into(), ErrorClass::Throttle)
        );
        assert_eq!(
            ResponsesAdapter::http_failure(503, b"not JSON"),
            ("503".into(), ErrorClass::Server)
        );

        for (status, code, expected) in [
            (429, "", ErrorClass::Throttle),
            (503, "slow_down", ErrorClass::Throttle),
            (503, "other", ErrorClass::Server),
            (401, "", ErrorClass::Final),
            (429, "insufficient_quota", ErrorClass::Final),
            (429, "quota_exhausted", ErrorClass::Final),
            (0, "network", ErrorClass::Network),
        ] {
            assert_eq!(ResponsesAdapter::classify(Some(status), code), expected);
        }
        for family in ["codex", "other-server-limit"] {
            assert!(ResponsesAdapter::rate_header(&format!(
                "x-{family}-primary-used-percent"
            )));
        }
        assert!(!ResponsesAdapter::rate_header("x-codex-promo-message"));
    }
}
