//! Pure credential redaction and usage decoding for passthrough responses.
//!
//! Record and delta handling is ported from Warden proxy.rs at 8996b94.
use serde::{Deserialize, Serialize};
use symbiotic_ai_runtime::model;

/// Token usage returned by the existing runtime, without inferred counts or cost.
pub type Usage = symbiotic_trace::UsageTrace;

/// Provider event format configured by a passthrough route.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamDialect {
    /// OpenAI Responses events.
    OpenaiResponses,
    /// OpenAI Chat Completions chunks.
    OpenaiChatCompletions,
}

/// A fixed diagnostic that never contains upstream data or credentials.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StreamError(&'static str);

impl std::fmt::Display for StreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for StreamError {}
impl From<serde_json::Error> for StreamError {
    fn from(_: serde_json::Error) -> Self {
        Self("malformed upstream JSON record")
    }
}

const RECORD_LIMIT: usize = 4 * 1024 * 1024;
const HELD_TEXT_LIMIT: usize = 4 * 1024 * 1024;
const HELD_EVENT_LIMIT: usize = 64 * 1024 * 1024;

/// Format the `symbiotic-usage` trailer value in its fixed field order.
/// Missing fields are omitted; no fields yields an empty value.
pub fn usage_trailer(usage: &Usage) -> String {
    [
        ("input_tokens", usage.input_tokens),
        ("cached_input_tokens", usage.cache_hit_tokens),
        ("output_tokens", usage.output_tokens),
        ("reasoning_tokens", usage.reasoning_tokens),
    ]
    .into_iter()
    .filter_map(|(name, count)| count.map(|count| format!("{name}={count}")))
    .collect::<Vec<_>>()
    .join(", ")
}

// Retain enough suffix bytes that a credential split over arbitrarily many
// upstream reads cannot escape. Matching precedes emission; no secret prefix is
// emitted before the following bytes can be inspected.
struct Scrubber {
    secrets: std::sync::Arc<Vec<Vec<u8>>>,
    pending: Vec<u8>,
}
impl Scrubber {
    fn new(secrets: Vec<Vec<u8>>) -> Self {
        Self {
            secrets: std::sync::Arc::new(secrets),
            pending: Vec::new(),
        }
    }
    fn shared(secrets: std::sync::Arc<Vec<Vec<u8>>>) -> Self {
        Self {
            secrets,
            pending: Vec::new(),
        }
    }
    fn feed(&mut self, input: &[u8], finished: bool) -> Vec<u8> {
        self.pending.extend_from_slice(input);
        let mut output = Vec::new();
        let mut offset = 0;
        'pending: while offset < self.pending.len() {
            let mut end = offset;
            let mut cursor = offset;
            // Inspect every start inside the redacted span, extending it for
            // overlaps. Adjacent matches begin a separate span. Secret list
            // ordering must not affect either the span or partial-match holding.
            while cursor == offset || cursor < end {
                let remaining = &self.pending[cursor..];
                if !finished
                    && self.secrets.iter().any(|secret| {
                        secret.len() > remaining.len() && secret.starts_with(remaining)
                    })
                {
                    // A longer or overlapping match can still grow this span.
                    // Retain it until the next read or EOF resolves that match.
                    break 'pending;
                }
                let longest = self
                    .secrets
                    .iter()
                    .filter(|secret| remaining.starts_with(secret))
                    .map(Vec::len)
                    .max()
                    .unwrap_or(0);
                end = end.max(cursor + longest);
                cursor += 1;
            }
            if end > offset {
                output.extend_from_slice(b"[redacted]");
                offset = end;
            } else {
                output.push(self.pending[offset]);
                offset += 1;
            }
        }
        self.pending.drain(..offset);
        output
    }
}

// Decode complete JSON strings before matching secrets: JSON's many equivalent
// Unicode/escape encodings cannot be enumerated safely as byte patterns.
fn scrub_json(input: &[u8], secrets: &[Vec<u8>]) -> Result<Vec<u8>, StreamError> {
    fn scrub_text(text: &str, secrets: &[Vec<u8>]) -> Result<String, StreamError> {
        String::from_utf8(Scrubber::new(secrets.to_vec()).feed(text.as_bytes(), true))
            .map_err(|_| StreamError("invalid redacted JSON string"))
    }
    fn scrub(value: &mut serde_json::Value, secrets: &[Vec<u8>]) -> Result<(), StreamError> {
        match value {
            serde_json::Value::String(text) => *text = scrub_text(text, secrets)?,
            serde_json::Value::Array(values) => {
                for value in values {
                    scrub(value, secrets)?;
                }
            }
            serde_json::Value::Object(values) => {
                let original = std::mem::take(values);
                for (key, mut value) in original {
                    scrub(&mut value, secrets)?;
                    if values.insert(scrub_text(&key, secrets)?, value).is_some() {
                        return Err(StreamError("redaction produced duplicate JSON keys"));
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }
    let mut value: serde_json::Value =
        serde_json::from_slice(input).map_err(|_| StreamError("malformed upstream JSON record"))?;
    scrub(&mut value, secrets)?;
    serde_json::to_vec(&value).map_err(|_| StreamError("cannot serialize redacted JSON"))
}

// A stream key identifies one concatenated model output, independent of the
// network read boundary and of unrelated choices, content parts or tool calls.
struct DeltaState {
    text: Vec<u8>,
    last_event: usize,
    pointer: String,
}
struct SseEvent {
    fields: Vec<u8>,
    value: Option<serde_json::Value>,
}

fn delta_fields(dialect: StreamDialect, value: &serde_json::Value) -> Vec<(String, String)> {
    fn strings(
        value: &serde_json::Value,
        pointer: String,
        key: String,
        out: &mut Vec<(String, String)>,
    ) {
        match value {
            serde_json::Value::String(_) => out.push((key, pointer)),
            serde_json::Value::Array(values) => {
                for (position, value) in values.iter().enumerate() {
                    let index = value
                        .get("index")
                        .and_then(serde_json::Value::as_u64)
                        .unwrap_or(position as u64);
                    strings(
                        value,
                        format!("{pointer}/{position}"),
                        format!("{key}/{index}"),
                        out,
                    );
                }
            }
            serde_json::Value::Object(values) => {
                for (name, value) in values {
                    // Role/type/index describe framing rather than concatenated output.
                    if matches!(name.as_str(), "type" | "role" | "index") {
                        continue;
                    }
                    let escaped = name.replace('~', "~0").replace('/', "~1");
                    strings(
                        value,
                        format!("{pointer}/{escaped}"),
                        format!("{key}/{escaped}"),
                        out,
                    );
                }
            }
            _ => {}
        }
    }
    let mut fields = Vec::new();
    if dialect == StreamDialect::OpenaiChatCompletions
        && let Some(choices) = value.get("choices").and_then(serde_json::Value::as_array)
    {
        for (position, choice) in choices.iter().enumerate() {
            if let Some(delta) = choice.get("delta") {
                let index = choice
                    .get("index")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(position as u64);
                strings(
                    delta,
                    format!("/choices/{position}/delta"),
                    format!("chat/{index}"),
                    &mut fields,
                );
            }
        }
    }
    if dialect == StreamDialect::OpenaiResponses
        && value.get("delta").is_some_and(serde_json::Value::is_string)
    {
        let kind = value
            .get("type")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("delta");
        let identity = serde_json::json!([
            kind,
            value.get("item_id"),
            value.get("output_index"),
            value.get("content_index"),
            value.get("call_id")
        ]);
        fields.push((identity.to_string(), "/delta".into()));
    }
    fields
}

/// Pure response filter. Delta events stay held until completion or EOF.
///
/// Text is grouped by choice, content part and tool-call index. The network read
/// boundary never affects matching or release. After an error, no further bytes
/// or usage may be released from this filter.
pub struct ResponseFilter {
    dialect: StreamDialect,
    usage: Option<Usage>,
    usage_complete: bool,
    usage_rejected: bool,
    failed: bool,
    ended: bool,
    sse: bool,
    pending: Vec<u8>,
    secrets: std::sync::Arc<Vec<Vec<u8>>>,
    deltas: std::collections::BTreeMap<String, DeltaState>,
    events: Vec<SseEvent>,
    held_event_bytes: usize,
    held_text_bytes: usize,
    finalized: bool,
}
impl ResponseFilter {
    /// Expand nonempty UTF-8 credentials to the credential boundary's encodings.
    /// Empty credentials are ignored, as at the existing credential boundary.
    pub fn new(
        dialect: StreamDialect,
        event_stream: bool,
        secrets: &[&str],
    ) -> Result<Self, StreamError> {
        let mut encodings = Vec::new();
        for secret in secrets.iter().filter(|secret| !secret.is_empty()) {
            let variants = model::credential_encodings(secret)
                .map_err(|_| StreamError("cannot encode credential"))?;
            encodings.extend(variants.iter().map(|value| value.as_bytes().to_vec()));
        }
        encodings.sort();
        encodings.dedup();
        Ok(Self {
            dialect,
            usage: None,
            usage_complete: false,
            usage_rejected: false,
            failed: false,
            ended: false,
            sse: event_stream,
            secrets: std::sync::Arc::new(encodings),
            pending: Vec::new(),
            deltas: Default::default(),
            events: Vec::new(),
            held_event_bytes: 0,
            held_text_bytes: 0,
            finalized: false,
        })
    }

    /// Measured usage, available only after a successful end-of-body feed.
    /// A failed or unfinished body has unknown usage.
    pub fn usage(&self) -> Option<&Usage> {
        self.usage
            .as_ref()
            .filter(|_| self.ended && !self.failed && self.usage_complete && !self.usage_rejected)
    }

    /// Return complete, scrubbed records that are safe to release.
    /// `finished` marks EOF. Errors are terminal and invalidate observed usage.
    pub fn feed(&mut self, bytes: &[u8], finished: bool) -> Result<Vec<u8>, StreamError> {
        if self.failed {
            return Err(StreamError("stream already failed"));
        }
        if self.ended {
            return if bytes.is_empty() && finished {
                Ok(Vec::new())
            } else {
                self.failed = true;
                self.usage = None;
                Err(StreamError("bytes after end of body"))
            };
        }
        let result = self.feed_records(bytes, finished);
        if result.is_err() {
            self.failed = true;
            self.usage = None;
        } else if finished {
            self.ended = true;
        }
        result
    }

    fn read_usage(&mut self, value: &serde_json::Value, sse: bool) -> Result<(), StreamError> {
        let kind = value.get("type").and_then(serde_json::Value::as_str);
        if matches!(
            kind,
            Some("response.failed" | "response.incomplete" | "error")
        ) || value.get("error").is_some_and(|error| !error.is_null())
            || value
                .pointer("/response/error")
                .is_some_and(|error| !error.is_null())
            || value
                .get("status")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|status| matches!(status, "failed" | "incomplete"))
            || value
                .pointer("/response/status")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|status| matches!(status, "failed" | "incomplete"))
        {
            self.usage_rejected = true;
            self.usage = None;
            return Ok(());
        }
        if self.usage_rejected {
            return Ok(());
        }
        let raw = match self.dialect {
            StreamDialect::OpenaiResponses => {
                if sse && kind != Some("response.completed") {
                    return Ok(());
                }
                self.usage = None;
                self.usage_complete = true;
                if sse {
                    value.get("response").unwrap_or(&serde_json::Value::Null)
                } else {
                    value
                }
            }
            StreamDialect::OpenaiChatCompletions => {
                // Only the last JSON chunk can supply chat usage. [DONE] confirms
                // stream completion; EOF before it leaves the receipt unknown.
                self.usage = None;
                self.usage_complete = !sse;
                value
            }
        };
        let Some(counts) = raw.get("usage").filter(|value| !value.is_null()) else {
            return Ok(());
        };
        if !counts.is_object() {
            return Err(StreamError("invalid upstream usage"));
        }
        let (input, cached, output, reasoning) = match self.dialect {
            StreamDialect::OpenaiResponses => (
                "/input_tokens",
                "/input_tokens_details/cached_tokens",
                "/output_tokens",
                "/output_tokens_details/reasoning_tokens",
            ),
            StreamDialect::OpenaiChatCompletions => (
                "/prompt_tokens",
                "/prompt_tokens_details/cached_tokens",
                "/completion_tokens",
                "/completion_tokens_details/reasoning_tokens",
            ),
        };
        let count = |pointer| match counts.pointer(pointer) {
            None | Some(serde_json::Value::Null) => Ok(None),
            Some(value) => value
                .as_u64()
                .map(Some)
                .ok_or(StreamError("invalid upstream token count")),
        };
        // Reuse the typed-route screening without a clock, random ID or trace I/O.
        // These local placeholders are never emitted or stored.
        let mut trace = symbiotic_trace::ModelInvocationTrace {
            trace_id: serde_json::from_value(serde_json::Value::String(String::new()))?,
            queue_item_id: None,
            model: model::ModelIdentity::new("", "", ""),
            role_binding: None,
            source: None,
            request_hash: String::new(),
            response_hash: None,
            cache: Default::default(),
            usage: Usage {
                input_tokens: count(input)?,
                cache_hit_tokens: count(cached)?,
                output_tokens: count(output)?,
                reasoning_tokens: count(reasoning)?,
                ..Usage::default()
            },
            timing: Default::default(),
            outcome: symbiotic_trace::InvocationOutcome::Succeeded,
            error_class: None,
            audit_refs: Vec::new(),
            metadata: serde_json::Value::Null,
            timestamp: chrono::DateTime::UNIX_EPOCH,
        };
        model::provider_usage_identity(&mut trace, raw, |identity| {
            // Passthrough never decodes the request. Screen against the logical
            // streamed output and credential encodings that this filter owns.
            self.deltas.values().any(|state| {
                state
                    .text
                    .windows(identity.len())
                    .any(|part| part == identity.as_bytes())
            }) || self.secrets.iter().any(|secret| {
                identity
                    .as_bytes()
                    .windows(secret.len())
                    .any(|part| part == secret)
            })
        });
        self.usage = Some(trace.usage);
        Ok(())
    }

    fn flush_events(&mut self) -> Result<Vec<u8>, StreamError> {
        let mut output = Vec::new();
        for event in self.events.drain(..) {
            output.extend(event.fields);
            if let Some(value) = event.value {
                output.extend_from_slice(b"data: ");
                output.extend(
                    serde_json::to_vec(&value)
                        .map_err(|_| StreamError("cannot serialize redacted event"))?,
                );
                output.extend_from_slice(b"\n\n");
            }
        }
        self.held_event_bytes = 0;
        self.held_text_bytes = 0;
        self.deltas.clear();
        Ok(output)
    }

    fn finish_deltas(&mut self) -> Result<Vec<u8>, StreamError> {
        if self.finalized {
            return Ok(Vec::new());
        }
        self.finalized = true;
        for state in self.deltas.values_mut() {
            let tail = Scrubber::shared(self.secrets.clone()).feed(&state.text, true);
            if !tail.is_empty() {
                let text = self
                    .events
                    .get(state.last_event)
                    .and_then(|event| {
                        event
                            .value
                            .as_ref()
                            .and_then(|value| value.pointer(&state.pointer))
                    })
                    .and_then(serde_json::Value::as_str)
                    .ok_or(StreamError("missing held delta"))?
                    .to_owned();
                let tail =
                    String::from_utf8(tail).map_err(|_| StreamError("invalid redacted delta"))?;
                *self.events[state.last_event]
                    .value
                    .as_mut()
                    .and_then(|value| value.pointer_mut(&state.pointer))
                    .ok_or(StreamError("missing held delta"))? =
                    serde_json::Value::String(text + &tail);
            }
        }
        self.flush_events()
    }

    fn feed_records(&mut self, input: &[u8], finished: bool) -> Result<Vec<u8>, StreamError> {
        self.pending.extend_from_slice(input);
        let mut output = Vec::new();
        if self.sse {
            while let Some(end) = self
                .pending
                .windows(2)
                .position(|w| w == b"\n\n")
                .map(|p| p + 2)
                .into_iter()
                .chain(
                    self.pending
                        .windows(4)
                        .position(|w| w == b"\r\n\r\n")
                        .map(|p| p + 4),
                )
                .min()
            {
                if end > RECORD_LIMIT {
                    return Err(StreamError("upstream SSE record exceeds limit"));
                }
                let record = self.pending.drain(..end).collect::<Vec<_>>();
                output.extend(self.sse_record(&record)?);
            }
        }
        if self.pending.len() > RECORD_LIMIT {
            return Err(StreamError("upstream record exceeds limit"));
        }
        if finished && !self.pending.is_empty() {
            if self.sse {
                return Err(StreamError("incomplete upstream SSE record"));
            }
            let value = serde_json::from_slice(&self.pending)?;
            self.read_usage(&value, false)?;
            output.extend(scrub_json(&self.pending, &self.secrets)?);
            self.pending.clear();
        } else if finished && !self.sse {
            return Err(StreamError("empty upstream JSON response"));
        }
        if finished && self.sse {
            output.extend(self.finish_deltas()?);
        }
        Ok(output)
    }
    fn sse_record(&mut self, record: &[u8]) -> Result<Vec<u8>, StreamError> {
        let text = std::str::from_utf8(record)
            .map_err(|_| StreamError("invalid upstream SSE encoding"))?;
        let mut data = Vec::new();
        let mut fields = Vec::new();
        for line in text.lines().filter(|line| !line.is_empty()) {
            if let Some(value) = line.strip_prefix("data:") {
                data.push(value.strip_prefix(' ').unwrap_or(value));
            } else if line == "data" {
                data.push("");
            } else {
                fields.extend_from_slice(line.as_bytes());
                fields.push(b'\n');
            }
        }
        // Only plain SSE metadata uses byte matching. Re-scrubbing serialized
        // JSON could replace syntax when a credential contains quote characters.
        let mut fields = Scrubber::shared(self.secrets.clone()).feed(&fields, true);
        if !data.is_empty() {
            let data = data.join("\n");
            if data == "[DONE]" {
                if self.dialect == StreamDialect::OpenaiChatCompletions {
                    self.usage_complete = true;
                }
                let mut output = self.finish_deltas()?;
                fields.extend_from_slice(b"data: [DONE]\n\n");
                output.extend(fields);
                return Ok(output);
            }
            let mut value: serde_json::Value = serde_json::from_str(&data)
                .map_err(|_| StreamError("malformed upstream JSON record"))?;
            self.read_usage(&value, true)?;
            let terminal = matches!(
                value.get("type").and_then(serde_json::Value::as_str),
                Some("response.completed" | "response.failed" | "response.incomplete")
            );
            let mut output = if terminal {
                self.finish_deltas()?
            } else {
                Vec::new()
            };
            let pointers = delta_fields(self.dialect, &value);
            if self.finalized && !pointers.is_empty() {
                return Err(StreamError("delta after stream completion"));
            }
            let mut clean_deltas = Vec::new();
            for (key, pointer) in pointers {
                let text = value
                    .pointer(&pointer)
                    .and_then(serde_json::Value::as_str)
                    .ok_or(StreamError("invalid delta"))?;
                let state = self.deltas.entry(key).or_insert_with(|| DeltaState {
                    text: Vec::new(),
                    last_event: 0,
                    pointer: String::new(),
                });
                // No matching influences framing or release. Accumulate the whole
                // logical output and redact it once at completion/EOF.
                state.text.extend_from_slice(text.as_bytes());
                self.held_text_bytes += text.len();
                if self.held_text_bytes > HELD_TEXT_LIMIT {
                    return Err(StreamError("held streaming text exceeds limit"));
                }
                state.last_event = self.events.len();
                state.pointer = pointer.clone();
                *value
                    .pointer_mut(&pointer)
                    .ok_or(StreamError("missing delta"))? =
                    serde_json::Value::String(String::new());
                clean_deltas.push((pointer, String::new()));
            }
            // Scrub metadata, non-delta strings and keys using the same decoded
            // JSON policy, then restore the already scrubbed incremental values.
            let clean = scrub_json(&serde_json::to_vec(&value)?, &self.secrets)?;
            value = serde_json::from_slice(&clean)?;
            for (pointer, clean) in clean_deltas {
                *value
                    .pointer_mut(&pointer)
                    .ok_or(StreamError("redaction changed delta path"))? =
                    serde_json::Value::String(clean);
            }
            self.held_event_bytes += fields.len() + clean.len();
            if self.held_event_bytes > HELD_EVENT_LIMIT {
                return Err(StreamError("held streaming events exceed limit"));
            }
            self.events.push(SseEvent {
                fields,
                value: Some(value),
            });
            if self.deltas.is_empty() {
                output.extend(self.flush_events()?);
            }
            return Ok(output);
        }
        fields.push(b'\n');
        if !self.events.is_empty() {
            // Redaction can lengthen a record; charge what is actually held.
            self.held_event_bytes += fields.len();
            if self.held_event_bytes > HELD_EVENT_LIMIT {
                return Err(StreamError("held streaming events exceed limit"));
            }
            self.events.push(SseEvent {
                fields,
                value: None,
            });
            return Ok(Vec::new());
        }
        Ok(fields)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    fn make_filter(dialect: StreamDialect, sse: bool, secrets: Vec<Vec<u8>>) -> ResponseFilter {
        let secrets: Vec<_> = secrets
            .iter()
            .map(|secret| std::str::from_utf8(secret).unwrap())
            .collect();
        ResponseFilter::new(dialect, sse, &secrets).unwrap()
    }
    fn chat_records(sse: bool, secrets: Vec<Vec<u8>>) -> ResponseFilter {
        make_filter(StreamDialect::OpenaiChatCompletions, sse, secrets)
    }
    fn response_records(sse: bool, secrets: Vec<Vec<u8>>) -> ResponseFilter {
        make_filter(StreamDialect::OpenaiResponses, sse, secrets)
    }
    #[test]
    fn r3_stream_frames_and_release_boundaries_do_not_disclose_credential_prefixes() {
        let upstream = [
            b"data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"o\"}}]}\n\n".as_slice(),
            b": heartbeat\n\n".as_slice(),
            b"data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"!\"}}]}\n\n".as_slice(),
            b"data: [DONE]\n\n".as_slice(),
        ];
        let observe = |secret: &[u8]| {
            let mut records = chat_records(true, vec![secret.to_vec()]);
            upstream
                .iter()
                .map(|event| records.feed(event, false).expect("event"))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            observe(b"old-access"),
            observe(b"new-access"),
            "credential prefix changed frames or release boundaries"
        );
    }

    #[test]
    fn r2_credentials_split_across_semantic_deltas_are_redacted() {
        for responses in [false, true] {
            let secret = "old-access";
            for split in 1..secret.len() {
                let mut records = make_filter(
                    if responses {
                        StreamDialect::OpenaiResponses
                    } else {
                        StreamDialect::OpenaiChatCompletions
                    },
                    true,
                    vec![secret.as_bytes().to_vec()],
                );
                let mut clean = Vec::new();
                for delta in [&secret[..split], &secret[split..]] {
                    let value = if responses {
                        serde_json::json!({"type":"response.output_text.delta", "item_id":"message-1", "output_index":0, "content_index":0, "delta":delta})
                    } else {
                        serde_json::json!({"choices":[{"index":0,"delta":{"content":delta}}]})
                    };
                    let event = format!("data: {value}\n\n");
                    for byte in event.bytes() {
                        clean.extend(records.feed(&[byte], false).expect("delta"));
                    }
                }
                clean.extend(records.feed(b"data: [DONE]\n\n", false).expect("done"));
                clean.extend(records.feed(&[], true).expect("EOF"));
                let text = String::from_utf8(clean).expect("UTF8");
                let mut joined = String::new();
                for line in text
                    .lines()
                    .filter_map(|l| l.strip_prefix("data: "))
                    .filter(|l| *l != "[DONE]")
                {
                    let value: serde_json::Value = serde_json::from_str(line).expect("JSON");
                    joined.push_str(if responses {
                        value["delta"].as_str().expect("delta")
                    } else {
                        value["choices"][0]["delta"]["content"]
                            .as_str()
                            .expect("content")
                    });
                }
                assert_eq!(
                    joined, "[redacted]",
                    "credential escaped at split {split}: {joined}"
                );
            }
        }
    }

    fn sse_values(output: &[u8]) -> Vec<serde_json::Value> {
        std::str::from_utf8(output)
            .expect("UTF8")
            .lines()
            .filter_map(|line| line.strip_prefix("data: "))
            .filter(|line| *line != "[DONE]")
            .map(|line| serde_json::from_str(line).expect("JSON"))
            .collect()
    }
    fn feed_value(records: &mut ResponseFilter, value: serde_json::Value) -> Vec<u8> {
        records
            .feed(format!("data: {value}\n\n").as_bytes(), false)
            .expect("event")
    }

    #[test]
    fn streaming_delta_state_is_independent_and_flushes_prefix_before_terminal() {
        let mut records = chat_records(true, vec![b"old-access".to_vec()]);
        assert!(
            feed_value(
                &mut records,
                serde_json::json!({"choices":[{"index":0,"delta":{"content":"old-"}}]})
            )
            .is_empty()
        );
        assert!(
            feed_value(
                &mut records,
                serde_json::json!({"choices":[{"index":1,"delta":{"content":"access"}}]})
            )
            .is_empty()
        );
        let clean = records.feed(b"data: [DONE]\n\n", false).expect("finish");
        let values = sse_values(&clean);
        assert_eq!(values[0]["choices"][0]["delta"]["content"], "old-");
        assert_eq!(values[1]["choices"][0]["delta"]["content"], "access");
        assert!(clean.ends_with(b"data: [DONE]\n\n"));
        assert!(records.feed(&[], true).expect("EOF").is_empty());
    }

    #[test]
    fn streaming_prefix_survives_heartbeats_and_hold_limit_fails_visibly() {
        let mut records = chat_records(true, vec![b"old-access".to_vec()]);
        let event = |delta| serde_json::json!({"choices":[{"index":0,"delta":{"content":delta}}]});
        assert!(feed_value(&mut records, event("old-")).is_empty());
        assert!(
            records
                .feed(b": heartbeat\n\n", false)
                .expect("heartbeat")
                .is_empty()
        );
        assert!(feed_value(&mut records, event("access")).is_empty());
        let clean = records.feed(&[], true).expect("EOF");
        let joined: String = sse_values(&clean)
            .iter()
            .map(|v| {
                v["choices"][0]["delta"]["content"]
                    .as_str()
                    .expect("content")
            })
            .collect();
        assert_eq!(joined, "[redacted]");
        assert!(
            std::str::from_utf8(&clean)
                .expect("UTF8")
                .contains(": heartbeat\n\n")
        );
        // The documented envelope ceiling is 64 MiB; a held delta envelope is
        // far below 4 KiB. Heartbeats up to 64 MiB - 4 KiB are held, and 8 KiB
        // more is refused.
        const MIB: usize = 1024 * 1024;
        let mut records = chat_records(true, vec![b"old-access".to_vec()]);
        assert!(feed_value(&mut records, event("ordinary")).is_empty());
        let heartbeat = |bytes: usize| format!(": {}\n\n", "x".repeat(bytes - 4));
        for _ in 0..63 {
            records
                .feed(heartbeat(MIB).as_bytes(), false)
                .expect("held below the envelope ceiling");
        }
        records
            .feed(heartbeat(MIB - 4096).as_bytes(), false)
            .expect("held up to 4 KiB below the envelope ceiling");
        let error = records
            .feed(heartbeat(8192).as_bytes(), false)
            .expect_err("held envelopes past 64 MiB")
            .to_string();
        assert!(
            error.contains("held streaming events exceed limit"),
            "{error}"
        );
    }

    #[test]
    fn redacted_heartbeats_are_charged_their_held_size() {
        // A one-byte credential redacts to ten bytes, so a 4 MiB heartbeat
        // repeating it is held as about 40 MiB; the second one crosses 64 MiB.
        let mut records = response_records(true, vec![b"q".to_vec()]);
        assert!(feed_value(&mut records, report_delta(0, "abc")).is_empty());
        let heartbeat = format!(": {}\n\n", "q".repeat(4 * 1024 * 1024 - 8));
        records
            .feed(heartbeat.as_bytes(), false)
            .expect("first redacted heartbeat fits");
        let error = records
            .feed(heartbeat.as_bytes(), false)
            .expect_err("redacted heartbeats past 64 MiB")
            .to_string();
        assert!(
            error.contains("held streaming events exceed limit"),
            "{error}"
        );
    }

    // A long final report streams as many small deltas; their envelopes far
    // outweigh the text, which is what the held bound must not confuse.
    fn report_delta(sequence: usize, text: &str) -> serde_json::Value {
        serde_json::json!({
            "type": "response.output_text.delta",
            "item_id": "msg_0123456789abcdef0123456789abcdef",
            "output_index": 1,
            "content_index": 0,
            "delta": text,
            "sequence_number": sequence,
            "obfuscation": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        })
    }

    #[test]
    fn long_report_in_small_deltas_streams_completely_and_redacts() {
        let mut records = response_records(true, vec![b"old-access".to_vec()]);
        let mut clean = Vec::new();
        let mut envelope = 0;
        let mut expected = String::new();
        // 120 000 deltas of four characters: 480 000 characters of report,
        // about 24 MiB of SSE records. One delta carries the held credential.
        for sequence in 0..120_000 {
            let text = if sequence == 60_000 {
                "old-access"
            } else {
                "abcd"
            };
            expected.push_str(if sequence == 60_000 {
                "[redacted]"
            } else {
                "abcd"
            });
            let value = report_delta(sequence, text);
            envelope += format!("data: {value}\n\n").len();
            clean.extend(feed_value(&mut records, value));
        }
        assert!(envelope > 4 * RECORD_LIMIT, "{envelope}");
        assert!(clean.is_empty(), "deltas stay held until completion");
        clean.extend(feed_value(
            &mut records,
            serde_json::json!({"type": "response.completed", "response": {"id": "resp_1"}}),
        ));
        clean.extend(records.feed(&[], true).expect("EOF"));
        let values = sse_values(&clean);
        let joined: String = values
            .iter()
            .filter_map(|value| value["delta"].as_str())
            .collect();
        assert_eq!(joined.len(), expected.len());
        assert_eq!(joined, expected);
        assert_eq!(values.last().expect("events")["type"], "response.completed");
    }

    #[test]
    fn held_streaming_text_is_bounded_by_its_own_limit() {
        // The documented text ceiling is 4 MiB: exactly 4 MiB is held, one
        // byte more is refused.
        let mut records = response_records(true, Vec::new());
        let chunk = "x".repeat(64 * 1024);
        for sequence in 0..64 {
            records
                .feed(
                    format!("data: {}\n\n", report_delta(sequence, &chunk)).as_bytes(),
                    false,
                )
                .expect("held up to the text ceiling");
        }
        let error = records
            .feed(
                format!("data: {}\n\n", report_delta(64, "x")).as_bytes(),
                false,
            )
            .expect_err("held text past 4 MiB")
            .to_string();
        assert!(
            error.contains("held streaming text exceeds limit"),
            "{error}"
        );
    }

    #[test]
    fn streaming_tool_arguments_names_and_ids_redact_across_deltas_at_eof() {
        let mut records = chat_records(true, vec![b"old-access".to_vec()]);
        let mut clean = Vec::new();
        for delta in ["old-", "access"] {
            clean.extend(feed_value(&mut records, serde_json::json!({"choices":[{"index":0,"delta":{"tool_calls":[{"index":2,"id":delta,"function":{"name":delta,"arguments":delta}}]}}]})));
        }
        clean.extend(records.feed(&[], true).expect("EOF"));
        let values = sse_values(&clean);
        for pointer in [
            "/choices/0/delta/tool_calls/0/id",
            "/choices/0/delta/tool_calls/0/function/name",
            "/choices/0/delta/tool_calls/0/function/arguments",
        ] {
            let joined: String = values
                .iter()
                .map(|value| {
                    value
                        .pointer(pointer)
                        .and_then(serde_json::Value::as_str)
                        .expect("delta")
                })
                .collect();
            assert_eq!(joined, "[redacted]");
        }
    }

    #[test]
    fn response_completion_and_eof_flush_once_with_credential_independent_buffering() {
        for terminal in [false, true] {
            let mut records = response_records(true, vec![b"old-access".to_vec()]);
            let event = |delta| serde_json::json!({"type":"response.output_text.delta","item_id":"a","output_index":0,"content_index":0,"delta":delta});
            assert!(feed_value(&mut records, event("ordinary")).is_empty());
            assert!(feed_value(&mut records, event("old-")).is_empty());
            let clean = if terminal {
                feed_value(
                    &mut records,
                    serde_json::json!({"type":"response.completed","response":{"status":"completed"}}),
                )
            } else {
                records.feed(&[], true).expect("EOF")
            };
            let values = sse_values(&clean);
            assert_eq!(values[0]["delta"], "");
            assert_eq!(values[1]["delta"], "ordinaryold-");
            if terminal {
                assert_eq!(values[2]["type"], "response.completed");
            }
            assert!(records.feed(&[], true).expect("EOF twice").is_empty());
        }
    }

    #[test]
    fn json_sse_records_redact_across_every_read_boundary_and_flush_complete_events() {
        let event = b"event: error\r\ndata: {\"text\":\"\\u006fld-access\"}\r\n\r\n";
        for split in 0..=event.len() {
            let mut records = chat_records(true, vec![b"old-access".to_vec()]);
            let mut clean = records.feed(&event[..split], false).expect("first chunk");
            clean.extend(records.feed(&event[split..], false).expect("second chunk"));
            assert_eq!(clean, b"event: error\ndata: {\"text\":\"[redacted]\"}\n\n");
            assert!(records.feed(&[], true).expect("EOF").is_empty());
        }
        let mut records = chat_records(true, vec![b"old-access".to_vec()]);
        let event = b"data: {\ndata: \"text\": \"old-access\"}\n\n";
        let mut clean = Vec::new();
        for byte in event {
            clean.extend(records.feed(&[*byte], false).expect("one byte"));
        }
        assert_eq!(clean, b"data: {\"text\":\"[redacted]\"}\n\n");
        assert_eq!(
            records.feed(b"data: [DONE]\n\n", false).expect("done"),
            b"data: [DONE]\n\n"
        );
    }

    #[test]
    fn malformed_incomplete_and_oversized_records_fail_without_forwarding_payloads() {
        for sse in [false, true] {
            let mut records = chat_records(sse, vec![b"old-access".to_vec()]);
            let malformed = if sse {
                b"data: {\"token\":\"\\u006fld-access\"\n\n".as_slice()
            } else {
                b"{\"token\":\"\\u006fld-access\"".as_slice()
            };
            assert!(records.feed(malformed, true).is_err());
            let mut records = chat_records(sse, Vec::new());
            assert!(records.feed(&vec![b'x'; RECORD_LIMIT + 1], false).is_err());
        }
        let mut records = chat_records(true, Vec::new());
        assert!(records.feed(b"data: {}\n", true).is_err());
        let mut records = chat_records(false, Vec::new());
        assert!(records.feed(&[], true).is_err());
    }

    #[test]
    fn decoded_redaction_preserves_json_syntax_for_credentials_containing_quotes() {
        let mut records = chat_records(true, vec![b"\"token\"".to_vec()]);
        let result = records
            .feed(b"data: {\"token\":\"\\\"token\\\"\"}\n\n", false)
            .expect("redact");
        assert_eq!(result, b"data: {\"token\":\"[redacted]\"}\n\n");
    }
    #[test]
    fn scrubber_delivers_ordinary_sse_events_without_waiting_for_eof() {
        let mut scrub = Scrubber::new(vec![b"very-long-subscription-credential".to_vec()]);
        let event = b"data: {\"text\":\"OK\"}\n\n";
        assert_eq!(scrub.feed(event, false), event);
        assert!(scrub.feed(b"very-long-", false).is_empty());
        assert_eq!(
            scrub.feed(b"subscription-credential\n", false),
            b"[redacted]\n"
        );
    }
    #[test]
    fn scrubber_redacts_credentials_split_at_every_boundary() {
        let secret = b"real-credential-secret";
        for split in 0..=secret.len() {
            let mut scrub = Scrubber::new(vec![secret.to_vec()]);
            let mut result = scrub.feed(b"prefix:", false);
            result.extend(scrub.feed(&secret[..split], false));
            result.extend(scrub.feed(&secret[split..], false));
            result.extend(scrub.feed(b":suffix", true));
            assert_eq!(result, b"prefix:[redacted]:suffix");
        }
    }
    #[test]
    fn scrubber_handles_one_byte_stream_and_adjacent_credentials() {
        let mut scrub = Scrubber::new(vec![b"secret".to_vec(), b"token".to_vec()]);
        let mut result = Vec::new();
        for b in b"secrettokensecret" {
            result.extend(scrub.feed(&[*b], false));
        }
        result.extend(scrub.feed(&[], true));
        assert_eq!(result, b"[redacted][redacted][redacted]");
    }

    const ENCODED_SECRET: &str = "syn\"/+\n?=é🦄\u{10ffff}";
    const ENCODINGS: &[&str] = &[
        "syn\"/+\n?=é🦄\u{10ffff}",
        "syn\\\"/+\\n?=é🦄\u{10ffff}",
        "syn%22%2f%2b%0a%3f%3d%c3%a9%f0%9f%a6%84%f4%8f%bf%bf",
        "syn%22%2F%2B%0A%3F%3D%C3%A9%F0%9F%A6%84%F4%8F%BF%BF",
        "%73%79%6e%22%2f%2b%0a%3f%3d%c3%a9%f0%9f%a6%84%f4%8f%bf%bf",
        "%73%79%6E%22%2F%2B%0A%3F%3D%C3%A9%F0%9F%A6%84%F4%8F%BF%BF",
        "c3luIi8rCj89w6nwn6aE9I+/vw==",
        "c3luIi8rCj89w6nwn6aE9I+/vw",
        "c3luIi8rCj89w6nwn6aE9I-_vw==",
        "c3luIi8rCj89w6nwn6aE9I-_vw",
    ];

    #[test]
    fn every_credential_encoding_is_redacted_at_every_network_boundary() {
        for encoded in ENCODINGS {
            for sse in [false, true] {
                let value = json!({"text": format!("before:{encoded}:after")});
                let body = if sse {
                    format!("data: {value}\n\n")
                } else {
                    value.to_string()
                };
                for split in 0..=body.len() {
                    let mut filter =
                        ResponseFilter::new(StreamDialect::OpenaiResponses, sse, &[ENCODED_SECRET])
                            .unwrap();
                    let mut clean = filter.feed(&body.as_bytes()[..split], false).unwrap();
                    clean.extend(filter.feed(&body.as_bytes()[split..], true).unwrap());
                    let values = if sse {
                        sse_values(&clean)
                    } else {
                        vec![serde_json::from_slice(&clean).unwrap()]
                    };
                    assert_eq!(
                        values,
                        vec![json!({"text":"before:[redacted]:after"})],
                        "{encoded}, split {split}, SSE={sse}"
                    );
                }
            }
            let event = format!(": {encoded}\n\n");
            for split in 0..=event.len() {
                let mut filter =
                    ResponseFilter::new(StreamDialect::OpenaiResponses, true, &[ENCODED_SECRET])
                        .unwrap();
                let mut clean = filter.feed(&event.as_bytes()[..split], false).unwrap();
                clean.extend(filter.feed(&event.as_bytes()[split..], true).unwrap());
                assert_eq!(
                    clean, b": [redacted]\n\n",
                    "metadata {encoded}, split {split}"
                );
            }
        }
    }

    fn delta(dialect: StreamDialect, field: &str, text: &str) -> (Value, &'static str) {
        if dialect == StreamDialect::OpenaiResponses {
            (
                json!({"type": field, "item_id":"item", "output_index":0, "content_index":0, "delta":text}),
                "/delta",
            )
        } else {
            let value = match field {
                "arguments" | "name" => json!({"tool_calls":[{"index":0,"function":{field:text}}]}),
                "id" => json!({"tool_calls":[{"index":0,"id":text}]}),
                _ => json!({field:text}),
            };
            (
                json!({"choices":[{"index":0,"delta":value}]}),
                match field {
                    "arguments" => "/choices/0/delta/tool_calls/0/function/arguments",
                    "name" => "/choices/0/delta/tool_calls/0/function/name",
                    "id" => "/choices/0/delta/tool_calls/0/id",
                    "reasoning" => "/choices/0/delta/reasoning",
                    "reasoning_content" => "/choices/0/delta/reasoning_content",
                    _ => "/choices/0/delta/content",
                },
            )
        }
    }

    #[test]
    fn encoded_secrets_across_both_dialects_delta_fields_are_redacted() {
        for (dialect, fields) in [
            (
                StreamDialect::OpenaiResponses,
                &[
                    "response.output_text.delta",
                    "response.reasoning_text.delta",
                    "response.reasoning_summary_text.delta",
                    "response.function_call_arguments.delta",
                ][..],
            ),
            (
                StreamDialect::OpenaiChatCompletions,
                &[
                    "content",
                    "reasoning",
                    "reasoning_content",
                    "arguments",
                    "name",
                    "id",
                ][..],
            ),
        ] {
            for field in fields {
                for encoded in ENCODINGS {
                    for split in
                        (0..=encoded.len()).filter(|split| encoded.is_char_boundary(*split))
                    {
                        let mut filter =
                            ResponseFilter::new(dialect, true, &[ENCODED_SECRET]).unwrap();
                        let mut pointer = "";
                        for part in [&encoded[..split], &encoded[split..]] {
                            let (value, path) = delta(dialect, field, part);
                            pointer = path;
                            let event = format!("data: {value}\n\n");
                            for byte in event.bytes() {
                                assert_eq!(filter.feed(&[byte], false).unwrap(), Vec::<u8>::new());
                            }
                        }
                        let clean = filter.feed(&[], true).unwrap();
                        let joined: String = sse_values(&clean)
                            .iter()
                            .map(|value| value.pointer(pointer).unwrap().as_str().unwrap())
                            .collect();
                        assert_eq!(
                            joined, "[redacted]",
                            "{dialect:?} {field} {encoded} split {split}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn overlapping_credentials_redact_the_longest_union_in_any_order() {
        for (secrets, input, expected) in [
            (
                vec!["acct", "acct-token-value"],
                "acct-token-valueacctacct-token-value",
                "[redacted][redacted][redacted]",
            ),
            (
                vec!["abcd", "cdef", "fghi"],
                "before:abcdefghicdef:after",
                "before:[redacted][redacted]:after",
            ),
            (vec!["aba"], "abababa", "[redacted]"),
        ] {
            for secrets in [secrets.clone(), secrets.into_iter().rev().collect()] {
                // The public filter covers JSON, SSE metadata and semantic deltas.
                for sse in [false, true] {
                    let body = json!({input:input}).to_string();
                    let body = if sse {
                        format!("id: {input}\ndata: {body}\n\n")
                    } else {
                        body
                    };
                    for split in 0..=body.len() {
                        let mut filter =
                            ResponseFilter::new(StreamDialect::OpenaiResponses, sse, &secrets)
                                .unwrap();
                        let mut clean = filter.feed(&body.as_bytes()[..split], false).unwrap();
                        clean.extend(filter.feed(&body.as_bytes()[split..], true).unwrap());
                        let values = if sse {
                            sse_values(&clean)
                        } else {
                            vec![serde_json::from_slice(&clean).unwrap()]
                        };
                        assert_eq!(values, vec![json!({expected:expected})]);
                        if sse {
                            assert!(clean.starts_with(format!("id: {expected}\n").as_bytes()));
                        }
                    }
                }
                for dialect in [
                    StreamDialect::OpenaiResponses,
                    StreamDialect::OpenaiChatCompletions,
                ] {
                    let mut filter = ResponseFilter::new(dialect, true, &secrets).unwrap();
                    let mut pointer = "";
                    for byte in input.bytes() {
                        let text = char::from(byte).to_string();
                        let (value, path) = delta(
                            dialect,
                            if dialect == StreamDialect::OpenaiResponses {
                                "response.output_text.delta"
                            } else {
                                "content"
                            },
                            &text,
                        );
                        pointer = path;
                        assert_eq!(feed_value(&mut filter, value), Vec::<u8>::new());
                    }
                    let clean = filter.feed(&[], true).unwrap();
                    let joined: String = sse_values(&clean)
                        .iter()
                        .map(|value| value.pointer(pointer).unwrap().as_str().unwrap())
                        .collect();
                    assert_eq!(joined, expected);
                }
            }
        }
    }

    #[test]
    fn escaped_json_keys_and_values_are_decoded_before_redaction() {
        for sse in [false, true] {
            for encoded in [
                r"\u006fld-access",
                r"old-\u0061ccess",
                r"\u006f\u006c\u0064-access",
            ] {
                let body = format!(r#"{{"{encoded}":"prefix {encoded} suffix"}}"#);
                let body = if sse {
                    format!("data: {body}\n\n")
                } else {
                    body
                };
                let mut filter = chat_records(sse, vec![b"old-access".to_vec()]);
                let clean = filter.feed(body.as_bytes(), true).unwrap();
                let values = if sse {
                    sse_values(&clean)
                } else {
                    vec![serde_json::from_slice(&clean).unwrap()]
                };
                assert_eq!(
                    values,
                    vec![json!({"[redacted]":"prefix [redacted] suffix"})]
                );
            }
        }
    }

    #[test]
    fn redaction_colliding_json_keys_fail_and_poison_the_filter() {
        let mut filter = chat_records(false, vec![b"synthetic-key".to_vec()]);
        assert_eq!(
            filter.feed(br#"{"synthetic-key":1,"[redacted]":2}"#, true),
            Err(StreamError("redaction produced duplicate JSON keys"))
        );
        assert_eq!(
            filter.feed(b"{}", true),
            Err(StreamError("stream already failed"))
        );
        assert!(filter.usage().is_none());
    }

    fn usage_event(dialect: StreamDialect) -> Value {
        match dialect {
            StreamDialect::OpenaiResponses => json!({"type":"response.completed","response":{
                "id":"resp-1", "model":"model-1", "usage":{"input_tokens":1200,"input_tokens_details":{"cached_tokens":800},"output_tokens":310,"output_tokens_details":{"reasoning_tokens":96}}
            }}),
            StreamDialect::OpenaiChatCompletions => {
                json!({"id":"resp-1","model":"model-1","choices":[],
                    "usage":{"prompt_tokens":1200,"prompt_tokens_details":{"cached_tokens":800},"completion_tokens":310,"completion_tokens_details":{"reasoning_tokens":96}}
                })
            }
        }
    }
    fn finish_usage(filter: &mut ResponseFilter, value: Value) {
        feed_value(filter, value);
        if filter.dialect == StreamDialect::OpenaiChatCompletions {
            filter.feed(b"data: [DONE]\n\n", false).unwrap();
        }
        assert!(filter.usage().is_none(), "EOF still required");
        filter.feed(&[], true).unwrap();
    }

    #[test]
    fn final_usage_in_both_dialects_maps_to_the_existing_usage_trace() {
        for dialect in [
            StreamDialect::OpenaiResponses,
            StreamDialect::OpenaiChatCompletions,
        ] {
            let mut filter = ResponseFilter::new(dialect, true, &[]).unwrap();
            finish_usage(&mut filter, usage_event(dialect));
            let expected = Usage {
                input_tokens: Some(1200),
                cache_hit_tokens: Some(800),
                output_tokens: Some(310),
                reasoning_tokens: Some(96),
                response_id: Some("resp-1".into()),
                served_model: Some("model-1".into()),
                ..Usage::default()
            };
            assert_eq!(
                serde_json::to_value(filter.usage()).unwrap(),
                serde_json::to_value(expected).unwrap()
            );
        }
    }

    #[test]
    fn missing_usage_and_cut_or_failed_streams_keep_usage_unknown() {
        for dialect in [
            StreamDialect::OpenaiResponses,
            StreamDialect::OpenaiChatCompletions,
        ] {
            for suffix in [b"".as_slice(), b"data: [DONE]\n\n"] {
                let mut filter = ResponseFilter::new(dialect, true, &[]).unwrap();
                feed_value(&mut filter, json!({"text":"ordinary response"}));
                filter.feed(suffix, true).unwrap();
                assert!(filter.usage().is_none());
            }
            let mut filter = ResponseFilter::new(dialect, true, &[]).unwrap();
            feed_value(&mut filter, usage_event(dialect));
            assert!(filter.usage().is_none());
            let error = filter.feed(b"data: broken\n\n", true);
            assert_eq!(error, Err(StreamError("malformed upstream JSON record")));
            assert!(filter.usage().is_none());
            for failed in [
                json!({"type":"response.failed"}),
                json!({"type":"response.incomplete"}),
                json!({"error":{"code":"failure"}}),
                json!({"type":"response.completed","response":{"status":"failed"}}),
                json!({"type":"response.completed","response":{"status":"incomplete"}}),
            ] {
                let mut filter = ResponseFilter::new(dialect, true, &[]).unwrap();
                feed_value(&mut filter, usage_event(dialect));
                feed_value(&mut filter, failed);
                filter.feed(b"data: [DONE]\n\n", true).unwrap();
                assert!(filter.usage().is_none());
            }
        }
        let mut filter = chat_records(true, vec![]);
        feed_value(
            &mut filter,
            usage_event(StreamDialect::OpenaiChatCompletions),
        );
        filter.feed(&[], true).unwrap();
        assert!(
            filter.usage().is_none(),
            "chat EOF before DONE is a cut stream"
        );
    }

    #[test]
    fn absent_usage_fields_stay_absent_and_invalid_counts_fail_visibly() {
        for dialect in [
            StreamDialect::OpenaiResponses,
            StreamDialect::OpenaiChatCompletions,
        ] {
            let mut event = usage_event(dialect);
            let raw = if dialect == StreamDialect::OpenaiResponses {
                &mut event["response"]
            } else {
                &mut event
            };
            *raw = json!({"usage":{}});
            let mut filter = ResponseFilter::new(dialect, true, &[]).unwrap();
            finish_usage(&mut filter, event);
            assert_eq!(
                serde_json::to_value(filter.usage()).unwrap(),
                serde_json::to_value(Usage::default()).unwrap()
            );
            for invalid in [json!(-1), json!(1.5), json!("1200"), json!({})] {
                let mut event = usage_event(dialect);
                let raw = if dialect == StreamDialect::OpenaiResponses {
                    &mut event["response"]
                } else {
                    &mut event
                };
                let field = if dialect == StreamDialect::OpenaiResponses {
                    "input_tokens"
                } else {
                    "prompt_tokens"
                };
                raw["usage"][field] = invalid;
                let mut filter = ResponseFilter::new(dialect, true, &[]).unwrap();
                assert_eq!(
                    filter.feed(format!("data: {event}\n\n").as_bytes(), true),
                    Err(StreamError("invalid upstream token count"))
                );
            }
        }
    }

    #[test]
    fn usage_identities_follow_existing_screening_and_cannot_echo_secrets_or_deltas() {
        for dialect in [
            StreamDialect::OpenaiResponses,
            StreamDialect::OpenaiChatCompletions,
        ] {
            for identity in [
                "resp-1",
                "bad identity",
                "",
                "synthetic-key",
                "payload",
                "reasoned",
            ] {
                let mut event = usage_event(dialect);
                let raw = if dialect == StreamDialect::OpenaiResponses {
                    &mut event["response"]
                } else {
                    &mut event
                };
                raw["id"] = json!(identity);
                raw["model"] = json!(identity);
                raw["content"] = json!("the payload");
                let mut filter = ResponseFilter::new(dialect, true, &["synthetic-key"]).unwrap();
                let (text, _) = delta(
                    dialect,
                    if dialect == StreamDialect::OpenaiResponses {
                        "response.reasoning_text.delta"
                    } else {
                        "reasoning_content"
                    },
                    "reasoned",
                );
                feed_value(&mut filter, text);
                finish_usage(&mut filter, event);
                let usage = filter.usage().unwrap();
                let expected = (identity == "resp-1").then_some(identity);
                assert_eq!(usage.response_id.as_deref(), expected, "{identity}");
                assert_eq!(usage.served_model.as_deref(), expected, "{identity}");
                assert_eq!(usage.input_tokens, Some(1200));
            }
        }
    }

    #[test]
    fn only_terminal_responses_and_the_last_chat_chunk_supply_usage() {
        let mut filter = response_records(true, vec![]);
        let mut created = usage_event(StreamDialect::OpenaiResponses);
        created["type"] = json!("response.created");
        feed_value(&mut filter, created);
        filter.feed(b"data: [DONE]\n\n", true).unwrap();
        assert!(filter.usage().is_none());
        let mut filter = response_records(true, vec![]);
        feed_value(&mut filter, usage_event(StreamDialect::OpenaiResponses));
        feed_value(
            &mut filter,
            json!({"type":"response.completed","response":{}}),
        );
        filter.feed(&[], true).unwrap();
        assert!(filter.usage().is_none(), "the last completion has no usage");
        let mut filter = chat_records(true, vec![]);
        feed_value(
            &mut filter,
            usage_event(StreamDialect::OpenaiChatCompletions),
        );
        feed_value(&mut filter, json!({"choices":[]}));
        filter.feed(b"data: [DONE]\n\n", true).unwrap();
        assert!(filter.usage().is_none());
    }

    #[test]
    fn json_bodies_are_held_until_eof_and_report_usage() {
        for dialect in [
            StreamDialect::OpenaiResponses,
            StreamDialect::OpenaiChatCompletions,
        ] {
            let event = usage_event(dialect);
            let body = if dialect == StreamDialect::OpenaiResponses {
                &event["response"]
            } else {
                &event
            };
            let body = body.to_string();
            let mut filter = ResponseFilter::new(dialect, false, &[]).unwrap();
            assert_eq!(
                filter.feed(body.as_bytes(), false).unwrap(),
                Vec::<u8>::new()
            );
            assert!(filter.usage().is_none());
            let clean = filter.feed(&[], true).unwrap();
            assert_eq!(clean, body.as_bytes());
            assert_eq!(filter.usage().unwrap().input_tokens, Some(1200));
            assert_eq!(filter.feed(&[], true).unwrap(), Vec::<u8>::new());
        }
    }

    #[test]
    fn usage_trailer_formats_ordered_counts_and_omits_only_absent_fields() {
        for mask in 0..16 {
            let mut usage = Usage::default();
            let mut fields = Vec::new();
            for (bit, name, count, slot) in [
                (1, "input_tokens", 1200, &mut usage.input_tokens),
                (2, "cached_input_tokens", 800, &mut usage.cache_hit_tokens),
                (4, "output_tokens", 310, &mut usage.output_tokens),
                (8, "reasoning_tokens", 96, &mut usage.reasoning_tokens),
            ] {
                if mask & bit != 0 {
                    *slot = Some(count);
                    fields.push(format!("{name}={count}"));
                }
            }
            assert_eq!(usage_trailer(&usage), fields.join(", "));
        }
        let usage = Usage {
            input_tokens: Some(0),
            ..Usage::default()
        };
        assert_eq!(usage_trailer(&usage), "input_tokens=0");
    }

    #[test]
    fn dialect_configuration_uses_the_approved_snake_case_tags() {
        for (dialect, tag) in [
            (StreamDialect::OpenaiResponses, "openai_responses"),
            (
                StreamDialect::OpenaiChatCompletions,
                "openai_chat_completions",
            ),
        ] {
            assert_eq!(serde_json::to_value(dialect).unwrap(), json!(tag));
            assert_eq!(
                serde_json::from_value::<StreamDialect>(json!(tag)).unwrap(),
                dialect
            );
        }
    }
    #[test]
    fn deltas_after_completion_and_bytes_after_eof_fail_without_usage() {
        for dialect in [
            StreamDialect::OpenaiResponses,
            StreamDialect::OpenaiChatCompletions,
        ] {
            let mut filter = ResponseFilter::new(dialect, true, &[]).unwrap();
            feed_value(&mut filter, usage_event(dialect));
            filter.feed(b"data: [DONE]\n\n", false).unwrap();
            let (value, _) = delta(
                dialect,
                if dialect == StreamDialect::OpenaiResponses {
                    "response.output_text.delta"
                } else {
                    "content"
                },
                "late",
            );
            assert_eq!(
                filter.feed(format!("data: {value}\n\n").as_bytes(), true),
                Err(StreamError("delta after stream completion"))
            );
            assert!(filter.usage().is_none());
            let mut filter = ResponseFilter::new(dialect, true, &[]).unwrap();
            finish_usage(&mut filter, usage_event(dialect));
            assert_eq!(
                filter.feed(b"late", true),
                Err(StreamError("bytes after end of body"))
            );
            assert!(filter.usage().is_none());
        }
    }
}
