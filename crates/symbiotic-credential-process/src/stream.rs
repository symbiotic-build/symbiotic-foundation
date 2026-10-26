//! Complete-item holding and continuation joining.
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use symbiotic_ai_runtime::model::{
    self,
    item_stream::{ItemEvent, ResponsesAdapter},
};

/// Provider event dialect. Only Responses currently has an item-stream adapter.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamDialect {
    /// OpenAI Responses SSE and JSON.
    OpenaiResponses,
    /// Reserved configuration spelling; refused until its adapter is implemented.
    OpenaiChatCompletions,
}
/// Static diagnostic with no provider content.
#[derive(Debug)]
pub struct StreamError(pub(crate) &'static str);
impl std::fmt::Display for StreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for StreamError {}
impl From<model::item_stream::ItemStreamError> for StreamError {
    fn from(error: model::item_stream::ItemStreamError) -> Self {
        match error {
            model::item_stream::ItemStreamError::InvalidEvent => Self("invalid_provider_stream"),
            model::item_stream::ItemStreamError::HeldEventBytesExceeded => {
                Self("max_held_event_bytes")
            }
        }
    }
}
/// Provisional per-item event byte bound.
pub(crate) fn default_max_held_event_bytes() -> usize {
    67_108_864
}

struct Held {
    event: ItemEvent,
    item: Option<String>,
    ready: bool,
}
/// Shared item-stream consumer retaining complete opaque items for replay.
pub(crate) struct ItemJoin {
    held: VecDeque<Held>,
    sizes: HashMap<String, usize>,
    completed: HashMap<String, Option<u64>>,
    items: Vec<Value>,
    response_id: Option<String>,
    sequence: u64,
    joined: bool,
    limit: usize,
    usage: symbiotic_trace::UsageTrace,
}
impl ItemJoin {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            held: VecDeque::new(),
            sizes: HashMap::new(),
            completed: HashMap::new(),
            items: Vec::new(),
            response_id: None,
            sequence: 0,
            joined: false,
            limit,
            usage: Default::default(),
        }
    }
    pub(crate) fn next_attempt(&mut self) {
        self.held.clear();
        self.sizes.clear();
        self.completed.clear();
        self.joined = self.response_id.is_some() || self.has_completed_items();
    }
    pub(crate) fn has_completed_items(&self) -> bool {
        !self.items.is_empty()
    }
    pub(crate) fn continuation(&self, original: &[u8]) -> Result<Vec<u8>, StreamError> {
        ResponsesAdapter::continuation_request(original, &self.items).map_err(Into::into)
    }

    pub(crate) fn add_usage(&mut self, u: &symbiotic_trace::UsageTrace) -> Result<(), StreamError> {
        fn add(a: &mut Option<u64>, b: Option<u64>) -> Result<(), StreamError> {
            if let Some(b) = b {
                *a = Some(
                    a.unwrap_or(0)
                        .checked_add(b)
                        .ok_or(StreamError("usage_overflow"))?,
                );
            }
            Ok(())
        }
        add(&mut self.usage.input_tokens, u.input_tokens)?;
        add(&mut self.usage.output_tokens, u.output_tokens)?;
        add(&mut self.usage.cache_hit_tokens, u.cache_hit_tokens)?;
        add(&mut self.usage.reasoning_tokens, u.reasoning_tokens)
    }
    pub(crate) fn accept(&mut self, event: ItemEvent) -> Result<Vec<u8>, StreamError> {
        let record = event.record();
        if self.joined
            && matches!(
                event,
                ItemEvent::ResponseStarted(_) | ItemEvent::ResponseInProgress(_)
            )
        {
            return Ok(Vec::new());
        }
        if self.response_id.is_none() {
            self.response_id = record.response_id.clone();
        }
        let item = record
            .output_index
            .map(|i| format!("index:{i}"))
            .or_else(|| record.item_id.as_ref().map(|i| format!("id:{i}")));
        if matches!(event, ItemEvent::ItemDelta(_)) && item.is_none() {
            return Err(StreamError("missing_item_identity"));
        }
        let done = matches!(event, ItemEvent::ItemDone { .. });
        if matches!(
            event,
            ItemEvent::Usage {
                completed: true,
                ..
            }
        ) && !self.sizes.is_empty()
        {
            return Err(StreamError("incomplete_output_item"));
        }
        if let Some(key) = &item {
            if self.completed.contains_key(key) {
                return Err(StreamError("event_after_item_done"));
            }
            let size = self.sizes.entry(key.clone()).or_default();
            *size = size
                .checked_add(ResponsesAdapter::encode(&event)?.len())
                .filter(|n| *n <= self.limit)
                .ok_or(StreamError("max_held_event_bytes"))?;
        }
        self.held.push_back(Held {
            event,
            item: item.clone(),
            ready: item.is_none(),
        });
        if done {
            let key = item.ok_or(StreamError("missing_item_identity"))?;
            self.sizes.remove(&key);
            self.completed.insert(key.clone(), None);
            for h in &mut self.held {
                if h.item.as_ref() == Some(&key) {
                    h.ready = true;
                }
            }
        }
        self.release_ready()
    }
    /// Drop incomplete items, retaining complete items for both the caller and replay.
    pub(crate) fn discard_partial(&mut self) -> Result<Vec<u8>, StreamError> {
        self.held.retain(|h| h.ready);
        self.sizes.clear();
        self.release_ready()
    }
    fn release_ready(&mut self) -> Result<Vec<u8>, StreamError> {
        if !self.held.front().is_some_and(|h| h.ready) {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        while self.held.front().is_some_and(|h| h.ready) {
            let mut h = self
                .held
                .pop_front()
                .ok_or(StreamError("missing_held_item"))?;
            let position = if let Some(key) = &h.item {
                let position = match self.completed.get(key).copied().flatten() {
                    Some(position) => position,
                    None => {
                        let item = match &h.event {
                            ItemEvent::ItemDone { item, .. } => item.clone(),
                            _ => self
                                .held
                                .iter()
                                .find_map(|held| match &held.event {
                                    ItemEvent::ItemDone { item, .. }
                                        if held.item.as_ref() == Some(key) =>
                                    {
                                        Some(item.clone())
                                    }
                                    _ => None,
                                })
                                .ok_or(StreamError("missing_complete_item"))?,
                        };
                        let position = u64::try_from(self.items.len())
                            .map_err(|_| StreamError("output_index_overflow"))?;
                        self.items.push(item);
                        self.completed.insert(key.clone(), Some(position));
                        position
                    }
                };
                h.event.record().output_index.map(|_| position)
            } else {
                None
            };
            if let ItemEvent::Usage { usage, .. } = &h.event {
                self.add_usage(usage)?;
            }
            self.rewrite(&mut h.event, position)?;
            out.extend(ResponsesAdapter::encode(&h.event)?);
        }
        Ok(out)
    }
    fn rewrite(
        &mut self,
        event: &mut ItemEvent,
        output_index: Option<u64>,
    ) -> Result<(), StreamError> {
        let completed = self.joined
            && matches!(
                event,
                ItemEvent::Usage {
                    completed: true,
                    ..
                }
            );
        let sequence_number = event
            .record()
            .provider_sequence_number
            .map(|_| self.sequence);
        if sequence_number.is_some() {
            self.sequence = self
                .sequence
                .checked_add(1)
                .ok_or(StreamError("sequence_overflow"))?;
        }
        ResponsesAdapter::rewrite(
            event,
            model::item_stream::JoinFields {
                response_id: self.response_id.as_deref(),
                sequence_number,
                output_index,
                completed_items: completed.then_some(self.items.as_slice()),
                usage: completed.then_some(&self.usage),
            },
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn events(values: Vec<Value>) -> Vec<ItemEvent> {
        let wire: Vec<u8> = values
            .into_iter()
            .flat_map(|v| format!("data: {v}\n\n").into_bytes())
            .collect();
        ResponsesAdapter::default().feed(&wire, 100_000).unwrap()
    }
    fn decode(bytes: &[u8]) -> Vec<Value> {
        ResponsesAdapter::default()
            .feed(bytes, 100_000)
            .unwrap()
            .into_iter()
            .map(|e| e.record().value.clone())
            .collect()
    }
    fn feed(join: &mut ItemJoin, values: Vec<Value>) -> Vec<u8> {
        events(values)
            .into_iter()
            .flat_map(|e| join.accept(e).unwrap())
            .collect()
    }
    #[test]
    fn partial_item_is_never_released_and_is_discarded_on_retry() {
        let malformed = events(vec![
            json!({"type":"response.output_text.delta","delta":"unidentified"}),
        ])
        .pop()
        .unwrap();
        assert_eq!(
            ItemJoin::new(100_000)
                .accept(malformed)
                .unwrap_err()
                .to_string(),
            "missing_item_identity"
        );
        let mut join = ItemJoin::new(100_000);
        let held = feed(
            &mut join,
            vec![
                json!({"type":"response.output_item.added","output_index":0,"item":{"id":"half"}}),
                json!({"type":"response.output_text.delta","output_index":0,"delta":"partial"}),
            ],
        );
        assert_eq!(held, b"");
        join.next_attempt();
        assert_eq!(
            serde_json::from_slice::<Value>(&join.continuation(br#"{"input":[]}"#).unwrap())
                .unwrap(),
            json!({"input":[]})
        );
        let clean = feed(
            &mut join,
            vec![
                json!({"type":"response.output_item.done","output_index":0,"item":{"id":"whole","text":"answer"}}),
            ],
        );
        assert_eq!(
            decode(&clean),
            vec![
                json!({"type":"response.output_item.done","output_index":0,"item":{"id":"whole","text":"answer"}})
            ]
        );
    }
    #[test]
    fn continuation_replays_opaque_items_and_joins_ids_indexes_sequence_and_usage() {
        for disconnect in [false, true] {
            let mut join = ItemJoin::new(100_000);
            let reasoning = json!({"type":"reasoning","id":"reason","encrypted_content":"opaque-encrypted-content","summary":[],"unknown":{"number":1.25}});
            let mut out = feed(
                &mut join,
                vec![
                    json!({"type":"response.created","sequence_number":0,"response":{"id":"first"}}),
                    json!({"type":"response.output_item.done","sequence_number":1,"output_index":0,"item":reasoning}),
                    json!({"type":"response.output_item.added","sequence_number":2,"output_index":1,"item":{"id":"half"}}),
                    json!({"type":"response.output_text.delta","sequence_number":3,"output_index":1,"delta":"discarded"}),
                ],
            );
            if !disconnect {
                let failure=events(vec![json!({"type":"response.failed","response":{"usage":{"input_tokens":5,"output_tokens":2,"input_tokens_details":{"cached_tokens":1},"output_tokens_details":{"reasoning_tokens":1}},"error":{"code":"slow_down"}}})]).pop().unwrap();
                join.add_usage(&ResponsesAdapter::event_usage(&failure).unwrap())
                    .unwrap();
            }
            assert_eq!(
                serde_json::from_slice::<Value>(
                    &join
                        .continuation(
                            br#"{"input":[{"role":"user","content":"question"}],"store":false}"#
                        )
                        .unwrap()
                )
                .unwrap(),
                json!({"input":[{"role":"user","content":"question"},reasoning],"store":false})
            );
            join.next_attempt();
            let answer = json!({"id":"answer","type":"message","content":[{"type":"output_text","text":"joined"}]});
            out.extend(feed(&mut join,vec![
                json!({"type":"response.created","sequence_number":0,"response":{"id":"second"}}),
                json!({"type":"response.in_progress","sequence_number":1,"response":{"id":"second"}}),
                json!({"type":"response.output_item.done","sequence_number":2,"output_index":0,"response_id":"second","item":answer}),
                json!({"type":"response.completed","sequence_number":3,"response":{"id":"second","output":[answer],"usage":{"input_tokens":7,"output_tokens":3,"input_tokens_details":{"cached_tokens":2},"output_tokens_details":{"reasoning_tokens":1}}}})
            ]));
            let got = decode(&out);
            assert_eq!(
                got.iter()
                    .map(|v| v["sequence_number"].as_u64().unwrap())
                    .collect::<Vec<_>>(),
                vec![0, 1, 2, 3]
            );
            assert_eq!(got[0]["response"]["id"], "first");
            assert_eq!(got[2]["response_id"], "first");
            assert_eq!(got[2]["output_index"], 1);
            assert_eq!(got[3]["response"]["id"], "first");
            assert_eq!(got[3]["response"]["output"], json!([reasoning, answer]));
            assert_eq!(
                got[3]["response"]["usage"],
                if disconnect {
                    json!({"input_tokens":7,"output_tokens":3,"total_tokens":10,"input_tokens_details":{"cached_tokens":2},"output_tokens_details":{"reasoning_tokens":1}})
                } else {
                    json!({"input_tokens":12,"output_tokens":5,"total_tokens":17,"input_tokens_details":{"cached_tokens":3},"output_tokens_details":{"reasoning_tokens":2}})
                }
            );
            assert_eq!(
                got.iter()
                    .map(|v| v["type"].as_str().unwrap())
                    .collect::<Vec<_>>(),
                vec![
                    "response.created",
                    "response.output_item.done",
                    "response.output_item.done",
                    "response.completed"
                ]
            );
        }
    }
    #[test]
    fn interleaved_items_release_in_order_and_never_join_unrelated_fields() {
        let mut join = ItemJoin::new(100_000);
        let held = feed(
            &mut join,
            vec![
                json!({"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"pro"}),
                json!({"type":"response.output_text.delta","output_index":1,"content_index":0,"delta":"tected"}),
                json!({"type":"response.output_item.done","output_index":1,"item":{"id":"second","text":"tected"}}),
            ],
        );
        assert_eq!(held, b"");
        let released = feed(
            &mut join,
            vec![
                json!({"type":"response.output_item.done","output_index":0,"item":{"id":"first","text":"pro"}}),
            ],
        );
        let values = decode(&released);
        assert_eq!(
            values
                .iter()
                .map(|v| v["output_index"].as_u64().unwrap())
                .collect::<Vec<_>>(),
            [0, 1, 1, 0]
        );
        assert_eq!(values[0]["delta"], "pro");
        assert_eq!(values[1]["delta"], "tected");
    }
    #[test]
    fn retry_keeps_completed_items_behind_partial_items_and_compacts_output_positions() {
        let mut join = ItemJoin::new(100_000);
        let kept = json!({"id":"kept","type":"reasoning","encrypted_content":"opaque"});
        let mut out = feed(
            &mut join,
            vec![
                json!({"type":"response.created","sequence_number":0,"response":{"id":"first"}}),
                json!({"type":"response.output_text.delta","sequence_number":1,"output_index":0,"delta":"discarded"}),
                json!({"type":"response.output_item.done","sequence_number":2,"output_index":1,"item":kept}),
            ],
        );
        assert_eq!(
            decode(&out),
            vec![json!({"type":"response.created","sequence_number":0,"response":{"id":"first"}})]
        );
        out.extend(join.discard_partial().unwrap());
        assert_eq!(
            decode(&out),
            vec![
                json!({"type":"response.created","sequence_number":0,"response":{"id":"first"}}),
                json!({"type":"response.output_item.done","sequence_number":1,"output_index":0,"item":kept})
            ]
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&join.continuation(br#"{"input":[]}"#).unwrap())
                .unwrap(),
            json!({"input":[kept]})
        );
        join.next_attempt();
        out.extend(feed(&mut join, vec![
            json!({"type":"response.output_item.done","sequence_number":0,"output_index":0,"item":{"id":"next"}}),
            json!({"type":"response.completed","sequence_number":1,"response":{"id":"second","output":[{"id":"next"}]}}),
        ]));
        let values = decode(&out);
        assert_eq!(values[2]["output_index"], 1);
        assert_eq!(values[3]["response"]["output"], json!([kept,{"id":"next"}]));
        assert_eq!(
            values
                .iter()
                .map(|v| v["sequence_number"].as_u64().unwrap())
                .collect::<Vec<_>>(),
            [0, 1, 2, 3]
        );
    }
    #[test]
    fn held_item_over_limit_returns_visible_static_error() {
        let wire=b"data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"delta\":\"held\"}\n\n";
        let mut join = ItemJoin::new(wire.len());
        let first = ResponsesAdapter::default()
            .feed(wire, wire.len())
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(join.accept(first).unwrap(), b"");
        let second = ResponsesAdapter::default()
            .feed(wire, wire.len())
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(
            join.accept(second).unwrap_err().to_string(),
            "max_held_event_bytes"
        );
    }
}
