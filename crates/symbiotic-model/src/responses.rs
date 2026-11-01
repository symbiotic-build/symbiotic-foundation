//! OpenAI Responses requests with function tools, and the function calls in their answers.
//!
//! The request has the shape the Codex subscription backend accepts: encoding always
//! sets `store: false` and `stream: true`. The answer is a Responses event stream;
//! [`crate::item_stream::ResponsesAdapter`] yields its complete output items, and
//! [`FunctionCall::from_item`] reads the function calls among them.

use serde::Serialize;
use serde_json::Value;

/// One Responses request. [`ResponsesRequest::encode`] adds `store: false` and `stream: true`.
#[derive(Clone, Debug)]
pub struct ResponsesRequest {
    /// Provider model name.
    pub model: String,
    /// System instructions, sent as the top-level `instructions` field.
    pub instructions: String,
    /// Input messages, in order.
    pub input: Vec<UserMessage>,
    /// Function tools the model may call.
    pub tools: Vec<FunctionTool>,
    /// Whether the model must call a tool.
    pub tool_choice: ToolChoice,
    /// Whether one answer may hold several tool calls.
    pub parallel_tool_calls: bool,
    /// Reasoning effort, such as `low` or `high`; absent leaves the provider default.
    pub reasoning_effort: Option<String>,
    /// Upper bound on output tokens, reasoning included; absent leaves the provider default.
    pub max_output_tokens: Option<u32>,
}

/// One user message with a single text part.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UserMessage {
    /// Message text.
    pub text: String,
}

/// One function tool definition.
#[derive(Clone, Debug, PartialEq)]
pub struct FunctionTool {
    /// Name the model calls the tool by.
    pub name: String,
    /// What the tool does, for the model.
    pub description: String,
    /// JSON Schema of the arguments object.
    pub parameters: Value,
    /// Whether the provider enforces the schema exactly.
    pub strict: bool,
}

/// Tool use the request allows.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolChoice {
    /// The model decides whether to call tools.
    Auto,
    /// The model must call at least one tool.
    Required,
}

#[derive(Serialize)]
struct RequestBody<'a> {
    model: &'a str,
    instructions: &'a str,
    input: Vec<MessageBody<'a>>,
    tools: Vec<ToolBody<'a>>,
    tool_choice: ToolChoice,
    parallel_tool_calls: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    reasoning: Option<ReasoningBody<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_output_tokens: Option<u32>,
    store: bool,
    stream: bool,
}

#[derive(Serialize)]
struct MessageBody<'a> {
    r#type: &'static str,
    role: &'static str,
    content: [TextBody<'a>; 1],
}

#[derive(Serialize)]
struct TextBody<'a> {
    r#type: &'static str,
    text: &'a str,
}

#[derive(Serialize)]
struct ToolBody<'a> {
    r#type: &'static str,
    name: &'a str,
    description: &'a str,
    parameters: &'a Value,
    strict: bool,
}

#[derive(Serialize)]
struct ReasoningBody<'a> {
    effort: &'a str,
}

impl ResponsesRequest {
    /// JSON request body.
    pub fn encode(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(&RequestBody {
            model: &self.model,
            instructions: &self.instructions,
            input: self
                .input
                .iter()
                .map(|message| MessageBody {
                    r#type: "message",
                    role: "user",
                    content: [TextBody {
                        r#type: "input_text",
                        text: &message.text,
                    }],
                })
                .collect(),
            tools: self
                .tools
                .iter()
                .map(|tool| ToolBody {
                    r#type: "function",
                    name: &tool.name,
                    description: &tool.description,
                    parameters: &tool.parameters,
                    strict: tool.strict,
                })
                .collect(),
            tool_choice: self.tool_choice,
            parallel_tool_calls: self.parallel_tool_calls,
            reasoning: self
                .reasoning_effort
                .as_deref()
                .map(|effort| ReasoningBody { effort }),
            max_output_tokens: self.max_output_tokens,
            store: false,
            stream: true,
        })
    }
}

/// One function call from an answer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FunctionCall {
    /// Provider call identity.
    pub call_id: String,
    /// Called tool name.
    pub name: String,
    /// Arguments as the model wrote them: JSON text, not yet checked against the schema.
    pub arguments: String,
}

/// Static decode error; never contains provider text.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum OutputItemError {
    /// An item has no type, or a function call lacks a text field.
    #[error("invalid_output_item")]
    InvalidItem,
}

impl FunctionCall {
    /// Read one complete output item, as carried by a `response.output_item.done` event.
    /// Items of other types, such as reasoning or messages, give `None`.
    pub fn from_item(item: &Value) -> Result<Option<Self>, OutputItemError> {
        let text = |field: &str| {
            item[field]
                .as_str()
                .map(str::to_owned)
                .ok_or(OutputItemError::InvalidItem)
        };
        match item["type"].as_str() {
            Some("function_call") => Ok(Some(Self {
                call_id: text("call_id")?,
                name: text("name")?,
                arguments: text("arguments")?,
            })),
            Some(_) => Ok(None),
            None => Err(OutputItemError::InvalidItem),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::item_stream::{ItemEvent, ResponsesAdapter};
    use serde_json::json;

    fn check_request() -> ResponsesRequest {
        ResponsesRequest {
            model: "gpt-6.1-sol".into(),
            instructions: "You read files only through the read tool.".into(),
            input: vec![UserMessage {
                text: "Read lines 1 to 5 of README.md.".into(),
            }],
            tools: vec![FunctionTool {
                name: "read".into(),
                description: "Return numbered lines start..=end of one file.".into(),
                parameters: json!({
                    "type": "object",
                    "properties": {
                        "path": {"type": "string"},
                        "start": {"type": "integer"},
                        "end": {"type": "integer"}
                    },
                    "required": ["path", "start", "end"],
                    "additionalProperties": false
                }),
                strict: true,
            }],
            tool_choice: ToolChoice::Auto,
            parallel_tool_calls: true,
            reasoning_effort: Some("low".into()),
            max_output_tokens: Some(2000),
        }
    }

    #[test]
    fn encode_writes_the_body_the_backend_check_sent() {
        let body = check_request().encode().unwrap();
        assert_eq!(
            std::str::from_utf8(&body).unwrap(),
            include_str!("responses_tests/check_request.json").trim_end()
        );
    }

    #[test]
    fn encode_omits_unset_reasoning_and_output_limit() {
        let mut request = check_request();
        request.reasoning_effort = None;
        request.max_output_tokens = None;
        request.tool_choice = ToolChoice::Required;
        let body: Value = serde_json::from_slice(&request.encode().unwrap()).unwrap();
        assert!(body.get("reasoning").is_none());
        assert!(body.get("max_output_tokens").is_none());
        assert_eq!(body["tool_choice"], "required");
        assert_eq!(body["store"], false);
        assert_eq!(body["stream"], true);
    }

    #[test]
    fn from_item_reads_function_calls_and_skips_other_items() {
        let call = json!({"type":"function_call","id":"fc_1","call_id":"call_1",
            "name":"read","arguments":"{\"path\":\"a\"}","status":"completed"});
        assert_eq!(
            FunctionCall::from_item(&call),
            Ok(Some(FunctionCall {
                call_id: "call_1".into(),
                name: "read".into(),
                arguments: "{\"path\":\"a\"}".into(),
            }))
        );
        for item in [
            json!({"type":"reasoning","encrypted_content":"opaque"}),
            json!({"type":"message","role":"assistant","content":[]}),
        ] {
            assert_eq!(FunctionCall::from_item(&item), Ok(None));
        }
    }

    #[test]
    fn from_item_refuses_incomplete_function_calls() {
        for item in [
            json!({"type":"function_call","call_id":"c","name":"read","arguments":{}}),
            json!({"type":"function_call","name":"read","arguments":"{}"}),
            json!({"call_id":"c"}),
        ] {
            assert_eq!(
                FunctionCall::from_item(&item),
                Err(OutputItemError::InvalidItem)
            );
        }
    }

    #[test]
    fn recorded_backend_answer_yields_a_function_call_and_usage() {
        let mut adapter = ResponsesAdapter::default();
        let events = adapter
            .feed(include_bytes!("responses_tests/check_answer.sse"), 1 << 20)
            .unwrap();
        let mut calls = Vec::new();
        let mut usage = None;
        for event in events {
            match event {
                ItemEvent::ItemDone { item, .. } => {
                    calls.extend(FunctionCall::from_item(&item).unwrap());
                }
                ItemEvent::Usage {
                    usage: u,
                    completed: true,
                    ..
                } => usage = Some(u),
                ItemEvent::Error { code, .. } => panic!("provider error {code}"),
                _ => {}
            }
        }
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].name, "read");
        let arguments: Value = serde_json::from_str(&calls[0].arguments).unwrap();
        assert_eq!(arguments["path"], "README.md");
        assert!(usage.unwrap().input_tokens.is_some_and(|n| n > 0));
    }
}
