use reqwest::{
    Client, StatusCode, Url,
    header::{AUTHORIZATION, HeaderMap, HeaderValue},
};
use symbiotic_model::{
    item_stream::{ItemEvent, ResponsesAdapter},
    responses::{FunctionCall, ResponsesRequest},
};
use uuid::Uuid;

pub(crate) struct Model {
    client: Client,
    url: Url,
    session_id: String,
}

#[derive(Default)]
pub(crate) struct Answer {
    pub response_id: Option<String>,
    pub calls: Vec<FunctionCall>,
}

impl Model {
    pub fn new(url: &str) -> Result<Self, String> {
        let url = Url::parse(url).map_err(|e| format!("model-url: {e}"))?;
        if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
            return Err("model-url must be an HTTP or HTTPS URL".into());
        }
        let mut headers = HeaderMap::new();
        match std::env::var("SYMBIOTIC_AGENT_MODEL_TOKEN") {
            Ok(token) => {
                if token.is_empty() {
                    return Err("SYMBIOTIC_AGENT_MODEL_TOKEN must not be empty".into());
                }
                let mut value = HeaderValue::from_str(&format!("Bearer {token}"))
                    .map_err(|_| "SYMBIOTIC_AGENT_MODEL_TOKEN is not a valid header value")?;
                value.set_sensitive(true);
                headers.insert(AUTHORIZATION, value);
            }
            Err(std::env::VarError::NotPresent) => {}
            Err(std::env::VarError::NotUnicode(_)) => {
                return Err("SYMBIOTIC_AGENT_MODEL_TOKEN must be valid Unicode".into());
            }
        }
        let client = Client::builder()
            .default_headers(headers)
            .no_proxy()
            .http1_only()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .build()
            .map_err(|e| e.to_string())?;
        Ok(Self {
            client,
            url,
            session_id: Uuid::new_v4().to_string(),
        })
    }

    // Populate the answer during parsing so failure logs retain any response id received.
    pub async fn call(
        &self,
        request: &ResponsesRequest,
        answer: &mut Answer,
    ) -> Result<(), String> {
        let mut response = self
            .client
            .post(self.url.clone())
            .header("content-type", "application/json")
            .header("accept", "text/event-stream")
            .header("session-id", &self.session_id)
            .body(request.encode().map_err(|e| e.to_string())?)
            .send()
            .await
            .map_err(|e| format!("request failed: {e}"))?;
        if response.status() != StatusCode::OK {
            return Err(format!("HTTP {}", response.status()));
        }
        let mut adapter = ResponsesAdapter::default();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|e| format!("broken stream: {e}"))?
        {
            let mut completed = false;
            for event in adapter
                .feed(&chunk, 64 * 1024 * 1024)
                .map_err(|e| e.to_string())?
            {
                if let Some(id) = &event.record().response_id {
                    answer.response_id = Some(id.clone());
                }
                match event {
                    ItemEvent::ItemDone { item, .. } => answer
                        .calls
                        .extend(FunctionCall::from_item(&item).map_err(|e| e.to_string())?),
                    ItemEvent::Usage {
                        completed: true, ..
                    } => completed = true,
                    ItemEvent::Error { code, .. } => return Err(format!("provider error: {code}")),
                    _ => {}
                }
            }
            if completed {
                return Ok(());
            }
        }
        Err("stream ended without response.completed".into())
    }
}
