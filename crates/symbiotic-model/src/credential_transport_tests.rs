//! Raw adapter regression fixtures. The private Gemini endpoint override exists
//! only in this crate's unit-test build; production cannot inject transports.
use super::*;
use std::{
    io::{Read, Write},
    net::TcpListener,
};

const KEY: &str = "synthetic-private-key-7613";

fn fixture(
    listener: TcpListener,
    status: u16,
    body: &str,
    location: Option<&str>,
) -> (String, std::thread::JoinHandle<String>) {
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let body = body.to_owned();
    let location = location.map(str::to_owned);
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream.set_nodelay(true).unwrap();
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();
        let mut bytes = Vec::new();
        let head = loop {
            let mut chunk = [0; 4096];
            let n = stream.read(&mut chunk).unwrap();
            assert!(n > 0);
            bytes.extend_from_slice(&chunk[..n]);
            if let Some(offset) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                let headers = String::from_utf8(bytes[..offset].to_vec()).unwrap();
                let length = headers
                    .lines()
                    .find_map(|line| {
                        let (key, value) = line.split_once(':')?;
                        key.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().unwrap())
                    })
                    .unwrap();
                if bytes.len() >= offset + 4 + length {
                    break headers;
                }
            }
        };
        let location = location.map_or(String::new(), |url| format!("Location: {url}\r\n"));
        write!(stream, "HTTP/1.1 {status} Fixture\r\nContent-Type: application/json\r\n{location}Content-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
        head
    });
    (endpoint, server)
}

fn embedding_request(batch: bool) -> EmbeddingRequest {
    EmbeddingRequest {
        inputs: if batch {
            vec!["one".into(), "two".into()]
        } else {
            vec!["one".into()]
        },
        dimensions: None,
        task: None,
        role_binding: None,
        source: None,
        metadata: Value::Null,
    }
}

#[tokio::test]
async fn gemini_single_and_batch_results_cross_the_complete_credential_boundary() {
    for batch in [false, true] {
        for (status, key, body) in [
            (401, KEY, format!("unauthorized {KEY}")),
            (200, KEY, format!(r#"{{"embedding":{{"values":[0.5]}},"embeddings":[{{"values":[0.5]}},{{"values":[0.5]}}],"ignored":"{KEY}"}}"#)),
            (200, KEY, format!(r#"{{"embedding":{{"values":["{KEY}"]}},"embeddings":[{{"values":["{KEY}"]}}]}}"#)),
            (200, "123400000", r#"{"embedding":{"values":[123400000]},"embeddings":[{"values":[123400000]},{"values":[123400000]}]}"#.into()),
        ] {
            let (endpoint, server) = fixture(TcpListener::bind("127.0.0.1:0").unwrap(), status, &body, None);
            let result = GeminiEmbeddingProvider::new("gemini", "model", key, 1)
                .at_test_endpoint(endpoint).with_timeout(2).unwrap()
                .with_request_limit(65536).with_response_limit(65536)
                .embed(embedding_request(batch)).await;
            let head = server.join().unwrap();
            assert!(head.to_ascii_lowercase().contains("x-goog-api-key:"));
            let error = result.unwrap_err();
            assert!(!error.to_string().contains(key), "credential escaped final result");
            if status == 401 { assert!(matches!(error.primary(), ModelError::Auth(_))); }
        }
        let body = r#"{"embedding":{"values":[0.5]},"embeddings":[{"values":[0.5]},{"values":[0.5]}],"ignored":"private raw detail"}"#;
        let (endpoint, server) =
            fixture(TcpListener::bind("127.0.0.1:0").unwrap(), 200, body, None);
        let response = GeminiEmbeddingProvider::new("gemini", "model", KEY, 1)
            .at_test_endpoint(endpoint)
            .with_request_limit(65536)
            .with_response_limit(65536)
            .embed(embedding_request(batch))
            .await
            .unwrap();
        server.join().unwrap();
        assert!(response.raw_provider_response.is_none());
        assert!(
            !serde_json::to_string(&response)
                .unwrap()
                .contains("private raw detail")
        );
    }
}

#[tokio::test]
async fn gemini_redirects_and_hidden_http_retries_never_send_a_second_request() {
    for status in [302, 307, 503] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let origin = listener.try_clone().unwrap();
        let target = TcpListener::bind("127.0.0.1:0").unwrap();
        let location = if status == 307 {
            "/same-origin".to_owned()
        } else {
            format!(
                "http://localhost:{}/cross-host",
                target.local_addr().unwrap().port()
            )
        };
        let (endpoint, server) = fixture(listener, status, "{}", Some(&location));
        let error = GeminiEmbeddingProvider::new("gemini", "model", KEY, 1)
            .at_test_endpoint(endpoint)
            .with_timeout(2)
            .unwrap()
            .with_request_limit(65536)
            .with_response_limit(65536)
            .embed(embedding_request(false))
            .await
            .unwrap_err();
        server.join().unwrap();
        assert!(matches!(
            error.primary(),
            ModelError::Provider(_) | ModelError::Unavailable(_)
        ));
        for listener in [origin, target] {
            listener.set_nonblocking(true).unwrap();
            assert_eq!(
                listener.accept().unwrap_err().kind(),
                std::io::ErrorKind::WouldBlock
            );
        }
    }
}

#[tokio::test]
async fn chat_classifier_final_errors_and_normalized_answers_cross_the_same_boundary() {
    for (key, reply, questions) in [
        (
            "123400000",
            r#"{"answer":123400000}"#,
            vec![ClassifierQuestion::noul("answer", "yes?", None, None)],
        ),
        (
            KEY,
            r#"{"answer":"private decoding detail"}"#,
            vec![ClassifierQuestion::noul("answer", "yes?", None, None)],
        ),
        (
            "0.3",
            r#"{"answer":{"a":0.294,"b":0.686}}"#,
            vec![ClassifierQuestion::choice(
                "answer",
                "which?",
                [("a", "first"), ("b", "second")],
            )],
        ),
    ] {
        let body = serde_json::json!({"choices":[{"message":{"content":reply}}]}).to_string();
        let (endpoint, server) =
            fixture(TcpListener::bind("127.0.0.1:0").unwrap(), 200, &body, None);
        let chat = OpenAiCompatibleChatProvider::new("op", "model", endpoint, key)
            .with_request_limit(65536)
            .with_response_limit(65536);
        let provider = ChatClassifierProvider::new(Arc::new(chat)).with_max_output_tokens(128);
        let error = provider
            .classify(ClassifyRequest::new(serde_json::Map::new(), questions))
            .await
            .unwrap_err();
        server.join().unwrap();
        assert!(matches!(error, ModelError::Provider(_)));
        assert!(!error.to_string().contains(key));
    }
}
