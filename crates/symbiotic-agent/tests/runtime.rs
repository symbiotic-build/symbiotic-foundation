use std::{
    collections::BTreeMap,
    fs,
    process::{Command, Output},
    sync::{Arc, Mutex},
};

use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    task::JoinHandle,
};

#[derive(Clone, Debug)]
struct Request {
    headers: BTreeMap<String, String>,
    body: Value,
}

struct Reply {
    status: u16,
    body: String,
}

fn call(name: &str, arguments: Value) -> Value {
    raw_call(name, &arguments.to_string())
}

fn raw_call(name: &str, arguments: &str) -> Value {
    json!({"type":"function_call","call_id":format!("call_{name}"),"name":name,"arguments":arguments})
}

fn finish() -> Value {
    call("finish", json!({"report":{"ok":true}}))
}

fn reply(items: Vec<Value>) -> Reply {
    let mut events = vec![json!({"type":"response.created","response":{"id":"resp_test"}})];
    events.extend(items.into_iter().enumerate().map(|(index,item)| json!({"type":"response.output_item.done","output_index":index,"item":item})));
    events.push(json!({"type":"response.completed","response":{"id":"resp_test","usage":{"input_tokens":10,"output_tokens":5}}}));
    Reply {
        status: 200,
        body: events
            .into_iter()
            .map(|event| format!("data: {event}\n\n"))
            .collect(),
    }
}

struct Server {
    url: String,
    requests: Arc<Mutex<Vec<Request>>>,
    worker: JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.worker.abort();
    }
}

impl Server {
    async fn start(replies: Vec<Reply>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/v1/responses", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let seen = requests.clone();
        let worker = tokio::spawn(async move {
            let mut replies = replies.into_iter();
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                let end = loop {
                    let mut buffer = [0; 4096];
                    let read = socket.read(&mut buffer).await.unwrap();
                    assert_ne!(read, 0, "request headers incomplete");
                    bytes.extend_from_slice(&buffer[..read]);
                    if let Some(end) = bytes.windows(4).position(|b| b == b"\r\n\r\n") {
                        break end + 4;
                    }
                };
                let header_text = std::str::from_utf8(&bytes[..end]).unwrap();
                assert!(header_text.starts_with("POST /v1/responses HTTP/1.1\r\n"));
                let headers: BTreeMap<_, _> = header_text
                    .lines()
                    .skip(1)
                    .filter_map(|line| line.split_once(':'))
                    .map(|(key, value)| (key.to_ascii_lowercase(), value.trim().to_owned()))
                    .collect();
                let length: usize = headers["content-length"].parse().unwrap();
                while bytes.len() - end < length {
                    let mut buffer = [0; 4096];
                    let read = socket.read(&mut buffer).await.unwrap();
                    assert_ne!(read, 0, "request body incomplete");
                    bytes.extend_from_slice(&buffer[..read]);
                }
                seen.lock().unwrap().push(Request {
                    headers,
                    body: serde_json::from_slice(&bytes[end..end + length]).unwrap(),
                });
                let reply = replies.next().unwrap_or(Reply {
                    status: 500,
                    body: "unexpected request".into(),
                });
                let header = format!(
                    "HTTP/1.1 {} Reply\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    reply.status,
                    reply.body.len()
                );
                socket.write_all(header.as_bytes()).await.unwrap();
                socket.write_all(reply.body.as_bytes()).await.unwrap();
                socket.shutdown().await.unwrap();
            }
        });
        Self {
            url,
            requests,
            worker,
        }
    }

    fn requests(&self) -> Vec<Request> {
        self.requests.lock().unwrap().clone()
    }
}

struct Fixture {
    _dir: TempDir,
    root: std::path::PathBuf,
    out: std::path::PathBuf,
    task: std::path::PathBuf,
    policy: std::path::PathBuf,
}

impl Fixture {
    fn new(policy_changes: Value) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("root");
        let out = dir.path().join("out");
        fs::create_dir(&root).unwrap();
        fs::create_dir(&out).unwrap();
        fs::write(
            root.join("file.txt"),
            "first line\nsecond line\nthird line\n",
        )
        .unwrap();
        let task = dir.path().join("task.json");
        fs::write(&task, json!({"instructions":"Read the task and return a report.","task":{"unit":"synthetic"},"report_schema":{"type":"object","properties":{"ok":{"type":"boolean"}},"required":["ok"],"additionalProperties":false}}).to_string()).unwrap();
        let policy = dir.path().join("policy.json");
        let mut config = json!({"model":"synthetic-model","reasoning_effort":"high","max_output_tokens":1000,"max_steps":6,"read_max_lines":200,"reads_per_step":8,"tool_output_max_chars":20000,"search_max_hits":50,"outline":null});
        for (key, value) in policy_changes.as_object().unwrap() {
            config[key] = value.clone();
        }
        fs::write(&policy, config.to_string()).unwrap();
        Self {
            _dir: dir,
            root,
            out,
            task,
            policy,
        }
    }

    async fn run(&self, server: &Server) -> Output {
        self.run_args(server, Vec::new()).await
    }

    async fn run_args(&self, server: &Server, extra: Vec<String>) -> Output {
        let mut command = self.command(&server.url);
        command.args(extra);
        tokio::task::spawn_blocking(move || command.output().unwrap())
            .await
            .unwrap()
    }

    fn report_schema(&self, schema: Value) {
        let mut task: Value = serde_json::from_slice(&fs::read(&self.task).unwrap()).unwrap();
        task["report_schema"] = schema;
        fs::write(&self.task, task.to_string()).unwrap();
    }

    fn command(&self, model_url: &str) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_symbiotic-agent"));
        command
            .env_remove("SYMBIOTIC_AGENT_MODEL_TOKEN")
            .arg("run")
            .arg("--task")
            .arg(&self.task)
            .arg("--policy")
            .arg(&self.policy)
            .arg("--root")
            .arg(&self.root)
            .arg("--out")
            .arg(&self.out)
            .arg("--model-url")
            .arg(model_url);
        command
    }

    fn logs(&self) -> Vec<Value> {
        fs::read_to_string(self.out.join("steps.jsonl"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    fn notes(&self) -> Value {
        serde_json::from_slice(&fs::read(self.out.join("notes.json")).unwrap()).unwrap()
    }
}

fn tail(request: &Request) -> &str {
    request.body["input"][1]["content"][0]["text"]
        .as_str()
        .unwrap()
}

// These cases issue one successful call, so the remaining tail is its complete result.
fn result_text(request: &Request) -> &str {
    tail(request)
        .split_once("\nResult step")
        .unwrap()
        .1
        .split_once('\n')
        .unwrap()
        .1
        .strip_suffix('\n')
        .unwrap()
}

fn exit(output: &Output, code: i32) {
    assert_eq!(
        output.status.code(),
        Some(code),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    if code == 0 {
        assert!(output.stderr.is_empty(), "{:?}", output);
    } else {
        assert!(output.stdout.is_empty());
        assert_eq!(String::from_utf8_lossy(&output.stderr).lines().count(), 1);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn optional_model_token_authorizes_every_request_without_leaking_to_outputs() {
    let token = "synthetic-model-token-7d3a";
    for configured in [true, false] {
        let fixture = Fixture::new(json!({}));
        let server = Server::start(vec![reply(vec![]), reply(vec![finish()])]).await;
        let mut command = fixture.command(&server.url);
        if configured {
            command.env("SYMBIOTIC_AGENT_MODEL_TOKEN", token);
        }
        let output = tokio::task::spawn_blocking(move || command.output().unwrap())
            .await
            .unwrap();
        exit(&output, 0);
        let requests = server.requests();
        assert_eq!(requests.len(), 2);
        let expected = configured.then(|| format!("Bearer {token}"));
        for request in requests {
            assert_eq!(request.headers.get("authorization"), expected.as_ref());
        }
        for bytes in [
            fs::read(fixture.out.join("steps.jsonl")).unwrap(),
            fs::read(fixture.out.join("notes.json")).unwrap(),
            output.stdout,
            output.stderr,
        ] {
            assert!(!String::from_utf8_lossy(&bytes).contains(token));
        }
    }
}

#[test]
fn empty_or_invalid_model_token_refuses_start_without_printing_the_value() {
    for (token, reason) in [
        ("", "SYMBIOTIC_AGENT_MODEL_TOKEN must not be empty"),
        (
            "synthetic-invalid-token\nheader-injection",
            "SYMBIOTIC_AGENT_MODEL_TOKEN is not a valid header value",
        ),
    ] {
        let fixture = Fixture::new(json!({}));
        let output = fixture
            .command("http://127.0.0.1:9/v1/responses")
            .env("SYMBIOTIC_AGENT_MODEL_TOKEN", token)
            .output()
            .unwrap();
        exit(&output, 1);
        assert_eq!(
            String::from_utf8(output.stderr).unwrap(),
            format!("symbiotic-agent: refused to start: {reason}\n")
        );
        assert_eq!(fs::read_dir(&fixture.out).unwrap().count(), 0);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_note_finish_persists_records_and_prints_only_report_fence() {
    let fixture = Fixture::new(json!({}));
    let note = json!({"kind":"covered","text":"read the file","refs":[{"path":"file.txt","ranges":[[1,2]]}]});
    let server = Server::start(vec![
        reply(vec![call(
            "read",
            json!({"path":"file.txt","start":1,"end":2}),
        )]),
        reply(vec![call("note", note.clone())]),
        reply(vec![finish(), call("note_drop", json!({"id":1}))]),
    ])
    .await;
    let output = fixture.run(&server).await;
    exit(&output, 0);
    assert_eq!(output.stdout, b"```json\n{\"ok\":true}\n```\n");
    let logs = fixture.logs();
    assert_eq!(logs.len(), 3);
    assert_eq!(
        logs[0],
        json!({"step":1,"steps_left":5,"final":false,"response_id":"resp_test","calls":[{"call_id":"call_read","name":"read","arguments":"{\"end\":2,\"path\":\"file.txt\",\"start\":1}","outcome":"ok","result_chars":54,"shown_chars":54}],"notes_added":[],"notes_dropped":[],"stop":null})
    );
    assert_eq!(logs[1]["notes_added"], json!([1]));
    assert_eq!(logs[2]["stop"], "finished");
    assert_eq!(logs[2]["calls"][1]["outcome"], "skipped");
    assert_eq!(
        fixture.notes(),
        json!({"next_id":2,"items":[{"id":1,"step":2,"kind":"covered","text":"read the file","refs":[{"path":"file.txt","ranges":[[1,2]]}]}]})
    );
    let requests = server.requests();
    assert!(
        tail(&requests[1]).contains("file.txt: lines 1-2 of 3\n1| first line\n2| second line\n")
    );
    assert!(tail(&requests[2]).contains("\"text\":\"read the file\""));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn results_expire_after_the_configured_number_of_steps() {
    for keep in [1, 2] {
        let fixture = Fixture::new(json!({"keep_results_steps":keep}));
        let server = Server::start(vec![
            reply(vec![call(
                "read",
                json!({"path":"file.txt","start":1,"end":1}),
            )]),
            reply(vec![]),
            reply(vec![]),
            reply(vec![finish()]),
        ])
        .await;
        exit(&fixture.run(&server).await, 0);
        let requests = server.requests();
        for (index, request) in requests.iter().enumerate().skip(1) {
            assert_eq!(
                tail(request).contains("1| first line"),
                index <= keep,
                "keep={keep}, step={}",
                index + 1
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn final_step_offers_only_required_finish_and_enforces_budget() {
    for valid in [true, false] {
        let fixture = Fixture::new(json!({"max_steps":3}));
        let server = Server::start(vec![
            reply(vec![]),
            reply(vec![]),
            reply(if valid { vec![finish()] } else { vec![] }),
        ])
        .await;
        exit(&fixture.run(&server).await, if valid { 0 } else { 3 });
        let requests = server.requests();
        assert_eq!(requests.len(), 3);
        assert_eq!(requests[2].body["tool_choice"], "required");
        assert_eq!(
            requests[2].body["tools"]
                .as_array()
                .unwrap()
                .iter()
                .map(|t| t["name"].as_str().unwrap())
                .collect::<Vec<_>>(),
            vec!["finish"]
        );
        assert!(tail(&requests[2]).starts_with("Step 3 of 3; 0 steps left after this one.\nThis is the last step. You must call finish.\n"));
        assert_eq!(
            fixture.logs()[2]["stop"],
            if valid { "finished" } else { "budget_spent" }
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_report_returns_first_json_path_to_the_next_step() {
    let fixture = Fixture::new(json!({}));
    let mut task: Value = serde_json::from_slice(&fs::read(&fixture.task).unwrap()).unwrap();
    task["report_schema"] = json!({"type":"object","properties":{"findings":{"type":"array","items":{"type":"object","properties":{"evidence":{"type":"string","minLength":1}},"required":["evidence"]}}},"required":["findings"]});
    fs::write(&fixture.task, task.to_string()).unwrap();
    let server = Server::start(vec![
        reply(vec![call("finish", json!({"report":{"findings":[{}]}}))]),
        reply(vec![call(
            "finish",
            json!({"report":{"findings":[{"evidence":"file.txt:1"}]}}),
        )]),
    ])
    .await;
    exit(&fixture.run(&server).await, 0);
    assert!(
        tail(&server.requests()[1])
            .contains("error: invalid report /findings/0/evidence: required property is missing")
    );
    assert_eq!(fixture.logs()[0]["calls"][0]["outcome"], "error");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_line_output_and_per_step_caps_are_visible() {
    let fixture =
        Fixture::new(json!({"read_max_lines":2,"tool_output_max_chars":150,"reads_per_step":2}));
    fs::write(
        fixture.root.join("long.txt"),
        format!("{}\nshort\nthird\n", "界".repeat(200)),
    )
    .unwrap();
    let server = Server::start(vec![
        reply(vec![
            call("read", json!({"path":"file.txt","start":1,"end":100})),
            call("read", json!({"path":"long.txt","start":1,"end":100})),
            call("read", json!({"path":"file.txt","start":1,"end":1})),
        ]),
        reply(vec![finish()]),
    ])
    .await;
    exit(&fixture.run(&server).await, 0);
    let requests = server.requests();
    let text = tail(&requests[1]);
    assert!(text.contains("[cut: read_max_lines 2; requested through line 3]"));
    assert!(!text.contains("3| third"));
    let capped = format!(
        "long.txt: lines 1-2 of 3\n1| {}[cut: 122 of 288 characters]",
        "界".repeat(94)
    );
    let result = text
        .split("\nResult step 1 tool \"read\" arguments ")
        .nth(2)
        .unwrap()
        .split_once('\n')
        .unwrap()
        .1
        .split("\n\nError step")
        .next()
        .unwrap();
    assert_eq!(result, capped);
    assert!(result.chars().count() <= 150);
    assert!(text.contains("error: reads_per_step exceeded"));
    let logs = fixture.logs();
    assert_eq!(logs[0]["calls"][1]["result_chars"], 288);
    assert_eq!(logs[0]["calls"][1]["shown_chars"], capped.chars().count());
    assert_eq!(logs[0]["calls"][2]["error"], "reads_per_step exceeded");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prefix_and_session_id_are_identical_for_all_requests() {
    let fixture = Fixture::new(json!({}));
    let outline = fixture._dir.path().join("outline.txt");
    fs::write(&outline, "outline line\n").unwrap();
    let mut policy: Value = serde_json::from_slice(&fs::read(&fixture.policy).unwrap()).unwrap();
    policy["outline"] = json!(outline);
    fs::write(&fixture.policy, policy.to_string()).unwrap();
    let server = Server::start(vec![reply(vec![]), reply(vec![]), reply(vec![finish()])]).await;
    exit(&fixture.run(&server).await, 0);
    let requests = server.requests();
    let session = &requests[0].headers["session-id"];
    let id = uuid::Uuid::parse_str(session).unwrap();
    assert_eq!(id.get_version_num(), 4);
    for request in &requests {
        assert_eq!(request.body["input"].as_array().unwrap().len(), 2);
        assert_eq!(request.body["input"][0], requests[0].body["input"][0]);
        assert_eq!(
            request.body["input"][0]["content"][0]["text"],
            "{\"unit\":\"synthetic\"}\noutline line\n"
        );
        assert_eq!(&request.headers["session-id"], session);
        assert_eq!(request.headers["content-type"], "application/json");
        assert_eq!(request.headers["accept"], "text/event-stream");
        assert_eq!(request.body["model"], "synthetic-model");
        assert_eq!(request.body["reasoning"]["effort"], "high");
        assert_eq!(request.body["max_output_tokens"], 1000);
        assert_eq!(
            request.body["instructions"],
            "Read the task and return a report."
        );
        assert_eq!(request.body["parallel_tool_calls"], true);
        assert_eq!(request.body["tool_choice"], "auto");
        assert_eq!(request.body["stream"], true);
        assert_eq!(request.body["store"], false);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn model_failure_stops_after_one_request_and_logs_the_failed_step() {
    for (response, id) in [
        (Reply {status:500,body:"unavailable".into()},Value::Null),
        (Reply {status:200,body:"data: {\"type\":\"response.failed\",\"response\":{\"id\":\"resp_fail\",\"error\":{\"code\":\"failed\"}}}\n\n".into()},json!("resp_fail")),
        (Reply {status:200,body:"data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_partial\"}}\n\n".into()},json!("resp_partial")),
    ] {
        let fixture = Fixture::new(json!({}));
        let server = Server::start(vec![response]).await;
        exit(&fixture.run(&server).await,2);
        assert_eq!(server.requests().len(),1);
        let logs = fixture.logs();
        assert_eq!(logs.len(),1);
        assert_eq!(logs[0]["stop"],"model_failed");
        assert_eq!(logs[0]["response_id"],id);
        assert_eq!(fixture.notes(),json!({"next_id":1,"items":[]}));
    }
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unsafe_paths_return_errors_without_outside_file_contents() {
    let fixture = Fixture::new(json!({"reads_per_step":64}));
    let outside = fixture._dir.path().join("outside.txt");
    fs::write(&outside, "OUTSIDE_CONTENT").unwrap();
    std::os::unix::fs::symlink(&outside, fixture.root.join("link")).unwrap();
    let mut calls = Vec::new();
    for path in [
        "",
        ".",
        "..",
        "../outside.txt",
        "/outside",
        "a//b",
        "a/./b",
        "a/../b",
        "a/",
        "a\\b",
        "a\0b",
        "missing",
        "link",
        outside.to_str().unwrap(),
    ] {
        calls.push(call("read", json!({"path":path,"start":1,"end":1})));
        calls.push(call(
            "search",
            // The pattern matches the outside text without containing it, since the tail
            // echoes every call's arguments.
            json!({"pattern":"OUTSIDE.CONTENT","path":path,"max_hits":null}),
        ));
    }
    let server = Server::start(vec![reply(calls), reply(vec![finish()])]).await;
    exit(&fixture.run(&server).await, 0);
    let logs = fixture.logs();
    assert_eq!(
        logs[0]["calls"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["outcome"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["error"; 28]
    );
    let requests = server.requests();
    assert!(!tail(&requests[1]).contains("OUTSIDE_CONTENT"));
    assert!(tail(&requests[1]).contains("path leaves root"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_startup_inputs_send_no_request() {
    for case in [
        "unknown_policy",
        "unsupported_schema",
        "missing_task",
        "duplicate_flag",
        "zero_limit",
    ] {
        let fixture = Fixture::new(json!({}));
        let server = Server::start(vec![]).await;
        let mut config: Value =
            serde_json::from_slice(&fs::read(&fixture.policy).unwrap()).unwrap();
        let mut task: Value = serde_json::from_slice(&fs::read(&fixture.task).unwrap()).unwrap();
        let mut extra = Vec::new();
        match case {
            "unknown_policy" => config["unexpected"] = json!(1),
            "unsupported_schema" => task["report_schema"]["pattern"] = json!("x"),
            "missing_task" => {
                task.as_object_mut().unwrap().remove("task");
            }
            "duplicate_flag" => extra = vec!["--model-url".into(), server.url.clone()],
            "zero_limit" => config["reads_per_step"] = json!(0),
            _ => unreachable!(),
        }
        fs::write(&fixture.policy, config.to_string()).unwrap();
        fs::write(&fixture.task, task.to_string()).unwrap();
        exit(&fixture.run_args(&server, extra).await, 1);
        assert!(server.requests().is_empty(), "{case}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn search_and_list_are_sorted_and_search_skips_binary_and_git_files() {
    let fixture = Fixture::new(json!({"search_max_hits":2}));
    fs::create_dir(fixture.root.join("sub")).unwrap();
    fs::create_dir(fixture.root.join("sub/.git")).unwrap();
    fs::write(fixture.root.join("sub/.git/hidden"), "needle").unwrap();
    fs::write(
        fixture.root.join("sub/a.txt"),
        "needle a\nneedle b\nneedle c\n",
    )
    .unwrap();
    fs::write(fixture.root.join("sub/b.bin"), b"needle\0hidden").unwrap();
    fs::write(fixture.root.join("sub/c.txt"), "needle d\n").unwrap();
    let server = Server::start(vec![
        reply(vec![
            call("list", json!({"path":"sub"})),
            call(
                "search",
                json!({"pattern":"needle","path":null,"max_hits":null}),
            ),
            call(
                "search",
                json!({"pattern":"needle","path":"sub","max_hits":20}),
            ),
            call(
                "search",
                json!({"pattern":"hidden|needle d","path":"sub","max_hits":null}),
            ),
        ]),
        reply(vec![finish()]),
    ])
    .await;
    exit(&fixture.run(&server).await, 0);
    let requests = server.requests();
    let text = tail(&requests[1]);
    assert!(text.contains(".git/\na.txt\nb.bin\nc.txt"));
    assert_eq!(
        text.matches(
            "sub/a.txt:1: needle a\nsub/a.txt:2: needle b\n[cut: search hit limit reached]"
        )
        .count(),
        2
    );
    assert!(text.contains("sub/c.txt:1: needle d"));
    assert!(!text.contains("needle c") && !text.contains("hidden\n"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_tool_calls_are_errors_and_note_ids_never_repeat() {
    let fixture = Fixture::new(json!({"reads_per_step":20}));
    let note = json!({"kind":"plan","text":"synthetic note","refs":[{"path":"missing.txt","ranges":[[1,2]]}]});
    let server = Server::start(vec![reply(vec![
        call("note",note.clone()), call("note_drop",json!({"id":1})), call("note",note),
        call("note_drop",json!({"id":1})),
        raw_call("read","{"),
        call("read",json!({"path":"file.txt","start":1,"end":2,"extra":true})),
        call("read",json!({"path":"file.txt","start":0,"end":2})),
        call("search",json!({"pattern":"[","path":null,"max_hits":null})),
        call("search",json!({"pattern":"x","path":"","max_hits":null})),
        call("note",json!({"kind":"covered","text":"bad ref","refs":[{"path":"file.txt","ranges":[[2,1]]}]})),
        call("note",json!({"kind":"covered","text":"bad ref","refs":[{"path":"../x","ranges":[[1,1]]}]})),
        call("unknown",json!({})),
    ]),reply(vec![finish()])]).await;
    exit(&fixture.run(&server).await, 0);
    let logs = fixture.logs();
    assert_eq!(logs[0]["notes_added"], json!([1, 2]));
    assert_eq!(logs[0]["notes_dropped"], json!([1]));
    assert_eq!(
        logs[0]["calls"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["outcome"].as_str().unwrap())
            .collect::<Vec<_>>(),
        [vec!["ok"; 3], vec!["error"; 9]].concat()
    );
    assert_eq!(
        fixture.notes(),
        json!({"next_id":3,"items":[{"id":2,"step":1,"kind":"plan","text":"synthetic note","refs":[{"path":"missing.txt","ranges":[[1,2]]}]}]})
    );
    let requests = server.requests();
    assert!(tail(&requests[1]).contains("unknown note id: 1"));
    assert!(tail(&requests[1]).contains("unknown tool: unknown"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn notes_write_failure_logs_io_failed_and_does_not_print_a_report() {
    let fixture = Fixture::new(json!({}));
    // A directory at the record destination makes the atomic rename fail.
    fs::create_dir(fixture.out.join("notes.json")).unwrap();
    let server = Server::start(vec![reply(vec![finish()])]).await;
    exit(&fixture.run(&server).await, 4);
    assert_eq!(server.requests().len(), 1);
    assert_eq!(fixture.logs()[0]["stop"], "io_failed");
    assert_eq!(fs::read_dir(&fixture.out).unwrap().count(), 2);
}

#[test]
fn command_line_requires_exact_flags_and_version_prints_one_line() {
    let binary = env!("CARGO_BIN_EXE_symbiotic-agent");
    let output = Command::new(binary).arg("--version").output().unwrap();
    exit(&output, 0);
    assert_eq!(
        output.stdout,
        format!("symbiotic-agent {}\n", env!("CARGO_PKG_VERSION")).as_bytes()
    );
    for (args, reason) in [
        (
            vec!["run", "--task", "a", "--task", "b"],
            "duplicate argument: --task",
        ),
        (vec!["run", "--unknown", "a"], "unknown argument: --unknown"),
        (vec!["run", "--root"], "missing value for --root"),
        (vec!["--version", "extra"], "expected run or --version"),
        (vec![], "expected run or --version"),
    ] {
        let output = Command::new(binary).args(args).output().unwrap();
        exit(&output, 1);
        assert!(String::from_utf8_lossy(&output.stderr).contains(reason));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn report_constraints_return_json_paths_and_accept_valid_reports() {
    for (schema, good, bad, expected_path) in [
        (
            json!({"type":"string","minLength":2}),
            json!("界a"),
            json!("界"),
            "/",
        ),
        (
            json!({"type":["integer","null"]}),
            json!(null),
            json!(1.5),
            "/",
        ),
        (json!({"type":"number"}), json!(1.5), json!(true), "/"),
        (json!({"type":"boolean"}), json!(true), json!(1), "/"),
        (json!({"type":"null"}), json!(null), json!(false), "/"),
        (
            json!({"type":"array","minItems":1,"maxItems":2,"items":{"enum":[1,2]}}),
            json!([1, 2]),
            json!([3]),
            "/0",
        ),
        (json!({"minItems":1}), json!([1]), json!([]), "/"),
        (json!({"maxItems":1}), json!([1]), json!([1, 2]), "/"),
        (
            json!({"type":"object","properties":{"a/b~c":{"type":"string"}},"required":["a/b~c"],"additionalProperties":false}),
            json!({"a/b~c":"ok"}),
            json!({}),
            "/a~1b~0c",
        ),
        (
            json!({"properties":{},"additionalProperties":false}),
            json!({}),
            json!({"extra":1}),
            "/extra",
        ),
        (
            json!({"enum":[9007199254740992_u64]}),
            json!(9007199254740992_u64),
            json!(9007199254740993_u64),
            "/",
        ),
        (
            json!({"type":"integer","enum":[1]}),
            json!(1.0),
            json!(2),
            "/",
        ),
    ] {
        let fixture = Fixture::new(json!({}));
        fixture.report_schema(schema.clone());
        let server = Server::start(vec![
            reply(vec![call("finish", json!({"report":bad}))]),
            reply(vec![call("finish", json!({"report":good}))]),
        ])
        .await;
        let output = fixture.run(&server).await;
        exit(&output, 0);
        assert_eq!(output.stdout, format!("```json\n{good}\n```\n").as_bytes());
        let logs = fixture.logs();
        assert_eq!(logs[0]["calls"][0]["outcome"], "error", "{schema} {bad}");
        assert!(
            logs[0]["calls"][0]["error"]
                .as_str()
                .unwrap()
                .starts_with(&format!("invalid report {expected_path}:")),
            "{schema} {bad}"
        );
        assert_eq!(logs[1]["stop"], "finished");
    }
}

#[test]
fn unsupported_or_malformed_report_schemas_refuse_start() {
    for schema in [
        json!({"pattern":"x"}),
        json!({"properties":{"nested":{"format":"uri"}}}),
        json!({"items":{"type":"unknown"}}),
        json!({"type":["string","unknown"]}),
        json!({"type":[]}),
        json!({"additionalProperties":{}}),
        json!({"required":[1]}),
        json!({"minLength":-1}),
        json!({"minItems":"1"}),
        json!({"maxItems":1.5}),
        json!({"enum":[]}),
        json!({"description":1}),
        json!({"items":true}),
        json!({"properties":[]}),
    ] {
        let fixture = Fixture::new(json!({}));
        fixture.report_schema(schema.clone());
        let output = fixture
            .command("http://127.0.0.1:9/v1/responses")
            .output()
            .unwrap();
        exit(&output, 1);
        assert!(
            String::from_utf8_lossy(&output.stderr)
                .starts_with("symbiotic-agent: refused to start:"),
            "{schema}"
        );
        assert_eq!(fs::read_dir(&fixture.out).unwrap().count(), 0);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn required_without_properties_starts_and_checks_presence_only() {
    for schema in [
        json!({"type":"object","required":["unit"]}),
        json!({"type":"object","properties":{},"required":["unit"]}),
    ] {
        let fixture = Fixture::new(json!({}));
        fixture.report_schema(schema);
        let server = Server::start(vec![
            reply(vec![call("finish", json!({"report":{}}))]),
            reply(vec![call("finish", json!({"report":{"unit":42}}))]),
        ])
        .await;
        let output = fixture.run(&server).await;
        exit(&output, 0);
        assert_eq!(output.stdout, b"```json\n{\"unit\":42}\n```\n");
        assert_eq!(
            fixture.logs()[0]["calls"][0]["error"],
            "invalid report /unit: required property is missing"
        );
        assert!(
            tail(&server.requests()[1])
                .contains("error: invalid report /unit: required property is missing")
        );
        assert_eq!(fixture.logs()[1]["stop"], "finished");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_null_path_returns_sorted_root_entries() {
    let fixture = Fixture::new(json!({}));
    fs::create_dir(fixture.root.join("a")).unwrap();
    fs::write(fixture.root.join("z.txt"), "last").unwrap();
    fs::write(fixture.root.join("b.txt"), "middle").unwrap();
    let server = Server::start(vec![
        reply(vec![call("list", json!({"path":null}))]),
        reply(vec![finish()]),
    ])
    .await;
    exit(&fixture.run(&server).await, 0);
    assert_eq!(
        result_text(&server.requests()[1]),
        "a/\nb.txt\nfile.txt\nz.txt"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn search_skips_non_utf8_and_special_files_and_reads_past_binary_prefix() {
    let fixture = Fixture::new(json!({}));
    fs::write(fixture.root.join("a-invalid.txt"), b"needle\xff").unwrap();
    let mut late_invalid = vec![b'a'; 8192];
    late_invalid.extend_from_slice(b"needle\xff");
    fs::write(fixture.root.join("a-late-invalid.txt"), late_invalid).unwrap();
    let mut binary = vec![b'a'; 8191];
    binary.extend_from_slice(b"\0needle");
    fs::write(fixture.root.join("b-binary"), binary).unwrap();
    #[cfg(unix)]
    {
        assert!(
            Command::new("mkfifo")
                .arg(fixture.root.join("b-fifo"))
                .status()
                .unwrap()
                .success()
        );
        std::os::unix::fs::symlink("missing", fixture.root.join("d-dangling")).unwrap();
    }
    fs::write(
        fixture.root.join("z-text.txt"),
        format!("{}\nneedle text\n", "a".repeat(8192)),
    )
    .unwrap();
    let server = Server::start(vec![
        reply(vec![call(
            "search",
            json!({"pattern":"needle","path":null,"max_hits":null}),
        )]),
        reply(vec![finish()]),
    ])
    .await;
    exit(&fixture.run(&server).await, 0);
    assert_eq!(
        result_text(&server.requests()[1]),
        "z-text.txt:2: needle text"
    );
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn search_skips_unreadable_files_and_directories_and_returns_text_hits() {
    use std::os::unix::fs::PermissionsExt;
    let fixture = Fixture::new(json!({}));
    let file = fixture.root.join("a-unreadable.txt");
    let directory = fixture.root.join("b-unreadable");
    fs::write(&file, "needle unreadable").unwrap();
    fs::create_dir(&directory).unwrap();
    fs::write(directory.join("hidden.txt"), "needle hidden").unwrap();
    fs::write(fixture.root.join("z-text.txt"), "needle text\n").unwrap();
    let file_permissions = fs::metadata(&file).unwrap().permissions();
    let directory_permissions = fs::metadata(&directory).unwrap().permissions();
    fs::set_permissions(&file, fs::Permissions::from_mode(0o0)).unwrap();
    fs::set_permissions(&directory, fs::Permissions::from_mode(0o0)).unwrap();
    let server = Server::start(vec![
        reply(vec![call(
            "search",
            json!({"pattern":"needle","path":null,"max_hits":null}),
        )]),
        reply(vec![finish()]),
    ])
    .await;
    let output = fixture.run(&server).await;
    fs::set_permissions(&file, file_permissions).unwrap();
    fs::set_permissions(&directory, directory_permissions).unwrap();
    exit(&output, 0);
    assert_eq!(
        result_text(&server.requests()[1]),
        "z-text.txt:1: needle text"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_streams_requested_lines_and_counts_the_remaining_lines() {
    for (bytes, start, expected) in [
        (
            &b"one\ntwo\nthree\n"[..],
            2,
            "file.txt: lines 2-3 of 3\n2| two\n3| three\n",
        ),
        (
            &b"one\r\ntwo\r\nthree\r"[..],
            1,
            "file.txt: lines 1-2 of 3\n1| one\n2| two\n[cut: read_max_lines 2; requested through line 3]\n",
        ),
        (
            &b"\n\nlast"[..],
            1,
            "file.txt: lines 1-2 of 3\n1| \n2| \n[cut: read_max_lines 2; requested through line 3]\n",
        ),
    ] {
        let fixture = Fixture::new(json!({"read_max_lines":2}));
        fs::write(fixture.root.join("file.txt"), bytes).unwrap();
        let server = Server::start(vec![
            reply(vec![call(
                "read",
                json!({"path":"file.txt","start":start,"end":100}),
            )]),
            reply(vec![finish()]),
        ])
        .await;
        exit(&fixture.run(&server).await, 0);
        assert_eq!(result_text(&server.requests()[1]), expected);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn output_cap_counts_unicode_and_keeps_the_marker_for_tiny_limits() {
    for (limit, expected) in [
        (
            60,
            format!(
                "file.txt: lines 1-1 of 1\n1| {}[cut: 33 of 229 characters]",
                "界".repeat(5)
            ),
        ),
        (1, "[cut: 0 of 229 characters]".into()),
    ] {
        let fixture = Fixture::new(json!({"tool_output_max_chars":limit}));
        fs::write(fixture.root.join("file.txt"), "界".repeat(200)).unwrap();
        let server = Server::start(vec![
            reply(vec![call(
                "read",
                json!({"path":"file.txt","start":1,"end":1}),
            )]),
            reply(vec![finish()]),
        ])
        .await;
        exit(&fixture.run(&server).await, 0);
        let requests = server.requests();
        let result = result_text(&requests[1]);
        assert_eq!(result, expected);
        assert_eq!(
            fixture.logs()[0]["calls"][0]["shown_chars"],
            expected.chars().count()
        );
    }
    let fixture = Fixture::new(json!({"tool_output_max_chars":2}));
    fs::write(fixture.root.join("界a"), "").unwrap();
    fs::remove_file(fixture.root.join("file.txt")).unwrap();
    let server = Server::start(vec![
        reply(vec![call("list", json!({"path":null}))]),
        reply(vec![finish()]),
    ])
    .await;
    exit(&fixture.run(&server).await, 0);
    assert_eq!(result_text(&server.requests()[1]), "界a");
}
