//! Bounded-step agent runtime. Interface: crates/symbiotic-agent/README.md.

mod client;
mod notes;
mod policy;
mod step;
mod task;
mod tools;

use std::{collections::BTreeMap, fs, path::PathBuf, process::ExitCode};

use crate::{
    client::Model,
    policy::Policy,
    step::{Failure, Job},
    task::Task,
    tools::Tools,
};

fn prepare(args: Vec<String>) -> Result<Job, String> {
    if args.first().map(String::as_str) != Some("run") {
        return Err("expected run or --version".into());
    }
    let mut flags = BTreeMap::new();
    let mut args = args.into_iter().skip(1);
    while let Some(flag) = args.next() {
        if !matches!(
            flag.as_str(),
            "--task" | "--policy" | "--root" | "--model-url" | "--out"
        ) {
            return Err(format!("unknown argument: {flag}"));
        }
        let value = args
            .next()
            .filter(|v| !v.starts_with("--"))
            .ok_or_else(|| format!("missing value for {flag}"))?;
        if flags.insert(flag.clone(), value).is_some() {
            return Err(format!("duplicate argument: {flag}"));
        }
    }
    let value = |flag: &str| {
        flags
            .get(flag)
            .ok_or_else(|| format!("missing argument: {flag}"))
    };
    let task: Task =
        serde_json::from_slice(&fs::read(value("--task")?).map_err(|e| format!("task: {e}"))?)
            .map_err(|e| format!("task: {e}"))?;
    let policy: Policy =
        serde_json::from_slice(&fs::read(value("--policy")?).map_err(|e| format!("policy: {e}"))?)
            .map_err(|e| format!("policy: {e}"))?;
    policy.validate()?;
    task::check_schema(&task.report_schema)?;
    let mut prefix = serde_json::to_string(&task.task).map_err(|e| e.to_string())?;
    if let Some(outline) = &policy.outline {
        prefix.push('\n');
        prefix.push_str(&fs::read_to_string(outline).map_err(|e| format!("outline: {e}"))?);
    }
    let tools = Tools::new(&PathBuf::from(value("--root")?))?;
    let out = PathBuf::from(value("--out")?);
    if !out.is_dir() {
        return Err("out must be an existing directory".into());
    }
    let model = Model::new(value("--model-url")?)?;
    Ok(Job {
        task,
        policy,
        tools,
        model,
        out,
        prefix,
    })
}

/// Run the command-line interface.
#[doc(hidden)]
pub async fn run() -> ExitCode {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args == ["--version"] {
        println!("symbiotic-agent {}", env!("CARGO_PKG_VERSION"));
        return ExitCode::SUCCESS;
    }
    let result = match prepare(args) {
        Ok(job) => job.run().await,
        Err(reason) => Err(Failure {
            code: 1,
            reason: format!("refused to start: {reason}"),
        }),
    };
    match result {
        Ok(report) => {
            println!("```json\n{report}\n```");
            ExitCode::SUCCESS
        }
        Err(failure) => {
            // Paths and provider errors can contain newlines; stderr has one physical line.
            eprintln!(
                "symbiotic-agent: {}",
                failure.reason.replace(['\n', '\r'], " ")
            );
            ExitCode::from(failure.code)
        }
    }
}

/// Exercise one model-controlled call using the runtime's decoders and checks.
/// `root` is the directory the file tools read.
#[doc(hidden)]
pub fn fuzz_call(
    root: &std::path::Path,
    name: &str,
    arguments: &str,
    report_schema: &serde_json::Value,
) {
    let tools = Tools::new(root).expect("open fuzz root");
    let policy = Policy {
        model: String::new(),
        reasoning_effort: None,
        max_output_tokens: None,
        max_steps: 2,
        read_max_lines: 2,
        reads_per_step: 8,
        tool_output_max_chars: 150,
        search_max_hits: 2,
        keep_results_steps: 1,
        outline: None,
    };
    let _ = step::execute_call(
        name,
        arguments,
        &tools,
        &policy,
        report_schema,
        &mut notes::Notes::new(),
        1,
    );
}
