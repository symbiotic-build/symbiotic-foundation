use std::{
    fs::{File, OpenOptions},
    io::Write,
    path::PathBuf,
};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use symbiotic_model::responses::{FunctionCall, ResponsesRequest, ToolChoice, UserMessage};

use crate::{
    client::{Answer, Model},
    notes::{Note, Notes},
    policy::Policy,
    task::{Task, validate_report},
    tools::{self, Tools},
};

pub(crate) struct Job {
    pub task: Task,
    pub policy: Policy,
    pub tools: Tools,
    pub model: Model,
    pub out: PathBuf,
    pub prefix: String,
}

pub(crate) struct Failure {
    pub code: u8,
    pub reason: String,
}

impl Failure {
    pub fn io(reason: String) -> Self {
        Self {
            code: 4,
            reason: format!("io_failed: {reason}"),
        }
    }
}

#[derive(Serialize)]
struct CallLog {
    call_id: String,
    name: String,
    arguments: String,
    outcome: &'static str,
    result_chars: usize,
    shown_chars: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Serialize)]
struct StepLog {
    step: usize,
    steps_left: usize,
    r#final: bool,
    response_id: Option<String>,
    calls: Vec<CallLog>,
    notes_added: Vec<u64>,
    notes_dropped: Vec<u64>,
    stop: Option<&'static str>,
}

struct ResultRecord {
    step: usize,
    name: String,
    arguments: String,
    text: String,
    error: bool,
}

impl ResultRecord {
    fn append(&self, tail: &mut String) {
        tail.push_str(&format!(
            "\n{} step {} tool {} arguments {}\n{}\n",
            if self.error { "Error" } else { "Result" },
            self.step,
            serde_json::Value::String(self.name.clone()),
            serde_json::Value::String(self.arguments.clone()),
            self.text
        ));
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Finish {
    #[serde(deserialize_with = "crate::policy::present")]
    report: Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DropNote {
    id: u64,
}

impl Job {
    pub async fn run(self) -> Result<Value, Failure> {
        let mut log_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(self.out.join("steps.jsonl"))
            .map_err(|e| Failure::io(format!("steps.jsonl: {e}")))?;
        let mut notes = Notes::new();
        let mut records: Vec<ResultRecord> = Vec::new();
        for step in 1..=self.policy.max_steps {
            let final_step = step == self.policy.max_steps;
            let mut log = StepLog {
                step,
                steps_left: self.policy.max_steps - step,
                r#final: final_step,
                response_id: None,
                calls: Vec::new(),
                notes_added: Vec::new(),
                notes_dropped: Vec::new(),
                stop: None,
            };
            records.retain(|r| step - r.step <= self.policy.keep_results_steps);
            let mut tail = format!(
                "Step {step} of {}; {} steps left after this one.\n",
                self.policy.max_steps, log.steps_left
            );
            if final_step {
                tail.push_str("This is the last step. You must call finish.\n");
            }
            tail.push_str("Notes\n");
            tail.push_str(&notes.compact().map_err(|e| Failure::io(e.to_string()))?);
            tail.push('\n');
            for error in [false, true] {
                for record in records.iter().filter(|r| r.error == error) {
                    record.append(&mut tail);
                }
            }
            let request = ResponsesRequest {
                model: self.policy.model.clone(),
                instructions: self.task.instructions.clone(),
                input: vec![
                    UserMessage {
                        text: self.prefix.clone(),
                    },
                    UserMessage { text: tail },
                ],
                tools: tools::definitions(&self.task.report_schema, final_step),
                tool_choice: if final_step {
                    ToolChoice::Required
                } else {
                    ToolChoice::Auto
                },
                parallel_tool_calls: true,
                reasoning_effort: self.policy.reasoning_effort.clone(),
                max_output_tokens: self.policy.max_output_tokens,
            };
            let mut answer = Answer::default();
            let model_result = self.model.call(&request, &mut answer).await;
            log.response_id = answer.response_id;
            if let Err(reason) = model_result {
                log.stop = Some("model_failed");
                self.persist(&notes, &mut log_file, &mut log)?;
                return Err(Failure {
                    code: 2,
                    reason: format!("model_failed: {reason}"),
                });
            }
            let mut report = None;
            let mut reads = 0;
            if answer.calls.is_empty() {
                records.push(ResultRecord {
                    step,
                    name: "none".into(),
                    arguments: "{}".into(),
                    text: tools::cap(
                        "error: answer contained no tool call",
                        self.policy.tool_output_max_chars,
                    )
                    .text,
                    error: true,
                });
            }
            for call in answer.calls {
                if report.is_some() {
                    log.calls.push(CallLog {
                        call_id: call.call_id,
                        name: call.name,
                        arguments: call.arguments,
                        outcome: "skipped",
                        result_chars: 0,
                        shown_chars: 0,
                        error: None,
                    });
                    continue;
                }
                let result = self.execute(&call, &mut notes, &mut log, &mut reads, &mut report);
                let (text, error) = match result {
                    Ok(text) => (text, None),
                    Err(error) => (format!("error: {error}"), Some(error)),
                };
                let capped = tools::cap(&text, self.policy.tool_output_max_chars);
                records.push(ResultRecord {
                    step,
                    name: call.name.clone(),
                    arguments: call.arguments.clone(),
                    text: capped.text,
                    error: error.is_some(),
                });
                log.calls.push(CallLog {
                    call_id: call.call_id,
                    name: call.name,
                    arguments: call.arguments,
                    outcome: if error.is_some() { "error" } else { "ok" },
                    result_chars: capped.total,
                    shown_chars: capped.shown,
                    error,
                });
            }
            log.stop = if report.is_some() {
                Some("finished")
            } else if final_step {
                Some("budget_spent")
            } else {
                None
            };
            self.persist(&notes, &mut log_file, &mut log)?;
            if let Some(report) = report {
                return Ok(report);
            }
            if final_step {
                return Err(Failure {
                    code: 3,
                    reason: "budget_spent: no valid finish report".into(),
                });
            }
        }
        unreachable!("policy requires at least two steps")
    }

    fn execute(
        &self,
        call: &FunctionCall,
        notes: &mut Notes,
        log: &mut StepLog,
        reads: &mut usize,
        report: &mut Option<Value>,
    ) -> Result<String, String> {
        if log.r#final && call.name != "finish" {
            return Err("only finish is allowed on the last step".into());
        }
        match call.name.as_str() {
            "read" | "search" | "list" => {
                *reads += 1;
                if *reads > self.policy.reads_per_step {
                    return Err("reads_per_step exceeded".into());
                }
                self.tools.run(&call.name, &call.arguments, &self.policy)
            }
            "note" => {
                let note: Note = tools::arguments(&call.arguments)?;
                let id = notes.add(note, log.step)?;
                log.notes_added.push(id);
                Ok(format!("note {id} added"))
            }
            "note_drop" => {
                let args: DropNote = tools::arguments(&call.arguments)?;
                notes.drop(args.id)?;
                log.notes_dropped.push(args.id);
                Ok(format!("note {} dropped", args.id))
            }
            "finish" => {
                let args: Finish = tools::arguments(&call.arguments)?;
                validate_report(&self.task.report_schema, &args.report)
                    .map_err(|e| format!("invalid report {e}"))?;
                *report = Some(args.report);
                Ok("finished".into())
            }
            _ => Err(format!("unknown tool: {}", call.name)),
        }
    }

    fn persist(&self, notes: &Notes, file: &mut File, log: &mut StepLog) -> Result<(), Failure> {
        if let Err(reason) = notes.write(&self.out) {
            log.stop = Some("io_failed");
            write_log(file, log).map_err(|e| Failure::io(format!("{reason}; steps.jsonl: {e}")))?;
            return Err(Failure::io(reason));
        }
        write_log(file, log).map_err(|e| Failure::io(format!("steps.jsonl: {e}")))
    }
}

fn write_log(file: &mut File, log: &StepLog) -> std::io::Result<()> {
    let mut bytes = serde_json::to_vec(log)?;
    bytes.push(b'\n');
    file.write_all(&bytes)?;
    file.flush()
}
