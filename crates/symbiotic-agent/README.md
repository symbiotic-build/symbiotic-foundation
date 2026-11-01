# Symbiotic Agent

`symbiotic-agent` runs one job as a sequence of bounded steps. A step is one
model request. The runtime rebuilds every request from records; it never sends
the conversation so far. The model works through six tools on a read-only
directory, keeps what it needs in a notes record, and ends the job with a report.

It is built to run inside Warden's sandbox as the CLI family `symbiotic`. It
reaches the model only through the URL it is given, which in the sandbox is the
per-job proxy in front of the gateway. It adds no retry, records no usage and
keeps no state between runs: the gateway retries and meters every request, and
Warden's rerun path starts the next attempt.

## Command line

```sh
symbiotic-agent run --task <task.json> --policy <policy.json> --root <dir> --model-url <url> --out <dir>
symbiotic-agent --version
```

- `--task`: the task file (below).
- `--policy`: the context policy file (below).
- `--root`: the directory the tools read. Tool paths are relative to it and may not leave it.
- `--model-url`: the full Responses URL, for example `http://10.0.0.1:80/v1/responses`. The client sends plain HTTP/1.1 POST requests to it and ignores proxy environment variables.
- `--out`: an existing, writable directory for the step log and the notes record.
- `--version` prints `symbiotic-agent <version>` and exits 0.

## Task file

```json
{
  "instructions": "role text and the output contract text",
  "task": {"unit": {"key": "unit-a"}},
  "report_schema": {"type": "object", "properties": {}, "required": [], "additionalProperties": false}
}
```

- `instructions` (string, required): sent as the request's `instructions` field in every step.
- `task` (any JSON value, required): sent unchanged, as compact JSON, in the first user message of every step. Warden puts the auditor task here.
- `report_schema` (JSON Schema object, required): the schema of the `finish` tool's `report` argument. The runtime checks reports against it (see Stop rules).

Unknown fields are refused. The runtime supports this JSON Schema subset in `report_schema`: `type` (a name or an array of names among `object`, `array`, `string`, `integer`, `number`, `boolean`, `null`), `properties`, `required`, `additionalProperties` (boolean), `items`, `enum`, `minItems`, `maxItems`, `minLength` and `description`. A schema with any other keyword is refused at start, so a report is never accepted against a check the runtime does not run.

## Policy file

```json
{
  "model": "gpt-6.1-sol",
  "reasoning_effort": "high",
  "max_output_tokens": 16000,
  "max_steps": 40,
  "read_max_lines": 200,
  "reads_per_step": 8,
  "tool_output_max_chars": 20000,
  "search_max_hits": 50,
  "keep_results_steps": 1,
  "outline": null
}
```

The values above are examples. The token research session sets the real values; Warden writes the file from its audit configuration.

- `model` (string): the provider model name.
- `reasoning_effort` (string or null): sent as `reasoning.effort`; null leaves the provider default.
- `max_output_tokens` (positive integer or null): sent as `max_output_tokens`; null leaves the provider default.
- `max_steps` (integer, at least 2): the most requests the job makes, the final finish-only step included.
- `read_max_lines` (positive integer): the most lines one `read` call returns.
- `reads_per_step` (positive integer): the most `read`, `search` and `list` calls run in one step. Later calls of those tools in the same answer get an error result.
- `tool_output_max_chars` (positive integer): the most characters of one tool result shown to the model. A longer result is cut and ends with `[cut: <shown> of <total> characters]`.
- `search_max_hits` (positive integer): the most hits one `search` call returns.
- `keep_results_steps` (positive integer, default 1): how many steps a tool result stays in the request. A result from step `n` is shown in steps `n+1` to `n+keep_results_steps`.
- `outline` (string or null): a path to a text file, relative to the working directory, read once at start and sent after the task in the first user message.

Unknown fields are refused.

## One step

Every request has the same two user messages:

1. **Prefix**, byte-identical in every step of a job: the task JSON, then the outline when the policy names one. With `instructions` and the tool definitions, it is the part the provider's prompt cache serves.
2. **Tail**, rebuilt each step: the step number and the steps left, the notes record, and the kept tool results with their step numbers, each with its call's tool name and arguments.

Each request sends one `session-id` header, a random UUID fixed for the job, so the provider routes all steps of the job to one prompt cache.

The model answers with tool calls, several in one answer. The runtime runs them in order and builds the next step. An answer with no tool call is an error result in the next step.

### Tail text layout

The tail uses LF newlines and this exact layout (angle-bracket values are replaced):

```text
Step <n> of <max_steps>; <left> steps left after this one.
This is the last step. You must call finish.
Notes
<compact notes JSON>

Result step <n> tool <JSON string of tool name> arguments <JSON string of original arguments>
<result text>

Error step <n> tool <JSON string of tool name> arguments <JSON string of original arguments>
error: <reason>
```

The last-step line appears only on the final step. Each kept successful result
gets a `Result` block in call order; kept errors follow in call order with `Error`
blocks. Each block starts and ends with one added LF; result text stays verbatim.
Names and argument text are JSON strings, so each header occupies one line.
An answer with no tool call uses name `none` and argument text `{}`. Results and errors expire after `keep_results_steps`.

## Tools

Tool schemas are strict except `finish`, whose Warden-supplied report schema may exceed the provider's strict subset and is checked by the runtime. Optional arguments are nullable.

- `read {path, start, end}`: lines `start` to `end` of one file, inclusive, numbered. At most `read_max_lines` lines; a longer range is cut and says so.
- `search {pattern, path, max_hits}`: a regular-expression search over the files under `path` (null: the whole root). Hits are `path:line: text`, at most `min(max_hits, search_max_hits)`. Files with a NUL byte in their first 8 KiB, non-UTF-8 files, special files (including sockets, FIFOs and dangling links), unreadable files and directories, and `.git` directories are skipped. Missing or out-of-root starting paths remain errors.
- `list {path}`: the entries of one directory (null: the root), sorted, directories ending in `/`.
- `note {kind, text, refs}`: adds one notes item and returns its id. `kind` is `covered`, `finding`, `question` or `plan`. `refs` is a list of `{path, ranges}` with inclusive `[start, end]` line ranges.
- `note_drop {id}`: removes one notes item.
- `finish {report}`: ends the job with a report.

Paths are relative, slash-separated, and may not contain `..` or leave `--root`, including through symbolic links.

## Stop rules

- **Finish:** a `finish` call whose `report` is valid against `report_schema` ends the job: the runtime prints the report and exits 0. Calls after it in the same answer are not run. An invalid report is an error result in the next step, with the first failing JSON path.
- **Step budget:** step `max_steps` offers only the `finish` tool, with `tool_choice: required`. If its report is invalid, or it calls no `finish`, the runtime exits 3.
- **Model call failure:** a non-200 answer, a broken stream or a provider error event ends the job at once with exit 2. The runtime does not retry.

## Output

- **stdout**, on exit 0 only: the final message Warden's parser reads, one JSON fence holding the report:

  ````text
  ```json
  {"unit":"unit-a", ...}
  ```
  ````

- **stderr:** one line per failure, naming the exit reason.
- **`<out>/steps.jsonl`:** one JSON line per step, written when the step ends.
- **`<out>/notes.json`:** the notes record, rewritten atomically after every step.

### Step log line

```json
{"step":3,"steps_left":37,"final":false,"response_id":"resp_...","calls":[{"call_id":"call_...","name":"read","arguments":"{\"path\":\"src/lib.rs\",\"start\":1,\"end\":200}","outcome":"ok","result_chars":8123,"shown_chars":8123}],"notes_added":[4],"notes_dropped":[],"stop":null}
```

- `step`: 1-based. The job makes exactly one request per step and no other request, so step `n` is the job's `n`-th model request: the gateway's request `<job_id>-<n>` when the proxy numbers requests from 1.
- `steps_left`: steps remaining after this one.
- `final`: true for the finish-only step.
- `response_id`: the provider response id, or null when the answer failed before one arrived.
- `calls[].outcome`: `ok`, `error` or `skipped` (after a valid `finish`). An error adds `"error": "<reason>"`.
- `result_chars` and `shown_chars`: the result's length before and after `tool_output_max_chars`.
- `stop`: null while the job continues; else `finished`, `budget_spent`, `model_failed` or `io_failed`.

### Notes record

```json
{"next_id":5,"items":[{"id":4,"step":3,"kind":"covered","text":"argument parsing","refs":[{"path":"src/lib.rs","ranges":[[1,200]]}]}]}
```

## Exit codes

- `0`: finished with a valid report on stdout.
- `1`: refused to start: invalid arguments, task file, policy file or `report_schema`. No request was sent.
- `2`: a model call failed.
- `3`: the step budget was spent without a valid report.
- `4`: the step log or notes record could not be written.

## Build and test

Use `cargo check --release --locked -p symbiotic-agent --all-targets` to compile.
Run the fake-server integration tests with
`cargo test --release --locked -p symbiotic-agent --test runtime`.
Run schema and cap unit tests with
`cargo test --release --locked -p symbiotic-agent --bins`.
