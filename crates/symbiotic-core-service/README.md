# symbiotic-core-service

The core service stores jobs, model request records, provider budgets and operator decisions in one SQLite database. Each state-changing write commits its records and append-only events together. It tracks work reported by products; it does not execute jobs or make model calls. The library exposes `Store::open`, `Store::open_version`, `migrate` and `dispatch` for in-process use. The binary `symbiotic-core` serves the owning user over a Unix socket.

## State folder

- `core.sqlite`: canonical records and events; SQLite may also create `core.sqlite-wal` and `core.sqlite-shm`.
- `core.sock`: owning-user socket. The server runs with umask 077, so the socket, store and backups give group and others no access.
- `backups/core-YYYYMMDDTHHMMSSZ.sqlite`: a backup at server start and hourly thereafter; the newest 48 matching files remain.

The state folder has mode 0700. The store uses a 60 s busy timeout, WAL, `synchronous=FULL` and `fullfsync=ON`. Numbered [migrations](migrations/0001_initial.sql) run in one IMMEDIATE transaction; [read indexes](migrations/0002_read_indexes.sql) are version 2, and [job heartbeats](migrations/0003_job_heartbeat.sql) are version 3. `PRAGMA user_version` records the version. Newer files fail with `store_version_newer`. Updates and deletes on events fail with `events are append-only`.

The bare SQLite operations are inserts and updates in one transaction per write, indexed record lookups and ordered event reads. The service layer adds field validation, state rules and JSON responses. It keeps no extra cache.

The tables are `events`, `jobs`, `job_decisions`, `decisions`, `budgets` and `requests`. They are required by the core service record contract. The event payload carries the data needed to rebuild each record. Job `heartbeat_at`, like derived `cost`, is outside the canonical job record. Heartbeats append no event, and a store rebuilt from events has null heartbeats. A heartbeat each minute would otherwise add 1 440 event rows per job per day.

The seven read indexes cover `events(subject_kind, subject_id)`, `requests(job_id)`, `requests(account, time)`, `jobs(created)`, `budgets(account)`, `decisions(budget_id, state)` and `requests(time)`. Each index adds one B-tree entry per row and maintenance on indexed writes. The account and decision indexes avoid unrelated rows on every request; the time indexes select the report day in SQLite. `job.list` walks the primary key in ascending id order and applies its filters without a new index. An index is added when a measured listing needs one. Listing reads stored job fields without reading requests or deriving cost.

Job cost is derived from requests; there is no stored cost or charged column. Budgets store their account once; window fields stay inside the stored reading (`meter`), without separate `owner` or `window` columns.

## Naming convention

JSON fields use lower snake case. Calls and event kinds are dotted, subject first (`job.create`, `budget.warning`). Error reasons use lower snake case, subject first (`job_not_found`). States use lower snake case. Embedded `DecisionEnvelope`, `Answer` and `ActorRef` preserve the existing lower camel case contract fields (`sourceEvents`, `optionId`, `displayName`). Rust names follow the language conventions.

## Wire format

The core listens on the Unix socket `<state>/core.sock`. It accepts connections only from the user that owns the process.
Each request is one JSON object on one line; each response is one JSON object on one line, in request order.

```json
{"call": "request.record", "args": { ... }}
```

A success response holds `ok`; a failure holds `error`:

```json
{"ok": { ... }}
{"error": {"reason": "request_id_conflict", "message": "request audit-ab12-7 is already recorded with a different body"}}
```

Naming: JSON fields are lower snake case. Calls and event kinds are dotted, subject first (`request.record`, `budget.warning`). Error reasons are lower snake case, subject first (`request_id_conflict`, `args_invalid`, `call_unknown`).
Times are RFC 3339 UTC strings, for example `2026-11-02T10:15:30.123Z`.

## `request.record`

The gateway sends one per model request, from its outbox, after the response ends or after a local refusal.

```json
{"call": "request.record", "args": {
  "request_id": "audit-20261102-ab12-7",
  "job_id": "audit-20261102-ab12",
  "account": "codex-gmail",
  "route": "codex",
  "model": "gpt-5.5",
  "status": 200,
  "time": "2026-11-02T10:15:30.123Z",
  "usage": {
    "normalized": {
      "input_tokens": 41250,
      "cached_input_tokens": 38912,
      "output_tokens": 1830,
      "reasoning_tokens": 1216
    },
    "provider": {
      "input_tokens": 41250,
      "input_tokens_details": {"cached_tokens": 38912},
      "output_tokens": 1830,
      "output_tokens_details": {"reasoning_tokens": 1216},
      "total_tokens": 43080
    }
  },
  "meter": {
    "readings": [
      {"name": "codex.secondary", "unit": "percent", "used": 91.0, "limit": 100.0,
       "window_minutes": 10080, "resets_at": "2026-11-04T06:51:00Z"},
      {"name": "codex.primary", "unit": "percent", "used": 12.0,
       "window_minutes": 300, "resets_at": "2026-11-02T13:00:00Z"},
      {"name": "codex.credits", "unit": "credits", "remaining": 9809.4}
    ],
    "provider": {
      "x-codex-secondary-used-percent": "91",
      "x-codex-secondary-window-minutes": "10080",
      "x-codex-primary-used-percent": "12",
      "x-codex-credits-balance": "9809.40"
    }
  }
}}
```

Fields:

- **One record per upstream attempt:** a caller request that the gateway retries, continues or falls back to another account sends one record per attempt, each with its own usage, meter, account and route. Job cost is the sum of all attempts.
- `request_id` (string, required): on the owner socket, attempt 1 carries the caller's valid `x-symbiotic-request-id`, which for Warden is `<model_job_id>-<n>`.
  An absent or invalid header leaves the generated `local-<uuid>` ID.
  On the [forwarder socket](../symbiotic-gateway/README.md#forwarder-socket), the gateway ignores that header and generates `<run identity>-<32 hexadecimal characters>` from the connection's first line.
  On both sockets, attempt n ≥ 2 carries `<request ID of attempt 1>.<n>`. The core is idempotent on it.
- `job_id` (string, required): the same for every attempt.
  On the owner socket, it is the request ID of attempt 1 before the last `-`, or the whole ID if no `-` occurs.
  For Warden, this is the caller's job id. On the forwarder socket, it is the run identity.
  The gateway sends it; the core does not parse ids. A request whose job is not in the core yet is still stored; reconcile reports it if the job never arrives.
- `account`, `route`, `model` (strings, required).
- `status` (integer, required): that attempt's upstream HTTP status, 502 for a network failure.
  A local refusal or local error after route selection, with no upstream call, is one record with that socket's attempt-1 request ID and the status returned to the caller.
  Connections rejected before route selection write no record, including forwarder connections with missing, overlong or malformed first lines.
- `account` names: the route's sharing key as `shared:<key>` when set, else `<tenant>/<account>` (deployed Warden routes: `warden/codex` and so on). Budgets and the quota gate use the same names; the core never splits an account, budget or decision id.
- `refused` (string, optional): present only for a local refusal with no upstream call. Step 1 value: `quota_exhausted`, with `status` 402 and `usage` null.
- `time` (string, required): when the response ended or the refusal happened. The request row normalizes it to UTC with millisecond precision and `Z` for text range queries. The `model.request` event preserves the args exactly as sent.
- `usage` (object or null, required): null when the provider returned no usage (error, cut stream, refusal). Reconcile lists null usage without `refused` in the failing `requests.without_usage` list. It lists null usage with `refused` in `requests.refused`, which never fails the report.
  - `normalized`: any of `input_tokens`, `cached_input_tokens`, `cache_write_tokens`, `output_tokens`, `reasoning_tokens`, as non-negative integers. A field the provider does not give is absent, never 0.
    `input_tokens` counts all input, cached and cache-write included; `cached_input_tokens` and `cache_write_tokens` are parts of it. `output_tokens` includes `reasoning_tokens`. The gateway converts each provider's fields into this meaning (Anthropic's `input_tokens` excludes cache reads, so the gateway adds them).
  - `provider`: the provider's usage object as it arrived, every field kept. The core stores it unread.
- `meter` (object, required; empty lists and objects allowed):
  - `readings`: the gateway's numeric reading of each quota or balance on this response, or the latest balance poll.
    - `name` (string, required): stable per account, for example `codex.secondary`, `codex.credits`, `deepseek.balance`.
    - `unit` (string, required): the provider's unit, for example `percent`, `credits`, `CNY`, `USD`.
    - `used`, `remaining`, `limit` (numbers, each optional).
    - `window_minutes` (integer, optional), `resets_at` (time, optional).
    - `read_at` (time, optional): when the reading was taken, for a polled balance. Default: the record's `time`.
  - `provider`: every quota or balance header on the response, name to value, as strings.

## How the core charges budgets

- **A reading with a `limit` is a budget.** The core keeps one budget per account and reading name, with id `<account>/<name>`, created on its first reading. A reading without a `limit` is stored with the request but is no budget. The gateway decides which readings carry a limit: for Codex the weekly window (100), for DeepSeek the configured allowance.
- **Spent** is `used` when present, else `limit - remaining`.
- **States:** `open` below 90 % of the limit; `warning` from 90 %; `exhausted` from 100 %.
- **Decisions:** entering `warning` opens one decision (continue to 100 %, pause new jobs on this account, or move them to another route). Entering `exhausted` opens a second decision: allow spending past the limit (on Codex, credits) or keep refusing. Answering it with `allow_over_limit` sets `over_limit_allowed`.
- **Window reset:** a reading below 90 % moves the budget back to `open`, withdraws its open decisions and clears `over_limit_allowed`.
- **Order:** a reading older than the budget's last reading (by `read_at`) is stored with its request but does not move the budget, so an outbox draining old records cannot roll the meter back.
- **Job cost** is the per-field sum of `usage.normalized` over the job's requests.

## Response of `request.record`

```json
{"ok": {
  "duplicate": false,
  "seq": 1042,
  "budgets": [
    {"id": "codex-gmail/codex.secondary", "account": "codex-gmail", "name": "codex.secondary",
     "unit": "percent", "limit": 100.0, "spent": 91.0, "warn_at": 0.9, "state": "warning",
     "over_limit_allowed": false, "open_decision_ids": ["budget-codex-gmail-codex.secondary-warning-1"],
     "reading": {"name": "codex.secondary", "unit": "percent", "used": 91.0, "limit": 100.0,
                 "window_minutes": 10080, "resets_at": "2026-11-04T06:51:00Z",
                 "read_at": "2026-11-02T10:15:30.123Z"},
     "updated": "2026-11-02T10:15:30.123Z"}
  ]
}}
```

- `budgets` lists every budget of the request's account after this record, so the gateway can update its quota gate without a second call.
- `seq` is the `model.request` event's sequence number.
- A repeat with the same `request_id` and the same body returns `"duplicate": true` with the original `seq`. A repeat with a different body fails with `request_id_conflict`; the gateway should log it and drop that outbox row.

## `budget.get`

```json
{"call": "budget.get", "args": {"account": "codex-gmail"}}
{"ok": {"budgets": [ ...same budget objects as above... ]}}
```

An account with no budget returns an empty list: the account has no quota meter. The gateway refuses a Codex request when the weekly budget's `state` is `exhausted` and `over_limit_allowed` is false.

## Other calls

Unknown calls fail with `call_unknown`. Invalid JSON or args fail with `args_invalid`; the message names the field. Each mutating call uses one IMMEDIATE transaction, including all its events. Event ids are UUID v4; event times use UTC with millisecond precision and `Z`. Events are ordered by `seq`.

### `job.create`

Args: `{id, kind, product, inputs?: [string], rule_revision?: string, model_route?: string, budget_id?: string, waiting_for?: [decision_id]}`.

Warden jobs use the exact, case-sensitive `product` value `"warden"`. Other product names remain unrestricted.

Response: `{duplicate, seq, job}`. `job` holds `id`, `kind`, `product`, `inputs`, `rule_revision`, `model_route`, `budget_id`, `state`, `waiting_for`, `result_ref`, `created`, `updated`, `heartbeat_at` and `cost`. Absent optional strings are null; absent arrays are empty. `cost` holds per-field sums of `usage.normalized`, `requests` and `requests_without_usage`. Missing usage fields stay absent.

Creation is idempotent on `id` and the create args. A different body fails with `job_id_conflict`. Each named decision must exist (`decision_not_found`). The initial state is `waiting` if any named decision is not `answered`; otherwise it is `ready`. Creation writes `job_decisions` and `job.created`. A duplicate returns the original sequence and the current job without another event.

### `job.transition`

Args: `{id, to, result_ref?: string}`. `to` is `ready`, `running`, `done`, `failed` or `cancelled`.

Response: `{duplicate, seq, job}`. A transition to the current nonterminal state returns `{duplicate: true, seq: null, job}` and appends no event.

- A missing job fails with `job_not_found`. A terminal job (`done`, `failed`, `cancelled`) fails with `job_state_terminal`.
- `ready` requires `waiting`. `running` requires `waiting` or `ready`. Both fail with `job_waiting_for_decision` if a named decision is not answered.
- `done` and `failed` require `running`; other invalid transitions fail with `job_transition_invalid`. `cancelled` accepts every nonterminal state.
- Events are `job.ready`, `job.started`, `job.finished`, `job.failed` and `job.cancelled`, respectively.

### `job.heartbeat`

Args: `{id}`. Response: `{id, heartbeat_at}`.

A running job stores the service's current UTC time in `heartbeat_at`. The call changes no other field and appends no event. It does not derive cost or read requests. A new job has null `heartbeat_at`; an ended job keeps its last value.

A missing job fails with `job_not_found`. A terminal job (`done`, `failed`, `cancelled`) fails with `job_state_terminal`. A `waiting` or `ready` job fails with `job_not_running`. Invalid args fail with `args_invalid`.

A stale heartbeat changes nothing by itself. The service does not fail, reclaim or restart a job. Heartbeats provide liveness reporting without claims, leases, owners, attempts or retries.

### `job.list`

Args: `{product?: string, state?: string, after?: string, limit?: integer}`.

`product` matches exactly. `state` is `waiting`, `ready`, `running`, `done`, `failed` or `cancelled`. Supplied filters combine with AND. `after` is an exclusive job-id cursor. `limit` defaults to 1 000 and accepts 0 through 10 000. Invalid filters, cursors or limits fail with `args_invalid`.

Response: `{jobs: [...], next_after}`. Jobs follow ascending `id`. Each job holds `id`, `kind`, `product`, `inputs`, `rule_revision`, `model_route`, `budget_id`, `state`, `waiting_for`, `result_ref`, `created`, `updated` and `heartbeat_at`. The response excludes derived `cost` and appends no event.

The query fetches `limit + 1` matching rows. `next_after` is the last returned id when another matching row exists, otherwise null. With `limit` 0, the result is `{jobs: [], next_after: null}` because no id was returned.

### `decision.answer`

Args: `{decision_id, revision, answer, answered_by}`. `answer` is the existing kind-tagged `Answer`; `answered_by` is an `ActorRef`.

```json
{"decision_id":"budget-account-weekly-exhausted-1","revision":1,"answer":{"kind":"option","optionId":"keep_refusing"},"answered_by":{"id":"operator","kind":"human","displayName":"Operator"}}
```

Response: `{seq, decision}`. `decision` contains `id`, `state`, `envelope`, `answer`, `answered_by`, `answered_at`, `budget_id`.

Errors: `decision_not_found`, `decision_not_open`, `decision_revision_stale`, `answer_option_unknown`. An answer other than `option` or `more_context` fails with `args_invalid`. An option answer updates the record and envelope to `answered`, stores an `AnswerRecord` on the question, and appends `decision.answered`. `more_context` records the answer and event but leaves the decision `open`.

Core decisions have revision 1 and one authority question with option answers and severity `decision`. The audience is the human `operator`; `wakes` is empty. `sourceEvents` names the threshold event. The resolution trail has no attempts; its escalation reason is "a budget rule raises this decision directly". Fact-labelled context states account, meter, spent, limit, unit, read time and known reset time. Decision ids are `budget-<account>-<name>-warning-<n>` or `budget-<account>-<name>-exhausted-<n>`, counted from 1 per budget and kind.

Warning options are `continue_to_limit` (recommended), `pause_new_jobs` and `move_to_other_route`; each is reversible. An answer changes only the decision record. Exhaustion options are `allow_over_limit` and `keep_refusing` (recommended). `allow_over_limit` sets the budget allowance only while it remains exhausted. On Codex it permits spending credits, a protected asset. A reset clears this allowance and withdraws open decisions with `decision.withdrawn`; answered decisions retain their answer history. Decision states are exactly `open`, `answered`, `withdrawn`.

A reading needs a limit and either used or remaining to create a budget. A jump directly to 100 % emits the warning and opens its decision before the exhaustion event and decision. A fall from exhausted to between 90 % and 100 % returns to warning without a new decision. Returning to exhaustion reuses any still-open exhaustion decision; each budget has at most one open decision of each kind. Equal read times apply in arrival order; older readings do not change budget state.

### `events.since`

Args: `{after?: integer, limit?: integer}`. Defaults: `after` 0, `limit` 1000. `after` must be non-negative; `limit` is 0 through 10000.

Response: `{events: [{seq, id, time, subject_kind, subject_id, kind, payload}], last_seq}`. Events follow ascending `seq`. `last_seq` is the last returned sequence, or `after` when the result is empty.

### `reconcile`

Args: `{day: "YYYY-MM-DD", warden_state: "directory"}`. The day is a UTC date.

Response example:

```json
{"day":"2026-11-02","passed":false,"jobs":{"in_core":3,"in_warden":4,"missing_from_core":["missing-job"],"extra_in_core":[]},"requests":{"total":120,"without_usage":[{"request_id":"request-1","account":"account","route":"route","status":500}],"refused":[{"request_id":"request-4","account":"account","route":"route","status":402,"refused":"quota_exhausted"}],"unknown_job":["request-2"]},"accounts_without_quota_meter":["kimi-1"],"quota":{"violations":[{"budget_id":"account/weekly","request_id":"request-3","problem":"request sent while exhausted without an answered allow_over_limit decision"}]},"warden_errors":[]}
```

The core selects jobs and requests with inclusive UTC midnight and exclusive next-midnight bounds. The job comparison selects only jobs with `product = "warden"`. Request and quota checks cover all products. It checks unknown job ids with a `LEFT JOIN` and reads only whether request usage is null.

The three checks are:

- Compare the day's core Warden jobs (`created` UTC date, exact `product = "warden"`) with Warden jobs. Other products' jobs never enter `jobs.in_core` or `jobs.extra_in_core`, even when their ids match a manifest. Walk the Warden directory recursively without following symlinks. Every regular `sandbox.json` supplies string `job_directory` and `retirement_receipt`; its modification date selects the day. Missing or extra Warden jobs fail.
- Inspect the day's requests (`time` UTC date) for every product. Null usage without `refused` and job ids absent from core fail. A request's job is known when its id exists for any product. Null usage with `refused` appears in `requests.refused` with request id, account, route, status and refusal reason, without failing. Accounts with requests but no budget appear in `accounts_without_quota_meter` and do not fail.
- Replay all relevant events in `seq` order without an arrival-time cutoff, streaming rows from SQLite. Judge each `model.request` whose response `time` (the normalized request row, joined in SQL, so no request payload is decoded) falls on the report day against the budget state at that replay position, including outbox deliveries on later days. A sent request while any account budget is exhausted without an answered `allow_over_limit` in force fails. A return to `open` clears that permission. An exhaustion event arriving on the report day without a warning since the last return to open also fails.

The Warden job id uses the first 10 bytes of the directory's file name, replacing bytes outside ASCII alphanumeric, `.`, `_`, `-` with `_`. Append `-` and the first 32 lowercase hex characters of SHA-256 over directory bytes, a zero byte and retirement receipt bytes. Directory and file errors, invalid JSON and missing fields appear in `warden_errors` with path and reason. Every such error fails reconciliation. `passed` requires all three checks and no Warden errors.

## CLI

```sh
symbiotic-core --help
symbiotic-core --state /state serve
symbiotic-core --state /state job.create '{"id":"job","kind":"review","product":"warden"}'
symbiotic-core --state /state request.record -
symbiotic-core --state /state budget.get '{"account":"codex-gmail"}'
symbiotic-core --state /state decision.answer '<JSON args>'
symbiotic-core --state /state job.transition '{"id":"job","to":"running"}'
symbiotic-core --state /state job.heartbeat '{"id":"job"}'
symbiotic-core --state /state job.list '{"product":"warden","state":"running","limit":100}'
symbiotic-core --state /state events.since '{"after":0}'
symbiotic-core --state /state reconcile --day 2026-11-02 --warden-state /warden-state
```

`--state DIR` precedes the subcommand. Otherwise `SYMBIOTIC_CORE_STATE` supplies the directory. Missing state or invalid usage exits 2. Call subcommands take one JSON argument or `-` to read stdin. Success prints the `ok` value on one stdout line and exits 0. A service error prints its error object on stderr and exits 1. An unreachable socket exits 2. Reconcile prints pretty JSON; it exits 0 for `passed`, 1 for failed checks and 2 for an error.

The server removes a stale socket only after a connection is refused. It authenticates the peer uid before reading a line. All calls use one writer connection behind a mutex on `spawn_blocking`. A separate connection runs backups. Backup failures go to stderr and retry at the next hour; they do not stop serving. Retention removes only regular files with the exact dated backup name pattern.

## Threat model

The socket admits only peers with the server's effective uid. The server checks the owner before reading any line. Callers are the same user's gateway and Warden. The state folder, socket, store and backups give group and others no access. No untrusted input reaches the protocol under this owning-user trust model.

Coverage-guided fuzzing (House Rules test-discipline, "code that parses untrusted input or exposes a network protocol") is not set up because no untrusted input reaches this protocol. The seeded malformed-input test checks that `dispatch` never panics. If the socket is ever exposed beyond the owning user, for example HTTP in step 4, a fuzz target becomes required.

## Guarantees and their tests

Each bullet names the failure prevented and the test function in its linked file.

- Events reject updates and deletes, preventing rewritten history: [tests/store.rs](tests/store.rs), `version_one_job_survives_migration_and_events_remain_append_only`.
- Records and events commit together, preventing partial requests or budget changes after a failed write: [tests/requests.rs](tests/requests.rs), `invalid_derived_reading_rolls_back_request_and_prior_readings`.
- Idempotent `job.create` prevents duplicate jobs and rejects changed create bodies: [tests/jobs.rs](tests/jobs.rs), `create_is_idempotent_after_transitions_and_conflicts_on_changed_body`.
- Heartbeats store service time without changing other job fields or event history, preventing liveness reports from changing state or growing the event log: [tests/jobs.rs](tests/jobs.rs), `running_job_heartbeat_stores_the_service_time_and_appends_no_event`.
- Missing, non-running and terminal jobs reject heartbeats without writes, preventing invalid liveness updates: [tests/jobs.rs](tests/jobs.rs), `heartbeat_rejects_missing_waiting_ready_and_final_jobs_without_writes`.
- Job events exclude heartbeats even after a transition, preventing liveness from entering canonical history: [tests/jobs.rs](tests/jobs.rs), `job_events_never_carry_the_heartbeat`. Ended jobs retain their last heartbeat.
- A create retry after a heartbeat preserves the original sequence, preventing duplicate creation history: [tests/jobs.rs](tests/jobs.rs), `create_retry_after_heartbeat_returns_the_original_sequence`.
- Job listing combines exact product and state filters, preventing unrelated jobs from entering results: [tests/jobs.rs](tests/jobs.rs), `job_list_filters_by_state_and_product_alone_and_together`.
- Dispatched cursor listings return complete expected pages with and without product and state filters. On 1 000 matching jobs, a page after job 900 uses at most twice the first page's SQLite virtual-machine steps, preventing repeated scans of earlier jobs: [src/calls.rs](src/calls.rs), `job_list_seeks_to_the_cursor_instead_of_scanning_earlier_jobs`.
- Job listing uses exclusive id cursors and matching-row continuation, preventing repeated rows and false continuation: [tests/jobs.rs](tests/jobs.rs), `job_list_pages_by_id_and_reports_continuation`.
- Job listing returns stored fields and heartbeats without cost or new events, preventing request-history work and listing writes: [tests/jobs.rs](tests/jobs.rs), `job_list_returns_stored_fields_and_heartbeat_without_cost`.
- Invalid listing filters, cursors and limits fail without writes, preventing malformed queries from being accepted: [tests/jobs.rs](tests/jobs.rs), `job_list_rejects_invalid_filters_cursors_and_limits`.
- Migration from version 2 preserves jobs and events with null heartbeats, preventing upgrade data loss: [tests/store.rs](tests/store.rs), `version_two_records_survive_the_heartbeat_migration`. Creation, heartbeats and reopening work after upgrade.
- Both calls work through the real socket and CLI, preventing a dispatch-only implementation: [tests/cli.rs](tests/cli.rs), `cli_records_a_heartbeat_and_lists_running_jobs`.
- Idempotent `request.record` prevents duplicate charges and rejects changed request bodies: [tests/requests.rs](tests/requests.rs), `request_retry_uses_original_sequence_and_rejects_changed_body`.
- A job naming an unanswered decision starts `waiting` and cannot run, preventing unauthorized work: [tests/jobs.rs](tests/jobs.rs), `unanswered_decision_blocks_running_until_answered`.
- The 90 % and 100 % thresholds raise their decisions, preventing missed budget choices: [tests/budgets.rs](tests/budgets.rs), `thresholds_open_one_decision_each_and_reset_withdraws_open_decisions`.
- A direct jump to 100 % raises warning before exhaustion, preventing an exhaustion without warning: [tests/budgets.rs](tests/budgets.rs), `jump_to_limit_opens_warning_before_exhaustion_and_reset_withdraws_both_open_decisions`.
- Reset withdraws open decisions and clears the allowance, preventing stale permission in a new window: [tests/budgets.rs](tests/budgets.rs), `thresholds_open_one_decision_each_and_reset_withdraws_open_decisions`.
- An older reading never moves a budget, preventing an outbox from rolling it back: [tests/budgets.rs](tests/budgets.rs), `old_reading_changes_nothing_and_reading_without_limit_is_not_a_budget`.
- Repeated exhaustion reuses an open exhaustion decision, preventing duplicate questions: [tests/budgets.rs](tests/budgets.rs), `exhaustion_after_warning_reuses_unanswered_exhaustion_decision`.
- Each budget's `spent`, `limit` and `state` equal its last `budget.reading` event. The allowance (`over_limit_allowed`) is not in that event: rebuilding it also needs the `decision.answered` events of its exhaustion decisions and the readings that return the budget to `open`, and no test checks that rebuild. Sequence checks prevent record/event divergence, duplicate open decisions and jobs proceeding with unanswered decisions: [tests/sequences.rs](tests/sequences.rs), `generated_operations_preserve_budget_event_and_job_invariants` (32 fixed seeds, 80 operations each; `SEQUENCE_SEED` replays one seed).
- Generated heartbeats change only running jobs' liveness and never enter events. Generated listings return every stored job once in id order without cost. Last accepted heartbeats, unchanged event counts and complete pages are checked after each operation and after reopening: [tests/sequences.rs](tests/sequences.rs), `generated_operations_preserve_budget_event_and_job_invariants`.
- Migration from version 1 preserves jobs and append-only history, preventing data loss during upgrade: [tests/store.rs](tests/store.rs), `version_one_job_survives_migration_and_events_remain_append_only`.
- Owner-only socket permissions prevent group and other access: [tests/cli.rs](tests/cli.rs), `standalone_cli_serves_calls_reconciles_and_creates_private_socket_and_backup`. Rejection of a different peer uid has no automated test.
- Reconcile compares only core jobs with exact `product = "warden"` against Warden manifests, preventing unrelated products from causing false failures or satisfying missing Warden jobs: [tests/reconcile.rs](tests/reconcile.rs), `warden_job_comparison_ignores_other_products`. Missing or extra Warden jobs fail in `missing_extra_without_usage_unknown_job_and_exhausted_requests_are_listed`; `matching_warden_jobs_pass_and_model_job_id_matches_fixed_digest` checks agreement and the Warden id.
- Reconcile checks usage, refusals, unmetered accounts and quota violations for all products, preventing non-Warden requests from bypassing checks: [tests/reconcile.rs](tests/reconcile.rs), `known_non_warden_requests_still_receive_usage_and_quota_checks`.
- Reconcile recognizes a request's job id from any product, preventing late job creation from leaving a false unknown-job failure: [tests/reconcile.rs](tests/reconcile.rs), `request_stops_being_unknown_when_any_product_creates_its_job`.
- Reconcile flags null usage without refusal and unknown job ids, preventing unaudited requests from passing: [tests/reconcile.rs](tests/reconcile.rs), `missing_extra_without_usage_unknown_job_and_exhausted_requests_are_listed`.
- Reconcile lists refusals and unmetered accounts without failing, preventing expected refusals or absent meters from being mistaken for missing usage: [tests/reconcile.rs](tests/reconcile.rs), `account_without_budget_is_reported_without_failure_and_local_refusal_is_not_missing_usage`.
- Reconcile judges requests against exhausted budgets and replays allowance/reset, preventing unauthorized spending from passing: [tests/reconcile.rs](tests/reconcile.rs), `allowance_is_replayed_and_reset_clears_it`.
- Reconcile judges late deliveries on their response day, preventing an outbox delay from bypassing checks: [tests/reconcile.rs](tests/reconcile.rs), `yesterday_response_delivered_after_exhaustion_today_fails_yesterday_report`.
- Reconcile flags exhaustion without warning on its arrival day, preventing incomplete threshold history from passing: [tests/reconcile.rs](tests/reconcile.rs), `exhaustion_without_warning_is_a_quota_violation`.
- UTC day bounds prevent adjacent-day jobs and requests from entering a report: [tests/reconcile.rs](tests/reconcile.rs), `utc_day_bounds_include_start_and_exclude_next_day_for_jobs_and_requests`. Stored UTC milliseconds preserve text ordering without changing event args: [tests/requests.rs](tests/requests.rs), `request_time_is_stored_as_utc_milliseconds_while_event_preserves_original_args`.
- Reconcile exposes invalid manifests and ignores symlinks, preventing silent Warden input failures or following linked directories: [tests/reconcile.rs](tests/reconcile.rs), `malformed_manifests_fail_visibly_and_symlinks_are_not_followed`.
- Malformed protocol lines return one JSON result without a panic, preventing a malformed caller request from crashing dispatch: [tests/protocol.rs](tests/protocol.rs), `seeded_malformed_lines_never_panic_and_return_exactly_one_json_result` (2 000 seeded mutations across every call).
- Hourly backup scheduling, retention of 48 matching backups and backup file permissions have no automated test. The CLI test checks creation of the startup backup only.
