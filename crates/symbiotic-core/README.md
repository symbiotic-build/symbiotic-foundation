# symbiotic-core

Tiny shared vocabulary for Symbiotic components: identifiers, labels, the `retry`
module and the contract types below. The crate has no HTTP client, async runtime,
queue or database.

## Contract types

Two modules hold the types that several systems exchange. Each component imports
them from here and keeps no copy of its own. The types are serde types only: they
carry no behaviour and do no I/O.

### `decision::DecisionEnvelope`

The one object that asks a person for anything. It holds a group of numbered
questions that resolve together and are delivered as one page. Each question
carries its kind, prompt, claim-labelled context, why-now, evidence, options with
consequence and reversibility, one recommended option with its confidence, a
severity, a safe default, the cost of delay and its escalation trail.

- An answer names the revision it answers, so an answer to a stale revision is
  refused.
- `more_context` is always a valid answer. It names the missing parts: evidence,
  alternatives, cost, an example or the consequences.
- The `ResolutionTrail` lists the tiers tried before a person saw the question:
  recall, peer, council, operator.
- Text fields follow the House Rules question format: the prompt is the question
  heading, and each context sentence starts with `fact` or `assessment`.

Producers: the Symbiotic runtime's question groups, and Mission Control's policy
runner for the `ask` action. Consumer: Mission Control, which stores and delivers
decisions and wakes the sessions in `wakes` with the answer.

`QuestionSeverity` (trivial, informational, decision, critical) rates a question.
It is a different scale from `EventSeverity`, which rates an event.

### `event::SystemEvent`

One typed fact a system emitted: a job failed, a provider refused, a CI run
failed. `detail` holds numbers and enumeration tokens only; a value with spaces
or more than 64 characters fails to parse, so prompt and transcript text cannot
ride in an event.

Producers: source adapters for Warden, the Foundation model proxy, GitHub,
session hooks, hosts and the build queue. Consumer: Mission Control's event
endpoint and policy runner.

## Naming conventions

- Types are upper camel case nouns: `SystemEvent`, `DecisionEnvelope`.
- Rust fields are lower snake case. Wire fields are lower camel case through
  `#[serde(rename_all = "camelCase")]`, because Mission Control is TypeScript:
  `observed_at` is `observedAt`.
- Enumeration values on the wire are lower snake case: `more_context`,
  `attached_repo`.
- Tagged unions put their variant in a field named `kind`:
  `{ "kind": "more_context", "missing": ["evidence"] }`.
- Event kinds are dotted, subject first, verb in the past tense:
  `provider.refused`, `job.failed`.
- Identifiers are strings wrapped in one newtype per object: `EventId`,
  `DecisionId`, `CampaignId`.
- Timestamps are ISO 8601 UTC strings, carried unparsed in `Timestamp`.
- Unknown fields fail to parse.

## Schemas

The `schema` feature derives JSON schemas. `schemas/` holds one file per
top-level type. A test checks that the files match the types; refresh them after
a type change:

```sh
UPDATE_SCHEMAS=1 cargo test --release -p symbiotic-core --features schema --test schemas
```

## Fixtures

`tests/fixtures/` holds one record per type, shaped after records of 2026-10-31.
`tests/contracts.rs` parses each one and checks it round-trips without loss.
The fixtures are synthetic: their quoted words and free text are invented, and
they carry no transcript text. Replays against real records run in a private
corpus outside this repository.
