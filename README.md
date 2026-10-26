# Symbiotic Foundation

Reusable Rust contracts for durable AI work:

- `symbiotic-egress` — versioned durable-attempt/permit schema and `EgressClient`
  for Memory, including the bounded local-socket client.
- `symbiotic-credential-process` — credential-owning daemon with owner-only-file
  credentials and keyless routes, single-use permits and typed usage. It reuses the
  model runtime with caching off. See [model egress](docs/architecture/model-egress.md).
- `symbiotic-ai-runtime` — **the entry point for model calls.** Open one
  `Runtime` (dispatch requires an explicit state directory) and get
  ready chat, embedding, rerank and classifier providers. Queueing, retries,
  limits, cooldowns, attempt budgets, caching, traces, usage receipts and
  persistence are internal. See [docs/architecture/ai-runtime.md](docs/architecture/ai-runtime.md).
- `symbiotic-core` — tiny shared vocabulary, identifiers, and the public `retry`
  module for Retry-After parsing, backoff, and deterministic jitter. It needs no
  HTTP client, async runtime, queue, or database.
- `symbiotic-queue` — durable execution queue traits and state vocabulary,
  plus the in-process `MemoryQueue` backend and a backend conformance suite; no
  storage dependency.
- `symbiotic-queue-sqlite` — the local SQLite backend (`SqliteQueue`) for those
  traits.
- `symbiotic-model` — provider-neutral model/operator runtime traits: chat,
  embedding, rerank and classification (typed questions answered with
  probabilities, served by TypeSafe System One or any chat model). The
  queue-bound `Queued*` wrappers are its default `queue` feature;
  `default-features = false` gives the contracts and HTTP providers without the
  queue runtime. Neither configuration links SQLite. Hosts use the queued
  providers through `symbiotic-ai-runtime`; building them directly is
  unsupported.
- `symbiotic-trace` — normalized invocation traces and pluggable sinks.
- `symbiotic-portability` — external record interchange validation and explicit
  CSV/Markdown presentations; no Memory or application mutation dependency.

This repository is contract-first. It intentionally does not own Symbiotic memory, Archive,
Gatekeeper, Vault, Matrix transport, or agent role evolution.
Ownership follows the [boundary contract](docs/architecture/boundary.md#ownership);
current API gaps are documented in the architecture pages.

## Why This Exists

The current Symbiotic runtime and memory experiments proved the shape, but the code grew around
urgent benchmark and product needs. This workspace rebuilds the generic layer directly:

```text
symbiotic-memory  -> foundation traits
symbiotic-runtime -> foundation implementations + policy
foundation        -> no dependency on memory/runtime
```

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the current crate map.

## Working on this repository

This repository uses [House Rules](https://github.com/jak-pan/house-rules) as the base rules for agentic work, and Warden, our review service, reviews its pull requests on request. If you work here, with or without an agent, follow House Rules too. This repository's own rules, including any that tighten or loosen House Rules, are in [AGENTS.md](AGENTS.md).
