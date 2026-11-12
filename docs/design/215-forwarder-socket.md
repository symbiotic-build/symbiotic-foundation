---
status: draft
issue: 215
---

# 215 — Forwarder socket and trusted run identity

A byte forwarder needs no provider credentials or gateway-owned files.
This change adds a separate socket for the forwarder and leaves the core record schema unchanged.

## Example and existing behavior

For run `r-42`, a forwarder connects to its socket and writes `run r-42\n`.
It then copies the sandboxed workload's HTTP bytes without parsing them.
The gateway selects a route of the forwarder's tenant and records `job_id: "r-42"`, regardless of workload headers.

Previously, one socket required an owner-only directory, mode 0600 and the gateway's own peer user ID.
A route was globally unique by method and path.
Caller request IDs came from `x-symbiotic-request-id` or `local-<uuid>`.
The record removed the final hyphen and suffix to derive `job_id`.
Retries appended `.2`, `.3` and so on to the request ID.
Records carried account and route without a tenant field.

## Naming conventions

New public names follow the existing configuration conventions: the `forwarder`
block and its fields `socket_path`, `peer_uid` and `tenant` use lower snake case,
subject first. The Rust type `ForwarderConfig` uses upper camel case with the
existing `Config` suffix. The first-line command `run` is one lower-case word,
followed by a single space, the run identity and a line feed.
The first-line rule below defines the identity's character set; this change adds no error reasons.

## Decisions

- **Optional block:** `PassthroughConfig.forwarder` contains required `socket_path`, `peer_uid` and `tenant` fields and refuses unknown fields.
  Absence preserves behavior and serialized configuration.
- **Configuration checks:** the forwarder socket path is absolute, distinct from other configured paths and in a different directory from the owner socket.
  Its tenant is nonblank and names at least one configured route.
  Invalid settings return the existing configuration error.
- **Binding:** both listeners share bind locking and stale-socket recovery.
  The forwarder directory belongs to the gateway user and has mode 0750 or 02750 exactly.
  Its socket has mode 0660 and must inherit the directory group; the gateway changes no ownership.
  Failed directory or group checks stop startup; deployment must correct provisioning before restart.
- **Peer admission:** only `peer_uid` reaches reads on the forwarder listener.
  The owner listener retains its same-user rule.
  Both share `max_connections` and stop together.
- **First line:** exactly `run ` followed by 1–64 characters from `A-Za-z0-9._-`, then LF without CR.
  Read no more than 70 bytes and never beyond LF, within `io_timeout_seconds`.
  Timeout, malformed and overlong lines close without answer bytes or records and release their slot.
  The forwarder may reconnect with a valid line; no connection state survives.
- **Tenant routes:** the forwarder selects only its tenant's routes; the owner selects all remaining routes.
  Method/path pairs are unique within each set; names remain globally unique.
  A path belonging only to the other set gets 404 `route_not_found`. Fallbacks remain in the same set.
- **Request identity:** forwarder records and logs use `<run>-<32 hexadecimal UUID characters>` as the request ID.
  The suffix contains neither hyphens nor dots, so existing record splitting preserves the complete run identity across retries.
  Caller `x-symbiotic-request-id` values are ignored without validation and never forwarded upstream.
- **Record schema:** no tenant field or core-service change.
  Readers derive tenant from the recorded route.
- **Public entry points:** preserve `bind` and `serve` signatures.
  Serving binds the optional second listener before publishing running health.

No table, index, queue, cache or record field is added.
The additional socket and its retained bind-lock file implement the second listener.

## Rejected alternatives

- **One socket per run:** requires runtime listener creation and removal, a control call, a limit and crash cleanup.
- **A forwarder-added HTTP header:** requires parsing HTTP and lets the workload send the same header first.
- **A second user on the existing socket:** requires opening its owner-only directory to that user.
