# ADR-027: Callback ingress as a deployable surface

## Status

Proposed

## Context

Awa currently exposes HTTP-worker callback endpoints through `awa-ui`.
`awa serve` builds one axum router that contains:

- the operator UI static assets,
- the admin REST API under `/api`,
- the HTTP-worker callback receiver under `/api/callbacks`.

That bundling is convenient for development, but it mixes surfaces with
different exposure requirements.

The admin UI and REST API are an operator surface. They should normally be
private, authenticated by deployment infrastructure, and reachable only from
trusted networks. The callback receiver is different: when using
`HttpWorker` async mode, the remote function must be able to reach it to
complete, fail, or heartbeat a parked job. In many deployments that means the
callback receiver is public or partner-facing, even when the admin UI is not.

Today users have two imperfect options:

1. Run `awa serve` and expose only `/api/callbacks` through a reverse proxy.
   This works, but the process still contains the whole admin/UI surface and
   permissive UI CORS configuration.
2. Implement their own callback endpoint around `admin::complete_external`,
   `admin::fail_external`, and `admin::heartbeat_callback`. This gives them
   control over FastAPI, axum, Django, or another framework, but forces them
   to reimplement Awa's callback signature contract, payload parsing, error
   mapping, and future compatibility rules.

ADR-018 deliberately made HTTP workers possible without new storage
primitives, and ADR-021 defines the callback semantics. This ADR narrows the
HTTP deployment surface so those semantics can be exposed safely without
requiring the admin UI.

## Decision

Make callback ingress a first-class Awa surface, separate from the admin UI.

### Surface split

Awa's HTTP-facing pieces should be documented and structured as distinct
surfaces:

| Surface | Purpose | Expected exposure |
|---|---|---|
| Admin UI / admin REST API | Operator inspection and mutation | Private operator network |
| Callback receiver | Complete, fail, and heartbeat callback waits | Public or partner-facing, signed |
| Worker runtime | Claim and execute jobs | Internal |
| Maintenance runtime | Promote, rescue, prune, and refresh runtime state | Internal |

`awa serve` remains the admin/UI command. It may continue to mount callback
routes for backward compatibility, but the callback receiver should no longer
be treated as an incidental part of the UI crate.

### Shared callback contract and auth

Extract the HTTP callback contract into a shared module or crate that is not
owned by `awa-ui`:

- request and response types for `complete`, `fail`, and `heartbeat`,
- the `X-Awa-Signature` header name,
- BLAKE3 keyed-hash signing and verification,
- 32-byte secret parsing,
- timeout validation and error mapping,
- contract tests shared by every HTTP integration.

The worker-side signer and receiver-side verifier must use the same
implementation. The option name `hmac_secret` may remain for compatibility,
but docs and new APIs should describe the algorithm precisely as BLAKE3 keyed
hashing.

### Callback-only axum router

Expose a callback-only axum router, for example:

```rust
let router = awa_callbacks::router(pool, CallbackReceiverConfig {
    secret: Some(callback_secret),
    path_prefix: "/api/callbacks".into(),
    ..Default::default()
});
```

The callback-only router should:

- mount only callback routes,
- serve no UI assets,
- expose no admin REST routes,
- avoid permissive CORS by default,
- require writable database access,
- require a callback signing secret by default unless the caller explicitly
  opts into unsigned callbacks.

This router can be mounted by Awa's own CLI, by an existing axum application,
or by users who want a minimal callback receiver binary.

### Framework-neutral service layer

Provide lower-level callback receiver functions that can be used outside
axum:

- `complete_callback(pool, callback_id, signature, payload)`,
- `fail_callback(pool, callback_id, signature, error)`,
- `heartbeat_callback(pool, callback_id, signature, timeout)`.

The service layer should own validation and signature verification, then call
the storage/admin APIs. This lets Rust frameworks share one implementation and
gives the Python package a clear contract to mirror for FastAPI, Starlette,
Django, or Flask examples.

Python helpers should at minimum expose signature verification and typed
request parsing examples. If the Python admin/client API grows a callback
receiver helper, it should preserve the same contract and tests.

### CLI

Add a callback-only command:

```text
awa callbacks serve
```

The command should accept the normal database/pool options plus callback
receiver configuration:

- host and port,
- callback signing secret,
- path prefix,
- optional explicit unsigned mode for private-network deployments.

It should not inherit `awa serve`'s admin routes, static file fallback, or UI
CORS behavior.

### Callback URL construction

`HttpWorker` should stop hard-coding callback URLs as:

```text
{callback_base_url}/api/callbacks/{callback_id}/complete
```

Keep that as the compatibility default, but add configuration for a path
prefix or URL template. That allows callback-only servers and user-owned API
layers to expose framework-native routes without reverse-proxy rewriting.

### Lifecycle hooks

Callback ingress resolves callback state. It must not assume a co-located
`Client` runtime or local in-process lifecycle hook registry.

If callback completion should produce lifecycle notifications beyond the
storage transition itself, that mechanism needs to be durable or otherwise
runtime-independent. The callback receiver should call a shared resolution
service and inherit whatever durable side effects the model layer guarantees,
rather than invoking process-local hooks directly.

## Consequences

### Positive

- The recommended secure deployment becomes obvious: public callback ingress,
  private admin UI, internal workers and maintenance.
- Users can embed Awa callbacks in FastAPI, axum, or another application
  without copying the signature algorithm and payload contract from docs.
- Minimal callback deployments no longer carry the embedded frontend or admin
  REST surface.
- `HttpWorker` async mode becomes easier to operate behind real routing
  schemes instead of relying on the UI path layout.

### Negative / Risks

- There is one more public surface to version and test.
- The split overlaps with, but does not replace, the broader `awa-api` /
  `awa-ui` separation discussed elsewhere. Keeping this narrowly scoped is
  important.
- Tightening callback-only defaults to require signatures is safer but may
  surprise users who expect the old `awa serve` behavior where missing secrets
  disable verification. Compatibility docs and explicit opt-out flags are
  required.
- Lifecycle hook behavior around externally resolved jobs must be stated
  precisely so users do not assume the callback receiver runs in the same
  process as their worker hooks.

## Alternatives considered

### Keep callback routes inside `awa serve`

This is the smallest implementation, and it works when deployments can rely on
private networking or a reverse proxy. It keeps the security guidance awkward:
the process users need to expose for callbacks also contains admin mutation
routes and UI concerns.

### Split all API handlers into a new `awa-api` crate first

A broader API/UI split may still be useful, especially if Awa ships an
API-only image. It is more work than the callback problem requires. Callback
ingress has a distinct security boundary and should be separable even if the
admin API remains in `awa-ui` for now.

### Require every application to implement callbacks itself

This gives maximum framework flexibility, but makes every integration
responsible for reproducing auth, parsing, and compatibility behavior. That is
exactly the glue ADR-018 tried to remove for HTTP workers.

### Rely on reverse-proxy path filtering

Path filtering is still useful defense-in-depth, but it should not be the only
way to avoid exposing the admin UI. A callback-only router gives the proxy a
smaller upstream to protect.

## Relationship to other ADRs

- ADR-018 introduced `HttpWorker` and the callback receiver contract this ADR
  narrows into its own deployable surface.
- ADR-021 defines callback completion, sequential waits, and heartbeats. This
  ADR changes HTTP exposure, not callback semantics.
- ADR-015 defines builder-side lifecycle hooks. Callback ingress should not
  depend on process-local hook registries unless a future ADR makes those
  effects durable or runtime-independent.
