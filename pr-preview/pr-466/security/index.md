> For the complete AWA documentation index, see [`llms.txt`](../llms.txt).

# Security

Awa's security boundary has two parts: PostgreSQL privileges determine who can read or mutate queue state, while network placement and callback authentication determine which HTTP surfaces are reachable. Production deployments should separate both.

## Start here

| Concern | Guidance |
| --- | --- |
| Database ownership and grants | [Database roles and privileges](database-roles/index.md) |
| Admin UI, callbacks, workers, and network exposure | [Deployable surfaces](deployable-surfaces/index.md) |
| Callback authentication and custom receivers | [Callback security](callback-security/index.md) |

## Production baseline

1. Use a non-login schema owner, a migration login, and a separate runtime login.
2. Keep `awa serve` on an authenticated operator network. It is a database administration surface, not a public application endpoint.
3. Put externally reachable callbacks on a callback-only listener or in your own application router; do not expose the admin router with them.
4. Configure callback signatures unless an authenticating proxy or trusted network already provides the boundary.
5. Use TLS, rotate secrets per environment, and avoid logging callback signatures.

The current 0.6 runtime still needs broad DML and `TRUNCATE` privileges on Awa's internal tables. [ADR-043](../adr/043-postgresql-capability-functions/index.md) defines a proposed capability-function design for narrower roles; do not treat that future design as a shipped security control.

## Deployable roles

The admin UI, callback ingress, workers, and maintenance tasks have different exposure profiles. See [Deployable surfaces](deployable-surfaces/index.md) for supported deployment shapes and network boundaries.
