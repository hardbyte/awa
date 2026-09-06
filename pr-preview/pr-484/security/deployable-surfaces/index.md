> For the complete AWA documentation index, see [`llms.txt`](../../llms.txt).

# Deployable surfaces

Awa ships one binary, but its runtime surfaces have different trust boundaries. Production deployments should not expose them all on one listener.

| Surface | Purpose | Recommended exposure |
| --- | --- | --- |
| Admin UI and API (`awa serve`) | Inspect and mutate jobs, queues, runtime and DLQ state | Authenticated operator network |
| Callback receiver | Complete, fail, or heartbeat externally executed jobs | Public or partner-facing only when authenticated |
| Workers and dispatchers | Claim jobs and execute handlers | Internal network |
| Maintenance | Promote, rescue, prune, and refresh metadata | Internal; elected from the worker fleet |
| PostgreSQL | Authoritative storage and coordination | Private network |

## Admin UI

`awa serve` is an operator surface. It includes the dashboard and mutating administration routes and currently can also include callback routes. Put it behind normal authentication and authorization, restrict it with ingress or firewall policy, and prefer a private address.

Do not publish the all-in-one development router to the internet. When callbacks must be reachable externally, run `awa callbacks serve` on a separate listener or mount the callback contract in an existing application. The callback-only router omits the admin API, UI assets, and permissive admin CORS behavior.

## Common deployment shapes

- **Local development:** admin UI, callback routes, workers, and PostgreSQL can share one machine.
- **Private admin, public callbacks:** place `awa serve` inside the operator network and expose only `awa callbacks serve` through the external load balancer.
- **Application-owned callback API:** mount the verified callback routes in an existing FastAPI, axum, or Flask service.
- **HTTP worker:** an Awa client still needs to claim jobs and dispatch the function. A function endpoint alone does not consume queued work.

See [Callback security](../callback-security/index.md) and [HTTP callbacks](../../http-callbacks/index.md) before exposing callback ingress.
