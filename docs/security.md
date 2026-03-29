# Security Notes

## Admin Surface

`awa serve` exposes a read/write admin API and UI. Treat it as an operator surface, not a public endpoint.

- Put it behind your normal authentication and authorization layer.
- Restrict network access with ingress policy, firewall rules, or private networking.
- Prefer binding to localhost or an internal service address unless you explicitly need external access.

## Callback Endpoints

When using `HttpWorker` async mode, `awa-ui` exposes these callback receiver endpoints:

- `POST /api/callbacks/:callback_id/complete`
- `POST /api/callbacks/:callback_id/fail`
- `POST /api/callbacks/:callback_id/heartbeat`

These endpoints mutate job state and should not be exposed without protection.

## Callback Signature Verification

Awa supports per-callback request authentication with a 32-byte BLAKE3 keyed hash.

- Configure the callback receiver with `--callback-hmac-secret <64-hex-chars>` or `AWA_CALLBACK_HMAC_SECRET`.
- Configure `HttpWorkerConfig.hmac_secret` with the same 32-byte key.
- The worker signs the callback ID and sends the signature as `X-Awa-Signature`.
- The callback receiver verifies that header before accepting `complete`, `fail`, or `heartbeat`.

Example:

```bash
export AWA_CALLBACK_HMAC_SECRET=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
awa --database-url "$DATABASE_URL" serve --host 0.0.0.0 --port 3000
```

If no callback secret is configured, signature verification is disabled. That is acceptable only for trusted internal deployments where the callback receiver is already protected by network boundaries or an authenticating proxy.

## Custom Callback Receivers

If you do not use `awa-ui` and instead mount your own callback handlers around `admin::complete_external`, `admin::fail_external`, or `admin::heartbeat_callback`, you must provide equivalent request authentication yourself.

## Operational Guidance

- Rotate callback secrets like any other shared secret.
- Use different secrets per environment.
- Prefer HTTPS/TLS termination in front of any externally reachable callback receiver.
- Avoid logging callback signatures or other shared-secret material.
