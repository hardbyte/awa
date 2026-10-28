> For the complete AWA documentation index, see [`llms.txt`](../../llms.txt).

# Callback security

The callback receiver exposes state-changing endpoints for externally executed attempts:

```text
POST {prefix}/:callback_id/complete
POST {prefix}/:callback_id/fail
POST {prefix}/:callback_id/heartbeat
```

The default prefix is `/api/callbacks`. These routes can complete, fail, or extend an attempt, so they require an authentication boundary.

## Signed callbacks

Awa supports a 32-byte BLAKE3 keyed hash over the callback ID. Despite the historical `hmac` option name, this is BLAKE3 keyed hashing, not RFC HMAC.

- Configure the receiver with `--callback-hmac-secret` or `AWA_CALLBACK_HMAC_SECRET` using 64 hexadecimal characters.
- Configure `HttpWorkerConfig.hmac_secret` with the same 32-byte key.
- The dispatcher sends `X-Awa-Signature`; the external worker forwards it when calling the receiver.
- The receiver verifies the signature before accepting a callback mutation.

If no secret is configured, signature verification is disabled. Use that only when a trusted network or authenticating proxy already protects the receiver.

## Custom receivers

Use `awa::callback_contract` in Rust or `awa.callback_contract` in Python rather than reimplementing signature verification. Both language surfaces call the same Rust implementation and share a pinned test vector. The [callback receiver guide](../../callback-receivers/index.md) includes axum and FastAPI examples.

## Operational checklist

- Terminate TLS before any externally reachable receiver.
- Use a different secret in each environment and rotate it like any shared credential.
- Do not log callback signatures or secret material.
- Expose only the callback routes, not the admin router.
- Grant the receiver only the database/runtime authority its deployment model requires; never give it migrator credentials.
