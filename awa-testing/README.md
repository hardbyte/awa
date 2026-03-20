# awa-testing

Test utilities for the [Awa](https://crates.io/crates/awa) job queue.

Provides `TestClient` for integration testing job handlers without running the full worker runtime:

```rust
let client = TestClient::from_pool(pool).await;
client.migrate().await?;

let job = client.insert(&SendEmail { to: "test@example.com".into(), subject: "Test".into() }).await?;
let result = client.work_one_in_queue(&MyWorker, Some("default")).await?;
assert!(result.is_completed());
```

## License

MIT OR Apache-2.0
