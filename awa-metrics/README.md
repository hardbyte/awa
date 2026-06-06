# awa-metrics

OpenTelemetry metric definitions shared across the [Awa](https://github.com/hardbyte/awa) job queue crates.

`awa-metrics` is an implementation detail of Awa. The metric names and attribute sets are public (so dashboards / alerts can build against them), but the Rust API is not intended for direct use outside the Awa workspace — `awa-worker` re-exports `AwaMetrics` from this crate, and that's the supported entry point.

The crate is published to crates.io because `awa-worker` depends on the type directly; `cargo publish` strips path dependencies, so consumers of `awa-worker` need to resolve `awa-metrics` from the registry.

## Metric naming

All metrics use the `awa` meter name and follow OpenTelemetry naming and unit guidance: dot-separated namespaces (`awa.job.*`, `awa.dispatch.*`, …), singular nouns, and UCUM units declared via `with_unit` where a unit applies. The metric names and attributes are Awa-specific rather than standardized semantic-convention metrics. Names are exported as constants from [`awa_metrics::names`](src/lib.rs) for tests asserting against an OTLP exporter.
