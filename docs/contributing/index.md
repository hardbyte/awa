# Contributing

AWA's runtime behavior is backed by code tests, PostgreSQL integration tests, compatibility matrices, benchmarks, and TLA+ models. Changes should update the evidence at the same boundary they change.

- [Development](../development.md) is the contributor workflow, including migrations and local checks.
- [Benchmarking](../benchmarking.md) explains which performance results are comparable and which are historical evidence only.
- [Architecture decisions](../adr/README.md) records durable design choices and their status.
- [Correctness models](https://github.com/hardbyte/awa/tree/main/correctness) hold the TLA+ specifications and model-to-code mapping.

Start from the repository's [agent instructions](https://github.com/hardbyte/awa/blob/main/AGENTS.md) when working with a coding agent; it contains the always-on validation and Agent Skills rules.
