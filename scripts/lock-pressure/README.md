# Lock pressure diagnostics

Experimental source helpers for awa maintenance traces and PostgreSQL JSON logs. These are individual measurement tools; a complete campaign runner and release acceptance criteria are not included. Python 3.10+ is required. Source and archive verification use assertions; those helpers reject Python's `-O` mode.

Run the synthetic parser controls from any directory:

```sh
python scripts/lock-pressure/check_parsers.py
```

Replay closed log files with a caller-supplied JSON source list. Paths are resolved relative to the source-list file; `original_path` supplies the original suffix and provenance identity. Use `.json` for PostgreSQL JSON log records and set `gzip: true` for compressed input. The output directory must be new.

```json
[{"path":"server.json","original_path":"server.json","source_key":"server:1","gzip":false}]
```

```sh
python scripts/lock-pressure/replay.py --sources sources.json --out analysis
python scripts/lock-pressure/prune_calls.py run-directory
```

`replay.py` writes `utility-events.jsonl`, `utility-summary.json` and `replay.json`. The default event-output cap is 256 MiB (`--event-cap-bytes`). That cap does not bound total input size, summary memory, or provenance memory; choose bounded input files. Invalid JSON and partial final lines are recorded. Counts describe direct SQL commands: successful statements do not prove transaction commit or internal lock-request counts. `prune_calls.py` scans `run.log` and top-level `*.stderr`/`*.stdout`, caps total input at 64 MiB (`--cap-bytes`), and writes `prune-call-ledger.json`; missing start/end pairs stay incomplete. Both tools write local outputs and preserve input files.

`evidence.py` exposes `replay`, `compare_feed_replay` and the rotation-aware `InodePump`. `archive.py` exposes read-only `replay_sources(raw, archive_dir, receipts, stream_index=None)`, validating compressed and decompressed hashes before selecting archived streams and suppressing duplicate raw copies. Receipts retain their recorded paths; archived packet relocation needs an explicit caller mapping. Live archival and process-closure probes are excluded because their guarantees depend on the capture environment. A replay call alone does not establish producer closure.

`slot_observer.py` exposes `observe(connection, phase, exact_rows=False, budget_seconds=2.0)` for an exclusively owned, idle psycopg autocommit connection. Install psycopg separately when integrating it. It reads the adjacent `catalog.sql`, targets the default `awa` schema, temporarily sets 100 ms statement/lock timeouts and restores the prior values. The two-second budget controls admission of additional exact-count queries; it is not a hard client or network deadline. Transport failures propagate and require closing/resetting the connection. Keep metadata and checkpoint policies identical between compared variants. Row estimates, exact physical rows, member counts, stale generations and unknown observations remain separate; physical rows across families are not unique jobs.

`relation-lock-snapshot.sql` samples relation-lock rows for the controlled `operation_lock_footprint_test` fixture. Bind its application-name list as `$1::text[]`; it restricts database names to `awa_footprint_%`. A caller must supply the polling interval and observation limits. Match database OID, backend start and the fixture's operation markers before attributing a sample. Activity and lock observations are not atomic, and sampled maxima are lower bounds on true peaks. The fixture and SQL do not set a one-millisecond sampling interval or change prune's lock timeout.

Generate a prune tracing patch without editing a checkout:

```sh
python scripts/lock-pressure/prepare_overlay.py --repository . --variant baseline=REF --variant candidate=OTHER_REF --out new-overlays
```

`prepare_overlay.py` resolves each ref to a commit, reads `awa-model/src/queue_storage.rs`, and writes a patch and source-hash/proof manifest beneath a new output directory. `prune_trace.rs` is the inserted template. Source anchors and reversible wrapper checks reject incompatible source layouts; arbitrary future versions are not supported. Generated patches are uncompiled and unrun until separately reviewed and validated. No runtime source is changed by generation. The overlay adds logging and timing work, so measured variants need identical instrumentation. Log writes can fail silently; missing events are unknown. Initial lock failures, later stages and transaction commits remain distinct in the derived ledger.

The included 41-record fixture is synthetic. Controls compare replayed event bytes with direct parsing, reject duplicated/missing provenance events and corrupt archives, verify raw/archive deduplication, and retain invalid/partial-line diagnostics. They do not run a database, compile Rust, establish live closure, or measure campaign performance.
