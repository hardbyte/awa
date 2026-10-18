# Initial finalizer capacity investigation

This packet compares main `88f9a69` with the first reviewed reconnect runtime
`3daaff7`. It is investigation data, not a performance check of the final
runtime. Each cell used 240 s warmup and 240 s measurement; all four passed
the actual queue/claim ring-wrap gates. The parent report describes the
shared-host method and measurement boundaries.

| Cell | Build | Handler returns/s | Median window p99, ms | Worst window p99, ms | Mean queue depth |
| --- | --- | ---: | ---: | ---: | ---: |
| 800 jobs/s | Baseline | 800.045 | 73.023 | 150.015 | 13.375 |
| 800 jobs/s | Candidate | 800.017 | 69.535 | 182.015 | 12.104 |
| Backlogged, 64 workers | Baseline | 3690.734 | 943.103 | 1789.951 | 1860.021 |
| Backlogged, 64 workers | Candidate | 2022.602 | 2707.455 | 8970.239 | 3564.083 |

The candidate limited pending finalizers to worker capacity: 64 here. That
prevents a configured 512-item completion batch from forming and leaves no
next batch queued while a full flush waits on PostgreSQL. Shared storage
interference also affected both runs, so these observations do not attribute
the exact throughput difference to that limit.

The correction derives the budget from the larger of worker capacity and
two configured batches per completion shard. A real SQL regression holds a
full flush, admits another full batch with one worker, checks backpressure
when the budget fills, then verifies all jobs durably complete. Restoring the
previous limit makes that regression fail while forming the first batch.
The parent packet repeats the measurements on the corrected executable.

All four harness subprocesses reached the runner's successful completion
checks and the final provenance files were written. The outer execution
reported status 1 without an accompanying error. A separate export
validation succeeded, and an independent review reproduced the raw-file
hashes, sample partitions and aggregates. This packet is retained for
diagnosis; it is not used as a successful final verification command.
