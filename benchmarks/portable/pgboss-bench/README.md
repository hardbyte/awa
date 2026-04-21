# pg-boss portable adapter

Long-horizon adapter for `pg-boss` using the official npm package.

- Runtime: `node:22-alpine`
- Library pin: `pg-boss@12.15.0`
- Schema install: `await boss.start()`
- Queue setup: `await boss.createQueue(queue, { partition: true })`

The adapter emits the standard portable JSONL contract:

- startup descriptor with runtime-discovered event tables
- rolling producer / subscriber / end-to-end latency metrics
- enqueue / completion rates
- queue depth
