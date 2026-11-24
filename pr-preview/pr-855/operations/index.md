> For the complete AWA documentation index, see [`llms.txt`](../llms.txt).

# Operations

AWA keeps its control plane in PostgreSQL, so safe operation begins with database privileges, migrations, compatibility, and observability.

## Production path

1. Review [Configuration](../configuration/index.md) for queues, workers, deadlines, priorities, and storage controls.
2. Read [Deployment](../deployment/index.md) for process topology, graceful shutdown, migration ordering, and container images.
3. Use [Managed Postgres](../deploying-on-managed-postgres/index.md) for hosted-service constraints and tuning.
4. Apply the role split in [Security](../security/index.md).
5. Import the [Grafana dashboards and alerts](../grafana/index.md), or query the same health surfaces with the CLI.
6. Keep [Troubleshooting](../troubleshooting/index.md) with your runbooks.

## Upgrades

- [Migrations](../migrations/index.md) explains forward-only schema changes and application/CLI entry points.
- [Upgrade 0.5 to 0.6](../upgrade-0.5-to-0.6/index.md) covers the queue-storage transition.
- [Upgrade 0.6 to 0.7](../upgrade-0.6-to-0.7/index.md) covers the current development-line rollout contract.

!!! note "Match documentation to your installed version"
    This site tracks `main` and 0.7 development. Use the release tag and package documentation for a stable 0.6 deployment, especially for migration and storage-transition procedures.
