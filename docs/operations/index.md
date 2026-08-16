# Operations

AWA keeps its control plane in PostgreSQL, so safe operation begins with database privileges, migrations, compatibility, and observability.

## Production path

1. Read [Deployment](../deployment.md) for process topology, graceful shutdown, migration ordering, and container images.
2. Use [Managed Postgres](../deploying-on-managed-postgres.md) for hosted-service constraints and tuning.
3. Apply the role split in [Security](../security.md).
4. Import the [Grafana dashboards and alerts](../grafana/README.md), or query the same health surfaces with the CLI.
5. Keep [Troubleshooting](../troubleshooting.md) with your runbooks.

## Upgrades

- [Migrations](../migrations.md) explains forward-only schema changes and application/CLI entry points.
- [Upgrade 0.5 to 0.6](../upgrade-0.5-to-0.6.md) covers the queue-storage transition.
- [Upgrade 0.6 to 0.7](../upgrade-0.6-to-0.7.md) covers the current development-line rollout contract.

!!! note "Match documentation to your installed version"
    This site tracks `main` and 0.7 development. Use the release tag and package documentation for a stable 0.6 deployment, especially for migration and storage-transition procedures.
