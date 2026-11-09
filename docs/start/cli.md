# Install the CLI

The `awa` command runs migrations, inspects and administers queues, and can host the optional web dashboard.

=== "uv tool (recommended)"

    ```bash
    uv tool install awa-cli
    awa --help
    ```

=== "Project dependency"

    ```bash
    uv add 'awa-pg[ui]'
    uv run python -m awa --help
    ```

    `python -m awa` delegates to the bundled `awa` binary inside the project environment.

=== "Release binary"

    Download the archive for your platform from the [GitHub Releases](https://github.com/hardbyte/awa/releases) page, then put `awa` on your `PATH`.

Point commands at PostgreSQL with `--database-url` or `DATABASE_URL`:

```bash
export DATABASE_URL=postgres://awa_runtime:secret@db.example.com/app
awa health
awa queue stats
awa job list --queue email
```

Continue to the [CLI command map](../reference/cli.md) or launch the dashboard with `awa serve`.
