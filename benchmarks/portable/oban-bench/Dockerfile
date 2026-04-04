FROM elixir:1.17-alpine AS builder
RUN apk add --no-cache build-base git

WORKDIR /app
ENV MIX_ENV=prod

# Install hex and rebar
RUN mix local.hex --force && mix local.rebar --force

# Install deps
COPY mix.exs ./
RUN mix deps.get && mix deps.compile

# Copy app code
COPY config/ config/
COPY lib/ lib/
COPY priv/ priv/

# Compile
RUN mix compile

# ── Runtime ────────────────────────────────────────────────────
FROM elixir:1.17-alpine
RUN apk add --no-cache libstdc++ postgresql-client

WORKDIR /app
ENV MIX_ENV=prod

RUN mix local.hex --force && mix local.rebar --force

COPY --from=builder /app /app

# Run: migrate then execute benchmark (or chaos scenario)
# SCENARIO=migrate_only  → run migrations only, then exit
# SCENARIO=worker_only   → run migrations, start app with --no-halt (blocks forever processing jobs)
# Otherwise              → run migrations, then execute benchmark runner
ENTRYPOINT ["sh", "-c", "\
  if [ \"$SCENARIO\" = 'migrate_only' ]; then \
    mix ecto.setup; \
  elif [ \"$SCENARIO\" = 'worker_only' ]; then \
    mix ecto.setup && mix run --no-halt -e 'ObanBench.run()'; \
  else \
    mix ecto.setup && mix run -e 'ObanBench.run()'; \
  fi"]
