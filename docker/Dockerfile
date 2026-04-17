# Multi-stage Dockerfile for awa CLI
# Builds a minimal static binary for multi-arch deployment.
#
# The frontend is built first (node stage), then rust-embed picks up
# `awa-ui/static/` when the cargo stage runs. Without this ordering the
# binary ships the placeholder HTML ("Frontend not built. ...") — see
# issue #162.

FROM node:22-alpine AS frontend
WORKDIR /src
COPY awa-ui/frontend/package.json awa-ui/frontend/package-lock.json awa-ui/frontend/
RUN --mount=type=cache,target=/root/.npm \
    cd awa-ui/frontend && npm ci
COPY awa-ui/frontend awa-ui/frontend
# Vite writes to ../static (i.e. awa-ui/static) relative to the frontend dir.
RUN cd awa-ui/frontend && npm run build

FROM rust:1.90-alpine AS builder
RUN apk add --no-cache musl-dev
WORKDIR /src
COPY . .
# Replace the source's empty static/ with the built frontend from the node stage.
COPY --from=frontend /src/awa-ui/static/ awa-ui/static/
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    SQLX_OFFLINE=true cargo build --release --package awa-cli \
    && cp target/release/awa /awa

FROM cgr.dev/chainguard/static:latest
COPY --from=builder /awa /usr/local/bin/awa
USER 65534
ENTRYPOINT ["awa"]
