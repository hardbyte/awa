# Multi-stage Dockerfile for awa CLI
# Builds a minimal static binary for multi-arch deployment.

FROM rust:1.84-alpine AS builder
RUN apk add --no-cache musl-dev
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    SQLX_OFFLINE=true cargo build --release --package awa-cli \
    && cp target/release/awa /awa

FROM cgr.dev/chainguard/static:latest
COPY --from=builder /awa /usr/local/bin/awa
USER 65534
ENTRYPOINT ["awa"]
