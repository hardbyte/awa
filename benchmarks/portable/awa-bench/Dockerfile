FROM rust:bookworm AS builder
WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY awa-model ./awa-model
COPY awa-macros ./awa-macros
COPY awa-worker ./awa-worker
COPY benchmarks/portable/awa-bench ./benchmarks/portable/awa-bench

ENV SQLX_OFFLINE=true
RUN cargo build --release --manifest-path benchmarks/portable/awa-bench/Cargo.toml

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/benchmarks/portable/awa-bench/target/release/awa-bench /usr/local/bin/awa-bench
ENTRYPOINT ["awa-bench"]
