FROM rust:bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-venv curl pkg-config libssl-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/*
RUN curl -LsSf https://astral.sh/uv/install.sh | sh

ENV PATH="/root/.local/bin:${PATH}"

WORKDIR /app
COPY Cargo.toml Cargo.lock README.md ./
COPY awa-model ./awa-model
COPY awa-macros ./awa-macros
COPY awa-worker ./awa-worker
COPY awa-python ./awa-python
COPY benchmarks/portable/awa-python-bench ./benchmarks/portable/awa-python-bench

WORKDIR /app/awa-python
RUN uv sync --group dev

WORKDIR /app
ENTRYPOINT ["sh", "-lc", "cd /app/awa-python && uv run python /app/benchmarks/portable/awa-python-bench/main.py"]
