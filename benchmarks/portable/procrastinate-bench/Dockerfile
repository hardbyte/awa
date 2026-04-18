FROM python:3.12-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*
RUN curl -LsSf https://astral.sh/uv/install.sh | sh

ENV PATH="/root/.local/bin:${PATH}"

WORKDIR /app
COPY benchmarks/portable/procrastinate-bench ./benchmarks/portable/procrastinate-bench

WORKDIR /app/benchmarks/portable/procrastinate-bench
RUN uv sync

ENTRYPOINT ["sh", "-lc", "cd /app/benchmarks/portable/procrastinate-bench && exec uv run python main.py"]
