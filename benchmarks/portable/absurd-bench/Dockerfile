FROM python:3.12-slim

ARG ABSURD_VERSION=0.3.0

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates postgresql-client \
    && rm -rf /var/lib/apt/lists/*
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
RUN curl -fsSL https://raw.githubusercontent.com/earendil-works/absurd/${ABSURD_VERSION}/sql/absurd.sql -o /opt/absurd.sql

ENV PATH="/root/.local/bin:${PATH}"
ENV ABSURD_SQL_PATH="/opt/absurd.sql"

WORKDIR /app
COPY absurd-bench ./absurd-bench

WORKDIR /app/absurd-bench
RUN uv sync

ENTRYPOINT ["sh", "-lc", "cd /app/absurd-bench && uv run python main.py"]
