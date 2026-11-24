# Assert-enabled, -O0 PostgreSQL 18 for the persona soak's pg18-cassert engine
# (scripts/persona-soak.sh). The image ships an initialised, trust-authenticated
# cluster in /data and runs as the postgres user.
#
#   docker build -t awa-pg18-cassert:18.6-O0 docker/pg18-cassert
#   docker build --build-arg PG_VERSION=18.7 -t awa-pg18-cassert:18.7-O0 docker/pg18-cassert
ARG PG_VERSION=18.6

FROM debian:bookworm-slim AS build
ARG PG_VERSION
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential bison flex ca-certificates curl pkg-config \
      libreadline-dev zlib1g-dev libicu-dev libssl-dev liblz4-dev libzstd-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
RUN curl -fsSL "https://ftp.postgresql.org/pub/source/v${PG_VERSION}/postgresql-${PG_VERSION}.tar.bz2" | tar -xj --strip-components=1
RUN ./configure --prefix=/usr/local/pgsql --enable-cassert --enable-debug \
      --with-icu --with-openssl --with-lz4 --with-zstd CFLAGS="-O0 -g" \
    && make -j"$(nproc)" world-bin && make install-world-bin

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
      libreadline8 zlib1g libicu72 libssl3 liblz4-1 libzstd1 mawk procps \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /usr/local/pgsql /usr/local/pgsql
ENV PATH=/usr/local/pgsql/bin:$PATH
RUN useradd -m postgres && mkdir -p /data /var/run/postgresql \
    && chown postgres:postgres /data /var/run/postgresql
USER postgres
RUN initdb -D /data --auth=trust --username=postgres --encoding=UTF8 --locale=C.UTF-8 \
    && echo "host all all 0.0.0.0/0 trust" >> /data/pg_hba.conf
EXPOSE 5432
CMD ["postgres", "-D", "/data", "-c", "listen_addresses=0.0.0.0"]
