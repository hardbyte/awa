#!/bin/sh
set -eu

if [ ! -s "$PGDATA/PG_VERSION" ]; then
  rm -rf "$PGDATA"/*
  export PGPASSWORD="$REPLICATION_PASSWORD"
  until pg_basebackup -h primary -p 5432 -D "$PGDATA" -U "$REPLICATION_USER" -Fp -Xs -P -R; do
    sleep 1
  done
  echo "hot_standby = on" >> "$PGDATA/postgresql.auto.conf"
fi

exec docker-entrypoint.sh postgres
