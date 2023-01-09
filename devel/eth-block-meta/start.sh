#!/usr/bin/env bash

ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

main() {
  cd "$ROOT"

  set -e

  dsn="${MONGO_DSN:-"mongodb://root:root@localhost:27017"}"
  sink="$ROOT/../substreams-sink-mongodb"

  echo dsn

  $sink run \
    ${dsn} \
    "api-unstable.streamingfast.io:443" \
    "./substreams.spkg" \
    "db_out" \
    "-1" \
    "$@"
}

main "$@"
