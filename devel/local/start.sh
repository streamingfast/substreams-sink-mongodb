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
    "mainnet.eth.streamingfast.io:443" \
    "./substreams-v0.0.1.spkg" \
    "db_out" \
    "12287507:12293007" \
    "$@"
}

main "$@"
