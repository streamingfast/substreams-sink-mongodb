#!/usr/bin/env bash

ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

main() {
  cd "$ROOT"

  set -e

  dsn="${MONGO_DSN:-"mongodb://root:root@localhost:27017"}"
  sink="$ROOT/../substreams-sink-mongodb"
  substreams_spkg="${SUBSTREAMS_SPKG:-"https://github.com/streamingfast/substreams-eth-block-meta/releases/download/v0.4.1/substreams-eth-block-meta-v0.4.1.spkg"}"

  $sink run \
    ${dsn} \
    dev_db \
    ${ROOT}/schema.json \
    "mainnet.eth.streamingfast.io:443" \
    "$substreams_spkg" \
    "db_out" \
    "$@"
}

main "$@"
