#!/usr/bin/env bash
#
# Script
#
# Must be run with nightly rust for example
# rustup default nightly

set -e

CRATES="
    -p arrow
    -p arrow-arith
    -p arrow-array
    -p arrow-avro
    -p arrow-buffer
    -p arrow-cast
    -p arrow-csv
    -p arrow-data
    -p arrow-ipc
    -p arrow-json
    -p arrow-ord
    -p arrow-row
    -p arrow-schema
    -p arrow-select
    -p arrow-string
"

setup_miri() {
    export MIRIFLAGS="-Zmiri-disable-isolation"
    export INSTA_WORKSPACE_ROOT="$PWD"
    cargo miri setup
    cargo clean
}


case $# in 
    0)
        setup_miri

        echo "Starting Arrow MIRI run..."
        cargo miri nextest run \
        $CRATES \
        --features ffi --no-fail-fast
    ;;
    2)
        setup_miri

        partition=$1
        total=$2

        echo "Starting Arrow MIRI run partition ${partition} out of ${total}..."
        cargo miri nextest run \
        --partition slice:"${partition}"/"${total}" \
        $CRATES \
        --features ffi --no-fail-fast
    ;;
    *)
        echo "usage: $0 [partition total]" >&2
        exit 1
    ;;
esac
