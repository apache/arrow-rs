#!/usr/bin/env bash
#
# Script
#
# Must be run with nightly rust for example
# rustup default nightly

set -e

setup_miri() {
    export MIRIFLAGS="-Zmiri-disable-isolation"
    cargo miri setup
    cargo clean
}


case $# in 
    0)
        setup_miri

        echo "Starting Arrow MIRI run..."
        cargo miri nextest run \
        -p arrow-buffer -p arrow-data \
        -p arrow-schema -p arrow-ord \
        -p arrow-array -p arrow-arith \
        --features ffi --no-fail-fast
    ;;
    2)
        setup_miri

        partition=$1
        total=$2

        echo "Starting Arrow MIRI run partition ${partition} out of ${total}..."
        cargo miri nextest run \
        --partition slice:"${partition}"/"${total}" \
        -p arrow-buffer -p arrow-data \
        -p arrow-schema -p arrow-ord \
        -p arrow-array -p arrow-arith \
        --features ffi --no-fail-fast
    ;;
    *)
        echo "usage: $0 [partition total]" >&2
        exit 1
    ;;
esac
