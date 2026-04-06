#!/bin/bash
#
# Script
#
# Must be run with nightly rust for example
# rustup default nightly

set -e

export MIRIFLAGS="-Zmiri-disable-isolation"
cargo miri setup
cargo clean

echo "Starting Arrow MIRI run..."
cargo miri nextest run \
    -p arrow-buffer -p arrow-data \
    -p arrow-schema -p arrow-ord \
    -p arrow-array -p arrow-arith \
    --features ffi --no-fail-fast