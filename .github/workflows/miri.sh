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
cargo miri nextest run -p arrow-buffer
cargo miri nextest run -p arrow-data --features ffi
cargo miri nextest run -p arrow-schema --features ffi
cargo miri nextest run -p arrow-ord
cargo miri nextest run -p arrow-array
cargo miri nextest run -p arrow-arith