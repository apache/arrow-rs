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
cargo miri test -p arrow-buffer
cargo miri test -p arrow-data --features ffi
cargo miri test -p arrow-schema --features ffi
cargo miri test -p arrow-ord
cargo miri test -p arrow-array
cargo miri test -p arrow-arith