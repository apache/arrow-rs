#!/bin/bash
#
# Script
#
# Must be run with nightly rust for example
# rustup default nightly
export MIRIFLAGS="-Zmiri-disable-isolation"
cargo miri setup
cargo clean
# Currently only the arrow crate is tested with miri
# IO related tests and some unsupported tests are skipped
cargo miri test -p arrow -- --skip csv --skip ipc --skip json
