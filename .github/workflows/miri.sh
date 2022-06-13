#!/bin/bash
#
# Script
#
# Must be run with nightly rust for example
# rustup default nightly


# stacked borrows checking uses too much memory to run successfully in github actions
# re-enable if the CI is migrated to something more powerful (https://github.com/apache/arrow-rs/issues/1833)
# see also https://github.com/rust-lang/miri/issues/1367
export MIRIFLAGS="-Zmiri-disable-isolation -Zmiri-disable-stacked-borrows"
cargo miri setup
cargo clean

echo "Starting Arrow MIRI run..."
cargo miri test -p arrow -- --skip csv --skip ipc --skip json
echo "Miri finished with exit code $?"
