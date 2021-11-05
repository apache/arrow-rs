#!/bin/bash
#
# Script
#
# Must be run with nightly rust for example
# rustup default nightly


export MIRIFLAGS="-Zmiri-disable-isolation"
cargo miri setup
cargo clean

run_miri() {
    # Currently only the arrow crate is tested with miri
    # IO related tests and some unsupported tests are skipped
    cargo miri test -p arrow -- --skip csv --skip ipc --skip json
}

# If MIRI fails, automatically retry
# Seems like miri is occasionally killed by the github runner
# https://github.com/apache/arrow-rs/issues/879
for i in `seq 1 5`; do
    echo "Starting Arrow MIRI run..."
    run_miri && break
    echo "foo" > /tmp/data.txt
done
