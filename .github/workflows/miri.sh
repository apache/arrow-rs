#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

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
    -p arrow-cmp
    -p arrow-ord
    -p arrow-row
    -p arrow-schema
    -p arrow-select
    -p arrow-string
    -p parquet
"

setup_miri() {
    export MIRIFLAGS="-Zmiri-disable-isolation -Zmiri-no-extra-rounding-error"
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
