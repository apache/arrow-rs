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


# run bench for 2 git hash/branch/tag
#
# ./run_bench.sh [baseline] [compare]
# to only run some bench, use EXTRA_ARGS="--bench interleave_kernels \"interleave list struct.*\"" ./run_bench.sh HEAD main

set -e

function patch_cargo_toml() {
    if [[ ! -z $(grep "bench = false" arrow/Cargo.toml) ]]; then 
        echo "Patch not required"
    else
        echo "Patch required"
        cp arrow/Cargo.toml arrow/Cargo.toml.org
        sed '/^\[lib\]/a bench = false' arrow/Cargo.toml.org > arrow/Cargo.toml
    fi
}

function run_bench( ) {
    cmd="cargo bench ${EXTRA_ARGS} -- --save-baseline arrow-$1"
    echo $cmd
    bash -c "$cmd"
}


# checkout baseline and run bench 
git checkout $1

patch_cargo_toml 
run_bench $1

# checkout compare and run bench
git reset --hard
git checkout $2

patch_cargo_toml 
run_bench $2