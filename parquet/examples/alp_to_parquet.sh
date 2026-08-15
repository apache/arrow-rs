#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0

set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <output-dir>" >&2
  exit 2
fi

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
REPO_ROOT="$(CDPATH= cd -- "${SCRIPT_DIR}/../.." && pwd)"
INPUT_DIR="${ALP_DATASET_DIR:-${REPO_ROOT}/target/alp-benchmark-data}"

cd "$REPO_ROOT"
export RUSTFLAGS="${RUSTFLAGS:--C target-cpu=native}"
exec cargo run --quiet --release -p parquet \
  --example alp_to_parquet \
  --features arrow,zstd,experimental -- "$INPUT_DIR" "$1"
