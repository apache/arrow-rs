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

# Generate the CodSpeed benchmark matrix.
#
# Emits to stdout a compact JSON array of {"crate","bench"} objects — one per
# `[[bench]]` target in the selected workspace crates. Each object becomes one
# CodSpeed shard (`cargo codspeed run -p <crate> --bench <bench>`). Sharding
# one job per bench target keeps every shard under CodSpeed's hard per-upload
# limit of 1000 benchmarks, on the assumption that no single bench target
# defines >1000 benchmark cases. If a target ever crosses that line, split the
# bench source rather than reworking the sharding here.
#
# Targets are discovered structurally via `cargo metadata` (no Cargo.toml text
# parsing, no hardcoded crate list), so new crates and new `[[bench]]` targets
# are picked up automatically.
#
# A bench target is dropped from the matrix when its crate's Cargo.toml marks
# it skipped:
#
#   [package.metadata.codspeed.benches]
#   merge_kernels = { skip = true }   # broken at runtime, fix and remove
#
# cargo surfaces that table at .packages[].metadata.codspeed.benches, so the
# skip list lives next to the bench in the crate that owns it.
#
# Usage:
#   codspeed-matrix.sh                # every workspace crate
#   codspeed-matrix.sh arrow parquet  # only the named crates (must be members)

set -euo pipefail

metadata="$(cargo metadata --format-version 1 --no-deps)"

# Reject explicitly-requested crates that are not workspace members, so a typo
# in a `bench:<crate>` label fails loudly instead of silently benching nothing.
if [ "$#" -gt 0 ]; then
  members="$(jq -r '.packages[].name' <<<"$metadata")"
  for crate in "$@"; do
    if ! grep -qxF "$crate" <<<"$members"; then
      echo "::error::Unknown workspace crate '$crate'" >&2
      exit 1
    fi
  done
fi

selected="$(printf '%s\n' "$@" | jq -Rsc 'split("\n") | map(select(length > 0))')"

jq -c \
  --argjson selected "$selected" '
  [ .packages[]
    | .name as $crate
    | (.metadata.codspeed.benches // {}) as $cfg
    | select(($selected | length) == 0 or ($selected | index($crate)))
    | .targets[]
    | select(.kind | index("bench"))
    | select(($cfg[.name].skip // false) | not)
    | { crate: $crate, bench: .name }
  ]
' <<<"$metadata"
