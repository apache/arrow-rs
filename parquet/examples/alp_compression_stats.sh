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

set -euo pipefail

readonly ARCHIVE_URL="https://drive.usercontent.google.com/download?id=1-S7NJHIu9V8qPYnIUO3JIwie_2BLRt6N&export=download&confirm=t"
readonly ARCHIVE_SHA256="1070817918b9e2b2cc7003995927bd04fe7b942045383913d3f40437eda29831"

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
REPO_ROOT="$(CDPATH= cd -- "${SCRIPT_DIR}/../.." && pwd)"
DATA_DIR="${ALP_DATASET_DIR:-${REPO_ROOT}/target/alp-benchmark-data}"
DOWNLOAD_DIR="${ALP_DOWNLOAD_DIR:-${REPO_ROOT}/target/alp-benchmark-download}"
ARCHIVE="${DOWNLOAD_DIR}/complete_binaries.zip"

readonly DATASETS=(
  arade4.bin
  basel_temp_f.bin
  basel_wind_f.bin
  bird_migration_f.bin
  bitcoin_f.bin
  bitcoin_transactions_f.bin
  city_temperature_f.bin
  cms1.bin
  cms25.bin
  cms9.bin
  food_prices.bin
  gov10.bin
  gov26.bin
  gov30.bin
  gov31.bin
  gov40.bin
  medicare1.bin
  medicare9.bin
  neon_air_pressure.bin
  neon_bio_temp_c.bin
  neon_dew_point_temp.bin
  neon_pm10_dust.bin
  neon_wind_dir.bin
  nyc29.bin
  poi_lat.bin
  poi_lon.bin
  ssd_hdd_benchmarks_f.bin
  stocks_de.bin
  stocks_uk.bin
  stocks_usa_c.bin
)

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "error: this script requires '$1'" >&2
    exit 1
  fi
}

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$1" | awk '{print $1}'
  else
    echo "error: this script requires 'sha256sum' or 'shasum'" >&2
    exit 1
  fi
}

datasets_present() {
  local dataset
  for dataset in "${DATASETS[@]}"; do
    if [[ ! -s "${DATA_DIR}/${dataset}" ]]; then
      return 1
    fi
  done
}

download_datasets() {
  local actual_sha256

  require_command curl
  require_command unzip
  mkdir -p "$DATA_DIR" "$DOWNLOAD_DIR"

  if [[ -f "$ARCHIVE" ]] && [[ "$(sha256_file "$ARCHIVE")" == "$ARCHIVE_SHA256" ]]; then
    echo "Using the complete archive in ${ARCHIVE}" >&2
  else
    echo "Downloading the CWI ALP corpus (6.7 GiB) to ${ARCHIVE}" >&2
    curl --location --fail --show-error --continue-at - \
      --output "$ARCHIVE" "$ARCHIVE_URL"
  fi

  echo "Verifying the archive SHA-256" >&2
  actual_sha256="$(sha256_file "$ARCHIVE")"
  if [[ "$actual_sha256" != "$ARCHIVE_SHA256" ]]; then
    echo "error: SHA-256 mismatch for ${ARCHIVE}" >&2
    echo "expected: ${ARCHIVE_SHA256}" >&2
    echo "actual:   ${actual_sha256}" >&2
    exit 1
  fi

  echo "Extracting the 30 f64 datasets to ${DATA_DIR}" >&2
  unzip -q -o "$ARCHIVE" '*.bin' \
    -x 'air_sensor_f.bin' 'sp_*.bin' 'dummy2.bin' \
    -d "$DATA_DIR"

  if ! datasets_present; then
    echo "error: the archive did not contain all 30 expected f64 datasets" >&2
    exit 1
  fi

  if [[ "${ALP_KEEP_ARCHIVE:-0}" != "1" ]]; then
    rm -f -- "$ARCHIVE"
    rmdir "$DOWNLOAD_DIR" 2>/dev/null || true
  fi
}

if datasets_present; then
  echo "Using the CWI ALP corpus in ${DATA_DIR}" >&2
else
  download_datasets
fi

echo "Running the ALP compression benchmark" >&2
cd "$REPO_ROOT"
export RUSTFLAGS="${RUSTFLAGS:--C target-cpu=native}"
exec cargo run --quiet --release -p parquet \
  --example alp_compression_stats --features arrow,zstd,experimental -- "$DATA_DIR"
