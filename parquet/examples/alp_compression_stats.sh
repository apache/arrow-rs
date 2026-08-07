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

markdown_escape() {
  local value="$1"
  value="${value//\\/\\\\}"
  value="${value//|/\\|}"
  printf '%s' "$value"
}

cpu_model() {
  local model=""

  if [[ -r /proc/cpuinfo ]]; then
    model="$(awk -F ': *' '/^model name[[:space:]]*:/{print $2; exit}' /proc/cpuinfo)"
  fi
  if [[ -z "$model" ]] && command -v sysctl >/dev/null 2>&1; then
    model="$(sysctl -n machdep.cpu.brand_string 2>/dev/null || true)"
  fi
  if [[ -z "$model" ]] && command -v sysctl >/dev/null 2>&1; then
    model="$(sysctl -n hw.model 2>/dev/null || true)"
  fi
  printf '%s' "${model:-unknown}"
}

cpu_simd() {
  local architecture features=""

  architecture="$(uname -m)"
  if [[ -r /proc/cpuinfo ]]; then
    features="$(awk -F ': *' '/^(flags|Features)[[:space:]]*:/{print $2; exit}' /proc/cpuinfo)"
  elif command -v sysctl >/dev/null 2>&1; then
    features="$(
      sysctl -n machdep.cpu.features machdep.cpu.leaf7_features \
        2>/dev/null || true
    )"
  fi
  features="$(printf '%s' "$features" | tr '[:upper:]' '[:lower:]')"

  case "$architecture" in
    x86_64 | amd64 | i386 | i686)
      if [[ " $features " == *" avx512f "* ]]; then
        printf '%s' "AVX-512F, AVX2, AVX"
      elif [[ " $features " == *" avx2 "* ]]; then
        printf '%s' "AVX2, AVX"
      elif [[ " $features " == *" avx "* ]]; then
        printf '%s' "AVX"
      else
        printf '%s' "no AVX"
      fi
      ;;
    aarch64 | arm64 | arm*)
      if [[ " $features " == *" sve2 "* ]]; then
        printf '%s' "SVE2, SVE, NEON"
      elif [[ " $features " == *" sve "* ]]; then
        printf '%s' "SVE, NEON"
      elif [[ " $features " == *" asimd "* ]] ||
        [[ " $features " == *" neon "* ]] || [[ "$(uname -s)" == "Darwin" ]]; then
        printf '%s' "NEON"
      else
        printf '%s' "unknown"
      fi
      ;;
    *)
      printf '%s' "unknown"
      ;;
  esac
}

logical_cpus() {
  local count=""

  if command -v getconf >/dev/null 2>&1; then
    count="$(getconf _NPROCESSORS_ONLN 2>/dev/null || true)"
  fi
  if [[ -z "$count" ]] && command -v nproc >/dev/null 2>&1; then
    count="$(nproc 2>/dev/null || true)"
  fi
  printf '%s' "${count:-unknown}"
}

cpu_governor() {
  local governor_file="/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor"
  if [[ -r "$governor_file" ]]; then
    tr -d '\n' < "$governor_file"
  else
    printf '%s' "unavailable"
  fi
}

safe_rustflags() {
  if [[ "$RUSTFLAGS" =~ ^[-A-Za-z0-9_=+.,[:space:]]+$ ]]; then
    printf '%s' "$RUSTFLAGS"
  else
    printf '%s' "set; value omitted because it contains paths or shell characters"
  fi
}

print_environment() {
  local commit worktree llvm_version

  commit="$(git rev-parse HEAD)"
  if [[ -n "$(git status --porcelain --untracked-files=normal)" ]]; then
    worktree="dirty"
  else
    worktree="clean"
  fi
  llvm_version="$(rustc --version --verbose | awk -F ': *' '$1 == "LLVM version" {print $2}')"

  printf '## Benchmark environment\n\n'
  printf '| Environment | Value |\n'
  printf '|---|---|\n'
  printf '| UTC timestamp | `%s` |\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  printf '| Commit | `%s` |\n' "$commit"
  printf '| Worktree | %s |\n' "$worktree"
  printf '| CPU | %s |\n' "$(markdown_escape "$(cpu_model)")"
  printf '| Architecture | `%s` |\n' "$(uname -m)"
  printf '| SIMD ISA | `%s` |\n' "$(cpu_simd)"
  printf '| Logical CPUs | %s |\n' "$(logical_cpus)"
  printf '| OS and kernel | `%s %s` |\n' "$(uname -s)" "$(uname -r)"
  printf '| CPU governor | `%s` |\n' "$(cpu_governor)"
  printf '| Rust | `%s` |\n' "$(markdown_escape "$(rustc --version)")"
  printf '| LLVM | `%s` |\n' "$(markdown_escape "${llvm_version:-unknown}")"
  printf '| Cargo | `%s` |\n' "$(markdown_escape "$(cargo --version)")"
  printf '| RUSTFLAGS | `%s` |\n' "$(markdown_escape "$(safe_rustflags)")"
  printf '| Dataset archive SHA-256 | `%s` |\n\n' "$ARCHIVE_SHA256"
}

if datasets_present; then
  echo "Using the CWI ALP corpus in ${DATA_DIR}" >&2
else
  download_datasets
fi

echo "Running the ALP compression benchmark" >&2
cd "$REPO_ROOT"
export RUSTFLAGS="${RUSTFLAGS:--C target-cpu=native}"
print_environment
exec cargo run --quiet --release -p parquet \
  --example alp_compression_stats --features arrow,zstd,experimental -- "$DATA_DIR"
