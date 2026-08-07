<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Apache Parquet ALP benchmark

This benchmark evaluates the Apache Parquet implementation of
[ALP (Adaptive Lossless floating-Point encoding)][alp] for the
[Parquet ALP blog post][blog-pr]. It compares columns of `f64` values using:

- PLAIN encoding without compression
- PLAIN encoding with ZSTD compression
- ALP encoding without an additional block compressor

It reports compression speed, decompression speed, and compressed size for all
30 double-precision datasets in the CWI ALP corpus, plus a focused random-access
comparison on `city_temperature_f`.

## Results preview

Example from an AMD Ryzen AI 9 HX PRO 470 using `-C target-cpu=native`. Speed is
machine-dependent; compressed size is deterministic. The speed and size rows
are arithmetic means of the 30 per-dataset results.

```text
AVERAGE OF ALL 30 DATASETS

                     COMPRESSION     DECOMPRESSION     COMPRESSED SIZE
PLAIN                 70.052 GB/s     70.237 GB/s       64.01 bits/value
PLAIN + ZSTD           1.485 GB/s      3.232 GB/s       22.75 bits/value
ALP                    2.145 GB/s     32.432 GB/s       24.27 bits/value

100 RANDOM ROWS FROM city_temperature_f

PLAIN                      2.744 µs
PLAIN + ZSTD          74,314.500 µs
ALP                        9.717 µs
```

<details>
<summary>Full results for all 30 datasets</summary>

| Dataset | Parquet choice | Compression (GB/s) | Decompression (GB/s) | Compressed size (bits/value) |
|---|---|---:|---:|---:|
| arade4 | PLAIN | 69.061 | 70.101 | 64.01 |
| arade4 | PLAIN + ZSTD | 0.642 | 1.610 | 37.39 |
| arade4 | ALP | 2.596 | 33.633 | 24.99 |
| basel_temp_f | PLAIN | 59.129 | 75.584 | 64.01 |
| basel_temp_f | PLAIN + ZSTD | 0.459 | 1.647 | 23.07 |
| basel_temp_f | ALP | 1.394 | 25.053 | 29.23 |
| basel_wind_f | PLAIN | 73.937 | 76.520 | 64.01 |
| basel_wind_f | PLAIN + ZSTD | 0.580 | 1.700 | 18.53 |
| basel_wind_f | ALP | 2.457 | 26.181 | 29.87 |
| bird_migration_f | PLAIN | 64.358 | 105.274 | 64.01 |
| bird_migration_f | PLAIN + ZSTD | 0.627 | 1.753 | 23.49 |
| bird_migration_f | ALP | 2.634 | 25.677 | 20.24 |
| bitcoin_f | PLAIN | 88.301 | 117.116 | 64.07 |
| bitcoin_f | PLAIN + ZSTD | 0.575 | 1.619 | 50.01 |
| bitcoin_f | ALP | 1.757 | 29.426 | 27.18 |
| bitcoin_transactions_f | PLAIN | 64.054 | 72.216 | 64.01 |
| bitcoin_transactions_f | PLAIN + ZSTD | 1.077 | 1.979 | 47.96 |
| bitcoin_transactions_f | ALP | 2.334 | 20.216 | 41.27 |
| city_temperature_f | PLAIN | 75.395 | 74.838 | 64.01 |
| city_temperature_f | PLAIN + ZSTD | 0.565 | 1.358 | 17.67 |
| city_temperature_f | ALP | 2.785 | 32.504 | 10.80 |
| cms1 | PLAIN | 73.205 | 28.906 | 64.01 |
| cms1 | PLAIN + ZSTD | 0.646 | 1.485 | 26.84 |
| cms1 | ALP | 1.357 | 13.815 | 35.19 |
| cms25 | PLAIN | 71.305 | 72.761 | 64.01 |
| cms25 | PLAIN + ZSTD | 0.805 | 1.808 | 58.11 |
| cms25 | ALP | 2.138 | 22.072 | 41.17 |
| cms9 | PLAIN | 72.501 | 72.698 | 64.01 |
| cms9 | PLAIN + ZSTD | 0.706 | 1.446 | 11.71 |
| cms9 | ALP | 2.755 | 31.976 | 12.16 |
| food_prices | PLAIN | 72.059 | 74.890 | 64.01 |
| food_prices | PLAIN + ZSTD | 0.575 | 1.337 | 18.13 |
| food_prices | ALP | 1.158 | 20.248 | 23.20 |
| gov10 | PLAIN | 73.446 | 74.376 | 64.01 |
| gov10 | PLAIN + ZSTD | 0.507 | 1.233 | 29.12 |
| gov10 | ALP | 1.745 | 25.863 | 29.88 |
| gov26 | PLAIN | 75.234 | 76.379 | 64.01 |
| gov26 | PLAIN + ZSTD | 12.403 | 25.304 | 0.20 |
| gov26 | ALP | 2.088 | 94.508 | 1.40 |
| gov30 | PLAIN | 75.650 | 76.346 | 64.01 |
| gov30 | PLAIN + ZSTD | 2.201 | 5.256 | 4.52 |
| gov30 | ALP | 1.207 | 38.220 | 17.88 |
| gov31 | PLAIN | 66.988 | 67.505 | 64.01 |
| gov31 | PLAIN + ZSTD | 3.851 | 8.849 | 1.65 |
| gov31 | ALP | 2.758 | 45.704 | 6.77 |
| gov40 | PLAIN | 58.871 | 61.174 | 64.01 |
| gov40 | PLAIN + ZSTD | 8.927 | 16.031 | 0.43 |
| gov40 | ALP | 2.920 | 71.463 | 2.59 |
| medicare1 | PLAIN | 56.866 | 53.477 | 64.01 |
| medicare1 | PLAIN + ZSTD | 0.518 | 1.372 | 31.68 |
| medicare1 | ALP | 1.215 | 16.534 | 40.46 |
| medicare9 | PLAIN | 60.603 | 63.031 | 64.01 |
| medicare9 | PLAIN + ZSTD | 0.691 | 1.434 | 11.86 |
| medicare9 | ALP | 2.697 | 32.472 | 12.82 |
| neon_air_pressure | PLAIN | 69.164 | 71.285 | 64.01 |
| neon_air_pressure | PLAIN + ZSTD | 0.784 | 1.972 | 11.85 |
| neon_air_pressure | ALP | 2.649 | 33.353 | 16.48 |
| neon_bio_temp_c | PLAIN | 74.058 | 75.026 | 64.01 |
| neon_bio_temp_c | PLAIN + ZSTD | 0.560 | 1.546 | 16.84 |
| neon_bio_temp_c | ALP | 2.770 | 32.755 | 10.81 |
| neon_dew_point_temp | PLAIN | 71.090 | 72.599 | 64.01 |
| neon_dew_point_temp | PLAIN + ZSTD | 0.473 | 1.598 | 23.73 |
| neon_dew_point_temp | ALP | 2.720 | 29.753 | 13.63 |
| neon_pm10_dust | PLAIN | 53.474 | 74.086 | 64.01 |
| neon_pm10_dust | PLAIN + ZSTD | 0.841 | 1.652 | 7.79 |
| neon_pm10_dust | ALP | 1.850 | 34.850 | 8.41 |
| neon_wind_dir | PLAIN | 69.536 | 71.668 | 64.01 |
| neon_wind_dir | PLAIN + ZSTD | 0.491 | 1.483 | 24.41 |
| neon_wind_dir | ALP | 2.706 | 46.888 | 15.94 |
| nyc29 | PLAIN | 71.389 | 69.178 | 64.01 |
| nyc29 | PLAIN + ZSTD | 0.625 | 1.496 | 24.67 |
| nyc29 | ALP | 2.478 | 22.596 | 40.43 |
| poi_lat | PLAIN | 73.164 | 17.588 | 64.01 |
| poi_lat | PLAIN + ZSTD | 0.683 | 1.670 | 57.78 |
| poi_lat | ALP | 1.534 | 11.840 | 88.19 |
| poi_lon | PLAIN | 74.497 | 21.699 | 64.01 |
| poi_lon | PLAIN + ZSTD | 0.864 | 1.816 | 60.44 |
| poi_lon | ALP | 1.681 | 15.246 | 79.12 |
| ssd_hdd_benchmarks_f | PLAIN | 84.857 | 106.390 | 64.02 |
| ssd_hdd_benchmarks_f | PLAIN + ZSTD | 0.803 | 1.749 | 12.98 |
| ssd_hdd_benchmarks_f | ALP | 2.724 | 34.192 | 16.04 |
| stocks_de | PLAIN | 70.813 | 71.589 | 64.01 |
| stocks_de | PLAIN + ZSTD | 0.675 | 1.626 | 10.07 |
| stocks_de | ALP | 1.483 | 33.083 | 11.20 |
| stocks_uk | PLAIN | 70.262 | 72.069 | 64.01 |
| stocks_uk | PLAIN + ZSTD | 0.669 | 1.488 | 11.29 |
| stocks_uk | ALP | 0.938 | 35.205 | 12.75 |
| stocks_usa_c | PLAIN | 68.303 | 70.749 | 64.01 |
| stocks_usa_c | PLAIN + ZSTD | 0.737 | 1.637 | 8.24 |
| stocks_usa_c | ALP | 2.818 | 37.636 | 7.95 |
| **ALL AVG.** | **PLAIN** | **70.052** | **70.237** | **64.01** |
| **ALL AVG.** | **PLAIN + ZSTD** | **1.485** | **3.232** | **22.75** |
| **ALL AVG.** | **ALP** | **2.145** | **32.432** | **24.27** |

</details>

## Run

Requirements are a Rust toolchain, `curl`, `unzip`, and either `sha256sum` or
`shasum`.

```shell
./parquet/examples/alp_compression_stats.sh \
  > target/alp-compression-and-speed-results.md
```

The script downloads and verifies the 6.7 GiB CWI bundle, extracts the 30 `f64`
datasets, builds with `-C target-cpu=native`, and writes Markdown results to
stdout. Downloads resume if interrupted, and later runs reuse the extracted
files.

The extracted corpus occupies approximately 15 GiB. Approximately 22 GiB is
needed while the archive is also present; the archive is deleted after
extraction by default.

Configuration:

- `ALP_DATASET_DIR=/path` changes the durable dataset location.
- `ALP_DOWNLOAD_DIR=/path` changes the archive download location.
- `ALP_KEEP_ARCHIVE=1` retains the verified archive.
- An existing `RUSTFLAGS` overrides `-C target-cpu=native`.

The Rust example also accepts an individual raw little-endian `f64` `.bin`
file, a one-value-per-line `.csv` file, or a recursively searched directory:

```shell
cargo run --quiet --release -p parquet \
  --example alp_compression_stats \
  --features arrow,zstd,experimental -- /path/to/data
```

The `experimental` feature exposes internal page APIs to the example; ALP
itself is not gated by that feature.

## What is measured

- **Compressed size:** An `ArrowWriter` with dictionary encoding disabled
  supplies the compressed column-chunk size. It includes data-page headers and
  excludes the file footer.
- **Speed:** Every value is encoded and decoded in pages of at most 131,072
  values. GB/s uses the uncompressed input size, and file I/O is excluded.
  PLAIN + ZSTD includes both pipeline stages. Short pages are repeated and
  normalized for stable timing; ALP parameter sampling is outside the timed
  region.
- **Random access:** A fixed seed selects the same 100 rows from
  `city_temperature_f` on every run. PLAIN and ALP skip to and decode one value.
  PLAIN + ZSTD decompresses the complete in-memory target page before the PLAIN
  lookup. File I/O and page discovery are excluded.

The complete output contains all 90 dataset/encoding combinations and explains
the units and averaging beside the tables.

## Reproducibility and privacy

The wrapper records a privacy-safe environment table: timestamp, commit and
worktree state, CPU, architecture, logical CPU count, governor when available,
OS/kernel, Rust/Cargo/LLVM versions, safe compiler flags, and the dataset
archive digest.

It does not print hostnames, usernames, local paths, network information, Git
remotes, the complete environment, or raw `/proc/cpuinfo`. Compiler flags that
contain paths or shell characters are reported as set but omitted. Review
generated results before publishing them.

Run publication measurements on an otherwise idle machine. Throughput varies
with hardware, CPU frequency, thermal state, and background load.

Benchmark implementation:
[`alp_compression_stats.rs`](parquet/examples/alp_compression_stats.rs) ·
[`alp_compression_stats.sh`](parquet/examples/alp_compression_stats.sh)

[alp]: https://ir.cwi.nl/pub/33334/33334.pdf
[blog-pr]: https://github.com/apache/parquet-site/pull/195
