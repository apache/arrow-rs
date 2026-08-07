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

This arrow-rs branch contains the benchmark used to evaluate the Apache
Parquet implementation of [ALP (Adaptive Lossless floating-Point encoding)][alp]
for the [Parquet ALP blog post][blog-pr]. It compares these Parquet choices for
columns of IEEE-754 `f64` values:

- PLAIN encoding without compression
- PLAIN encoding with ZSTD compression
- ALP encoding without an additional block compressor

The benchmark reports compression speed, decompression speed, and compressed
size for all 30 double-precision datasets from the CWI ALP corpus. It also
includes a focused random-access comparison using `city_temperature_f`.

This is a benchmark branch, not the main arrow-rs development branch. See
[apache/arrow-rs][arrow-rs] for the upstream project documentation.

## Run the complete benchmark

Requirements are a Rust toolchain, `curl`, `unzip`, and either `sha256sum` or
`shasum`.

```shell
./parquet/examples/alp_compression_stats.sh \
  > target/alp-compression-and-speed-results.md
```

The script:

1. Checks whether all 30 datasets already exist.
2. Downloads the 6.7 GiB CWI ALP bundle if necessary, resuming an interrupted
   download.
3. Verifies the archive using its pinned SHA-256 digest.
4. Extracts the 30 `f64` datasets into the durable, gitignored
   `target/alp-benchmark-data` directory.
5. Builds the benchmark in release mode with `-C target-cpu=native` and runs it.

The extracted datasets occupy approximately 15 GiB. Approximately 22 GiB is
required while both the archive and extracted data are present. The verified
archive is deleted after successful extraction unless `ALP_KEEP_ARCHIVE=1` is
set. Later runs reuse the extracted files.

Progress is written to stderr and the Markdown result tables are written to
stdout. The complete run normally produces one 90-row table—three Parquet
choices for each of 30 datasets—followed by the random-access table.

## Configuration

Store or reuse the complete corpus in a different directory:

```shell
ALP_DATASET_DIR=/data/cwi-alp \
  ./parquet/examples/alp_compression_stats.sh
```

Retain the downloaded archive:

```shell
ALP_KEEP_ARCHIVE=1 ./parquet/examples/alp_compression_stats.sh
```

Override the native compiler flags:

```shell
RUSTFLAGS="-C target-cpu=x86-64-v3" \
  ./parquet/examples/alp_compression_stats.sh
```

The Rust example can also run directly on a single file or a directory of
arbitrary `.bin` and `.csv` inputs:

```shell
cargo run --quiet --release -p parquet \
  --example alp_compression_stats \
  --features arrow,zstd,experimental -- /path/to/data
```

Binary inputs must contain raw little-endian `f64` values. CSV inputs must
contain one `f64` value per line. Directories are searched recursively.

The `experimental` Cargo feature exposes arrow-rs's internal page encoder,
decoder, and compression APIs to this example. ALP itself is not gated by that
feature.

## Measurements

### Compressed size

Each dataset is streamed through an `ArrowWriter` with dictionary encoding
disabled. The writer output is discarded, and Parquet metadata supplies the
compressed column-chunk size. The result includes data-page headers and excludes
the file footer.

```text
bits/value = compressed column-chunk bytes × 8 / number of values
```

### Compression and decompression speed

Every value in every dataset is processed in pages of at most 131,072 values,
or 1 MiB of uncompressed doubles. File I/O is outside the timed regions.

```text
GB/s = uncompressed values × 8 / elapsed seconds / 1,000,000,000
```

PLAIN and ALP time their Parquet page encoders and decoders. PLAIN + ZSTD
includes both stages: its compression time is PLAIN encoding plus ZSTD
compression, and its decompression time is ZSTD decompression plus PLAIN
decoding. ZSTD uses the Parquet default level.

Short final pages are repeated to stabilize timing, and elapsed time is
normalized to one execution before it is added to the dataset total. ALP's
initial row-group parameter sampling is performed outside the timed region. The
`ALL AVG.` rows are arithmetic means of the per-dataset results, so every
dataset has equal weight regardless of its size.

### Random access

The random-access case study performs 100 independent point lookups on
`city_temperature_f`. A fixed pseudo-random seed selects the same uniformly
distributed rows on every run, making the exception mix reproducible.

Each lookup begins with its encoded page already in memory:

- PLAIN resets its decoder, skips directly to the row, and decodes one value.
- ALP resets its decoder, uses the vector offsets to skip to the relevant
  vector, and decodes one value.
- PLAIN + ZSTD first decompresses the complete target page and then performs
  the PLAIN lookup.

File I/O and page discovery are excluded. The benchmark reports the elapsed
microseconds for all 100 lookups; lower is better. Measurements are repeated
adaptively, targeting at least 50 milliseconds of timed execution and a minimum
of three iterations.

## Reproducibility

Run publication measurements on an otherwise idle machine and record at least:

- CPU model
- operating system and kernel
- `rustc --version --verbose`
- full `RUSTFLAGS`
- benchmark commit SHA

Wall-clock throughput varies across machines and with CPU frequency, thermal
state, and background load. Compressed sizes and the selected random row indices
are deterministic for a fixed benchmark commit and dataset bundle.

The implementation is in
[`parquet/examples/alp_compression_stats.rs`](parquet/examples/alp_compression_stats.rs),
and the download/run wrapper is
[`parquet/examples/alp_compression_stats.sh`](parquet/examples/alp_compression_stats.sh).

[alp]: https://ir.cwi.nl/pub/33334/33334.pdf
[arrow-rs]: https://github.com/apache/arrow-rs
[blog-pr]: https://github.com/apache/parquet-site/pull/195
