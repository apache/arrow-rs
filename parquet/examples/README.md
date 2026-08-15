# Parquet ALP examples

These examples include tools for benchmarking and materializing the Parquet
ALP encoding.

## Convert `.bin` files to Parquet

`alp_to_parquet.sh` converts raw CWI ALP datasets into one-column Parquet
files. It expects the output directory as its only argument:

```shell
./parquet/examples/alp_to_parquet.sh target/alp-parquet-output
```

By default, the script reads `.bin` files recursively from
`target/alp-benchmark-data`. Set `ALP_DATASET_DIR` to use another input
directory:

```shell
ALP_DATASET_DIR=/path/to/alp-data \
  ./parquet/examples/alp_to_parquet.sh /path/to/parquet-output
```

For each input file named `<file>.bin`, the script writes:

```text
<file>.plain.zstd.parquet   # PLAIN encoding with ZSTD compression
<file>.alp.parquet          # ALP encoding without block compression
```

The Parquet files contain one required `DOUBLE` column named `value`. Existing
files with the same names are overwritten. Input is streamed in batches, so
the complete dataset is not loaded into memory.

The `.bin` files contain raw little-endian IEEE-754 `f64` values with no
header. Every eight bytes represent one value.

The script builds and runs the example with native CPU features. The direct
Cargo command is:

```shell
cargo run --release -p parquet \
  --example alp_to_parquet \
  --features arrow,zstd,experimental -- \
  target/alp-benchmark-data target/alp-parquet-output
```

The `arrow`, `zstd`, and `experimental` features are required by the example.

## Obtain the benchmark data

The companion `alp_compression_stats.sh` script downloads and verifies the
CWI ALP corpus, then extracts the datasets into `target/alp-benchmark-data`:

```shell
./parquet/examples/alp_compression_stats.sh \
  > target/alp-compression-and-speed-results.md
```

The download is approximately 6.7 GiB and the extracted datasets use roughly
15 GiB. The archive is removed after extraction by default. The converter can
then be run against the extracted files as shown above.

## Benchmark implementation

`alp_compression_stats.rs` compares PLAIN, PLAIN + ZSTD, and ALP encoding for
the CWI datasets. It reports compressed size, compression and decompression
throughput, and a deterministic random-access comparison.
