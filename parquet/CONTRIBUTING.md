## Build

Run `cargo build` or `cargo build --release` to build in release mode.
Some features take advantage of SSE4.2 instructions, which can be
enabled by adding `RUSTFLAGS="-C target-feature=+sse4.2"` before the
`cargo build` command.

## Test

Run `cargo test` for unit tests. To also run tests related to the binaries, use `cargo test --features cli`.

## Binaries

The following binaries are provided (use `cargo install --features cli` to install them):

- **parquet-schema** for printing Parquet file schema and metadata.
  `Usage: parquet-schema <file-path>`, where `file-path` is the path to a Parquet file. Use `-v/--verbose` flag
  to print full metadata or schema only (when not specified only schema will be printed).

- **parquet-read** for reading records from a Parquet file.
  `Usage: parquet-read <file-path> [num-records]`, where `file-path` is the path to a Parquet file,
  and `num-records` is the number of records to read from a file (when not specified all records will
  be printed). Use `-j/--json` to print records in JSON lines format.

- **parquet-rowcount** for reporting the number of records in one or more Parquet files.
  `Usage: parquet-rowcount <file-paths>...`, where `<file-paths>...` is a space separated list of one or more
  files to read.

If you see `Library not loaded` error, please make sure `LD_LIBRARY_PATH` is set properly:

```
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$(rustc --print sysroot)/lib
```

## Benchmarks

Run `cargo bench` for benchmarks.

## Docs

To build documentation, run `cargo doc --no-deps`.
To compile and view in the browser, run `cargo doc --no-deps --open`.
